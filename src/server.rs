use std::convert::{TryFrom, TryInto};
use std::pin::Pin;
use std::sync::Arc;

use backoff::ExponentialBackoff;
use futures::future::{FutureExt, TryFutureExt};
use futures::sink::SinkExt;
use futures::stream::{FuturesOrdered, StreamExt};
use tokio::sync::Mutex;
use tokio::{
    net::{TcpListener, TcpStream, ToSocketAddrs},
    select,
};
use tokio_util::codec::{FramedRead, FramedWrite};
use tower::{Service, ServiceExt};
use tracing::Instrument;
use tracing::Level;

use crate::{
    BoxError, ConsensusRequest, ConsensusResponse, InfoRequest, InfoResponse, MempoolRequest,
    MempoolResponse, MethodKind, Request, Response, SnapshotRequest, SnapshotResponse,
};

/// An ABCI server which listens for connections and forwards requests to four
/// component ABCI [`Service`]s.
pub struct Server<C, M, I, S> {
    consensus: C,
    mempool: M,
    info: I,
    snapshot: S,
}

pub struct ServerBuilder<C, M, I, S> {
    consensus: Option<C>,
    mempool: Option<M>,
    info: Option<I>,
    snapshot: Option<S>,
}

impl<C, M, I, S> Default for ServerBuilder<C, M, I, S> {
    fn default() -> Self {
        Self {
            consensus: None,
            mempool: None,
            info: None,
            snapshot: None,
        }
    }
}

impl<C, M, I, S> ServerBuilder<C, M, I, S>
where
    C: Service<ConsensusRequest, Response = ConsensusResponse, Error = BoxError>
        + Send
        + Clone
        + 'static,
    C::Future: Send + 'static,
    M: Service<MempoolRequest, Response = MempoolResponse, Error = BoxError>
        + Send
        + Clone
        + 'static,
    M::Future: Send + 'static,
    I: Service<InfoRequest, Response = InfoResponse, Error = BoxError> + Send + Clone + 'static,
    I::Future: Send + 'static,
    S: Service<SnapshotRequest, Response = SnapshotResponse, Error = BoxError>
        + Send
        + Clone
        + 'static,
    S::Future: Send + 'static,
{
    pub fn consensus(mut self, consensus: C) -> Self {
        self.consensus = Some(consensus);
        self
    }

    pub fn mempool(mut self, mempool: M) -> Self {
        self.mempool = Some(mempool);
        self
    }

    pub fn info(mut self, info: I) -> Self {
        self.info = Some(info);
        self
    }

    pub fn snapshot(mut self, snapshot: S) -> Self {
        self.snapshot = Some(snapshot);
        self
    }

    pub fn finish(self) -> Option<Server<C, M, I, S>> {
        let consensus = if let Some(consensus) = self.consensus {
            consensus
        } else {
            return None;
        };

        let mempool = if let Some(mempool) = self.mempool {
            mempool
        } else {
            return None;
        };

        let info = if let Some(info) = self.info {
            info
        } else {
            return None;
        };

        let snapshot = if let Some(snapshot) = self.snapshot {
            snapshot
        } else {
            return None;
        };

        Some(Server {
            consensus,
            mempool,
            info,
            snapshot,
        })
    }
}

impl<C, M, I, S> Server<C, M, I, S>
where
    C: Service<ConsensusRequest, Response = ConsensusResponse, Error = BoxError>
        + Send
        + Sync
        + Clone
        + 'static,
    C::Future: Send + 'static,
    M: Service<MempoolRequest, Response = MempoolResponse, Error = BoxError>
        + Send
        + Sync
        + Clone
        + 'static,
    M::Future: Send + 'static,
    I: Service<InfoRequest, Response = InfoResponse, Error = BoxError>
        + Send
        + Sync
        + Clone
        + 'static,
    I::Future: Send + 'static,
    S: Service<SnapshotRequest, Response = SnapshotResponse, Error = BoxError>
        + Send
        + Sync
        + Clone
        + 'static,
    S::Future: Send + 'static,
{
    pub fn builder() -> ServerBuilder<C, M, I, S> {
        ServerBuilder::default()
    }

    pub async fn listen<A: ToSocketAddrs + std::fmt::Debug>(self, addr: A) -> Result<(), BoxError> {
        tracing::info!(?addr, "starting ABCI server");
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        tracing::info!(?local_addr, "bound tcp listener");

        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    // set parent: None for the connection span, as it should
                    // exist independently of the listener's spans.
                    let span = tracing::span!(parent: None, Level::ERROR, "abci", ?addr);
                    let conn = Connection {
                        consensus: self.consensus.clone(),
                        mempool: self.mempool.clone(),
                        info: self.info.clone(),
                        snapshot: self.snapshot.clone(),
                    };
                    tokio::spawn(
                        async move { conn.run_with_backoff(socket).await.unwrap() }
                            .instrument(span),
                    );
                }
                Err(e) => {
                    tracing::warn!({ %e }, "error accepting new tcp connection");
                }
            }
        }
    }
}

#[derive(Clone)]
struct Connection<C, M, I, S> {
    consensus: C,
    mempool: M,
    info: I,
    snapshot: S,
}

impl<C, M, I, S> Connection<C, M, I, S>
where
    C: Service<ConsensusRequest, Response = ConsensusResponse, Error = BoxError>
        + Clone
        + Send
        + 'static,
    C::Future: Send + 'static,
    M: Service<MempoolRequest, Response = MempoolResponse, Error = BoxError>
        + Clone
        + Send
        + 'static,
    C: Service<ConsensusRequest, Response = ConsensusResponse, Error = BoxError>
        + Clone
        + Send
        + 'static,
    M::Future: Send + 'static,
    I: Service<InfoRequest, Response = InfoResponse, Error = BoxError> + Clone + Send + 'static,
    I::Future: Send + 'static,
    S: Service<SnapshotRequest, Response = SnapshotResponse, Error = BoxError>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    async fn run_with_backoff(self, socket: TcpStream) -> Result<(), BoxError> {
        let socket = Arc::new(Mutex::new(socket));
        backoff::future::retry::<_, BoxError, _, _, _>(ExponentialBackoff::default(), || async {
            let mut socket = socket.lock().await;
            let run_result = self.clone().run(&mut socket).await;

            if let Err(e) = run_result {
                match e.downcast::<tower::load_shed::error::Overloaded>() {
                    Err(e) => {
                        tracing::error!("error {e} in a connection handler");
                        return Err(backoff::Error::Permanent(e));
                    }
                    Ok(e) => {
                        tracing::warn!("a service is overloaded - backing off");
                        return Err(backoff::Error::transient(e));
                    }
                }
            }
            Ok(())
        })
        .await
    }

    async fn run(mut self, socket: &mut TcpStream) -> Result<(), BoxError> {
        tracing::info!("listening for requests");

        use tendermint_proto::abci as pb;

        let (mut request_stream, mut response_sink) = {
            use crate::codec::{Decode, Encode};
            let (read, write) = socket.split();
            (
                FramedRead::new(read, Decode::<pb::Request>::default()).peekable(),
                FramedWrite::new(write, Encode::<pb::Response>::default()),
            )
        };
        let mut pinned_stream = Pin::new(&mut request_stream);
        let mut responses = FuturesOrdered::new();

        // We only peek the next request once it's popped from the request_stream
        // to avoid crashing Tendermint in case the service call fails because
        // it's e.g. overloaded.
        let mut peeked_req = false;
        loop {
            select! {
                req = pinned_stream.as_mut().peek(), if !peeked_req => {
                    peeked_req = true;
                    let proto = match req {
                        Some(Ok(proto)) => proto.clone(),
                        Some(Err(_)) => return Err(pinned_stream.next().await.unwrap().unwrap_err()),
                        None => return Ok(()),
                    };
                    let request = Request::try_from(proto)?;
                    tracing::debug!(?request, "new request");
                    let kind = request.kind();
                    match &kind {
                        MethodKind::Consensus => {
                            let request = request.try_into().expect("checked kind");
                            match self.consensus.ready().await {
                                Ok(consensus) => {
                                    let response = consensus.call(request);
                                    // Need to box here for type erasure
                                    responses.push_back(response.map_ok(Response::from).map(|r| (r, kind)).boxed());
                                },
                                Err(err) => {
                                    tracing::error!("consensus service is not ready: {}", err);
                                }
                            }
                        }
                        MethodKind::Mempool => {
                            let request = request.try_into().expect("checked kind");
                            match self.mempool.ready().await {
                                Ok(mempool) => {
                                    let response = mempool.call(request);
                                    // Need to box here for type erasure
                                    responses.push_back(response.map_ok(Response::from).map(|r| (r, kind)).boxed());
                                },
                                Err(err) => {
                                    tracing::error!("mempool service is not ready: {}", err);
                                }
                            }
                        }
                        MethodKind::Snapshot => {
                            let request = request.try_into().expect("checked kind");
                            match self.snapshot.ready().await {
                                Ok(snapshot) => {
                                    let response = snapshot.call(request);
                                    // Need to box here for type erasure
                                    responses.push_back(response.map_ok(Response::from).map(|r| (r, kind)).boxed());
                                },
                                Err(err) => {
                                    tracing::error!("snapshot service is not ready: {}", err);
                                }
                            }
                        }
                        MethodKind::Info => {
                            let request = request.try_into().expect("checked kind");
                            match self.info.ready().await {
                                Ok(info) => {
                                    let response = info.call(request);
                                    // Need to box here for type erasure
                                    responses.push_back(response.map_ok(Response::from).map(|r| (r, kind)).boxed());
                                },
                                Err(err) => {
                                    tracing::error!("info service is not ready: {}", err);
                                }
                            }
                        }
                        MethodKind::Flush => {
                            // Instead of propagating Flush requests to the application,
                            // handle them here by awaiting all pending responses.
                            tracing::debug!(responses.len = responses.len(), "flushing responses");
                            while let Some((response, kind)) = responses.next().await {
                                tracing::debug!(?response, "flushing response");
                                let response = match response {
                                    Ok(rsp) => rsp,
                                    Err(err) => match kind {
                                        // TODO: allow to fail on Snapshot?
                                        MethodKind::Info =>
                                            Response::Exception(pb::ResponseException { error: err.to_string() }),
                                        _ => return Err(err)
                                    }
                                };
                                response_sink.send(response.into()).await?;
                            }
                            // Allow to peek next request if none of the `response?` above failed ...
                            peeked_req = false;
                            // ... and pop the last peeked request
                            pinned_stream.next().await.unwrap()?;
                            // Now we need to tell Tendermint we've flushed responses
                            response_sink.send(Response::Flush(Default::default()).into()).await?;
                        }
                    }
                }
                rsp = responses.next(), if !responses.is_empty() => {
                    let (rsp, kind) = rsp.expect("didn't poll when responses was empty");
                    let response = match rsp {
                        Ok(rsp) => rsp,
                        Err(err) => match kind {
                            // TODO: allow to fail on Snapshot?
                            MethodKind::Info =>
                                Response::Exception(pb::ResponseException { error: err.to_string() }),
                            _ => return Err(err)
                        }
                    };
                    // Allow to peek next request if the `?` above didn't fail ...
                    peeked_req = false;
                    // ... and pop the last peeked request
                    pinned_stream.next().await.unwrap()?;
                    tracing::debug!(?response, "sending response");
                    response_sink.send(response.into()).await?;
                }
            }
        }
    }
}
