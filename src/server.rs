use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use backoff::ExponentialBackoff;
use futures::future::{FutureExt, TryFutureExt};
use futures::sink::SinkExt;
use futures::stream::{FuturesOrdered, StreamExt};
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
#[derive(Clone)]
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

    pub async fn listen<A: ToSocketAddrs + std::fmt::Debug + Copy + Send + Sync + 'static>(
        self,
        addr: A,
    ) -> Result<(), BoxError> {
        tracing::info!(?addr, "starting ABCI server");
        let listener = Arc::new(TcpListener::bind(addr).await?);
        let local_addr = listener.local_addr()?;
        tracing::info!(?local_addr, "bound tcp listener");

        loop {
            // set parent: None for the connection span, as it should
            // exist independently of the listener's spans.
            // let span = tracing::span!(parent: None, Level::ERROR, "abci", ?addr);

            let server = self.clone();
            let listener_clone = listener.clone();
            tokio::spawn(async move {
                let s = server.clone();
                backoff::future::retry::<_, BoxError, _, _, _>(
                    ExponentialBackoff::default(),
                    || async {
                        match listener_clone.accept().await {
                            Ok((socket, _addr)) => {
                                let conn = Connection {
                                    consensus: s.consensus.clone(),
                                    mempool: s.mempool.clone(),
                                    info: s.info.clone(),
                                    snapshot: s.snapshot.clone(),
                                };

                                if let Err(e) = conn.run(socket).await {
                                    match e.downcast::<tower::load_shed::error::Overloaded>() {
                                        Err(e) => {
                                            tracing::error!({ %e }, "error in a connection handler");
                                            return Err(backoff::Error::Permanent(e));
                                        }
                                        Ok(e) => {
                                            tracing::warn!("Service overloaded - backing off");
                                            return Err(backoff::Error::transient(e));
                                        }
                                    }
                                }
                                Ok(())
                            }
                            Err(e) => {
                                tracing::error!({ %e }, "error accepting new tcp connection");
                                Ok(())
                            }
                        }
                    },
                )
                .await
            });
            // .instrument(span);
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
    C: Service<ConsensusRequest, Response = ConsensusResponse, Error = BoxError> + Send + 'static,
    C::Future: Send + 'static,
    M: Service<MempoolRequest, Response = MempoolResponse, Error = BoxError> + Send + 'static,
    M::Future: Send + 'static,
    I: Service<InfoRequest, Response = InfoResponse, Error = BoxError> + Send + 'static,
    I::Future: Send + 'static,
    S: Service<SnapshotRequest, Response = SnapshotResponse, Error = BoxError> + Send + 'static,
    S::Future: Send + 'static,
{
    // XXX handle errors gracefully
    // figure out how / if to return errors to tendermint
    async fn run(mut self, mut socket: TcpStream) -> Result<(), BoxError> {
        tracing::info!("listening for requests");

        use tendermint_proto::abci as pb;

        let (mut request_stream, mut response_sink) = {
            use crate::codec::{Decode, Encode};
            let (read, write) = socket.split();
            (
                FramedRead::new(read, Decode::<pb::Request>::default()),
                FramedWrite::new(write, Encode::<pb::Response>::default()),
            )
        };

        let mut responses = FuturesOrdered::new();

        loop {
            select! {
                req = request_stream.next() => {
                    let proto = match req.transpose()? {
                        Some(proto) => proto,
                        None => return Ok(()),
                    };
                    let request = Request::try_from(proto)?;
                    tracing::debug!(?request, "new request");
                    match request.kind() {
                        MethodKind::Consensus => {
                            let request = request.try_into().expect("checked kind");
                            match self.consensus.ready().await {
                                Ok(consensus) => {
                                    let response = consensus.call(request);
                                    // Need to box here for type erasure
                                    responses.push_back(response.map_ok(Response::from).boxed());
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
                                    responses.push_back(response.map_ok(Response::from).boxed());
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
                                    responses.push_back(response.map_ok(Response::from).boxed());
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
                                    responses.push_back(response.map_ok(Response::from).boxed());
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
                            while let Some(response) = responses.next().await {
                                // XXX: sometimes we might want to send errors to tendermint
                                // https://docs.tendermint.com/v0.32/spec/abci/abci.html#errors
                                tracing::debug!(?response, "flushing response");
                                response_sink.send(response?.into()).await?;
                            }
                            // Now we need to tell Tendermint we've flushed responses
                            response_sink.send(Response::Flush(Default::default()).into()).await?;
                        }
                    }
                }
                rsp = responses.next(), if !responses.is_empty() => {
                    let response = rsp.expect("didn't poll when responses was empty");
                    // XXX: sometimes we might want to send errors to tendermint
                    // https://docs.tendermint.com/v0.32/spec/abci/abci.html#errors
                    tracing::debug!(?response, "sending response");
                    response_sink.send(response?.into()).await?;
                }
            }
        }
    }
}
