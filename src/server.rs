use std::convert::{TryFrom, TryInto};
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use backoff::ExponentialBackoff;
use futures::future::{FutureExt, TryFutureExt};
use futures::sink::SinkExt;
use futures::stream::{FuturesOrdered, Peekable, StreamExt};
use futures::Future;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::sync::{Mutex, MutexGuard};
use tokio::{
    net::{TcpListener, TcpStream, ToSocketAddrs},
    select,
};
use tokio_util::codec::{FramedRead, FramedWrite};
use tower::{Service, ServiceExt};
use tracing::Instrument;
use tracing::Level;

use crate::{
    response, BoxError, ConsensusRequest, ConsensusResponse, InfoRequest, InfoResponse,
    MempoolRequest, MempoolResponse, MethodKind, Request, Response, SnapshotRequest,
    SnapshotResponse,
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

        // set parent: None for the connection span, as it should
        // exist independently of the listener's spans.
        // let span = tracing::span!(parent: None, Level::ERROR, "abci", ?addr);

        let listener_clone = listener.clone();

        // let socket = Arc::new(socket);
        // let conn_clone = conn.clone();
        // TODO: loop only if there's no error
        let mut i = 0;
        loop {
            println!("am looping {i}");
            match listener.accept().await {
                Ok((socket, _addr)) => {
                    println!(
                        "Accepting inner, socket: {socket:?}, linger: {:?}",
                        socket.linger()
                    );
                    // socket.set_linger(Some(Duration::new(100, 0))).unwrap();
                    let conn = Connection {
                        consensus: self.consensus.clone(),
                        mempool: self.mempool.clone(),
                        info: self.info.clone(),
                        snapshot: self.snapshot.clone(),
                    };

                    // let conn_clone = conn_clone.clone();
                    tokio::spawn(async move {
                        conn.run_with_backoff(socket, i).await.unwrap();
                        println!("Task {i} done");
                        // println!("am looping");
                        // backoff::future::retry::<_, BoxError, _, _, _>(
                        //     ExponentialBackoff::default(),
                        //     || async {
                        //         if let Err(e) = conn_clone.clone().run(socket).await {
                        //             match e.downcast::<tower::load_shed::error::Overloaded>() {
                        //                 Err(e) => {
                        //                     println!("error {e} in a connection handler");
                        //                     return Err(backoff::Error::Permanent(e));
                        //                 }
                        //                 Ok(e) => {
                        //                     println!("Overloaded - backing off");
                        //                     return Err(backoff::Error::transient(e));
                        //                 }
                        //             }
                        //         } else {
                        //             println!("Conn ran without error");
                        //             Ok(())
                        //         }
                        //     },
                        // )
                        // .await
                        // .unwrap();

                        // println!("Task done");
                    });
                    println!("Task spawned");
                }

                Err(e) => {
                    println!("error {e} accepting new tcp connection (inner)");
                    // return Err(backoff::Error::Permanent(e));
                }
            }
            i = i + 1;
        }
        // .instrument(span);
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
    async fn run_with_backoff(self, mut socket: TcpStream, i: u64) -> Result<(), BoxError> {
        let mut pinned_socket = Pin::new(Box::new(socket));
        // TODO try to keep the stream and sink open instead
        // let s = Arc::new(Mutex::new(socket));
        let (read, write) = pinned_socket.split();
        // let (mut request_stream, mut response_sink) = {
        let rw = {
            use crate::codec::{Decode, Encode};
            use tendermint_proto::abci as pb;
            let mut responses = FuturesOrdered::new();
            Arc::new(Mutex::new((
                FramedRead::new(read, Decode::<pb::Request>::default()).peekable(),
                FramedWrite::new(write, Encode::<pb::Response>::default()),
                responses,
            )))
        };

        backoff::future::retry::<_, BoxError, _, _, _>(ExponentialBackoff::default(), || async {
            println!("Trying to lock {i}");
            // let mut s_guard = s.lock().await;
            let mut rw_guard = rw.lock().await;
            println!("Locked {i}");
            // let (read, write) = s_guard.split();
            // let (mut request_stream, mut response_sink) = {
            //     use crate::codec::{Decode, Encode};
            //     use tendermint_proto::abci as pb;
            //     (
            //         FramedRead::new(read, Decode::<pb::Request>::default()),
            //         FramedWrite::new(write, Encode::<pb::Response>::default()),
            //     )
            // };

            println!("Trying to run {i}");
            let run_result = self
                .clone()
                // .run(&mut request_stream, &mut response_sink, i)
                .run(&mut rw_guard, i)
                .await;

            if let Err(e) = run_result {
                println!("some error in a connection handler");
                match e.downcast::<tower::load_shed::error::Overloaded>() {
                    Err(e) => {
                        println!("error {e} in a connection handler");
                        return Err(backoff::Error::Permanent(e));
                    }
                    Ok(e) => {
                        println!(
                            "Overloaded on {i} - backing off after trying to send reponses, if any"
                        );

                        let (_, response_sink, responses) = rw_guard.deref_mut();
                        if !responses.is_empty() {
                            println!("Sending reponses first");
                            while let Some(rsp) = responses.next().await {
                                let response = rsp.expect("didn't poll when responses was empty");
                                // XXX: sometimes we might want to send errors to tendermint
                                // https://docs.tendermint.com/v0.32/spec/abci/abci.html#errors
                                tracing::debug!(?response, "sending response");
                                println!("sending response");
                                response_sink.send(response.into()).await?;
                            }
                        }

                        return Err(backoff::Error::transient(e));
                    }
                }
            } else {
                println!("Conn {i} ran without error");
                Ok(())
            }
        })
        .await?;
        println!("Backoff {i} finished");
        // self.run_with_backoff(Arc::into_inner(s).unwrap().into_inner(), i)
        //     .await
        Ok(())
    }
    // XXX handle errors gracefully
    // figure out how / if to return errors to tendermint
    async fn run(
        mut self,
        rw_guard: &mut MutexGuard<
            '_,
            (
                Peekable<
                    FramedRead<ReadHalf<'_>, crate::codec::Decode<tendermint_proto::abci::Request>>,
                >,
                FramedWrite<WriteHalf<'_>, crate::codec::Encode<tendermint_proto::abci::Response>>,
                FuturesOrdered<Pin<Box<dyn Future<Output = Result<Response, BoxError>> + Send>>>,
            ),
        >,
        // request_stream: &mut FramedRead<
        //     ReadHalf<'_>,
        //     crate::codec::Decode<tendermint_proto::abci::Request>,
        // >,
        // response_sink: &mut FramedWrite<
        //     WriteHalf<'_>,
        //     crate::codec::Encode<tendermint_proto::abci::Response>,
        // >,
        i: u64,
    ) -> Result<(), BoxError> {
        tracing::info!("listening for requests");

        use tendermint_proto::abci as pb;

        let (request_stream, response_sink, responses) = rw_guard.deref_mut();
        let mut pinned_stream = Pin::new(request_stream);

        let mut peeked = false;
        loop {
            println!("Run loop, i {i}");
            select! {
                req = pinned_stream.as_mut().peek(), if !peeked => {
                    peeked = true;
                    let proto = match req {
                        Some(Ok(proto)) => proto.clone(),
                        Some(Err(err)) => {panic!("TODO:Request err")}
                        None => {
                            println!("HUH {i}");
                            return Ok(());
                        },
                    };
                    // let proto = match req.transpose()? {
                    //     Some(proto) => proto,
                    //     None => {
                    //         println!("HUH {i}");
                    //         return Ok(());
                    //     },
                    // };
                    let request = Request::try_from(proto)?;
                    tracing::debug!(?request, "new request");
                    println!("new request on {i}, kind {:?}", request.kind());
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
                                    println!("consensus service is not ready: {}", err);
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
                                    println!("mempool service is not ready: {}", err);
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
                                    println!("snapshot service is not ready: {}", err);
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
                                    println!("info service is not ready: {}", err);
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
                                println!("flushing response");
                                response_sink.send(response?.into()).await?;
                            }
                            peeked = false;
                            pinned_stream.next().await.unwrap().unwrap();
                            println!("send flush done");
                            // Now we need to tell Tendermint we've flushed responses
                            response_sink.send(Response::Flush(Default::default()).into()).await?;
                        }
                    }
                }
                rsp = responses.next(), if !responses.is_empty() => {
                    peeked = false;
                    let response = rsp.expect("didn't poll when responses was empty")?;
                    pinned_stream.next().await.unwrap().unwrap();
                    // XXX: sometimes we might want to send errors to tendermint
                    // https://docs.tendermint.com/v0.32/spec/abci/abci.html#errors
                    tracing::debug!(?response, "sending response");
                    println!("sending response");
                    response_sink.send(response.into()).await?;
                }
            }
        }
    }
}
