use std::convert::TryFrom;

use tendermint_proto::v0_37::abci as pb;

use crate::MethodKind;

#[doc(inline)]
pub use pb::RequestApplySnapshotChunk as ApplySnapshotChunk;
#[doc(inline)]
pub use pb::RequestCheckTx as CheckTx;
#[doc(inline)]
pub use pb::RequestCommit as Commit;
#[doc(inline)]
pub use pb::RequestEcho as Echo;
#[doc(inline)]
pub use pb::RequestFlush as Flush;
#[doc(inline)]
pub use pb::RequestInfo as Info;
#[doc(inline)]
pub use pb::RequestInitChain as InitChain;
#[doc(inline)]
pub use pb::RequestListSnapshots as ListSnapshots;
#[doc(inline)]
pub use pb::RequestLoadSnapshotChunk as LoadSnapshotChunk;
#[doc(inline)]
pub use pb::RequestOfferSnapshot as OfferSnapshot;
#[doc(inline)]
pub use pb::RequestQuery as Query;
#[doc(inline)]
pub use pb::RequestPrepareProposal as PrepareProposal;
#[doc(inline)]
pub use pb::RequestProcessProposal as ProcessProposal;
#[doc(inline)]
pub use pb::RequestBeginBlock as BeginBlock;
#[doc(inline)]
pub use pb::RequestDeliverTx as DeliverTx;
#[doc(inline)]
pub use pb::RequestEndBlock as EndBlock;


/// An ABCI request.
#[derive(Clone, PartialEq, Debug)]
pub enum Request {
    Echo(Echo),
    Flush(Flush),
    Info(Info),
    InitChain(InitChain),
    Query(Query),
    CheckTx(CheckTx),
    Commit(Commit),
    ListSnapshots(ListSnapshots),
    OfferSnapshot(OfferSnapshot),
    LoadSnapshotChunk(LoadSnapshotChunk),
    ApplySnapshotChunk(ApplySnapshotChunk),
    PrepareProposal(PrepareProposal),
    ProcessProposal(ProcessProposal),
    BeginBlock(BeginBlock),
    DeliverTx(DeliverTx),
    EndBlock(EndBlock),
}

impl Request {
    /// Get the method kind for this request.
    pub fn kind(&self) -> MethodKind {
        use Request::*;
        match self {
            Flush(_) => MethodKind::Flush,
            InitChain(_) => MethodKind::Consensus,
            Commit(_) => MethodKind::Consensus,
            PrepareProposal(_) => MethodKind::Consensus,
            ProcessProposal(_) => MethodKind::Consensus,
            BeginBlock(_) => MethodKind::Consensus,
            DeliverTx(_) => MethodKind::Consensus,
            EndBlock(_) => MethodKind::Consensus,
            CheckTx(_) => MethodKind::Mempool,
            ListSnapshots(_) => MethodKind::Snapshot,
            OfferSnapshot(_) => MethodKind::Snapshot,
            LoadSnapshotChunk(_) => MethodKind::Snapshot,
            ApplySnapshotChunk(_) => MethodKind::Snapshot,
            Info(_) => MethodKind::Info,
            Query(_) => MethodKind::Info,
            Echo(_) => MethodKind::Info,
        }
    }
}

impl TryFrom<pb::Request> for Request {
    type Error = &'static str;

    fn try_from(proto: pb::Request) -> Result<Self, Self::Error> {
        use pb::request::Value;
        match proto.value {
            Some(Value::Echo(x)) => Ok(Request::Echo(x)),
            Some(Value::Flush(x)) => Ok(Request::Flush(x)),
            Some(Value::Info(x)) => Ok(Request::Info(x)),
            Some(Value::InitChain(x)) => Ok(Request::InitChain(x)),
            Some(Value::Query(x)) => Ok(Request::Query(x)),
            Some(Value::CheckTx(x)) => Ok(Request::CheckTx(x)),
            Some(Value::Commit(x)) => Ok(Request::Commit(x)),
            Some(Value::ListSnapshots(x)) => Ok(Request::ListSnapshots(x)),
            Some(Value::OfferSnapshot(x)) => Ok(Request::OfferSnapshot(x)),
            Some(Value::LoadSnapshotChunk(x)) => Ok(Request::LoadSnapshotChunk(x)),
            Some(Value::ApplySnapshotChunk(x)) => Ok(Request::ApplySnapshotChunk(x)),
            Some(Value::PrepareProposal(x)) => Ok(Request::PrepareProposal(x)),
            Some(Value::ProcessProposal(x)) => Ok(Request::ProcessProposal(x)),
            Some(Value::BeginBlock(x)) => Ok(Request::BeginBlock(x)),
            Some(Value::DeliverTx(x)) => Ok(Request::DeliverTx(x)),
            Some(Value::EndBlock(x)) => Ok(Request::EndBlock(x)),
            None => Err("no request in proto"),
        }
    }
}

impl Into<pb::Request> for Request {
    fn into(self) -> pb::Request {
        use pb::request::Value;
        let value = match self {
            Request::Echo(x) => Some(Value::Echo(x)),
            Request::Flush(x) => Some(Value::Flush(x)),
            Request::Info(x) => Some(Value::Info(x)),
            Request::InitChain(x) => Some(Value::InitChain(x)),
            Request::Query(x) => Some(Value::Query(x)),
            Request::CheckTx(x) => Some(Value::CheckTx(x)),
            Request::Commit(x) => Some(Value::Commit(x)),
            Request::ListSnapshots(x) => Some(Value::ListSnapshots(x)),
            Request::OfferSnapshot(x) => Some(Value::OfferSnapshot(x)),
            Request::LoadSnapshotChunk(x) => Some(Value::LoadSnapshotChunk(x)),
            Request::ApplySnapshotChunk(x) => Some(Value::ApplySnapshotChunk(x)),
            Request::PrepareProposal(x) => Some(Value::PrepareProposal(x)),
            Request::ProcessProposal(x) => Some(Value::ProcessProposal(x)),
            Request::BeginBlock(x) => Some(Value::BeginBlock(x)),
            Request::DeliverTx(x) => Some(Value::DeliverTx(x)),
            Request::EndBlock(x) => Some(Value::EndBlock(x)),
        };
        pb::Request { value }
    }
}

/// An ABCI request sent over the consensus connection.
#[derive(Clone, PartialEq, Debug)]
pub enum ConsensusRequest {
    InitChain(InitChain),
    Commit(Commit),
    PrepareProposal(PrepareProposal),
    ProcessProposal(ProcessProposal),
    BeginBlock(BeginBlock),
    DeliverTx(DeliverTx),
    EndBlock(EndBlock),
}

impl From<ConsensusRequest> for Request {
    fn from(req: ConsensusRequest) -> Self {
        match req {
            ConsensusRequest::InitChain(x) => Self::InitChain(x),
            ConsensusRequest::Commit(x) => Self::Commit(x),
            ConsensusRequest::PrepareProposal(x) => Self::PrepareProposal(x),
            ConsensusRequest::ProcessProposal(x) => Self::ProcessProposal(x),
            ConsensusRequest::BeginBlock(x) => Self::BeginBlock(x),
            ConsensusRequest::DeliverTx(x) => Self::DeliverTx(x),
            ConsensusRequest::EndBlock(x) => Self::EndBlock(x),
        }
    }
}

impl TryFrom<Request> for ConsensusRequest {
    type Error = &'static str;
    fn try_from(req: Request) -> Result<Self, Self::Error> {
        match req {
            Request::InitChain(x) => Ok(Self::InitChain(x)),
            Request::Commit(x) => Ok(Self::Commit(x)),
            Request::PrepareProposal(x) => Ok(Self::PrepareProposal(x)),
            Request::ProcessProposal(x) => Ok(Self::ProcessProposal(x)),
            Request::BeginBlock(x) => Ok(Self::BeginBlock(x)),
            Request::DeliverTx(x) => Ok(Self::DeliverTx(x)),
            Request::EndBlock(x) => Ok(Self::EndBlock(x)),
            _ => Err("wrong request type"),
        }
    }
}

/// An ABCI request sent over the mempool connection.
#[derive(Clone, PartialEq, Debug)]
pub enum MempoolRequest {
    CheckTx(CheckTx),
}

impl From<MempoolRequest> for Request {
    fn from(req: MempoolRequest) -> Self {
        match req {
            MempoolRequest::CheckTx(x) => Self::CheckTx(x),
        }
    }
}

impl TryFrom<Request> for MempoolRequest {
    type Error = &'static str;
    fn try_from(req: Request) -> Result<Self, Self::Error> {
        match req {
            Request::CheckTx(x) => Ok(Self::CheckTx(x)),
            _ => Err("wrong request type"),
        }
    }
}

/// An ABCI request sent over the info connection.
#[derive(Clone, PartialEq, Debug)]
pub enum InfoRequest {
    Info(Info),
    Query(Query),
    Echo(Echo),
}

impl From<InfoRequest> for Request {
    fn from(req: InfoRequest) -> Self {
        match req {
            InfoRequest::Info(x) => Self::Info(x),
            InfoRequest::Query(x) => Self::Query(x),
            InfoRequest::Echo(x) => Self::Echo(x),
        }
    }
}

impl TryFrom<Request> for InfoRequest {
    type Error = &'static str;
    fn try_from(req: Request) -> Result<Self, Self::Error> {
        match req {
            Request::Info(x) => Ok(Self::Info(x)),
            Request::Query(x) => Ok(Self::Query(x)),
            Request::Echo(x) => Ok(Self::Echo(x)),
            _ => Err("wrong request type"),
        }
    }
}

/// An ABCI request sent over the snapshot connection.
#[derive(Clone, PartialEq, Debug)]
pub enum SnapshotRequest {
    ListSnapshots(ListSnapshots),
    OfferSnapshot(OfferSnapshot),
    LoadSnapshotChunk(LoadSnapshotChunk),
    ApplySnapshotChunk(ApplySnapshotChunk),
}

impl From<SnapshotRequest> for Request {
    fn from(req: SnapshotRequest) -> Self {
        match req {
            SnapshotRequest::ListSnapshots(x) => Self::ListSnapshots(x),
            SnapshotRequest::OfferSnapshot(x) => Self::OfferSnapshot(x),
            SnapshotRequest::LoadSnapshotChunk(x) => Self::LoadSnapshotChunk(x),
            SnapshotRequest::ApplySnapshotChunk(x) => Self::ApplySnapshotChunk(x),
        }
    }
}

impl TryFrom<Request> for SnapshotRequest {
    type Error = &'static str;
    fn try_from(req: Request) -> Result<Self, Self::Error> {
        match req {
            Request::ListSnapshots(x) => Ok(Self::ListSnapshots(x)),
            Request::OfferSnapshot(x) => Ok(Self::OfferSnapshot(x)),
            Request::LoadSnapshotChunk(x) => Ok(Self::LoadSnapshotChunk(x)),
            Request::ApplySnapshotChunk(x) => Ok(Self::ApplySnapshotChunk(x)),
            _ => Err("wrong request type"),
        }
    }
}
