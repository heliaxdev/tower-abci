use std::convert::TryFrom;

use tendermint_proto::abci as pb;

#[doc(inline)]
pub use pb::ResponseApplySnapshotChunk as ApplySnapshotChunk;
#[doc(inline)]
pub use pb::ResponseCheckTx as CheckTx;
#[doc(inline)]
pub use pb::ResponseCommit as Commit;
#[doc(inline)]
pub use pb::ResponseEcho as Echo;
#[doc(inline)]
pub use pb::ResponseException as Exception;
#[doc(inline)]
pub use pb::ResponseFlush as Flush;
#[doc(inline)]
pub use pb::ResponseInfo as Info;
#[doc(inline)]
pub use pb::ResponseInitChain as InitChain;
#[doc(inline)]
pub use pb::ResponseListSnapshots as ListSnapshots;
#[doc(inline)]
pub use pb::ResponseLoadSnapshotChunk as LoadSnapshotChunk;
#[doc(inline)]
pub use pb::ResponseOfferSnapshot as OfferSnapshot;
#[doc(inline)]
pub use pb::ResponseQuery as Query;
#[doc(inline)]
pub use pb::ResponsePrepareProposal as PrepareProposal;
#[doc(inline)]
pub use pb::ResponseProcessProposal as ProcessProposal;
#[doc(inline)]
pub use pb::ResponseExtendVote as ExtendVote;
#[doc(inline)]
pub use pb::ResponseVerifyVoteExtension as VerifyVoteExtension;
#[doc(inline)]
pub use pb::ResponseFinalizeBlock as FinalizeBlock;

/// An ABCI response.
#[derive(Debug, Clone)]
pub enum Response {
    Exception(Exception),
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
    ExtendVote(ExtendVote),
    VerifyVoteExtension(VerifyVoteExtension),
    FinalizeBlock(FinalizeBlock),
}

impl Into<pb::Response> for Response {
    fn into(self) -> pb::Response {
        use pb::response::Value;
        let value = Some(match self {
            Response::Exception(x) => Value::Exception(x),
            Response::Echo(x) => Value::Echo(x),
            Response::Flush(x) => Value::Flush(x),
            Response::Info(x) => Value::Info(x),
            Response::InitChain(x) => Value::InitChain(x),
            Response::Query(x) => Value::Query(x),
            Response::CheckTx(x) => Value::CheckTx(x),
            Response::Commit(x) => Value::Commit(x),
            Response::ListSnapshots(x) => Value::ListSnapshots(x),
            Response::OfferSnapshot(x) => Value::OfferSnapshot(x),
            Response::LoadSnapshotChunk(x) => Value::LoadSnapshotChunk(x),
            Response::ApplySnapshotChunk(x) => Value::ApplySnapshotChunk(x),
            Response::PrepareProposal(x) => Value::PrepareProposal(x),
            Response::ProcessProposal(x) => Value::ProcessProposal(x),
            Response::ExtendVote(x) => Value::ExtendVote(x),
            Response::VerifyVoteExtension(x) => Value::VerifyVoteExtension(x),
            Response::FinalizeBlock(x) => Value::FinalizeBlock(x),
        });
        pb::Response { value }
    }
}

/// An ABCI response sent over the consensus connection.
#[derive(Clone, PartialEq, Debug)]
pub enum ConsensusResponse {
    InitChain(InitChain),
    Commit(Commit),
    PrepareProposal(PrepareProposal),
    ProcessProposal(ProcessProposal),
    ExtendVote(ExtendVote),
    VerifyVoteExtension(VerifyVoteExtension),
    FinalizeBlock(FinalizeBlock),
}

impl From<ConsensusResponse> for Response {
    fn from(req: ConsensusResponse) -> Self {
        match req {
            ConsensusResponse::InitChain(x) => Self::InitChain(x),
            ConsensusResponse::Commit(x) => Self::Commit(x),
            ConsensusResponse::PrepareProposal(x) => Self::PrepareProposal(x),
            ConsensusResponse::ProcessProposal(x) => Self::ProcessProposal(x),
            ConsensusResponse::ExtendVote(x) => Self::ExtendVote(x),
            ConsensusResponse::VerifyVoteExtension(x) => Self::VerifyVoteExtension(x),
            ConsensusResponse::FinalizeBlock(x) => Self::FinalizeBlock(x),
        }
    }
}

impl TryFrom<Response> for ConsensusResponse {
    type Error = &'static str;
    fn try_from(req: Response) -> Result<Self, Self::Error> {
        match req {
            Response::InitChain(x) => Ok(Self::InitChain(x)),
            Response::PrepareProposal(x) => Ok(Self::PrepareProposal(x)),
            Response::ProcessProposal(x) => Ok(Self::ProcessProposal(x)),
            Response::Commit(x) => Ok(Self::Commit(x)),
            Response::ExtendVote(x) => Ok(Self::ExtendVote(x)),
            Response::VerifyVoteExtension(x) => Ok(Self::VerifyVoteExtension(x)),
            Response::FinalizeBlock(x) => Ok(Self::FinalizeBlock(x)),
            _ => Err("wrong request type"),
        }
    }
}

/// An ABCI response sent over the mempool connection.
#[derive(Clone, PartialEq, Debug)]
pub enum MempoolResponse {
    CheckTx(CheckTx),
}

impl From<MempoolResponse> for Response {
    fn from(req: MempoolResponse) -> Self {
        match req {
            MempoolResponse::CheckTx(x) => Self::CheckTx(x),
        }
    }
}

impl TryFrom<Response> for MempoolResponse {
    type Error = &'static str;
    fn try_from(req: Response) -> Result<Self, Self::Error> {
        match req {
            Response::CheckTx(x) => Ok(Self::CheckTx(x)),
            _ => Err("wrong request type"),
        }
    }
}

/// An ABCI request sent over the info connection.
#[derive(Clone, PartialEq, Debug)]
pub enum InfoResponse {
    Info(Info),
    Query(Query),
}

impl From<InfoResponse> for Response {
    fn from(req: InfoResponse) -> Self {
        match req {
            InfoResponse::Info(x) => Self::Info(x),
            InfoResponse::Query(x) => Self::Query(x),
        }
    }
}

impl TryFrom<Response> for InfoResponse {
    type Error = &'static str;
    fn try_from(req: Response) -> Result<Self, Self::Error> {
        match req {
            Response::Info(x) => Ok(Self::Info(x)),
            Response::Query(x) => Ok(Self::Query(x)),
            _ => Err("wrong request type"),
        }
    }
}

/// An ABCI request sent over the snapshot connection.
#[derive(Clone, PartialEq, Debug)]
pub enum SnapshotResponse {
    ListSnapshots(ListSnapshots),
    OfferSnapshot(OfferSnapshot),
    LoadSnapshotChunk(LoadSnapshotChunk),
    ApplySnapshotChunk(ApplySnapshotChunk),
}

impl From<SnapshotResponse> for Response {
    fn from(req: SnapshotResponse) -> Self {
        match req {
            SnapshotResponse::ListSnapshots(x) => Self::ListSnapshots(x),
            SnapshotResponse::OfferSnapshot(x) => Self::OfferSnapshot(x),
            SnapshotResponse::LoadSnapshotChunk(x) => Self::LoadSnapshotChunk(x),
            SnapshotResponse::ApplySnapshotChunk(x) => Self::ApplySnapshotChunk(x),
        }
    }
}

impl TryFrom<Response> for SnapshotResponse {
    type Error = &'static str;
    fn try_from(req: Response) -> Result<Self, Self::Error> {
        match req {
            Response::ListSnapshots(x) => Ok(Self::ListSnapshots(x)),
            Response::OfferSnapshot(x) => Ok(Self::OfferSnapshot(x)),
            Response::LoadSnapshotChunk(x) => Ok(Self::LoadSnapshotChunk(x)),
            Response::ApplySnapshotChunk(x) => Ok(Self::ApplySnapshotChunk(x)),
            _ => Err("wrong request type"),
        }
    }
}
