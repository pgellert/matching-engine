use std::sync::Arc;

use tokio::sync::RwLock;
use tonic::{Code, Request, Response, Status};
use tracing::info;

use crate::protobuf::raft_rpc_server::RaftRpc;
use crate::protobuf::*;
use crate::raft::Raft;

/// gRPC server implementation to reply to Raft's RPC requests
pub struct RaftRPCServer {
    pub raft: Arc<RwLock<Raft>>,
}

#[tonic::async_trait]
impl RaftRpc for RaftRPCServer {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn request_vote(
        &self,
        request: Request<VoteRequest>,
    ) -> Result<Response<VoteResponse>, Status> {
        let request = request.get_ref();
        let mut raft = self.raft.write().await;
        let response = raft.handle_request_vote(request).await;
        info!("VoteResponse: {:?}", response);
        match response {
            Ok(result) => Ok(Response::new(result)),
            Err(err) => Err(Status::new(
                Code::Internal,
                format!("handle_request_vote failed with {:}", err.to_string()),
            )),
        }
    }

    #[tracing::instrument(skip(self, request), level = "debug")]
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let request = request.get_ref();
        let mut raft = self.raft.write().await;
        info!(
            "AppendEntriesRequest term ({:}), log indices ({:}-{:})",
            request.term,
            request.prev_log_index + 1,
            request.prev_log_index + 1 + request.entries.len() as u64
        );
        let response = raft.handle_append_entries(request).await;
        info!("AppendEntriesResponse: {:?}", &response);
        match response {
            Ok(result) => Ok(Response::new(result)),
            Err(err) => Err(Status::new(
                Code::Internal,
                format!("handle_append_entries failed with {:?}", err.to_string()),
            )),
        }
    }
}
