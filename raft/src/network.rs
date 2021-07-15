use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::{Mutex, MutexGuard};

use crate::protobuf::raft_rpc_client::RaftRpcClient;
use crate::protobuf::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
use crate::raft::NodeId;
use crate::raft::Result;

pub type ClientEnd = RaftRpcClient<tonic::transport::Channel>;

/// Maintains a connection to another Raft node (peer)
#[derive(Debug, Clone)]
pub struct Peer {
    pub(crate) addr: SocketAddr,
    connection: Arc<Mutex<Option<ClientEnd>>>,
}

impl Peer {
    /// Creates a new peer connection-holder without initiating the connection
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            connection: Arc::new(Mutex::new(None)),
        }
    }

    /// Returns the internally stored gRPC connection, reloading it if the connection failed
    pub async fn load_connection(&self) -> Result<MutexGuard<'_, Option<ClientEnd>>> {
        let mut connection = self.connection.lock().await;

        if connection.is_none() {
            let addr = format!("http://{:}", self.addr);
            let conn = RaftRpcClient::connect(addr).await?;
            *connection = Some(conn);
        }

        Ok(connection)
    }

    /// Serialised and sends an [AppendEntriesRequest] RPC call to the peer and returns the result
    pub async fn send_append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse> {
        let mut conn = self.load_connection().await?;
        let conn = conn.as_mut().unwrap();
        let request = tonic::Request::new(request);
        let result = conn.append_entries(request).await?;
        Ok(result.into_inner())
    }

    /// Serialised and sends an [VoteRequest] RPC call to the peer and returns the result
    pub async fn send_request_vote(&self, request: VoteRequest) -> Result<VoteResponse> {
        let mut conn = self.load_connection().await?;
        let conn = conn.as_mut().unwrap();
        let request = tonic::Request::new(request);
        let result = conn.request_vote(request).await?;
        Ok(result.into_inner())
    }
}

/// Container for a collection of Raft peers
#[derive(Debug, Clone)]
pub struct RaftNetwork {
    pub peers: HashMap<NodeId, Arc<Peer>>,
}

impl RaftNetwork {
    pub fn new(peers: HashMap<NodeId, Arc<Peer>>) -> Self {
        Self { peers }
    }

    pub fn from_addresses(addrs: &Vec<SocketAddr>) -> Self {
        let peers: HashMap<NodeId, Arc<Peer>> = addrs
            .iter()
            .map(|&addr| Arc::new(Peer::new(addr)))
            .enumerate()
            .map(|(id, p)| (id as NodeId, p))
            .collect();
        Self { peers }
    }
}
