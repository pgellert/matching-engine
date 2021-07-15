extern crate raft;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::join;
use tokio::sync::{Mutex, RwLock};
use tokio::time::delay_for;
use tonic::transport::Server;

use raft::config::RaftConfig;
use raft::network::RaftNetwork;
use raft::protobuf::raft_rpc_server::RaftRpcServer;
use raft::protobuf::Command;
use raft::raft::Raft;
use raft::rpc_server::RaftRPCServer;
use raft::state_machine::StateMachine;
use raft::storage::RaftStorage;

#[derive(Debug)]
struct PrinterStateMachine;

impl PrinterStateMachine {
    fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl StateMachine for PrinterStateMachine {
    async fn apply_command(&self, command: &Command, _: bool) -> () {
        println!("Applying command {:}", command.sequence_id);
    }
}

#[tokio::test]
async fn test_tonic_is_shut_down_after_signal() -> Result<(), Box<dyn std::error::Error>> {
    let signal = AtomicBool::new(false);
    let config = RaftConfig::new(3);
    let id = 0u64;
    let addr = config.addresses.get(id as usize).unwrap().clone();
    let raft = Raft::new(id, config, PrinterStateMachine::new());
    let raft = Arc::new(RwLock::new(raft));
    let server = RaftRpcServer::new(RaftRPCServer { raft: raft.clone() });

    let shutdown_ft = async {
        delay_for(Duration::from_secs(1)).await;
        signal.swap(true, Ordering::Release);
        println!("Sending shutdown signal");
    };

    let server_ft = async {
        println!("Server starting");
        Server::builder()
            .add_service(server)
            .serve_with_shutdown(addr, shutdown_ft)
            .await
    };

    let _ = join!(server_ft);
    assert!(true); // Asserts we get there
    Ok(())
}
