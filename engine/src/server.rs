use std::sync::Arc;

use futures::future::try_join_all;
use tonic::transport::Server;

use protobuf::matching_engine_rpc_server::MatchingEngineRpcServer;
use raft::config::RaftConfig;
use raft::shutdown::ShutdownSignal;
use rpc::rpc_server::MatchingEngine;

mod algos;
mod protobuf;
mod rpc;

const N_REPLICAS: u64 = 5;

/// Starts [N_REPLICAS] Matching engine replicas, each running a Raft node underneath.
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let _shutdown_signal = Arc::new(ShutdownSignal::new());
    let config = RaftConfig::new(N_REPLICAS);

    let mut tasks = vec![];
    for id in 0..N_REPLICAS {
        let config = config.clone();
        let handle = tokio::spawn(async move {
            let engine = MatchingEngine::new(id, config.clone());
            let engine = Arc::new(engine);

            let ee = engine.clone();
            let raft_thread = tokio::spawn(async move {
                ee.run().await;
            });

            let server = MatchingEngineRpcServer::new(engine);

            let mut addr = config.addresses.get(id as usize).unwrap().clone();
            addr.set_port(addr.port() + 10);

            println!("Starting rpc server at {:?}", &addr);
            let engine_thread = tokio::spawn(async move {
                Server::builder().add_service(server).serve(addr).await;
            });
            tokio::join!(raft_thread, engine_thread);
        });
        tasks.push(handle);
    }
    let _ = try_join_all(tasks).await;
}
