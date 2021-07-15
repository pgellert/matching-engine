extern crate raft;

use std::sync::Arc;

use futures::future::try_join_all;
use itertools::Itertools;
use tokio::sync::RwLock;
use tokio::task::spawn;
use tokio::time::{delay_for, Duration};
use tracing::trace;

use raft::raft::{Raft, RaftNode};
use raft::shutdown::ShutdownSignal;

use super::common::*;

use self::raft::config::RaftConfig;

#[tokio::test(threaded_scheduler)]
async fn test_vote_converges_after_5_seconds() -> Result<()> {
    tracing_subscriber::fmt::init();

    let nodes = simulate_raft(5).await?;

    trace!("Simulation finished, checking end state assertions...");

    check_expected_state(nodes).await;

    Ok(())
}

async fn check_expected_state(nodes: Vec<RaftNode>) {
    let mut terms = vec![];
    let mut leaders = vec![];
    for node in nodes.into_iter() {
        let raft = node.raft.read().await;
        terms.push(raft.current_term);
        leaders.push(raft.voted_for);

        // Log consistency check
        println!("Logs: {:?}", raft.log);
        for (i, entry) in raft.log.iter().enumerate() {
            assert_eq!(i, entry.index as usize);
        }
    }
    println!("Terms: {:?}", &terms);
    assert!(terms.iter().all_equal());
    assert!(*terms.first().unwrap() > 0);
    println!("Leaders: {:?}", &leaders);
    println!("Leader: {:?}", leaders.first().unwrap());
    assert!(leaders.first().is_some());
    assert!(leaders.iter().all_equal());
}

async fn simulate_raft(simulation_length: u64) -> Result<Vec<RaftNode>> {
    let shutdown_signal = Arc::new(ShutdownSignal::new());

    let num_replicas = 3;
    let config = RaftConfig::new(num_replicas);
    let nodes: Vec<RaftNode> = (0..num_replicas)
        .into_iter()
        .map(|id| {
            let raft = Raft::new(id as u64, config.clone(), PrinterStateMachine::new());
            let shared_raft = Arc::new(RwLock::new(raft));
            RaftNode::new_with_shutdown(shared_raft, shutdown_signal.clone())
        })
        .collect();

    trace!("Nodes are ready. Running them now");
    let mut threads = Vec::new();
    for node in nodes.iter() {
        let node = node.clone();
        let handle = spawn(async move {
            node.run().await.unwrap();
        });

        threads.push(handle);
    }

    let signal = shutdown_signal.clone();
    threads.push(spawn(async move {
        delay_for(Duration::from_secs(simulation_length)).await;
        trace!("Sending shutdown signal");
        signal.shutdown();
        trace!("Shutdown signal sent");
    }));

    let _ = try_join_all(threads).await?;
    Ok(nodes)
}
