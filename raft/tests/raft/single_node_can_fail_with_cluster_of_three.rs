extern crate raft;

use std::iter::repeat;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::try_join_all;
use itertools::Itertools;
use tokio::sync::{Mutex, RwLock};
use tokio::task::spawn;
use tokio::time::{delay_for, Duration};
use tracing::debug;

use raft::network::RaftNetwork;
use raft::protobuf::{Command, CommandType, OrderSide};
use raft::raft::{Raft, RaftNode};
use raft::shutdown::ShutdownSignal;
use raft::storage::RaftStorage;

use super::common::*;

use self::raft::config::RaftConfig;
use self::raft::state_machine::StateMachine;

#[tokio::test(threaded_scheduler)]
async fn test_single_node_can_fail_with_cluster_of_three() -> Result<()> {
    tracing_subscriber::fmt::init();

    let (nodes, num_applied) = simulate_raft(5, 8).await?;

    debug!("Simulation finished, checking end state assertions...");

    check_expected_state(&nodes, num_applied).await;

    Ok(())
}

async fn check_expected_state(nodes: &Vec<RaftNode>, expected_last_index: usize) {
    let mut terms = vec![];
    let mut leaders = vec![];
    let mut last_index = vec![];
    for node in nodes.into_iter() {
        let raft = node.raft.read().await;
        terms.push(raft.current_term);
        leaders.push(raft.voted_for);
        last_index.push(raft.last_log_index);

        // Log consistency check
        println!("Logs: {:?}", raft.log);
        for (i, entry) in raft.log.iter().enumerate() {
            assert_eq!(i, entry.index as usize);
        }
    }

    let mut killed_node_id = None;
    let mut min_last_index = std::u64::MAX;
    for (i, &last_index) in last_index.iter().enumerate() {
        if min_last_index > last_index {
            min_last_index = last_index;
            killed_node_id = Some(i);
        }
    }

    let killed_node_id = killed_node_id.unwrap();
    println!("Killed node was: {:}", killed_node_id);

    terms.remove(killed_node_id);
    leaders.remove(killed_node_id);
    last_index.remove(killed_node_id);

    println!("Terms: {:?}", &terms);
    assert!(terms.iter().all_equal());
    assert!(*terms.first().unwrap() > 0);
    println!("Leaders: {:?}", &leaders);
    println!("Leader: {:?}", leaders.first().unwrap());
    assert!(leaders.first().is_some());
    assert!(leaders.iter().all_equal());
    println!("Last log index: {:?}", &last_index);
    assert!(last_index.iter().all_equal());
    assert_eq!(*last_index.first().unwrap(), expected_last_index as u64);
}

async fn simulate_raft(
    simulation_length: u64,
    target_apply_count: u32,
) -> Result<(Vec<RaftNode>, usize)> {
    let shutdown_signals = repeat(Arc::new(ShutdownSignal::new()))
        .take(3)
        .collect_vec();

    let num_replicas = 3;
    let config = RaftConfig::new(num_replicas);
    let nodes: Vec<RaftNode> = (0..num_replicas)
        .map(|id| {
            let raft = Raft::new(id as u64, config.clone(), PrinterStateMachine::new());
            let shared_raft = Arc::new(RwLock::new(raft));
            RaftNode::new_with_shutdown(shared_raft, shutdown_signals[id as usize].clone())
        })
        .collect();

    let apply_count = Arc::new(AtomicUsize::new(0));

    debug!("Nodes are ready. Running them now");
    let mut threads = Vec::new();
    for node in nodes.iter() {
        let raft_node = node.clone();
        threads.push(spawn(async move {
            raft_node.run().await.unwrap();
        }));

        let raft_node = node.clone();
        let apply_count = apply_count.clone();
        threads.push(spawn(async move {
            delay_for(Duration::from_millis(1000)).await;

            for i in 0..target_apply_count {
                delay_for(Duration::from_micros(200)).await;
                let command = Command {
                    r#type: CommandType::Limit as i32,
                    sequence_id: i as u64,
                    price: 17,
                    client_id: 0,
                    size: 0,
                    security_id: 0,
                    side: OrderSide::Buy as i32,
                    cancel_order_id: 0,
                };
                let result = raft_node.start(command).await;
                if let Ok(true) = result {
                    apply_count.fetch_add(1, Ordering::Release);
                }
            }
        }));

        let raft_node = node.clone();
        threads.push(spawn(async move {
            // Block the current leader

            delay_for(Duration::from_millis(500)).await;

            debug!("Trying to lock for killing");
            let raft = raft_node.raft.write().await;
            debug!("Locked for killing");
            if raft.target_state.is_leader() {
                debug!("Killing leader (node {:})", raft.voted_for.unwrap());
                delay_for(Duration::from_millis(5000)).await;
            }
        }));
    }

    threads.push(spawn(async move {
        delay_for(Duration::from_secs(simulation_length)).await;
        debug!("Sending shutdown signal");
        shutdown_signals.iter().for_each(|signal| signal.shutdown());
        debug!("Shutdown signal sent");
    }));

    let _ = try_join_all(threads).await?;
    Ok((nodes, apply_count.load(Ordering::Acquire)))
}
