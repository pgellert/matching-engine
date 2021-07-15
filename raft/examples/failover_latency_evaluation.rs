extern crate raft;

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::future::try_join_all;
use itertools::Itertools;
use rand::Rng;
use tokio::sync::Barrier;
use tokio::sync::{Mutex, RwLock};
use tokio::task::spawn;
use tokio::time::delay_for;
use tracing::{debug, info};

use raft::network::RaftNetwork;
use raft::raft::{Raft, RaftNode};
use raft::shutdown::ShutdownSignal;
use raft::state_machine::StateMachine;
use raft::storage::RaftStorage;

use self::raft::config::RaftConfig;
use self::raft::protobuf::Command;

const N_NODES: u64 = 3;
const N_EXPERIMENT: u64 = 20;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let params = vec![
        //(12u64,24u64),
        (25u64, 50u64),
        (50, 100),
        (100, 200),
        (150, 300),
        (150, 150),
        (150, 151),
        (150, 155),
        (150, 175),
        (150, 200),
        (150, 300),
    ];

    for i in 0..N_EXPERIMENT {
        eprintln!("Experiment #{:}...", i);
        for (min_timeout, max_timeout) in params.clone().into_iter() {
            info!("Setup: {:} {:} {:}", i, min_timeout, max_timeout);
            failover_latency_evaluation(min_timeout, max_timeout).await;
            delay_for(Duration::from_millis(100)).await;
        }
    }
}

async fn failover_latency_evaluation(
    reelection_timeout_min: u64,
    reelection_timeout_max: u64,
) -> Result<()> {
    let mut config = RaftConfig::new(N_NODES);
    config.reelection_timeout_min = Duration::from_millis(reelection_timeout_min);
    config.reelection_timeout_max = Duration::from_millis(reelection_timeout_max);
    config.heartbeat_period = config.reelection_timeout_min / 2;

    let start_barrier1 = Arc::new(Barrier::new((2 * N_NODES + 1) as usize));
    let start_barrier2 = Arc::new(Barrier::new((N_NODES + 1) as usize));
    let timeout_offset =
        rand::thread_rng().gen_range(Duration::from_millis(0), config.heartbeat_period);
    let th_nodes = tokio::spawn(setup_nodes(
        config,
        start_barrier1.clone(),
        start_barrier2.clone(),
        timeout_offset,
    ));

    //delay_for(Duration::from_millis(1)).await;
    debug!("Releasing start_barrier1");
    start_barrier1.wait().await;
    //delay_for(Duration::from_millis(1)).await;
    debug!("Releasing barrier2");
    start_barrier2.wait().await;
    th_nodes.await?;

    Ok(())
}

async fn setup_nodes(
    config: RaftConfig,
    start_barrier1: Arc<Barrier>,
    start_barrier2: Arc<Barrier>,
    timeout_offset: Duration,
) -> Result<()> {
    let shutdown_signal = Arc::new(ShutdownSignal::new());

    let raft_states: Vec<Arc<RwLock<Raft>>> = (0..config.num_replicas)
        .map(|id| Raft::new(id as u64, config.clone(), NoopStateMachine::new()))
        .map(|raft| Arc::new(RwLock::new(raft)))
        .collect();

    let nodes: Vec<RaftNode> = raft_states
        .into_iter()
        .map(|raft| RaftNode::new_with_shutdown(raft, shutdown_signal.clone()))
        .collect();

    debug!("Nodes are ready. Running them now");
    let mut threads = Vec::new();
    for node in nodes.iter() {
        let raft_node = node.clone();
        let barrier1 = start_barrier1.clone();
        let barrier2 = start_barrier2.clone();
        threads.push(spawn(async move {
            let mut raft = raft_node.raft.write().await;
            debug!("Setting up state for raft node {:}", raft.me);

            barrier1.wait().await;

            info!("Setting reelection timeout");
            raft.reelection_timeout = Some(Instant::now() + timeout_offset);

            barrier2.wait().await;

            if raft.target_state.is_leader() {
                debug!("Killing leader (node {:})", raft.voted_for.unwrap());
                delay_for(Duration::from_millis(5000)).await;
            }
        }));

        let barrier1 = start_barrier1.clone();
        let raft_node = node.clone();
        threads.push(spawn(async move {
            barrier1.wait().await;
            raft_node.run().await.unwrap();
        }));
    }

    threads.push(spawn(async move {
        delay_for(Duration::from_millis(1500)).await;
        debug!("Sending shutdown signal");
        shutdown_signal.shutdown();
        debug!("Shutdown signal sent");
    }));

    let _ = try_join_all(threads).await?;
    Ok(())
}

#[derive(Debug)]
pub struct NoopStateMachine;

impl NoopStateMachine {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl StateMachine for NoopStateMachine {
    async fn apply_command(&self, _: &Command, _: bool) -> () {}
}
