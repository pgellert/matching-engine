use std::cmp::{max, min};
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::{Duration, Instant};

use futures::future::try_join_all;
use rand::Rng;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio::time::delay_for;
use tokio::{join, spawn, task};
use tonic::codegen::Arc;
use tonic::transport::Server;
use tracing::*;

use crate::config::RaftConfig;
use crate::network::RaftNetwork;
use crate::protobuf::raft_rpc_server::RaftRpcServer;
use crate::protobuf::Command;
use crate::protobuf::LogEntry;
use crate::protobuf::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
use crate::rpc_server::RaftRPCServer;
use crate::shutdown::ShutdownSignal;
use crate::state_machine::StateMachine;
use crate::storage::RaftStorage;

pub type NodeId = u64;
type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

const LOOPING_PERIOD: Duration = Duration::from_micros(200);

/// Raft state stored as per the Raft paper
pub struct Raft {
    pub me: NodeId,
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>,

    commit_index: u64,
    pub last_log_index: u64,
    last_log_term: u64,

    // Initialised for leader only
    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,

    last_applied: Option<u64>,
    pub target_state: TargetState,

    pub reelection_timeout: Option<Instant>,
    next_heartbeat: Option<Instant>,

    network: Arc<RaftNetwork>,
    _storage: RaftStorage,
    config: RaftConfig,

    state_machine: Arc<dyn StateMachine + Send + Sync>,
}

/// Container for a shared Raft node
#[derive(Clone)]
pub struct RaftNode {
    shutdown_signal: Arc<ShutdownSignal>,
    pub raft: Arc<RwLock<Raft>>,
}

#[derive(Debug, Eq, PartialEq)]
pub enum TargetState {
    Unknown,
    Follower,
    Candidate,
    Leader,
}

impl TargetState {
    pub fn is_leader(&self) -> bool {
        *self == TargetState::Leader
    }
    pub fn is_follower(&self) -> bool {
        *self == TargetState::Follower
    }
    pub fn is_candidate(&self) -> bool {
        *self == TargetState::Candidate
    }
}

impl Raft {
    pub fn is_leader(&self) -> bool {
        self.target_state.is_leader()
    }
    pub fn is_follower(&self) -> bool {
        self.target_state.is_follower()
    }
    pub fn is_candidate(&self) -> bool {
        self.target_state.is_candidate()
    }
}

impl Raft {
    fn update_current_term(&mut self, term: u64, voted_for: Option<NodeId>) {
        if self.current_term < term {
            self.current_term = term;
            self.voted_for = voted_for;
        }
    }

    fn set_target_state(&mut self, target: TargetState) {
        self.target_state = target;
    }

    #[allow(dead_code)]
    async fn save_hard_state(&mut self) -> Result<()> {
        // TODO: implement saving the hard state of Raft to [self.storage]
        Ok(())
    }

    fn get_reelection_timeout(&mut self) -> Instant {
        self.reelection_timeout.unwrap_or_else(move || {
            let inst = Instant::now() + self.rand_reelection_timeout();
            self.reelection_timeout = Some(inst);
            inst
        })
    }

    fn update_reelection_timeout(&mut self) {
        let inst = Instant::now() + self.rand_reelection_timeout();
        self.reelection_timeout = Some(inst);
    }

    fn rand_reelection_timeout(&self) -> Duration {
        let min_timeout = self.config.reelection_timeout_min;
        let max_timeout = self.config.reelection_timeout_max;
        if min_timeout == max_timeout {
            min_timeout
        } else {
            rand::thread_rng().gen_range(min_timeout, max_timeout)
        }
    }

    fn get_next_heartbeat(&mut self) -> Instant {
        self.next_heartbeat.unwrap_or_else(move || {
            let inst = Instant::now();
            self.next_heartbeat = Some(inst);
            inst
        })
    }

    fn update_next_heartbeat(&mut self) {
        let inst = Instant::now() + self.config.heartbeat_period;
        self.next_heartbeat = Some(inst);
    }

    fn update_commit_index(&mut self) {
        let mut commit_index = self.commit_index;
        loop {
            let mut count_committed = 1;
            for id in self.network.clone().peers.keys() {
                if let Some(&committed) = self.match_index.get(id) {
                    if committed > commit_index {
                        count_committed += 1;
                    }
                }
            }

            if count_committed > self.network.peers.len() / 2 {
                commit_index += 1;
            } else {
                break;
            }
        }
        info!(
            "Updating leader commit index from {:} to {:}",
            self.commit_index, commit_index
        );
        if self.commit_index != commit_index {
            // Send early heartbeat to reduce the latency on applying logs
            self.next_heartbeat = Some(Instant::now());
        }
        self.commit_index = commit_index;
    }

    async fn replicate_to_state_machine_if_needed(&mut self) -> Result<()> {
        debug!("replicate_to_state_machine_if_needed called");
        while self.last_applied < Some(self.commit_index)
            && self.last_applied < Some(self.last_log_index)
        {
            let applied_index = self.last_applied.map_or(0, |val| val + 1) as usize;

            if let Some(command) = self.log[applied_index].command.clone() {
                info!("Applying command {:?}: {:?}", applied_index, command);
                self.state_machine
                    .apply_command(&command, self.is_leader())
                    .await;
            }
            self.last_applied = Some(applied_index as u64);
        }

        Ok(())
    }

    #[tracing::instrument(skip(self), fields(self.me))]
    pub(crate) async fn handle_request_vote(
        &mut self,
        request: &VoteRequest,
    ) -> Result<VoteResponse> {
        tracing::trace!("Handling request vote");
        // Reject outdated requests
        if request.term < self.current_term {
            return Ok(VoteResponse {
                term: self.current_term,
                vote_granted: false,
            });
        }

        // If the request coming from the future, update the local state
        if request.term > self.current_term {
            self.update_current_term(request.term, None);
            self.update_reelection_timeout();
            self.set_target_state(TargetState::Follower);
        }

        // If the candidate is not at least as uptodate as us, reject the vote
        let client_is_uptodate = (request.last_log_term >= self.last_log_term)
            && (request.last_log_index >= self.last_log_index as u64);
        if !client_is_uptodate {
            return Ok(VoteResponse {
                term: self.current_term,
                vote_granted: false,
            });
        }

        // Grant vote based on whether we have voted
        match &self.voted_for {
            Some(candidate_id) if candidate_id == &request.candidate_id => Ok(VoteResponse {
                term: self.current_term,
                vote_granted: true,
            }),
            Some(_) => Ok(VoteResponse {
                term: self.current_term,
                vote_granted: false,
            }),
            None => {
                self.voted_for = Some(request.candidate_id);
                self.set_target_state(TargetState::Follower);
                self.update_reelection_timeout();
                Ok(VoteResponse {
                    term: self.current_term,
                    vote_granted: true,
                })
            }
        }
    }

    #[tracing::instrument(skip(self, request), fields(self.me))]
    pub(crate) async fn handle_append_entries(
        &mut self,
        request: &AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse> {
        tracing::trace!("Handling append entries");

        // Reject outdated requests
        if request.term < self.current_term {
            tracing::trace!(
                self.current_term,
                rpc_term = request.term,
                "AppendEntries RPC term is less than current term"
            );
            return Ok(AppendEntriesResponse {
                term: self.current_term,
                success: false,
            });
        }

        self.update_reelection_timeout();
        self.commit_index = request.leader_commit;
        self.update_current_term(request.term, None);
        self.voted_for = Some(request.leader_id);
        self.set_target_state(TargetState::Follower);

        // Check that logs are consistent (ie. last log and terms match)
        let msg_index_and_term_match = (request.prev_log_index == self.last_log_index as u64)
            && (request.prev_log_term == self.last_log_term);
        if msg_index_and_term_match {
            // Only a heartbeat
            if request.entries.is_empty() {
                self.replicate_to_state_machine_if_needed().await?;
                return Ok(AppendEntriesResponse {
                    term: self.current_term,
                    success: true,
                });
            }

            self.append_log_entries(&request.entries).await?;
            self.replicate_to_state_machine_if_needed().await?;
            return Ok(AppendEntriesResponse {
                term: self.current_term,
                success: true,
            });
        }

        // Log consistency check
        let last_entry = self.log.get(request.prev_log_index as usize);
        let last_entry = match last_entry {
            Some(last_entry) => last_entry,
            None => {
                // If the leader's log is further ahead, reply false to get a response with earlier
                // logs
                return Ok(AppendEntriesResponse {
                    term: self.current_term,
                    success: false,
                });
            }
        };

        if last_entry.term != request.prev_log_term {
            // If the log terms are inconsistent, reply false
            return Ok(AppendEntriesResponse {
                term: self.current_term,
                success: false,
            });
        }

        // If the logs agree, but the leader is sending earlier logs, drop the local logs and us the
        // leader's logs instead
        if self.last_log_index > last_entry.index {
            self.log.truncate(1 + last_entry.index as usize);
        }

        self.append_log_entries(&request.entries).await?;
        self.replicate_to_state_machine_if_needed().await?;
        Ok(AppendEntriesResponse {
            term: self.current_term,
            success: true,
        })
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_log_entries(&mut self, entries: &Vec<LogEntry>) -> Result<()> {
        for entry in entries {
            debug!("Appending entry {:?}", entry);
            self.log.push(entry.clone());
        }
        if let Some(entry) = self.log.last() {
            self.last_log_index = entry.index;
            self.last_log_term = entry.term;
        }
        Ok(())
    }
}

impl Raft {
    pub fn new(
        id: NodeId,
        config: RaftConfig,
        state_machine: Arc<dyn StateMachine + Send + Sync>,
    ) -> Self {
        Self {
            me: id,
            current_term: 0,
            voted_for: None,
            log: vec![LogEntry {
                term: 0,
                index: 0,
                command: None,
            }],
            commit_index: 0,
            last_log_index: 0,
            last_log_term: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            last_applied: None,
            target_state: TargetState::Unknown,
            reelection_timeout: None,
            next_heartbeat: None,
            network: Arc::new(RaftNetwork::from_addresses(&config.addresses)),
            _storage: RaftStorage::new(),
            config,
            state_machine,
        }
    }
}

impl RaftNode {
    pub fn new(raft: Arc<RwLock<Raft>>) -> Self {
        Self {
            shutdown_signal: Arc::new(ShutdownSignal::new()),
            raft,
        }
    }

    pub fn new_with_shutdown(
        raft: Arc<RwLock<Raft>>,
        shutdown_signal: Arc<ShutdownSignal>,
    ) -> Self {
        Self {
            shutdown_signal,
            raft,
        }
    }

    /// Starts an RPC server ([RaftRpcServer]) to reply to peer Raft nodes
    async fn start_rpc_server(&self) -> Result<()> {
        let addr = {
            let raft = self.raft.read().await;
            let network = &raft.network;
            let me = network.peers.get(&raft.me).unwrap();
            me.addr
        };
        let server = RaftRpcServer::new(RaftRPCServer {
            raft: self.raft.clone(),
        });
        let signal = self.shutdown_signal.clone();
        tracing::trace!("Starting rpc server");
        Server::builder()
            .add_service(server)
            .serve_with_shutdown(addr, RaftNode::await_shutdown_signal(signal))
            .await?;
        trace!("Shutting down rpc server");
        Ok(())
    }

    async fn await_shutdown_signal(signal: Arc<ShutdownSignal>) {
        let _ = join!(task::spawn(
            async move { signal.await_shutdown_signal().await }
        ));
    }

    /// Runs a Raft node as per the Raft protocol.
    ///
    /// It starts the following threads:
    ///  - [start_rpc_server]
    ///  - [run_heartbeat_loop]
    ///  - [run_reelection_loop]
    #[tracing::instrument(skip(self))]
    pub async fn run(&self) -> Result<()> {
        let mut handles = vec![];

        let raft = self.clone();
        handles.push(task::spawn(async move { raft.start_rpc_server().await }));

        let raft = self.clone();
        handles.push(task::spawn(async move { raft.run_heartbeat_loop().await }));

        let raft = self.clone();
        handles.push(task::spawn(async move { raft.run_reelection_loop().await }));

        let _ = try_join_all(handles).await?;
        trace!("End of run reached");
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn start(&self, command: Command) -> Result<bool> {
        let mut raft = self.raft.write().await;

        if !raft.is_leader() {
            trace!("Node is not a leader, aborting start of command");
            return Ok(false);
        }

        let entry = LogEntry {
            term: raft.current_term,
            command: Some(command),
            index: raft.last_log_index + 1,
        };
        info!("Starting appending new log entry: {:?}", entry);
        raft.last_log_index += 1;
        raft.last_log_term = raft.current_term;
        raft.log.push(entry);

        raft.next_heartbeat = min(
            raft.next_heartbeat,
            Some(Instant::now() + raft.config.batching_period),
        );

        Ok(true)
    }

    /// Starts a reelection loop that sends an election request to Raft peers if the current leader
    /// times out (i.e. if a heartbeat is not heard within the heartbeat period)
    ///
    /// This function returns when the shutdown signal was sent to the Raft node or an error
    /// occured.
    async fn run_reelection_loop(&self) -> Result<()> {
        loop {
            let (terminated, is_leader, reelection_time) = {
                let mut raft = self.raft.write().await;
                let terminated = self.shutdown_signal.poll_terminated();
                (terminated, raft.is_leader(), raft.get_reelection_timeout())
            };

            if terminated {
                trace!("Terminated reelection loop");
                return Ok(());
            }

            if !is_leader && Instant::now() >= reelection_time {
                tracing::info!("Reelection timeout reached, starting reelection");
                let res = self.start_reelection().await;
                if let Err(err) = res {
                    tracing::error!("Reelection error {:?}", err.to_string());
                };
            }

            delay_for(LOOPING_PERIOD).await;
        }
    }

    async fn start_reelection(&self) -> Result<()> {
        let (me, network, term, last_log_index, last_log_term) = {
            let mut raft = self.raft.write().await;
            raft.set_target_state(TargetState::Candidate);
            raft.voted_for = None;
            raft.current_term += 1;
            raft.update_reelection_timeout();

            (
                raft.me,
                raft.network.clone(),
                raft.current_term,
                raft.last_log_index,
                raft.last_log_term,
            )
        };

        let request = VoteRequest {
            term,
            candidate_id: me,
            last_log_index: last_log_index as u64,
            last_log_term,
        };
        let mut handles = vec![];
        let result_handler;

        {
            let (s, r) = mpsc::channel(network.peers.len() - 1);

            for (&id, _) in network.peers.iter().filter(|(&id, _)| id != me) {
                let raft = self.raft.clone();
                let request = request.clone();
                let s = s.clone();
                handles.push(spawn(async move {
                    RaftNode::send_request_vote(raft, request, id, s).await
                }));
            }

            let quorum_size = {
                let raft = self.raft.write().await;
                1 + raft.network.peers.len() / 2
            };

            result_handler =
                RaftNode::handle_election_result(self.raft.clone(), term, quorum_size, r);
        }

        let _ = join!(try_join_all(handles), result_handler);

        Ok(())
    }

    async fn handle_election_result(
        raft: Arc<RwLock<Raft>>,
        term: u64,
        quorum_size: usize,
        mut r: Receiver<u64>,
    ) {
        let mut vote_count = 1;
        while let Some(_) = r.recv().await {
            vote_count += 1;

            debug!("Received vote #{:} out of {:}", vote_count, quorum_size);
            if vote_count >= quorum_size {
                break;
            }
        }

        let mut raft = raft.write().await;
        if raft.current_term != term {
            return;
        }
        let won_election = vote_count >= quorum_size && raft.is_candidate();
        if won_election {
            raft.set_target_state(TargetState::Leader);
            raft.voted_for = Some(raft.me);
            raft.next_heartbeat = Some(Instant::now());
            raft.voted_for = Some(raft.me);

            raft.match_index.values_mut().for_each(|val| *val = 0);
            let last_log_index = raft.last_log_index;
            raft.next_index
                .values_mut()
                .for_each(|val| *val = last_log_index + 1);

            tracing::info!({ node = raft.me }, "Converted to Leader")
        } else {
            raft.set_target_state(TargetState::Follower);
        }
    }

    async fn send_request_vote(
        raft: Arc<RwLock<Raft>>,
        request: VoteRequest,
        id: u64,
        mut s: Sender<u64>,
    ) -> Result<()> {
        let peer = {
            let raft = raft.read().await;
            raft.network.peers.get(&id).unwrap().clone()
        };
        tracing::debug!("Sending vote request to {:}: {:?}", id, &request);
        let response = peer.send_request_vote(request).await?;
        tracing::debug!("Received vote response from {:}: {:?}", id, response);

        if response.vote_granted {
            let _ = s.send(id).await;
        } else {
            let mut raft = raft.write().await;
            raft.update_current_term(response.term, None);
            raft.set_target_state(TargetState::Follower);
        }
        Ok(())
    }

    /// Starts a heartbeat loop that periodically sends heartbeats to other Raft replicas if the
    /// local replica is the leader of the cluster.
    ///
    /// This function returns when the shutdown signal was sent to the Raft node or an error
    /// occured.
    async fn run_heartbeat_loop(&self) -> Result<()> {
        loop {
            let (
                terminated,
                me,
                is_leader,
                current_term,
                next_heartbeat,
                network,
                leader_commit,
                leader_id,
            ) = {
                let mut raft = self.raft.write().await;
                let next_heartbeat = raft.get_next_heartbeat();
                let terminated = self.shutdown_signal.poll_terminated();
                (
                    terminated,
                    raft.me,
                    raft.is_leader(),
                    raft.current_term,
                    next_heartbeat,
                    raft.network.clone(),
                    raft.commit_index,
                    raft.voted_for,
                )
            };

            if terminated {
                trace!("Terminated heartbeat loop");
                return Ok(());
            }

            if is_leader && Instant::now() >= next_heartbeat {
                {
                    let mut raft = self.raft.write().await;
                    raft.update_next_heartbeat();
                };

                tracing::debug!("Sending heartbeat");

                self.send_heartbeats(me, current_term, network.clone(), leader_commit, leader_id)
                    .await?;
            } else {
                delay_for(LOOPING_PERIOD).await;
            }
        }
    }

    async fn send_heartbeats(
        &self,
        me: u64,
        current_term: u64,
        network: Arc<RaftNetwork>,
        leader_commit: u64,
        leader_id: Option<NodeId>,
    ) -> Result<()> {
        let others = network
            .peers
            .iter()
            .map(|(id, peer)| (*id, peer.clone()))
            .filter(|(id, _)| *id != me);

        for (id, peer) in others {
            let raft_node = self.clone();
            tokio::spawn(async move {
                let request = raft_node
                    .make_heartbeat_request(current_term, leader_commit, leader_id, id)
                    .await;

                let last_log_sent = request.prev_log_index + request.entries.len() as u64;

                tracing::trace!("Sending append entries request to {:}: {:?}", id, request);

                let response = match peer.send_append_entries(request).await {
                    Ok(response) => response,
                    Err(err) => {
                        error!("Error in sending append entries to {:}: {:?}", id, err);
                        return;
                    }
                };

                tracing::trace!(
                    "Received append entries response from {:}: {:?}",
                    id,
                    response
                );

                if response.term > current_term {
                    let mut raft = raft_node.raft.write().await;
                    if raft.current_term == current_term {
                        raft.update_current_term(response.term, None);
                        raft.set_target_state(TargetState::Follower);
                        raft.voted_for = None;
                    }
                } else if response.success {
                    let mut raft = raft_node.raft.write().await;
                    if raft.current_term > current_term {
                        return;
                    }

                    tracing::info!(
                        "append entries succeeded, match index to {:} is now {:} from {:}",
                        id,
                        last_log_sent,
                        raft.match_index.get(&id).cloned().unwrap_or(0)
                    );
                    let prev_match_index = raft.match_index.get(&id).cloned().unwrap_or(0);
                    raft.match_index
                        .insert(id, max(prev_match_index, last_log_sent));

                    raft.update_commit_index();
                    raft.replicate_to_state_machine_if_needed().await;
                } else {
                    tracing::trace!("retrying sending append entries to {:}", id);
                    let mut raft = raft_node.raft.write().await;
                    if raft.current_term > current_term {
                        return;
                    }
                    raft.next_index.entry(id).and_modify(|val| *val -= 1); // (id, last_log_index + 1);
                }
            });
        }

        Ok(())
    }

    async fn make_heartbeat_request(
        &self,
        current_term: u64,
        leader_commit: u64,
        leader_id: Option<u64>,
        id: u64,
    ) -> AppendEntriesRequest {
        let mut raft = self.raft.write().await;
        let next_index = raft.next_index.get(&id).cloned().unwrap_or(1);
        let next_index = next_index as usize;
        trace!("next index to {:} is {:}", id, next_index);

        let next_index = max(1, min(next_index, raft.log.len()));
        let prev_log_index = next_index - 1;
        let prev_log_term = raft
            .log
            .get(prev_log_index)
            .map(|log| log.term)
            .unwrap_or(0);

        let entries_end = min(raft.log.len(), next_index + raft.config.batching_size);
        raft.next_index.insert(id, entries_end as u64);

        let logs_to_send = raft.log[next_index..entries_end]
            .iter()
            .cloned()
            .collect::<Vec<LogEntry>>();

        AppendEntriesRequest {
            term: current_term,
            entries: logs_to_send,
            leader_commit: leader_commit as u64,
            leader_id: leader_id.unwrap(),
            prev_log_index: prev_log_index as u64,
            prev_log_term,
        }
    }
}
