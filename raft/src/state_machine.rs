use async_trait::async_trait;

use crate::protobuf::Command;

/// State-machine interface that Raft uses to deliver commands to the replicated state-machine
#[async_trait]
pub trait StateMachine {
    /// Delivers the replicated command (eg. an order request) to the state machine
    async fn apply_command(&self, command: &Command, is_leader: bool) -> ();
}
