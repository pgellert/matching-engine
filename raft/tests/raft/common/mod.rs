use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tracing::debug;

use raft::protobuf::Command;
use raft::state_machine::StateMachine;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug)]
pub struct PrinterStateMachine;

impl PrinterStateMachine {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl StateMachine for PrinterStateMachine {
    async fn apply_command(&self, command: &Command, _: bool) -> () {
        debug!("Applying command {:}", command.sequence_id);
    }
}

#[derive(Debug)]
pub struct NoopStateMachine;

impl NoopStateMachine {
    pub fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {}))
    }
}

#[async_trait]
impl StateMachine for NoopStateMachine {
    async fn apply_command(&self, _: &Command, _: bool) -> () {}
}
