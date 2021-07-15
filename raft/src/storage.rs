use crate::protobuf::LogEntry;
use crate::raft::Result;

/// !This is not yet integrated to the Raft implementation
///
/// This interface could be used to implement persistence for the Raft protocol
#[derive(Debug)]
pub struct RaftStorage {}

impl RaftStorage {
    pub fn new() -> Self {
        Self {}
    }
}

impl RaftStorage {
    #[allow(unused_variables)]
    pub async fn get_log_entries(&self, from_index: u32, to_index: u32) -> Result<&[LogEntry]> {
        //todo!("Implement")
        Ok(&[])
    }

    #[allow(unused_variables)]
    pub async fn delete_logs_from(&self, from_index: u32) -> Result<()> {
        //todo!("Implement")
        Ok(())
    }

    #[allow(unused_variables)]
    pub async fn replicate_to_log(&self, entries: &[LogEntry]) -> Result<()> {
        //todo!("Implement")
        Ok(())
    }
}
