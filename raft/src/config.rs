use std::net::*;
use std::time::Duration;

const DEFAULT_START_PORT: u16 = 9090;

/// Contains configuration parameters for a Raft cluster
#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub num_replicas: u64,
    pub addresses: Vec<SocketAddr>,
    pub reelection_timeout_min: Duration,
    pub reelection_timeout_max: Duration,
    pub heartbeat_period: Duration,
    pub batching_period: Duration,
    pub batching_size: usize,
}

impl RaftConfig {
    /// Creates a new Raft configuration with default parameters. Raft nodes are initialised to
    /// start at the loopback interface with port numberts starting at [DEFAULT_START_PORT]
    pub fn new(num_replicas: u64) -> Self {
        let default_ports = DEFAULT_START_PORT..(DEFAULT_START_PORT + num_replicas as u16);
        let addresses = default_ports
            .map(|port| SocketAddr::new(IpAddr::from(Ipv4Addr::LOCALHOST), port))
            .collect();
        Self {
            num_replicas,
            addresses,
            reelection_timeout_min: Duration::from_millis(150),
            reelection_timeout_max: Duration::from_millis(300),
            heartbeat_period: Duration::from_millis(75),
            batching_period: Duration::from_millis(1),
            batching_size: 100,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RaftConfig::new(7);
        println!("{:?}", config);
    }
}
