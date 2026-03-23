use std::net::SocketAddr;
use std::time::Duration;

/// Server configuration, loaded from environment variables.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind_addr: SocketAddr,
    pub query_timeout: Duration,
    pub metadata_poll_interval: Duration,
    pub cpu_threads: usize,
    /// Table definitions: name → location URL.
    pub tables: Vec<(String, String)>,
}

impl ServerConfig {
    pub fn from_env() -> Self {
        let bind_addr = std::env::var("LAKESEARCH_BIND_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:8080".to_owned())
            .parse()
            .expect("invalid LAKESEARCH_BIND_ADDR");

        let query_timeout_secs: u64 = std::env::var("LAKESEARCH_QUERY_TIMEOUT_SECS")
            .unwrap_or_else(|_| "300".to_owned())
            .parse()
            .expect("invalid LAKESEARCH_QUERY_TIMEOUT_SECS");

        let metadata_poll_secs: u64 = std::env::var("LAKESEARCH_METADATA_POLL_SECS")
            .unwrap_or_else(|_| "5".to_owned())
            .parse()
            .expect("invalid LAKESEARCH_METADATA_POLL_SECS");

        let cpu_threads: usize = std::env::var("LAKESEARCH_CPU_THREADS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(4)
            });

        // Tables from LAKESEARCH_TABLES env var: "name1=location1,name2=location2"
        let tables = std::env::var("LAKESEARCH_TABLES")
            .unwrap_or_default()
            .split(',')
            .filter(|s| !s.is_empty())
            .filter_map(|entry| {
                let (name, loc) = entry.split_once('=')?;
                Some((name.trim().to_owned(), loc.trim().to_owned()))
            })
            .collect();

        Self {
            bind_addr,
            query_timeout: Duration::from_secs(query_timeout_secs),
            metadata_poll_interval: Duration::from_secs(metadata_poll_secs),
            cpu_threads,
            tables,
        }
    }
}
