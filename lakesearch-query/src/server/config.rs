use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;

/// Server configuration, loaded from a YAML file.
///
/// Example `config.yaml`:
/// ```yaml
/// bind_addr: "0.0.0.0:8080"
/// flight_addr: "0.0.0.0:8081"
/// query_timeout_secs: 300
/// metadata_poll_secs: 5
/// cpu_threads: 8
/// io_concurrency: 8
/// max_io_tasks: 64
/// tables:
///   events: "s3://bucket/lakesearch/tables/events/"
///   logs: "file:///tmp/lakesearch/logs/"
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_bind_addr")]
    pub bind_addr: SocketAddr,
    #[serde(default = "default_flight_addr")]
    pub flight_addr: SocketAddr,
    #[serde(default = "default_query_timeout_secs")]
    pub query_timeout_secs: u64,
    #[serde(default = "default_metadata_poll_secs")]
    pub metadata_poll_secs: u64,
    #[serde(default = "default_cpu_threads")]
    pub cpu_threads: usize,
    /// Maximum concurrent object-store operations (GET, HEAD) across all
    /// queries sharing a table's store. Enforced by `LimitStore` wrapping
    /// the `ObjectStore` at registration time.
    #[serde(default = "default_io_concurrency")]
    pub io_concurrency: usize,
    /// Maximum concurrent I/O producer tasks per query pipeline. Each task
    /// streams batches from one Parquet file. This bounds how many files a
    /// single query opens simultaneously; actual store-level concurrency is
    /// further limited by `io_concurrency` above.
    #[serde(default = "default_max_io_tasks")]
    pub max_io_tasks: usize,
    /// Table definitions: name → location URL.
    #[serde(default)]
    pub tables: std::collections::HashMap<String, String>,
}

impl ServerConfig {
    /// Loads config from a YAML file.
    pub fn from_file(path: &Path) -> Result<Self> {
        let contents =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        serde_yaml::from_str(&contents).with_context(|| format!("parsing {}", path.display()))
    }

    pub fn query_timeout(&self) -> Duration {
        Duration::from_secs(self.query_timeout_secs)
    }

    pub fn metadata_poll_interval(&self) -> Duration {
        Duration::from_secs(self.metadata_poll_secs)
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: default_bind_addr(),
            flight_addr: default_flight_addr(),
            query_timeout_secs: default_query_timeout_secs(),
            metadata_poll_secs: default_metadata_poll_secs(),
            cpu_threads: default_cpu_threads(),
            io_concurrency: default_io_concurrency(),
            max_io_tasks: default_max_io_tasks(),
            tables: std::collections::HashMap::new(),
        }
    }
}

fn default_bind_addr() -> SocketAddr {
    "0.0.0.0:8080".parse().unwrap()
}

fn default_flight_addr() -> SocketAddr {
    "0.0.0.0:8081".parse().unwrap()
}

fn default_query_timeout_secs() -> u64 {
    300
}

fn default_metadata_poll_secs() -> u64 {
    5
}

fn default_cpu_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

fn default_io_concurrency() -> usize {
    8
}

fn default_max_io_tasks() -> usize {
    64
}
