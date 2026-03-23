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
/// query_timeout_secs: 300
/// metadata_poll_secs: 5
/// cpu_threads: 8
/// io_concurrency: 8
/// tables:
///   events: "s3://bucket/lakesearch/tables/events/"
///   logs: "file:///tmp/lakesearch/logs/"
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_bind_addr")]
    pub bind_addr: SocketAddr,
    #[serde(default = "default_query_timeout_secs")]
    pub query_timeout_secs: u64,
    #[serde(default = "default_metadata_poll_secs")]
    pub metadata_poll_secs: u64,
    #[serde(default = "default_cpu_threads")]
    pub cpu_threads: usize,
    #[serde(default = "default_io_concurrency")]
    pub io_concurrency: usize,
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
            query_timeout_secs: default_query_timeout_secs(),
            metadata_poll_secs: default_metadata_poll_secs(),
            cpu_threads: default_cpu_threads(),
            io_concurrency: default_io_concurrency(),
            tables: std::collections::HashMap::new(),
        }
    }
}

fn default_bind_addr() -> SocketAddr {
    "0.0.0.0:8080".parse().unwrap()
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
