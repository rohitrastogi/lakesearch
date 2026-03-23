use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;

/// Ingest worker configuration, loaded from a YAML file.
///
/// Example `config.yaml`:
/// ```yaml
/// bind_addr: "0.0.0.0:8082"
/// cascadq_url: "http://localhost:8000"
/// queues:
///   - events
///   - logs
/// poll_timeout_secs: 30
/// cpu_threads: 8
/// io_concurrency: 8
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct IngestWorkerConfig {
    #[serde(default = "default_bind_addr")]
    pub bind_addr: SocketAddr,
    #[serde(default = "default_cascadq_url")]
    pub cascadq_url: String,
    /// Queues to consume from (one consumer task per queue).
    pub queues: Vec<String>,
    #[serde(default = "default_poll_timeout_secs")]
    pub poll_timeout_secs: u64,
    #[serde(default = "default_cpu_threads")]
    pub cpu_threads: usize,
    #[serde(default = "default_io_concurrency")]
    pub io_concurrency: usize,
}

impl IngestWorkerConfig {
    /// Loads config from a YAML file.
    pub fn from_file(path: &Path) -> Result<Self> {
        let contents =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        serde_yaml::from_str(&contents).with_context(|| format!("parsing {}", path.display()))
    }

    pub fn poll_timeout(&self) -> Duration {
        Duration::from_secs(self.poll_timeout_secs)
    }
}

fn default_bind_addr() -> SocketAddr {
    "0.0.0.0:8082".parse().expect("valid default bind address")
}

fn default_cascadq_url() -> String {
    "http://localhost:8000".to_owned()
}

fn default_poll_timeout_secs() -> u64 {
    30
}

fn default_cpu_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

fn default_io_concurrency() -> usize {
    8
}
