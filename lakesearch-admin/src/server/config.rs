use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;

/// Admin server configuration, loaded from a YAML file.
///
/// Example `config.yaml`:
/// ```yaml
/// bind_addr: "0.0.0.0:8081"
/// cascadq_url: "http://localhost:8000"
/// backfill_poll_secs: 30
/// backfill_chunk_size: 100
/// tables:
///   events:
///     location: "s3://bucket/lakesearch/tables/events/"
///     queue: "events-index"
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct IngestConfig {
    #[serde(default = "default_bind_addr")]
    pub bind_addr: SocketAddr,
    #[serde(default = "default_cascadq_url")]
    pub cascadq_url: String,
    #[serde(default = "default_backfill_poll_secs")]
    pub backfill_poll_secs: u64,
    #[serde(default = "default_backfill_chunk_size")]
    pub backfill_chunk_size: usize,
    #[serde(default)]
    pub tables: std::collections::HashMap<String, TableConfig>,
}

/// Per-table configuration in the YAML file.
#[derive(Debug, Clone, Deserialize)]
pub struct TableConfig {
    pub location: String,
    pub queue: String,
}

impl IngestConfig {
    /// Loads config from a YAML file.
    pub fn from_file(path: &Path) -> Result<Self> {
        let contents =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        serde_yaml::from_str(&contents).with_context(|| format!("parsing {}", path.display()))
    }

    pub fn backfill_poll_interval(&self) -> Duration {
        Duration::from_secs(self.backfill_poll_secs)
    }
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            bind_addr: default_bind_addr(),
            cascadq_url: default_cascadq_url(),
            backfill_poll_secs: default_backfill_poll_secs(),
            backfill_chunk_size: default_backfill_chunk_size(),
            tables: std::collections::HashMap::new(),
        }
    }
}

fn default_bind_addr() -> SocketAddr {
    "0.0.0.0:8081".parse().expect("valid default bind address")
}

fn default_cascadq_url() -> String {
    "http://localhost:8000".to_owned()
}

fn default_backfill_poll_secs() -> u64 {
    30
}

fn default_backfill_chunk_size() -> usize {
    100
}
