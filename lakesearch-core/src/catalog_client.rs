//! Pluggable table discovery.
//!
//! `CatalogClient` is the abstraction that decouples LakeSearch from any
//! specific catalog system. Implementations can back onto a static config,
//! an Iceberg REST catalog, a Hive metastore, or anything else that can
//! list tables and their locations.
//!
//! LakeSearch's own index metadata (`current.json`, manifests, segments)
//! lives as a sidecar next to the data — the catalog only tells us which
//! tables exist and where they are.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use object_store::path::Path;
use object_store::ObjectStore;

/// Information about a table from the catalog.
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub location: String,
    pub store: Arc<dyn ObjectStore>,
    pub base: Path,
}

impl TableInfo {
    /// Cascadq queue name, derived from table name.
    pub fn queue_name(&self) -> String {
        self.name.clone()
    }
}

/// Trait for discovering tables.
///
/// Read-only implementations (e.g. Iceberg catalog) only need
/// `list_tables` and `get_table`. Mutable methods default to
/// returning an error.
#[async_trait]
pub trait CatalogClient: Send + Sync {
    /// List all known tables.
    async fn list_tables(&self) -> Result<Vec<TableInfo>>;

    /// Look up a single table by name.
    async fn get_table(&self, name: &str) -> Result<Option<TableInfo>>;

    /// Register a new table. Not all catalogs support this.
    async fn register_table(&self, _name: &str, _location: &str) -> Result<TableInfo> {
        anyhow::bail!("register_table not supported by this catalog")
    }

    /// Unregister a table by name. Not all catalogs support this.
    async fn unregister_table(&self, _name: &str) -> Result<bool> {
        anyhow::bail!("unregister_table not supported by this catalog")
    }
}

/// A simple catalog backed by a static list of tables.
/// Tables are provided at construction time (e.g. from YAML config).
pub struct StaticCatalog {
    tables: Vec<TableInfo>,
}

impl StaticCatalog {
    pub fn new(tables: Vec<TableInfo>) -> Self {
        Self { tables }
    }
}

#[async_trait]
impl CatalogClient for StaticCatalog {
    async fn list_tables(&self) -> Result<Vec<TableInfo>> {
        Ok(self.tables.clone())
    }

    async fn get_table(&self, name: &str) -> Result<Option<TableInfo>> {
        Ok(self.tables.iter().find(|t| t.name == name).cloned())
    }
}
