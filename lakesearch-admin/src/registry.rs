//! In-memory table registry.
//!
//! Volatile: tables must be re-registered on restart (from config or via API).
//! Each entry caches a parsed ObjectStore + base Path for the table location.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use object_store::path::Path;
use object_store::ObjectStore;
use tokio::sync::RwLock;

use crate::storage::parse_location;

/// A registered table with its object store handle.
#[derive(Clone)]
pub struct RegisteredTable {
    pub table_id: String,
    pub table_name: String,
    pub location: String,
    pub queue: String,
    pub store: Arc<dyn ObjectStore>,
    pub base: Path,
}

/// Thread-safe in-memory registry of tables.
#[derive(Default)]
pub struct TableRegistry {
    tables: RwLock<HashMap<String, RegisteredTable>>,
}

impl TableRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a table. Returns an error if the location URL cannot be parsed.
    pub async fn register(
        &self,
        table_id: &str,
        table_name: &str,
        location: &str,
        queue: &str,
    ) -> Result<()> {
        let (store, base) = parse_location(location)?;
        let entry = RegisteredTable {
            table_id: table_id.to_owned(),
            table_name: table_name.to_owned(),
            location: location.to_owned(),
            queue: queue.to_owned(),
            store,
            base,
        };
        self.tables.write().await.insert(table_id.to_owned(), entry);
        Ok(())
    }

    /// Removes a table from the registry. Returns true if the table existed.
    pub async fn unregister(&self, table_id: &str) -> bool {
        self.tables.write().await.remove(table_id).is_some()
    }

    /// Gets a registered table by ID.
    pub async fn get(&self, table_id: &str) -> Option<RegisteredTable> {
        self.tables.read().await.get(table_id).cloned()
    }

    /// Returns all registered tables.
    pub async fn list(&self) -> Vec<RegisteredTable> {
        self.tables.read().await.values().cloned().collect()
    }
}
