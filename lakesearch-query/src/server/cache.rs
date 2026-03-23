//! Per-table ObjectCache management.
//!
//! Lazily creates and caches an `ObjectCache` (moka LRU) for each table.
//! No metadata polling — callers read fresh `current.json` per request.

use std::collections::HashMap;
use std::sync::Arc;

use object_store::limit::LimitStore;
use object_store::ObjectStore;
use tokio::sync::RwLock;

use crate::object_cache::ObjectCache;

/// Manages a per-table `ObjectCache` for caching immutable objects
/// (segments, manifests, parquet metadata).
pub struct TableCache {
    tables: RwLock<HashMap<String, Arc<ObjectCache>>>,
    io_concurrency: usize,
}

impl TableCache {
    pub fn new(io_concurrency: usize) -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            io_concurrency,
        }
    }

    /// Returns the `ObjectCache` for a table, creating one if needed.
    pub async fn get_or_create(
        &self,
        table_name: &str,
        store: Arc<dyn ObjectStore>,
    ) -> Arc<ObjectCache> {
        // Fast path: already exists
        {
            let tables = self.tables.read().await;
            if let Some(cache) = tables.get(table_name) {
                return Arc::clone(cache);
            }
        }

        // Slow path: create and insert
        let limited: Arc<dyn ObjectStore> = Arc::new(LimitStore::new(store, self.io_concurrency));
        let cache = Arc::new(ObjectCache::new(limited));

        let mut tables = self.tables.write().await;
        // Double-check after acquiring write lock
        tables.entry(table_name.to_owned()).or_insert(cache).clone()
    }
}
