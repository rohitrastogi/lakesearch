use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use object_store::path::Path;
use object_store::ObjectStore;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use lakesearch_core::metadata::Metadata;

use crate::object_cache::ObjectCache;
use crate::storage::{parse_location, read_current, read_metadata};

/// Cached state for a single table.
struct TableState {
    base: Path,
    /// Latest known metadata_path from current.json.
    current_metadata_path: String,
    /// Parsed metadata (immutable once loaded, swapped atomically).
    metadata: Arc<Metadata>,
    /// Shared object cache for this table (segments, manifests, parquet metadata).
    object_cache: Arc<ObjectCache>,
}

/// Caches metadata for registered tables. Background task polls for updates.
pub struct MetadataCache {
    tables: RwLock<HashMap<String, TableState>>,
    poll_interval: Duration,
}

impl MetadataCache {
    pub fn new(poll_interval: Duration) -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            poll_interval,
        }
    }

    /// Registers a table by loading its metadata from object storage.
    pub async fn register(&self, name: &str, location: &str) -> Result<()> {
        let (store, base) = parse_location(location)?;
        self.register_with_store(name, store, base).await
    }

    /// Registers a table with an already-constructed object store and base path.
    /// Used in tests where the store is created programmatically (e.g., InMemory).
    pub async fn register_with_store(
        &self,
        name: &str,
        store: Arc<dyn ObjectStore>,
        base: Path,
    ) -> Result<()> {
        let current = read_current(store.as_ref(), &base).await?;
        let metadata = read_metadata(store.as_ref(), &current.value).await?;

        let state = TableState {
            base,
            current_metadata_path: current.value.metadata_path.clone(),
            metadata: Arc::new(metadata),
            object_cache: Arc::new(ObjectCache::new(store)),
        };

        self.tables.write().await.insert(name.to_owned(), state);
        Ok(())
    }

    /// Returns the cached metadata for a table.
    pub async fn get_metadata(&self, name: &str) -> Option<Arc<Metadata>> {
        self.tables
            .read()
            .await
            .get(name)
            .map(|s| Arc::clone(&s.metadata))
    }

    /// Returns the shared object cache and base path for a table.
    pub async fn get_cache(&self, name: &str) -> Option<(Arc<ObjectCache>, Path)> {
        self.tables
            .read()
            .await
            .get(name)
            .map(|s| (Arc::clone(&s.object_cache), s.base.clone()))
    }

    /// Returns names of all registered tables.
    pub async fn table_names(&self) -> Vec<String> {
        self.tables.read().await.keys().cloned().collect()
    }

    /// Starts the background metadata polling task. Returns a JoinHandle
    /// that can be aborted on shutdown.
    pub fn start_polling(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let cache = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(cache.poll_interval).await;
                cache.refresh_all().await;
            }
        })
    }

    async fn refresh_all(&self) {
        let names: Vec<String> = self.tables.read().await.keys().cloned().collect();
        for name in names {
            if let Err(e) = self.refresh_one(&name).await {
                warn!(table = %name, error = %e, "failed to refresh metadata");
            }
        }
    }

    async fn refresh_one(&self, name: &str) -> Result<()> {
        let (store, base, old_path) = {
            let tables = self.tables.read().await;
            let state = tables
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("table '{name}' not registered"))?;
            (
                Arc::clone(state.object_cache.store()),
                state.base.clone(),
                state.current_metadata_path.clone(),
            )
        };

        let current = read_current(store.as_ref(), &base).await?;
        if current.value.metadata_path == old_path {
            return Ok(()); // No change
        }

        debug!(table = %name, new_path = %current.value.metadata_path, "metadata updated");
        let metadata = read_metadata(store.as_ref(), &current.value).await?;

        let mut tables = self.tables.write().await;
        if let Some(state) = tables.get_mut(name) {
            state.current_metadata_path = current.value.metadata_path;
            state.metadata = Arc::new(metadata);
        }

        Ok(())
    }
}
