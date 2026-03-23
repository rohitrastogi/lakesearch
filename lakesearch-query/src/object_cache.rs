//! LRU cache for immutable objects in object storage.
//!
//! All objects except `current.json` are immutable once written (paths
//! contain UUIDs). Cache hits are always valid — no invalidation needed.
//! Eviction is by total byte size (configurable, default 256 MB).

use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use moka::future::Cache;
use object_store::ObjectStore;
use parquet::file::metadata::ParquetMetaData;

use lakesearch_core::parquet_util::load_parquet_metadata_async;
use lakesearch_core::storage::load_bytes;

/// Default max cache size in bytes (256 MB).
const DEFAULT_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// Default max number of cached Parquet metadata entries.
const DEFAULT_MAX_PARQUET_META: u64 = 1024;

/// Caches immutable objects by path with LRU eviction.
pub struct ObjectCache {
    store: Arc<dyn ObjectStore>,
    /// Byte-level cache (segments, manifests, manifest lists).
    /// Weighted by byte size, evicts LRU when total exceeds budget.
    bytes: Cache<String, Bytes>,
    /// Parquet metadata cache, evicts LRU by entry count.
    parquet_meta: Cache<String, Arc<ParquetMetaData>>,
}

impl ObjectCache {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self::with_capacity(store, DEFAULT_MAX_BYTES, DEFAULT_MAX_PARQUET_META)
    }

    pub fn with_capacity(
        store: Arc<dyn ObjectStore>,
        max_bytes: u64,
        max_parquet_entries: u64,
    ) -> Self {
        let bytes = Cache::builder()
            .max_capacity(max_bytes)
            .weigher(|_key: &String, value: &Bytes| -> u32 {
                value.len().try_into().unwrap_or(u32::MAX)
            })
            .build();

        let parquet_meta = Cache::builder().max_capacity(max_parquet_entries).build();

        Self {
            store,
            bytes,
            parquet_meta,
        }
    }

    /// Returns the underlying object store (for non-cached operations).
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// Fetches bytes from cache or object storage.
    /// Use for: manifest lists, manifests, segment bytes.
    pub async fn get_bytes(&self, path: &str) -> Result<Bytes> {
        let key = path.to_owned();
        let store = Arc::clone(&self.store);
        self.bytes
            .try_get_with::<_, Arc<str>>(key, async move {
                load_bytes(store.as_ref(), path)
                    .await
                    .map_err(|e| Arc::from(e.to_string()))
            })
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Fetches and deserializes a JSON object from cache or storage.
    pub async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let bytes = self.get_bytes(path).await?;
        serde_json::from_slice(&bytes).map_err(|e| anyhow::anyhow!("deserializing {path}: {e}"))
    }

    /// Fetches Parquet metadata from cache or storage.
    pub async fn get_parquet_metadata(&self, path: &str) -> Result<Arc<ParquetMetaData>> {
        let key = path.to_owned();
        let store = Arc::clone(&self.store);
        self.parquet_meta
            .try_get_with::<_, Arc<str>>(key, async move {
                load_parquet_metadata_async(&store, path)
                    .await
                    .map(Arc::new)
                    .map_err(|e| Arc::from(e.to_string()))
            })
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }
}
