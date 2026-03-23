//! Object storage helpers for reading/writing metadata, segments, and manifests.

use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use object_store::path::Path;
use object_store::{GetOptions, GetResult, ObjectStore, PutPayload};
use serde::de::DeserializeOwned;
use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use lakesearch_core::metadata::{CurrentPointer, Manifest, ManifestList, Metadata};

/// Result of reading a JSON object: the deserialized value and the ETag
/// (used for CAS conditional PUT).
pub struct ReadResult<T> {
    pub value: T,
    pub e_tag: Option<String>,
}

/// Reads and deserializes a JSON file from object storage.
pub async fn read_json<T: DeserializeOwned>(
    store: &dyn ObjectStore,
    path: &Path,
) -> Result<ReadResult<T>> {
    let result: GetResult = store
        .get(path)
        .await
        .with_context(|| format!("reading {path}"))?;
    let e_tag = result.meta.e_tag.clone();
    let data = result
        .bytes()
        .await
        .with_context(|| format!("reading bytes from {path}"))?;
    let value: T =
        serde_json::from_slice(&data).with_context(|| format!("deserializing {path}"))?;
    Ok(ReadResult { value, e_tag })
}

/// Serializes and writes a JSON file to object storage.
pub async fn write_json<T: Serialize>(
    store: &dyn ObjectStore,
    path: &Path,
    value: &T,
) -> Result<()> {
    let json = serde_json::to_vec_pretty(value).context("serializing JSON")?;
    store
        .put(path, PutPayload::from(json))
        .await
        .with_context(|| format!("writing {path}"))?;
    Ok(())
}

/// Reads `metadata/current.json` from the table base path.
pub async fn read_current(
    store: &dyn ObjectStore,
    base: &Path,
) -> Result<ReadResult<CurrentPointer>> {
    read_json(store, &base.child("metadata").child("current.json")).await
}

/// Reads the metadata file pointed to by a `CurrentPointer`.
pub async fn read_metadata(store: &dyn ObjectStore, pointer: &CurrentPointer) -> Result<Metadata> {
    let path = Path::from(pointer.metadata_path.as_str());
    let result = read_json(store, &path).await?;
    Ok(result.value)
}

/// Reads a manifest list from its full path.
pub async fn read_manifest_list(store: &dyn ObjectStore, path: &str) -> Result<ManifestList> {
    let path = Path::from(path);
    let result = read_json(store, &path).await?;
    Ok(result.value)
}

/// Reads a manifest from its full path.
pub async fn read_manifest(store: &dyn ObjectStore, path: &str) -> Result<Manifest> {
    let path = Path::from(path);
    let result = read_json(store, &path).await?;
    Ok(result.value)
}

/// Uploads a segment file and returns the relative path within the table.
pub async fn write_segment(
    store: &dyn ObjectStore,
    base: &Path,
    segment_bytes: Vec<u8>,
) -> Result<String> {
    let seg_name = format!("segment-{}.seg", Uuid::new_v4());
    let path = base.child("segments").child(seg_name.as_str());
    store
        .put(&path, PutPayload::from(Bytes::from(segment_bytes)))
        .await
        .with_context(|| format!("uploading segment to {path}"))?;
    Ok(path.to_string())
}

/// Writes a manifest and returns its path.
pub async fn write_manifest(
    store: &dyn ObjectStore,
    base: &Path,
    manifest: &Manifest,
) -> Result<String> {
    let name = format!("manifest-{}.json", Uuid::new_v4());
    let path = base.child("manifests").child(name.as_str());
    write_json(store, &path, manifest).await?;
    Ok(path.to_string())
}

/// Writes a manifest list and returns its path.
pub async fn write_manifest_list(
    store: &dyn ObjectStore,
    base: &Path,
    manifest_list: &ManifestList,
) -> Result<String> {
    let name = format!("manifest-list-{}.json", Uuid::new_v4());
    let path = base.child("manifest-lists").child(name.as_str());
    write_json(store, &path, manifest_list).await?;
    Ok(path.to_string())
}

/// Writes a new metadata file and returns its path.
pub async fn write_metadata(
    store: &dyn ObjectStore,
    base: &Path,
    metadata: &Metadata,
) -> Result<String> {
    let name = format!("metadata-{}.json", Uuid::new_v4());
    let path = base.child("metadata").child(name.as_str());
    write_json(store, &path, metadata).await?;
    Ok(path.to_string())
}

/// Checks whether `current.json` exists at the table base path.
pub async fn current_exists(store: &dyn ObjectStore, base: &Path) -> Result<bool> {
    let path = base.child("metadata").child("current.json");
    match store.get_opts(&path, GetOptions::default()).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(e) => Err(e).context("checking current.json existence"),
    }
}

/// Loads raw bytes from object storage (used for segments).
pub async fn load_bytes(store: &dyn ObjectStore, path: &str) -> Result<Bytes> {
    let path = Path::from(path);
    let result = store
        .get(&path)
        .await
        .with_context(|| format!("fetching {path}"))?;
    result
        .bytes()
        .await
        .with_context(|| format!("reading bytes from {path}"))
}

/// Computes a deterministic batch_id from a sorted list of file paths.
/// Format: `sha256:{hex}`.
pub fn compute_batch_id(file_paths: &[&str]) -> String {
    let mut sorted: Vec<&str> = file_paths.to_vec();
    sorted.sort();
    let mut hasher = Sha256::new();
    for path in &sorted {
        hasher.update(path.as_bytes());
        hasher.update(b"\n");
    }
    let hash = hasher.finalize();
    format!("sha256:{:x}", hash)
}

/// Parses a location URL into an ObjectStore + base Path.
///
/// Supports `file://`, `s3://`, `gs://`, `az://` schemes via object_store's
/// URL parsing. For local filesystem, use `file:///absolute/path/`.
pub fn parse_location(location: &str) -> Result<(Arc<dyn ObjectStore>, Path)> {
    let url: url::Url = location.parse().context("parsing location URL")?;
    let (store, path) = object_store::parse_url(&url).context("creating object store from URL")?;
    Ok((Arc::from(store), path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn json_round_trip() {
        let store = InMemory::new();
        let path = Path::from("test/data.json");
        let value = CurrentPointer {
            metadata_path: "meta.json".to_owned(),
            updated_at: "2026-01-01T00:00:00Z".to_owned(),
        };
        write_json(&store, &path, &value).await.unwrap();
        let result: ReadResult<CurrentPointer> = read_json(&store, &path).await.unwrap();
        assert_eq!(result.value, value);
        assert!(result.e_tag.is_some());
    }

    #[test]
    fn batch_id_deterministic() {
        let id1 = compute_batch_id(&["b.parquet", "a.parquet"]);
        let id2 = compute_batch_id(&["a.parquet", "b.parquet"]);
        assert_eq!(id1, id2, "batch_id should be order-independent");
        assert!(id1.starts_with("sha256:"));
    }

    #[test]
    fn batch_id_differs_for_different_files() {
        let id1 = compute_batch_id(&["a.parquet"]);
        let id2 = compute_batch_id(&["b.parquet"]);
        assert_ne!(id1, id2);
    }

    #[tokio::test]
    async fn current_exists_false_when_missing() {
        let store = InMemory::new();
        let base = Path::from("table");
        assert!(!current_exists(&store, &base).await.unwrap());
    }

    #[tokio::test]
    async fn current_exists_true_after_write() {
        let store = InMemory::new();
        let base = Path::from("table");
        let ptr = CurrentPointer {
            metadata_path: "m.json".to_owned(),
            updated_at: "now".to_owned(),
        };
        write_json(&store, &base.child("metadata").child("current.json"), &ptr)
            .await
            .unwrap();
        assert!(current_exists(&store, &base).await.unwrap());
    }
}
