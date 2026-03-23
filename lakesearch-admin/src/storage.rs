//! Object storage helpers for the admin service.
//!
//! Mirrors the subset of `lakesearch-query/src/storage.rs` needed for
//! table management. Admin does not depend on lakesearch-query.

use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::path::Path;
use object_store::{GetOptions, ObjectStore, PutPayload};
use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

use lakesearch_core::metadata::{CurrentPointer, ManifestList, Metadata};

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
    let result = store
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
    let result: ReadResult<Metadata> = read_json(store, &path).await?;
    Ok(result.value)
}

/// Reads a manifest list from its path.
pub async fn read_manifest_list(store: &dyn ObjectStore, path: &str) -> Result<ManifestList> {
    let path = Path::from(path);
    let result: ReadResult<ManifestList> = read_json(store, &path).await?;
    Ok(result.value)
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

/// Parses a location URL into an ObjectStore + base Path.
pub fn parse_location(location: &str) -> Result<(Arc<dyn ObjectStore>, Path)> {
    let url: url::Url = location.parse().context("parsing location URL")?;
    let (store, path) = object_store::parse_url(&url).context("creating object store from URL")?;
    Ok((Arc::from(store), path))
}
