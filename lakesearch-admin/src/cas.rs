//! CAS (compare-and-swap) commit protocol for metadata updates.
//!
//! Mirrors `lakesearch-query/src/cas.rs`. Admin needs CAS for column
//! updates and backfill transitions.

use anyhow::{bail, Context, Result};
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion};
use tracing::{info, warn};

use lakesearch_core::metadata::{CurrentPointer, Metadata};

use crate::storage::{read_current, read_metadata, write_metadata};

const MAX_RETRIES: usize = 5;
const BASE_BACKOFF_MS: u64 = 50;

/// Commits a metadata update using CAS on `current.json`.
///
/// `build_new_metadata` is called on each retry to rebase against the
/// latest state.
pub async fn commit_metadata<F>(
    store: &dyn ObjectStore,
    base: &Path,
    current_etag: Option<String>,
    current_metadata: &Metadata,
    build_new_metadata: F,
) -> Result<()>
where
    F: Fn(&Metadata) -> Metadata,
{
    let mut etag = current_etag;
    let mut latest = current_metadata.clone();

    for attempt in 0..MAX_RETRIES {
        let new_metadata = build_new_metadata(&latest);

        let metadata_path = write_metadata(store, base, &new_metadata).await?;

        let pointer = CurrentPointer {
            metadata_path,
            updated_at: chrono::Utc::now().to_rfc3339(),
        };
        let json = serde_json::to_vec_pretty(&pointer).context("serializing current pointer")?;
        let current_path = base.child("metadata").child("current.json");

        let put_opts = match &etag {
            Some(tag) => PutOptions {
                mode: PutMode::Update(UpdateVersion {
                    e_tag: Some(tag.clone()),
                    version: None,
                }),
                ..PutOptions::default()
            },
            None => PutOptions::default(),
        };

        let result = store
            .put_opts(&current_path, PutPayload::from(json.clone()), put_opts)
            .await;

        // LocalFileSystem fallback for conditional PUT
        let result = match &result {
            Err(object_store::Error::NotImplemented) if etag.is_some() => {
                warn!("conditional PUT not supported, falling back to unconditional write");
                store
                    .put(&current_path, PutPayload::from(json))
                    .await
                    .map(|_| ())
            }
            _ => result.map(|_| ()),
        };

        match result {
            Ok(()) => {
                info!(attempt, "metadata committed successfully");
                return Ok(());
            }
            Err(object_store::Error::Precondition { .. }) => {
                warn!(attempt, "CAS conflict, rebasing");
                let backoff = BASE_BACKOFF_MS * 2u64.pow(attempt as u32) + rand_jitter();
                tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;

                let result = read_current(store, base).await?;
                etag = result.e_tag;
                latest = read_metadata(store, &result.value).await?;
            }
            Err(e) => return Err(e).context("writing current.json"),
        }
    }

    bail!("CAS commit failed after {MAX_RETRIES} retries")
}

fn rand_jitter() -> u64 {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    t.subsec_nanos() as u64 % 50
}
