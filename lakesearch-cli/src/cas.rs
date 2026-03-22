//! CAS (compare-and-swap) commit protocol for metadata updates.
//!
//! Uses conditional PUT with ETag matching on `current.json` to ensure
//! atomic metadata transitions. On conflict, the protocol re-reads the
//! latest state, rebases the change, and retries.

use anyhow::{bail, Context, Result};
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion};
use tracing::{info, warn};

use lakesearch_core::metadata::{CurrentPointer, Metadata};

use crate::storage::{read_current, read_manifest_list, read_metadata, write_metadata};

const MAX_RETRIES: usize = 5;
const BASE_BACKOFF_MS: u64 = 50;

/// Commits a metadata update using CAS on `current.json`.
///
/// `build_new_metadata` is a closure that takes the current metadata and
/// returns the new metadata to commit. It is called on each retry to rebase
/// against the latest state.
///
/// If `batch_id` is provided, the protocol re-checks for duplicate batches
/// after each rebase. This closes the race where two workers process the
/// same batch concurrently: the loser's rebase reveals the winner's commit
/// and the loser exits cleanly instead of double-committing.
pub async fn commit_metadata<F>(
    store: &dyn ObjectStore,
    base: &Path,
    current_etag: Option<String>,
    current_metadata: &Metadata,
    batch_id: Option<&str>,
    build_new_metadata: F,
) -> Result<()>
where
    F: Fn(&Metadata) -> Metadata,
{
    let mut etag = current_etag;
    let mut latest = current_metadata.clone();

    for attempt in 0..MAX_RETRIES {
        let new_metadata = build_new_metadata(&latest);

        // Write new metadata file (UUID-named, unconditional — always unique)
        let metadata_path = write_metadata(store, base, &new_metadata).await?;

        let pointer = CurrentPointer {
            metadata_path,
            updated_at: chrono::Utc::now().to_rfc3339(),
        };
        let json = serde_json::to_vec_pretty(&pointer).context("serializing current pointer")?;
        let current_path = base.child("metadata").child("current.json");

        // Conditional PUT with ETag
        let put_opts = match &etag {
            Some(tag) => PutOptions {
                mode: PutMode::Update(UpdateVersion {
                    e_tag: Some(tag.clone()),
                    version: None,
                }),
                ..PutOptions::default()
            },
            // First write: no etag to match against, use overwrite
            None => PutOptions::default(),
        };

        let result = store
            .put_opts(&current_path, PutPayload::from(json.clone()), put_opts)
            .await;

        // LocalFileSystem doesn't support conditional PUT — fall back to
        // unconditional overwrite. This is safe for single-writer local use.
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

                // Exponential backoff with jitter
                let backoff = BASE_BACKOFF_MS * 2u64.pow(attempt as u32) + rand_jitter();
                tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;

                // Re-read latest state
                let result = read_current(store, base).await?;
                etag = result.e_tag;
                latest = read_metadata(store, &result.value).await?;

                // Re-check dedup after rebase: if a concurrent worker already
                // committed this batch, we're done.
                if let Some(bid) = batch_id {
                    if is_batch_duplicate(store, &latest, bid).await? {
                        info!(
                            batch_id = bid,
                            "batch committed by concurrent worker, skipping"
                        );
                        return Ok(());
                    }
                }
            }
            Err(e) => return Err(e).context("writing current.json"),
        }
    }

    bail!("CAS commit failed after {MAX_RETRIES} retries")
}

/// Checks whether any manifest list in the metadata has the given batch_id.
pub async fn is_batch_duplicate(
    store: &dyn ObjectStore,
    metadata: &Metadata,
    batch_id: &str,
) -> Result<bool> {
    for ml_path in &metadata.snapshot.manifest_lists {
        let ml = read_manifest_list(store, ml_path).await?;
        if ml.batch_id == batch_id {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Simple jitter: 0–49ms.
fn rand_jitter() -> u64 {
    // Use a simple source — exact randomness doesn't matter for backoff.
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    t.subsec_nanos() as u64 % 50
}

#[cfg(test)]
mod tests {
    use super::*;
    use lakesearch_core::metadata::{ColumnStatus, IndexedColumn, Snapshot};
    use object_store::memory::InMemory;

    fn test_metadata(manifest_lists: Vec<String>) -> Metadata {
        Metadata {
            format_version: 1,
            table_id: "test-table".to_owned(),
            table_name: "events".to_owned(),
            location: "mem://table/".to_owned(),
            indexed_columns: vec![IndexedColumn {
                name: "description".to_owned(),
                tokenizer: lakesearch_core::tokenizer::DEFAULT_TOKENIZER.to_owned(),
                status: ColumnStatus::Active,
            }],
            snapshot: Snapshot {
                timestamp_ms: 1000,
                manifest_lists,
            },
        }
    }

    #[tokio::test]
    async fn commit_success() {
        let store = InMemory::new();
        let base = Path::from("table");

        // Write initial current.json
        let initial = test_metadata(vec![]);
        let meta_path = write_metadata(&store, &base, &initial).await.unwrap();
        let pointer = CurrentPointer {
            metadata_path: meta_path,
            updated_at: "t0".to_owned(),
        };
        crate::storage::write_json(
            &store,
            &base.child("metadata").child("current.json"),
            &pointer,
        )
        .await
        .unwrap();

        // Read current + etag
        let result = read_current(&store, &base).await.unwrap();
        let current_meta = read_metadata(&store, &result.value).await.unwrap();

        // Commit with a new manifest list
        commit_metadata(&store, &base, result.e_tag, &current_meta, None, |meta| {
            let mut new = meta.clone();
            new.snapshot.manifest_lists.push("ml-new.json".to_owned());
            new
        })
        .await
        .unwrap();

        // Verify the commit
        let result = read_current(&store, &base).await.unwrap();
        let final_meta = read_metadata(&store, &result.value).await.unwrap();
        assert_eq!(final_meta.snapshot.manifest_lists, vec!["ml-new.json"]);
    }

    #[tokio::test]
    async fn batch_dedup_detection() {
        let store = InMemory::new();

        // Write a manifest list with a known batch_id
        let ml = lakesearch_core::metadata::ManifestList {
            job_kind: lakesearch_core::metadata::JobKind::Append,
            batch_id: "sha256:abc".to_owned(),
            data_files: vec![],
            manifests: vec![],
            replaces: None,
            compacted_column: None,
        };
        let ml_path = Path::from("table/manifest-lists/ml-1.json");
        crate::storage::write_json(&store, &ml_path, &ml)
            .await
            .unwrap();

        let meta = test_metadata(vec!["table/manifest-lists/ml-1.json".to_owned()]);

        assert!(is_batch_duplicate(&store, &meta, "sha256:abc")
            .await
            .unwrap());
        assert!(!is_batch_duplicate(&store, &meta, "sha256:different")
            .await
            .unwrap());
    }

    /// Simulates the concurrent duplicate race: worker reads stale metadata,
    /// a concurrent worker commits the same batch_id, then the first worker
    /// tries to commit. The CAS rebase should detect the duplicate and skip.
    #[tokio::test]
    async fn rebase_detects_concurrent_duplicate() {
        let store = InMemory::new();
        let base = Path::from("table");

        // Set up initial empty table
        let initial = test_metadata(vec![]);
        let meta_path = write_metadata(&store, &base, &initial)
            .await
            .expect("write initial metadata");
        let pointer = CurrentPointer {
            metadata_path: meta_path,
            updated_at: "t0".to_owned(),
        };
        crate::storage::write_json(
            &store,
            &base.child("metadata").child("current.json"),
            &pointer,
        )
        .await
        .expect("write initial current.json");

        // Worker A reads the current snapshot (empty)
        let worker_a_read = read_current(&store, &base)
            .await
            .expect("worker A read current");
        let worker_a_meta = read_metadata(&store, &worker_a_read.value)
            .await
            .expect("worker A read metadata");

        // Simulate concurrent worker B committing the same batch_id first.
        // Write a manifest list with batch_id "sha256:same"
        let ml = lakesearch_core::metadata::ManifestList {
            job_kind: lakesearch_core::metadata::JobKind::Append,
            batch_id: "sha256:same".to_owned(),
            data_files: vec![],
            manifests: vec![],
            replaces: None,
            compacted_column: None,
        };
        let ml_path = crate::storage::write_manifest_list(&store, &base, &ml)
            .await
            .expect("write manifest list for worker B");

        // Worker B commits (succeeds — no contention)
        commit_metadata(
            &store,
            &base,
            worker_a_read.e_tag.clone(),
            &worker_a_meta,
            Some("sha256:same"),
            |meta| {
                let mut new = meta.clone();
                new.snapshot.manifest_lists.push(ml_path.clone());
                new
            },
        )
        .await
        .expect("worker B commit");

        // Worker A now tries to commit with the SAME batch_id but stale etag.
        // It will hit CAS conflict, rebase, detect the duplicate, and skip.
        let result = commit_metadata(
            &store,
            &base,
            worker_a_read.e_tag,
            &worker_a_meta,
            Some("sha256:same"),
            |meta| {
                let mut new = meta.clone();
                new.snapshot
                    .manifest_lists
                    .push("should-not-appear".to_owned());
                new
            },
        )
        .await;

        // Should succeed (skip, not error)
        result.expect("worker A should succeed via dedup skip");

        // Verify: only ONE manifest list in the final state (worker B's)
        let final_read = read_current(&store, &base)
            .await
            .expect("read final current");
        let final_meta = read_metadata(&store, &final_read.value)
            .await
            .expect("read final metadata");
        assert_eq!(
            final_meta.snapshot.manifest_lists.len(),
            1,
            "should have exactly 1 manifest list, not 2"
        );
        assert!(
            final_meta.snapshot.manifest_lists[0].contains("manifest-list-"),
            "should be worker B's manifest list"
        );
    }
}
