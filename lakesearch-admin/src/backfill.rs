//! Backfill logic: frozen-snapshot reconciliation.
//!
//! The key insight is that a backfill targets a fixed set of manifest lists
//! captured at the moment `POST /backfill` is called. New files arriving via
//! ingest after the snapshot are ignored by the backfill, so "uncovered == 0"
//! is a reachable terminal state.

use std::collections::HashSet;

use anyhow::Result;
use futures::stream::{self, StreamExt, TryStreamExt};
use object_store::ObjectStore;

use lakesearch_core::metadata::{ManifestList, Metadata};

use lakesearch_core::storage::{read_manifest, read_manifest_list};

/// Files that exist in the frozen backfill snapshot but are not yet indexed
/// for the given column.
pub struct UncoveredFiles {
    /// Total files in the frozen snapshot.
    pub total_files: usize,
    /// Files already indexed for this column.
    pub indexed_files: usize,
    /// Files not yet indexed.
    pub uncovered: Vec<String>,
}

/// Walks the frozen manifest lists to find files not yet indexed for `column`.
///
/// 1. Load `backfill_manifest_lists` → union of `data_files` → target file set
/// 2. Load ALL current manifest lists → walk manifests for `column` → indexed file set
/// 3. Return target - indexed
pub async fn find_uncovered_files(
    store: &dyn ObjectStore,
    metadata: &Metadata,
    column: &str,
    backfill_manifest_lists: &[String],
    io_concurrency: usize,
) -> Result<UncoveredFiles> {
    // Step 1: target files from frozen snapshot
    let frozen_mls: Vec<ManifestList> = stream::iter(backfill_manifest_lists.iter().cloned())
        .map(|path| async move { read_manifest_list(store, &path).await })
        .buffered(io_concurrency)
        .try_collect()
        .await?;

    let mut target_files: HashSet<String> = HashSet::new();
    for ml in &frozen_mls {
        for df in &ml.data_files {
            target_files.insert(df.path.clone());
        }
    }

    let total_files = target_files.len();

    // Step 2: indexed files from ALL current manifest lists
    let current_mls: Vec<ManifestList> =
        stream::iter(metadata.snapshot.manifest_lists.iter().cloned())
            .map(|path| async move { read_manifest_list(store, &path).await })
            .buffered(io_concurrency)
            .try_collect()
            .await?;

    // Collect manifest paths for the target column
    let mut col_manifest_paths: Vec<String> = Vec::new();
    for ml in &current_mls {
        for me in &ml.manifests {
            if me.indexed_column == column {
                col_manifest_paths.push(me.manifest_path.clone());
            }
        }
    }

    // Load column manifests
    let col_manifests = stream::iter(col_manifest_paths.into_iter())
        .map(|path| async move { read_manifest(store, &path).await })
        .buffered(io_concurrency)
        .try_collect::<Vec<_>>()
        .await?;

    let mut indexed: HashSet<String> = HashSet::new();
    for manifest in &col_manifests {
        for seg in &manifest.segments {
            for pf in &seg.parquet_files {
                indexed.insert(pf.path.clone());
            }
        }
    }

    // Sort for deterministic chunking: prevents overlapping batches when
    // the reconciliation loop re-derives uncovered files across polls.
    let mut uncovered: Vec<String> = target_files.difference(&indexed).cloned().collect();
    uncovered.sort();
    let indexed_files = total_files - uncovered.len();

    Ok(UncoveredFiles {
        total_files,
        indexed_files,
        uncovered,
    })
}
