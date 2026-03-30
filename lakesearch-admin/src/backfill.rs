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

#[cfg(test)]
mod tests {
    use super::*;
    use lakesearch_core::metadata::*;
    use lakesearch_core::storage::write_json;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    fn test_metadata(manifest_lists: Vec<String>) -> Metadata {
        Metadata {
            format_version: 1,
            table_id: "test".to_owned(),
            table_name: "test".to_owned(),
            location: "mem://test/".to_owned(),
            indexed_columns: vec![IndexedColumn {
                name: "desc".to_owned(),
                tokenizer: "whitespace_lowercase".to_owned(),
                status: ColumnStatus::Active,
                backfill_manifest_lists: None,
            }],
            snapshot: Snapshot {
                timestamp_ms: 1000,
                manifest_lists,
            },
        }
    }

    fn test_manifest_list(files: &[&str], manifests: Vec<ManifestEntry>) -> ManifestList {
        ManifestList {
            job_kind: JobKind::Append,
            batch_id: "sha256:test".to_owned(),
            data_files: files
                .iter()
                .map(|f| DataFileEntry {
                    path: f.to_string(),
                    file_size_bytes: 1024,
                    row_count: 100,
                })
                .collect(),
            manifests,
            replaces: None,
            compacted_column: None,
        }
    }

    #[tokio::test]
    async fn uncovered_files_are_sorted() {
        let store = InMemory::new();
        let base = Path::from("table");

        // Create manifest list with files in non-alphabetical order
        let ml = test_manifest_list(&["c.parquet", "a.parquet", "b.parquet"], vec![]);
        let ml_path = base.child("manifest-lists").child("ml-1.json");
        write_json(&store, &ml_path, &ml).await.unwrap();

        let metadata = test_metadata(vec![]);

        let result = find_uncovered_files(&store, &metadata, "desc", &[ml_path.to_string()], 4)
            .await
            .unwrap();

        assert_eq!(result.total_files, 3);
        assert_eq!(result.uncovered.len(), 3);
        // Verify sorted order
        assert_eq!(
            result.uncovered,
            vec!["a.parquet", "b.parquet", "c.parquet"]
        );
    }

    #[tokio::test]
    async fn uncovered_excludes_indexed_files() {
        let store = InMemory::new();
        let base = Path::from("table");

        // Frozen snapshot: 3 files
        let ml = test_manifest_list(&["a.parquet", "b.parquet", "c.parquet"], vec![]);
        let ml_path = base.child("manifest-lists").child("ml-frozen.json");
        write_json(&store, &ml_path, &ml).await.unwrap();

        // Current manifest list: has a manifest that covers a.parquet
        let manifest = Manifest {
            indexed_column: "desc".to_owned(),
            segments: vec![SegmentInfo {
                segment_path: "seg-1.seg".to_owned(),
                size_bytes: 100,
                term_count: 10,
                doc_count: 5,
                total_rows: 100,
                total_tokens: 500,
                parquet_files: vec![ParquetFileRef {
                    file_ordinal: 0,
                    path: "a.parquet".to_owned(),
                    file_size_bytes: 1024,
                    row_group_count: 1,
                }],
            }],
        };
        let manifest_path = base.child("manifests").child("m-1.json");
        write_json(&store, &manifest_path, &manifest).await.unwrap();

        let current_ml = test_manifest_list(
            &["a.parquet", "b.parquet", "c.parquet"],
            vec![ManifestEntry {
                manifest_path: manifest_path.to_string(),
                indexed_column: "desc".to_owned(),
                segment_count: 1,
                term_stats: TermStats {
                    min_term: "a".to_owned(),
                    max_term: "z".to_owned(),
                    term_count: 10,
                },
            }],
        );
        let current_ml_path = base.child("manifest-lists").child("ml-current.json");
        write_json(&store, &current_ml_path, &current_ml)
            .await
            .unwrap();

        let metadata = test_metadata(vec![current_ml_path.to_string()]);

        let result = find_uncovered_files(&store, &metadata, "desc", &[ml_path.to_string()], 4)
            .await
            .unwrap();

        assert_eq!(result.total_files, 3);
        assert_eq!(result.indexed_files, 1);
        assert_eq!(result.uncovered, vec!["b.parquet", "c.parquet"]);
    }

    #[tokio::test]
    async fn empty_snapshot_yields_zero_uncovered() {
        let store = InMemory::new();
        let metadata = test_metadata(vec![]);

        let result = find_uncovered_files(&store, &metadata, "desc", &[], 4)
            .await
            .unwrap();

        assert_eq!(result.total_files, 0);
        assert_eq!(result.indexed_files, 0);
        assert!(result.uncovered.is_empty());
    }
}
