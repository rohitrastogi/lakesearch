//! Metadata protocol types for the LakeSearch object storage layer.
//!
//! These are pure serde structs with no I/O. Service crates serialize them
//! to/from JSON in object storage. See DESIGN.md § Metadata Protocol.

use serde::{Deserialize, Serialize};

/// Pointer to the current metadata file. The only mutable object in a table.
/// Updated atomically via conditional PUT (CAS on ETag).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CurrentPointer {
    pub metadata_path: String,
    pub updated_at: String,
}

/// Column indexing status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnStatus {
    Active,
    Backfilling,
    Dropped,
}

/// A column configured for full-text indexing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexedColumn {
    pub name: String,
    pub tokenizer: String,
    pub status: ColumnStatus,
    /// Frozen manifest list refs for an in-progress backfill.
    /// Present only when `status == Backfilling`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backfill_manifest_lists: Option<Vec<String>>,
}

/// Snapshot of a table's index state at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Snapshot {
    pub timestamp_ms: u64,
    pub manifest_lists: Vec<String>,
}

/// Immutable metadata file describing a table and its current index state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Metadata {
    pub format_version: u32,
    pub table_id: String,
    pub table_name: String,
    pub location: String,
    pub indexed_columns: Vec<IndexedColumn>,
    pub snapshot: Snapshot,
}

/// Kind of indexing job that produced a manifest list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    Append,
    Compact,
}

/// A Parquet data file referenced by a manifest list.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataFileEntry {
    pub path: String,
    pub file_size_bytes: u64,
    pub row_count: u64,
}

/// Summary statistics for terms in a manifest's segments.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TermStats {
    pub min_term: String,
    pub max_term: String,
    pub term_count: u64,
}

/// Reference to a manifest within a manifest list.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestEntry {
    pub manifest_path: String,
    pub indexed_column: String,
    pub segment_count: u32,
    pub term_stats: TermStats,
}

/// Groups manifests written together in one indexing operation.
///
/// `replaces` and `compacted_column` are only present for `compact` jobs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestList {
    pub job_kind: JobKind,
    pub batch_id: String,
    pub data_files: Vec<DataFileEntry>,
    pub manifests: Vec<ManifestEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replaces: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compacted_column: Option<String>,
}

/// Reference to a Parquet file within a segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParquetFileRef {
    pub file_ordinal: u32,
    pub path: String,
    pub file_size_bytes: u64,
    pub row_group_count: u16,
}

/// Information about a segment within a manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentInfo {
    pub segment_path: String,
    pub size_bytes: u64,
    pub term_count: u64,
    pub doc_count: u64,
    pub total_rows: u64,
    pub total_tokens: u64,
    pub parquet_files: Vec<ParquetFileRef>,
}

/// Maps segment files to the Parquet files they index, for a single column.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub indexed_column: String,
    pub segments: Vec<SegmentInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_pointer_round_trip() {
        let ptr = CurrentPointer {
            metadata_path: "s3://bucket/metadata/metadata-abc.json".to_owned(),
            updated_at: "2026-03-22T17:00:00Z".to_owned(),
        };
        let json = serde_json::to_string(&ptr).unwrap();
        let back: CurrentPointer = serde_json::from_str(&json).unwrap();
        assert_eq!(ptr, back);
    }

    #[test]
    fn column_status_snake_case() {
        assert_eq!(
            serde_json::to_string(&ColumnStatus::Active).unwrap(),
            "\"active\""
        );
        assert_eq!(
            serde_json::to_string(&ColumnStatus::Backfilling).unwrap(),
            "\"backfilling\""
        );
        assert_eq!(
            serde_json::to_string(&ColumnStatus::Dropped).unwrap(),
            "\"dropped\""
        );
        let back: ColumnStatus = serde_json::from_str("\"active\"").unwrap();
        assert_eq!(back, ColumnStatus::Active);
    }

    #[test]
    fn job_kind_snake_case() {
        assert_eq!(
            serde_json::to_string(&JobKind::Append).unwrap(),
            "\"append\""
        );
        assert_eq!(
            serde_json::to_string(&JobKind::Compact).unwrap(),
            "\"compact\""
        );
    }

    #[test]
    fn metadata_round_trip() {
        let meta = Metadata {
            format_version: 1,
            table_id: "550e8400-e29b-41d4-a716-446655440000".to_owned(),
            table_name: "events".to_owned(),
            location: "s3://bucket/lakesearch/tables/events/".to_owned(),
            indexed_columns: vec![IndexedColumn {
                name: "description".to_owned(),
                tokenizer: crate::tokenizer::DEFAULT_TOKENIZER.to_owned(),
                status: ColumnStatus::Active,
                backfill_manifest_lists: None,
            }],
            snapshot: Snapshot {
                timestamp_ms: 1711100000000,
                manifest_lists: vec!["s3://bucket/manifest-lists/ml-aaa.json".to_owned()],
            },
        };
        let json = serde_json::to_string_pretty(&meta).unwrap();
        let back: Metadata = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, back);
    }

    #[test]
    fn manifest_list_append_round_trip() {
        let ml = ManifestList {
            job_kind: JobKind::Append,
            batch_id: "sha256:abc123".to_owned(),
            data_files: vec![DataFileEntry {
                path: "s3://bucket/data/part-001.parquet".to_owned(),
                file_size_bytes: 1024,
                row_count: 100,
            }],
            manifests: vec![ManifestEntry {
                manifest_path: "s3://bucket/manifests/m-001.json".to_owned(),
                indexed_column: "description".to_owned(),
                segment_count: 1,
                term_stats: TermStats {
                    min_term: "aardvark".to_owned(),
                    max_term: "zebra".to_owned(),
                    term_count: 500,
                },
            }],
            replaces: None,
            compacted_column: None,
        };
        let json = serde_json::to_string(&ml).unwrap();
        // replaces and compacted_column should be omitted
        assert!(!json.contains("replaces"));
        assert!(!json.contains("compacted_column"));
        let back: ManifestList = serde_json::from_str(&json).unwrap();
        assert_eq!(ml, back);
    }

    #[test]
    fn manifest_list_compact_includes_replaces() {
        let ml = ManifestList {
            job_kind: JobKind::Compact,
            batch_id: "sha256:def456".to_owned(),
            data_files: vec![],
            manifests: vec![],
            replaces: Some(vec!["s3://bucket/ml-old.json".to_owned()]),
            compacted_column: Some("description".to_owned()),
        };
        let json = serde_json::to_string(&ml).unwrap();
        assert!(json.contains("replaces"));
        assert!(json.contains("compacted_column"));
        let back: ManifestList = serde_json::from_str(&json).unwrap();
        assert_eq!(ml, back);
    }

    #[test]
    fn manifest_round_trip() {
        let m = Manifest {
            indexed_column: "description".to_owned(),
            segments: vec![SegmentInfo {
                segment_path: "s3://bucket/segments/seg-xyz.seg".to_owned(),
                size_bytes: 2048,
                term_count: 100,
                doc_count: 50,
                total_rows: 1000,
                total_tokens: 5000,
                parquet_files: vec![ParquetFileRef {
                    file_ordinal: 0,
                    path: "s3://bucket/data/part-001.parquet".to_owned(),
                    file_size_bytes: 4096,
                    row_group_count: 2,
                }],
            }],
        };
        let json = serde_json::to_string_pretty(&m).unwrap();
        let back: Manifest = serde_json::from_str(&json).unwrap();
        assert_eq!(m, back);
    }
}
