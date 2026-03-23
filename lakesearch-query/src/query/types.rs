//! Public types for query results and statistics.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use lakesearch_core::types::DocTableEntry;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct QueryStats {
    pub candidate_pages: usize,
    pub rows_scanned: usize,
    pub rows_matched: usize,
}

impl QueryStats {
    /// Merges another stats into self by summing all counters.
    pub fn merge(&mut self, other: &QueryStats) {
        self.candidate_pages += other.candidate_pages;
        self.rows_scanned += other.rows_scanned;
        self.rows_matched += other.rows_matched;
    }
}

/// Return type for callers that need collected results (HTTP JSON, CLI).
pub struct CollectedQueryResult {
    pub batches: Vec<RecordBatch>,
    pub stats: QueryStats,
}

/// A unit of work for the indexed consumer: one (file, row_group) group
/// with its doc table entries and segment-level scoring context.
pub(crate) struct IndexedWorkItem {
    pub file_path: String,
    pub rg_idx: u16,
    /// Doc table entries for candidate pages, sorted by `first_row_index`
    /// and deduplicated.
    pub entries: Vec<DocTableEntry>,
    /// Per-segment average document length for BM25.
    pub avg_dl: f64,
    /// Per-segment total document count for BM25.
    pub total_rows: u64,
    /// Per-segment (term, doc_frequency) pairs for BM25 scoring.
    pub term_infos: Arc<Vec<(String, u32)>>,
}
