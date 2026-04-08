//! Public types for query results and statistics.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use lakesearch_core::types::DocTableEntry;
use serde::{Deserialize, Serialize};

use lakesearch_core::tokenizer::QueryTerm;

use crate::Operator;

/// Output column name for the matched text.
pub(crate) const TEXT_COL: &str = "text";
/// Output column name for the BM25 score.
pub(crate) const SCORE_COL: &str = "score";

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

/// Whether to use the ilike pre-filter (brute-force) or direct
/// tokenization (indexed).
#[derive(Clone, Copy)]
pub(crate) enum VerifyMode {
    Indexed,
    BruteForce,
}

/// Per-file projection context, shared across all batches from the same file.
pub(crate) struct FileProjection {
    pub indexed_batch_col: usize,
    pub select_col_map: Arc<[(usize, String)]>,
    pub is_large: bool,
}

/// A single RecordBatch ready for CPU verification.
pub(crate) struct CpuWorkItem {
    pub batch: RecordBatch,
    pub mode: VerifyMode,
    /// Whether to compute BM25 scores for this batch.
    pub with_score: bool,
    /// Shared across all batches from the same file.
    pub file_proj: Arc<FileProjection>,
    pub avg_dl: f64,
    pub total_rows: u64,
    pub term_infos: Arc<Vec<(String, u32)>>,
}

/// Aggregate BM25 scoring context for brute-force files.
pub(crate) struct BruteForceScoring {
    pub total_rows: u64,
    pub avg_dl: f64,
    pub term_infos: Arc<Vec<(String, u32)>>,
}

/// Query-wide context shared across all batches via `Arc`.
pub(crate) struct SharedQueryContext {
    pub query_terms: Arc<Vec<QueryTerm>>,
    pub operator: Operator,
    pub with_score: bool,
    pub schema: SchemaRef,
}
