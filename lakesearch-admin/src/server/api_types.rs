use lakesearch_core::metadata::ColumnStatus;
use serde::{Deserialize, Serialize};

// --- Health ---

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
}

// --- Tables ---

#[derive(Debug, Deserialize)]
pub struct CreateTableRequest {
    pub table_name: String,
    pub location: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    #[serde(default = "default_tokenizer")]
    pub tokenizer: String,
}

fn default_tokenizer() -> String {
    lakesearch_core::tokenizer::DEFAULT_TOKENIZER.to_owned()
}

#[derive(Debug, Serialize)]
pub struct CreateTableResponse {
    pub table_id: String,
    pub location: String,
}

#[derive(Debug, Serialize)]
pub struct TableInfoResponse {
    pub table_name: String,
    pub location: String,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub tokenizer: String,
    pub status: ColumnStatus,
}

#[derive(Debug, Serialize)]
pub struct ListTablesResponse {
    pub tables: Vec<TableSummary>,
}

#[derive(Debug, Serialize)]
pub struct TableSummary {
    pub table_name: String,
    pub location: String,
}

#[derive(Debug, Serialize)]
pub struct DeleteTableResponse {
    pub table_name: String,
    pub message: String,
}

// --- Columns ---

#[derive(Debug, Deserialize)]
pub struct UpdateColumnsRequest {
    /// Columns to add. Ignored if the column name already exists.
    #[serde(default)]
    pub add: Vec<ColumnDef>,
    /// Column names to drop (soft-delete: sets status to `dropped`).
    #[serde(default)]
    pub drop: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct UpdateColumnsResponse {
    pub columns: Vec<ColumnInfo>,
}

// --- Ingest ---

#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    /// Parquet file paths to index.
    pub files: Vec<String>,
    /// If provided, only push tasks for these columns. Otherwise, all active columns.
    #[serde(default)]
    pub columns: Vec<String>,
}

/// Task payload pushed to cascadq for indexer workers.
#[derive(Debug, Serialize)]
pub struct IndexTaskPayload {
    pub table_location: String,
    pub files: Vec<String>,
    pub column: String,
}

#[derive(Debug, Serialize)]
pub struct IngestResponse {
    pub tasks_pushed: usize,
}

// --- Backfill ---

#[derive(Debug, Deserialize)]
pub struct StartBackfillRequest {
    pub column: String,
}

#[derive(Debug, Serialize)]
pub struct StartBackfillResponse {
    pub column: String,
    pub status: String,
    pub manifest_lists_snapshot: usize,
}

#[derive(Debug, Serialize)]
pub struct BackfillStatusResponse {
    pub column: String,
    pub status: ColumnStatus,
    pub total_files: usize,
    pub indexed_files: usize,
    pub uncovered_files: usize,
    pub progress_pct: f64,
}
