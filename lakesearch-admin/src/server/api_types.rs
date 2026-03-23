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
    pub table_id: String,
    pub table_name: String,
    pub location: String,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub tokenizer: String,
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct ListTablesResponse {
    pub tables: Vec<TableSummary>,
}

#[derive(Debug, Serialize)]
pub struct TableSummary {
    pub table_id: String,
    pub table_name: String,
    pub location: String,
}

#[derive(Debug, Serialize)]
pub struct DeleteTableResponse {
    pub table_id: String,
    pub message: String,
}
