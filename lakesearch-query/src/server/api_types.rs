use serde::{Deserialize, Serialize};

// --- Search ---

#[derive(Debug, Deserialize)]
pub struct SearchRequest {
    #[serde(default)]
    pub select: Vec<String>,
    pub search: SearchClause,
    pub limit: Option<usize>,
    #[serde(default)]
    pub score: bool,
}

#[derive(Debug, Deserialize)]
pub struct SearchClause {
    pub column: String,
    #[serde(rename = "match")]
    pub match_text: String,
    #[serde(default = "default_operator")]
    pub operator: OperatorStr,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OperatorStr {
    And,
    Or,
}

fn default_operator() -> OperatorStr {
    OperatorStr::Or
}

impl From<OperatorStr> for crate::Operator {
    fn from(op: OperatorStr) -> Self {
        match op {
            OperatorStr::And => Self::And,
            OperatorStr::Or => Self::Or,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResponse {
    pub rows: Vec<SearchRow>,
    pub stats: SearchStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchRow {
    pub file: String,
    pub row_group: u16,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchStats {
    pub candidate_pages: usize,
    pub rows_scanned: usize,
    pub rows_matched: usize,
    pub elapsed_ms: u64,
}

// --- Tables ---

#[derive(Debug, Serialize, Deserialize)]
pub struct ListTablesResponse {
    pub tables: Vec<TableInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub location: String,
    pub indexed_columns: Vec<String>,
}

// --- Health ---

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
}
