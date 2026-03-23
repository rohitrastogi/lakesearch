use serde::{Deserialize, Serialize};

// --- Search ---

#[derive(Debug, Deserialize)]
pub struct SearchRequest {
    #[serde(default)]
    pub select: Vec<String>,
    pub search: SearchClause,
    pub limit: Option<usize>,
    /// Scoring mode: "none" (default), "indexed" (score indexed results
    /// only), or "all" (also score un-indexed files using aggregate stats).
    #[serde(default)]
    pub score: ScoreMode,
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

/// API-level score mode (serde-enabled). Maps to `crate::ScoreMode`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ScoreMode {
    #[default]
    None,
    Indexed,
    All,
}

impl From<ScoreMode> for crate::ScoreMode {
    fn from(m: ScoreMode) -> Self {
        match m {
            ScoreMode::None => Self::None,
            ScoreMode::Indexed => Self::Indexed,
            ScoreMode::All => Self::All,
        }
    }
}

impl From<OperatorStr> for crate::Operator {
    fn from(op: OperatorStr) -> Self {
        match op {
            OperatorStr::And => Self::And,
            OperatorStr::Or => Self::Or,
        }
    }
}

/// Search response: rows are JSON maps produced from Arrow RecordBatches.
#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResponse {
    pub rows: Vec<serde_json::Map<String, serde_json::Value>>,
    pub stats: SearchStats,
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
