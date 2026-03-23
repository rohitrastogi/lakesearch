pub mod cas;
pub mod object_cache;
pub mod parquet_util;
pub mod query;
pub mod server;
pub mod storage;

/// Boolean operator for combining query terms.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operator {
    And,
    Or,
}

/// How to score query results.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScoreMode {
    /// No scoring.
    None,
    /// Score indexed results only. Brute-force matches are unscored.
    Indexed,
    /// Score all results. Un-indexed files use aggregate stats from
    /// indexed segments for approximate BM25.
    All,
}
