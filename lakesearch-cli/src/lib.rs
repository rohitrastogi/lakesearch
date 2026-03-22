pub mod cas;
pub mod index;
pub mod parquet_util;
pub mod query;
pub mod storage;

/// Boolean operator for combining query terms.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operator {
    And,
    Or,
}
