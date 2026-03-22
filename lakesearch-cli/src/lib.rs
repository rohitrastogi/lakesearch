pub mod cas;
pub mod parquet_util;
pub mod remote_index;
pub mod remote_query;
pub mod storage;

/// Boolean operator for combining query terms.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operator {
    And,
    Or,
}
