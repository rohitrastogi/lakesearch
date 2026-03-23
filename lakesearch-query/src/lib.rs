pub mod cas;
pub mod object_cache;
pub mod parquet_util;
pub mod query;
pub mod server;
pub mod storage;

use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::Stream;

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

/// A stream of `RecordBatch`es with a known schema.
pub trait RecordBatchStream: Stream<Item = Result<RecordBatch, anyhow::Error>> {
    fn schema(&self) -> SchemaRef;
}

/// Pinned, boxed, sendable `RecordBatchStream`.
pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;

/// Wraps a `tokio::sync::mpsc::Receiver<Result<RecordBatch>>` as a
/// `RecordBatchStream`.
pub struct QueryResultStream {
    schema: SchemaRef,
    rx: tokio::sync::mpsc::Receiver<Result<RecordBatch, anyhow::Error>>,
}

impl QueryResultStream {
    pub fn new(
        schema: SchemaRef,
        rx: tokio::sync::mpsc::Receiver<Result<RecordBatch, anyhow::Error>>,
    ) -> Self {
        Self { schema, rx }
    }
}

impl Stream for QueryResultStream {
    type Item = Result<RecordBatch, anyhow::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl RecordBatchStream for QueryResultStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
