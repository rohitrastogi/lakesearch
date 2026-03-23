//! Async multi-segment query execution.
//!
//! The query pipeline has three phases:
//! 1. **Setup** — plan query, evaluate segments, group candidates, collect stats
//! 2. **Execute** — two concurrent consumers (indexed + brute-force) stream
//!    RecordBatches through a shared channel
//! 3. **Output** — forward stream directly (non-top-K) or accumulate + rank (top-K)

mod evaluate;
mod plan;
mod rank;
pub mod types;
mod verify;

pub use types::{CollectedQueryResult, QueryStats};

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use tracing::info;

use lakesearch_core::bm25;
use lakesearch_core::runtime::LakeRuntime;
use lakesearch_core::segment::SegmentReader;
use lakesearch_core::tokenizer::tokenize;
use object_store::path::Path;
use parquet::file::metadata::ParquetMetaData;

use crate::object_cache::ObjectCache;
use crate::{Operator, QueryResultStream, SendableRecordBatchStream};

use evaluate::{evaluate_segment, group_candidates};
use plan::{plan_query, resolve_schema};
use rank::rank_batches;
use verify::{brute_force_consumer, indexed_consumer};

// ---------------------------------------------------------------------------
// Schema construction
// ---------------------------------------------------------------------------

/// Builds the output schema from parquet metadata.
///
/// Layout: `text (Utf8)` + select columns (real types) + `score (Float64)`.
fn build_result_schema(
    pq_meta: &ParquetMetaData,
    column: &str,
    select_columns: &[String],
    with_score: bool,
) -> Result<SchemaRef> {
    let arrow_schema = parquet::arrow::parquet_to_arrow_schema(
        pq_meta.file_metadata().schema_descr(),
        pq_meta.file_metadata().key_value_metadata(),
    )
    .context("converting parquet schema to arrow")?;

    let mut fields = vec![Field::new("text", DataType::Utf8, true)];

    for sel in select_columns {
        if sel == column {
            continue;
        }
        let field = arrow_schema
            .field_with_name(sel)
            .with_context(|| format!("select column '{sel}' not found in parquet schema"))?;
        fields.push(field.clone());
    }

    if with_score {
        fields.push(Field::new("score", DataType::Float64, true));
    }

    Ok(Arc::new(Schema::new(fields)))
}

/// Builds a fallback schema when no parquet metadata is available (empty table).
fn build_empty_schema(select_columns: &[String], column: &str, with_score: bool) -> SchemaRef {
    let mut fields = vec![Field::new("text", DataType::Utf8, true)];
    for sel in select_columns {
        if sel == column {
            continue;
        }
        fields.push(Field::new(sel, DataType::Utf8, true));
    }
    if with_score {
        fields.push(Field::new("score", DataType::Float64, true));
    }
    Arc::new(Schema::new(fields))
}

// ---------------------------------------------------------------------------
// Aggregate stats (for BM25 scoring)
// ---------------------------------------------------------------------------

pub(crate) struct AggregateStats {
    pub total_rows: u64,
    pub total_tokens: u64,
    pub term_df: HashMap<String, u64>,
}

fn collect_aggregate_stats(segments: &[SegmentReader], query_terms: &[String]) -> AggregateStats {
    let mut total_rows = 0u64;
    let mut total_tokens = 0u64;
    let mut term_df: HashMap<String, u64> = HashMap::new();

    for reader in segments {
        let cs = reader.corpus_stats();
        total_rows += cs.total_rows;
        total_tokens += cs.total_tokens;
        for term in query_terms {
            if let Some(ord) = reader.term_ordinal(term) {
                if let Ok(info) = reader.term_info(ord) {
                    *term_df.entry(term.clone()).or_default() += info.doc_frequency as u64;
                }
            }
        }
    }

    AggregateStats {
        total_rows,
        total_tokens,
        term_df,
    }
}

// ---------------------------------------------------------------------------
// Setup: shared between run_query and run_query_collected
// ---------------------------------------------------------------------------

/// Spawns indexed and brute-force consumers, returning their join handles.
/// The caller must drop the original `tx` so the channel closes when both
/// consumers finish.
#[allow(clippy::too_many_arguments)]
fn spawn_consumers(
    work_items: Vec<types::IndexedWorkItem>,
    unindexed_files: Vec<String>,
    cache: Arc<ObjectCache>,
    column: String,
    query_terms: &[String],
    operator: Operator,
    score_mode: crate::ScoreMode,
    with_score: bool,
    limit: Option<usize>,
    select_columns: &[String],
    agg: &AggregateStats,
    schema: SchemaRef,
    io_concurrency: usize,
    runtime: Arc<LakeRuntime>,
    tx: tokio::sync::mpsc::Sender<Result<RecordBatch>>,
) -> (
    Option<tokio::task::JoinHandle<QueryStats>>,
    Option<tokio::task::JoinHandle<QueryStats>>,
) {
    let qt = Arc::new(query_terms.to_vec());
    let sc = Arc::new(select_columns.to_vec());

    let indexed_handle = if !work_items.is_empty() {
        let indexed_tx = tx.clone();
        let cache = Arc::clone(&cache);
        let qt = Arc::clone(&qt);
        let sc = Arc::clone(&sc);
        let schema = schema.clone();
        let runtime = Arc::clone(&runtime);
        let column = column.clone();
        Some(tokio::spawn(async move {
            indexed_consumer(
                work_items,
                cache,
                column,
                qt,
                operator,
                with_score,
                sc,
                schema,
                io_concurrency,
                runtime,
                indexed_tx,
            )
            .await
        }))
    } else {
        None
    };

    let bf_handle = if !unindexed_files.is_empty() {
        let bf_tx = tx.clone();
        let cache = Arc::clone(&cache);
        let qt = Arc::clone(&qt);
        let sc = Arc::clone(&sc);
        let schema = schema.clone();
        let runtime = Arc::clone(&runtime);
        let column = column.clone();
        let bf_score = score_mode == crate::ScoreMode::All;
        let agg_avg_dl = bm25::avg_dl(agg.total_tokens, agg.total_rows);
        let agg_term_infos: Arc<Vec<(String, u32)>> = Arc::new(
            query_terms
                .iter()
                .map(|t| {
                    let df = agg.term_df.get(t).copied().unwrap_or(1) as u32;
                    (t.clone(), df)
                })
                .collect(),
        );
        let bf_limit = if !bf_score { limit } else { None };
        let agg_total_rows = agg.total_rows;
        Some(tokio::spawn(async move {
            brute_force_consumer(
                unindexed_files,
                cache,
                column,
                qt,
                operator,
                bf_score,
                sc,
                agg_total_rows,
                agg_avg_dl,
                agg_term_infos,
                bf_limit,
                schema,
                runtime,
                bf_tx,
            )
            .await
        }))
    } else {
        None
    };

    // Drop the original sender so the channel closes when consumer clones are dropped
    drop(tx);

    (indexed_handle, bf_handle)
}

// ---------------------------------------------------------------------------
// Orchestrator
// ---------------------------------------------------------------------------

/// Queries a LakeSearch table, returning results as a `SendableRecordBatchStream`.
///
/// Two concurrent consumers (indexed + brute-force) stream `RecordBatch`es
/// through a shared channel. For top-K queries, an intermediate task
/// accumulates, ranks, and re-emits.
#[allow(clippy::too_many_arguments)]
pub async fn run_query(
    cache: Arc<ObjectCache>,
    base: Path,
    column: String,
    query_text: &str,
    operator: Operator,
    score_mode: crate::ScoreMode,
    limit: Option<usize>,
    select_columns: Vec<String>,
    io_concurrency: usize,
    runtime: Arc<LakeRuntime>,
) -> Result<(SendableRecordBatchStream, QueryStats)> {
    let with_score = score_mode != crate::ScoreMode::None;
    let query_terms = tokenize(query_text);
    if query_terms.is_empty() {
        let schema = build_empty_schema(&select_columns, &column, with_score);
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(tx);
        return Ok((
            Box::pin(QueryResultStream::new(schema, rx)),
            QueryStats::default(),
        ));
    }

    let plan = plan_query(
        &cache,
        &base,
        &column,
        &query_terms,
        operator,
        io_concurrency,
    )
    .await?;
    let schema = resolve_schema(&cache, &plan, &column, &select_columns, with_score).await?;

    let readers: Vec<SegmentReader> = plan
        .segments
        .iter()
        .filter_map(|bytes| SegmentReader::open(bytes.clone()).ok())
        .collect();

    let agg = collect_aggregate_stats(&readers, &query_terms);
    let needs_top_k = with_score && limit.is_some();

    // Build work items from all segments
    let mut all_work_items = Vec::new();
    for reader in &readers {
        let candidates = evaluate_segment(reader, &query_terms, operator)?;
        if !candidates.is_empty() {
            all_work_items.extend(group_candidates(reader, &candidates, &query_terms));
        }
    }

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(16);

    let (indexed_handle, bf_handle) = spawn_consumers(
        all_work_items,
        plan.unindexed_files,
        cache,
        column,
        &query_terms,
        operator,
        score_mode,
        with_score,
        limit,
        &select_columns,
        &agg,
        schema.clone(),
        io_concurrency,
        runtime,
        tx,
    );

    if needs_top_k {
        // Accumulate, rank, re-emit
        let schema_clone = schema.clone();
        let (final_tx, final_rx) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(4);
        tokio::spawn(async move {
            let mut all_batches = Vec::new();
            let mut rx = rx;
            while let Some(batch_result) = rx.recv().await {
                match batch_result {
                    Ok(batch) => all_batches.push(batch),
                    Err(e) => {
                        let _ = final_tx.send(Err(e)).await;
                        return;
                    }
                }
            }
            // Wait for consumer stats (not used in streaming path, but ensures cleanup)
            if let Some(h) = indexed_handle {
                let _ = h.await;
            }
            if let Some(h) = bf_handle {
                let _ = h.await;
            }
            match rank_batches(all_batches, &schema_clone, true, limit) {
                Ok(ranked) => {
                    for batch in ranked {
                        if final_tx.send(Ok(batch)).await.is_err() {
                            return;
                        }
                    }
                }
                Err(e) => {
                    let _ = final_tx.send(Err(e)).await;
                }
            }
        });
        let result_stream = QueryResultStream::new(schema, final_rx);
        Ok((Box::pin(result_stream), QueryStats::default()))
    } else {
        let result_stream = QueryResultStream::new(schema, rx);
        Ok((Box::pin(result_stream), QueryStats::default()))
    }
}

/// Collects all results and returns them with final stats.
///
/// Spawns concurrent consumers, drains the shared channel, joins both
/// tasks for stats, then ranks and limits.
#[allow(clippy::too_many_arguments)]
pub async fn run_query_collected(
    cache: Arc<ObjectCache>,
    base: Path,
    column: String,
    query_text: &str,
    operator: Operator,
    score_mode: crate::ScoreMode,
    limit: Option<usize>,
    select_columns: Vec<String>,
    io_concurrency: usize,
    runtime: Arc<LakeRuntime>,
) -> Result<CollectedQueryResult> {
    let with_score = score_mode != crate::ScoreMode::None;
    let query_terms = tokenize(query_text);
    if query_terms.is_empty() {
        return Ok(CollectedQueryResult {
            batches: vec![],
            stats: QueryStats::default(),
        });
    }

    let plan = plan_query(
        &cache,
        &base,
        &column,
        &query_terms,
        operator,
        io_concurrency,
    )
    .await?;
    let schema = resolve_schema(&cache, &plan, &column, &select_columns, with_score).await?;

    let readers: Vec<SegmentReader> = plan
        .segments
        .iter()
        .filter_map(|bytes| SegmentReader::open(bytes.clone()).ok())
        .collect();

    if !readers.is_empty() {
        info!(segments = readers.len(), "loaded segments");
    }
    if !plan.unindexed_files.is_empty() {
        info!(
            unindexed_files = plan.unindexed_files.len(),
            "brute-force scanning un-indexed files"
        );
    }

    let agg = collect_aggregate_stats(&readers, &query_terms);

    // Build work items from all segments
    let mut all_work_items = Vec::new();
    for reader in &readers {
        let candidates = evaluate_segment(reader, &query_terms, operator)?;
        if !candidates.is_empty() {
            all_work_items.extend(group_candidates(reader, &candidates, &query_terms));
        }
    }

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(16);

    let (indexed_handle, bf_handle) = spawn_consumers(
        all_work_items,
        plan.unindexed_files,
        cache,
        column,
        &query_terms,
        operator,
        score_mode,
        with_score,
        limit,
        &select_columns,
        &agg,
        schema.clone(),
        io_concurrency,
        runtime,
        tx,
    );

    // Collect all batches from the merged stream
    let mut all_batches = Vec::new();
    while let Some(batch_result) = rx.recv().await {
        all_batches.push(batch_result?);
    }

    // Merge stats from both consumers
    let mut stats = QueryStats::default();
    if let Some(h) = indexed_handle {
        stats.merge(&h.await.unwrap_or_default());
    }
    if let Some(h) = bf_handle {
        stats.merge(&h.await.unwrap_or_default());
    }

    // Rank and limit
    all_batches = rank_batches(all_batches, &schema, with_score, limit)?;

    info!(
        candidate_pages = stats.candidate_pages,
        rows_scanned = stats.rows_scanned,
        rows_matched = stats.rows_matched,
        "query complete"
    );

    Ok(CollectedQueryResult {
        batches: all_batches,
        stats,
    })
}
