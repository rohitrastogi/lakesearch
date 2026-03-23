//! Async multi-segment query execution.
//!
//! The query pipeline has three stages connected by bounded channels:
//! 1. **I/O Producers** — tokio tasks stream Parquet pages into a work queue
//! 2. **CPU Dispatcher** — dispatches verification to the rayon pool via
//!    `FuturesUnordered`, sends output batches forward
//! 3. **Coalescer** — accumulates small batches, concatenates at 8192 rows

mod evaluate;
mod pipeline;
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
use pipeline::run_pipeline;
use plan::{plan_query, resolve_schema};
use rank::rank_batches;
use types::SharedQueryContext;

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

/// Common setup: plan, evaluate segments, group candidates, collect stats.
/// Returns everything needed to start the pipeline.
#[allow(clippy::type_complexity, clippy::too_many_arguments)]
async fn setup_query(
    cache: &Arc<ObjectCache>,
    base: &Path,
    column: &str,
    query_terms: &[String],
    operator: Operator,
    select_columns: &[String],
    with_score: bool,
    io_concurrency: usize,
) -> Result<(
    Vec<types::IndexedWorkItem>,
    Vec<String>,
    SchemaRef,
    AggregateStats,
)> {
    let plan = plan_query(cache, base, column, query_terms, operator, io_concurrency).await?;
    let schema = resolve_schema(cache, &plan, column, select_columns, with_score).await?;

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

    let agg = collect_aggregate_stats(&readers, query_terms);

    let mut all_work_items = Vec::new();
    for reader in &readers {
        let candidates = evaluate_segment(reader, query_terms, operator)?;
        if !candidates.is_empty() {
            all_work_items.extend(group_candidates(reader, &candidates, query_terms));
        }
    }

    Ok((all_work_items, plan.unindexed_files, schema, agg))
}

// ---------------------------------------------------------------------------
// Orchestrator
// ---------------------------------------------------------------------------

/// Queries a LakeSearch table, returning results as a `SendableRecordBatchStream`.
///
/// Launches a three-stage pipeline (I/O → CPU → Coalescer). For top-K
/// queries, an intermediate task accumulates, ranks, and re-emits.
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

    let (work_items, unindexed_files, schema, agg) = setup_query(
        &cache,
        &base,
        &column,
        &query_terms,
        operator,
        &select_columns,
        with_score,
        io_concurrency,
    )
    .await?;

    let needs_top_k = with_score && limit.is_some();
    let candidate_pages: usize = work_items.iter().map(|w| w.entries.len()).sum();
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

    let shared_ctx = Arc::new(SharedQueryContext {
        query_terms: Arc::new(query_terms),
        operator,
        with_score: bf_score || with_score,
        schema: schema.clone(),
    });

    let early_limit = if !with_score { limit } else { None };

    let (mut final_rx, stats_handle) = run_pipeline(
        work_items,
        unindexed_files,
        cache,
        shared_ctx,
        agg.total_rows,
        agg_avg_dl,
        agg_term_infos,
        column,
        early_limit,
        candidate_pages,
        runtime,
    );

    if needs_top_k {
        let schema_clone = schema.clone();
        let (ranked_tx, ranked_rx) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(4);
        tokio::spawn(async move {
            let mut all_batches = Vec::new();
            while let Some(batch_result) = final_rx.recv().await {
                match batch_result {
                    Ok(batch) => all_batches.push(batch),
                    Err(e) => {
                        let _ = ranked_tx.send(Err(e)).await;
                        return;
                    }
                }
            }
            let _ = stats_handle.await;
            match rank_batches(all_batches, &schema_clone, true, limit) {
                Ok(ranked) => {
                    for batch in ranked {
                        if ranked_tx.send(Ok(batch)).await.is_err() {
                            return;
                        }
                    }
                }
                Err(e) => {
                    let _ = ranked_tx.send(Err(e)).await;
                }
            }
        });
        let result_stream = QueryResultStream::new(schema, ranked_rx);
        Ok((Box::pin(result_stream), QueryStats::default()))
    } else {
        let result_stream = QueryResultStream::new(schema, final_rx);
        Ok((Box::pin(result_stream), QueryStats::default()))
    }
}

/// Collects all results and returns them with final stats.
///
/// Launches the three-stage pipeline, drains the output, gets stats
/// from the CPU dispatcher, then ranks and limits.
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

    let (work_items, unindexed_files, schema, agg) = setup_query(
        &cache,
        &base,
        &column,
        &query_terms,
        operator,
        &select_columns,
        with_score,
        io_concurrency,
    )
    .await?;

    let candidate_pages: usize = work_items.iter().map(|w| w.entries.len()).sum();
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

    let shared_ctx = Arc::new(SharedQueryContext {
        query_terms: Arc::new(query_terms),
        operator,
        with_score: bf_score || with_score,
        schema: schema.clone(),
    });

    let early_limit = if !with_score { limit } else { None };

    let (mut final_rx, stats_handle) = run_pipeline(
        work_items,
        unindexed_files,
        cache,
        shared_ctx,
        agg.total_rows,
        agg_avg_dl,
        agg_term_infos,
        column,
        early_limit,
        candidate_pages,
        runtime,
    );

    // Collect all batches from the pipeline
    let mut all_batches = Vec::new();
    while let Some(batch_result) = final_rx.recv().await {
        all_batches.push(batch_result?);
    }

    // Get stats from the CPU dispatcher
    let stats = stats_handle.await.unwrap_or_default();

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
