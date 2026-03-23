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
use types::{BruteForceScoring, SharedQueryContext, SCORE_COL, TEXT_COL};

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

    let mut fields = vec![Field::new(TEXT_COL, DataType::Utf8, true)];

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
        fields.push(Field::new(SCORE_COL, DataType::Float64, true));
    }

    Ok(Arc::new(Schema::new(fields)))
}

/// Builds a fallback schema when no parquet metadata is available (empty table).
fn build_empty_schema(select_columns: &[String], column: &str, with_score: bool) -> SchemaRef {
    let mut fields = vec![Field::new(TEXT_COL, DataType::Utf8, true)];
    for sel in select_columns {
        if sel == column {
            continue;
        }
        fields.push(Field::new(sel, DataType::Utf8, true));
    }
    if with_score {
        fields.push(Field::new(SCORE_COL, DataType::Float64, true));
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

/// Builds aggregate stats from per-segment term_infos already collected
/// during evaluation — no additional FST lookups needed.
fn collect_aggregate_stats(
    segments: &[SegmentReader],
    segment_term_infos: &[Vec<(String, u32)>],
) -> AggregateStats {
    let mut total_rows = 0u64;
    let mut total_tokens = 0u64;
    let mut term_df: HashMap<String, u64> = HashMap::new();

    for (reader, infos) in segments.iter().zip(segment_term_infos.iter()) {
        let cs = reader.corpus_stats();
        total_rows += cs.total_rows;
        total_tokens += cs.total_tokens;
        for (term, df) in infos {
            *term_df.entry(term.clone()).or_default() += *df as u64;
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

    // Evaluate all segments: single set of FST lookups per segment produces
    // both candidates and term_infos for aggregate stats.
    let mut all_work_items = Vec::new();
    let mut all_term_infos = Vec::new();
    for reader in &readers {
        let eval = evaluate_segment(reader, query_terms, operator)?;
        all_term_infos.push(eval.term_infos.clone());
        if !eval.candidates.is_empty() {
            all_work_items.extend(group_candidates(reader, &eval.candidates, &eval.term_infos));
        }
    }

    let agg = collect_aggregate_stats(&readers, &all_term_infos);

    Ok((all_work_items, plan.unindexed_files, schema, agg))
}

/// Launches the three-stage pipeline from setup results.
/// Shared between `run_query` and `run_query_collected`.
#[allow(clippy::too_many_arguments)]
fn launch_pipeline(
    work_items: Vec<types::IndexedWorkItem>,
    unindexed_files: Vec<String>,
    cache: Arc<ObjectCache>,
    column: String,
    query_terms: Vec<String>,
    operator: Operator,
    score_mode: crate::ScoreMode,
    with_score: bool,
    limit: Option<usize>,
    agg: &AggregateStats,
    schema: SchemaRef,
    max_io_tasks: usize,
    runtime: Arc<LakeRuntime>,
) -> (
    tokio::sync::mpsc::Receiver<Result<RecordBatch>>,
    tokio::task::JoinHandle<QueryStats>,
) {
    let candidate_pages: usize = work_items.iter().map(|w| w.entries.len()).sum();
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
        with_score,
        schema,
    });

    let select_columns: Arc<[String]> = shared_ctx
        .schema
        .fields()
        .iter()
        .filter_map(|f| {
            let n = f.name().as_str();
            if n == TEXT_COL || n == SCORE_COL {
                None
            } else {
                Some(n.to_owned())
            }
        })
        .collect();

    let early_limit = if !with_score { limit } else { None };

    let bf_scoring = BruteForceScoring {
        total_rows: agg.total_rows,
        avg_dl: agg_avg_dl,
        term_infos: agg_term_infos,
    };

    // Only enable brute-force scoring when ScoreMode::All
    let bf_with_score = score_mode == crate::ScoreMode::All;

    run_pipeline(
        work_items,
        unindexed_files,
        cache,
        shared_ctx,
        bf_scoring,
        bf_with_score,
        Arc::from(column),
        select_columns,
        early_limit,
        candidate_pages,
        max_io_tasks,
        runtime,
    )
}

// ---------------------------------------------------------------------------
// Orchestrator
// ---------------------------------------------------------------------------

/// Queries a LakeSearch table, returning results as a `SendableRecordBatchStream`.
///
/// Launches a three-stage pipeline (I/O → CPU → Coalescer). For top-K
/// queries, an intermediate task accumulates, ranks, and re-emits.
/// Stats are logged via `tracing` when the pipeline completes.
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
    max_io_tasks: usize,
    runtime: Arc<LakeRuntime>,
) -> Result<SendableRecordBatchStream> {
    let with_score = score_mode != crate::ScoreMode::None;
    let query_terms = tokenize(query_text);
    if query_terms.is_empty() {
        let schema = build_empty_schema(&select_columns, &column, with_score);
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(tx);
        return Ok(Box::pin(QueryResultStream::new(schema, rx)));
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

    let (mut final_rx, stats_handle) = launch_pipeline(
        work_items,
        unindexed_files,
        cache,
        column,
        query_terms,
        operator,
        score_mode,
        with_score,
        limit,
        &agg,
        schema.clone(),
        max_io_tasks,
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
            let stats = stats_handle.await.unwrap_or_default();
            log_query_stats(&stats);
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
        Ok(Box::pin(QueryResultStream::new(schema, ranked_rx)))
    } else {
        // Log stats when the pipeline completes (after stream is consumed).
        tokio::spawn(async move {
            let stats = stats_handle.await.unwrap_or_default();
            log_query_stats(&stats);
        });
        Ok(Box::pin(QueryResultStream::new(schema, final_rx)))
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
    max_io_tasks: usize,
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

    let (mut final_rx, stats_handle) = launch_pipeline(
        work_items,
        unindexed_files,
        cache,
        column,
        query_terms,
        operator,
        score_mode,
        with_score,
        limit,
        &agg,
        schema.clone(),
        max_io_tasks,
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

    log_query_stats(&stats);

    Ok(CollectedQueryResult {
        batches: all_batches,
        stats,
    })
}

fn log_query_stats(stats: &QueryStats) {
    info!(
        candidate_pages = stats.candidate_pages,
        rows_scanned = stats.rows_scanned,
        rows_matched = stats.rows_matched,
        "query complete"
    );
}
