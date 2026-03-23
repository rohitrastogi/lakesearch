//! Three-stage query pipeline: I/O producers → CPU dispatcher → Coalescer.
//!
//! Decouples I/O from CPU via bounded channels. Multiple I/O tasks feed
//! work items into a shared queue. A single CPU dispatcher pulls items and
//! dispatches to the rayon pool via `FuturesUnordered`. A coalescer
//! accumulates small output batches into larger ones before sending.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use futures::stream::{FuturesUnordered, StreamExt, TryStreamExt};
use tokio::sync::mpsc;
use tracing::warn;

use lakesearch_core::runtime::LakeRuntime;

use crate::object_cache::ObjectCache;
use crate::parquet_util::{build_row_selection, find_column, open_parquet_stream};

use super::types::{CpuWorkItem, IndexedWorkItem, QueryStats, SharedQueryContext};
use super::verify::{brute_force_verify_batch, resolve_projection, verify_batch, VerifyContext};

/// Target rows per coalesced output batch.
const TARGET_BATCH_SIZE: usize = 8192;

/// Runs the three-stage pipeline: I/O → CPU → Coalescer.
///
/// Returns a receiver for final output batches and a handle that resolves
/// to aggregated `QueryStats` when the pipeline completes.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_pipeline(
    work_items: Vec<IndexedWorkItem>,
    unindexed_files: Vec<String>,
    cache: Arc<ObjectCache>,
    shared_ctx: Arc<SharedQueryContext>,
    agg_total_rows: u64,
    agg_avg_dl: f64,
    agg_term_infos: Arc<Vec<(String, u32)>>,
    column: String,
    early_limit: Option<usize>,
    candidate_pages: usize,
    runtime: Arc<LakeRuntime>,
) -> (
    mpsc::Receiver<Result<RecordBatch>>,
    tokio::task::JoinHandle<QueryStats>,
) {
    let (input_tx, input_rx) = mpsc::channel::<CpuWorkItem>(16);
    let (output_tx, output_rx) = mpsc::channel::<RecordBatch>(16);
    let (final_tx, final_rx) = mpsc::channel::<Result<RecordBatch>>(16);

    // Stage 1: I/O producers
    spawn_io_producers(
        work_items,
        unindexed_files,
        cache,
        &shared_ctx,
        agg_total_rows,
        agg_avg_dl,
        agg_term_infos,
        column,
        input_tx,
        final_tx.clone(),
    );

    // Stage 2: CPU dispatcher
    let stats_handle = spawn_cpu_dispatcher(
        input_rx,
        output_tx,
        shared_ctx.clone(),
        early_limit,
        candidate_pages,
        runtime,
    );

    // Stage 3: Coalescer
    spawn_coalescer(output_rx, final_tx, shared_ctx.schema.clone());

    (final_rx, stats_handle)
}

// ---------------------------------------------------------------------------
// Stage 1: I/O Producers
// ---------------------------------------------------------------------------

/// Spawns one tokio task per indexed work item and one per unindexed file.
/// Each task streams Parquet batches and sends `CpuWorkItem`s to `input_tx`.
/// Errors are sent to `error_tx` (the final output channel).
#[allow(clippy::too_many_arguments)]
fn spawn_io_producers(
    work_items: Vec<IndexedWorkItem>,
    unindexed_files: Vec<String>,
    cache: Arc<ObjectCache>,
    shared_ctx: &Arc<SharedQueryContext>,
    agg_total_rows: u64,
    agg_avg_dl: f64,
    agg_term_infos: Arc<Vec<(String, u32)>>,
    column: String,
    input_tx: mpsc::Sender<CpuWorkItem>,
    error_tx: mpsc::Sender<Result<RecordBatch>>,
) {
    // Group work items by file to share metadata fetches
    let mut items_by_file: HashMap<String, Vec<IndexedWorkItem>> = HashMap::new();
    for item in work_items {
        items_by_file
            .entry(item.file_path.clone())
            .or_default()
            .push(item);
    }

    let select_columns: Vec<String> = shared_ctx
        .schema
        .fields()
        .iter()
        .filter_map(|f| {
            let n = f.name().as_str();
            if n == "text" || n == "score" {
                None
            } else {
                Some(n.to_owned())
            }
        })
        .collect();

    // Spawn indexed I/O producers (one task per file)
    for (file_path, file_items) in items_by_file {
        let cache = Arc::clone(&cache);
        let input_tx = input_tx.clone();
        let error_tx = error_tx.clone();
        let column = column.clone();
        let select_columns = select_columns.clone();

        tokio::spawn(async move {
            if let Err(e) = indexed_io_producer(
                &file_path,
                file_items,
                &cache,
                &column,
                &select_columns,
                &input_tx,
            )
            .await
            {
                let _ = error_tx.send(Err(e)).await;
            }
        });
    }

    // Spawn brute-force I/O producers (one task per file)
    for file_path in unindexed_files {
        let cache = Arc::clone(&cache);
        let input_tx = input_tx.clone();
        let error_tx = error_tx.clone();
        let column = column.clone();
        let select_columns = select_columns.clone();
        let agg_term_infos = Arc::clone(&agg_term_infos);

        tokio::spawn(async move {
            if let Err(e) = brute_force_io_producer(
                &file_path,
                &cache,
                &column,
                &select_columns,
                agg_total_rows,
                agg_avg_dl,
                agg_term_infos,
                &input_tx,
            )
            .await
            {
                let _ = error_tx.send(Err(e)).await;
            }
        });
    }

    // All producer clones of input_tx are moved into tasks above.
    // The original input_tx is dropped here, so the channel closes
    // when all producer tasks complete.
}

/// I/O producer for indexed work items from a single file.
async fn indexed_io_producer(
    file_path: &str,
    items: Vec<IndexedWorkItem>,
    cache: &ObjectCache,
    column: &str,
    select_columns: &[String],
    input_tx: &mpsc::Sender<CpuWorkItem>,
) -> Result<()> {
    let pq_meta = cache.get_parquet_metadata(file_path).await?;
    let projection = resolve_projection(&pq_meta, column, select_columns)?;

    let arrow_schema = parquet::arrow::parquet_to_arrow_schema(
        pq_meta.file_metadata().schema_descr(),
        pq_meta.file_metadata().key_value_metadata(),
    )?;
    let is_large = arrow_schema
        .field_with_name(column)
        .map(|f| f.data_type() == &arrow::datatypes::DataType::LargeUtf8)
        .unwrap_or(false);

    for item in &items {
        let rg_total_rows = pq_meta.row_group(item.rg_idx as usize).num_rows();
        let entry_refs: Vec<&lakesearch_core::types::DocTableEntry> = item.entries.iter().collect();
        let selection = build_row_selection(&entry_refs, rg_total_rows);

        let mut pq_stream = open_parquet_stream(
            cache.store(),
            file_path,
            item.rg_idx as usize,
            &projection.leaf_indices,
            Some(selection),
        )
        .await?;

        while let Some(batch) = pq_stream.try_next().await? {
            let work = CpuWorkItem {
                batch,
                use_ilike: false,
                indexed_batch_col: projection.indexed_batch_col,
                select_col_map: projection.select_col_map.clone(),
                is_large,
                avg_dl: item.avg_dl,
                total_rows: item.total_rows,
                term_infos: Arc::clone(&item.term_infos),
            };
            if input_tx.send(work).await.is_err() {
                return Ok(()); // pipeline shut down
            }
        }
    }
    Ok(())
}

/// I/O producer for brute-force scanning a single unindexed file.
#[allow(clippy::too_many_arguments)]
async fn brute_force_io_producer(
    file_path: &str,
    cache: &ObjectCache,
    column: &str,
    select_columns: &[String],
    agg_total_rows: u64,
    agg_avg_dl: f64,
    agg_term_infos: Arc<Vec<(String, u32)>>,
    input_tx: &mpsc::Sender<CpuWorkItem>,
) -> Result<()> {
    let pq_meta = match cache.get_parquet_metadata(file_path).await {
        Ok(m) => m,
        Err(e) => {
            warn!(file = %file_path, error = %e, "skipping file");
            return Ok(());
        }
    };

    if find_column(&pq_meta, column).is_err() {
        warn!(file = %file_path, column = %column, "skipping: column not found");
        return Ok(());
    }

    let projection = resolve_projection(&pq_meta, column, select_columns)?;

    let arrow_schema = parquet::arrow::parquet_to_arrow_schema(
        pq_meta.file_metadata().schema_descr(),
        pq_meta.file_metadata().key_value_metadata(),
    )?;
    let is_large = arrow_schema
        .field_with_name(column)
        .map(|f| f.data_type() == &arrow::datatypes::DataType::LargeUtf8)
        .unwrap_or(false);

    for rg_idx in 0..pq_meta.num_row_groups() {
        let mut pq_stream = open_parquet_stream(
            cache.store(),
            file_path,
            rg_idx,
            &projection.leaf_indices,
            None,
        )
        .await?;

        while let Some(batch) = pq_stream.try_next().await? {
            let work = CpuWorkItem {
                batch,
                use_ilike: true,
                indexed_batch_col: projection.indexed_batch_col,
                select_col_map: projection.select_col_map.clone(),
                is_large,
                avg_dl: agg_avg_dl,
                total_rows: agg_total_rows,
                term_infos: Arc::clone(&agg_term_infos),
            };
            if input_tx.send(work).await.is_err() {
                return Ok(()); // pipeline shut down
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Stage 2: CPU Dispatcher
// ---------------------------------------------------------------------------

/// Pulls `CpuWorkItem`s from `input_rx`, dispatches them to the rayon pool
/// via `FuturesUnordered`, collects results, and sends output batches to
/// `output_tx`. Returns aggregated `QueryStats` when all work is drained.
fn spawn_cpu_dispatcher(
    mut input_rx: mpsc::Receiver<CpuWorkItem>,
    output_tx: mpsc::Sender<RecordBatch>,
    shared_ctx: Arc<SharedQueryContext>,
    early_limit: Option<usize>,
    candidate_pages: usize,
    runtime: Arc<LakeRuntime>,
) -> tokio::task::JoinHandle<QueryStats> {
    tokio::spawn(async move {
        let max_cpu = runtime.num_threads();
        let mut in_flight = FuturesUnordered::new();
        let mut total_stats = QueryStats {
            candidate_pages,
            ..Default::default()
        };
        let mut input_done = false;

        loop {
            tokio::select! {
                biased;

                // Collect completed CPU work first
                Some(result) = in_flight.next(), if !in_flight.is_empty() => {
                    let (batch_opt, batch_stats): (Option<RecordBatch>, QueryStats) = result;
                    total_stats.merge(&batch_stats);
                    if let Some(batch) = batch_opt {
                        if output_tx.send(batch).await.is_err() {
                            return total_stats;
                        }
                    }
                    // Early termination for unscored limit queries
                    if let Some(lim) = early_limit {
                        if total_stats.rows_matched >= lim {
                            return total_stats;
                        }
                    }
                }

                // Pull new work if under capacity and input is open
                item = input_rx.recv(), if !input_done && in_flight.len() < max_cpu => {
                    match item {
                        Some(work) => {
                            let ctx = Arc::clone(&shared_ctx);
                            in_flight.push(runtime.cpu(move || dispatch_verify(work, &ctx)));
                        }
                        None => {
                            input_done = true;
                        }
                    }
                }

                else => break,
            }
        }

        // Drain remaining in-flight tasks
        while let Some((batch_opt, batch_stats)) = in_flight.next().await {
            total_stats.merge(&batch_stats);
            if let Some(batch) = batch_opt {
                if output_tx.send(batch).await.is_err() {
                    return total_stats;
                }
            }
        }

        total_stats
    })
}

/// Dispatches a single work item to the appropriate verify function.
fn dispatch_verify(
    work: CpuWorkItem,
    ctx: &SharedQueryContext,
) -> (Option<RecordBatch>, QueryStats) {
    let verify_ctx = VerifyContext {
        query_terms: &ctx.query_terms,
        term_infos: &work.term_infos,
        operator: ctx.operator,
        with_score: ctx.with_score,
        avg_dl: work.avg_dl,
        total_rows: work.total_rows,
        is_large: work.is_large,
        indexed_batch_col: work.indexed_batch_col,
        select_col_map: &work.select_col_map,
    };

    if work.use_ilike {
        brute_force_verify_batch(&work.batch, &verify_ctx, &ctx.schema)
    } else {
        verify_batch(&work.batch, &verify_ctx, &ctx.schema)
    }
}

// ---------------------------------------------------------------------------
// Stage 3: Coalescer
// ---------------------------------------------------------------------------

/// Accumulates small output batches, concatenating them when the total
/// rows reach `TARGET_BATCH_SIZE`, then sends to the final output channel.
fn spawn_coalescer(
    mut output_rx: mpsc::Receiver<RecordBatch>,
    final_tx: mpsc::Sender<Result<RecordBatch>>,
    schema: arrow::datatypes::SchemaRef,
) {
    tokio::spawn(async move {
        let mut pending: Vec<RecordBatch> = Vec::new();
        let mut pending_rows: usize = 0;

        while let Some(batch) = output_rx.recv().await {
            pending_rows += batch.num_rows();
            pending.push(batch);

            if pending_rows >= TARGET_BATCH_SIZE {
                if let Some(flushed) = coalesce_batches(&schema, &mut pending) {
                    pending_rows = 0;
                    if final_tx.send(Ok(flushed)).await.is_err() {
                        return;
                    }
                }
            }
        }

        // Flush remaining
        if let Some(flushed) = coalesce_batches(&schema, &mut pending) {
            let _ = final_tx.send(Ok(flushed)).await;
        }
    });
}

/// Concatenates pending batches into a single batch and clears the buffer.
fn coalesce_batches(
    schema: &arrow::datatypes::SchemaRef,
    pending: &mut Vec<RecordBatch>,
) -> Option<RecordBatch> {
    if pending.is_empty() {
        return None;
    }
    let refs: Vec<&RecordBatch> = pending.iter().collect();
    let result = arrow::compute::concat_batches(schema, refs.iter().copied()).ok();
    pending.clear();
    result
}
