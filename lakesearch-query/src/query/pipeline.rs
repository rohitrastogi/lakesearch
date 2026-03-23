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
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::{arrow_reader::ArrowReaderMetadata, ParquetRecordBatchStreamBuilder};
use tokio::sync::mpsc;
use tracing::warn;

use lakesearch_core::runtime::LakeRuntime;

use crate::object_cache::ObjectCache;
use lakesearch_core::parquet_util::{build_row_selection, find_column};

use super::types::{
    BruteForceScoring, CpuWorkItem, FileProjection, IndexedWorkItem, QueryStats,
    SharedQueryContext, VerifyMode,
};
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
    bf_scoring: BruteForceScoring,
    bf_with_score: bool,
    column: Arc<str>,
    select_columns: Arc<[String]>,
    early_limit: Option<usize>,
    candidate_pages: usize,
    max_io_tasks: usize,
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
        bf_scoring,
        bf_with_score,
        column,
        select_columns,
        max_io_tasks,
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

/// Spawns one tokio task per file (indexed and unindexed). A shared
/// semaphore limits the number of concurrently active I/O tasks.
#[allow(clippy::too_many_arguments)]
fn spawn_io_producers(
    work_items: Vec<IndexedWorkItem>,
    unindexed_files: Vec<String>,
    cache: Arc<ObjectCache>,
    bf_scoring: BruteForceScoring,
    bf_with_score: bool,
    column: Arc<str>,
    select_columns: Arc<[String]>,
    max_io_tasks: usize,
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

    let sem = Arc::new(tokio::sync::Semaphore::new(max_io_tasks));

    // Spawn indexed I/O producers (one task per file)
    for (file_path, file_items) in items_by_file {
        let cache = Arc::clone(&cache);
        let input_tx = input_tx.clone();
        let error_tx = error_tx.clone();
        let column = Arc::clone(&column);
        let select_columns = Arc::clone(&select_columns);
        let sem = Arc::clone(&sem);

        tokio::spawn(async move {
            let _permit = sem.acquire().await;
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
    let bf_scoring = Arc::new(bf_scoring);
    for file_path in unindexed_files {
        let cache = Arc::clone(&cache);
        let input_tx = input_tx.clone();
        let error_tx = error_tx.clone();
        let column = Arc::clone(&column);
        let select_columns = Arc::clone(&select_columns);
        let bf_scoring = Arc::clone(&bf_scoring);
        let sem = Arc::clone(&sem);

        tokio::spawn(async move {
            let _permit = sem.acquire().await;
            if let Err(e) = brute_force_io_producer(
                &file_path,
                &cache,
                &column,
                &select_columns,
                &bf_scoring,
                bf_with_score,
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
/// Uses cached `ParquetMetaData` to build the stream without re-reading
/// the footer or issuing a HEAD request per row group.
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
    let file_proj = Arc::new(FileProjection {
        indexed_batch_col: projection.indexed_batch_col,
        select_col_map: projection.select_col_map,
        is_large: projection.is_large,
    });

    let object_meta = cache
        .store()
        .head(&object_store::path::Path::from(file_path))
        .await?;
    let reader_meta = ArrowReaderMetadata::try_new(
        Arc::new(pq_meta.as_ref().clone()),
        ArrowReaderOptions::default(),
    )?;

    for item in &items {
        let rg_total_rows = reader_meta
            .metadata()
            .row_group(item.rg_idx as usize)
            .num_rows();
        let entry_refs: Vec<&lakesearch_core::types::DocTableEntry> = item.entries.iter().collect();
        let selection = build_row_selection(&entry_refs, rg_total_rows);

        let obj_reader = parquet::arrow::async_reader::ParquetObjectReader::new(
            Arc::clone(cache.store()),
            object_meta.clone(),
        );
        let mask = parquet::arrow::ProjectionMask::leaves(
            reader_meta.parquet_schema(),
            projection.leaf_indices.iter().copied(),
        );
        let mut pq_stream =
            ParquetRecordBatchStreamBuilder::new_with_metadata(obj_reader, reader_meta.clone())
                .with_row_groups(vec![item.rg_idx as usize])
                .with_projection(mask)
                .with_row_selection(selection)
                .build()?;

        while let Some(batch) = pq_stream.try_next().await? {
            let work = CpuWorkItem {
                batch,
                mode: VerifyMode::Indexed,
                with_score: true, // indexed always scores when schema has score col
                file_proj: Arc::clone(&file_proj),
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
async fn brute_force_io_producer(
    file_path: &str,
    cache: &ObjectCache,
    column: &str,
    select_columns: &[String],
    bf_scoring: &BruteForceScoring,
    bf_with_score: bool,
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
    let file_proj = Arc::new(FileProjection {
        indexed_batch_col: projection.indexed_batch_col,
        select_col_map: projection.select_col_map,
        is_large: projection.is_large,
    });

    let object_meta = cache
        .store()
        .head(&object_store::path::Path::from(file_path))
        .await?;
    let reader_meta = ArrowReaderMetadata::try_new(
        Arc::new(pq_meta.as_ref().clone()),
        ArrowReaderOptions::default(),
    )?;

    let term_infos = Arc::clone(&bf_scoring.term_infos);

    for rg_idx in 0..reader_meta.metadata().num_row_groups() {
        let obj_reader = parquet::arrow::async_reader::ParquetObjectReader::new(
            Arc::clone(cache.store()),
            object_meta.clone(),
        );
        let mask = parquet::arrow::ProjectionMask::leaves(
            reader_meta.parquet_schema(),
            projection.leaf_indices.iter().copied(),
        );
        let mut pq_stream =
            ParquetRecordBatchStreamBuilder::new_with_metadata(obj_reader, reader_meta.clone())
                .with_row_groups(vec![rg_idx])
                .with_projection(mask)
                .build()?;

        while let Some(batch) = pq_stream.try_next().await? {
            let work = CpuWorkItem {
                batch,
                mode: VerifyMode::BruteForce,
                with_score: bf_with_score,
                file_proj: Arc::clone(&file_proj),
                avg_dl: bf_scoring.avg_dl,
                total_rows: bf_scoring.total_rows,
                term_infos: Arc::clone(&term_infos),
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
        with_score: work.with_score && ctx.with_score,
        avg_dl: work.avg_dl,
        total_rows: work.total_rows,
        is_large: work.file_proj.is_large,
        indexed_batch_col: work.file_proj.indexed_batch_col,
        select_col_map: &work.file_proj.select_col_map,
    };

    match work.mode {
        VerifyMode::BruteForce => brute_force_verify_batch(&work.batch, &verify_ctx, &ctx.schema),
        VerifyMode::Indexed => verify_batch(&work.batch, &verify_ctx, &ctx.schema),
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
                match coalesce_batches(&schema, &mut pending) {
                    Ok(flushed) => {
                        pending_rows = 0;
                        if final_tx.send(Ok(flushed)).await.is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "coalescer concat failed");
                        pending_rows = 0;
                    }
                }
            }
        }

        // Flush remaining
        if let Ok(flushed) = coalesce_batches(&schema, &mut pending) {
            let _ = final_tx.send(Ok(flushed)).await;
        }
    });
}

/// Concatenates pending batches into a single batch and clears the buffer.
fn coalesce_batches(
    schema: &arrow::datatypes::SchemaRef,
    pending: &mut Vec<RecordBatch>,
) -> Result<RecordBatch> {
    let refs: Vec<&RecordBatch> = pending.iter().collect();
    let result = arrow::compute::concat_batches(schema, refs.iter().copied())?;
    pending.clear();
    Ok(result)
}
