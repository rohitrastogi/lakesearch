//! Stage 3: Fetch, verify, and score — reads Parquet pages, re-tokenizes rows,
//! produces `RecordBatch`es directly via Arrow column builders.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Builder, RecordBatch, Scalar,
    StringArray, StringBuilder,
};
use arrow::compute;
use arrow::datatypes::SchemaRef;
use futures::stream::{self, StreamExt, TryStreamExt};
use parquet::file::metadata::ParquetMetaData;

use lakesearch_core::bm25;
use lakesearch_core::runtime::LakeRuntime;
use lakesearch_core::tokenizer::tokenize;

use crate::object_cache::ObjectCache;
use crate::parquet_util::{build_row_selection, find_column, open_parquet_stream, string_value};
use crate::Operator;

use super::types::{IndexedWorkItem, QueryStats};

// ---------------------------------------------------------------------------
// Projection resolution
// ---------------------------------------------------------------------------

/// Resolved column projection: which parquet leaves to read and how they
/// map to batch column indices.
pub(crate) struct Projection {
    pub leaf_indices: Vec<usize>,
    pub indexed_batch_col: usize,
    pub select_col_map: Vec<(usize, String)>,
}

/// Resolves parquet leaf indices for the indexed column + select columns.
pub(crate) fn resolve_projection(
    pq_meta: &ParquetMetaData,
    column: &str,
    select_columns: &[String],
) -> Result<Projection> {
    let indexed_leaf = find_column(pq_meta, column)
        .with_context(|| format!("resolving indexed column '{column}'"))?;

    let mut select_leaves: Vec<(usize, String)> = Vec::new();
    for sel in select_columns {
        if sel == column {
            continue;
        }
        let leaf = find_column(pq_meta, sel)
            .with_context(|| format!("resolving select column '{sel}'"))?;
        select_leaves.push((leaf, sel.clone()));
    }

    let mut all_leaves: Vec<(usize, Option<String>)> = vec![(indexed_leaf, None)];
    for (leaf, name) in &select_leaves {
        all_leaves.push((*leaf, Some(name.clone())));
    }
    all_leaves.sort_by_key(|(leaf, _)| *leaf);

    let leaf_indices: Vec<usize> = all_leaves.iter().map(|(l, _)| *l).collect();
    let indexed_batch_col = all_leaves
        .iter()
        .position(|(l, _)| *l == indexed_leaf)
        .unwrap();
    let select_col_map: Vec<(usize, String)> = all_leaves
        .iter()
        .enumerate()
        .filter_map(|(idx, (_, name))| name.as_ref().map(|n| (idx, n.clone())))
        .collect();

    Ok(Projection {
        leaf_indices,
        indexed_batch_col,
        select_col_map,
    })
}

// ---------------------------------------------------------------------------
// CPU-bound verification
// ---------------------------------------------------------------------------

/// Context for row verification.
pub(crate) struct VerifyContext<'a> {
    pub query_terms: &'a [String],
    pub term_infos: &'a [(String, u32)],
    pub operator: Operator,
    pub with_score: bool,
    pub avg_dl: f64,
    pub total_rows: u64,
    pub is_large: bool,
    pub indexed_batch_col: usize,
    pub select_col_map: &'a [(usize, String)],
}

/// Target rows per output batch. Balances vectorization efficiency
/// against memory pressure and output latency.
pub(crate) const TARGET_BATCH_SIZE: usize = 8192;

/// Accumulates verified query results across multiple input batches.
///
/// Text and score columns are built row-by-row (they're computed values).
/// Select columns are projected via `arrow::compute::filter` and stored
/// as array chunks, then concatenated at flush time. This handles all
/// Arrow types without per-type dispatch.
pub(crate) struct ColumnBuilders {
    text: StringBuilder,
    /// Accumulated filtered chunks for each select column.
    select_chunks: Vec<(String, Vec<ArrayRef>)>,
    score: Option<Float64Builder>,
    count: usize,
}

impl ColumnBuilders {
    pub fn new(schema: &SchemaRef) -> Self {
        let text = StringBuilder::new();
        let mut select_chunks: Vec<(String, Vec<ArrayRef>)> = Vec::new();
        let mut score: Option<Float64Builder> = None;

        for field in schema.fields() {
            match field.name().as_str() {
                "text" | "score" => {
                    if field.name() == "score" {
                        score = Some(Float64Builder::new());
                    }
                }
                name => {
                    select_chunks.push((name.to_owned(), Vec::new()));
                }
            }
        }

        Self {
            text,
            select_chunks,
            score,
            count: 0,
        }
    }

    /// Returns true if enough rows have accumulated to justify flushing.
    pub fn should_flush(&self) -> bool {
        self.count >= TARGET_BATCH_SIZE
    }

    /// Materializes buffered rows as a `RecordBatch` and resets for reuse.
    /// Returns `None` if no rows are buffered.
    pub fn flush_batch(&mut self, schema: &SchemaRef) -> Option<RecordBatch> {
        if self.count == 0 {
            return None;
        }

        let mut columns: Vec<ArrayRef> = Vec::new();

        for field in schema.fields() {
            match field.name().as_str() {
                "text" => {
                    columns.push(Arc::new(self.text.finish()));
                }
                "score" => {
                    if let Some(ref mut b) = self.score {
                        columns.push(Arc::new(b.finish()));
                    }
                }
                _ => {
                    if let Some(pos) = self
                        .select_chunks
                        .iter()
                        .position(|(n, _)| n == field.name().as_str())
                    {
                        let (_, chunks) = &mut self.select_chunks[pos];
                        let refs: Vec<&dyn Array> = chunks.iter().map(|a| a.as_ref()).collect();
                        if refs.is_empty() {
                            columns.push(arrow::array::new_empty_array(field.data_type()));
                        } else {
                            match arrow::compute::concat(&refs) {
                                Ok(arr) => columns.push(arr),
                                Err(_) => {
                                    columns.push(arrow::array::new_empty_array(field.data_type()))
                                }
                            }
                        }
                        chunks.clear();
                    }
                }
            }
        }

        self.count = 0;
        RecordBatch::try_new(schema.clone(), columns).ok()
    }

    /// Consumes the builders, returning any remaining rows as a final batch.
    pub fn finish(mut self, schema: &SchemaRef) -> Option<RecordBatch> {
        self.flush_batch(schema)
    }
}

/// Verifies all rows in a single input batch. Builds a match mask via
/// tokenization, appends text + score for matched rows, and filters
/// select columns using `arrow::compute::filter` (type-generic).
///
/// Used by the indexed consumer path.
pub(crate) fn verify_batch(
    batch: &RecordBatch,
    ctx: &VerifyContext<'_>,
    builders: &mut ColumnBuilders,
    stats: &mut QueryStats,
) {
    let col = batch.column(ctx.indexed_batch_col);
    let mut match_mask = BooleanBuilder::with_capacity(batch.num_rows());

    for row in 0..batch.num_rows() {
        stats.rows_scanned += 1;
        if col.is_null(row) {
            match_mask.append_value(false);
            continue;
        }
        let matched = verify_row(col.as_ref(), row, ctx, builders);
        match_mask.append_value(matched);
        if matched {
            stats.rows_matched += 1;
        }
    }

    let mask = match_mask.finish();
    filter_select_columns(batch, &mask, ctx, builders);
}

/// CPU-bound: applies ilike pre-filter to a single batch, then verifies
/// matching rows via tokenization. Uses `arrow::compute::filter` for
/// select column projection (handles all Arrow types).
pub(crate) fn brute_force_verify_batch(
    batch: &RecordBatch,
    ctx: &VerifyContext<'_>,
    builders: &mut ColumnBuilders,
    stats: &mut QueryStats,
) {
    let col = batch.column(ctx.indexed_batch_col);

    // Arrow pre-filter: case-insensitive substring check via ILIKE
    let term_masks: Vec<BooleanArray> = ctx
        .query_terms
        .iter()
        .filter_map(|term| {
            let pattern = format!("%{term}%");
            let scalar = Scalar::new(StringArray::from(vec![pattern.as_str()]));
            arrow::compute::kernels::comparison::ilike(col, &scalar).ok()
        })
        .collect();

    let candidate_mask = if term_masks.is_empty() {
        return;
    } else {
        let mut mask = term_masks[0].clone();
        for m in &term_masks[1..] {
            mask = match ctx.operator {
                Operator::And => compute::and(&mask, m).unwrap_or(mask),
                Operator::Or => compute::or(&mask, m).unwrap_or(mask),
            };
        }
        mask
    };

    let mut match_mask = BooleanBuilder::with_capacity(batch.num_rows());

    for row in 0..batch.num_rows() {
        stats.rows_scanned += 1;
        if !candidate_mask.value(row) || col.is_null(row) {
            match_mask.append_value(false);
            continue;
        }
        let matched = verify_row(col.as_ref(), row, ctx, builders);
        match_mask.append_value(matched);
        if matched {
            stats.rows_matched += 1;
        }
    }

    let mask = match_mask.finish();
    filter_select_columns(batch, &mask, ctx, builders);
}

/// Tokenizes a single row, checks the boolean predicate, optionally scores,
/// and appends text + score to builders. Returns true if the row matched.
fn verify_row(
    col: &dyn Array,
    row: usize,
    ctx: &VerifyContext<'_>,
    builders: &mut ColumnBuilders,
) -> bool {
    let text = string_value(col, row, ctx.is_large);
    let tokens = tokenize(text);

    let single_term = ctx.query_terms.len() == 1;
    let few_terms = ctx.query_terms.len() <= 4;

    let matches_query = if single_term {
        tokens.iter().any(|t| t == &ctx.query_terms[0])
    } else if few_terms {
        match ctx.operator {
            Operator::And => ctx
                .query_terms
                .iter()
                .all(|q| tokens.iter().any(|t| t == q)),
            Operator::Or => ctx
                .query_terms
                .iter()
                .any(|q| tokens.iter().any(|t| t == q)),
        }
    } else {
        let token_set: HashSet<&str> = tokens.iter().map(|s| s.as_str()).collect();
        match ctx.operator {
            Operator::And => ctx
                .query_terms
                .iter()
                .all(|t| token_set.contains(t.as_str())),
            Operator::Or => ctx
                .query_terms
                .iter()
                .any(|t| token_set.contains(t.as_str())),
        }
    };

    if !matches_query {
        return false;
    }

    builders.text.append_value(text);

    if let Some(ref mut score_builder) = builders.score {
        if ctx.with_score {
            let dl = tokens.len() as u32;
            let mut freq: HashMap<&str, u32> = HashMap::new();
            for t in &tokens {
                *freq.entry(t.as_str()).or_default() += 1;
            }
            let mut total_score = 0.0;
            for (term, df) in ctx.term_infos {
                if let Some(&tf) = freq.get(term.as_str()) {
                    total_score += bm25::score(tf, dl, ctx.avg_dl, *df as u64, ctx.total_rows);
                }
            }
            score_builder.append_value(total_score);
        } else {
            score_builder.append_null();
        }
    }

    builders.count += 1;
    true
}

/// Filters select columns from the source batch using the match mask and
/// stores the filtered arrays in the builders for later concatenation.
/// Uses `arrow::compute::filter` which handles all Arrow types.
fn filter_select_columns(
    batch: &RecordBatch,
    mask: &BooleanArray,
    ctx: &VerifyContext<'_>,
    builders: &mut ColumnBuilders,
) {
    for (sel_name, chunks) in &mut builders.select_chunks {
        if let Some((src_idx, _)) = ctx
            .select_col_map
            .iter()
            .find(|(_, name)| name == sel_name.as_str())
        {
            let src_col = batch.column(*src_idx);
            if let Ok(filtered) = compute::filter(src_col.as_ref(), mask) {
                if !filtered.is_empty() {
                    chunks.push(filtered);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Streaming consumers
// ---------------------------------------------------------------------------

/// Runs the indexed path: processes work items by streaming Parquet pages,
/// verifying on the CPU pool, coalescing results, and sending batches.
///
/// Runs as a standalone tokio task. Returns accumulated `QueryStats`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn indexed_consumer(
    work_items: Vec<IndexedWorkItem>,
    cache: Arc<ObjectCache>,
    column: String,
    query_terms: Arc<Vec<String>>,
    operator: Operator,
    with_score: bool,
    select_columns: Arc<Vec<String>>,
    schema: SchemaRef,
    io_concurrency: usize,
    runtime: Arc<LakeRuntime>,
    tx: tokio::sync::mpsc::Sender<Result<RecordBatch>>,
) -> QueryStats {
    let mut stats = QueryStats::default();
    let mut builders = ColumnBuilders::new(&schema);

    // Pre-fetch parquet metadata for all unique files
    let unique_files: Vec<String> = work_items
        .iter()
        .map(|w| w.file_path.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let pq_metas: HashMap<String, Arc<ParquetMetaData>> = match stream::iter(unique_files)
        .map(|fp| {
            let cache = Arc::clone(&cache);
            async move { cache.get_parquet_metadata(&fp).await.map(|m| (fp, m)) }
        })
        .buffered(io_concurrency)
        .try_collect::<Vec<_>>()
        .await
    {
        Ok(v) => v.into_iter().collect(),
        Err(e) => {
            let _ = tx.send(Err(e)).await;
            return stats;
        }
    };

    for item in &work_items {
        let pq_meta = &pq_metas[&item.file_path];
        let projection = match resolve_projection(pq_meta, &column, &select_columns) {
            Ok(p) => p,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return stats;
            }
        };

        stats.candidate_pages += item.entries.len();

        let rg_total_rows = pq_meta.row_group(item.rg_idx as usize).num_rows();
        let entry_refs: Vec<&lakesearch_core::types::DocTableEntry> = item.entries.iter().collect();
        let selection = build_row_selection(&entry_refs, rg_total_rows);

        // Detect is_large from parquet metadata
        let arrow_schema = match parquet::arrow::parquet_to_arrow_schema(
            pq_meta.file_metadata().schema_descr(),
            pq_meta.file_metadata().key_value_metadata(),
        ) {
            Ok(s) => s,
            Err(e) => {
                let _ = tx.send(Err(e.into())).await;
                return stats;
            }
        };
        let is_large = arrow_schema
            .field_with_name(&column)
            .map(|f| f.data_type() == &arrow::datatypes::DataType::LargeUtf8)
            .unwrap_or(false);

        let mut pq_stream = match open_parquet_stream(
            cache.store(),
            &item.file_path,
            item.rg_idx as usize,
            &projection.leaf_indices,
            Some(selection),
        )
        .await
        {
            Ok(s) => s,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return stats;
            }
        };

        // Prime the pipeline
        let mut next_batch = match pq_stream.try_next().await {
            Ok(b) => b,
            Err(e) => {
                let _ = tx.send(Err(e.into())).await;
                return stats;
            }
        };

        while let Some(input_batch) = next_batch {
            let qt = Arc::clone(&query_terms);
            let ti = Arc::clone(&item.term_infos);
            let scm = projection.select_col_map.clone();
            let ibc = projection.indexed_batch_col;
            let avg_dl = item.avg_dl;
            let total_rows = item.total_rows;

            let cpu_future = runtime.cpu(move || {
                let ctx = VerifyContext {
                    query_terms: &qt,
                    term_infos: &ti,
                    operator,
                    with_score,
                    avg_dl,
                    total_rows,
                    is_large,
                    indexed_batch_col: ibc,
                    select_col_map: &scm,
                };
                let mut batch_stats = QueryStats::default();
                verify_batch(&input_batch, &ctx, &mut builders, &mut batch_stats);
                (builders, batch_stats)
            });

            // Overlap: I/O fetches batch N+1 while CPU processes batch N
            let (cpu_result, prefetched) = tokio::join!(cpu_future, pq_stream.try_next());

            let (returned_builders, batch_stats) = cpu_result;
            builders = returned_builders;
            stats.merge(&batch_stats);

            next_batch = match prefetched {
                Ok(b) => b,
                Err(e) => {
                    let _ = tx.send(Err(e.into())).await;
                    return stats;
                }
            };

            if builders.should_flush() {
                if let Some(batch) = builders.flush_batch(&schema) {
                    if tx.send(Ok(batch)).await.is_err() {
                        return stats;
                    }
                }
            }
        }
    }

    // Final drain
    if let Some(batch) = builders.finish(&schema) {
        let _ = tx.send(Ok(batch)).await;
    }

    stats
}

/// Runs the brute-force path: scans un-indexed files row-by-row,
/// using ilike as a fast pre-filter, then re-tokenizes candidates.
///
/// Runs as a standalone tokio task. Returns accumulated `QueryStats`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn brute_force_consumer(
    files: Vec<String>,
    cache: Arc<ObjectCache>,
    column: String,
    query_terms: Arc<Vec<String>>,
    operator: Operator,
    with_score: bool,
    select_columns: Arc<Vec<String>>,
    agg_total_rows: u64,
    agg_avg_dl: f64,
    agg_term_infos: Arc<Vec<(String, u32)>>,
    limit: Option<usize>,
    schema: SchemaRef,
    runtime: Arc<LakeRuntime>,
    tx: tokio::sync::mpsc::Sender<Result<RecordBatch>>,
) -> QueryStats {
    let mut stats = QueryStats::default();
    let mut builders = ColumnBuilders::new(&schema);

    for file_path in &files {
        let pq_meta = match cache.get_parquet_metadata(file_path).await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(file = %file_path, error = %e, "skipping file");
                continue;
            }
        };

        if find_column(&pq_meta, &column).is_err() {
            tracing::warn!(file = %file_path, column = %column, "skipping: column not found");
            continue;
        }

        let projection = match resolve_projection(&pq_meta, &column, &select_columns) {
            Ok(p) => p,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return stats;
            }
        };

        // Detect is_large from parquet metadata
        let arrow_schema = match parquet::arrow::parquet_to_arrow_schema(
            pq_meta.file_metadata().schema_descr(),
            pq_meta.file_metadata().key_value_metadata(),
        ) {
            Ok(s) => s,
            Err(e) => {
                let _ = tx.send(Err(e.into())).await;
                return stats;
            }
        };
        let is_large = arrow_schema
            .field_with_name(&column)
            .map(|f| f.data_type() == &arrow::datatypes::DataType::LargeUtf8)
            .unwrap_or(false);

        for rg_idx in 0..pq_meta.num_row_groups() {
            let mut pq_stream = match open_parquet_stream(
                cache.store(),
                file_path,
                rg_idx,
                &projection.leaf_indices,
                None,
            )
            .await
            {
                Ok(s) => s,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return stats;
                }
            };

            // Prime the pipeline
            let mut next_batch = match pq_stream.try_next().await {
                Ok(b) => b,
                Err(e) => {
                    let _ = tx.send(Err(e.into())).await;
                    return stats;
                }
            };

            while let Some(input_batch) = next_batch {
                let qt = Arc::clone(&query_terms);
                let ti = Arc::clone(&agg_term_infos);
                let scm = projection.select_col_map.clone();
                let ibc = projection.indexed_batch_col;

                let cpu_future = runtime.cpu(move || {
                    let ctx = VerifyContext {
                        query_terms: &qt,
                        term_infos: &ti,
                        operator,
                        with_score,
                        avg_dl: agg_avg_dl,
                        total_rows: agg_total_rows,
                        is_large,
                        indexed_batch_col: ibc,
                        select_col_map: &scm,
                    };
                    let mut batch_stats = QueryStats::default();
                    brute_force_verify_batch(&input_batch, &ctx, &mut builders, &mut batch_stats);
                    (builders, batch_stats)
                });

                // Overlap: I/O fetches batch N+1 while CPU processes batch N
                let (cpu_result, prefetched) = tokio::join!(cpu_future, pq_stream.try_next());

                let (returned_builders, batch_stats) = cpu_result;
                builders = returned_builders;
                stats.merge(&batch_stats);

                next_batch = match prefetched {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = tx.send(Err(e.into())).await;
                        return stats;
                    }
                };

                if builders.should_flush() {
                    if let Some(batch) = builders.flush_batch(&schema) {
                        if tx.send(Ok(batch)).await.is_err() {
                            return stats;
                        }
                    }
                }
            }

            // Early termination for unscored queries with limit
            if let Some(lim) = limit {
                if stats.rows_matched >= lim {
                    if let Some(batch) = builders.finish(&schema) {
                        let _ = tx.send(Ok(batch)).await;
                    }
                    return stats;
                }
            }
        }
    }

    // Final drain
    if let Some(batch) = builders.finish(&schema) {
        let _ = tx.send(Ok(batch)).await;
    }

    stats
}
