//! Stage 3: Verify and score — re-tokenizes rows from Parquet batches,
//! produces `RecordBatch`es via pure functions.
//!
//! All functions are stateless: `(RecordBatch, VerifyContext, Schema) →
//! (Option<RecordBatch>, QueryStats)`. I/O and coalescing are handled by
//! the pipeline module.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Builder, RecordBatch, Scalar,
    StringArray, StringBuilder,
};
use arrow::compute;
use arrow::datatypes::SchemaRef;
use parquet::file::metadata::ParquetMetaData;

use lakesearch_core::bm25;
use lakesearch_core::tokenizer::tokenize;

use crate::Operator;
use lakesearch_core::parquet_util::{find_column, string_value, validate_arrow_column};

use super::types::{QueryStats, SCORE_COL, TEXT_COL};

// ---------------------------------------------------------------------------
// Projection resolution
// ---------------------------------------------------------------------------

/// Resolved column projection: which parquet leaves to read and how they
/// map to batch column indices.
pub(crate) struct Projection {
    pub leaf_indices: Vec<usize>,
    pub indexed_batch_col: usize,
    pub select_col_map: Arc<[(usize, String)]>,
    /// True when the indexed column is LargeUtf8 (vs Utf8).
    pub is_large: bool,
}

/// Resolves parquet leaf indices for the indexed column + select columns.
/// Also detects whether the indexed column is LargeUtf8.
pub(crate) fn resolve_projection(
    pq_meta: &ParquetMetaData,
    column: &str,
    select_columns: &[String],
) -> Result<Projection> {
    let indexed_leaf = find_column(pq_meta, column)
        .with_context(|| format!("resolving indexed column '{column}'"))?;

    let arrow_schema = parquet::arrow::parquet_to_arrow_schema(
        pq_meta.file_metadata().schema_descr(),
        pq_meta.file_metadata().key_value_metadata(),
    )
    .context("converting parquet schema to arrow")?;
    let is_large = validate_arrow_column(&arrow_schema, column)?;

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
    let select_col_map: Arc<[(usize, String)]> = all_leaves
        .iter()
        .enumerate()
        .filter_map(|(idx, (_, name))| name.as_ref().map(|n| (idx, n.clone())))
        .collect();

    Ok(Projection {
        leaf_indices,
        indexed_batch_col,
        select_col_map,
        is_large,
    })
}

// ---------------------------------------------------------------------------
// CPU-bound verification (pure functions)
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

/// Verifies all rows in a single input batch. Pure function: takes a batch
/// and context, returns matched rows as a new RecordBatch (or None) and stats.
///
/// Builds a match mask via row-by-row tokenization, constructs text + score
/// for matched rows, filters select columns via `arrow::compute::filter`.
pub(crate) fn verify_batch(
    batch: &RecordBatch,
    ctx: &VerifyContext<'_>,
    schema: &SchemaRef,
) -> (Option<RecordBatch>, QueryStats) {
    verify_rows(batch, ctx, schema, None)
}

/// CPU-bound: applies ilike pre-filter to a single batch, then verifies
/// matching rows via tokenization. Pure function returning matched rows.
pub(crate) fn brute_force_verify_batch(
    batch: &RecordBatch,
    ctx: &VerifyContext<'_>,
    schema: &SchemaRef,
) -> (Option<RecordBatch>, QueryStats) {
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

    if term_masks.is_empty() {
        let mut stats = QueryStats::default();
        stats.rows_scanned += batch.num_rows();
        return (None, stats);
    }

    let mut candidate = term_masks[0].clone();
    for m in &term_masks[1..] {
        candidate = match ctx.operator {
            Operator::And => compute::and(&candidate, m).unwrap_or(candidate),
            Operator::Or => compute::or(&candidate, m).unwrap_or(candidate),
        };
    }

    verify_rows(batch, ctx, schema, Some(&candidate))
}

/// Shared row-verification loop used by both indexed and brute-force paths.
/// When `pre_filter` is `Some`, rows that fail the pre-filter are skipped
/// without tokenization.
fn verify_rows(
    batch: &RecordBatch,
    ctx: &VerifyContext<'_>,
    schema: &SchemaRef,
    pre_filter: Option<&BooleanArray>,
) -> (Option<RecordBatch>, QueryStats) {
    let mut stats = QueryStats::default();
    let col = batch.column(ctx.indexed_batch_col);
    let mut match_mask = BooleanBuilder::with_capacity(batch.num_rows());
    let mut text_builder = StringBuilder::new();
    let mut score_builder = if ctx.with_score {
        Some(Float64Builder::new())
    } else {
        None
    };

    for row in 0..batch.num_rows() {
        stats.rows_scanned += 1;
        let skip = col.is_null(row) || pre_filter.is_some_and(|pf| !pf.value(row));
        if skip {
            match_mask.append_value(false);
            continue;
        }
        let text = string_value(col.as_ref(), row, ctx.is_large);
        let tokens = tokenize(text);

        if !matches_predicate(&tokens, ctx.query_terms, ctx.operator) {
            match_mask.append_value(false);
            continue;
        }

        match_mask.append_value(true);
        stats.rows_matched += 1;
        text_builder.append_value(text);

        if let Some(ref mut sb) = score_builder {
            sb.append_value(compute_bm25(&tokens, ctx));
        }
    }

    if stats.rows_matched == 0 {
        return (None, stats);
    }

    let mask = match_mask.finish();
    let batch = build_output_batch(schema, text_builder, score_builder, batch, &mask, ctx);
    (batch, stats)
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

/// Checks whether the tokenized row matches the boolean query predicate.
fn matches_predicate(tokens: &[String], query_terms: &[String], operator: Operator) -> bool {
    let single_term = query_terms.len() == 1;
    let few_terms = query_terms.len() <= 4;

    if single_term {
        tokens.iter().any(|t| t == &query_terms[0])
    } else if few_terms {
        match operator {
            Operator::And => query_terms.iter().all(|q| tokens.iter().any(|t| t == q)),
            Operator::Or => query_terms.iter().any(|q| tokens.iter().any(|t| t == q)),
        }
    } else {
        let token_set: HashSet<&str> = tokens.iter().map(|s| s.as_str()).collect();
        match operator {
            Operator::And => query_terms.iter().all(|t| token_set.contains(t.as_str())),
            Operator::Or => query_terms.iter().any(|t| token_set.contains(t.as_str())),
        }
    }
}

/// Computes BM25 score for a matched row.
fn compute_bm25(tokens: &[String], ctx: &VerifyContext<'_>) -> f64 {
    let dl = tokens.len() as u32;
    let mut freq: HashMap<&str, u32> = HashMap::new();
    for t in tokens {
        *freq.entry(t.as_str()).or_default() += 1;
    }
    let mut total_score = 0.0;
    for (term, df) in ctx.term_infos {
        if let Some(&tf) = freq.get(term.as_str()) {
            total_score += bm25::score(tf, dl, ctx.avg_dl, *df as u64, ctx.total_rows);
        }
    }
    total_score
}

/// Assembles a RecordBatch from matched-row builders, select column filters,
/// and the output schema.
fn build_output_batch(
    schema: &SchemaRef,
    text_builder: StringBuilder,
    score_builder: Option<Float64Builder>,
    source_batch: &RecordBatch,
    mask: &BooleanArray,
    ctx: &VerifyContext<'_>,
) -> Option<RecordBatch> {
    let mut text_builder = text_builder;
    let mut score_builder = score_builder;
    let mut columns: Vec<ArrayRef> = Vec::new();

    for field in schema.fields() {
        match field.name().as_str() {
            TEXT_COL => {
                columns.push(Arc::new(text_builder.finish()));
            }
            SCORE_COL => {
                if let Some(ref mut sb) = score_builder {
                    columns.push(Arc::new(sb.finish()));
                }
            }
            name => {
                if let Some((src_idx, _)) = ctx.select_col_map.iter().find(|(_, n)| n == name) {
                    let src_col = source_batch.column(*src_idx);
                    let filtered = compute::filter(src_col.as_ref(), mask)
                        .expect("filter on matched mask should not fail");
                    columns.push(filtered);
                }
            }
        }
    }

    Some(
        RecordBatch::try_new(schema.clone(), columns)
            .expect("output schema/columns mismatch is a bug"),
    )
}
