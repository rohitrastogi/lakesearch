//! Stage 1: Query planning — load segments and identify un-indexed files.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use futures::stream::{self, StreamExt, TryStreamExt};

use lakesearch_core::metadata::{Manifest, ManifestList, TermStats};
use lakesearch_core::segment::SegmentReader;

use lakesearch_core::tokenizer::QueryTerm;

use crate::object_cache::ObjectCache;
use crate::Operator;
use lakesearch_core::storage::{read_current, read_metadata};

use object_store::path::Path;

/// Result of query planning: segments to search and files needing brute-force.
pub(crate) struct QueryPlan {
    pub segments: Vec<Vec<u8>>,
    /// Parquet file paths that are not yet indexed for the target column.
    pub unindexed_files: Vec<String>,
}

/// Loads metadata, resolves manifests, prunes by term stats, fetches segment
/// bytes. Also identifies un-indexed files that need brute-force scanning.
pub(crate) async fn plan_query(
    cache: &Arc<ObjectCache>,
    base: &Path,
    column: &str,
    query_terms: &[QueryTerm],
    operator: Operator,
    io_concurrency: usize,
) -> Result<QueryPlan> {
    // Read metadata chain
    let current = read_current(cache.store().as_ref(), base).await?;
    let metadata = read_metadata(cache.store().as_ref(), &current.value).await?;

    // Load manifest lists in parallel
    let manifest_lists: Vec<ManifestList> =
        stream::iter(metadata.snapshot.manifest_lists.into_iter())
            .map(|ml_path| {
                let cache = Arc::clone(cache);
                async move { cache.get_json(&ml_path).await }
            })
            .buffered(io_concurrency)
            .try_collect()
            .await?;

    // Single pass: collect all data files and all column manifest paths.
    let mut all_files: HashSet<String> = HashSet::new();
    let mut col_manifest_paths: Vec<String> = Vec::new();
    let mut pruned_paths: HashSet<String> = HashSet::new();

    for ml in &manifest_lists {
        for df in &ml.data_files {
            all_files.insert(df.path.clone());
        }
        for me in &ml.manifests {
            if me.indexed_column != column {
                continue;
            }
            col_manifest_paths.push(me.manifest_path.clone());
            if should_prune_segment(&me.term_stats, query_terms, operator) {
                pruned_paths.insert(me.manifest_path.clone());
            }
        }
    }

    // Load all column manifests once
    let col_manifests: Vec<Manifest> = stream::iter(col_manifest_paths.iter().cloned())
        .map(|path| {
            let cache = Arc::clone(cache);
            async move { cache.get_json(&path).await }
        })
        .buffered(io_concurrency)
        .try_collect()
        .await?;

    // Walk all manifests to determine indexed files + collect segment paths
    let mut indexed_files: HashSet<String> = HashSet::new();
    let mut segment_paths: Vec<String> = Vec::new();

    for (path, manifest) in col_manifest_paths.iter().zip(col_manifests.iter()) {
        for seg in &manifest.segments {
            for pf in &seg.parquet_files {
                indexed_files.insert(pf.path.clone());
            }
            if !pruned_paths.contains(path) {
                segment_paths.push(seg.segment_path.clone());
            }
        }
    }

    let unindexed_files: Vec<String> = all_files.difference(&indexed_files).cloned().collect();

    // Load segment bytes in parallel
    let segments = stream::iter(segment_paths.into_iter())
        .map(|path| {
            let cache = Arc::clone(cache);
            async move { cache.get_bytes(&path).await.map(|b| b.to_vec()) }
        })
        .buffered(io_concurrency)
        .try_collect()
        .await?;

    Ok(QueryPlan {
        segments,
        unindexed_files,
    })
}

/// Returns true if the segment can be skipped based on term stats.
/// Wildcard terms are never pruned (they could match any range).
fn should_prune_segment(
    term_stats: &TermStats,
    query_terms: &[QueryTerm],
    operator: Operator,
) -> bool {
    if term_stats.min_term.is_empty() {
        return false;
    }
    let in_range = |qt: &QueryTerm| -> bool {
        if qt.is_wildcard() {
            return true; // Never prune wildcards
        }
        let t = qt.term();
        t >= term_stats.min_term.as_str() && t <= term_stats.max_term.as_str()
    };
    match operator {
        Operator::And => query_terms.iter().any(|t| !in_range(t)),
        Operator::Or => query_terms.iter().all(|t| !in_range(t)),
    }
}

/// Resolves the output schema from table metadata without planning a full query.
///
/// Loads the metadata chain, finds any data file, and reads its Parquet
/// schema to build the result schema. Used by Flight `get_flight_info` to
/// return schema without executing a query.
pub async fn resolve_schema_from_table(
    cache: &Arc<ObjectCache>,
    base: &Path,
    column: &str,
    select_columns: &[String],
    with_score: bool,
) -> Result<arrow::datatypes::SchemaRef> {
    use super::{build_empty_schema, build_result_schema};

    let current = lakesearch_core::storage::read_current(cache.store().as_ref(), base).await?;
    let metadata =
        lakesearch_core::storage::read_metadata(cache.store().as_ref(), &current.value).await?;

    // Try to find any data file from manifest lists to read its parquet schema.
    for ml_path in &metadata.snapshot.manifest_lists {
        let ml: lakesearch_core::metadata::ManifestList = cache.get_json(ml_path).await?;
        for df in &ml.data_files {
            if let Ok(pq_meta) = cache.get_parquet_metadata(&df.path).await {
                return build_result_schema(&pq_meta, column, select_columns, with_score);
            }
        }
    }

    // Fallback when no data files exist yet.
    Ok(build_empty_schema(select_columns, column, with_score))
}

/// Resolves the output schema from available parquet metadata.
pub(crate) async fn resolve_schema(
    cache: &Arc<ObjectCache>,
    plan: &QueryPlan,
    column: &str,
    select_columns: &[String],
    with_score: bool,
) -> Result<arrow::datatypes::SchemaRef> {
    use super::{build_empty_schema, build_result_schema};

    // Try to get metadata from the first segment's first file
    for seg_bytes in &plan.segments {
        if let Ok(reader) = SegmentReader::open(seg_bytes.clone()) {
            let file_table = reader.file_table();
            if let Some(first_file) = file_table.first() {
                if let Ok(pq_meta) = cache.get_parquet_metadata(&first_file.path).await {
                    return build_result_schema(&pq_meta, column, select_columns, with_score);
                }
            }
        }
    }
    // Try unindexed files
    for file_path in &plan.unindexed_files {
        if let Ok(pq_meta) = cache.get_parquet_metadata(file_path).await {
            return build_result_schema(&pq_meta, column, select_columns, with_score);
        }
    }
    // Fallback
    Ok(build_empty_schema(select_columns, column, with_score))
}
