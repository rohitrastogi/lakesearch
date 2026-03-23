//! Async multi-segment query execution.
//!
//! The query pipeline has four stages:
//! 1. **Load segments** — read metadata chain, prune by term stats, fetch bytes
//! 2. **Evaluate segments** — look up posting lists, combine with boolean ops
//! 3. **Fetch and verify** — read Parquet pages, re-tokenize rows, score
//! 4. **Rank results** — top-K heap or sort+truncate

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use futures::stream::{self, StreamExt, TryStreamExt};
use tracing::info;

use lakesearch_core::bm25;
use lakesearch_core::boolean;
use lakesearch_core::metadata::{Manifest, ManifestList, TermStats};
use lakesearch_core::runtime::LakeRuntime;
use lakesearch_core::segment::SegmentReader;
use lakesearch_core::tokenizer::tokenize;
use lakesearch_core::types::{DocId, DocTableEntry};
use object_store::path::Path;
use parquet::file::metadata::ParquetMetaData;

use serde::{Deserialize, Serialize};

use arrow::array::{Array, BooleanArray, Scalar, StringArray};
use arrow::compute;

use crate::object_cache::ObjectCache;
use crate::parquet_util::{
    arrow_value_to_json, build_row_selection, read_parquet_batches_async, string_value,
    validate_column,
};
use crate::storage::{read_current, read_metadata};
use crate::Operator;

/// Max concurrent I/O operations for parallel loading.
const IO_CONCURRENCY: usize = 8;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResult {
    pub matches: Vec<MatchedRow>,
    pub stats: QueryStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MatchedRow {
    pub file: String,
    pub row_group: u16,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct QueryStats {
    pub candidate_pages: usize,
    pub rows_scanned: usize,
    pub rows_matched: usize,
}

// ---------------------------------------------------------------------------
// Orchestrator
// ---------------------------------------------------------------------------

/// Queries a LakeSearch table across all segments.
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
    runtime: Arc<LakeRuntime>,
) -> Result<QueryResult> {
    let with_score = score_mode != crate::ScoreMode::None;
    let query_terms = tokenize(query_text);
    if query_terms.is_empty() {
        return Ok(QueryResult {
            matches: vec![],
            stats: QueryStats::default(),
        });
    }

    // Stage 1: plan query — load segments + identify un-indexed files
    let plan = plan_query(&cache, &base, &column, &query_terms, operator).await?;

    let mut all_matches = Vec::new();
    let mut stats = QueryStats::default();

    // Stage 2+3: indexed path — evaluate segments and fetch/verify
    // Also collect aggregate corpus stats for brute-force scoring.
    let mut agg_total_rows: u64 = 0;
    let mut agg_total_tokens: u64 = 0;
    let mut agg_term_df: HashMap<String, u64> = HashMap::new();

    if !plan.segments.is_empty() {
        info!(segments = plan.segments.len(), "loaded segments");
        for seg_bytes in plan.segments {
            let reader = SegmentReader::open(seg_bytes).context("opening segment")?;

            // Accumulate corpus stats across segments
            let cs = reader.corpus_stats();
            agg_total_rows += cs.total_rows;
            agg_total_tokens += cs.total_tokens;
            for term in &query_terms {
                if let Some(ord) = reader.term_ordinal(term) {
                    if let Ok(info) = reader.term_info(ord) {
                        *agg_term_df.entry(term.clone()).or_default() += info.doc_frequency as u64;
                    }
                }
            }

            let candidates = evaluate_segment(&reader, &query_terms, operator)?;
            if candidates.is_empty() {
                continue;
            }

            let (mut matches, scan_stats) = fetch_and_verify(
                &cache,
                &reader,
                &candidates,
                &column,
                &query_terms,
                operator,
                with_score,
                &select_columns,
                &runtime,
            )
            .await?;

            stats.candidate_pages += scan_stats.candidate_pages;
            stats.rows_scanned += scan_stats.rows_scanned;
            stats.rows_matched += scan_stats.rows_matched;
            all_matches.append(&mut matches);
        }
    }

    // Stage 3b: brute-force path — scan un-indexed files
    if !plan.unindexed_files.is_empty() {
        info!(
            unindexed_files = plan.unindexed_files.len(),
            "brute-force scanning un-indexed files"
        );

        // Build aggregate term infos for brute-force scoring
        let bf_score = score_mode == crate::ScoreMode::All;
        let agg_avg_dl = bm25::avg_dl(agg_total_tokens, agg_total_rows);
        let agg_term_infos: Vec<(String, u32)> = query_terms
            .iter()
            .map(|t| {
                let df = agg_term_df.get(t).copied().unwrap_or(1) as u32;
                (t.clone(), df)
            })
            .collect();

        let (mut matches, scan_stats) = brute_force_scan(
            &cache,
            &plan.unindexed_files,
            &column,
            &query_terms,
            operator,
            bf_score,
            &select_columns,
            agg_total_rows,
            agg_avg_dl,
            &agg_term_infos,
            &runtime,
        )
        .await?;

        stats.rows_scanned += scan_stats.rows_scanned;
        stats.rows_matched += scan_stats.rows_matched;
        all_matches.append(&mut matches);
    }

    // Stage 4: rank and limit
    rank_results(&mut all_matches, with_score, limit);

    info!(
        candidate_pages = stats.candidate_pages,
        rows_scanned = stats.rows_scanned,
        rows_matched = stats.rows_matched,
        "query complete"
    );

    Ok(QueryResult {
        matches: all_matches,
        stats,
    })
}

// ---------------------------------------------------------------------------
// Stage 1: Load segments
// ---------------------------------------------------------------------------

/// Result of query planning: segments to search and files needing brute-force.
struct QueryPlan {
    segments: Vec<Vec<u8>>,
    /// Parquet file paths that are not yet indexed for the target column.
    unindexed_files: Vec<String>,
}

/// Loads metadata, resolves manifests, prunes by term stats, fetches segment
/// bytes. Also identifies un-indexed files that need brute-force scanning.
async fn plan_query(
    cache: &Arc<ObjectCache>,
    base: &Path,
    column: &str,
    query_terms: &[String],
    operator: Operator,
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
            .buffered(IO_CONCURRENCY)
            .try_collect()
            .await?;

    // Derive full file set and indexed file set
    let mut all_files: HashSet<String> = HashSet::new();
    let mut indexed_files: HashSet<String> = HashSet::new();

    // Collect manifest paths for the target column, pruning by term stats
    let mut manifest_paths: Vec<String> = Vec::new();
    for ml in &manifest_lists {
        // All data_files across all manifest lists = full file inventory
        for df in &ml.data_files {
            all_files.insert(df.path.clone());
        }
        for me in &ml.manifests {
            if me.indexed_column != column {
                continue;
            }
            // Files referenced by manifests for this column are indexed
            // (we'll collect them after loading the manifests)
            if should_prune_segment(&me.term_stats, query_terms, operator) {
                continue;
            }
            manifest_paths.push(me.manifest_path.clone());
        }
    }

    // Also collect ALL manifest paths for the column (not just non-pruned)
    // to determine which files are indexed
    let mut all_col_manifest_paths: Vec<String> = Vec::new();
    for ml in &manifest_lists {
        for me in &ml.manifests {
            if me.indexed_column == column {
                all_col_manifest_paths.push(me.manifest_path.clone());
            }
        }
    }

    // Load all manifests for this column to determine indexed files
    let all_col_manifests: Vec<Manifest> = stream::iter(all_col_manifest_paths.into_iter())
        .map(|path| {
            let cache = Arc::clone(cache);
            async move { cache.get_json(&path).await }
        })
        .buffered(IO_CONCURRENCY)
        .try_collect()
        .await?;

    for manifest in &all_col_manifests {
        for seg in &manifest.segments {
            for pf in &seg.parquet_files {
                indexed_files.insert(pf.path.clone());
            }
        }
    }

    // Un-indexed files = all_files - indexed_files
    let unindexed_files: Vec<String> = all_files.difference(&indexed_files).cloned().collect();

    // Load non-pruned manifests (subset we already fetched above — use cache)
    let manifests: Vec<Manifest> = stream::iter(manifest_paths.into_iter())
        .map(|path| {
            let cache = Arc::clone(cache);
            async move { cache.get_json(&path).await }
        })
        .buffered(IO_CONCURRENCY)
        .try_collect()
        .await?;

    // Collect segment paths from surviving manifests
    let mut segment_paths: Vec<String> = Vec::new();
    for manifest in &manifests {
        for seg in &manifest.segments {
            segment_paths.push(seg.segment_path.clone());
        }
    }

    // Load segment bytes in parallel
    let segments = stream::iter(segment_paths.into_iter())
        .map(|path| {
            let cache = Arc::clone(cache);
            async move { cache.get_bytes(&path).await.map(|b| b.to_vec()) }
        })
        .buffered(IO_CONCURRENCY)
        .try_collect()
        .await?;

    Ok(QueryPlan {
        segments,
        unindexed_files,
    })
}

/// Returns true if the segment can be skipped based on term stats.
fn should_prune_segment(
    term_stats: &TermStats,
    query_terms: &[String],
    operator: Operator,
) -> bool {
    if term_stats.min_term.is_empty() {
        return false; // No stats, can't prune
    }
    let in_range = |t: &str| t >= term_stats.min_term.as_str() && t <= term_stats.max_term.as_str();
    match operator {
        // AND: skip if any term is definitely missing
        Operator::And => query_terms.iter().any(|t| !in_range(t)),
        // OR: skip only if all terms are definitely missing
        Operator::Or => query_terms.iter().all(|t| !in_range(t)),
    }
}

// ---------------------------------------------------------------------------
// Stage 2: Evaluate segment
// ---------------------------------------------------------------------------

/// Looks up posting lists for each query term and combines them with boolean
/// ops. For AND queries, terms are sorted by doc_frequency (rarest first) so
/// the smallest posting list drives the intersection, reducing intermediate
/// sizes and downstream work.
fn evaluate_segment(
    reader: &SegmentReader,
    query_terms: &[String],
    operator: Operator,
) -> Result<Vec<DocId>> {
    // Collect (doc_frequency, posting_list) per term
    let mut entries: Vec<(u32, Vec<DocId>)> = Vec::new();
    for term in query_terms {
        match reader.term_ordinal(term) {
            Some(ord) => {
                let info = reader.term_info(ord)?;
                let postings = reader.posting_list(ord)?;
                entries.push((info.doc_frequency, postings));
            }
            None => {
                if operator == Operator::And {
                    return Ok(vec![]);
                }
            }
        }
    }

    if entries.is_empty() {
        return Ok(vec![]);
    }

    // For AND: sort by doc_frequency ascending (rarest first) to minimize
    // intermediate result sizes during intersection.
    if operator == Operator::And {
        entries.sort_by_key(|(df, _)| *df);
    }

    let mut combined = entries.swap_remove(0).1;
    for (_, postings) in &entries {
        combined = match operator {
            Operator::And => boolean::intersect(&combined, postings),
            Operator::Or => boolean::union(&combined, postings),
        };
    }
    Ok(combined)
}

// ---------------------------------------------------------------------------
// Stage 3: Fetch and verify
// ---------------------------------------------------------------------------

/// Groups candidates by (file, row_group), reads Parquet pages, verifies
/// rows on the CPU pool, and optionally scores with BM25.
#[allow(clippy::too_many_arguments)]
async fn fetch_and_verify(
    cache: &Arc<ObjectCache>,
    reader: &SegmentReader,
    candidates: &[DocId],
    column: &str,
    query_terms: &[String],
    operator: Operator,
    with_score: bool,
    select_columns: &[String],
    runtime: &Arc<LakeRuntime>,
) -> Result<(Vec<MatchedRow>, QueryStats)> {
    // Group by (file_ordinal, row_group)
    let mut groups: BTreeMap<(u32, u16), Vec<&DocTableEntry>> = BTreeMap::new();
    for &doc_id in candidates {
        if let Some(entry) = reader.doc_entry(doc_id) {
            groups
                .entry((entry.file_ordinal, entry.row_group))
                .or_default()
                .push(entry);
        }
    }

    // Scoring context
    let corpus_stats = reader.corpus_stats();
    let avg_dl = bm25::avg_dl(corpus_stats.total_tokens, corpus_stats.total_rows);
    let term_infos: Vec<(String, u32)> = query_terms
        .iter()
        .filter_map(|t| {
            reader.term_ordinal(t).map(|ord| {
                let info = reader.term_info(ord).expect("valid ordinal from FST");
                (t.clone(), info.doc_frequency)
            })
        })
        .collect();

    let file_table = reader.file_table();

    // Pre-load Parquet metadata for unique files in parallel
    let unique_file_ords: Vec<u32> = groups
        .keys()
        .map(|(fo, _)| *fo)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let pq_metas: Vec<(u32, Arc<ParquetMetaData>)> = stream::iter(unique_file_ords.into_iter())
        .map(|fo| {
            let cache = Arc::clone(cache);
            let fp = file_table[fo as usize].path.clone();
            async move { cache.get_parquet_metadata(&fp).await.map(|m| (fo, m)) }
        })
        .buffered(IO_CONCURRENCY)
        .try_collect()
        .await?;
    let pq_meta_map: HashMap<u32, Arc<ParquetMetaData>> = pq_metas.into_iter().collect();

    let mut all_matches = Vec::new();
    let mut stats = QueryStats::default();

    for ((file_ordinal, rg_idx), entries) in &groups {
        let file_path = &file_table[*file_ordinal as usize].path;

        let mut sorted_entries: Vec<&DocTableEntry> = entries.clone();
        sorted_entries.sort_by_key(|e| e.first_row_index);
        sorted_entries.dedup_by_key(|e| e.first_row_index);
        stats.candidate_pages += sorted_entries.len();

        let pq_meta = &pq_meta_map[file_ordinal];
        let rg_total_rows = pq_meta.row_group(*rg_idx as usize).num_rows();
        let selection = build_row_selection(&sorted_entries, rg_total_rows);

        // Resolve column projection
        let projection = resolve_projection(pq_meta, column, select_columns)?;

        let batches = read_parquet_batches_async(
            cache.store(),
            file_path,
            *rg_idx as usize,
            &projection.leaf_indices,
            Some(selection),
        )
        .await?;

        let is_large = batches
            .first()
            .map(|b| {
                b.schema().field(projection.indexed_batch_col).data_type()
                    == &arrow::datatypes::DataType::LargeUtf8
            })
            .unwrap_or(false);

        // CPU: verify and score
        let qt = query_terms.to_vec();
        let ti = term_infos.clone();
        let fp = file_path.clone();
        let rg = *rg_idx;
        let scm = projection.select_col_map.clone();
        let ibc = projection.indexed_batch_col;

        let (mut matches, scan) = runtime
            .cpu(move || {
                verify_and_score(
                    &batches,
                    &qt,
                    &ti,
                    operator,
                    with_score,
                    avg_dl,
                    corpus_stats.total_rows,
                    is_large,
                    &fp,
                    rg,
                    ibc,
                    &scm,
                )
            })
            .await;

        stats.rows_scanned += scan.rows_scanned;
        stats.rows_matched += scan.rows_matched;
        all_matches.append(&mut matches);
    }

    Ok((all_matches, stats))
}

/// Resolved column projection: which parquet leaves to read and how they
/// map to batch column indices.
struct Projection {
    leaf_indices: Vec<usize>,
    indexed_batch_col: usize,
    select_col_map: Vec<(usize, String)>,
}

/// Resolves parquet leaf indices for the indexed column + select columns.
fn resolve_projection(
    pq_meta: &ParquetMetaData,
    column: &str,
    select_columns: &[String],
) -> Result<Projection> {
    let indexed_leaf = validate_column(pq_meta, column)
        .with_context(|| format!("resolving indexed column '{column}'"))?;

    let mut select_leaves: Vec<(usize, String)> = Vec::new();
    for sel in select_columns {
        if sel == column {
            continue;
        }
        let leaf = validate_column(pq_meta, sel)
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
// Stage 3b: Brute-force scan for un-indexed files
// ---------------------------------------------------------------------------

/// Scans un-indexed Parquet files row-by-row using arrow `contains` as a
/// fast pre-filter, then re-tokenizes candidates for exact verification.
#[allow(clippy::too_many_arguments)]
async fn brute_force_scan(
    cache: &Arc<ObjectCache>,
    files: &[String],
    column: &str,
    query_terms: &[String],
    operator: Operator,
    with_score: bool,
    select_columns: &[String],
    agg_total_rows: u64,
    agg_avg_dl: f64,
    agg_term_infos: &[(String, u32)],
    runtime: &Arc<LakeRuntime>,
) -> Result<(Vec<MatchedRow>, QueryStats)> {
    let mut all_matches = Vec::new();
    let mut stats = QueryStats::default();

    for file_path in files {
        let pq_meta = cache.get_parquet_metadata(file_path).await?;
        validate_column(&pq_meta, column)
            .with_context(|| format!("brute-force: column '{column}' in '{file_path}'"))?;

        // Resolve projection (indexed column + select columns)
        let projection = resolve_projection(&pq_meta, column, select_columns)?;

        for rg_idx in 0..pq_meta.num_row_groups() {
            // Read entire row group (no RowSelection — brute force)
            let batches = read_parquet_batches_async(
                cache.store(),
                file_path,
                rg_idx,
                &projection.leaf_indices,
                None,
            )
            .await?;

            let is_large = batches
                .first()
                .map(|b| {
                    b.schema().field(projection.indexed_batch_col).data_type()
                        == &arrow::datatypes::DataType::LargeUtf8
                })
                .unwrap_or(false);

            // CPU: pre-filter with arrow contains, then tokenize+verify candidates
            let qt = query_terms.to_vec();
            let op = operator;
            let fp = file_path.clone();
            let rg = rg_idx as u16;
            let scm = projection.select_col_map.clone();
            let ibc = projection.indexed_batch_col;

            let ati = agg_term_infos.to_vec();
            let atr = agg_total_rows;
            let aad = agg_avg_dl;

            let (mut matches, scan) = runtime
                .cpu(move || {
                    brute_force_verify(
                        &batches, &qt, &ati, op, with_score, is_large, &fp, rg, ibc, &scm, atr, aad,
                    )
                })
                .await;

            stats.rows_scanned += scan.rows_scanned;
            stats.rows_matched += scan.rows_matched;
            all_matches.append(&mut matches);
        }
    }

    Ok((all_matches, stats))
}

/// CPU-bound: uses arrow `contains` as pre-filter, then tokenizes candidates.
/// Scores using aggregate corpus stats from indexed segments when `with_score`.
#[allow(clippy::too_many_arguments)]
fn brute_force_verify(
    batches: &[arrow::array::RecordBatch],
    query_terms: &[String],
    term_infos: &[(String, u32)],
    operator: Operator,
    with_score: bool,
    is_large: bool,
    file_path: &str,
    rg_idx: u16,
    indexed_batch_col: usize,
    select_col_map: &[(usize, String)],
    total_rows: u64,
    avg_dl: f64,
) -> (Vec<MatchedRow>, QueryStats) {
    let mut matches = Vec::new();
    let mut stats = QueryStats::default();

    for batch in batches {
        let col = batch.column(indexed_batch_col);

        // Arrow pre-filter: for each query term, check substring containment
        let term_masks: Vec<BooleanArray> = query_terms
            .iter()
            .filter_map(|term| {
                let scalar = Scalar::new(StringArray::from(vec![term.as_str()]));
                arrow::compute::kernels::comparison::contains(col, &scalar).ok()
            })
            .collect();

        // Combine masks: AND = all terms must contain, OR = any term
        let candidate_mask = if term_masks.is_empty() {
            continue;
        } else {
            let mut mask = term_masks[0].clone();
            for m in &term_masks[1..] {
                mask = match operator {
                    Operator::And => compute::and(&mask, m).unwrap_or(mask),
                    Operator::Or => compute::or(&mask, m).unwrap_or(mask),
                };
            }
            mask
        };

        // Only tokenize+verify rows that passed the pre-filter
        for row in 0..batch.num_rows() {
            stats.rows_scanned += 1;

            if !candidate_mask.value(row) || col.is_null(row) {
                continue;
            }

            let text = string_value(col.as_ref(), row, is_large);
            let tokens = tokenize(text);

            // Exact verification using tokenizer semantics
            let matches_query = if query_terms.len() == 1 {
                tokens.iter().any(|t| t == &query_terms[0])
            } else {
                match operator {
                    Operator::And => query_terms.iter().all(|q| tokens.iter().any(|t| t == q)),
                    Operator::Or => query_terms.iter().any(|q| tokens.iter().any(|t| t == q)),
                }
            };

            if !matches_query {
                continue;
            }

            stats.rows_matched += 1;

            let score = if with_score {
                let dl = tokens.len() as u32;
                let mut freq: HashMap<&str, u32> = HashMap::new();
                for t in &tokens {
                    *freq.entry(t.as_str()).or_default() += 1;
                }
                let mut total_score = 0.0;
                for (term, df) in term_infos {
                    if let Some(&tf) = freq.get(term.as_str()) {
                        total_score += bm25::score(tf, dl, avg_dl, *df as u64, total_rows);
                    }
                }
                Some(total_score)
            } else {
                None
            };

            let columns = if select_col_map.is_empty() {
                None
            } else {
                let mut map = serde_json::Map::new();
                for (batch_idx, name) in select_col_map {
                    let val = arrow_value_to_json(batch.column(*batch_idx).as_ref(), row);
                    map.insert(name.clone(), val);
                }
                Some(map)
            };

            matches.push(MatchedRow {
                file: file_path.to_owned(),
                row_group: rg_idx,
                text: text.to_owned(),
                score,
                columns,
            });
        }
    }

    (matches, stats)
}

// ---------------------------------------------------------------------------
// Stage 4: Rank results
// ---------------------------------------------------------------------------

/// Sorts and/or truncates the result set.
fn rank_results(matches: &mut Vec<MatchedRow>, with_score: bool, limit: Option<usize>) {
    if let (true, Some(k)) = (with_score, limit) {
        // Top-K via min-heap
        let mut heap: std::collections::BinaryHeap<std::cmp::Reverse<ScoredMatch>> =
            std::collections::BinaryHeap::with_capacity(k + 1);
        for m in matches.drain(..) {
            let s = m.score.unwrap_or(0.0);
            heap.push(std::cmp::Reverse(ScoredMatch { score: s, row: m }));
            if heap.len() > k {
                heap.pop();
            }
        }
        *matches = heap
            .into_sorted_vec()
            .into_iter()
            .map(|r| r.0.row)
            .collect();
    } else if with_score {
        matches.sort_by(|a, b| {
            b.score
                .unwrap_or(0.0)
                .partial_cmp(&a.score.unwrap_or(0.0))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    } else if let Some(limit) = limit {
        matches.truncate(limit);
    }
}

struct ScoredMatch {
    score: f64,
    row: MatchedRow,
}

impl PartialEq for ScoredMatch {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}
impl Eq for ScoredMatch {}
impl PartialOrd for ScoredMatch {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for ScoredMatch {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

// ---------------------------------------------------------------------------
// Row verification (CPU-bound)
// ---------------------------------------------------------------------------

/// Re-tokenizes each row, checks the boolean predicate, optionally scores.
#[allow(clippy::too_many_arguments)]
fn verify_and_score(
    batches: &[arrow::array::RecordBatch],
    query_terms: &[String],
    term_infos: &[(String, u32)],
    operator: Operator,
    with_score: bool,
    avg_dl: f64,
    total_rows: u64,
    is_large: bool,
    file_path: &str,
    rg_idx: u16,
    indexed_batch_col: usize,
    select_col_map: &[(usize, String)],
) -> (Vec<MatchedRow>, QueryStats) {
    let mut matches = Vec::new();
    let mut stats = QueryStats::default();
    let single_term = query_terms.len() == 1;
    let few_terms = query_terms.len() <= 4;

    for batch in batches {
        let col = batch.column(indexed_batch_col);

        for row in 0..batch.num_rows() {
            stats.rows_scanned += 1;

            if col.is_null(row) {
                continue;
            }

            let text = string_value(col.as_ref(), row, is_large);
            let tokens = tokenize(text);

            let matches_query = if single_term {
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
            };

            if !matches_query {
                continue;
            }

            stats.rows_matched += 1;

            let score = if with_score {
                let dl = tokens.len() as u32;
                // Build frequency map once per row instead of scanning per term
                let mut freq: HashMap<&str, u32> = HashMap::new();
                for t in &tokens {
                    *freq.entry(t.as_str()).or_default() += 1;
                }
                let mut total_score = 0.0;
                for (term, df) in term_infos {
                    if let Some(&tf) = freq.get(term.as_str()) {
                        total_score += bm25::score(tf, dl, avg_dl, *df as u64, total_rows);
                    }
                }
                Some(total_score)
            } else {
                None
            };

            let columns = if select_col_map.is_empty() {
                None
            } else {
                let mut map = serde_json::Map::new();
                for (batch_idx, name) in select_col_map {
                    let val = arrow_value_to_json(batch.column(*batch_idx).as_ref(), row);
                    map.insert(name.clone(), val);
                }
                Some(map)
            };

            matches.push(MatchedRow {
                file: file_path.to_owned(),
                row_group: rg_idx,
                text: text.to_owned(),
                score,
                columns,
            });
        }
    }

    (matches, stats)
}
