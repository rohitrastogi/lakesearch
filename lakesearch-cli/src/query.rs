//! Async multi-segment query command for object storage.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::info;

use lakesearch_core::bm25;
use lakesearch_core::boolean;
use lakesearch_core::runtime::LakeRuntime;
use lakesearch_core::segment::SegmentReader;
use lakesearch_core::tokenizer::tokenize;
use lakesearch_core::types::DocId;
use object_store::path::Path;
use object_store::ObjectStore;

use serde::Serialize;

use crate::parquet_util::{
    arrow_value_to_json, build_row_selection, load_parquet_metadata_async,
    read_parquet_batches_async, string_value, validate_column,
};
use crate::storage::{load_bytes, read_current, read_manifest, read_manifest_list, read_metadata};
use crate::Operator;

#[derive(Debug, Serialize)]
pub struct QueryResult {
    pub matches: Vec<MatchedRow>,
    pub stats: QueryStats,
}

#[derive(Debug, Serialize)]
pub struct MatchedRow {
    pub file: String,
    pub row_group: u16,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, Default, Serialize)]
pub struct QueryStats {
    pub candidate_pages: usize,
    pub rows_scanned: usize,
    pub rows_matched: usize,
}

/// Queries a LakeSearch table in object storage across all segments.
#[allow(clippy::too_many_arguments)]
pub async fn run_query(
    store: &Arc<dyn ObjectStore>,
    base: &Path,
    column: &str,
    query_text: &str,
    operator: Operator,
    with_score: bool,
    limit: Option<usize>,
    select_columns: &[String],
    runtime: &LakeRuntime,
) -> Result<QueryResult> {
    // 1. Load metadata chain
    let current_result = read_current(store.as_ref(), base).await?;
    let metadata = read_metadata(store.as_ref(), &current_result.value).await?;

    // 2. Collect all segments for the target column
    let mut segment_bytes_list: Vec<Vec<u8>> = Vec::new();

    for ml_path in &metadata.snapshot.manifest_lists {
        let ml = read_manifest_list(store.as_ref(), ml_path).await?;
        for me in &ml.manifests {
            if me.indexed_column != column {
                continue;
            }
            let manifest = read_manifest(store.as_ref(), &me.manifest_path).await?;
            for seg_info in &manifest.segments {
                let bytes = load_bytes(store.as_ref(), &seg_info.segment_path).await?;
                segment_bytes_list.push(bytes.to_vec());
            }
        }
    }

    if segment_bytes_list.is_empty() {
        return Ok(QueryResult {
            matches: vec![],
            stats: QueryStats::default(),
        });
    }

    info!(segments = segment_bytes_list.len(), "loaded segments");

    // 3. Tokenize query
    let query_terms = tokenize(query_text);
    if query_terms.is_empty() {
        return Ok(QueryResult {
            matches: vec![],
            stats: QueryStats::default(),
        });
    }

    let mut all_matches = Vec::new();
    let mut stats = QueryStats::default();

    // 4. Process each segment independently
    for seg_bytes in segment_bytes_list {
        let reader = SegmentReader::open(seg_bytes).context("opening segment")?;

        // Look up posting lists for this segment
        let mut posting_lists: Vec<Vec<DocId>> = Vec::new();
        let mut any_missing = false;
        for term in &query_terms {
            match reader.search_term(term)? {
                Some(postings) => posting_lists.push(postings),
                None => {
                    if operator == Operator::And {
                        any_missing = true;
                        break;
                    }
                }
            }
        }

        if any_missing || posting_lists.is_empty() {
            continue;
        }

        // Combine posting lists
        let mut combined = posting_lists[0].clone();
        for postings in &posting_lists[1..] {
            combined = match operator {
                Operator::And => boolean::intersect(&combined, postings),
                Operator::Or => boolean::union(&combined, postings),
            };
        }

        if combined.is_empty() {
            continue;
        }

        // Group candidates by (file_ordinal, row_group)
        let mut groups: BTreeMap<(u32, u16), Vec<DocId>> = BTreeMap::new();
        for &doc_id in &combined {
            if let Some(entry) = reader.doc_entry(doc_id) {
                groups
                    .entry((entry.file_ordinal, entry.row_group))
                    .or_default()
                    .push(doc_id);
            }
        }

        // Prepare scoring data for this segment
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

        // Read and verify rows per group
        for ((file_ordinal, rg_idx), doc_ids) in &groups {
            let file_path = &file_table[*file_ordinal as usize].path;

            let mut entries: Vec<_> = doc_ids
                .iter()
                .map(|&id| reader.doc_entry(id).expect("validated"))
                .collect();
            entries.sort_by_key(|e| e.first_row_index);
            entries.dedup_by_key(|e| e.first_row_index);
            stats.candidate_pages += entries.len();

            // Get parquet metadata for column resolution
            let pq_meta = load_parquet_metadata_async(store, file_path).await?;
            let rg_total_rows = pq_meta.row_group(*rg_idx as usize).num_rows();

            let selection = build_row_selection(&entries, rg_total_rows);

            // Resolve leaf indices: indexed column + select columns
            let indexed_leaf = validate_column(&pq_meta, column)
                .with_context(|| format!("validating column in '{file_path}'"))?;

            let mut select_leaves: Vec<(usize, String)> = Vec::new();
            for sel_name in select_columns {
                if sel_name == column {
                    continue; // Already included as the indexed column
                }
                let leaf = validate_column(&pq_meta, sel_name)
                    .with_context(|| format!("select column '{sel_name}' in '{file_path}'"))?;
                select_leaves.push((leaf, sel_name.clone()));
            }

            // Build sorted leaf indices and track positions in the projected batch
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

            // Map: batch column index → select column name
            let select_col_map: Vec<(usize, String)> = all_leaves
                .iter()
                .enumerate()
                .filter_map(|(batch_idx, (_, name))| name.as_ref().map(|n| (batch_idx, n.clone())))
                .collect();

            let batches = read_parquet_batches_async(
                store,
                file_path,
                *rg_idx as usize,
                &leaf_indices,
                Some(selection),
            )
            .await?;

            // Determine if LargeUtf8
            let is_large = batches
                .first()
                .map(|b| {
                    b.schema().field(indexed_batch_col).data_type()
                        == &arrow::datatypes::DataType::LargeUtf8
                })
                .unwrap_or(false);

            // CPU: verify, score, and extract select columns
            let qt = query_terms.clone();
            let ti = term_infos.clone();
            let op = operator;
            let fp = file_path.clone();
            let rg = *rg_idx;
            let scm = select_col_map.clone();
            let ibc = indexed_batch_col;

            let (mut matches, scan_stats) = runtime
                .cpu(move || {
                    verify_and_score_batches(
                        &batches,
                        &qt,
                        &ti,
                        op,
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

            stats.rows_scanned += scan_stats.0;
            stats.rows_matched += scan_stats.1;
            all_matches.append(&mut matches);
        }
    }

    // Sort by score descending if scoring
    if with_score {
        all_matches.sort_by(|a, b| {
            b.score
                .unwrap_or(0.0)
                .partial_cmp(&a.score.unwrap_or(0.0))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    if let Some(limit) = limit {
        all_matches.truncate(limit);
    }

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

/// CPU-bound: verify rows against query, optionally score, and extract select columns.
/// Returns (matches, (rows_scanned, rows_matched)).
#[allow(clippy::too_many_arguments)]
fn verify_and_score_batches(
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
) -> (Vec<MatchedRow>, (usize, usize)) {
    let mut matches = Vec::new();
    let mut rows_scanned = 0usize;
    let mut rows_matched = 0usize;

    for batch in batches {
        let col = batch.column(indexed_batch_col);

        for row in 0..batch.num_rows() {
            rows_scanned += 1;

            if col.is_null(row) {
                continue;
            }

            let text = string_value(col.as_ref(), row, is_large);
            let tokens = tokenize(text);
            let token_set: HashSet<&str> = tokens.iter().map(|s| s.as_str()).collect();

            let matches_query = match operator {
                Operator::And => query_terms.iter().all(|t| token_set.contains(t.as_str())),
                Operator::Or => query_terms.iter().any(|t| token_set.contains(t.as_str())),
            };

            if !matches_query {
                continue;
            }

            rows_matched += 1;

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

    (matches, (rows_scanned, rows_matched))
}
