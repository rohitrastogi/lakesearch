use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;

use anyhow::{bail, Context, Result};
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;
use serde::Serialize;
use tracing::info;

use lakesearch_core::bm25;
use lakesearch_core::boolean;
use lakesearch_core::segment::SegmentReader;
use lakesearch_core::tokenizer::tokenize;
use lakesearch_core::types::DocId;

use crate::parquet_util::{
    build_row_selection, string_value, validate_arrow_column, validate_column,
};
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
}

#[derive(Debug, Default, Serialize)]
pub struct QueryStats {
    pub candidate_pages: usize,
    pub rows_scanned: usize,
    pub rows_matched: usize,
}

/// Queries the segment and returns matching rows from the Parquet files.
pub fn run_query(
    segment_path: &str,
    files: &[String],
    column: &str,
    query_text: &str,
    operator: Operator,
    with_score: bool,
    limit: Option<usize>,
) -> Result<QueryResult> {
    // 1. Load segment
    let segment_data =
        std::fs::read(segment_path).with_context(|| format!("reading segment '{segment_path}'"))?;
    let reader = SegmentReader::open(segment_data).context("opening segment")?;

    let file_table = reader.file_table();
    if files.len() != file_table.len() {
        bail!(
            "segment has {} file(s) but {} were provided",
            file_table.len(),
            files.len()
        );
    }

    // 2. Tokenize query
    let query_terms = tokenize(query_text);
    if query_terms.is_empty() {
        return Ok(QueryResult {
            matches: vec![],
            stats: QueryStats::default(),
        });
    }

    // 3. Look up posting lists
    let mut posting_lists: Vec<Vec<DocId>> = Vec::new();
    for term in &query_terms {
        match reader.search_term(term)? {
            Some(postings) => posting_lists.push(postings),
            None => {
                if operator == Operator::And {
                    info!(missing_term = %term, "AND short-circuit: term not in index");
                    return Ok(QueryResult {
                        matches: vec![],
                        stats: QueryStats::default(),
                    });
                }
            }
        }
    }

    if posting_lists.is_empty() {
        return Ok(QueryResult {
            matches: vec![],
            stats: QueryStats::default(),
        });
    }

    // 4. Combine posting lists
    let mut combined = posting_lists[0].clone();
    for postings in &posting_lists[1..] {
        combined = match operator {
            Operator::And => boolean::intersect(&combined, postings),
            Operator::Or => boolean::union(&combined, postings),
        };
    }

    if combined.is_empty() {
        return Ok(QueryResult {
            matches: vec![],
            stats: QueryStats::default(),
        });
    }

    // 5. Group candidate doc_ids by (file_ordinal, row_group)
    let mut groups: BTreeMap<(u32, u16), Vec<DocId>> = BTreeMap::new();
    for &doc_id in &combined {
        let entry = reader
            .doc_entry(doc_id)
            .with_context(|| format!("doc_id {doc_id} not in doc table"))?;
        groups
            .entry((entry.file_ordinal, entry.row_group))
            .or_default()
            .push(doc_id);
    }

    // 6. Prepare scoring data
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

    let mut all_matches = Vec::new();
    let mut stats = QueryStats::default();

    // 7. For each (file, rg) group, read candidate pages and verify rows
    for ((file_ordinal, rg_idx), doc_ids) in &groups {
        let file_path = &files[*file_ordinal as usize];

        // Collect doc table entries, sorted and deduped by first_row_index
        let mut entries: Vec<_> = doc_ids
            .iter()
            .map(|&id| reader.doc_entry(id).expect("validated above"))
            .collect();
        entries.sort_by_key(|e| e.first_row_index);
        entries.dedup_by_key(|e| e.first_row_index);

        stats.candidate_pages += entries.len();

        // Open file once: read metadata for row count, then build reader
        let f =
            File::open(file_path).with_context(|| format!("opening '{file_path}' for query"))?;
        let options = ArrowReaderOptions::new().with_page_index(true);
        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(f, options)?;
        let rg_total_rows = builder.metadata().row_group(*rg_idx as usize).num_rows();

        // Build RowSelection
        let selection = build_row_selection(&entries, rg_total_rows);

        // Use parquet leaf index (not arrow field index) for correct projection
        // with nested schemas.
        let col_idx = validate_column(builder.metadata(), column)
            .with_context(|| format!("validating column in '{file_path}'"))?;
        let is_large = validate_arrow_column(builder.schema(), column)?;

        let mask = ProjectionMask::leaves(builder.parquet_schema(), [col_idx]);
        let batch_reader = builder
            .with_row_groups(vec![*rg_idx as usize])
            .with_projection(mask)
            .with_row_selection(selection)
            .build()?;

        // Process returned rows
        for batch in batch_reader {
            let batch = batch?;
            let col = batch.column(0);

            for row in 0..batch.num_rows() {
                stats.rows_scanned += 1;

                if col.is_null(row) {
                    continue;
                }

                let text = string_value(col.as_ref(), row, is_large);

                // Re-tokenize for verification
                let tokens = tokenize(text);
                let token_set: HashSet<&str> = tokens.iter().map(|s| s.as_str()).collect();

                let matches_query = match operator {
                    Operator::And => query_terms.iter().all(|t| token_set.contains(t.as_str())),
                    Operator::Or => query_terms.iter().any(|t| token_set.contains(t.as_str())),
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
                    for (term, df) in &term_infos {
                        if let Some(&tf) = freq.get(term.as_str()) {
                            total_score +=
                                bm25::score(tf, dl, avg_dl, *df as u64, corpus_stats.total_rows);
                        }
                    }
                    Some(total_score)
                } else {
                    None
                };

                all_matches.push(MatchedRow {
                    file: file_path.clone(),
                    row_group: *rg_idx,
                    text: text.to_owned(),
                    score,
                });
            }
        }
    }

    // 8. Sort by score (descending) if scoring, then apply limit
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
