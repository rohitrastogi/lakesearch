//! Stage 2: Segment evaluation — look up posting lists and combine with boolean ops.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{bail, Result};

use lakesearch_core::bm25;
use lakesearch_core::boolean;
use lakesearch_core::segment::SegmentReader;
use lakesearch_core::tokenizer::{QueryTerm, MAX_WILDCARD_EXPANSION};
use lakesearch_core::types::DocId;

use crate::Operator;

use super::types::IndexedWorkItem;

/// Result of evaluating a segment: candidate doc IDs and per-term info
/// (term string, doc_frequency). Returned together so the FST lookups
/// happen only once per segment.
pub(crate) struct SegmentEvaluation {
    pub candidates: Vec<DocId>,
    pub term_infos: Vec<(String, u32)>,
}

/// Looks up posting lists for each query term and combines them with boolean
/// ops. Returns candidates and term infos from a single set of FST lookups.
///
/// For wildcard terms (prefix/suffix), expands the wildcard to all matching
/// terms in the segment's FST, unions their posting lists, and treats the
/// result as a single "term" for boolean combination.
pub(crate) fn evaluate_segment(
    reader: &SegmentReader,
    query_terms: &[QueryTerm],
    operator: Operator,
) -> Result<SegmentEvaluation> {
    let mut entries: Vec<(u32, Vec<DocId>)> = Vec::new();
    let mut term_infos: Vec<(String, u32)> = Vec::new();

    for qt in query_terms {
        match qt {
            QueryTerm::Exact(term) => match reader.term_ordinal(term) {
                Some(ord) => {
                    let info = reader.term_info(ord)?;
                    let postings = reader.posting_list(ord)?;
                    term_infos.push((term.clone(), info.doc_frequency));
                    entries.push((info.doc_frequency, postings));
                }
                None => {
                    if operator == Operator::And {
                        return Ok(SegmentEvaluation {
                            candidates: vec![],
                            term_infos: vec![],
                        });
                    }
                }
            },
            QueryTerm::Prefix(pat) | QueryTerm::Suffix(pat) => {
                let expanded = if matches!(qt, QueryTerm::Prefix(_)) {
                    reader.prefix_terms(pat)
                } else {
                    reader.suffix_terms(pat)
                };
                if expanded.len() > MAX_WILDCARD_EXPANSION {
                    bail!(
                        "wildcard '{pat}' expanded to {} terms (max {MAX_WILDCARD_EXPANSION})",
                        expanded.len()
                    );
                }
                let postings = expand_wildcard(reader, &expanded, &mut term_infos)?;
                if let Some(p) = postings {
                    let df = p.len() as u32;
                    entries.push((df, p));
                } else if operator == Operator::And {
                    return Ok(SegmentEvaluation {
                        candidates: vec![],
                        term_infos: vec![],
                    });
                }
            }
        }
    }

    if entries.is_empty() {
        return Ok(SegmentEvaluation {
            candidates: vec![],
            term_infos,
        });
    }

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
    Ok(SegmentEvaluation {
        candidates: combined,
        term_infos,
    })
}

/// Expands wildcard matches: unions posting lists from all matching terms.
/// Collects term_infos for BM25 scoring. Returns None if no terms matched.
fn expand_wildcard(
    reader: &SegmentReader,
    matches: &[(String, u64)],
    term_infos: &mut Vec<(String, u32)>,
) -> Result<Option<Vec<DocId>>> {
    if matches.is_empty() {
        return Ok(None);
    }

    let mut combined: Option<Vec<DocId>> = None;
    for (term, ordinal) in matches {
        let info = reader.term_info(*ordinal)?;
        let postings = reader.posting_list(*ordinal)?;
        term_infos.push((term.clone(), info.doc_frequency));
        combined = Some(match combined {
            Some(prev) => boolean::union(&prev, &postings),
            None => postings,
        });
    }
    Ok(combined)
}

/// Groups evaluated candidates into `IndexedWorkItem`s, one per (file, row_group).
///
/// Resolves each `DocId` through the segment's doc table, groups entries by
/// (file_ordinal, row_group), sorts by `first_row_index`, deduplicates, and
/// attaches per-segment BM25 scoring context.
pub(crate) fn group_candidates(
    reader: &SegmentReader,
    candidates: &[DocId],
    term_infos: &[(String, u32)],
) -> Vec<IndexedWorkItem> {
    let file_table = reader.file_table();
    let corpus_stats = reader.corpus_stats();
    let avg_dl = bm25::avg_dl(corpus_stats.total_tokens, corpus_stats.total_rows);

    let term_infos: Arc<Vec<(String, u32)>> = Arc::new(term_infos.to_vec());

    let mut groups: BTreeMap<(u32, u16), Vec<lakesearch_core::types::DocTableEntry>> =
        BTreeMap::new();
    for &doc_id in candidates {
        if let Some(entry) = reader.doc_entry(doc_id) {
            groups
                .entry((entry.file_ordinal, entry.row_group))
                .or_default()
                .push(*entry);
        }
    }

    groups
        .into_iter()
        .map(|((file_ordinal, rg_idx), mut entries)| {
            entries.sort_by_key(|e| e.first_row_index);
            entries.dedup_by_key(|e| e.first_row_index);

            IndexedWorkItem {
                file_path: file_table[file_ordinal as usize].path.clone(),
                rg_idx,
                entries,
                avg_dl,
                total_rows: corpus_stats.total_rows,
                term_infos: Arc::clone(&term_infos),
            }
        })
        .collect()
}
