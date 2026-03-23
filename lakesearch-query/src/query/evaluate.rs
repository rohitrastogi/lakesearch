//! Stage 2: Segment evaluation — look up posting lists and combine with boolean ops.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;

use lakesearch_core::bm25;
use lakesearch_core::boolean;
use lakesearch_core::segment::SegmentReader;
use lakesearch_core::types::DocId;

use crate::Operator;

use super::types::IndexedWorkItem;

/// Looks up posting lists for each query term and combines them with boolean
/// ops. For AND queries, terms are sorted by doc_frequency (rarest first).
pub(crate) fn evaluate_segment(
    reader: &SegmentReader,
    query_terms: &[String],
    operator: Operator,
) -> Result<Vec<DocId>> {
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

/// Groups evaluated candidates into `IndexedWorkItem`s, one per (file, row_group).
///
/// Resolves each `DocId` through the segment's doc table, groups entries by
/// (file_ordinal, row_group), sorts by `first_row_index`, deduplicates, and
/// attaches per-segment BM25 scoring context.
pub(crate) fn group_candidates(
    reader: &SegmentReader,
    candidates: &[DocId],
    query_terms: &[String],
) -> Vec<IndexedWorkItem> {
    let file_table = reader.file_table();
    let corpus_stats = reader.corpus_stats();
    let avg_dl = bm25::avg_dl(corpus_stats.total_tokens, corpus_stats.total_rows);

    let term_infos: Arc<Vec<(String, u32)>> = Arc::new(
        query_terms
            .iter()
            .filter_map(|t| {
                reader.term_ordinal(t).map(|ord| {
                    let info = reader.term_info(ord).expect("valid ordinal from FST");
                    (t.clone(), info.doc_frequency)
                })
            })
            .collect(),
    );

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
