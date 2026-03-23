//! Pure segment merge algorithm.
//!
//! Given N segments for the same column, produces a single merged segment with
//! remapped doc_ids, deduped file tables, and summed corpus stats. This is a
//! strategy-agnostic primitive — the caller decides *which* segments to merge.

use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result};

use lakesearch_core::segment::{SegmentBuilder, SegmentReader};
use lakesearch_core::types::{CorpusStats, DocId, DocTableEntry, FileTableEntry};

/// Result of merging multiple segments: the serialized bytes plus metadata
/// extracted during construction (avoids re-parsing the segment).
#[must_use]
pub struct MergedSegment {
    pub bytes: Vec<u8>,
    pub min_term: String,
    pub max_term: String,
    pub term_count: u64,
    pub doc_count: u64,
    pub total_rows: u64,
    pub total_tokens: u64,
    pub file_table: Vec<FileTableEntry>,
}

/// Merges N segments for the same column into a single segment.
///
/// Pure CPU work — no I/O. Each input segment's doc_ids are remapped
/// to disjoint ranges so the merged posting lists remain sorted.
/// Takes ownership of segment data to avoid cloning into `SegmentReader`.
pub fn merge_segments(segment_data: Vec<Vec<u8>>) -> Result<MergedSegment> {
    let readers: Vec<SegmentReader> = segment_data
        .into_iter()
        .map(|data| SegmentReader::open(data).context("opening segment for merge"))
        .collect::<Result<Vec<_>>>()?;

    // 1. Compute doc_id offsets: segment i starts at sum(doc_count(0..i-1))
    let mut offsets: Vec<u32> = Vec::with_capacity(readers.len());
    let mut running = 0u32;
    for reader in &readers {
        offsets.push(running);
        running += reader.doc_count() as u32;
    }

    // 2. Build global file table (dedup by path, remap file_ordinals)
    let mut global_file_table: Vec<FileTableEntry> = Vec::new();
    let mut path_to_global: HashMap<String, u32> = HashMap::new();
    // Per-segment mapping: local file_ordinal → global file_ordinal
    let mut file_remap: Vec<Vec<u32>> = Vec::with_capacity(readers.len());

    for reader in &readers {
        let mut seg_remap = Vec::with_capacity(reader.file_table().len());
        for entry in reader.file_table() {
            let global_ord = if let Some(&existing) = path_to_global.get(&entry.path) {
                existing
            } else {
                let ord = global_file_table.len() as u32;
                path_to_global.insert(entry.path.clone(), ord);
                global_file_table.push(entry.clone());
                ord
            };
            seg_remap.push(global_ord);
        }
        file_remap.push(seg_remap);
    }

    // 3. Concatenate doc tables with remapped file_ordinals
    let mut merged_doc_table: Vec<DocTableEntry> = Vec::with_capacity(running as usize);
    for (seg_idx, reader) in readers.iter().enumerate() {
        for entry in reader.doc_table() {
            merged_doc_table.push(DocTableEntry {
                file_ordinal: file_remap[seg_idx][entry.file_ordinal as usize],
                ..*entry
            });
        }
    }

    // 4. Merge terms lexicographically: collect posting lists, remap doc_ids,
    //    sum doc_frequencies. BTreeMap maintains sorted order, avoiding a
    //    separate sort pass.
    let mut merged_terms: BTreeMap<String, (Vec<DocId>, u32)> = BTreeMap::new();

    for (seg_idx, reader) in readers.iter().enumerate() {
        let offset = offsets[seg_idx];
        for (term, ordinal) in reader.prefix_terms("") {
            let posting = reader
                .posting_list(ordinal)
                .context("decoding posting list during merge")?;
            let info = reader
                .term_info(ordinal)
                .context("reading term info during merge")?;

            let entry = merged_terms.entry(term).or_insert_with(|| (Vec::new(), 0));
            // Remap doc_ids by adding the segment's offset
            entry.0.extend(posting.iter().map(|&id| id + offset));
            entry.1 += info.doc_frequency;
        }
    }

    // 5. Sum corpus stats
    let mut total_rows: u64 = 0;
    let mut total_tokens: u64 = 0;
    for reader in &readers {
        let stats = reader.corpus_stats();
        total_rows += stats.total_rows;
        total_tokens += stats.total_tokens;
    }

    // 6. Build merged segment via SegmentBuilder
    let mut builder = SegmentBuilder::new();

    let min_term = merged_terms.keys().next().cloned().unwrap_or_default();
    let max_term = merged_terms.keys().next_back().cloned().unwrap_or_default();
    let term_count = merged_terms.len() as u64;

    // BTreeMap iterates in sorted order — no extra sort needed.
    for (term, (doc_ids, df)) in merged_terms {
        // doc_ids are already sorted — segments have disjoint ranges
        builder.add_term(&term, doc_ids, df);
    }

    // Capture counts before moving into builder
    let doc_count = merged_doc_table.len() as u64;
    let file_table_out = global_file_table.clone();
    builder.set_doc_table(merged_doc_table);
    builder.set_file_table(global_file_table);
    builder.set_corpus_stats(CorpusStats {
        total_rows,
        total_tokens,
    });

    let bytes = builder.build().context("building merged segment")?;

    Ok(MergedSegment {
        bytes,
        min_term,
        max_term,
        term_count,
        doc_count,
        total_rows,
        total_tokens,
        file_table: file_table_out,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use lakesearch_core::segment::SegmentReader;

    fn build_test_segment(
        terms: &[(&str, Vec<DocId>, u32)],
        doc_table: Vec<DocTableEntry>,
        file_table: Vec<FileTableEntry>,
        corpus_stats: CorpusStats,
    ) -> Vec<u8> {
        let mut builder = SegmentBuilder::new();
        for (term, doc_ids, df) in terms {
            builder.add_term(term, doc_ids.clone(), *df);
        }
        builder.set_doc_table(doc_table);
        builder.set_file_table(file_table);
        builder.set_corpus_stats(corpus_stats);
        builder.build().expect("building test segment")
    }

    #[test]
    fn merge_two_segments_basic() {
        let seg1 = build_test_segment(
            &[("hello", vec![0, 1], 5), ("world", vec![0], 3)],
            vec![
                DocTableEntry {
                    file_ordinal: 0,
                    row_group: 0,
                    page_index: 0,
                    first_row_index: 0,
                    row_count: 100,
                },
                DocTableEntry {
                    file_ordinal: 0,
                    row_group: 0,
                    page_index: 1,
                    first_row_index: 100,
                    row_count: 100,
                },
            ],
            vec![FileTableEntry {
                path: "data/file1.parquet".to_owned(),
                row_group_count: 1,
            }],
            CorpusStats {
                total_rows: 200,
                total_tokens: 1000,
            },
        );

        let seg2 = build_test_segment(
            &[("hello", vec![0], 2), ("rust", vec![0, 1], 4)],
            vec![
                DocTableEntry {
                    file_ordinal: 0,
                    row_group: 0,
                    page_index: 0,
                    first_row_index: 0,
                    row_count: 50,
                },
                DocTableEntry {
                    file_ordinal: 0,
                    row_group: 0,
                    page_index: 1,
                    first_row_index: 50,
                    row_count: 50,
                },
            ],
            vec![FileTableEntry {
                path: "data/file2.parquet".to_owned(),
                row_group_count: 1,
            }],
            CorpusStats {
                total_rows: 100,
                total_tokens: 500,
            },
        );

        let merged = merge_segments(vec![seg1, seg2]).unwrap();

        let reader = SegmentReader::open(merged.bytes).unwrap();
        assert_eq!(reader.doc_count(), 4); // 2 + 2
        assert_eq!(reader.corpus_stats().total_rows, 300);
        assert_eq!(reader.corpus_stats().total_tokens, 1500);
        assert_eq!(reader.file_table().len(), 2);

        // "hello" should have doc_ids [0, 1, 2] (0,1 from seg1 + 0+2=2 from seg2)
        let hello = reader.search_term("hello").unwrap().unwrap();
        assert_eq!(hello, vec![0, 1, 2]);

        // "world" should have doc_ids [0] (only in seg1)
        let world = reader.search_term("world").unwrap().unwrap();
        assert_eq!(world, vec![0]);

        // "rust" should have doc_ids [2, 3] (0+2=2, 1+2=3 from seg2)
        let rust_term = reader.search_term("rust").unwrap().unwrap();
        assert_eq!(rust_term, vec![2, 3]);
    }

    #[test]
    fn merge_overlapping_terms() {
        let seg1 = build_test_segment(
            &[("alpha", vec![0], 3)],
            vec![DocTableEntry {
                file_ordinal: 0,
                row_group: 0,
                page_index: 0,
                first_row_index: 0,
                row_count: 50,
            }],
            vec![FileTableEntry {
                path: "f1.parquet".to_owned(),
                row_group_count: 1,
            }],
            CorpusStats {
                total_rows: 50,
                total_tokens: 200,
            },
        );

        let seg2 = build_test_segment(
            &[("alpha", vec![0], 7)],
            vec![DocTableEntry {
                file_ordinal: 0,
                row_group: 0,
                page_index: 0,
                first_row_index: 0,
                row_count: 80,
            }],
            vec![FileTableEntry {
                path: "f2.parquet".to_owned(),
                row_group_count: 1,
            }],
            CorpusStats {
                total_rows: 80,
                total_tokens: 350,
            },
        );

        let merged = merge_segments(vec![seg1, seg2]).unwrap();
        let reader = SegmentReader::open(merged.bytes).unwrap();

        let alpha = reader.search_term("alpha").unwrap().unwrap();
        assert_eq!(alpha, vec![0, 1]);

        // doc_frequency should be summed: 3 + 7 = 10
        let ord = reader.term_ordinal("alpha").unwrap();
        let info = reader.term_info(ord).unwrap();
        assert_eq!(info.doc_frequency, 10);
    }

    #[test]
    fn merge_disjoint_terms() {
        let seg1 = build_test_segment(
            &[("aaa", vec![0], 1)],
            vec![DocTableEntry {
                file_ordinal: 0,
                row_group: 0,
                page_index: 0,
                first_row_index: 0,
                row_count: 10,
            }],
            vec![FileTableEntry {
                path: "f.parquet".to_owned(),
                row_group_count: 1,
            }],
            CorpusStats {
                total_rows: 10,
                total_tokens: 50,
            },
        );

        let seg2 = build_test_segment(
            &[("zzz", vec![0], 1)],
            vec![DocTableEntry {
                file_ordinal: 0,
                row_group: 0,
                page_index: 0,
                first_row_index: 0,
                row_count: 10,
            }],
            vec![FileTableEntry {
                path: "g.parquet".to_owned(),
                row_group_count: 1,
            }],
            CorpusStats {
                total_rows: 10,
                total_tokens: 50,
            },
        );

        let merged = merge_segments(vec![seg1, seg2]).unwrap();
        let reader = SegmentReader::open(merged.bytes).unwrap();

        assert!(reader.search_term("aaa").unwrap().is_some());
        assert!(reader.search_term("zzz").unwrap().is_some());
        assert_eq!(reader.term_count(), 2);
    }

    #[test]
    fn merge_shared_files() {
        // Both segments reference the same parquet file
        let shared_file = FileTableEntry {
            path: "shared.parquet".to_owned(),
            row_group_count: 2,
        };

        let seg1 = build_test_segment(
            &[("term", vec![0], 1)],
            vec![DocTableEntry {
                file_ordinal: 0,
                row_group: 0,
                page_index: 0,
                first_row_index: 0,
                row_count: 10,
            }],
            vec![shared_file.clone()],
            CorpusStats {
                total_rows: 10,
                total_tokens: 50,
            },
        );

        let seg2 = build_test_segment(
            &[("term", vec![0], 1)],
            vec![DocTableEntry {
                file_ordinal: 0,
                row_group: 1,
                page_index: 0,
                first_row_index: 10,
                row_count: 10,
            }],
            vec![shared_file],
            CorpusStats {
                total_rows: 10,
                total_tokens: 50,
            },
        );

        let merged = merge_segments(vec![seg1, seg2]).unwrap();
        let reader = SegmentReader::open(merged.bytes).unwrap();

        // File table should be deduped: only 1 entry
        assert_eq!(reader.file_table().len(), 1);
        assert_eq!(reader.file_table()[0].path, "shared.parquet");
        // Both doc table entries should map to file_ordinal 0
        assert!(reader.doc_table().iter().all(|e| e.file_ordinal == 0));
    }
}
