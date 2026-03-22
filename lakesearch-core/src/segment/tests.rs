use super::*;
use crate::types::{CorpusStats, DocTableEntry, FileTableEntry};

fn build_test_segment() -> Vec<u8> {
    let mut builder = SegmentBuilder::new();

    builder.set_file_table(vec![
        FileTableEntry {
            path: "s3://bucket/data/part-00001.parquet".to_owned(),
            row_group_count: 2,
        },
        FileTableEntry {
            path: "s3://bucket/data/part-00002.parquet".to_owned(),
            row_group_count: 1,
        },
    ]);

    builder.set_doc_table(vec![
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
        DocTableEntry {
            file_ordinal: 0,
            row_group: 1,
            page_index: 0,
            first_row_index: 0,
            row_count: 50,
        },
        DocTableEntry {
            file_ordinal: 1,
            row_group: 0,
            page_index: 0,
            first_row_index: 0,
            row_count: 200,
        },
    ]);

    builder.set_corpus_stats(CorpusStats {
        total_rows: 450,
        total_tokens: 9000,
    });

    // Add terms in sorted order
    builder.add_term("connection", vec![0, 1, 3], 150);
    builder.add_term("error", vec![0, 1, 2, 3], 300);
    builder.add_term("timeout", vec![0, 3], 50);

    builder.build().expect("segment build should succeed")
}

#[test]
fn round_trip_basic() {
    let data = build_test_segment();
    let reader = SegmentReader::open(data).expect("segment open should succeed");

    // Check corpus stats
    let stats = reader.corpus_stats();
    assert_eq!(stats.total_rows, 450);
    assert_eq!(stats.total_tokens, 9000);

    // Check term count
    assert_eq!(reader.term_count(), 3);

    // Check doc count
    assert_eq!(reader.doc_count(), 4);
}

#[test]
fn term_lookup() {
    let data = build_test_segment();
    let reader = SegmentReader::open(data).unwrap();

    // Existing terms
    let ord = reader.term_ordinal("error").expect("error should exist");
    let info = reader.term_info(ord).unwrap();
    assert_eq!(info.doc_frequency, 300);

    let posting = reader.posting_list(ord).unwrap();
    assert_eq!(posting, vec![0, 1, 2, 3]);

    // Convenience method
    let posting2 = reader.search_term("timeout").unwrap().unwrap();
    assert_eq!(posting2, vec![0, 3]);

    // Missing term
    assert!(reader.search_term("nonexistent").unwrap().is_none());
}

#[test]
fn doc_table_entries() {
    let data = build_test_segment();
    let reader = SegmentReader::open(data).unwrap();

    let entry = reader.doc_entry(0).expect("doc 0 should exist");
    assert_eq!(entry.file_ordinal, 0);
    assert_eq!(entry.row_group, 0);
    assert_eq!(entry.page_index, 0);
    assert_eq!(entry.first_row_index, 0);
    assert_eq!(entry.row_count, 100);

    let entry3 = reader.doc_entry(3).expect("doc 3 should exist");
    assert_eq!(entry3.file_ordinal, 1);
    assert_eq!(entry3.row_count, 200);

    assert!(reader.doc_entry(4).is_none());
}

#[test]
fn file_table_entries() {
    let data = build_test_segment();
    let reader = SegmentReader::open(data).unwrap();

    let ft = reader.file_table();
    assert_eq!(ft.len(), 2);
    assert_eq!(ft[0].path, "s3://bucket/data/part-00001.parquet");
    assert_eq!(ft[0].row_group_count, 2);
    assert_eq!(ft[1].path, "s3://bucket/data/part-00002.parquet");
    assert_eq!(ft[1].row_group_count, 1);
}

#[test]
fn prefix_search() {
    let data = build_test_segment();
    let reader = SegmentReader::open(data).unwrap();

    let matches = reader.prefix_terms("con");
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].0, "connection");

    let all = reader.prefix_terms("");
    assert_eq!(all.len(), 3);

    let none = reader.prefix_terms("xyz");
    assert!(none.is_empty());
}

#[test]
fn empty_segment() {
    let mut builder = SegmentBuilder::new();
    builder.set_corpus_stats(CorpusStats {
        total_rows: 0,
        total_tokens: 0,
    });
    let data = builder.build().unwrap();
    let reader = SegmentReader::open(data).unwrap();

    assert_eq!(reader.term_count(), 0);
    assert_eq!(reader.doc_count(), 0);
    assert!(reader.search_term("anything").unwrap().is_none());
}

#[test]
fn checksum_detects_corruption() {
    let mut data = build_test_segment();
    // Corrupt a byte in the middle
    let mid = data.len() / 2;
    data[mid] ^= 0xFF;

    let result = SegmentReader::open(data);
    assert!(result.is_err());
    match result.unwrap_err() {
        crate::types::SegmentError::ChecksumMismatch { .. } => {}
        other => panic!("expected ChecksumMismatch, got: {other}"),
    }
}

#[test]
fn invalid_magic_rejected() {
    let mut data = build_test_segment();
    data[0] = b'X';

    let result = SegmentReader::open(data);
    assert!(matches!(
        result.unwrap_err(),
        crate::types::SegmentError::InvalidMagic
    ));
}

#[test]
fn too_short_rejected() {
    let result = SegmentReader::open(vec![0; 10]);
    assert!(matches!(
        result.unwrap_err(),
        crate::types::SegmentError::TooShort { .. }
    ));
}

#[test]
fn footer_offset_corruption_detected() {
    let data = build_test_segment();
    let footer_start = data.len() - 56;

    // Corrupt each footer offset field — all should be caught by CRC
    for field_idx in 0..6 {
        let byte_pos = footer_start + field_idx * 8;
        let mut corrupted = data.clone();
        corrupted[byte_pos] ^= 0x01;
        let result = SegmentReader::open(corrupted);
        assert!(
            matches!(
                result,
                Err(crate::types::SegmentError::ChecksumMismatch { .. })
            ),
            "footer offset field {field_idx} corruption should be detected"
        );
    }
}

#[test]
fn suffix_search_ascii() {
    let data = build_test_segment();
    let reader = SegmentReader::open(data).unwrap();

    // "connection" and "timeout" both end in "tion"
    let matches = reader.suffix_terms("tion");
    let terms: Vec<&str> = matches.iter().map(|(t, _)| t.as_str()).collect();
    assert!(terms.contains(&"connection"), "should match 'connection'");

    // "out" suffix should match "timeout"
    let matches = reader.suffix_terms("out");
    let terms: Vec<&str> = matches.iter().map(|(t, _)| t.as_str()).collect();
    assert!(terms.contains(&"timeout"), "should match 'timeout'");

    // No term ends in "xyz"
    assert!(reader.suffix_terms("xyz").is_empty());
}

#[test]
fn suffix_search_unicode() {
    let mut builder = SegmentBuilder::new();
    builder.set_corpus_stats(CorpusStats {
        total_rows: 10,
        total_tokens: 50,
    });
    builder.add_term("café", vec![0], 5);
    builder.add_term("naïveté", vec![0, 1], 8);
    builder.set_doc_table(vec![
        DocTableEntry {
            file_ordinal: 0,
            row_group: 0,
            page_index: 0,
            first_row_index: 0,
            row_count: 10,
        },
        DocTableEntry {
            file_ordinal: 0,
            row_group: 0,
            page_index: 1,
            first_row_index: 10,
            row_count: 10,
        },
    ]);

    let data = builder.build().unwrap();
    let reader = SegmentReader::open(data).unwrap();

    // Suffix "é" should match "café" and "naïveté"
    let matches = reader.suffix_terms("é");
    let mut terms: Vec<String> = matches.into_iter().map(|(t, _)| t).collect();
    terms.sort();
    assert_eq!(terms, vec!["café", "naïveté"]);

    // Suffix "fé" should match only "café"
    let matches = reader.suffix_terms("fé");
    let terms: Vec<&str> = matches.iter().map(|(t, _)| t.as_str()).collect();
    assert_eq!(terms, vec!["café"]);

    // Suffix "té" should match only "naïveté"
    let matches = reader.suffix_terms("té");
    let terms: Vec<&str> = matches.iter().map(|(t, _)| t.as_str()).collect();
    assert_eq!(terms, vec!["naïveté"]);
}
