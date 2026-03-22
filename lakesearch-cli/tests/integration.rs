use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

use lakesearch_cli::index::run_index;
use lakesearch_cli::query::run_query;
use lakesearch_cli::Operator;
use lakesearch_core::segment::SegmentReader;
use lakesearch_core::test_utils::write_test_parquet;
use lakesearch_core::tokenizer::tokenize;

fn test_descriptions() -> Vec<&'static str> {
    vec![
        "error timeout connection refused",
        "success response ok",
        "error connection reset",
        "warning slow query",
        "error timeout database",
    ]
}

/// Helper: index a single parquet file and return paths.
fn index_test_file(
    dir: &TempDir,
    num_rows: usize,
    page_size: usize,
    descs: &[&str],
) -> (String, String) {
    let parquet_path = dir.path().join("data.parquet");
    let segment_path = dir.path().join("index.seg");

    write_test_parquet(&parquet_path, num_rows, page_size, descs).unwrap();

    run_index(
        &[parquet_path.to_str().unwrap().to_string()],
        "description",
        segment_path.to_str().unwrap(),
    )
    .unwrap();

    (
        parquet_path.to_str().unwrap().to_string(),
        segment_path.to_str().unwrap().to_string(),
    )
}

#[test]
fn and_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let descs = test_descriptions();
    let (parquet_path, segment_path) = index_test_file(&dir, 100, 25, &descs);

    let result = run_query(
        &segment_path,
        &[parquet_path],
        "description",
        "error timeout",
        Operator::And,
        false,
        None,
    )
    .unwrap();

    // Descriptions 0 and 4 contain both "error" and "timeout" → 2/5 * 100 = 40
    assert_eq!(result.stats.rows_matched, 40);
    assert_eq!(result.matches.len(), 40);

    for m in &result.matches {
        let tokens: HashSet<String> = tokenize(&m.text).into_iter().collect();
        assert!(tokens.contains("error"), "missing 'error' in: {}", m.text);
        assert!(
            tokens.contains("timeout"),
            "missing 'timeout' in: {}",
            m.text
        );
    }
}

#[test]
fn or_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let descs = test_descriptions();
    let (parquet_path, segment_path) = index_test_file(&dir, 100, 25, &descs);

    let result = run_query(
        &segment_path,
        &[parquet_path],
        "description",
        "error timeout",
        Operator::Or,
        false,
        None,
    )
    .unwrap();

    // Descriptions 0, 2, 4 contain "error" or "timeout" → 3/5 * 100 = 60
    assert_eq!(result.stats.rows_matched, 60);
    assert_eq!(result.matches.len(), 60);

    for m in &result.matches {
        let tokens: HashSet<String> = tokenize(&m.text).into_iter().collect();
        assert!(
            tokens.contains("error") || tokens.contains("timeout"),
            "should contain 'error' or 'timeout': {}",
            m.text
        );
    }
}

#[test]
fn null_handling() {
    let dir = tempfile::tempdir().unwrap();
    let parquet_path = dir.path().join("nulls.parquet");
    let segment_path = dir.path().join("nulls.seg");

    // Write a Parquet file where every 3rd row is NULL
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("description", DataType::Utf8, true),
    ]));

    let num_rows = 30;
    let ids: Vec<i32> = (0..num_rows).collect();
    let descs: Vec<Option<&str>> = (0..num_rows as usize)
        .map(|i| {
            if i % 3 == 0 {
                None
            } else {
                Some("error timeout")
            }
        })
        .collect();

    let file = std::fs::File::create(&parquet_path).unwrap();
    let props = WriterProperties::builder()
        .set_data_page_row_count_limit(10)
        .set_max_row_group_size(num_rows as usize)
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(descs)) as ArrayRef,
        ],
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    run_index(
        &[parquet_path.to_str().unwrap().to_string()],
        "description",
        segment_path.to_str().unwrap(),
    )
    .unwrap();

    // Verify segment stats: total_rows includes NULLs
    let seg_data = std::fs::read(&segment_path).unwrap();
    let reader = SegmentReader::open(seg_data).unwrap();
    assert_eq!(reader.corpus_stats().total_rows, 30);

    // Query: only non-NULL rows should match
    let result = run_query(
        segment_path.to_str().unwrap(),
        &[parquet_path.to_str().unwrap().to_string()],
        "description",
        "error timeout",
        Operator::And,
        false,
        None,
    )
    .unwrap();

    // 10 NULLs (rows 0,3,6,...,27), 20 non-NULL rows, all matching
    assert_eq!(result.stats.rows_matched, 20);
}

#[test]
fn multiple_row_groups() {
    let dir = tempfile::tempdir().unwrap();
    let parquet_path = dir.path().join("multi_rg.parquet");
    let segment_path = dir.path().join("multi_rg.seg");

    // Write with small max_row_group_size to force multiple row groups
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("description", DataType::Utf8, true),
    ]));

    let num_rows = 100usize;
    let ids: Vec<i32> = (0..num_rows as i32).collect();
    let descs: Vec<Option<&str>> = (0..num_rows)
        .map(|i| {
            let choices = test_descriptions();
            Some(choices[i % choices.len()])
        })
        .collect();

    let file = std::fs::File::create(&parquet_path).unwrap();
    let props = WriterProperties::builder()
        .set_data_page_row_count_limit(10)
        .set_max_row_group_size(25) // Forces 4 row groups
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(descs)) as ArrayRef,
        ],
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    run_index(
        &[parquet_path.to_str().unwrap().to_string()],
        "description",
        segment_path.to_str().unwrap(),
    )
    .unwrap();

    // Verify segment has pages across all row groups
    let seg_data = std::fs::read(&segment_path).unwrap();
    let reader = SegmentReader::open(seg_data).unwrap();
    assert_eq!(reader.corpus_stats().total_rows, 100);

    // Verify file table shows multiple row groups
    let ft = reader.file_table();
    assert_eq!(ft.len(), 1);
    assert_eq!(ft[0].row_group_count, 4);

    // Query should find results across all row groups
    let result = run_query(
        segment_path.to_str().unwrap(),
        &[parquet_path.to_str().unwrap().to_string()],
        "description",
        "error timeout",
        Operator::And,
        false,
        None,
    )
    .unwrap();

    assert_eq!(result.stats.rows_matched, 40);

    // Verify matches came from multiple row groups
    let rgs: HashSet<u16> = result.matches.iter().map(|m| m.row_group).collect();
    assert!(rgs.len() > 1, "matches should span multiple row groups");
}

#[test]
fn bm25_scoring_order() {
    let dir = tempfile::tempdir().unwrap();
    // "rare" appears in 1/4 descriptions, "common" appears in 3/4
    let descs = vec![
        "rare unique special term",
        "common everyday normal word",
        "common regular standard phrase",
        "common typical ordinary text",
    ];
    let (parquet_path, segment_path) = index_test_file(&dir, 100, 25, &descs);

    // Query for "rare" with scoring
    let result_rare = run_query(
        &segment_path,
        std::slice::from_ref(&parquet_path),
        "description",
        "rare",
        Operator::Or,
        true,
        None,
    )
    .unwrap();

    // Query for "common" with scoring
    let result_common = run_query(
        &segment_path,
        &[parquet_path],
        "description",
        "common",
        Operator::Or,
        true,
        None,
    )
    .unwrap();

    // Rare term should have higher scores than common term
    let max_rare = result_rare
        .matches
        .iter()
        .filter_map(|m| m.score)
        .fold(0.0f64, f64::max);
    let max_common = result_common
        .matches
        .iter()
        .filter_map(|m| m.score)
        .fold(0.0f64, f64::max);

    assert!(
        max_rare > max_common,
        "rare term score ({max_rare}) should exceed common term score ({max_common})"
    );

    // Verify scores are sorted descending
    for m in result_rare.matches.windows(2) {
        assert!(
            m[0].score.unwrap() >= m[1].score.unwrap(),
            "scores should be descending"
        );
    }
}

#[test]
fn empty_result() {
    let dir = tempfile::tempdir().unwrap();
    let descs = test_descriptions();
    let (parquet_path, segment_path) = index_test_file(&dir, 50, 25, &descs);

    let result = run_query(
        &segment_path,
        &[parquet_path],
        "description",
        "nonexistent",
        Operator::Or,
        false,
        None,
    )
    .unwrap();

    assert!(result.matches.is_empty());
    assert_eq!(result.stats.rows_matched, 0);
    assert_eq!(result.stats.rows_scanned, 0);
    assert_eq!(result.stats.candidate_pages, 0);
}

#[test]
fn segment_integrity() {
    let dir = tempfile::tempdir().unwrap();
    let descs = test_descriptions();
    let (parquet_path, segment_path) = index_test_file(&dir, 100, 25, &descs);

    let seg_data = std::fs::read(&segment_path).unwrap();
    let reader = SegmentReader::open(seg_data).unwrap();

    // Verify corpus stats
    assert_eq!(reader.corpus_stats().total_rows, 100);
    assert!(reader.corpus_stats().total_tokens > 0);

    // Verify file table
    assert_eq!(reader.file_table().len(), 1);
    assert_eq!(reader.file_table()[0].path, parquet_path);

    // Verify doc table has entries (should have pages)
    assert!(reader.doc_count() > 0);

    // Verify terms exist
    assert!(reader.term_count() > 0);
    assert!(reader.search_term("error").unwrap().is_some());
    assert!(reader.search_term("timeout").unwrap().is_some());
    assert!(reader.search_term("success").unwrap().is_some());
    assert!(reader.search_term("nonexistent").unwrap().is_none());

    // Verify posting lists are non-empty for known terms
    let error_postings = reader.search_term("error").unwrap().unwrap();
    assert!(!error_postings.is_empty());

    // Verify doc_frequency for "error": appears in descriptions 0, 2, 4 → 3/5 * 100 = 60 rows
    let ord = reader.term_ordinal("error").unwrap();
    let info = reader.term_info(ord).unwrap();
    assert_eq!(info.doc_frequency, 60);
}

#[test]
fn limit_truncates_results() {
    let dir = tempfile::tempdir().unwrap();
    let descs = test_descriptions();
    let (parquet_path, segment_path) = index_test_file(&dir, 100, 25, &descs);

    let result = run_query(
        &segment_path,
        &[parquet_path],
        "description",
        "error",
        Operator::Or,
        false,
        Some(5),
    )
    .unwrap();

    assert_eq!(result.matches.len(), 5);
    // rows_matched should reflect total matches, but matches are truncated
    // Actually our implementation truncates after collecting all, so rows_matched is the true count
    assert_eq!(result.stats.rows_matched, 60);
}

#[test]
fn nested_schema_column_projection() {
    // Regression test: when a struct column precedes the indexed column,
    // the parquet leaf index differs from the arrow field index. The CLI
    // must use the parquet leaf index for ProjectionMask::leaves.
    let dir = tempfile::tempdir().unwrap();
    let parquet_path = dir.path().join("nested.parquet");
    let segment_path = dir.path().join("nested.seg");

    let nested_fields = Fields::from(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("meta", DataType::Struct(nested_fields), false),
        Field::new("description", DataType::Utf8, true),
    ]));

    let meta = StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("b", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40])) as ArrayRef,
        ),
    ]);
    let desc = StringArray::from(vec![
        Some("error timeout"),
        Some("success ok"),
        Some("error reset"),
        Some("warning slow"),
    ]);

    let file = std::fs::File::create(&parquet_path).unwrap();
    let props = WriterProperties::builder()
        .set_data_page_row_count_limit(2)
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(meta) as ArrayRef, Arc::new(desc) as ArrayRef],
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    run_index(
        &[parquet_path.to_str().unwrap().to_string()],
        "description",
        segment_path.to_str().unwrap(),
    )
    .unwrap();

    let result = run_query(
        segment_path.to_str().unwrap(),
        &[parquet_path.to_str().unwrap().to_string()],
        "description",
        "error",
        Operator::Or,
        false,
        None,
    )
    .unwrap();

    assert_eq!(result.stats.rows_matched, 2);
    for m in &result.matches {
        assert!(m.text.contains("error"), "expected 'error' in: {}", m.text);
    }
}

#[test]
fn type_mismatch_across_files_is_error() {
    // If a later file has a non-string column with the same name,
    // indexing should return an error, not panic.
    let dir = tempfile::tempdir().unwrap();
    let good_path = dir.path().join("good.parquet");
    let bad_path = dir.path().join("bad.parquet");
    let segment_path = dir.path().join("mixed.seg");

    // Good file: description is Utf8
    write_test_parquet(&good_path, 10, 5, &["hello world"]).unwrap();

    // Bad file: description is Int32
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("description", DataType::Int32, true),
    ]));
    let file = std::fs::File::create(&bad_path).unwrap();
    let props = WriterProperties::builder()
        .set_data_page_row_count_limit(5)
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef,
        ],
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let result = run_index(
        &[
            good_path.to_str().unwrap().to_string(),
            bad_path.to_str().unwrap().to_string(),
        ],
        "description",
        segment_path.to_str().unwrap(),
    );

    assert!(result.is_err(), "should fail on type mismatch");
    let err_msg = format!("{:#}", result.unwrap_err());
    assert!(
        err_msg.contains("Utf8") || err_msg.contains("expected"),
        "error should mention type: {err_msg}"
    );
}

#[test]
fn nested_struct_field_rejected() {
    // Indexing a leaf inside a struct should give a clear error,
    // not silently misbehave.
    let dir = tempfile::tempdir().unwrap();
    let parquet_path = dir.path().join("nested_leaf.parquet");
    let segment_path = dir.path().join("nested_leaf.seg");

    let nested_fields = Fields::from(vec![
        Field::new("title", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
    ]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("content", DataType::Struct(nested_fields), false),
    ]));

    let title = StringArray::from(vec![Some("hello"), Some("world")]);
    let body = StringArray::from(vec![Some("foo bar"), Some("baz qux")]);
    let content = StructArray::from(vec![
        (
            Arc::new(Field::new("title", DataType::Utf8, true)),
            Arc::new(title) as ArrayRef,
        ),
        (
            Arc::new(Field::new("body", DataType::Utf8, true)),
            Arc::new(body) as ArrayRef,
        ),
    ]);

    let file = std::fs::File::create(&parquet_path).unwrap();
    let props = WriterProperties::builder()
        .set_data_page_row_count_limit(2)
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(content) as ArrayRef,
        ],
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let result = run_index(
        &[parquet_path.to_str().unwrap().to_string()],
        "title",
        segment_path.to_str().unwrap(),
    );

    assert!(result.is_err(), "should reject nested struct field");
    let err_msg = format!("{:#}", result.unwrap_err());
    // validate_arrow_column rejects it: "title" isn't a top-level arrow field
    assert!(
        err_msg.contains("not found") || err_msg.contains("expected"),
        "error should indicate the column can't be indexed: {err_msg}"
    );
}
