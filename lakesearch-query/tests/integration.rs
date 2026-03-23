//! Integration tests for object storage commands using InMemory store.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use lakesearch_cli::index::run_index;
use lakesearch_core::metadata::{ColumnStatus, CurrentPointer, IndexedColumn, Metadata, Snapshot};
use lakesearch_core::runtime::LakeRuntime;
use lakesearch_query::object_cache::ObjectCache;
use lakesearch_query::query::{self, QueryResult};
use lakesearch_query::storage::{read_current, read_metadata, write_json};
use lakesearch_query::Operator;

/// Test helper: wraps run_query with reference-based args for convenience.
#[allow(clippy::too_many_arguments)]
async fn run_query(
    store: &Arc<dyn ObjectStore>,
    base: &Path,
    column: &str,
    query_text: &str,
    operator: Operator,
    with_score: bool,
    limit: Option<usize>,
    select_columns: &[String],
    _runtime: &LakeRuntime,
) -> anyhow::Result<QueryResult> {
    let cache = Arc::new(ObjectCache::new(Arc::clone(store)));
    query::run_query(
        cache,
        base.clone(),
        column.to_owned(),
        query_text,
        operator,
        with_score,
        limit,
        select_columns.to_vec(),
        Arc::new(LakeRuntime::new(2)),
    )
    .await
}

/// Creates a test Parquet file in memory and uploads it to the InMemory store.
/// Returns the path where it was stored.
async fn upload_test_parquet(
    store: &dyn ObjectStore,
    path: &str,
    num_rows: usize,
    page_size_rows: usize,
    descriptions: &[&str],
) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("description", DataType::Utf8, true),
    ]));

    let ids: Vec<i32> = (0..num_rows as i32).collect();
    let descs: Vec<Option<&str>> = (0..num_rows)
        .map(|i| Some(descriptions[i % descriptions.len()]))
        .collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(descs)) as ArrayRef,
        ],
    )
    .unwrap();

    let mut buf = Vec::new();
    let props = WriterProperties::builder()
        .set_data_page_row_count_limit(page_size_rows)
        .set_max_row_group_size(num_rows)
        .set_dictionary_enabled(false)
        .build();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    let obj_path = Path::from(path);
    store
        .put(&obj_path, PutPayload::from(Bytes::from(buf)))
        .await
        .unwrap();

    path.to_owned()
}

/// Creates a table with initial metadata in the InMemory store.
async fn create_test_table(store: &dyn ObjectStore, base: &Path, columns: &[&str]) {
    let indexed_columns: Vec<IndexedColumn> = columns
        .iter()
        .map(|name| IndexedColumn {
            name: (*name).to_owned(),
            tokenizer: lakesearch_core::tokenizer::DEFAULT_TOKENIZER.to_owned(),
            status: ColumnStatus::Active,
        })
        .collect();

    let metadata = Metadata {
        format_version: 1,
        table_id: "test-table-id".to_owned(),
        table_name: "test".to_owned(),
        location: "mem://table/".to_owned(),
        indexed_columns,
        snapshot: Snapshot {
            timestamp_ms: 1000,
            manifest_lists: vec![],
        },
    };

    let meta_path = format!("{}/metadata/metadata-init.json", base);
    write_json(store, &Path::from(meta_path.as_str()), &metadata)
        .await
        .unwrap();

    let pointer = CurrentPointer {
        metadata_path: meta_path,
        updated_at: "2026-01-01T00:00:00Z".to_owned(),
    };
    write_json(
        store,
        &base.child("metadata").child("current.json"),
        &pointer,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn create_index_query_round_trip() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    // Create table
    create_test_table(store.as_ref(), &base, &["description"]).await;

    // Upload test parquet
    let file_path = upload_test_parquet(
        store.as_ref(),
        "data/test.parquet",
        100,
        25,
        &[
            "error timeout connection refused",
            "success response ok",
            "error connection reset",
            "warning slow query",
            "error timeout database",
        ],
    )
    .await;

    // Index
    run_index(
        &store,
        &base,
        std::slice::from_ref(&file_path),
        "description",
        &runtime,
    )
    .await
    .unwrap();

    // Verify metadata was updated
    let current = read_current(store.as_ref(), &base).await.unwrap();
    let meta = read_metadata(store.as_ref(), &current.value).await.unwrap();
    assert_eq!(meta.snapshot.manifest_lists.len(), 1);

    // Query: AND
    let result = run_query(
        &store,
        &base,
        "description",
        "error timeout",
        Operator::And,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();

    // Descriptions 0 and 4 contain both "error" and "timeout" → 2/5 * 100 = 40
    assert_eq!(result.stats.rows_matched, 40);

    // Query: OR
    let result = run_query(
        &store,
        &base,
        "description",
        "error timeout",
        Operator::Or,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();

    // Descriptions 0, 2, 4 contain "error" or "timeout" → 3/5 * 100 = 60
    assert_eq!(result.stats.rows_matched, 60);
}

#[tokio::test]
async fn multiple_appends_both_queried() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    create_test_table(store.as_ref(), &base, &["description"]).await;

    // Upload and index file A (has "alpha")
    let file_a = upload_test_parquet(
        store.as_ref(),
        "data/a.parquet",
        20,
        10,
        &["alpha bravo charlie"],
    )
    .await;
    run_index(&store, &base, &[file_a], "description", &runtime)
        .await
        .unwrap();

    // Upload and index file B (has "delta")
    let file_b = upload_test_parquet(
        store.as_ref(),
        "data/b.parquet",
        20,
        10,
        &["delta echo foxtrot"],
    )
    .await;
    run_index(&store, &base, &[file_b], "description", &runtime)
        .await
        .unwrap();

    // Metadata should have 2 manifest lists
    let current = read_current(store.as_ref(), &base).await.unwrap();
    let meta = read_metadata(store.as_ref(), &current.value).await.unwrap();
    assert_eq!(meta.snapshot.manifest_lists.len(), 2);

    // Query for "alpha" — only in file A
    let result = run_query(
        &store,
        &base,
        "description",
        "alpha",
        Operator::Or,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();
    assert_eq!(result.stats.rows_matched, 20);

    // Query for "delta" — only in file B
    let result = run_query(
        &store,
        &base,
        "description",
        "delta",
        Operator::Or,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();
    assert_eq!(result.stats.rows_matched, 20);

    // Query for "nonexistent" — in neither
    let result = run_query(
        &store,
        &base,
        "description",
        "nonexistent",
        Operator::Or,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();
    assert_eq!(result.stats.rows_matched, 0);
}

#[tokio::test]
async fn batch_dedup_prevents_double_index() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    create_test_table(store.as_ref(), &base, &["description"]).await;

    let file_path = upload_test_parquet(
        store.as_ref(),
        "data/dedup.parquet",
        20,
        10,
        &["hello world"],
    )
    .await;

    // Index once
    run_index(
        &store,
        &base,
        std::slice::from_ref(&file_path),
        "description",
        &runtime,
    )
    .await
    .unwrap();

    // Index again with same files — should be skipped
    run_index(
        &store,
        &base,
        std::slice::from_ref(&file_path),
        "description",
        &runtime,
    )
    .await
    .unwrap();

    // Should still have only 1 manifest list
    let current = read_current(store.as_ref(), &base).await.unwrap();
    let meta = read_metadata(store.as_ref(), &current.value).await.unwrap();
    assert_eq!(meta.snapshot.manifest_lists.len(), 1);

    // Query should find 20 matches, not 40 (no double-counting)
    let result = run_query(
        &store,
        &base,
        "description",
        "hello",
        Operator::Or,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();
    assert_eq!(result.stats.rows_matched, 20);
}

#[tokio::test]
async fn bm25_scoring_across_segments() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    create_test_table(store.as_ref(), &base, &["description"]).await;

    // File with "rare" in 1/4 descriptions
    let file_path = upload_test_parquet(
        store.as_ref(),
        "data/scoring.parquet",
        100,
        25,
        &[
            "rare unique special term",
            "common everyday normal word",
            "common regular standard phrase",
            "common typical ordinary text",
        ],
    )
    .await;

    run_index(&store, &base, &[file_path], "description", &runtime)
        .await
        .unwrap();

    let result = run_query(
        &store,
        &base,
        "description",
        "rare",
        Operator::Or,
        true,
        Some(5),
        &[],
        &runtime,
    )
    .await
    .unwrap();

    assert_eq!(result.matches.len(), 5);
    // All scores should be positive and finite
    for m in &result.matches {
        let s = m.score.unwrap();
        assert!(s > 0.0 && s.is_finite(), "bad score: {s}");
    }
    // Scores should be sorted descending
    for w in result.matches.windows(2) {
        assert!(w[0].score.unwrap() >= w[1].score.unwrap());
    }
}

#[tokio::test]
async fn empty_table_query() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    create_test_table(store.as_ref(), &base, &["description"]).await;

    let result = run_query(
        &store,
        &base,
        "description",
        "anything",
        Operator::Or,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();

    assert!(result.matches.is_empty());
    assert_eq!(result.stats.rows_matched, 0);
}

#[tokio::test]
async fn select_projects_additional_columns() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    create_test_table(store.as_ref(), &base, &["description"]).await;

    let file_path = upload_test_parquet(
        store.as_ref(),
        "data/select.parquet",
        10,
        5,
        &["error timeout"],
    )
    .await;

    run_index(
        &store,
        &base,
        std::slice::from_ref(&file_path),
        "description",
        &runtime,
    )
    .await
    .unwrap();

    // Query with --select id
    let result = run_query(
        &store,
        &base,
        "description",
        "error",
        Operator::Or,
        false,
        Some(3),
        &["id".to_owned()],
        &runtime,
    )
    .await
    .unwrap();

    assert_eq!(result.matches.len(), 3);
    for m in &result.matches {
        let cols = m.columns.as_ref().expect("should have columns");
        assert!(cols.contains_key("id"), "should have 'id' column");
        assert!(cols["id"].is_number(), "id should be a number");
    }
}

#[tokio::test]
async fn select_without_columns_omits_field() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    create_test_table(store.as_ref(), &base, &["description"]).await;

    let file_path = upload_test_parquet(
        store.as_ref(),
        "data/noselect.parquet",
        10,
        5,
        &["hello world"],
    )
    .await;

    run_index(
        &store,
        &base,
        std::slice::from_ref(&file_path),
        "description",
        &runtime,
    )
    .await
    .unwrap();

    let result = run_query(
        &store,
        &base,
        "description",
        "hello",
        Operator::Or,
        false,
        Some(1),
        &[],
        &runtime,
    )
    .await
    .unwrap();

    assert_eq!(result.matches.len(), 1);
    // columns field should be None (omitted in JSON)
    assert!(result.matches[0].columns.is_none());
}
