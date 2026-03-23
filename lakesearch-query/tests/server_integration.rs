//! HTTP server integration tests using InMemory object store.

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
use lakesearch_core::tokenizer::DEFAULT_TOKENIZER;
use lakesearch_query::server::api_types::{
    HealthResponse, ListTablesResponse, SearchResponse, TableInfo,
};
use lakesearch_query::server::cache::MetadataCache;
use lakesearch_query::server::config::ServerConfig;
use lakesearch_query::server::routes::router;
use lakesearch_query::server::state::AppState;
use lakesearch_query::storage::write_json;

/// Uploads a test Parquet file to the InMemory store.
async fn upload_test_parquet(
    store: &dyn ObjectStore,
    path: &str,
    num_rows: usize,
    descriptions: &[&str],
) {
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
        .set_data_page_row_count_limit(25)
        .set_max_row_group_size(num_rows)
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    store
        .put(&Path::from(path), PutPayload::from(Bytes::from(buf)))
        .await
        .unwrap();
}

/// Creates table metadata in the InMemory store.
async fn create_table_metadata(store: &dyn ObjectStore, base: &Path) {
    let metadata = Metadata {
        format_version: 1,
        table_id: "test-id".to_owned(),
        table_name: "test".to_owned(),
        location: "mem://table/".to_owned(),
        indexed_columns: vec![IndexedColumn {
            name: "description".to_owned(),
            tokenizer: DEFAULT_TOKENIZER.to_owned(),
            status: ColumnStatus::Active,
        }],
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

/// Starts a test server with the given InMemory store and table registered.
/// Returns the base URL and a handle to the server task.
async fn start_test_server(store: Arc<dyn ObjectStore>) -> (String, tokio::task::JoinHandle<()>) {
    let base = Path::from("table");
    create_table_metadata(store.as_ref(), &base).await;

    let cache = Arc::new(MetadataCache::new(std::time::Duration::from_secs(60)));
    cache
        .register_with_store("test", store.clone(), base)
        .await
        .unwrap();

    let config = ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        query_timeout: std::time::Duration::from_secs(30),
        metadata_poll_interval: std::time::Duration::from_secs(60),
        cpu_threads: 2,
        tables: vec![],
    };

    let state = AppState {
        config: Arc::new(config),
        runtime: Arc::new(LakeRuntime::new(2)),
        cache,
    };

    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{addr}"), handle)
}

#[tokio::test]
async fn health_check() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (base_url, handle) = start_test_server(store).await;

    let resp: HealthResponse = reqwest::get(format!("{base_url}/health"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(resp.status, "ok");
    handle.abort();
}

#[tokio::test]
async fn list_and_get_tables() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (base_url, handle) = start_test_server(store).await;

    let resp: ListTablesResponse = reqwest::get(format!("{base_url}/tables"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(resp.tables.len(), 1);
    assert_eq!(resp.tables[0].name, "test");

    let resp: TableInfo = reqwest::get(format!("{base_url}/tables/test"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(resp.name, "test");
    assert_eq!(resp.indexed_columns, vec!["description"]);

    handle.abort();
}

#[tokio::test]
async fn search_round_trip() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");

    // Upload and index
    upload_test_parquet(
        store.as_ref(),
        "data/test.parquet",
        100,
        &[
            "error timeout connection refused",
            "success response ok",
            "error connection reset",
            "warning slow query",
            "error timeout database",
        ],
    )
    .await;

    let (base_url, handle) = start_test_server(Arc::clone(&store)).await;

    // Index via CLI library
    let runtime = LakeRuntime::new(2);
    run_index(
        &store,
        &base,
        &["data/test.parquet".to_owned()],
        "description",
        &runtime,
    )
    .await
    .unwrap();

    // Force cache refresh (metadata changed after indexing)
    // We need to wait or re-register. For tests, just re-query — run_query
    // reads current.json directly so it picks up the new metadata.

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/v1/tables/test/search"))
        .json(&serde_json::json!({
            "search": {"column": "description", "match": "error timeout", "operator": "and"},
            "score": true,
            "limit": 3
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: SearchResponse = resp.json().await.unwrap();

    // 2/5 descriptions have both "error" and "timeout" → 40 matches
    assert_eq!(body.stats.rows_matched, 40);
    assert_eq!(body.rows.len(), 3);
    assert!(body.rows[0].score.is_some());
    assert!(body.stats.elapsed_ms > 0);

    handle.abort();
}

#[tokio::test]
async fn search_table_not_found() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (base_url, handle) = start_test_server(store).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/v1/tables/nonexistent/search"))
        .json(&serde_json::json!({
            "search": {"column": "description", "match": "test"}
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);
    handle.abort();
}

#[tokio::test]
async fn search_bad_column() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (base_url, handle) = start_test_server(store).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/v1/tables/test/search"))
        .json(&serde_json::json!({
            "search": {"column": "nonexistent", "match": "test"}
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    handle.abort();
}
