//! HTTP and Arrow Flight server integration tests using InMemory object store.

mod helpers;

use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::{FlightDescriptor, Ticket};
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use tonic::transport::Endpoint;

use arrow_flight::FlightClient;
use lakesearch_cli::index::run_index;
use lakesearch_core::runtime::LakeRuntime;
use lakesearch_query::server::api_types::{
    HealthResponse, ListTablesResponse, SearchResponse, TableInfo,
};
use lakesearch_query::server::cache::MetadataCache;
use lakesearch_query::server::config::ServerConfig;
use lakesearch_query::server::flight::LakeSearchFlightService;
use lakesearch_query::server::routes::router;
use lakesearch_query::server::state::AppState;

use helpers::{create_test_table, upload_test_parquet};

/// Builds shared test state: creates table, registers with cache.
async fn build_test_state(store: Arc<dyn ObjectStore>) -> AppState {
    let base = Path::from("table");
    create_test_table(store.as_ref(), &base, &["description"]).await;

    let cache = Arc::new(MetadataCache::new(std::time::Duration::from_secs(60), 8));
    cache
        .register_with_store("test", store.clone(), base)
        .await
        .unwrap();

    let config = ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        flight_addr: "127.0.0.1:0".parse().unwrap(),
        query_timeout_secs: 30,
        metadata_poll_secs: 60,
        cpu_threads: 2,
        io_concurrency: 8,
        max_io_tasks: 64,
        tables: std::collections::HashMap::new(),
    };

    AppState {
        config: Arc::new(config),
        runtime: Arc::new(LakeRuntime::new(2)),
        cache,
    }
}

/// Starts a REST-only test server. Returns the base URL and handle.
async fn start_test_server(store: Arc<dyn ObjectStore>) -> (String, tokio::task::JoinHandle<()>) {
    let state = build_test_state(store).await;
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

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base_url}/v1/tables/test/search"))
        .json(&serde_json::json!({
            "search": {"column": "description", "match": "error timeout", "operator": "and"},
            "score": "indexed",
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
    // Rows should have "text" and "score" fields
    assert!(body.rows[0].contains_key("text"));
    assert!(body.rows[0].contains_key("score"));
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

// ---------------------------------------------------------------------------
// Arrow Flight tests
// ---------------------------------------------------------------------------

/// Starts a Flight-only server on a random port. Returns the URL and handle.
async fn start_flight_server(store: Arc<dyn ObjectStore>) -> (String, tokio::task::JoinHandle<()>) {
    let state = build_test_state(store).await;
    let flight_svc = LakeSearchFlightService::new(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(flight_svc))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    (format!("http://{addr}"), handle)
}

#[tokio::test]
async fn flight_do_get_round_trip() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");

    // Upload test data
    upload_test_parquet(
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

    // Start server first (creates initial table metadata)
    let (url, handle) = start_flight_server(Arc::clone(&store)).await;

    // Index after server start so current.json exists
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

    let channel = Endpoint::from_shared(url).unwrap().connect().await.unwrap();
    let mut client = FlightClient::new(channel);

    let ticket = Ticket::new(
        serde_json::to_vec(&serde_json::json!({
            "table": "test",
            "column": "description",
            "match": "error timeout",
            "operator": "and"
        }))
        .unwrap(),
    );

    let record_batch_stream = client.do_get(ticket).await.unwrap();
    let batches: Vec<_> = record_batch_stream.try_collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // 2/5 descriptions have both "error" and "timeout" -> 40 matches
    assert_eq!(total_rows, 40);

    handle.abort();
}

#[tokio::test]
async fn flight_get_flight_info_returns_schema() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");

    upload_test_parquet(store.as_ref(), "data/test.parquet", 10, 5, &["hello world"]).await;

    // Start server first (creates initial table metadata)
    let (url, handle) = start_flight_server(Arc::clone(&store)).await;

    // Index after server start so current.json exists
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

    let channel = Endpoint::from_shared(url).unwrap().connect().await.unwrap();
    let mut client = FlightClient::new(channel);

    let cmd = serde_json::to_vec(&serde_json::json!({
        "table": "test",
        "column": "description",
        "match": "hello",
        "select": ["id"],
        "score": "indexed"
    }))
    .unwrap();

    let descriptor = FlightDescriptor::new_cmd(cmd);
    let flight_info = client.get_flight_info(descriptor).await.unwrap();

    let schema = flight_info.try_decode_schema().unwrap();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(field_names.contains(&"text"));
    assert!(field_names.contains(&"id"));
    assert!(field_names.contains(&"score"));

    handle.abort();
}

#[tokio::test]
async fn flight_table_not_found() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (url, handle) = start_flight_server(store).await;

    let channel = Endpoint::from_shared(url).unwrap().connect().await.unwrap();
    let mut client = FlightClient::new(channel);

    let ticket = Ticket::new(
        serde_json::to_vec(&serde_json::json!({
            "table": "nonexistent",
            "column": "description",
            "match": "test"
        }))
        .unwrap(),
    );

    let result = client.do_get(ticket).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("not found"),
        "expected NotFound, got: {err}"
    );

    handle.abort();
}
