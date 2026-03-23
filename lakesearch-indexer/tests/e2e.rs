//! End-to-end pipeline tests: admin payload → ingest worker → query server.
//!
//! Verifies the JSON contract between the admin service and the ingest
//! worker, then runs the full index → query path through the HTTP API.

mod helpers;

use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use serde::Deserialize;

use lakesearch_admin::server::api_types::IndexTaskPayload;
use lakesearch_core::catalog_client::{CatalogClient, StaticCatalog, TableInfo};
use lakesearch_core::runtime::LakeRuntime;
use lakesearch_indexer::run_index;
use lakesearch_query::server::api_types::SearchResponse;
use lakesearch_query::server::cache::TableCache;
use lakesearch_query::server::config::ServerConfig;
use lakesearch_query::server::routes::router;
use lakesearch_query::server::state::AppState;

use helpers::{create_test_table, upload_test_parquet};

/// Mirrors the ingest worker's private `IndexTaskPayload` (Deserialize).
/// The test proves this stays in sync with admin's Serialize version.
#[derive(Debug, Deserialize)]
struct IngestTaskPayload {
    table_location: String,
    files: Vec<String>,
    column: String,
}

/// Starts a test query server. Returns the base URL and server task handle.
async fn start_query_server(
    store: Arc<dyn ObjectStore>,
    table_base: Path,
) -> (String, tokio::task::JoinHandle<()>) {
    let catalog: Arc<dyn CatalogClient> = Arc::new(StaticCatalog::new(vec![TableInfo {
        name: "test".to_owned(),
        location: "mem://table/".to_owned(),
        store,
        base: table_base,
    }]));

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

    let state = AppState {
        config: Arc::new(config),
        runtime: Arc::new(LakeRuntime::new(2)),
        catalog,
        table_cache: Arc::new(TableCache::new(8)),
    };

    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{addr}"), handle)
}

/// Simulates the ingest worker: deserializes the admin payload and calls
/// run_index. Uses the provided store directly (InMemory doesn't support
/// parse_location).
async fn simulate_ingest(
    store: &Arc<dyn ObjectStore>,
    index_base: &Path,
    payload_json: serde_json::Value,
    runtime: &LakeRuntime,
) {
    let task: IngestTaskPayload = serde_json::from_value(payload_json)
        .expect("ingest worker should deserialize admin payload");
    run_index(store, index_base, &task.files, &task.column, runtime)
        .await
        .unwrap();
}

// -------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------

/// Admin serializes `IndexTaskPayload` → JSON → ingest worker deserializes.
/// Catches field renames or type changes that would silently break the
/// pipeline at runtime.
#[tokio::test]
async fn payload_contract_round_trip() {
    let admin_payload = IndexTaskPayload {
        table_location: "s3://bucket/warehouse/events/".to_owned(),
        files: vec![
            "data/part-001.parquet".to_owned(),
            "data/part-002.parquet".to_owned(),
        ],
        column: "description".to_owned(),
    };

    let json = serde_json::to_value(&admin_payload).expect("admin payload should serialize");
    let ingest: IngestTaskPayload =
        serde_json::from_value(json).expect("ingest worker should deserialize admin payload");

    assert_eq!(ingest.table_location, admin_payload.table_location);
    assert_eq!(ingest.files, admin_payload.files);
    assert_eq!(ingest.column, admin_payload.column);
}

/// Full pipeline: admin payload → ingest worker indexes → query server
/// returns correct results.
#[tokio::test]
async fn admin_ingest_query_pipeline() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let table_base = Path::from("table");
    let index_base = table_base.child("lakesearch");

    create_test_table(store.as_ref(), &index_base, &["description"]).await;
    let file_path = upload_test_parquet(
        store.as_ref(),
        "data/events.parquet",
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

    // Admin handler builds and serializes the payload
    let admin_payload = IndexTaskPayload {
        table_location: "mem://table/".to_owned(),
        files: vec![file_path],
        column: "description".to_owned(),
    };
    let payload_json = serde_json::to_value(&admin_payload).unwrap();

    // Ingest worker processes the task
    let runtime = LakeRuntime::new(2);
    simulate_ingest(&store, &index_base, payload_json, &runtime).await;

    // Query server returns results
    let (base_url, handle) = start_query_server(Arc::clone(&store), table_base).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base_url}/v1/tables/test/search"))
        .json(&serde_json::json!({
            "search": {"column": "description", "match": "error timeout", "operator": "and"},
            "score": "indexed",
            "limit": 5
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: SearchResponse = resp.json().await.unwrap();

    // 2/5 descriptions have both "error" and "timeout" → 40 matches out of 100
    assert_eq!(body.stats.rows_matched, 40);
    assert!(body.rows.len() <= 5);
    assert!(body.rows[0].contains_key("text"));
    assert!(body.rows[0].contains_key("score"));

    // Index was used: candidate_pages > 0 proves the query consulted the
    // segment rather than brute-forcing. All pages are candidates here
    // because descriptions cycle uniformly across pages.
    assert!(
        body.stats.candidate_pages > 0,
        "query should use index, not brute-force"
    );

    handle.abort();
}

/// Multiple ingest batches (simulating sequential admin ingest calls) are
/// all queryable afterward.
#[tokio::test]
async fn multiple_ingest_batches_queryable() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let table_base = Path::from("table");
    let index_base = table_base.child("lakesearch");

    create_test_table(store.as_ref(), &index_base, &["description"]).await;

    let file1 = upload_test_parquet(
        store.as_ref(),
        "data/batch1.parquet",
        50,
        25,
        &["alpha bravo charlie", "alpha delta echo"],
    )
    .await;

    let file2 = upload_test_parquet(
        store.as_ref(),
        "data/batch2.parquet",
        50,
        25,
        &["foxtrot golf hotel", "foxtrot india juliet"],
    )
    .await;

    let runtime = LakeRuntime::new(2);

    // Two separate ingest tasks, processed sequentially by the worker
    for file in [&file1, &file2] {
        let payload = IndexTaskPayload {
            table_location: "mem://table/".to_owned(),
            files: vec![file.clone()],
            column: "description".to_owned(),
        };
        simulate_ingest(
            &store,
            &index_base,
            serde_json::to_value(&payload).unwrap(),
            &runtime,
        )
        .await;
    }

    let (base_url, handle) = start_query_server(Arc::clone(&store), table_base).await;
    let client = reqwest::Client::new();

    // "alpha" — only in batch 1, index should be used
    let resp: SearchResponse = client
        .post(format!("{base_url}/v1/tables/test/search"))
        .json(&serde_json::json!({
            "search": {"column": "description", "match": "alpha"}
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp.stats.rows_matched, 50);
    assert!(
        resp.stats.candidate_pages > 0,
        "query should use index for batch 1 term"
    );

    // "foxtrot" — only in batch 2, index should be used
    let resp: SearchResponse = client
        .post(format!("{base_url}/v1/tables/test/search"))
        .json(&serde_json::json!({
            "search": {"column": "description", "match": "foxtrot"}
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp.stats.rows_matched, 50);
    assert!(
        resp.stats.candidate_pages > 0,
        "query should use index for batch 2 term"
    );

    // "alpha foxtrot" OR — rows from both batches, both segments used
    let resp: SearchResponse = client
        .post(format!("{base_url}/v1/tables/test/search"))
        .json(&serde_json::json!({
            "search": {"column": "description", "match": "alpha foxtrot", "operator": "or"}
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp.stats.rows_matched, 100);
    assert!(
        resp.stats.candidate_pages > 0,
        "query should use index across both segments"
    );

    handle.abort();
}
