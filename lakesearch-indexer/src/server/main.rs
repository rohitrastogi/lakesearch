use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::routing::get;
use axum::Router;
use cascadq_client::{CascadqClient, ClientConfig};
use clap::Parser;
use serde::Deserialize;
use tracing::{error, info};

use lakesearch_core::runtime::LakeRuntime;
use lakesearch_core::storage;
use lakesearch_indexer::server::config::IngestWorkerConfig;

#[derive(Parser)]
#[command(name = "lakesearch-ingest", about = "LakeSearch ingest worker")]
struct Args {
    /// Path to YAML config file
    #[arg(long, default_value = "config.yaml")]
    config: String,
}

/// Task payload matching the shape serialized by the admin service.
#[derive(Debug, Deserialize)]
struct IndexTaskPayload {
    table_location: String,
    files: Vec<String>,
    column: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let config = IngestWorkerConfig::from_file(std::path::Path::new(&args.config))?;

    let runtime = Arc::new(LakeRuntime::new(config.cpu_threads));
    let cascadq = Arc::new(CascadqClient::new(ClientConfig {
        base_url: config.cascadq_url.clone(),
        ..ClientConfig::default()
    }));

    let poll_timeout = config.poll_timeout();

    // Spawn one consumer task per queue
    let mut consumer_handles = Vec::new();
    for queue in &config.queues {
        let handle = tokio::spawn(consume_loop(
            queue.clone(),
            Arc::clone(&cascadq),
            Arc::clone(&runtime),
            poll_timeout,
        ));
        consumer_handles.push(handle);
    }

    info!(queues = ?config.queues, "started consumer tasks");

    // Health endpoint
    let app = Router::new().route("/health", get(health));
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    info!(addr = %config.bind_addr, "starting health endpoint");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Shutdown: abort consumer tasks
    for handle in consumer_handles {
        handle.abort();
    }

    info!("ingest worker stopped");
    Ok(())
}

async fn consume_loop(
    queue: String,
    cascadq: Arc<CascadqClient>,
    runtime: Arc<LakeRuntime>,
    poll_timeout: Duration,
) {
    info!(queue = %queue, "consumer loop started");
    loop {
        let task = match cascadq.claim(&queue, Some(poll_timeout)).await {
            Ok(Some(task)) => task,
            Ok(None) => continue,
            Err(e) => {
                error!(queue = %queue, error = %e, "claim failed");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let task_id = task.task_id().to_owned();
        info!(queue = %queue, task_id = %task_id, "claimed task");

        let payload: IndexTaskPayload = match serde_json::from_value(task.payload().clone()) {
            Ok(p) => p,
            Err(e) => {
                error!(queue = %queue, task_id = %task_id, error = %e, "invalid task payload");
                // Drop task — cascadq will requeue after lease expiry.
                // A malformed payload will fail again on retry, but that's
                // correct: it surfaces the problem rather than silently acking.
                drop(task);
                continue;
            }
        };

        let (store, base) = match storage::parse_location(&payload.table_location) {
            Ok(pair) => pair,
            Err(e) => {
                error!(
                    queue = %queue,
                    task_id = %task_id,
                    location = %payload.table_location,
                    error = %e,
                    "failed to parse table location"
                );
                drop(task);
                continue;
            }
        };

        match lakesearch_indexer::run_index(
            &store,
            &base,
            &payload.files,
            &payload.column,
            &runtime,
        )
        .await
        {
            Ok(()) => {
                if let Err(e) = task.finish().await {
                    error!(queue = %queue, task_id = %task_id, error = %e, "failed to finish task");
                } else {
                    info!(queue = %queue, task_id = %task_id, "task finished");
                }
            }
            Err(e) => {
                error!(queue = %queue, task_id = %task_id, error = %e, "index task failed");
                drop(task);
            }
        }
    }
}

async fn health() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({"status": "ok"}))
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c().await.ok();
    info!("received shutdown signal");
}
