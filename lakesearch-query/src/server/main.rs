use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use tracing::info;

use lakesearch_core::catalog_client::{StaticCatalog, TableInfo};
use lakesearch_core::runtime::LakeRuntime;
use lakesearch_core::storage;
use lakesearch_query::server::cache::TableCache;
use lakesearch_query::server::config::ServerConfig;
use lakesearch_query::server::flight::LakeSearchFlightService;
use lakesearch_query::server::routes::router;
use lakesearch_query::server::state::AppState;

#[derive(Parser)]
#[command(name = "lakesearch-query", about = "LakeSearch query server")]
struct Args {
    /// Path to YAML config file
    #[arg(long, default_value = "config.yaml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let config = ServerConfig::from_file(std::path::Path::new(&args.config))?;

    let runtime = Arc::new(LakeRuntime::new(config.cpu_threads));

    // Build catalog from config
    let mut tables = Vec::new();
    for (name, location) in &config.tables {
        let (store, base) = storage::parse_location(location)?;
        tables.push(TableInfo {
            name: name.clone(),
            location: location.clone(),
            store,
            base,
        });
    }
    let catalog: Arc<dyn lakesearch_core::catalog_client::CatalogClient> =
        Arc::new(StaticCatalog::new(tables));
    info!(tables = config.tables.len(), "loaded catalog");

    let table_cache = Arc::new(TableCache::new(config.io_concurrency));

    let state = AppState {
        config: Arc::new(config.clone()),
        runtime,
        catalog,
        table_cache,
    };

    let app = router(state.clone());
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    info!(addr = %config.bind_addr, "starting REST query server");

    let flight_svc = LakeSearchFlightService::new(state);
    let flight_addr = config.flight_addr;
    info!(addr = %flight_addr, "starting Flight query server");

    // Use select! so an early failure in either server surfaces immediately
    // instead of being masked by the other running indefinitely.
    tokio::select! {
        result = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal()) => {
            result.context("REST server exited")?;
        }
        result = tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(flight_svc))
            .serve_with_shutdown(flight_addr, shutdown_signal()) => {
            result.context("Flight server exited")?;
        }
    }

    info!("server stopped");
    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c().await.ok();
    info!("received shutdown signal");
}
