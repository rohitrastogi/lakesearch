use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tracing::info;

use cascadq_client::{CascadqClient, ClientConfig};
use lakesearch_admin::reconcile;
use lakesearch_admin::server::config::IngestConfig;
use lakesearch_admin::server::routes::router;
use lakesearch_admin::server::state::AppState;
use lakesearch_core::catalog_client::{StaticCatalog, TableInfo};
use lakesearch_core::storage;

#[derive(Parser)]
#[command(name = "lakesearch-admin", about = "LakeSearch admin / ingest service")]
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
    let config = IngestConfig::from_file(std::path::Path::new(&args.config))?;

    let cascadq = Arc::new(CascadqClient::new(ClientConfig {
        base_url: config.cascadq_url.clone(),
        ..ClientConfig::default()
    }));

    // Build catalog from config tables
    let mut tables = Vec::new();
    for (name, location) in &config.tables {
        let (store, base) = storage::parse_location(location)?;
        tables.push(TableInfo {
            name: name.clone(),
            location: location.clone(),
            store,
            base,
        });
        info!(table = %name, location = %location, "registered table");
    }
    let catalog: Arc<dyn lakesearch_core::catalog_client::CatalogClient> =
        Arc::new(StaticCatalog::new(tables));

    let config = Arc::new(config);

    // Start backfill reconciliation loop
    let reconcile_handle = reconcile::start(
        Arc::clone(&config),
        Arc::clone(&catalog),
        Arc::clone(&cascadq),
    );

    let state = AppState {
        config: Arc::clone(&config),
        cascadq,
        catalog,
    };

    let app = router(state);
    let bind_addr = config.bind_addr;
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    info!(addr = %bind_addr, "starting admin server");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    reconcile_handle.abort();
    info!("server stopped");
    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c().await.ok();
    info!("received shutdown signal");
}
