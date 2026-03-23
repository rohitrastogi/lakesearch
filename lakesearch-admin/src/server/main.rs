use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tracing::info;

use cascadq_client::{CascadqClient, ClientConfig};
use lakesearch_admin::reconcile;
use lakesearch_admin::registry::TableRegistry;
use lakesearch_admin::server::config::IngestConfig;
use lakesearch_admin::server::routes::router;
use lakesearch_admin::server::state::AppState;
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

    let registry = Arc::new(TableRegistry::new());

    // Register tables from config
    for (name, table_cfg) in &config.tables {
        let (store, base) = storage::parse_location(&table_cfg.location)?;
        let current = storage::read_current(store.as_ref(), &base).await?;
        let metadata = storage::read_metadata(store.as_ref(), &current.value).await?;

        registry
            .register(
                &metadata.table_id,
                name,
                &table_cfg.location,
                &table_cfg.queue,
            )
            .await?;
        info!(
            table = %name,
            table_id = %metadata.table_id,
            location = %table_cfg.location,
            "registered table from config"
        );
    }

    let config = Arc::new(config);

    // Start backfill reconciliation loop
    let reconcile_handle = reconcile::start(
        Arc::clone(&config),
        Arc::clone(&registry),
        Arc::clone(&cascadq),
    );

    let state = AppState {
        config: Arc::clone(&config),
        cascadq,
        registry,
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
