use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tracing::info;

use cascadq_client::{CascadqClient, ClientConfig};
use lakesearch_admin::server::config::IngestConfig;
use lakesearch_admin::server::routes::router;
use lakesearch_admin::server::state::AppState;

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

    let state = AppState {
        config: Arc::new(config.clone()),
        cascadq,
    };

    let app = router(state);
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    info!(addr = %config.bind_addr, "starting admin server");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("server stopped");
    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c().await.ok();
    info!("received shutdown signal");
}
