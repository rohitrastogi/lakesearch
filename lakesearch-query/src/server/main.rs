use std::sync::Arc;

use anyhow::Result;
use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use tracing::info;

use lakesearch_core::runtime::LakeRuntime;
use lakesearch_query::server::cache::MetadataCache;
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
    let cache = Arc::new(MetadataCache::new(
        config.metadata_poll_interval(),
        config.io_concurrency,
    ));

    // Register tables from config
    for (name, location) in &config.tables {
        info!(table = %name, location = %location, "registering table");
        cache.register(name, location).await?;
    }

    let poll_handle = cache.start_polling();

    let state = AppState {
        config: Arc::new(config.clone()),
        runtime,
        cache,
    };

    let app = router(state.clone());
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    info!(addr = %config.bind_addr, "starting REST query server");

    let flight_svc = LakeSearchFlightService::new(state);
    let flight_addr = config.flight_addr;
    info!(addr = %flight_addr, "starting Flight query server");

    let (rest_result, flight_result) = tokio::join!(
        axum::serve(listener, app).with_graceful_shutdown(shutdown_signal()),
        tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(flight_svc))
            .serve_with_shutdown(flight_addr, shutdown_signal()),
    );
    rest_result?;
    flight_result?;

    poll_handle.abort();
    info!("server stopped");
    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c().await.ok();
    info!("received shutdown signal");
}
