use axum::routing::{delete, get, post, put};
use axum::Router;
use tower_http::trace::TraceLayer;

use super::handlers;
use super::state::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(handlers::health))
        .route("/tables", post(handlers::create_table))
        .route("/tables", get(handlers::list_tables))
        .route("/tables/{table_name}", get(handlers::get_table))
        .route("/tables/{table_name}", delete(handlers::delete_table))
        .route(
            "/tables/{table_name}/columns",
            put(handlers::update_columns),
        )
        .route("/tables/{table_name}/ingest", post(handlers::ingest))
        .route(
            "/tables/{table_name}/backfill",
            post(handlers::start_backfill),
        )
        .route(
            "/tables/{table_name}/backfill/{column}",
            get(handlers::backfill_status),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
