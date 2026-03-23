use axum::routing::{get, post};
use axum::Router;
use tower_http::trace::TraceLayer;

use super::handlers;
use super::state::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(handlers::health))
        .route("/tables", get(handlers::list_tables))
        .route("/tables/{table_name}", get(handlers::get_table))
        .route("/v1/tables/{table_name}/search", post(handlers::search))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
