use axum::routing::{delete, get, post};
use axum::Router;
use tower_http::trace::TraceLayer;

use super::handlers;
use super::state::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(handlers::health))
        .route("/tables", post(handlers::create_table))
        .route("/tables", get(handlers::list_tables))
        .route("/tables/{table_id}", get(handlers::get_table))
        .route("/tables/{table_id}", delete(handlers::delete_table))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
