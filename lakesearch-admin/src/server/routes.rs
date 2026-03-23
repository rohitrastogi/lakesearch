use axum::routing::get;
use axum::Router;
use tower_http::trace::TraceLayer;

use super::handlers;
use super::state::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(handlers::health))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
