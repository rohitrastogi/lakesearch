use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

/// Unified error type for admin API handlers.
pub enum ApiError {
    NotFound(String),
    BadRequest(String),
    Conflict(String),
    Internal(anyhow::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            Self::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            Self::Conflict(msg) => (StatusCode::CONFLICT, msg),
            Self::Internal(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")),
        };
        (status, Json(json!({"error": message}))).into_response()
    }
}
