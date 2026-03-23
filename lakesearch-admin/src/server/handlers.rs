use axum::Json;

use super::api_types::*;

pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_owned(),
    })
}
