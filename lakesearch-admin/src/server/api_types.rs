use serde::Serialize;

// --- Health ---

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
}
