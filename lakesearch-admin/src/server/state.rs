use std::sync::Arc;

use cascadq_client::CascadqClient;

use super::config::IngestConfig;

/// Shared application state for axum handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<IngestConfig>,
    pub cascadq: Arc<CascadqClient>,
}
