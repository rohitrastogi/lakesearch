use std::sync::Arc;

use cascadq_client::CascadqClient;

use crate::registry::TableRegistry;

use super::config::IngestConfig;

/// Shared application state for axum handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<IngestConfig>,
    pub cascadq: Arc<CascadqClient>,
    pub registry: Arc<TableRegistry>,
}
