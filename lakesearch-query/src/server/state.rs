use std::sync::Arc;

use lakesearch_core::catalog_client::CatalogClient;
use lakesearch_core::runtime::LakeRuntime;

use super::cache::TableCache;
use super::config::ServerConfig;

/// Shared application state for axum handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<ServerConfig>,
    pub runtime: Arc<LakeRuntime>,
    pub catalog: Arc<dyn CatalogClient>,
    pub table_cache: Arc<TableCache>,
}
