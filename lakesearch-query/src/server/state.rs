use std::sync::Arc;

use lakesearch_core::runtime::LakeRuntime;

use super::cache::MetadataCache;
use super::config::ServerConfig;

/// Shared application state for axum handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<ServerConfig>,
    pub runtime: Arc<LakeRuntime>,
    pub cache: Arc<MetadataCache>,
}
