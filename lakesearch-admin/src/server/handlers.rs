use axum::extract::{Path, State};
use axum::Json;
use tracing::info;

use lakesearch_core::metadata::{
    ColumnStatus, CurrentPointer, IndexedColumn, Metadata, Snapshot,
};

use crate::storage;

use super::api_types::*;
use super::error::ApiError;
use super::state::AppState;

pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_owned(),
    })
}

pub async fn create_table(
    State(state): State<AppState>,
    Json(req): Json<CreateTableRequest>,
) -> Result<Json<CreateTableResponse>, ApiError> {
    if req.columns.is_empty() {
        return Err(ApiError::BadRequest(
            "at least one column is required".to_owned(),
        ));
    }

    let (store, base) = storage::parse_location(&req.location).map_err(ApiError::Internal)?;

    if storage::current_exists(store.as_ref(), &base)
        .await
        .map_err(ApiError::Internal)?
    {
        return Err(ApiError::Conflict(format!(
            "table already exists at {}",
            req.location
        )));
    }

    let table_id = uuid::Uuid::new_v4().to_string();

    let indexed_columns: Vec<IndexedColumn> = req
        .columns
        .iter()
        .map(|c| IndexedColumn {
            name: c.name.clone(),
            tokenizer: c.tokenizer.clone(),
            status: ColumnStatus::Active,
        })
        .collect();

    let metadata = Metadata {
        format_version: 1,
        table_id: table_id.clone(),
        table_name: req.table_name.clone(),
        location: req.location.clone(),
        indexed_columns,
        snapshot: Snapshot {
            timestamp_ms: chrono::Utc::now().timestamp_millis() as u64,
            manifest_lists: vec![],
        },
    };

    let meta_path = storage::write_metadata(store.as_ref(), &base, &metadata)
        .await
        .map_err(ApiError::Internal)?;

    let pointer = CurrentPointer {
        metadata_path: meta_path,
        updated_at: chrono::Utc::now().to_rfc3339(),
    };
    storage::write_json(
        store.as_ref(),
        &base.child("metadata").child("current.json"),
        &pointer,
    )
    .await
    .map_err(ApiError::Internal)?;

    // Determine queue name: use table_name as default queue
    let queue = state
        .config
        .tables
        .get(&req.table_name)
        .map(|t| t.queue.clone())
        .unwrap_or_else(|| format!("{}-index", req.table_name));

    state
        .registry
        .register(&table_id, &req.table_name, &req.location, &queue)
        .await
        .map_err(ApiError::Internal)?;

    info!(
        table_id = %table_id,
        table_name = %req.table_name,
        location = %req.location,
        "created table"
    );

    Ok(Json(CreateTableResponse {
        table_id,
        location: req.location,
    }))
}

pub async fn list_tables(State(state): State<AppState>) -> Json<ListTablesResponse> {
    let tables = state.registry.list().await;
    let summaries = tables
        .into_iter()
        .map(|t| TableSummary {
            table_id: t.table_id,
            table_name: t.table_name,
            location: t.location,
        })
        .collect();
    Json(ListTablesResponse { tables: summaries })
}

pub async fn get_table(
    State(state): State<AppState>,
    Path(table_id): Path<String>,
) -> Result<Json<TableInfoResponse>, ApiError> {
    let reg = state
        .registry
        .get(&table_id)
        .await
        .ok_or_else(|| ApiError::NotFound(format!("table '{table_id}' not found")))?;

    let current = storage::read_current(reg.store.as_ref(), &reg.base)
        .await
        .map_err(ApiError::Internal)?;
    let metadata = storage::read_metadata(reg.store.as_ref(), &current.value)
        .await
        .map_err(ApiError::Internal)?;

    let columns = metadata
        .indexed_columns
        .iter()
        .map(|c| ColumnInfo {
            name: c.name.clone(),
            tokenizer: c.tokenizer.clone(),
            status: serde_json::to_value(&c.status)
                .ok()
                .and_then(|v| v.as_str().map(String::from))
                .unwrap_or_else(|| format!("{:?}", c.status)),
        })
        .collect();

    Ok(Json(TableInfoResponse {
        table_id: metadata.table_id,
        table_name: metadata.table_name,
        location: metadata.location,
        columns,
    }))
}

pub async fn delete_table(
    State(state): State<AppState>,
    Path(table_id): Path<String>,
) -> Result<Json<DeleteTableResponse>, ApiError> {
    if !state.registry.unregister(&table_id).await {
        return Err(ApiError::NotFound(format!(
            "table '{table_id}' not found in registry"
        )));
    }

    info!(table_id = %table_id, "unregistered table (data not deleted)");

    Ok(Json(DeleteTableResponse {
        table_id,
        message: "table unregistered (data not deleted)".to_owned(),
    }))
}
