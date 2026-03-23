use axum::extract::{Path, State};
use axum::Json;
use tracing::info;

use lakesearch_core::metadata::{
    ColumnStatus, CurrentPointer, IndexedColumn, Metadata, Snapshot,
};

use crate::cas;
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
            backfill_manifest_lists: None,
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

    let columns = columns_to_info(&metadata.indexed_columns);

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

pub async fn update_columns(
    State(state): State<AppState>,
    Path(table_id): Path<String>,
    Json(req): Json<UpdateColumnsRequest>,
) -> Result<Json<UpdateColumnsResponse>, ApiError> {
    if req.add.is_empty() && req.drop.is_empty() {
        return Err(ApiError::BadRequest(
            "at least one add or drop is required".to_owned(),
        ));
    }

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

    let add_defs = req.add;
    let drop_names = req.drop;

    cas::commit_metadata(
        reg.store.as_ref(),
        &reg.base,
        current.e_tag,
        &metadata,
        |meta| {
            let mut new = meta.clone();

            // Drop columns (soft-delete)
            for col in &mut new.indexed_columns {
                if drop_names.contains(&col.name) && col.status != ColumnStatus::Dropped {
                    col.status = ColumnStatus::Dropped;
                    col.backfill_manifest_lists = None;
                }
            }

            // Add new columns (skip if name already exists)
            for def in &add_defs {
                if new.indexed_columns.iter().any(|c| c.name == def.name) {
                    continue;
                }
                new.indexed_columns.push(IndexedColumn {
                    name: def.name.clone(),
                    tokenizer: def.tokenizer.clone(),
                    status: ColumnStatus::Active,
                    backfill_manifest_lists: None,
                });
            }

            new
        },
    )
    .await
    .map_err(ApiError::Internal)?;

    // Re-read committed state for response
    let current = storage::read_current(reg.store.as_ref(), &reg.base)
        .await
        .map_err(ApiError::Internal)?;
    let final_meta = storage::read_metadata(reg.store.as_ref(), &current.value)
        .await
        .map_err(ApiError::Internal)?;

    let columns = columns_to_info(&final_meta.indexed_columns);

    info!(table_id = %table_id, "updated columns");

    Ok(Json(UpdateColumnsResponse { columns }))
}

pub async fn ingest(
    State(state): State<AppState>,
    Path(table_id): Path<String>,
    Json(req): Json<IngestRequest>,
) -> Result<Json<IngestResponse>, ApiError> {
    if req.files.is_empty() {
        return Err(ApiError::BadRequest("files list is empty".to_owned()));
    }

    let reg = state
        .registry
        .get(&table_id)
        .await
        .ok_or_else(|| ApiError::NotFound(format!("table '{table_id}' not found")))?;

    // Read metadata to get active columns
    let current = storage::read_current(reg.store.as_ref(), &reg.base)
        .await
        .map_err(ApiError::Internal)?;
    let metadata = storage::read_metadata(reg.store.as_ref(), &current.value)
        .await
        .map_err(ApiError::Internal)?;

    // Determine which columns to push tasks for
    let target_columns: Vec<String> = if req.columns.is_empty() {
        metadata
            .indexed_columns
            .iter()
            .filter(|c| c.status != ColumnStatus::Dropped)
            .map(|c| c.name.clone())
            .collect()
    } else {
        // Validate that requested columns exist and are not dropped
        for col_name in &req.columns {
            let col = metadata
                .indexed_columns
                .iter()
                .find(|c| c.name == *col_name);
            match col {
                None => {
                    return Err(ApiError::BadRequest(format!(
                        "column '{col_name}' not found"
                    )))
                }
                Some(c) if c.status == ColumnStatus::Dropped => {
                    return Err(ApiError::BadRequest(format!(
                        "column '{col_name}' is dropped"
                    )))
                }
                _ => {}
            }
        }
        req.columns
    };

    let mut tasks_pushed = 0;
    for column in &target_columns {
        let payload = IndexTaskPayload {
            table_location: reg.location.clone(),
            files: req.files.clone(),
            column: column.clone(),
        };
        let json_payload = serde_json::to_value(&payload).map_err(|e| {
            ApiError::Internal(anyhow::anyhow!("failed to serialize task payload: {e}"))
        })?;

        state
            .cascadq
            .push(&reg.queue, json_payload)
            .await
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("failed to push to cascadq: {e}")))?;

        tasks_pushed += 1;
    }

    info!(
        table_id = %table_id,
        files = req.files.len(),
        columns = target_columns.len(),
        tasks_pushed,
        "pushed ingest tasks"
    );

    Ok(Json(IngestResponse { tasks_pushed }))
}

pub async fn start_backfill(
    State(state): State<AppState>,
    Path(table_id): Path<String>,
    Json(req): Json<StartBackfillRequest>,
) -> Result<Json<StartBackfillResponse>, ApiError> {
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

    // Validate column exists and is not dropped
    let col = metadata
        .indexed_columns
        .iter()
        .find(|c| c.name == req.column)
        .ok_or_else(|| ApiError::NotFound(format!("column '{}' not found", req.column)))?;
    if col.status == ColumnStatus::Dropped {
        return Err(ApiError::BadRequest(format!(
            "column '{}' is dropped",
            req.column
        )));
    }

    // Snapshot current manifest lists
    let snapshot_lists = metadata.snapshot.manifest_lists.clone();
    let snapshot_count = snapshot_lists.len();
    let column_name = req.column.clone();

    // CAS commit: set column status to Backfilling with frozen snapshot
    cas::commit_metadata(
        reg.store.as_ref(),
        &reg.base,
        current.e_tag,
        &metadata,
        |meta| {
            let mut new = meta.clone();
            if let Some(col) = new.indexed_columns.iter_mut().find(|c| c.name == column_name) {
                col.status = ColumnStatus::Backfilling;
                col.backfill_manifest_lists = Some(snapshot_lists.clone());
            }
            new
        },
    )
    .await
    .map_err(ApiError::Internal)?;

    info!(
        table_id = %table_id,
        column = %req.column,
        manifest_lists = snapshot_count,
        "started backfill"
    );

    Ok(Json(StartBackfillResponse {
        column: req.column,
        status: "backfilling".to_owned(),
        manifest_lists_snapshot: snapshot_count,
    }))
}

pub async fn backfill_status(
    State(state): State<AppState>,
    Path((table_id, column)): Path<(String, String)>,
) -> Result<Json<BackfillStatusResponse>, ApiError> {
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

    let col = metadata
        .indexed_columns
        .iter()
        .find(|c| c.name == column)
        .ok_or_else(|| ApiError::NotFound(format!("column '{column}' not found")))?;

    let status_str = serde_json::to_value(&col.status)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_else(|| format!("{:?}", col.status));

    // If not backfilling (or no snapshot), return zeroed progress
    let backfill_lists = match &col.backfill_manifest_lists {
        Some(lists) if col.status == ColumnStatus::Backfilling => lists,
        _ => {
            return Ok(Json(BackfillStatusResponse {
                column,
                status: status_str,
                total_files: 0,
                indexed_files: 0,
                uncovered_files: 0,
                progress_pct: if col.status == ColumnStatus::Active {
                    100.0
                } else {
                    0.0
                },
            }));
        }
    };

    let result = crate::backfill::find_uncovered_files(
        reg.store.as_ref(),
        &metadata,
        &column,
        backfill_lists,
        8, // default io_concurrency
    )
    .await
    .map_err(ApiError::Internal)?;

    let progress_pct = if result.total_files == 0 {
        100.0
    } else {
        (result.indexed_files as f64 / result.total_files as f64) * 100.0
    };

    Ok(Json(BackfillStatusResponse {
        column,
        status: status_str,
        total_files: result.total_files,
        indexed_files: result.indexed_files,
        uncovered_files: result.uncovered.len(),
        progress_pct,
    }))
}

fn columns_to_info(columns: &[IndexedColumn]) -> Vec<ColumnInfo> {
    columns
        .iter()
        .map(|c| ColumnInfo {
            name: c.name.clone(),
            tokenizer: c.tokenizer.clone(),
            status: serde_json::to_value(&c.status)
                .ok()
                .and_then(|v| v.as_str().map(String::from))
                .unwrap_or_else(|| format!("{:?}", c.status)),
        })
        .collect()
}
