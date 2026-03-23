use axum::extract::{Path, State};
use axum::Json;
use tracing::info;

use lakesearch_core::catalog_client::TableInfo;
use lakesearch_core::metadata::{ColumnStatus, CurrentPointer, IndexedColumn, Metadata, Snapshot};

use lakesearch_core::cas;
use lakesearch_core::storage;

use super::api_types::*;
use super::error::ApiError;
use super::state::AppState;

/// Helper: look up a table by name from the catalog.
async fn lookup_table(state: &AppState, table_name: &str) -> Result<TableInfo, ApiError> {
    state
        .catalog
        .get_table(table_name)
        .await
        .map_err(ApiError::Internal)?
        .ok_or_else(|| ApiError::NotFound(format!("table '{table_name}' not found")))
}

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

    // Register in catalog (which also parses the location)
    let table = state
        .catalog
        .register_table(&req.table_name, &req.location)
        .await
        .map_err(ApiError::Internal)?;

    // Check if index metadata already exists
    if storage::current_exists(table.store.as_ref(), &table.index_base())
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

    let meta_path = storage::write_metadata(table.store.as_ref(), &table.index_base(), &metadata)
        .await
        .map_err(ApiError::Internal)?;

    let pointer = CurrentPointer {
        metadata_path: meta_path,
        updated_at: chrono::Utc::now().to_rfc3339(),
    };
    storage::write_json(
        table.store.as_ref(),
        &table.index_base().child("metadata").child("current.json"),
        &pointer,
    )
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

pub async fn list_tables(
    State(state): State<AppState>,
) -> Result<Json<ListTablesResponse>, ApiError> {
    let tables = state
        .catalog
        .list_tables()
        .await
        .map_err(ApiError::Internal)?;
    let summaries = tables
        .into_iter()
        .map(|t| TableSummary {
            table_name: t.name,
            location: t.location,
        })
        .collect();
    Ok(Json(ListTablesResponse { tables: summaries }))
}

pub async fn get_table(
    State(state): State<AppState>,
    Path(table_name): Path<String>,
) -> Result<Json<TableInfoResponse>, ApiError> {
    let table = lookup_table(&state, &table_name).await?;

    let current = storage::read_current(table.store.as_ref(), &table.index_base())
        .await
        .map_err(ApiError::Internal)?;
    let metadata = storage::read_metadata(table.store.as_ref(), &current.value)
        .await
        .map_err(ApiError::Internal)?;

    let columns = columns_to_info(&metadata.indexed_columns);

    Ok(Json(TableInfoResponse {
        table_name: metadata.table_name,
        location: metadata.location,
        columns,
    }))
}

pub async fn delete_table(
    State(state): State<AppState>,
    Path(table_name): Path<String>,
) -> Result<Json<DeleteTableResponse>, ApiError> {
    let found = state
        .catalog
        .unregister_table(&table_name)
        .await
        .map_err(ApiError::Internal)?;

    if !found {
        return Err(ApiError::NotFound(format!(
            "table '{table_name}' not found"
        )));
    }

    info!(table_name = %table_name, "unregistered table (data not deleted)");

    Ok(Json(DeleteTableResponse {
        table_name,
        message: "table unregistered (data not deleted)".to_owned(),
    }))
}

pub async fn update_columns(
    State(state): State<AppState>,
    Path(table_name): Path<String>,
    Json(req): Json<UpdateColumnsRequest>,
) -> Result<Json<UpdateColumnsResponse>, ApiError> {
    if req.add.is_empty() && req.drop.is_empty() {
        return Err(ApiError::BadRequest(
            "at least one add or drop is required".to_owned(),
        ));
    }

    let table = lookup_table(&state, &table_name).await?;

    let current = storage::read_current(table.store.as_ref(), &table.index_base())
        .await
        .map_err(ApiError::Internal)?;
    let metadata = storage::read_metadata(table.store.as_ref(), &current.value)
        .await
        .map_err(ApiError::Internal)?;

    let add_defs = req.add;
    let drop_names = req.drop;

    let apply_changes = |meta: &Metadata| {
        let mut new = meta.clone();

        for col in &mut new.indexed_columns {
            if drop_names.contains(&col.name) && col.status != ColumnStatus::Dropped {
                col.status = ColumnStatus::Dropped;
                col.backfill_manifest_lists = None;
            }
        }

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
    };

    cas::commit_metadata(
        table.store.as_ref(),
        &table.index_base(),
        current.e_tag,
        &metadata,
        None,
        &apply_changes,
    )
    .await
    .map_err(ApiError::Internal)?;

    let committed = apply_changes(&metadata);
    let columns = columns_to_info(&committed.indexed_columns);

    info!(table_name = %table_name, "updated columns");

    Ok(Json(UpdateColumnsResponse { columns }))
}

pub async fn ingest(
    State(state): State<AppState>,
    Path(table_name): Path<String>,
    Json(req): Json<IngestRequest>,
) -> Result<Json<IngestResponse>, ApiError> {
    if req.files.is_empty() {
        return Err(ApiError::BadRequest("files list is empty".to_owned()));
    }

    let table = lookup_table(&state, &table_name).await?;

    let current = storage::read_current(table.store.as_ref(), &table.index_base())
        .await
        .map_err(ApiError::Internal)?;
    let metadata = storage::read_metadata(table.store.as_ref(), &current.value)
        .await
        .map_err(ApiError::Internal)?;

    let target_columns: Vec<String> = if req.columns.is_empty() {
        metadata
            .indexed_columns
            .iter()
            .filter(|c| c.status != ColumnStatus::Dropped)
            .map(|c| c.name.clone())
            .collect()
    } else {
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

    let queue = table.queue_name();
    let mut tasks_pushed = 0;
    for column in &target_columns {
        let payload = IndexTaskPayload {
            table_location: table.location.clone(),
            files: req.files.clone(),
            column: column.clone(),
        };
        let json_payload = serde_json::to_value(&payload).map_err(|e| {
            ApiError::Internal(anyhow::anyhow!("failed to serialize task payload: {e}"))
        })?;

        state
            .cascadq
            .push(&queue, json_payload)
            .await
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("failed to push to cascadq: {e}")))?;

        tasks_pushed += 1;
    }

    info!(
        table_name = %table_name,
        files = req.files.len(),
        columns = target_columns.len(),
        tasks_pushed,
        "pushed ingest tasks"
    );

    Ok(Json(IngestResponse { tasks_pushed }))
}

pub async fn start_backfill(
    State(state): State<AppState>,
    Path(table_name): Path<String>,
    Json(req): Json<StartBackfillRequest>,
) -> Result<Json<StartBackfillResponse>, ApiError> {
    let table = lookup_table(&state, &table_name).await?;

    let current = storage::read_current(table.store.as_ref(), &table.index_base())
        .await
        .map_err(ApiError::Internal)?;
    let metadata = storage::read_metadata(table.store.as_ref(), &current.value)
        .await
        .map_err(ApiError::Internal)?;

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

    let snapshot_lists = metadata.snapshot.manifest_lists.clone();
    let snapshot_count = snapshot_lists.len();
    let column_name = req.column.clone();

    cas::commit_metadata(
        table.store.as_ref(),
        &table.index_base(),
        current.e_tag,
        &metadata,
        None,
        |meta| {
            let mut new = meta.clone();
            if let Some(col) = new
                .indexed_columns
                .iter_mut()
                .find(|c| c.name == column_name)
            {
                col.status = ColumnStatus::Backfilling;
                col.backfill_manifest_lists = Some(snapshot_lists.clone());
            }
            new
        },
    )
    .await
    .map_err(ApiError::Internal)?;

    info!(
        table_name = %table_name,
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
    Path((table_name, column)): Path<(String, String)>,
) -> Result<Json<BackfillStatusResponse>, ApiError> {
    let table = lookup_table(&state, &table_name).await?;

    let current = storage::read_current(table.store.as_ref(), &table.index_base())
        .await
        .map_err(ApiError::Internal)?;
    let metadata = storage::read_metadata(table.store.as_ref(), &current.value)
        .await
        .map_err(ApiError::Internal)?;

    let col = metadata
        .indexed_columns
        .iter()
        .find(|c| c.name == column)
        .ok_or_else(|| ApiError::NotFound(format!("column '{column}' not found")))?;

    let backfill_lists = match &col.backfill_manifest_lists {
        Some(lists) if col.status == ColumnStatus::Backfilling => lists,
        _ => {
            return Ok(Json(BackfillStatusResponse {
                column,
                status: col.status,
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
        table.store.as_ref(),
        &metadata,
        &column,
        backfill_lists,
        state.config.io_concurrency,
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
        status: col.status,
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
            status: c.status,
        })
        .collect()
}
