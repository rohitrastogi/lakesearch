use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use axum::extract::{Path, State};
use axum::Json;

use lakesearch_core::metadata::{ColumnStatus, IndexedColumn};

use super::api_types::*;
use super::error::ApiError;
use super::state::AppState;

fn active_column_names(columns: &[IndexedColumn]) -> Vec<String> {
    columns
        .iter()
        .filter(|c| c.status != ColumnStatus::Dropped)
        .map(|c| c.name.clone())
        .collect()
}

/// Converts `RecordBatch`es to JSON row maps using `arrow_json::ArrayWriter`.
fn batches_to_json_rows(
    batches: &[RecordBatch],
) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, anyhow::Error> {
    if batches.is_empty() {
        return Ok(vec![]);
    }
    let mut buf = Vec::new();
    {
        let mut writer = arrow_json::ArrayWriter::new(&mut buf);
        let batch_refs: Vec<&RecordBatch> = batches.iter().collect();
        writer.write_batches(&batch_refs)?;
        writer.finish()?;
    }
    let rows: Vec<serde_json::Map<String, serde_json::Value>> = serde_json::from_slice(&buf)?;
    Ok(rows)
}

pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_owned(),
    })
}

pub async fn list_tables(State(state): State<AppState>) -> Json<ListTablesResponse> {
    let names = state.cache.table_names().await;
    let mut tables = Vec::with_capacity(names.len());
    for name in names {
        if let Some(meta) = state.cache.get_metadata(&name).await {
            tables.push(TableInfo {
                name,
                location: meta.location.clone(),
                indexed_columns: active_column_names(&meta.indexed_columns),
            });
        }
    }
    Json(ListTablesResponse { tables })
}

pub async fn get_table(
    State(state): State<AppState>,
    Path(table_name): Path<String>,
) -> Result<Json<TableInfo>, ApiError> {
    let meta = state
        .cache
        .get_metadata(&table_name)
        .await
        .ok_or_else(|| ApiError::NotFound(format!("table '{table_name}' not found")))?;

    Ok(Json(TableInfo {
        name: table_name,
        location: meta.location.clone(),
        indexed_columns: active_column_names(&meta.indexed_columns),
    }))
}

pub async fn search(
    State(state): State<AppState>,
    Path(table_name): Path<String>,
    Json(req): Json<SearchRequest>,
) -> Result<Json<SearchResponse>, ApiError> {
    let start = std::time::Instant::now();

    let (object_cache, base, meta) = state
        .cache
        .get_table_state(&table_name)
        .await
        .ok_or_else(|| ApiError::NotFound(format!("table '{table_name}' not found")))?;

    meta.indexed_columns
        .iter()
        .find(|c| c.name == req.search.column && c.status != ColumnStatus::Dropped)
        .ok_or_else(|| {
            ApiError::BadRequest(format!(
                "column '{}' not found or dropped",
                req.search.column
            ))
        })?;

    let operator: crate::Operator = req.search.operator.into();
    let result = tokio::time::timeout(
        state.config.query_timeout(),
        crate::query::run_query_collected(
            object_cache,
            base,
            req.search.column.clone(),
            &req.search.match_text,
            operator,
            req.score.into(),
            req.limit,
            req.select.clone(),
            state.config.io_concurrency,
            Arc::clone(&state.runtime),
        ),
    )
    .await
    .map_err(|_| ApiError::Timeout)?
    .map_err(ApiError::Internal)?;

    let elapsed_ms = start.elapsed().as_millis() as u64;

    let json_rows = batches_to_json_rows(&result.batches).map_err(ApiError::Internal)?;

    Ok(Json(SearchResponse {
        rows: json_rows,
        stats: SearchStats {
            candidate_pages: result.stats.candidate_pages,
            rows_scanned: result.stats.rows_scanned,
            rows_matched: result.stats.rows_matched,
            elapsed_ms,
        },
    }))
}
