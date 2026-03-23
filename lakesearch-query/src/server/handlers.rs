use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;

use lakesearch_core::metadata::ColumnStatus;

use super::api_types::*;
use super::error::ApiError;
use super::state::AppState;

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
                indexed_columns: meta
                    .indexed_columns
                    .iter()
                    .filter(|c| c.status != ColumnStatus::Dropped)
                    .map(|c| c.name.clone())
                    .collect(),
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
        indexed_columns: meta
            .indexed_columns
            .iter()
            .filter(|c| c.status != ColumnStatus::Dropped)
            .map(|c| c.name.clone())
            .collect(),
    }))
}

pub async fn search(
    State(state): State<AppState>,
    Path(table_name): Path<String>,
    Json(req): Json<SearchRequest>,
) -> Result<Json<SearchResponse>, ApiError> {
    let start = std::time::Instant::now();

    // Look up table
    let (store, base) = state
        .cache
        .get_store(&table_name)
        .await
        .ok_or_else(|| ApiError::NotFound(format!("table '{table_name}' not found")))?;

    // Validate column exists
    let meta = state
        .cache
        .get_metadata(&table_name)
        .await
        .ok_or_else(|| ApiError::NotFound(format!("table '{table_name}' not found")))?;

    let _col = meta
        .indexed_columns
        .iter()
        .find(|c| c.name == req.search.column && c.status != ColumnStatus::Dropped)
        .ok_or_else(|| {
            ApiError::BadRequest(format!(
                "column '{}' not found or dropped",
                req.search.column
            ))
        })?;

    // Execute query with timeout
    let operator: crate::Operator = req.search.operator.into();
    let result = tokio::time::timeout(
        state.config.query_timeout,
        crate::query::run_query(
            store,
            base,
            req.search.column.clone(),
            &req.search.match_text,
            operator,
            req.score,
            req.limit,
            req.select.clone(),
            Arc::clone(&state.runtime),
        ),
    )
    .await
    .map_err(|_| ApiError::Timeout)?
    .map_err(ApiError::Internal)?;

    let elapsed_ms = start.elapsed().as_millis() as u64;

    Ok(Json(SearchResponse {
        rows: result
            .matches
            .into_iter()
            .map(|m| SearchRow {
                file: m.file,
                row_group: m.row_group,
                text: m.text,
                score: m.score,
                columns: m.columns,
            })
            .collect(),
        stats: SearchStats {
            candidate_pages: result.stats.candidate_pages,
            rows_scanned: result.stats.rows_scanned,
            rows_matched: result.stats.rows_matched,
            elapsed_ms,
        },
    }))
}
