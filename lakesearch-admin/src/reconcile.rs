//! Background reconciliation loop for backfill progress.
//!
//! For each registered table, checks columns with `status == backfilling`
//! and `backfill_manifest_lists` set. Pushes one chunk of uncovered files
//! to cascadq per iteration. When all files are covered, transitions the
//! column to `active` via CAS.

use anyhow::Result;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use cascadq_client::CascadqClient;
use lakesearch_core::metadata::{ColumnStatus, Metadata};
use object_store::path::Path;
use object_store::ObjectStore;

use crate::backfill::find_uncovered_files;
use crate::registry::TableRegistry;
use crate::server::api_types::IndexTaskPayload;
use crate::server::config::IngestConfig;
use lakesearch_core::cas;
use lakesearch_core::storage;

/// Starts the reconciliation loop. Returns a `JoinHandle` that can be
/// aborted for graceful shutdown.
pub fn start(
    config: Arc<IngestConfig>,
    registry: Arc<TableRegistry>,
    cascadq: Arc<CascadqClient>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let interval = config.backfill_poll_interval();
        loop {
            if let Err(e) = reconcile_once(&config, &registry, &cascadq).await {
                error!(error = %e, "reconciliation loop error");
            }
            tokio::time::sleep(interval).await;
        }
    })
}

async fn reconcile_once(
    config: &IngestConfig,
    registry: &TableRegistry,
    cascadq: &CascadqClient,
) -> Result<()> {
    let tables = registry.list().await;

    for table in tables {
        let current = match storage::read_current(table.store.as_ref(), &table.base).await {
            Ok(c) => c,
            Err(e) => {
                warn!(
                    table_id = %table.table_id,
                    error = %e,
                    "failed to read current.json, skipping"
                );
                continue;
            }
        };
        let metadata = match storage::read_metadata(table.store.as_ref(), &current.value).await {
            Ok(m) => m,
            Err(e) => {
                warn!(
                    table_id = %table.table_id,
                    error = %e,
                    "failed to read metadata, skipping"
                );
                continue;
            }
        };

        for col in &metadata.indexed_columns {
            if col.status != ColumnStatus::Backfilling {
                continue;
            }
            let backfill_lists = match &col.backfill_manifest_lists {
                Some(lists) => lists,
                None => continue,
            };

            // Empty snapshot (e.g. brand-new table) → nothing to backfill
            if backfill_lists.is_empty() {
                let column_name = col.name.clone();
                info!(
                    table_id = %table.table_id,
                    column = %column_name,
                    "empty backfill snapshot, transitioning to active"
                );
                if let Err(e) = transition_to_active(
                    table.store.as_ref(),
                    &table.base,
                    current.e_tag.clone(),
                    &metadata,
                    &column_name,
                )
                .await
                {
                    warn!(
                        table_id = %table.table_id,
                        column = %col.name,
                        error = %e,
                        "failed to transition column to active"
                    );
                }
                continue;
            }

            let uncovered = match find_uncovered_files(
                table.store.as_ref(),
                &metadata,
                &col.name,
                backfill_lists,
                config.io_concurrency,
            )
            .await
            {
                Ok(u) => u,
                Err(e) => {
                    warn!(
                        table_id = %table.table_id,
                        column = %col.name,
                        error = %e,
                        "failed to find uncovered files"
                    );
                    continue;
                }
            };

            if uncovered.uncovered.is_empty() {
                let column_name = col.name.clone();
                info!(
                    table_id = %table.table_id,
                    column = %column_name,
                    "backfill complete, transitioning to active"
                );
                if let Err(e) = transition_to_active(
                    table.store.as_ref(),
                    &table.base,
                    current.e_tag.clone(),
                    &metadata,
                    &column_name,
                )
                .await
                {
                    warn!(
                        table_id = %table.table_id,
                        column = %col.name,
                        error = %e,
                        "failed to transition column to active"
                    );
                }
                continue;
            }

            // Push one chunk of uncovered files
            let chunk: Vec<String> = uncovered
                .uncovered
                .into_iter()
                .take(config.backfill_chunk_size)
                .collect();

            let payload = IndexTaskPayload {
                table_location: table.location.clone(),
                files: chunk.clone(),
                column: col.name.clone(),
            };

            let json_payload = match serde_json::to_value(&payload) {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "failed to serialize backfill task payload");
                    continue;
                }
            };

            match cascadq.push(&table.queue, json_payload).await {
                Ok(()) => {
                    info!(
                        table_id = %table.table_id,
                        column = %col.name,
                        files = chunk.len(),
                        remaining = uncovered.total_files - uncovered.indexed_files - chunk.len(),
                        "pushed backfill chunk"
                    );
                }
                Err(e) => {
                    warn!(
                        table_id = %table.table_id,
                        column = %col.name,
                        error = %e,
                        "failed to push backfill chunk to cascadq"
                    );
                }
            }
        }
    }

    Ok(())
}

/// Transitions a backfilling column to active via CAS.
///
/// Guards against concurrent drops: if the column was dropped between
/// the read and the CAS rebase, the closure preserves the dropped status
/// instead of overwriting it with active.
async fn transition_to_active(
    store: &dyn ObjectStore,
    base: &Path,
    etag: Option<String>,
    metadata: &Metadata,
    column_name: &str,
) -> Result<()> {
    let name = column_name.to_owned();
    cas::commit_metadata(store, base, etag, metadata, None, |meta| {
        let mut new = meta.clone();
        if let Some(c) = new.indexed_columns.iter_mut().find(|c| c.name == name) {
            // Only transition if still backfilling — don't overwrite a concurrent drop
            if c.status == ColumnStatus::Backfilling {
                c.status = ColumnStatus::Active;
                c.backfill_manifest_lists = None;
            }
        }
        new
    })
    .await
}
