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

    let mut tasks = futures::stream::FuturesUnordered::new();
    for table in tables {
        tasks.push(reconcile_table(config, cascadq, table));
    }

    use futures::StreamExt;
    while let Some(result) = tasks.next().await {
        if let Err(e) = result {
            error!(error = %e, "table reconciliation failed");
        }
    }

    Ok(())
}

async fn reconcile_table(
    config: &IngestConfig,
    cascadq: &CascadqClient,
    table: crate::registry::RegisteredTable,
) -> Result<()> {
    let current = match storage::read_current(table.store.as_ref(), &table.base).await {
        Ok(c) => c,
        Err(e) => {
            warn!(
                table_id = %table.table_id,
                error = %e,
                "failed to read current.json, skipping"
            );
            return Ok(());
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
            return Ok(());
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

    Ok(())
}

/// Transitions a backfilling column to active via CAS.
///
/// Guards against concurrent drops: if the column was dropped between
/// the read and the CAS rebase, the closure preserves the dropped status
/// instead of overwriting it with active.
pub(crate) async fn transition_to_active(
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

#[cfg(test)]
mod tests {
    use super::*;
    use lakesearch_core::metadata::*;
    use lakesearch_core::storage::{read_current, read_metadata, write_json, write_metadata};
    use object_store::memory::InMemory;

    fn test_metadata(columns: Vec<IndexedColumn>) -> Metadata {
        Metadata {
            format_version: 1,
            table_id: "test".to_owned(),
            table_name: "test".to_owned(),
            location: "mem://test/".to_owned(),
            indexed_columns: columns,
            snapshot: Snapshot {
                timestamp_ms: 1000,
                manifest_lists: vec![],
            },
        }
    }

    async fn setup_table(store: &InMemory, base: &Path, metadata: &Metadata) -> Option<String> {
        let meta_path = write_metadata(store, base, metadata).await.unwrap();
        let pointer = CurrentPointer {
            metadata_path: meta_path,
            updated_at: "t0".to_owned(),
        };
        write_json(
            store,
            &base.child("metadata").child("current.json"),
            &pointer,
        )
        .await
        .unwrap();
        let result = read_current(store, base).await.unwrap();
        result.e_tag
    }

    #[tokio::test]
    async fn transition_backfilling_to_active() {
        let store = InMemory::new();
        let base = Path::from("table");

        let meta = test_metadata(vec![IndexedColumn {
            name: "desc".to_owned(),
            tokenizer: "whitespace_lowercase".to_owned(),
            status: ColumnStatus::Backfilling,
            backfill_manifest_lists: Some(vec![]),
        }]);

        let etag = setup_table(&store, &base, &meta).await;

        transition_to_active(&store, &base, etag, &meta, "desc")
            .await
            .unwrap();

        let result = read_current(&store, &base).await.unwrap();
        let final_meta = read_metadata(&store, &result.value).await.unwrap();

        assert_eq!(final_meta.indexed_columns[0].status, ColumnStatus::Active);
        assert!(final_meta.indexed_columns[0]
            .backfill_manifest_lists
            .is_none());
    }

    #[tokio::test]
    async fn transition_preserves_dropped_status() {
        let store = InMemory::new();
        let base = Path::from("table");

        // Column is already dropped (simulates concurrent drop)
        let meta = test_metadata(vec![IndexedColumn {
            name: "desc".to_owned(),
            tokenizer: "whitespace_lowercase".to_owned(),
            status: ColumnStatus::Dropped,
            backfill_manifest_lists: None,
        }]);

        let etag = setup_table(&store, &base, &meta).await;

        transition_to_active(&store, &base, etag, &meta, "desc")
            .await
            .unwrap();

        let result = read_current(&store, &base).await.unwrap();
        let final_meta = read_metadata(&store, &result.value).await.unwrap();

        // Should still be dropped, not overwritten to active
        assert_eq!(final_meta.indexed_columns[0].status, ColumnStatus::Dropped);
    }

    #[tokio::test]
    async fn transition_ignores_unknown_column() {
        let store = InMemory::new();
        let base = Path::from("table");

        let meta = test_metadata(vec![IndexedColumn {
            name: "desc".to_owned(),
            tokenizer: "whitespace_lowercase".to_owned(),
            status: ColumnStatus::Backfilling,
            backfill_manifest_lists: Some(vec![]),
        }]);

        let etag = setup_table(&store, &base, &meta).await;

        // Transition a column that doesn't exist — should succeed without error
        transition_to_active(&store, &base, etag, &meta, "nonexistent")
            .await
            .unwrap();

        let result = read_current(&store, &base).await.unwrap();
        let final_meta = read_metadata(&store, &result.value).await.unwrap();

        // desc should still be backfilling
        assert_eq!(
            final_meta.indexed_columns[0].status,
            ColumnStatus::Backfilling
        );
    }
}
