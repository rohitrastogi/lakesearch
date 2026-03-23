//! Shared test helpers for lakesearch-query integration tests.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use lakesearch_core::metadata::{ColumnStatus, CurrentPointer, IndexedColumn, Metadata, Snapshot};
use lakesearch_core::tokenizer::DEFAULT_TOKENIZER;
use lakesearch_query::storage::write_json;

/// Creates a test Parquet file and uploads it to the object store.
/// Returns the path where it was stored.
pub async fn upload_test_parquet(
    store: &dyn ObjectStore,
    path: &str,
    num_rows: usize,
    page_size_rows: usize,
    descriptions: &[&str],
) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("description", DataType::Utf8, true),
    ]));

    let ids: Vec<i32> = (0..num_rows as i32).collect();
    let descs: Vec<Option<&str>> = (0..num_rows)
        .map(|i| Some(descriptions[i % descriptions.len()]))
        .collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(descs)) as ArrayRef,
        ],
    )
    .unwrap();

    let mut buf = Vec::new();
    let props = WriterProperties::builder()
        .set_data_page_row_count_limit(page_size_rows)
        .set_max_row_group_size(num_rows)
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    store
        .put(&Path::from(path), PutPayload::from(Bytes::from(buf)))
        .await
        .unwrap();

    path.to_owned()
}

/// Creates initial table metadata in the object store.
pub async fn create_test_table(store: &dyn ObjectStore, base: &Path, columns: &[&str]) {
    let indexed_columns: Vec<IndexedColumn> = columns
        .iter()
        .map(|name| IndexedColumn {
            name: (*name).to_owned(),
            tokenizer: DEFAULT_TOKENIZER.to_owned(),
            status: ColumnStatus::Active,
        })
        .collect();

    let metadata = Metadata {
        format_version: 1,
        table_id: "test-table-id".to_owned(),
        table_name: "test".to_owned(),
        location: "mem://table/".to_owned(),
        indexed_columns,
        snapshot: Snapshot {
            timestamp_ms: 1000,
            manifest_lists: vec![],
        },
    };

    let meta_path = format!("{}/metadata/metadata-init.json", base);
    write_json(store, &Path::from(meta_path.as_str()), &metadata)
        .await
        .unwrap();

    let pointer = CurrentPointer {
        metadata_path: meta_path,
        updated_at: "2026-01-01T00:00:00Z".to_owned(),
    };
    write_json(
        store,
        &base.child("metadata").child("current.json"),
        &pointer,
    )
    .await
    .unwrap();
}
