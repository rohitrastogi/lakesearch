//! Shared test utilities for creating Parquet files with known content.
//!
//! Gated behind the `test-utils` feature so downstream crates can use it
//! in their dev-dependencies without pulling parquet/arrow into production.

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::path::Path;
use std::sync::Arc;

/// Create a Parquet file with deterministic content for testing.
///
/// Schema: `id` (Int32, non-null), `description` (Utf8, nullable).
/// `descriptions` cycle through the provided strings.
/// Page indices (offset_index) are always written.
pub fn write_test_parquet(
    path: &Path,
    num_rows: usize,
    page_size_rows: usize,
    descriptions: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("description", DataType::Utf8, true),
    ]));

    let file = std::fs::File::create(path)?;
    let props = WriterProperties::builder()
        .set_data_page_row_count_limit(page_size_rows)
        .set_max_row_group_size(num_rows)
        .set_dictionary_enabled(false)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let ids: Vec<i32> = (0..num_rows as i32).collect();
    let descs: Vec<Option<&str>> = (0..num_rows)
        .map(|i| {
            if descriptions.is_empty() {
                None
            } else {
                Some(descriptions[i % descriptions.len()])
            }
        })
        .collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(descs)) as ArrayRef,
        ],
    )?;

    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
