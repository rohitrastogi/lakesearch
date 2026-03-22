use std::sync::Arc;

use anyhow::{bail, Context, Result};
use arrow::array::{Array, LargeStringArray, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use futures::TryStreamExt;
use lakesearch_core::types::{DocId, DocTableEntry, FileTableEntry};
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::ParquetMetaData;

/// Information about a single page for row-to-doc_id mapping.
pub struct PageEntry {
    pub doc_id: DocId,
    pub first_row_index: i64,
}

/// Per-row-group page information.
pub struct RowGroupPages {
    pub pages: Vec<PageEntry>,
}

impl RowGroupPages {
    /// Returns the doc_id of the page containing `row_index` (0-based within
    /// the row group). Uses binary search on first_row_index boundaries.
    pub fn doc_id_for_row(&self, row_index: i64) -> DocId {
        let idx = self
            .pages
            .partition_point(|p| p.first_row_index <= row_index)
            .saturating_sub(1);
        self.pages[idx].doc_id
    }
}

/// Complete page inventory for all indexed files.
pub struct PageInventory {
    pub doc_table: Vec<DocTableEntry>,
    pub file_table: Vec<FileTableEntry>,
    /// `pages[file_ordinal][rg_idx]` — page entries for row-to-doc_id mapping.
    pub pages: Vec<Vec<RowGroupPages>>,
}

/// Validates that the Parquet file has offset_index and the target column
/// exists. Returns the leaf column index in the parquet schema.
pub fn validate_column(metadata: &ParquetMetaData, column: &str) -> Result<usize> {
    if metadata.offset_index().is_none() {
        bail!(
            "Parquet file lacks offset_index (page locations). \
             Rewrite with page_index enabled."
        );
    }

    let schema_descr = metadata.file_metadata().schema_descr();
    schema_descr
        .columns()
        .iter()
        .position(|c| c.name() == column)
        .with_context(|| {
            let names: Vec<&str> = schema_descr.columns().iter().map(|c| c.name()).collect();
            format!("column '{column}' not found. Available: {names:?}")
        })
}

/// Validates that the arrow schema column is Utf8 or LargeUtf8.
/// Returns `true` if the column is LargeUtf8.
pub fn validate_arrow_column(schema: &arrow::datatypes::Schema, column: &str) -> Result<bool> {
    let field = schema
        .field_with_name(column)
        .with_context(|| format!("column '{column}' not found in arrow schema"))?;

    match field.data_type() {
        DataType::Utf8 => Ok(false),
        DataType::LargeUtf8 => Ok(true),
        dt => bail!("column '{column}' has type {dt:?}, expected Utf8 or LargeUtf8"),
    }
}

/// Builds the page inventory from Parquet metadata for all files.
pub fn build_page_inventory(files: &[(String, ParquetMetaData)], col_idx: usize) -> PageInventory {
    let mut doc_table = Vec::new();
    let mut file_table = Vec::new();
    let mut pages_by_file = Vec::new();
    let mut next_doc_id: DocId = 0;

    for (file_ordinal, (path, metadata)) in files.iter().enumerate() {
        let offset_index = metadata
            .offset_index()
            .expect("validated: offset_index exists");
        let num_rgs = metadata.num_row_groups();

        file_table.push(FileTableEntry {
            path: path.clone(),
            row_group_count: num_rgs as u16,
        });

        let mut rg_pages_all = Vec::with_capacity(num_rgs);

        for (rg_idx, rg_offset_index) in offset_index.iter().enumerate().take(num_rgs) {
            let rg = metadata.row_group(rg_idx);
            let page_locations = rg_offset_index[col_idx].page_locations();
            let total_rows = rg.num_rows();

            let mut rg_page_entries = Vec::with_capacity(page_locations.len());

            for (page_idx, loc) in page_locations.iter().enumerate() {
                let first_row = loc.first_row_index;
                let row_count = if page_idx + 1 < page_locations.len() {
                    (page_locations[page_idx + 1].first_row_index - first_row) as u32
                } else {
                    (total_rows - first_row) as u32
                };

                let doc_id = next_doc_id;
                next_doc_id += 1;

                doc_table.push(DocTableEntry {
                    file_ordinal: file_ordinal as u32,
                    row_group: rg_idx as u16,
                    page_index: page_idx as u16,
                    first_row_index: first_row as u64,
                    row_count,
                });

                rg_page_entries.push(PageEntry {
                    doc_id,
                    first_row_index: first_row,
                });
            }

            rg_pages_all.push(RowGroupPages {
                pages: rg_page_entries,
            });
        }

        pages_by_file.push(rg_pages_all);
    }

    PageInventory {
        doc_table,
        file_table,
        pages: pages_by_file,
    }
}

/// Builds a `RowSelection` from doc table entries for pages within a single
/// row group. `entries` must be sorted by `first_row_index`.
pub fn build_row_selection(entries: &[&DocTableEntry], total_rg_rows: i64) -> RowSelection {
    let mut selectors = Vec::new();
    let mut prev_end: u64 = 0;

    for entry in entries {
        let start = entry.first_row_index;
        let end = start + entry.row_count as u64;

        if start > prev_end {
            selectors.push(RowSelector::skip((start - prev_end) as usize));
        }
        selectors.push(RowSelector::select(entry.row_count as usize));
        prev_end = end;
    }

    let total = total_rg_rows as u64;
    if prev_end < total {
        selectors.push(RowSelector::skip((total - prev_end) as usize));
    }

    RowSelection::from(selectors)
}

/// Extracts a string value from a Utf8 or LargeUtf8 column array.
pub fn string_value(col: &dyn Array, row: usize, is_large: bool) -> &str {
    if is_large {
        col.as_any()
            .downcast_ref::<LargeStringArray>()
            .expect("expected LargeUtf8 column")
            .value(row)
    } else {
        col.as_any()
            .downcast_ref::<StringArray>()
            .expect("expected Utf8 column")
            .value(row)
    }
}

// --- Async helpers for object storage Parquet access ---

/// Loads Parquet metadata (with page indexes) from object storage.
pub async fn load_parquet_metadata_async(
    store: &Arc<dyn ObjectStore>,
    path: &str,
) -> Result<ParquetMetaData> {
    let location = Path::from(path);
    let meta = store
        .head(&location)
        .await
        .with_context(|| format!("HEAD for '{path}'"))?;
    let reader = ParquetObjectReader::new(Arc::clone(store), meta);
    let options = parquet::arrow::arrow_reader::ArrowReaderOptions::new().with_page_index(true);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
        .await
        .with_context(|| format!("reading Parquet metadata from '{path}'"))?;
    Ok(builder.metadata().as_ref().clone())
}

/// Reads record batches from a specific row group of a Parquet file in object
/// storage, with column projection and optional row selection.
///
/// `leaf_indices` are parquet leaf column indices to project.
pub async fn read_parquet_batches_async(
    store: &Arc<dyn ObjectStore>,
    path: &str,
    rg_idx: usize,
    leaf_indices: &[usize],
    selection: Option<RowSelection>,
) -> Result<Vec<RecordBatch>> {
    let location = Path::from(path);
    let meta = store
        .head(&location)
        .await
        .with_context(|| format!("HEAD for '{path}'"))?;
    let reader = ParquetObjectReader::new(Arc::clone(store), meta);
    let mut builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .with_context(|| format!("opening Parquet reader for '{path}'"))?;

    let mask = ProjectionMask::leaves(builder.parquet_schema(), leaf_indices.iter().copied());
    builder = builder.with_row_groups(vec![rg_idx]).with_projection(mask);

    if let Some(sel) = selection {
        builder = builder.with_row_selection(sel);
    }

    let stream = builder
        .build()
        .with_context(|| format!("building Parquet stream for '{path}' rg {rg_idx}"))?;

    stream
        .try_collect()
        .await
        .with_context(|| format!("reading batches from '{path}' rg {rg_idx}"))
}

/// Extracts a value from an arrow array at the given row as a JSON value.
pub fn arrow_value_to_json(col: &dyn Array, row: usize) -> serde_json::Value {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if col.is_null(row) {
        return serde_json::Value::Null;
    }

    match col.data_type() {
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            serde_json::Value::Bool(arr.value(row))
        }
        DataType::Int8 => json_num(col.as_any().downcast_ref::<Int8Array>().unwrap().value(row)),
        DataType::Int16 => json_num(
            col.as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row),
        ),
        DataType::Int32 => json_num(
            col.as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row),
        ),
        DataType::Int64 => json_num(
            col.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
        ),
        DataType::UInt8 => json_num(
            col.as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(row),
        ),
        DataType::UInt16 => json_num(
            col.as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(row),
        ),
        DataType::UInt32 => json_num(
            col.as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(row),
        ),
        DataType::UInt64 => json_num(
            col.as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row),
        ),
        DataType::Float32 => {
            let v = col
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row);
            serde_json::Value::from(v)
        }
        DataType::Float64 => {
            let v = col
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row);
            serde_json::Value::from(v)
        }
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
            serde_json::Value::String(arr.value(row).to_owned())
        }
        DataType::LargeUtf8 => {
            let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
            serde_json::Value::String(arr.value(row).to_owned())
        }
        // Fallback: use debug format
        _ => serde_json::Value::String(format!("{col:?}")),
    }
}

fn json_num<T: Into<serde_json::Number>>(v: T) -> serde_json::Value {
    serde_json::Value::Number(v.into())
}
