use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs::File;

use anyhow::{Context, Result};
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaDataReader;
use tracing::info;

use lakesearch_core::segment::SegmentBuilder;
use lakesearch_core::tokenizer::tokenize;
use lakesearch_core::types::{CorpusStats, DocId};

use crate::parquet_util::{
    build_page_inventory, string_value, validate_arrow_column, validate_column,
};

/// Indexes the given Parquet files and writes a segment to `output`.
pub fn run_index(files: &[String], column: &str, output: &str) -> Result<()> {
    // 1. Load metadata and validate
    let mut file_metadata = Vec::new();
    let mut parquet_col_idx: Option<usize> = None;

    for path in files {
        let f = File::open(path).with_context(|| format!("opening '{path}'"))?;
        let metadata = ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .parse_and_finish(&f)
            .with_context(|| format!("reading metadata from '{path}'"))?;

        let idx =
            validate_column(&metadata, column).with_context(|| format!("validating '{path}'"))?;

        // Validate arrow type upfront (before any indexing work begins)
        let f = File::open(path)?;
        let options = ArrowReaderOptions::new().with_page_index(true);
        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(f, options)?;
        validate_arrow_column(builder.schema(), column)
            .with_context(|| format!("column type check for '{path}'"))?;

        if let Some(prev) = parquet_col_idx {
            anyhow::ensure!(
                prev == idx,
                "column '{column}' has index {idx} in '{path}' but {prev} in a previous file"
            );
        }
        parquet_col_idx = Some(idx);
        file_metadata.push((path.clone(), metadata));
    }

    let col_idx = parquet_col_idx.context("no files provided")?;

    // 2. Build page inventory
    let inventory = build_page_inventory(&file_metadata, col_idx);
    info!(
        files = files.len(),
        pages = inventory.doc_table.len(),
        "built page inventory"
    );

    // 3. Index all rows
    let mut term_doc_ids: HashMap<String, BTreeSet<DocId>> = HashMap::new();
    let mut term_doc_freq: HashMap<String, u32> = HashMap::new();
    let mut total_rows: u64 = 0;
    let mut total_tokens: u64 = 0;

    for (file_ordinal, (path, metadata)) in file_metadata.iter().enumerate() {
        for rg_idx in 0..metadata.num_row_groups() {
            let f = File::open(path)?;
            let options = ArrowReaderOptions::new().with_page_index(true);
            let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(f, options)?;

            // Type was validated upfront; just check if LargeUtf8
            let is_large = builder
                .schema()
                .field_with_name(column)
                .expect("validated upfront")
                .data_type()
                == &DataType::LargeUtf8;

            let mask = ProjectionMask::leaves(builder.parquet_schema(), [col_idx]);
            let reader = builder
                .with_row_groups(vec![rg_idx])
                .with_projection(mask)
                .build()?;

            let rg_pages = &inventory.pages[file_ordinal][rg_idx];
            let mut row_idx: i64 = 0;

            for batch in reader {
                let batch = batch?;
                let col = batch.column(0);

                for row in 0..batch.num_rows() {
                    total_rows += 1;

                    if col.is_null(row) {
                        row_idx += 1;
                        continue;
                    }

                    let text = string_value(col.as_ref(), row, is_large);
                    let tokens = tokenize(text);
                    total_tokens += tokens.len() as u64;

                    let doc_id = rg_pages.doc_id_for_row(row_idx);

                    let mut seen = HashSet::new();
                    for token in &tokens {
                        term_doc_ids
                            .entry(token.clone())
                            .or_default()
                            .insert(doc_id);
                        if seen.insert(token.as_str()) {
                            *term_doc_freq.entry(token.clone()).or_default() += 1;
                        }
                    }

                    row_idx += 1;
                }
            }
        }
    }

    info!(
        terms = term_doc_ids.len(),
        total_rows, total_tokens, "indexing complete"
    );

    // 4. Build segment
    let mut builder = SegmentBuilder::new();

    let mut sorted_terms: Vec<String> = term_doc_ids.keys().cloned().collect();
    sorted_terms.sort();

    for term in sorted_terms {
        let doc_ids: Vec<DocId> = term_doc_ids.remove(&term).unwrap().into_iter().collect();
        let df = term_doc_freq[&term];
        builder.add_term(&term, doc_ids, df);
    }

    builder.set_doc_table(inventory.doc_table);
    builder.set_file_table(inventory.file_table);
    builder.set_corpus_stats(CorpusStats {
        total_rows,
        total_tokens,
    });

    let segment_bytes = builder.build().context("building segment")?;

    std::fs::write(output, &segment_bytes)
        .with_context(|| format!("writing segment to '{output}'"))?;

    info!(segment_size = segment_bytes.len(), output, "wrote segment");

    Ok(())
}
