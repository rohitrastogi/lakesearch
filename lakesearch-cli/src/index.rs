//! Async index command for object storage.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use tracing::info;

use lakesearch_core::metadata::{
    ColumnStatus, DataFileEntry, JobKind, Manifest, ManifestEntry, ManifestList, ParquetFileRef,
    SegmentInfo, TermStats,
};
use lakesearch_core::runtime::LakeRuntime;
use lakesearch_core::segment::SegmentBuilder;
use lakesearch_core::tokenizer::tokenize;
use lakesearch_core::types::{CorpusStats, DocId};
use object_store::path::Path;
use object_store::ObjectStore;

use lakesearch_query::cas::{commit_metadata, is_batch_duplicate};
use lakesearch_query::parquet_util::{
    build_page_inventory, load_parquet_metadata_async, read_parquet_batches_async, string_value,
    validate_arrow_column, validate_column,
};
use lakesearch_query::storage::{
    compute_batch_id, read_current, read_metadata, write_manifest, write_manifest_list,
    write_segment,
};

/// Indexes Parquet files from object storage into a LakeSearch table.
pub async fn run_index(
    store: &Arc<dyn ObjectStore>,
    base: &Path,
    file_urls: &[String],
    column: &str,
    runtime: &LakeRuntime,
) -> Result<()> {
    // 1. Read current metadata
    let current_result = read_current(store.as_ref(), base).await?;
    let current_meta = read_metadata(store.as_ref(), &current_result.value).await?;

    // Validate column is active
    let col_config = current_meta
        .indexed_columns
        .iter()
        .find(|c| c.name == column)
        .with_context(|| format!("column '{column}' not in table's indexed_columns"))?;
    if col_config.status == ColumnStatus::Dropped {
        bail!("column '{column}' is dropped");
    }

    // 2. Compute batch_id and check for duplicates
    let path_refs: Vec<&str> = file_urls.iter().map(|s| s.as_str()).collect();
    let batch_id = compute_batch_id(&path_refs);
    if is_batch_duplicate(store.as_ref(), &current_meta, &batch_id).await? {
        info!(batch_id = %batch_id, "batch already indexed, skipping");
        return Ok(());
    }

    // 3. Load Parquet metadata (concurrent)
    let mut file_metadata = Vec::new();
    let mut parquet_col_idx: Option<usize> = None;

    for file_url in file_urls {
        let metadata = load_parquet_metadata_async(store, file_url).await?;
        let idx = validate_column(&metadata, column)
            .with_context(|| format!("validating '{file_url}'"))?;

        if let Some(prev) = parquet_col_idx {
            anyhow::ensure!(
                prev == idx,
                "column '{column}' has index {idx} in '{file_url}' but {prev} in a previous file"
            );
        }
        parquet_col_idx = Some(idx);
        file_metadata.push((file_url.clone(), metadata));
    }

    let col_idx = parquet_col_idx.context("no files provided")?;

    // 4. Build page inventory (sync, pure)
    let inventory = build_page_inventory(&file_metadata, col_idx);
    info!(
        files = file_urls.len(),
        pages = inventory.doc_table.len(),
        "built page inventory"
    );

    // 5. Index all rows: async read batches, CPU tokenize on rayon
    let mut term_doc_ids: HashMap<String, BTreeSet<DocId>> = HashMap::new();
    let mut term_doc_freq: HashMap<String, u32> = HashMap::new();
    let mut total_rows: u64 = 0;
    let mut total_tokens: u64 = 0;
    let mut is_large = false;

    for (file_ordinal, (file_url, metadata)) in file_metadata.iter().enumerate() {
        for rg_idx in 0..metadata.num_row_groups() {
            // Async: read all batches from this row group
            let batches =
                read_parquet_batches_async(store, file_url, rg_idx, &[col_idx], None).await?;

            // Check LargeUtf8 on first batch
            if file_ordinal == 0 && rg_idx == 0 {
                if let Some(first_batch) = batches.first() {
                    is_large = first_batch.schema().field(0).data_type()
                        == &arrow::datatypes::DataType::LargeUtf8;
                    validate_arrow_column(first_batch.schema().as_ref(), column)
                        .with_context(|| format!("column type check for '{file_url}'"))?;
                }
            }

            let rg_pages = &inventory.pages[file_ordinal][rg_idx];

            // CPU: tokenize rows on rayon
            // We need to process batches and accumulate into our maps.
            // Since the maps are not Send-friendly across the rayon boundary,
            // collect per-rg results and merge.
            let pages_clone: Vec<(DocId, i64)> = rg_pages
                .pages
                .iter()
                .map(|p| (p.doc_id, p.first_row_index))
                .collect();
            let is_large_copy = is_large;

            let rg_result = runtime
                .cpu(move || index_batches(&batches, &pages_clone, is_large_copy))
                .await;

            // Merge per-rg results
            for (term, doc_ids) in rg_result.term_doc_ids {
                term_doc_ids.entry(term).or_default().extend(doc_ids);
            }
            for (term, df) in rg_result.term_doc_freq {
                *term_doc_freq.entry(term).or_default() += df;
            }
            total_rows += rg_result.total_rows;
            total_tokens += rg_result.total_tokens;
        }
    }

    info!(
        terms = term_doc_ids.len(),
        total_rows, total_tokens, "indexing complete"
    );

    // 6. Build segment on rayon
    let doc_table = inventory.doc_table;
    let file_table = inventory.file_table;
    let segment_bytes = runtime
        .cpu(move || {
            build_segment(
                term_doc_ids,
                term_doc_freq,
                doc_table,
                file_table,
                total_rows,
                total_tokens,
            )
        })
        .await?;

    // 7. Upload artifacts
    let segment_bytes_for_upload = segment_bytes.clone();
    let segment_path = write_segment(store.as_ref(), base, segment_bytes_for_upload).await?;

    let parquet_file_refs: Vec<ParquetFileRef> = file_metadata
        .iter()
        .enumerate()
        .map(|(i, (path, meta))| ParquetFileRef {
            file_ordinal: i as u32,
            path: path.clone(),
            file_size_bytes: 0,
            row_group_count: meta.num_row_groups() as u16,
        })
        .collect();

    let (min_term, max_term, term_count) = {
        let reader = lakesearch_core::segment::SegmentReader::open(segment_bytes)
            .context("reading built segment for stats")?;
        let all_terms = reader.prefix_terms("");
        if all_terms.is_empty() {
            (String::new(), String::new(), 0u64)
        } else {
            (
                all_terms.first().unwrap().0.clone(),
                all_terms.last().unwrap().0.clone(),
                all_terms.len() as u64,
            )
        }
    };

    let manifest = Manifest {
        indexed_column: column.to_owned(),
        segments: vec![SegmentInfo {
            segment_path: segment_path.clone(),
            size_bytes: 0, // Could stat the object, not critical
            term_count,
            doc_count: inventory
                .pages
                .iter()
                .map(|f| f.iter().map(|rg| rg.pages.len() as u64).sum::<u64>())
                .sum(),
            total_rows,
            total_tokens,
            parquet_files: parquet_file_refs,
        }],
    };

    let manifest_path = write_manifest(store.as_ref(), base, &manifest).await?;

    let data_files: Vec<DataFileEntry> = file_metadata
        .iter()
        .map(|(path, meta)| DataFileEntry {
            path: path.clone(),
            file_size_bytes: 0,
            row_count: meta.file_metadata().num_rows() as u64,
        })
        .collect();

    let manifest_list = ManifestList {
        job_kind: JobKind::Append,
        batch_id: batch_id.clone(),
        data_files,
        manifests: vec![ManifestEntry {
            manifest_path,
            indexed_column: column.to_owned(),
            segment_count: 1,
            term_stats: TermStats {
                min_term,
                max_term,
                term_count,
            },
        }],
        replaces: None,
        compacted_column: None,
    };

    let manifest_list_path = write_manifest_list(store.as_ref(), base, &manifest_list).await?;

    info!(
        segment = %segment_path,
        manifest_list = %manifest_list_path,
        "artifacts uploaded"
    );

    // 8. CAS commit metadata
    let ml_path = manifest_list_path;
    commit_metadata(
        store.as_ref(),
        base,
        current_result.e_tag,
        &current_meta,
        Some(&batch_id),
        |meta| {
            let mut new = meta.clone();
            new.snapshot.manifest_lists.push(ml_path.clone());
            new.snapshot.timestamp_ms = chrono::Utc::now().timestamp_millis() as u64;
            new
        },
    )
    .await?;

    info!("metadata committed");
    Ok(())
}

/// Per-row-group indexing result, returned from rayon CPU work.
struct RgIndexResult {
    term_doc_ids: HashMap<String, BTreeSet<DocId>>,
    term_doc_freq: HashMap<String, u32>,
    total_rows: u64,
    total_tokens: u64,
}

/// CPU-bound: tokenize all rows in the given batches and accumulate index data.
fn index_batches(
    batches: &[arrow::array::RecordBatch],
    pages: &[(DocId, i64)], // (doc_id, first_row_index)
    is_large: bool,
) -> RgIndexResult {
    let mut term_doc_ids: HashMap<String, BTreeSet<DocId>> = HashMap::new();
    let mut term_doc_freq: HashMap<String, u32> = HashMap::new();
    let mut total_rows: u64 = 0;
    let mut total_tokens: u64 = 0;
    let mut row_idx: i64 = 0;

    for batch in batches {
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

            // Binary search for the page containing this row
            let page_idx = pages
                .partition_point(|&(_, fri)| fri <= row_idx)
                .saturating_sub(1);
            let doc_id = pages[page_idx].0;

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

    RgIndexResult {
        term_doc_ids,
        term_doc_freq,
        total_rows,
        total_tokens,
    }
}

/// CPU-bound: build a segment from accumulated index data.
fn build_segment(
    mut term_doc_ids: HashMap<String, BTreeSet<DocId>>,
    term_doc_freq: HashMap<String, u32>,
    doc_table: Vec<lakesearch_core::types::DocTableEntry>,
    file_table: Vec<lakesearch_core::types::FileTableEntry>,
    total_rows: u64,
    total_tokens: u64,
) -> Result<Vec<u8>> {
    let mut builder = SegmentBuilder::new();

    let mut sorted_terms: Vec<String> = term_doc_ids.keys().cloned().collect();
    sorted_terms.sort();

    for term in sorted_terms {
        let doc_ids: Vec<DocId> = term_doc_ids.remove(&term).unwrap().into_iter().collect();
        let df = term_doc_freq[&term];
        builder.add_term(&term, doc_ids, df);
    }

    builder.set_doc_table(doc_table);
    builder.set_file_table(file_table);
    builder.set_corpus_stats(CorpusStats {
        total_rows,
        total_tokens,
    });

    builder.build().context("building segment")
}
