//! Batch indexing of Parquet files into LakeSearch segments.
//!
//! The pipeline is parallelized at every stage: concurrent metadata loads,
//! pipelined I/O reads feeding rayon CPU tokenization, and parallel posting
//! list encoding in the segment builder.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use futures::stream::{self, StreamExt, TryStreamExt};
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

use lakesearch_core::cas::{commit_metadata, is_batch_duplicate};
use lakesearch_core::parquet_util::{
    build_page_inventory, load_parquet_metadata_async, read_parquet_batches_async, string_value,
    validate_column,
};
use lakesearch_core::storage::{
    compute_batch_id, read_current, read_metadata, write_manifest, write_manifest_list,
    write_segment,
};

/// Default number of concurrent I/O operations.
const DEFAULT_IO_CONCURRENCY: usize = 8;

/// Indexes Parquet files from object storage into a LakeSearch table.
pub async fn run_index(
    store: &Arc<dyn ObjectStore>,
    base: &Path,
    file_urls: &[String],
    column: &str,
    runtime: &LakeRuntime,
) -> Result<()> {
    run_index_with_concurrency(
        store,
        base,
        file_urls,
        column,
        runtime,
        DEFAULT_IO_CONCURRENCY,
    )
    .await
}

/// Indexes Parquet files with configurable I/O concurrency.
async fn run_index_with_concurrency(
    store: &Arc<dyn ObjectStore>,
    base: &Path,
    file_urls: &[String],
    column: &str,
    runtime: &LakeRuntime,
    io_concurrency: usize,
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

    // 3. Load Parquet metadata concurrently
    let file_metadata: Vec<(String, _)> = stream::iter(file_urls.iter().cloned())
        .map(|file_url| {
            let store = Arc::clone(store);
            async move {
                let metadata = load_parquet_metadata_async(&store, &file_url).await?;
                Ok::<_, anyhow::Error>((file_url, metadata))
            }
        })
        .buffered(io_concurrency)
        .try_collect()
        .await?;

    // Validate column index consistency across all files
    let mut parquet_col_idx: Option<usize> = None;
    for (file_url, metadata) in &file_metadata {
        let idx = validate_column(metadata, column)
            .with_context(|| format!("validating '{file_url}'"))?;
        if let Some(prev) = parquet_col_idx {
            anyhow::ensure!(
                prev == idx,
                "column '{column}' has index {idx} in '{file_url}' but {prev} in a previous file"
            );
        }
        parquet_col_idx = Some(idx);
    }

    let col_idx = parquet_col_idx.context("no files provided")?;

    // 4. Build page inventory (sync, pure)
    let inventory = build_page_inventory(&file_metadata, col_idx);
    info!(
        files = file_urls.len(),
        pages = inventory.doc_table.len(),
        "built page inventory"
    );

    // 5. Index all rows: pipelined async reads with CPU tokenization on rayon
    //
    // Flatten all (file_ordinal, rg_idx) pairs into a stream, read ahead with
    // buffered concurrency, then feed each completed read to rayon for
    // tokenization. Collect all results and merge once at the end.

    // Derive is_large from Parquet metadata's Arrow schema (no batch I/O needed)
    let is_large = {
        let meta = &file_metadata[0].1;
        let schema = parquet::arrow::parquet_to_arrow_schema(
            meta.file_metadata().schema_descr(),
            meta.file_metadata().key_value_metadata(),
        )
        .context("deriving Arrow schema from Parquet metadata")?;
        let field = schema
            .field_with_name(column)
            .with_context(|| format!("column '{column}' not in Arrow schema"))?;
        match field.data_type() {
            arrow::datatypes::DataType::Utf8 => false,
            arrow::datatypes::DataType::LargeUtf8 => true,
            dt => bail!("column '{column}' has type {dt:?}, expected Utf8 or LargeUtf8"),
        }
    };

    // Build list of (file_ordinal, rg_idx) work items
    let work_items: Vec<(usize, usize)> = file_metadata
        .iter()
        .enumerate()
        .flat_map(|(file_ord, (_, meta))| {
            (0..meta.num_row_groups()).map(move |rg_idx| (file_ord, rg_idx))
        })
        .collect();

    // Pipeline: buffered I/O reads feeding into rayon CPU work
    let rg_results: Vec<RgIndexResult> = stream::iter(work_items)
        .map(|(file_ordinal, rg_idx)| {
            let store = Arc::clone(store);
            let file_url = file_metadata[file_ordinal].0.clone();
            let rg_pages: Vec<(DocId, i64)> = inventory.pages[file_ordinal][rg_idx]
                .pages
                .iter()
                .map(|p| (p.doc_id, p.first_row_index))
                .collect();

            async move {
                let batches =
                    read_parquet_batches_async(&store, &file_url, rg_idx, &[col_idx], None).await?;
                Ok::<_, anyhow::Error>((batches, rg_pages, is_large))
            }
        })
        .buffered(io_concurrency)
        .and_then(|(batches, rg_pages, is_large)| async move {
            // CPU: tokenize on rayon
            let result = runtime
                .cpu(move || index_batches(&batches, &rg_pages, is_large))
                .await;
            Ok(result)
        })
        .try_collect()
        .await?;

    // Merge all per-RG results in a single pass
    let mut term_doc_ids: HashMap<String, Vec<DocId>> = HashMap::new();
    let mut term_doc_freq: HashMap<String, u32> = HashMap::new();
    let mut total_rows: u64 = 0;
    let mut total_tokens: u64 = 0;

    for rg_result in rg_results {
        for (term, doc_ids) in rg_result.term_doc_ids {
            term_doc_ids.entry(term).or_default().extend(doc_ids);
        }
        for (term, df) in rg_result.term_doc_freq {
            *term_doc_freq.entry(term).or_default() += df;
        }
        total_rows += rg_result.total_rows;
        total_tokens += rg_result.total_tokens;
    }

    // Deduplicate doc_ids per term (sort + dedup)
    for doc_ids in term_doc_ids.values_mut() {
        doc_ids.sort_unstable();
        doc_ids.dedup();
    }

    info!(
        terms = term_doc_ids.len(),
        total_rows, total_tokens, "indexing complete"
    );

    // 6. Build segment on rayon
    let doc_table = inventory.doc_table;
    let file_table = inventory.file_table;
    let built = runtime
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
    let segment_path = write_segment(store.as_ref(), base, built.bytes).await?;

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

    let manifest = Manifest {
        indexed_column: column.to_owned(),
        segments: vec![SegmentInfo {
            segment_path: segment_path.clone(),
            size_bytes: 0, // Could stat the object, not critical
            term_count: built.term_count,
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
                min_term: built.min_term,
                max_term: built.max_term,
                term_count: built.term_count,
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
    term_doc_ids: HashMap<String, Vec<DocId>>,
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
    let mut term_doc_ids: HashMap<String, Vec<DocId>> = HashMap::new();
    let mut term_doc_freq: HashMap<String, u32> = HashMap::new();
    let mut total_rows: u64 = 0;
    let mut total_tokens: u64 = 0;
    let mut row_idx: i64 = 0;
    // Sequential page pointer — rows arrive in order, so we advance
    // the pointer when row_idx crosses the next page boundary. O(N)
    // instead of O(N log P) from binary search.
    let mut cur_page: usize = 0;
    // Track which (term, doc_id) pairs have been recorded in this RG
    // to avoid pushing duplicate doc_ids. Many rows share a page's
    // doc_id, so without this a term in 1000 rows would push the
    // same doc_id 1000 times.
    let mut term_doc_seen: HashSet<(String, DocId)> = HashSet::new();

    for batch in batches {
        let col = batch.column(0);
        for row in 0..batch.num_rows() {
            total_rows += 1;

            // Advance page pointer if we've crossed into the next page
            while cur_page + 1 < pages.len() && pages[cur_page + 1].1 <= row_idx {
                cur_page += 1;
            }

            if col.is_null(row) {
                row_idx += 1;
                continue;
            }

            let text = string_value(col.as_ref(), row, is_large);
            let tokens = tokenize(text);
            total_tokens += tokens.len() as u64;

            let doc_id = pages[cur_page].0;

            let mut seen_in_row = HashSet::new();
            for token in &tokens {
                if term_doc_seen.insert((token.clone(), doc_id)) {
                    term_doc_ids.entry(token.clone()).or_default().push(doc_id);
                }
                if seen_in_row.insert(token.as_str()) {
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

/// Result of building a segment: the serialized bytes plus term stats
/// extracted during construction (avoids re-parsing the segment).
struct BuiltSegment {
    bytes: Vec<u8>,
    min_term: String,
    max_term: String,
    term_count: u64,
}

/// CPU-bound: build a segment from accumulated index data.
fn build_segment(
    mut term_doc_ids: HashMap<String, Vec<DocId>>,
    term_doc_freq: HashMap<String, u32>,
    doc_table: Vec<lakesearch_core::types::DocTableEntry>,
    file_table: Vec<lakesearch_core::types::FileTableEntry>,
    total_rows: u64,
    total_tokens: u64,
) -> Result<BuiltSegment> {
    let mut builder = SegmentBuilder::new();

    let mut sorted_terms: Vec<String> = term_doc_ids.keys().cloned().collect();
    sorted_terms.sort();

    let min_term = sorted_terms.first().cloned().unwrap_or_default();
    let max_term = sorted_terms.last().cloned().unwrap_or_default();
    let term_count = sorted_terms.len() as u64;

    for term in sorted_terms {
        let doc_ids = term_doc_ids.remove(&term).expect("term exists in keys");
        let df = term_doc_freq[&term];
        builder.add_term(&term, doc_ids, df);
    }

    builder.set_doc_table(doc_table);
    builder.set_file_table(file_table);
    builder.set_corpus_stats(CorpusStats {
        total_rows,
        total_tokens,
    });

    let bytes = builder.build().context("building segment")?;
    Ok(BuiltSegment {
        bytes,
        min_term,
        max_term,
        term_count,
    })
}
