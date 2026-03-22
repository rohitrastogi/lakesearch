use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{
    ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::Encoding;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use std::fs::File;
use std::sync::Arc;

fn main() {
    // Test 1: Dictionary-encoded columns with RowSelection
    println!("################################################################");
    println!("# TEST 1: RowSelection with DICTIONARY-ENCODED columns        #");
    println!("################################################################\n");
    test_row_selection_with_dict("/tmp/spike_dict.parquet");

    // Test 2: Plain-encoded columns with RowSelection (baseline)
    println!("\n################################################################");
    println!("# TEST 2: RowSelection with PLAIN-ENCODED columns             #");
    println!("################################################################\n");
    test_row_selection_with_plain("/tmp/spike_plain.parquet");

    // Test 3: Non-contiguous page reads (simulating index hits on scattered pages)
    println!("\n################################################################");
    println!("# TEST 3: Non-contiguous RowSelection (scattered page hits)   #");
    println!("################################################################\n");
    test_non_contiguous_selection("/tmp/spike_scattered.parquet");

    // Test 4: Column projection + RowSelection (read only specific columns)
    println!("\n################################################################");
    println!("# TEST 4: Column projection + RowSelection                    #");
    println!("################################################################\n");
    test_projection_with_selection("/tmp/spike_projection.parquet");

    // Test 5: Mixed encodings - some columns dict, some plain
    println!("\n################################################################");
    println!("# TEST 5: Mixed encodings (dict + plain) with RowSelection    #");
    println!("################################################################\n");
    test_mixed_encodings("/tmp/spike_mixed.parquet");
}

fn write_parquet(path: &str, props_fn: fn(WriterPropertiesBuilder) -> WriterPropertiesBuilder) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("message", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("severity", DataType::Utf8, false),
    ]));

    let file = File::create(path).unwrap();

    // Use row-count limit to force many pages even with dictionary encoding
    // (byte-size limits don't work well with dict because encoded values are tiny)
    let base_props = WriterProperties::builder()
        .set_data_page_row_count_limit(25) // 25 rows per page
        .set_max_row_group_size(500) // big row groups so we get many pages
        .set_write_batch_size(10)
        .set_column_index_truncate_length(Some(64));

    let props = props_fn(base_props).build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    // Generate unique-ish messages so dict encoding still applies but data is realistic
    let base_messages = vec![
        "error timeout connection refused to upstream service",
        "success response ok from api gateway",
        "error connection reset by peer during handshake",
        "warning slow query detected on users table",
        "error timeout waiting for response from database",
        "info request completed successfully in 42ms",
        "error ECONNREFUSED on port 443 https endpoint",
        "debug processing batch upload of 1000 records",
        "error disk space low on volume /data/primary",
        "info heartbeat check passed all health endpoints",
    ];
    let categories: Vec<&str> = vec![
        "network", "http", "network", "database", "network",
        "http", "network", "batch", "storage", "health",
    ];
    let severities: Vec<&str> = vec![
        "error", "info", "error", "warn", "error",
        "info", "error", "debug", "error", "info",
    ];

    for batch_num in 0..5 {
        let size = 100;
        let ids: Vec<i32> = (0..size).map(|i| (batch_num * size + i) as i32).collect();
        // Make messages unique per row to stress dict encoding
        let msgs: Vec<String> = (0..size)
            .map(|i| format!("{} req_id={}", base_messages[i % base_messages.len()], batch_num * size + i))
            .collect();
        let msg_refs: Vec<&str> = msgs.iter().map(|s| s.as_str()).collect();
        let cats: Vec<&str> = (0..size).map(|i| categories[i % categories.len()]).collect();
        let sevs: Vec<&str> = (0..size).map(|i| severities[i % severities.len()]).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(msg_refs)) as ArrayRef,
                Arc::new(StringArray::from(cats)) as ArrayRef,
                Arc::new(StringArray::from(sevs)) as ArrayRef,
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
    }

    writer.close().unwrap();
}

fn print_page_structure(path: &str) {
    let file = File::open(path).unwrap();
    let metadata = ParquetMetaDataReader::new()
        .with_page_indexes(true)
        .parse_and_finish(&file)
        .unwrap();

    let file_meta = metadata.file_metadata();
    println!("  {} rows, {} row groups", file_meta.num_rows(), metadata.num_row_groups());

    for rg_idx in 0..metadata.num_row_groups() {
        let rg = metadata.row_group(rg_idx);
        println!("  Row group {}: {} rows", rg_idx, rg.num_rows());

        for col_idx in 0..rg.num_columns() {
            let col = rg.column(col_idx);
            let col_name = file_meta.schema().get_fields()[col_idx].name();
            let encoding_strs: Vec<String> = col.encodings().iter().map(|e| format!("{:?}", e)).collect();
            let has_dict = col.dictionary_page_offset().is_some();
            println!(
                "    Col '{}': encodings={:?}, has_dict_page={}, dict_page_offset={:?}",
                col_name,
                encoding_strs,
                has_dict,
                col.dictionary_page_offset(),
            );
        }
    }

    if let Some(offset_idx) = metadata.offset_index() {
        for (rg_idx, rg_oi) in offset_idx.iter().enumerate() {
            let rg = metadata.row_group(rg_idx);
            for (col_idx, col_oi) in rg_oi.iter().enumerate() {
                let col_name = file_meta.schema().get_fields()[col_idx].name();
                let pages = col_oi.page_locations();
                let ranges: Vec<String> = pages.iter().enumerate().map(|(i, loc)| {
                    let end = if i + 1 < pages.len() {
                        pages[i + 1].first_row_index
                    } else {
                        rg.num_rows()
                    };
                    format!("[{},{})", loc.first_row_index, end)
                }).collect();
                if rg_idx == 0 {
                    println!("    Col '{}' rg{}: {} pages, rows: {}", col_name, rg_idx, pages.len(), ranges.join(" "));
                }
            }
        }
    }
}

fn build_selection_for_pages(
    path: &str,
    target_col_idx: usize,
    target_rg_idx: usize,
    target_page_indices: &[usize],
) -> (RowSelection, i64) {
    let file = File::open(path).unwrap();
    let metadata = ParquetMetaDataReader::new()
        .with_page_indexes(true)
        .parse_and_finish(&file)
        .unwrap();

    let offset_idx = metadata.offset_index().unwrap();
    let rg = metadata.row_group(target_rg_idx);
    let pages = offset_idx[target_rg_idx][target_col_idx].page_locations();

    let mut selectors = Vec::new();
    let mut prev_end: i64 = 0;
    let total_rows = rg.num_rows();

    for &page_idx in target_page_indices {
        let page_start = pages[page_idx].first_row_index;
        let page_end = if page_idx + 1 < pages.len() {
            pages[page_idx + 1].first_row_index
        } else {
            total_rows
        };

        // Skip rows before this page
        if page_start > prev_end {
            selectors.push(RowSelector::skip((page_start - prev_end) as usize));
        }
        // Select rows in this page
        selectors.push(RowSelector::select((page_end - page_start) as usize));
        prev_end = page_end;
    }

    // Skip remaining rows after last selected page
    if prev_end < total_rows {
        selectors.push(RowSelector::skip((total_rows - prev_end) as usize));
    }

    let sel = RowSelection::from(selectors);
    (sel, total_rows)
}

fn test_row_selection_with_dict(path: &str) {
    // Write with dictionary encoding (the default for string columns)
    write_parquet(path, |builder| {
        builder
            .set_dictionary_enabled(true) // explicit: dictionary encoding on
    });

    println!("  Written with DICTIONARY encoding.");
    print_page_structure(path);

    // Now try to read specific pages using RowSelection
    println!("\n  Attempting RowSelection on pages [0, 2] of 'message' column (rg 0)...");
    let (selection, _total) = build_selection_for_pages(path, 1, 0, &[0, 2]);
    println!("  Selection: {:?}", selection);

    let file = File::open(path).unwrap();
    let options = ArrowReaderOptions::new().with_page_index(true);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).unwrap();

    let reader = builder
        .with_row_selection(selection)
        .with_batch_size(1024)
        .build();

    match reader {
        Ok(reader) => {
            let mut total_rows = 0;
            for (batch_idx, batch) in reader.enumerate() {
                match batch {
                    Ok(batch) => {
                        total_rows += batch.num_rows();
                        let msg_col = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                        let id_col = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                        println!(
                            "    Batch {}: {} rows, ids [{}, {}], first msg: {:?}",
                            batch_idx,
                            batch.num_rows(),
                            id_col.value(0),
                            id_col.value(batch.num_rows() - 1),
                            &msg_col.value(0)[..50.min(msg_col.value(0).len())],
                        );
                    }
                    Err(e) => {
                        println!("    ERROR reading batch {}: {}", batch_idx, e);
                        return;
                    }
                }
            }
            println!("  SUCCESS: Read {} total rows with dictionary encoding + RowSelection", total_rows);
        }
        Err(e) => {
            println!("  FAILED to build reader: {}", e);
        }
    }
}

fn test_row_selection_with_plain(path: &str) {
    write_parquet(path, |builder| {
        builder
            .set_dictionary_enabled(false)
            .set_encoding(Encoding::PLAIN)
    });

    println!("  Written with PLAIN encoding.");
    print_page_structure(path);

    println!("\n  Attempting RowSelection on pages [0, 2] of 'message' column (rg 0)...");
    let (selection, _total) = build_selection_for_pages(path, 1, 0, &[0, 2]);
    println!("  Selection: {:?}", selection);

    let file = File::open(path).unwrap();
    let options = ArrowReaderOptions::new().with_page_index(true);
    let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)
        .unwrap()
        .with_row_selection(selection)
        .with_batch_size(1024)
        .build()
        .unwrap();

    let mut total_rows = 0;
    for batch in reader {
        let batch = batch.unwrap();
        total_rows += batch.num_rows();
    }
    println!("  SUCCESS: Read {} total rows with plain encoding + RowSelection", total_rows);
}

fn test_non_contiguous_selection(path: &str) {
    write_parquet(path, |builder| {
        builder.set_dictionary_enabled(true)
    });

    println!("  Written with DICTIONARY encoding.");
    print_page_structure(path);

    // Select pages 0, 3, 5 from message column (non-contiguous, with gaps)
    let file = File::open(path).unwrap();
    let metadata = ParquetMetaDataReader::new()
        .with_page_indexes(true)
        .parse_and_finish(&file)
        .unwrap();

    let offset_idx = metadata.offset_index().unwrap();
    let msg_pages = offset_idx[0][1].page_locations();
    let num_pages = msg_pages.len();
    println!("\n  'message' col rg0 has {} pages", num_pages);

    // Pick non-contiguous pages: first, middle, last
    let target_pages: Vec<usize> = if num_pages >= 3 {
        vec![0, num_pages / 2, num_pages - 1]
    } else {
        vec![0]
    };
    println!("  Selecting pages: {:?}", target_pages);

    let (selection, _total) = build_selection_for_pages(path, 1, 0, &target_pages);
    println!("  Selection: {:?}", selection);

    let file2 = File::open(path).unwrap();
    let options = ArrowReaderOptions::new().with_page_index(true);
    let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(file2, options)
        .unwrap()
        .with_row_selection(selection)
        .with_batch_size(1024)
        .build()
        .unwrap();

    let mut total_rows = 0;
    for (i, batch) in reader.enumerate() {
        let batch = batch.unwrap();
        total_rows += batch.num_rows();
        let id_col = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let msg_col = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        println!(
            "    Batch {}: {} rows, ids [{}, {}]",
            i, batch.num_rows(), id_col.value(0), id_col.value(batch.num_rows() - 1),
        );
        // Verify data integrity: check a few values make sense
        for row in 0..3.min(batch.num_rows()) {
            let id = id_col.value(row);
            let msg = msg_col.value(row);
            println!("      row {}: id={}, msg={:?}", row, id, &msg[..40.min(msg.len())]);
        }
    }
    println!("  SUCCESS: Read {} rows from non-contiguous pages with dict encoding", total_rows);
}

fn test_projection_with_selection(path: &str) {
    write_parquet(path, |builder| {
        builder.set_dictionary_enabled(true)
    });

    println!("  Written with DICTIONARY encoding.");

    // Read only 'id' and 'message' columns (skip 'category' and 'severity')
    // combined with RowSelection for specific pages
    let (selection, _total) = build_selection_for_pages(path, 1, 0, &[0, 2]);

    let file = File::open(path).unwrap();
    let options = ArrowReaderOptions::new().with_page_index(true);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).unwrap();

    // Project only columns 0 (id) and 1 (message)
    let mask = parquet::arrow::ProjectionMask::leaves(
        builder.parquet_schema(),
        [0, 1],
    );

    let reader = builder
        .with_projection(mask)
        .with_row_selection(selection)
        .with_batch_size(1024)
        .build()
        .unwrap();

    let mut total_rows = 0;
    for batch in reader {
        let batch = batch.unwrap();
        total_rows += batch.num_rows();
        println!(
            "    Batch: {} rows, {} columns (expected 2)",
            batch.num_rows(),
            batch.num_columns(),
        );
        // Verify we only got 2 columns
        assert_eq!(batch.num_columns(), 2, "Should only have projected columns");
    }
    println!(
        "  SUCCESS: Read {} rows with column projection + RowSelection + dict encoding",
        total_rows
    );
}

fn test_mixed_encodings(path: &str) {
    use parquet::schema::types::ColumnPath;

    // Write with mixed encodings: message=PLAIN (no dict), category=DICT, severity=DICT
    write_parquet(path, |builder| {
        builder
            .set_dictionary_enabled(true) // default: dict on
            .set_column_dictionary_enabled(
                ColumnPath::new(vec!["message".to_string()]),
                false,
            ) // message: plain
            .set_column_encoding(
                ColumnPath::new(vec!["message".to_string()]),
                Encoding::PLAIN,
            )
    });

    println!("  Written with MIXED encoding (message=PLAIN, category=DICT, severity=DICT).");
    print_page_structure(path);

    // Cross-column RowSelection: select pages from 'message' (plain) and read
    // all columns including 'category' (dict) for the same rows
    let (selection, _total) = build_selection_for_pages(path, 1, 0, &[0, 2]);

    let file = File::open(path).unwrap();
    let options = ArrowReaderOptions::new().with_page_index(true);
    let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)
        .unwrap()
        .with_row_selection(selection)
        .with_batch_size(1024)
        .build()
        .unwrap();

    let mut total_rows = 0;
    for batch in reader {
        let batch = batch.unwrap();
        total_rows += batch.num_rows();
        let id_col = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let msg_col = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let cat_col = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        let sev_col = batch.column(3).as_any().downcast_ref::<StringArray>().unwrap();
        println!(
            "    Batch: {} rows, first: id={} msg={:?} cat={:?} sev={:?}",
            batch.num_rows(),
            id_col.value(0),
            &msg_col.value(0)[..40.min(msg_col.value(0).len())],
            cat_col.value(0),
            sev_col.value(0),
        );
        // Verify data integrity: id should match expected pattern
        let first_id = id_col.value(0);
        let expected_msg_idx = (first_id as usize) % 10;
        let expected_cat = ["network", "http", "network", "database", "network",
                            "http", "network", "batch", "storage", "health"][expected_msg_idx];
        assert_eq!(
            cat_col.value(0), expected_cat,
            "Category mismatch for id={}: got {:?}, expected {:?}",
            first_id, cat_col.value(0), expected_cat
        );
    }
    println!(
        "  SUCCESS: Read {} rows with mixed encoding, cross-column RowSelection verified",
        total_rows
    );
}
