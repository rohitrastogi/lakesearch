//! Integration tests for object storage commands using InMemory store.

mod helpers;

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use lakesearch_cli::index::run_index;
use lakesearch_core::runtime::LakeRuntime;
use lakesearch_query::object_cache::ObjectCache;
use lakesearch_query::query::{self, QueryResult};
use lakesearch_query::storage::{read_current, read_metadata};
use lakesearch_query::Operator;

use helpers::{create_test_table, upload_test_parquet};

/// Test helper: wraps run_query with reference-based args for convenience.
#[allow(clippy::too_many_arguments)]
async fn run_query(
    store: &Arc<dyn ObjectStore>,
    base: &Path,
    column: &str,
    query_text: &str,
    operator: Operator,
    with_score: bool,
    limit: Option<usize>,
    select_columns: &[String],
    _runtime: &LakeRuntime,
) -> anyhow::Result<QueryResult> {
    let score_mode = if with_score {
        lakesearch_query::ScoreMode::Indexed
    } else {
        lakesearch_query::ScoreMode::None
    };
    let cache = Arc::new(ObjectCache::new(Arc::clone(store)));
    query::run_query(
        cache,
        base.clone(),
        column.to_owned(),
        query_text,
        operator,
        score_mode,
        limit,
        select_columns.to_vec(),
        8,
        Arc::new(LakeRuntime::new(2)),
    )
    .await
}

#[tokio::test]
async fn create_index_query_round_trip() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    // Create table
    create_test_table(store.as_ref(), &base, &["description"]).await;

    // Upload test parquet
    let file_path = upload_test_parquet(
        store.as_ref(),
        "data/test.parquet",
        100,
        25,
        &[
            "error timeout connection refused",
            "success response ok",
            "error connection reset",
            "warning slow query",
            "error timeout database",
        ],
    )
    .await;

    // Index
    run_index(
        &store,
        &base,
        std::slice::from_ref(&file_path),
        "description",
        &runtime,
    )
    .await
    .unwrap();

    // Verify metadata was updated
    let current = read_current(store.as_ref(), &base).await.unwrap();
    let meta = read_metadata(store.as_ref(), &current.value).await.unwrap();
    assert_eq!(meta.snapshot.manifest_lists.len(), 1);

    // Query: AND
    let result = run_query(
        &store,
        &base,
        "description",
        "error timeout",
        Operator::And,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();

    // Descriptions 0 and 4 contain both "error" and "timeout" → 2/5 * 100 = 40
    assert_eq!(result.stats.rows_matched, 40);

    // Query: OR
    let result = run_query(
        &store,
        &base,
        "description",
        "error timeout",
        Operator::Or,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();

    // Descriptions 0, 2, 4 contain "error" or "timeout" → 3/5 * 100 = 60
    assert_eq!(result.stats.rows_matched, 60);
}

#[tokio::test]
async fn multiple_appends_both_queried() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    create_test_table(store.as_ref(), &base, &["description"]).await;

    // Upload and index file A (has "alpha")
    let file_a = upload_test_parquet(
        store.as_ref(),
        "data/a.parquet",
        20,
        10,
        &["alpha bravo charlie"],
    )
    .await;
    run_index(&store, &base, &[file_a], "description", &runtime)
        .await
        .unwrap();

    // Upload and index file B (has "delta")
    let file_b = upload_test_parquet(
        store.as_ref(),
        "data/b.parquet",
        20,
        10,
        &["delta echo foxtrot"],
    )
    .await;
    run_index(&store, &base, &[file_b], "description", &runtime)
        .await
        .unwrap();

    // Metadata should have 2 manifest lists
    let current = read_current(store.as_ref(), &base).await.unwrap();
    let meta = read_metadata(store.as_ref(), &current.value).await.unwrap();
    assert_eq!(meta.snapshot.manifest_lists.len(), 2);

    // Query for "alpha" — only in file A
    let result = run_query(
        &store,
        &base,
        "description",
        "alpha",
        Operator::Or,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();
    assert_eq!(result.stats.rows_matched, 20);

    // Query for "delta" — only in file B
    let result = run_query(
        &store,
        &base,
        "description",
        "delta",
        Operator::Or,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();
    assert_eq!(result.stats.rows_matched, 20);

    // Query for "nonexistent" — in neither
    let result = run_query(
        &store,
        &base,
        "description",
        "nonexistent",
        Operator::Or,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();
    assert_eq!(result.stats.rows_matched, 0);
}

#[tokio::test]
async fn batch_dedup_prevents_double_index() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    create_test_table(store.as_ref(), &base, &["description"]).await;

    let file_path = upload_test_parquet(
        store.as_ref(),
        "data/dedup.parquet",
        20,
        10,
        &["hello world"],
    )
    .await;

    // Index once
    run_index(
        &store,
        &base,
        std::slice::from_ref(&file_path),
        "description",
        &runtime,
    )
    .await
    .unwrap();

    // Index again with same files — should be skipped
    run_index(
        &store,
        &base,
        std::slice::from_ref(&file_path),
        "description",
        &runtime,
    )
    .await
    .unwrap();

    // Should still have only 1 manifest list
    let current = read_current(store.as_ref(), &base).await.unwrap();
    let meta = read_metadata(store.as_ref(), &current.value).await.unwrap();
    assert_eq!(meta.snapshot.manifest_lists.len(), 1);

    // Query should find 20 matches, not 40 (no double-counting)
    let result = run_query(
        &store,
        &base,
        "description",
        "hello",
        Operator::Or,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();
    assert_eq!(result.stats.rows_matched, 20);
}

#[tokio::test]
async fn bm25_scoring_across_segments() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    create_test_table(store.as_ref(), &base, &["description"]).await;

    // File with "rare" in 1/4 descriptions
    let file_path = upload_test_parquet(
        store.as_ref(),
        "data/scoring.parquet",
        100,
        25,
        &[
            "rare unique special term",
            "common everyday normal word",
            "common regular standard phrase",
            "common typical ordinary text",
        ],
    )
    .await;

    run_index(&store, &base, &[file_path], "description", &runtime)
        .await
        .unwrap();

    let result = run_query(
        &store,
        &base,
        "description",
        "rare",
        Operator::Or,
        true,
        Some(5),
        &[],
        &runtime,
    )
    .await
    .unwrap();

    assert_eq!(result.matches.len(), 5);
    // All scores should be positive and finite
    for m in &result.matches {
        let s = m.score.unwrap();
        assert!(s > 0.0 && s.is_finite(), "bad score: {s}");
    }
    // Scores should be sorted descending
    for w in result.matches.windows(2) {
        assert!(w[0].score.unwrap() >= w[1].score.unwrap());
    }
}

#[tokio::test]
async fn empty_table_query() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    create_test_table(store.as_ref(), &base, &["description"]).await;

    let result = run_query(
        &store,
        &base,
        "description",
        "anything",
        Operator::Or,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();

    assert!(result.matches.is_empty());
    assert_eq!(result.stats.rows_matched, 0);
}

#[tokio::test]
async fn select_projects_additional_columns() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    create_test_table(store.as_ref(), &base, &["description"]).await;

    let file_path = upload_test_parquet(
        store.as_ref(),
        "data/select.parquet",
        10,
        5,
        &["error timeout"],
    )
    .await;

    run_index(
        &store,
        &base,
        std::slice::from_ref(&file_path),
        "description",
        &runtime,
    )
    .await
    .unwrap();

    // Query with --select id
    let result = run_query(
        &store,
        &base,
        "description",
        "error",
        Operator::Or,
        false,
        Some(3),
        &["id".to_owned()],
        &runtime,
    )
    .await
    .unwrap();

    assert_eq!(result.matches.len(), 3);
    for m in &result.matches {
        let cols = m.columns.as_ref().expect("should have columns");
        assert!(cols.contains_key("id"), "should have 'id' column");
        assert!(cols["id"].is_number(), "id should be a number");
    }
}

#[tokio::test]
async fn select_without_columns_omits_field() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    create_test_table(store.as_ref(), &base, &["description"]).await;

    let file_path = upload_test_parquet(
        store.as_ref(),
        "data/noselect.parquet",
        10,
        5,
        &["hello world"],
    )
    .await;

    run_index(
        &store,
        &base,
        std::slice::from_ref(&file_path),
        "description",
        &runtime,
    )
    .await
    .unwrap();

    let result = run_query(
        &store,
        &base,
        "description",
        "hello",
        Operator::Or,
        false,
        Some(1),
        &[],
        &runtime,
    )
    .await
    .unwrap();

    assert_eq!(result.matches.len(), 1);
    // columns field should be None (omitted in JSON)
    assert!(result.matches[0].columns.is_none());
}

// --- Optimization tests ---

#[tokio::test]
async fn top_k_heap_picks_highest_scores() {
    // Use documents with genuinely different BM25 scores: varying document
    // lengths with the same query term produces different scores due to
    // BM25's length normalization.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    create_test_table(store.as_ref(), &base, &["description"]).await;

    let file_path = upload_test_parquet(
        store.as_ref(),
        "data/topk.parquet",
        120,
        25,
        &[
            "error",                                                    // 1 token  → highest score
            "error timeout connection refused upstream",                // 5 tokens → medium
            "error timeout connection refused upstream gateway disk space network health batch upload", // 12 tokens → lowest
            "success response ok completed",                            // no match
        ],
    )
    .await;

    run_index(&store, &base, &[file_path], "description", &runtime)
        .await
        .unwrap();

    // Get all matches to know the full score distribution
    let all = run_query(
        &store,
        &base,
        "description",
        "error",
        Operator::Or,
        true,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();

    // There are 90 matches (3/4 * 120), with 3 distinct score levels
    assert_eq!(all.stats.rows_matched, 90);
    let best_score = all.matches[0].score.unwrap();
    let worst_score = all.matches.last().unwrap().score.unwrap();
    assert!(
        best_score > worst_score,
        "scores should differ: best={best_score}, worst={worst_score}"
    );

    // Now query with limit=5 — the heap should pick the 5 highest-scored rows
    let top5 = run_query(
        &store,
        &base,
        "description",
        "error",
        Operator::Or,
        true,
        Some(5),
        &[],
        &runtime,
    )
    .await
    .unwrap();

    assert_eq!(top5.matches.len(), 5);
    // All top-5 should have the best possible score (short "error" docs)
    for m in &top5.matches {
        assert_eq!(
            m.score.unwrap(),
            best_score,
            "top-K should only contain highest-scored rows"
        );
    }
    // Sorted descending
    for w in top5.matches.windows(2) {
        assert!(w[0].score.unwrap() >= w[1].score.unwrap());
    }
}

#[tokio::test]
async fn single_term_query_correctness() {
    // Single-term queries exercise the fast path that skips HashSet.
    // Verify exact match count and that AND/OR produce identical results.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    create_test_table(store.as_ref(), &base, &["description"]).await;

    // 3 descriptions, "alpha" appears in 2 of them
    let file_path = upload_test_parquet(
        store.as_ref(),
        "data/single.parquet",
        60, // divisible by 3
        20,
        &["alpha bravo", "charlie delta", "alpha echo"],
    )
    .await;

    run_index(&store, &base, &[file_path], "description", &runtime)
        .await
        .unwrap();

    let result_and = run_query(
        &store,
        &base,
        "description",
        "alpha",
        Operator::And,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();

    let result_or = run_query(
        &store,
        &base,
        "description",
        "alpha",
        Operator::Or,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();

    // Single term: AND and OR must match
    assert_eq!(result_and.stats.rows_matched, result_or.stats.rows_matched);
    // Exactly 2/3 of 60 = 40
    assert_eq!(result_and.stats.rows_matched, 40);
    // All matched text should contain "alpha"
    for m in &result_and.matches {
        assert!(
            m.text.contains("alpha"),
            "matched row should contain 'alpha': {}",
            m.text
        );
    }
}

#[tokio::test]
async fn segment_pruning_skips_irrelevant_segments() {
    // Two segments with non-overlapping term ranges.
    // Query for a term in one should not scan the other.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    create_test_table(store.as_ref(), &base, &["description"]).await;

    // Segment A: terms "apple", "banana", "cherry" (range a-c)
    let file_a = upload_test_parquet(
        store.as_ref(),
        "data/a.parquet",
        20,
        10,
        &["apple banana cherry"],
    )
    .await;
    run_index(&store, &base, &[file_a], "description", &runtime)
        .await
        .unwrap();

    // Segment B: terms "xray", "yankee", "zulu" (range x-z)
    let file_b = upload_test_parquet(
        store.as_ref(),
        "data/b.parquet",
        20,
        10,
        &["xray yankee zulu"],
    )
    .await;
    run_index(&store, &base, &[file_b], "description", &runtime)
        .await
        .unwrap();

    // "apple" is in segment A's range, outside segment B's → prune B
    let result = run_query(
        &store,
        &base,
        "description",
        "apple",
        Operator::Or,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();

    assert_eq!(result.stats.rows_matched, 20);
    assert_eq!(result.stats.rows_scanned, 20, "should prune segment B");
}

#[tokio::test]
async fn segment_pruning_boundary_term_not_pruned() {
    // A query term that exactly equals min_term or max_term should NOT be pruned.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    create_test_table(store.as_ref(), &base, &["description"]).await;

    let file_path = upload_test_parquet(
        store.as_ref(),
        "data/boundary.parquet",
        20,
        10,
        &["alpha omega"],
    )
    .await;
    run_index(&store, &base, &[file_path], "description", &runtime)
        .await
        .unwrap();

    // "alpha" is the min_term, "omega" is the max_term — both should match
    let result_min = run_query(
        &store,
        &base,
        "description",
        "alpha",
        Operator::Or,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();
    assert_eq!(
        result_min.stats.rows_matched, 20,
        "min_term boundary should not be pruned"
    );

    let result_max = run_query(
        &store,
        &base,
        "description",
        "omega",
        Operator::Or,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();
    assert_eq!(
        result_max.stats.rows_matched, 20,
        "max_term boundary should not be pruned"
    );
}

// --- Brute-force fallback tests ---

#[tokio::test]
async fn brute_force_matches_indexed_results() {
    // Upload two identical files with the same content.
    // Index only one. Query should find the same matches from both
    // (indexed path for file A, brute-force for file B).
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    create_test_table(store.as_ref(), &base, &["description"]).await;

    let descs = &["error timeout connection", "success response ok"];

    // Upload file A and index it
    let file_a = upload_test_parquet(store.as_ref(), "data/a.parquet", 20, 10, descs).await;
    run_index(
        &store,
        &base,
        std::slice::from_ref(&file_a),
        "description",
        &runtime,
    )
    .await
    .unwrap();

    // Upload file B with same content but DON'T index it.
    // It becomes part of data_files (via the manifest list) but has no
    // manifest entry for "description" — so it's un-indexed.
    let file_b = upload_test_parquet(store.as_ref(), "data/b.parquet", 20, 10, descs).await;

    // Manually add file B to the data_files of a new manifest list
    // (simulates a new append that hasn't been indexed yet for this column)
    let current = read_current(store.as_ref(), &base).await.unwrap();
    let mut meta = read_metadata(store.as_ref(), &current.value).await.unwrap();

    let ml = lakesearch_core::metadata::ManifestList {
        job_kind: lakesearch_core::metadata::JobKind::Append,
        batch_id: lakesearch_query::storage::compute_batch_id(&[&file_b]),
        data_files: vec![lakesearch_core::metadata::DataFileEntry {
            path: file_b.clone(),
            file_size_bytes: 0,
            row_count: 20,
        }],
        manifests: vec![], // No manifests — file is un-indexed
        replaces: None,
        compacted_column: None,
    };
    let ml_path = lakesearch_query::storage::write_manifest_list(store.as_ref(), &base, &ml)
        .await
        .unwrap();
    meta.snapshot.manifest_lists.push(ml_path);
    let meta_path = lakesearch_query::storage::write_metadata(store.as_ref(), &base, &meta)
        .await
        .unwrap();
    let pointer = lakesearch_core::metadata::CurrentPointer {
        metadata_path: meta_path,
        updated_at: "now".to_owned(),
    };
    lakesearch_query::storage::write_json(
        store.as_ref(),
        &base.child("metadata").child("current.json"),
        &pointer,
    )
    .await
    .unwrap();

    // Query "error" — should find 10 matches from file A (indexed)
    // and 10 from file B (brute-force) = 20 total
    let result = run_query(
        &store,
        &base,
        "description",
        "error",
        Operator::Or,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();

    assert_eq!(
        result.stats.rows_matched, 20,
        "should find matches from both indexed and un-indexed files"
    );

    // Verify matches come from both files
    let files: HashSet<&str> = result.matches.iter().map(|m| m.file.as_str()).collect();
    assert!(files.contains("data/a.parquet"), "should have indexed file");
    assert!(
        files.contains("data/b.parquet"),
        "should have brute-force file"
    );

    // All matched text should contain "error"
    for m in &result.matches {
        assert!(
            m.text.contains("error"),
            "matched row should contain 'error': {}",
            m.text
        );
    }
}

#[tokio::test]
async fn fully_indexed_and_fully_unindexed_same_results() {
    // Same data, query both ways: fully indexed vs fully un-indexed.
    // Match counts should be identical.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    create_test_table(store.as_ref(), &base, &["description"]).await;

    let descs = &[
        "error timeout connection",
        "success response ok",
        "error reset",
    ];
    let file_path = upload_test_parquet(store.as_ref(), "data/test.parquet", 30, 10, descs).await;

    // First: query with no index at all (file is un-indexed).
    // Add file to data_files without any manifest.
    let ml = lakesearch_core::metadata::ManifestList {
        job_kind: lakesearch_core::metadata::JobKind::Append,
        batch_id: lakesearch_query::storage::compute_batch_id(&[&file_path]),
        data_files: vec![lakesearch_core::metadata::DataFileEntry {
            path: file_path.clone(),
            file_size_bytes: 0,
            row_count: 30,
        }],
        manifests: vec![],
        replaces: None,
        compacted_column: None,
    };
    let ml_path = lakesearch_query::storage::write_manifest_list(store.as_ref(), &base, &ml)
        .await
        .unwrap();
    let current = read_current(store.as_ref(), &base).await.unwrap();
    let mut meta = read_metadata(store.as_ref(), &current.value).await.unwrap();
    meta.snapshot.manifest_lists.push(ml_path);
    let meta_path = lakesearch_query::storage::write_metadata(store.as_ref(), &base, &meta)
        .await
        .unwrap();
    let pointer = lakesearch_core::metadata::CurrentPointer {
        metadata_path: meta_path,
        updated_at: "t1".to_owned(),
    };
    lakesearch_query::storage::write_json(
        store.as_ref(),
        &base.child("metadata").child("current.json"),
        &pointer,
    )
    .await
    .unwrap();

    let brute_result = run_query(
        &store,
        &base,
        "description",
        "error",
        Operator::Or,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();

    // Now index the file
    run_index(&store, &base, &[file_path], "description", &runtime)
        .await
        .unwrap();

    let indexed_result = run_query(
        &store,
        &base,
        "description",
        "error",
        Operator::Or,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();

    // Same number of matches
    assert_eq!(
        brute_result.stats.rows_matched, indexed_result.stats.rows_matched,
        "brute-force ({}) and indexed ({}) should find same match count",
        brute_result.stats.rows_matched, indexed_result.stats.rows_matched
    );
}

#[tokio::test]
async fn brute_force_case_insensitive() {
    // Un-indexed file with mixed-case text. Query in lowercase should match
    // because the tokenizer lowercases and the pre-filter must be case-insensitive.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    create_test_table(store.as_ref(), &base, &["description"]).await;

    // Upload file with mixed-case descriptions — NOT indexed
    let file_path = upload_test_parquet(
        store.as_ref(),
        "data/mixed_case.parquet",
        20,
        10,
        &["Error Timeout CONNECTION", "Success Response OK"],
    )
    .await;

    // Add to data_files without indexing
    let current = read_current(store.as_ref(), &base).await.unwrap();
    let mut meta = read_metadata(store.as_ref(), &current.value).await.unwrap();
    let ml = lakesearch_core::metadata::ManifestList {
        job_kind: lakesearch_core::metadata::JobKind::Append,
        batch_id: lakesearch_query::storage::compute_batch_id(&[&file_path]),
        data_files: vec![lakesearch_core::metadata::DataFileEntry {
            path: file_path.clone(),
            file_size_bytes: 0,
            row_count: 20,
        }],
        manifests: vec![],
        replaces: None,
        compacted_column: None,
    };
    let ml_path = lakesearch_query::storage::write_manifest_list(store.as_ref(), &base, &ml)
        .await
        .unwrap();
    meta.snapshot.manifest_lists.push(ml_path);
    let meta_path = lakesearch_query::storage::write_metadata(store.as_ref(), &base, &meta)
        .await
        .unwrap();
    let pointer = lakesearch_core::metadata::CurrentPointer {
        metadata_path: meta_path,
        updated_at: "now".to_owned(),
    };
    lakesearch_query::storage::write_json(
        store.as_ref(),
        &base.child("metadata").child("current.json"),
        &pointer,
    )
    .await
    .unwrap();

    // Query lowercase "error" — should match mixed-case "Error"
    let result = run_query(
        &store,
        &base,
        "description",
        "error",
        Operator::Or,
        false,
        None,
        &[],
        &runtime,
    )
    .await
    .unwrap();

    // 1/2 descriptions contain "error" (case-insensitive) → 10 matches
    assert_eq!(
        result.stats.rows_matched, 10,
        "case-insensitive pre-filter should match mixed-case text"
    );
    for m in &result.matches {
        let lower = m.text.to_lowercase();
        assert!(
            lower.contains("error"),
            "matched row should contain 'error': {}",
            m.text
        );
    }
}

#[tokio::test]
async fn brute_force_early_termination_with_limit() {
    // With a small limit on an unscored brute-force query, the scan should
    // stop early — rows_scanned should be less than total rows.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = Path::from("table");
    let runtime = LakeRuntime::new(2);

    create_test_table(store.as_ref(), &base, &["description"]).await;

    // Create with multiple small row groups so early termination can
    // kick in between them. 4 row groups × 25 rows = 100 rows.
    let file_path = {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("description", DataType::Utf8, true),
        ]));
        let ids: Vec<i32> = (0..100).collect();
        let descs: Vec<Option<&str>> = (0..100).map(|_| Some("hello world")).collect();
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
            .set_data_page_row_count_limit(10)
            .set_max_row_group_size(25)
            .set_dictionary_enabled(false)
            .build();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        store
            .put(
                &Path::from("data/early.parquet"),
                PutPayload::from(Bytes::from(buf)),
            )
            .await
            .unwrap();
        "data/early.parquet".to_owned()
    };

    // Add as un-indexed
    let current = read_current(store.as_ref(), &base).await.unwrap();
    let mut meta = read_metadata(store.as_ref(), &current.value).await.unwrap();
    let ml = lakesearch_core::metadata::ManifestList {
        job_kind: lakesearch_core::metadata::JobKind::Append,
        batch_id: lakesearch_query::storage::compute_batch_id(&[&file_path]),
        data_files: vec![lakesearch_core::metadata::DataFileEntry {
            path: file_path.clone(),
            file_size_bytes: 0,
            row_count: 100,
        }],
        manifests: vec![],
        replaces: None,
        compacted_column: None,
    };
    let ml_path = lakesearch_query::storage::write_manifest_list(store.as_ref(), &base, &ml)
        .await
        .unwrap();
    meta.snapshot.manifest_lists.push(ml_path);
    let meta_path = lakesearch_query::storage::write_metadata(store.as_ref(), &base, &meta)
        .await
        .unwrap();
    let pointer = lakesearch_core::metadata::CurrentPointer {
        metadata_path: meta_path,
        updated_at: "now".to_owned(),
    };
    lakesearch_query::storage::write_json(
        store.as_ref(),
        &base.child("metadata").child("current.json"),
        &pointer,
    )
    .await
    .unwrap();

    // Query with limit=3 (unscored)
    let result = run_query(
        &store,
        &base,
        "description",
        "hello",
        Operator::Or,
        false,
        Some(3),
        &[],
        &runtime,
    )
    .await
    .unwrap();

    assert_eq!(result.matches.len(), 3, "should return exactly 3 matches");
    // Early termination: should NOT have scanned all 100 rows
    assert!(
        result.stats.rows_scanned < 100,
        "should stop early, scanned {} of 100",
        result.stats.rows_scanned
    );
}
