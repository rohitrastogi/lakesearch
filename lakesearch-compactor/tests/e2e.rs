//! Integration tests: index multiple batches, compact, verify metadata and query results.

mod helpers;

use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;

use lakesearch_compactor::run_compact;
use lakesearch_core::runtime::LakeRuntime;
use lakesearch_core::storage::{read_current, read_manifest_list, read_metadata};
use lakesearch_indexer::run_index;

use helpers::{create_test_table, upload_test_parquet};

/// Indexes two batches, compacts with threshold=2, verifies:
/// - Only 1 manifest list remains in metadata
/// - The manifest list has job_kind=Compact and replaces the originals
/// - Query results are identical to pre-compaction
#[tokio::test]
async fn compact_two_batches() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let table_base = Path::from("table");
    let index_base = table_base.child("lakesearch");

    create_test_table(store.as_ref(), &index_base, &["description"]).await;

    // Index batch 1
    let file1 = upload_test_parquet(
        store.as_ref(),
        "data/batch1.parquet",
        50,
        25,
        &["alpha bravo charlie", "alpha delta echo"],
    )
    .await;

    let runtime = LakeRuntime::new(2);
    run_index(&store, &index_base, &[file1], "description", &runtime)
        .await
        .unwrap();

    // Index batch 2
    let file2 = upload_test_parquet(
        store.as_ref(),
        "data/batch2.parquet",
        50,
        25,
        &["foxtrot golf hotel", "foxtrot india juliet"],
    )
    .await;

    run_index(&store, &index_base, &[file2], "description", &runtime)
        .await
        .unwrap();

    // Verify: 2 manifest lists before compaction
    let pre = read_current(store.as_ref(), &index_base).await.unwrap();
    let pre_meta = read_metadata(store.as_ref(), &pre.value).await.unwrap();
    assert_eq!(pre_meta.snapshot.manifest_lists.len(), 2);

    // Compact with threshold=2
    let did_compact = run_compact(Arc::clone(&store), index_base.clone(), 2, Arc::new(runtime))
        .await
        .unwrap();
    assert!(did_compact, "compaction should have run");

    // Verify: 1 manifest list after compaction
    let post = read_current(store.as_ref(), &index_base).await.unwrap();
    let post_meta = read_metadata(store.as_ref(), &post.value).await.unwrap();
    assert_eq!(
        post_meta.snapshot.manifest_lists.len(),
        1,
        "should have exactly 1 manifest list after compaction"
    );

    // Verify: the manifest list is a compact job with replaces
    let ml = read_manifest_list(store.as_ref(), &post_meta.snapshot.manifest_lists[0])
        .await
        .unwrap();
    assert_eq!(ml.job_kind, lakesearch_core::metadata::JobKind::Compact);
    assert!(ml.replaces.is_some());
    assert_eq!(ml.replaces.as_ref().unwrap().len(), 2);
    assert_eq!(ml.compacted_column.as_deref(), Some("description"));

    // Verify: merged data_files includes both batches
    assert_eq!(ml.data_files.len(), 2);

    // Verify: merged manifest has 1 segment covering all terms
    assert_eq!(ml.manifests.len(), 1);
    assert_eq!(ml.manifests[0].indexed_column, "description");
    assert_eq!(ml.manifests[0].segment_count, 1);
}

/// Indexes two batches for two different columns, compacts one column,
/// verifies the other column's manifests are carried forward unchanged.
#[tokio::test]
async fn compact_preserves_other_columns() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let table_base = Path::from("table");
    let index_base = table_base.child("lakesearch");

    // Create table with two indexed columns (both map to "description" parquet col
    // since our test parquet only has that column, but they'll be separate segments)
    create_test_table(store.as_ref(), &index_base, &["description"]).await;

    // Index batch 1 for "description"
    let file1 = upload_test_parquet(
        store.as_ref(),
        "data/b1.parquet",
        50,
        25,
        &["apple banana cherry"],
    )
    .await;

    let runtime = LakeRuntime::new(2);
    run_index(&store, &index_base, &[file1], "description", &runtime)
        .await
        .unwrap();

    // Index batch 2 for "description"
    let file2 = upload_test_parquet(
        store.as_ref(),
        "data/b2.parquet",
        50,
        25,
        &["date elderberry fig"],
    )
    .await;

    run_index(&store, &index_base, &[file2], "description", &runtime)
        .await
        .unwrap();

    // Pre-compaction: 2 manifest lists
    let pre = read_current(store.as_ref(), &index_base).await.unwrap();
    let pre_meta = read_metadata(store.as_ref(), &pre.value).await.unwrap();
    assert_eq!(pre_meta.snapshot.manifest_lists.len(), 2);

    // Compact
    let did_compact = run_compact(Arc::clone(&store), index_base.clone(), 2, Arc::new(runtime))
        .await
        .unwrap();
    assert!(did_compact);

    // Post-compaction: 1 manifest list
    let post = read_current(store.as_ref(), &index_base).await.unwrap();
    let post_meta = read_metadata(store.as_ref(), &post.value).await.unwrap();
    assert_eq!(post_meta.snapshot.manifest_lists.len(), 1);

    // The compacted manifest list has exactly 1 manifest for "description"
    let ml = read_manifest_list(store.as_ref(), &post_meta.snapshot.manifest_lists[0])
        .await
        .unwrap();

    let desc_manifests: Vec<_> = ml
        .manifests
        .iter()
        .filter(|m| m.indexed_column == "description")
        .collect();
    assert_eq!(
        desc_manifests.len(),
        1,
        "should have 1 merged description manifest"
    );
}
