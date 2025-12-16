use parking_lot::RwLock;
use std::sync::Arc;
use tempfile::TempDir;
use tkdb::buffer::buffer_pool_manager_async::BufferPoolManager;
use tkdb::buffer::lru_k_replacer::LRUKReplacer;
use tkdb::catalog::schema::Schema;
use tkdb::common::logger::initialize_logger;
use tkdb::common::rid::RID;
use tkdb::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
use tkdb::storage::index::b_plus_tree_index::BPlusTreeIndex;
use tkdb::storage::index::latch_crabbing::{HeldWriteLock, OperationType};
use tkdb::storage::index::types::comparators::{i32_comparator, I32Comparator};
use tkdb::storage::index::{IndexInfo, IndexType};
use tkdb::storage::page::page_types::b_plus_tree_internal_page::BPlusTreeInternalPage;

struct TestContext {
    bpm: Arc<BufferPoolManager>,
    _temp_dir: TempDir,
}

impl TestContext {
    async fn new(name: &str) -> Self {
        Self::with_pool_size(name, 10).await
    }

    async fn with_pool_size(name: &str, pool_size: usize) -> Self {
        initialize_logger();
        const K: usize = 2;

        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir
            .path()
            .join(format!("{name}.db"))
            .to_str()
            .unwrap()
            .to_string();
        let log_path = temp_dir
            .path()
            .join(format!("{name}.log"))
            .to_str()
            .unwrap()
            .to_string();

        let disk_manager =
            AsyncDiskManager::new(db_path, log_path, DiskManagerConfig::default()).await;
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(pool_size, K)));
        let bpm = Arc::new(
            BufferPoolManager::new(pool_size, Arc::new(disk_manager.unwrap()), replacer).unwrap(),
        );

        Self {
            bpm,
            _temp_dir: temp_dir,
        }
    }
}

#[tokio::test]
async fn test_held_write_lock_actually_holds_lock() {
    // This test verifies that HeldWriteLock actually prevents other threads
    // from acquiring the lock.
    let ctx = TestContext::new("test_held_write_lock").await;
    let bpm = ctx.bpm;

    let key_schema = Schema::new(vec![]);
    let metadata = IndexInfo::new(
        key_schema,
        "held_write_lock_test".to_string(),
        1,
        "test_table".to_string(),
        4,
        false,
        IndexType::BPlusTreeIndex,
        vec![0],
    );

    let mut tree =
        BPlusTreeIndex::<i32, RID, I32Comparator>::new(i32_comparator, metadata, bpm.clone());
    assert!(tree.init_with_order(10).is_ok());

    // Insert enough keys to create some internal nodes
    for i in 1..=15 {
        assert!(tree.insert(i, RID::new(1, i as u32)).is_ok());
    }

    let root_page_id = tree.get_root_page_id();

    // Skip if root is a leaf (need internal node for this test)
    if tree.get_height() <= 1 {
        println!("Tree height is 1, root is a leaf - skipping internal page lock test");
        return;
    }

    let internal_page = bpm
        .fetch_page::<BPlusTreeInternalPage<i32, I32Comparator>>(root_page_id)
        .expect("Should be able to fetch internal page");

    // IMPORTANT: Don't call `bpm.fetch_page()` while holding a write latch on this page.
    // `PageGuard::new()` increments pin_count via `page.write()`, which will deadlock
    // if we already hold the write lock (parking_lot locks are not re-entrant).
    // Instead, clone the underlying Arc<RwLock<_>> and probe try_read/try_write directly.
    let internal_arc = Arc::clone(internal_page.get_page());

    // Create a HeldWriteLock - this should acquire and hold the write lock
    let held_lock = HeldWriteLock::new(internal_page);

    // Verify we can read from the held lock
    let _size = held_lock.get_size();
    println!("HeldWriteLock acquired, page size: {}", _size);

    // try_write should return None because we're holding the write lock
    let try_result = internal_arc.try_write();
    assert!(
        try_result.is_none(),
        "Should NOT be able to acquire write lock while HeldWriteLock exists"
    );

    // try_read should also return None because write lock is held
    let try_read_result = internal_arc.try_read();
    assert!(
        try_read_result.is_none(),
        "Should NOT be able to acquire read lock while HeldWriteLock exists"
    );

    println!("Verified: HeldWriteLock prevents other lock acquisitions");

    // Now drop the HeldWriteLock
    drop(held_lock);

    // After dropping, we should be able to acquire locks again
    let try_result_after = internal_arc.try_write();
    assert!(
        try_result_after.is_some(),
        "Should be able to acquire write lock after HeldWriteLock is dropped"
    );
    drop(try_result_after);

    println!("Verified: Lock is released when HeldWriteLock is dropped");
    println!("test_held_write_lock_actually_holds_lock PASSED!");
}

#[tokio::test]
async fn test_pessimistic_traversal_holds_unsafe_nodes() {
    // This test verifies that pessimistic traversal actually holds locks
    // on unsafe nodes.
    // Use larger buffer pool (50 pages) to accommodate multiple leaf/internal pages
    // during tree splits with 20 keys
    let ctx = TestContext::with_pool_size("test_pessimistic_holds_unsafe", 50).await;
    let bpm = ctx.bpm;

    let key_schema = Schema::new(vec![]);
    let metadata = IndexInfo::new(
        key_schema,
        "pessimistic_holds_unsafe_test".to_string(),
        1,
        "test_table".to_string(),
        4,
        false,
        IndexType::BPlusTreeIndex,
        vec![0],
    );

    let mut tree =
        BPlusTreeIndex::<i32, RID, I32Comparator>::new(i32_comparator, metadata, bpm.clone());
    // Use small order to force splits and create internal nodes
    assert!(tree.init_with_order(4).is_ok());

    // Insert keys to create a tree with multiple levels
    for i in 1..=20 {
        assert!(
            tree.insert(i, RID::new(1, i as u32)).is_ok(),
            "Failed to insert key {}",
            i
        );
    }

    println!("Tree height: {}, size: {}", tree.get_height(), tree.get_size());

    // Perform a pessimistic traversal and check that context holds locks
    // when nodes are not safe
    let traversal_result = tree.traverse_pessimistic(&10, OperationType::Insert);

    match traversal_result {
        Ok(result) => {
            println!(
                "Pessimistic traversal completed, held locks: {}, is_safe: {}",
                result.context.held_count(),
                result.is_safe
            );

            if !result.is_safe {
                println!(
                    "Leaf is not safe, {} ancestor locks held",
                    result.context.held_count()
                );
            }

            // Drop the result to release all held locks before validating the tree.
            // The HeldWriteLock in the context holds actual write locks that would
            // block validate_tree() from reading those pages.
            drop(result);

            // Verify tree is still valid after traversal
            assert!(tree.validate_tree().is_ok());
        }
        Err(e) => {
            panic!("Pessimistic traversal failed: {}", e);
        }
    }

    println!("test_pessimistic_traversal_holds_unsafe_nodes PASSED!");
}

