#![allow(dead_code)]

use crate::common::logger::init_test_logger;
use ferrite::buffer::buffer_pool_manager_async::BufferPoolManager;
use ferrite::buffer::lru_k_replacer::LRUKReplacer;
use ferrite::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
use parking_lot::RwLock;
use std::sync::Arc;
use tempfile::TempDir;

struct AsyncTestContext {
    bpm: Arc<BufferPoolManager>,
    _temp_dir: TempDir,
}

impl AsyncTestContext {
    async fn new(name: &str) -> Self {
        init_test_logger();
        const BUFFER_POOL_SIZE: usize = 64;
        const K: usize = 2;
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir
            .path()
            .join(format!("{name}.db"))
            .to_string_lossy()
            .to_string();
        let log_path = temp_dir
            .path()
            .join(format!("{name}.log"))
            .to_string_lossy()
            .to_string();
        let disk = AsyncDiskManager::new(db_path, log_path, DiskManagerConfig::default())
            .await
            .unwrap();
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
        let bpm =
            Arc::new(BufferPoolManager::new(BUFFER_POOL_SIZE, Arc::new(disk), replacer).unwrap());
        Self {
            bpm,
            _temp_dir: temp_dir,
        }
    }
}

#[tokio::test]
async fn async_basic_page_operations() {
    let _ctx = AsyncTestContext::new("async_basic_page_operations").await;
    // Placeholder: exercise new_page/fetch_page briefly via storage TablePage in unit tests
}
