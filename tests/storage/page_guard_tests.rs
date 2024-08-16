use crate::test_setup::initialize_logger;
use chrono::Utc;
use spin::{Mutex, RwLock};
use std::sync::Arc;
use log::LevelFilter;
use tkdb::buffer::buffer_pool_manager::BufferPoolManager;
use tkdb::buffer::lru_k_replacer::LRUKReplacer;
use tkdb::storage::disk::disk_manager::FileDiskManager;
use tkdb::storage::disk::disk_scheduler::DiskScheduler;
use tkdb::storage::page::page_guard::{BasicPageGuard, ReadPageGuard, WritePageGuard};
use tkdb::common::time::{SystemTimeSource, TimeSource};

struct TestContext {
    bpm: Arc<BufferPoolManager>,
    db_file: String,
    db_log_file: String,
}

impl TestContext {
    fn new(test_name: &str) -> Self {
        initialize_logger();
        const BUFFER_POOL_SIZE: usize = 5;
        const K: usize = 2;
        let time_source: Arc<dyn TimeSource> = Arc::new(SystemTimeSource);

        let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
        let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
        let db_log_file = format!("tests/data/{}_{}.log", test_name, timestamp);

        let disk_manager = Arc::new(FileDiskManager::new(db_file.clone(), db_log_file.clone()));
        let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K, time_source)));
        let bpm = Arc::new(BufferPoolManager::new(
            BUFFER_POOL_SIZE,
            disk_scheduler,
            disk_manager.clone(),
            replacer.clone(),
        ));

        Self {
            bpm,
            db_file,
            db_log_file,
        }
    }

    fn cleanup(&self) {
        let _ = std::fs::remove_file(&self.db_file);
        let _ = std::fs::remove_file(&self.db_log_file);
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        self.cleanup();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_tests() {
        let ctx = TestContext::new("basic_tests");
        let bpm = Arc::clone(&ctx.bpm);

        // Create a new guarded page with bpm
        let page0 = bpm.new_page().unwrap();
        let basic_guarded_page = ReadPageGuard::new(Arc::clone(&bpm), Arc::clone(&page0));

        // Ensure page0 is pinned and the guard works as expected
        assert_eq!(page0.read().get_pin_count(), 1); // Pin count should be 1 after creation
        assert_eq!(page0.read().get_page_id(), basic_guarded_page.page_id());
        // assert_eq!(page0.read().get_data(), guarded_page.get_data());

        // Create another page and guard it with ReadPageGuard
        let page2 = bpm.new_page().unwrap();
        let read_guard = ReadPageGuard::new(Arc::clone(&bpm), Arc::clone(&page2));
        assert_eq!(page2.read().get_pin_count(), 1); // Pin count should be 1 after creating the guard

        // Ensure dropping the guard decreases the pin count
        drop(read_guard);
        assert_eq!(page2.read().get_pin_count(), 0); // Pin count should be 0 after dropping the guard

        // Test WritePageGuard functionality
        let page3 = bpm.new_page().unwrap();
        {
            let write_guard = WritePageGuard::new(Arc::clone(&bpm), Arc::clone(&page3));
            assert_eq!(page3.read().get_pin_count(), 1); // Pin count should be 1 after creating WritePageGuard
        } // WritePageGuard goes out of scope here

        // Ensure pin count is reduced after dropping WritePageGuard
        assert_eq!(page3.read().get_pin_count(), 0);
    }
}
