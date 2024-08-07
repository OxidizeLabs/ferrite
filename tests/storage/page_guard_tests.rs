use spin::Mutex;
use std::sync::Arc;
use chrono::Utc;
use tokio::fs::remove_file;
use tkdb::buffer::buffer_pool_manager::BufferPoolManager;
use tkdb::storage::page::page_guard::{BasicPageGuard, ReadPageGuard, WritePageGuard};
use tkdb::buffer::lru_k_replacer::LRUKReplacer;
use tkdb::storage::disk::disk_manager::DiskManager;
use tkdb::storage::disk::disk_scheduler::DiskScheduler;
use crate::test_setup::initialize_logger;

struct TestContext {
    bpm: Arc<BufferPoolManager>,
    db_file: String,
    db_log_file: String,
}

impl TestContext {
    fn new() -> Self {
        const BUFFER_POOL_SIZE: usize = 5;
        const K: usize = 2;
        initialize_logger();
        let timestamp = Utc::now().format("%Y%m%d%H%M%S").to_string();
        let db_file = format!("test_page_guard_{}.db", timestamp);
        let db_log_file = format!("test_page_guard_{}.log", timestamp);
        let disk_manager = Arc::new(DiskManager::new(db_file.clone(), db_log_file.clone()));
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::clone(&disk_manager)));
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
        let bpm = Arc::new(BufferPoolManager::new(
            BUFFER_POOL_SIZE,
            disk_scheduler.clone(),
            disk_manager.clone(),
            replacer.clone(),
        ));
        Self { bpm, db_file, db_log_file }
    }

    fn cleanup(&self) {
        let _ = remove_file(&self.db_file);
        let _ = remove_file(&self.db_log_file);
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
        let context = TestContext::new();
        let bpm = &context.bpm;

        let page0 = bpm.new_page().unwrap();
        let page0_arc = Arc::new(Mutex::new(page0.clone()));
        let mut guarded_page = BasicPageGuard::new(bpm.clone(), page0_arc.clone());

        assert_eq!(&*page0.get_data(), &*guarded_page.get_data());
        assert_eq!(page0.get_page_id(), guarded_page.page_id());
        assert_eq!(1, page0_arc.lock().get_pin_count()); // Pin count should be 1 after creation

        // Manually dropping the guard to decrease pin count
        guarded_page.drop();
        let check = page0_arc.lock().get_pin_count();

        assert_eq!(0, check); // Pin count should be 0 after dropping the guard

        {
            let page2 = bpm.new_page().unwrap();
            let page2_arc = Arc::new(Mutex::new(page2));
            let _guard2 = ReadPageGuard::new(bpm.clone(), page2_arc.clone());
            // Pin count should increase to 1
            assert_eq!(1, page2_arc.lock().get_pin_count());
        } // _guard2 goes out of scope and should decrease pin count

        // Verify that the pin count has decreased
        assert_eq!(0, page0_arc.lock().get_pin_count());

        // Test WritePageGuard
        {
            let page3 = bpm.new_page().unwrap();
            let page3_arc = Arc::new(Mutex::new(page3));
            let page_guard3 = WritePageGuard::new(bpm.clone(), page3_arc.clone());
            // Pin count should increase to 1
            assert_eq!(1, page3_arc.lock().get_pin_count());

        } // _guard3 goes out of scope and should decrease pin count
    }
}
