use crate::test_setup::initialize_logger;
use chrono::Utc;
use spin::Mutex;
use std::sync::Arc;
use tkdb::buffer::buffer_pool_manager::BufferPoolManager;
use tkdb::buffer::lru_k_replacer::LRUKReplacer;
use tkdb::storage::disk::disk_manager::FileDiskManager;
use tkdb::storage::disk::disk_scheduler::DiskScheduler;
use tkdb::storage::page::page_guard::{BasicPageGuard, ReadPageGuard, WritePageGuard};
use tokio::sync::Mutex as TokioMutex;

struct TestContext {
    bpm: Arc<BufferPoolManager>,
    db_file: String,
    db_log_file: String,
}

impl TestContext {
    async fn new(test_name: &str) -> Self {
        initialize_logger();
        const BUFFER_POOL_SIZE: usize = 5;
        const K: usize = 2;
        let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
        let db_file = format!("{}_{}.db", test_name, timestamp);
        let db_log_file = format!("{}_{}.log", test_name, timestamp);
        let disk_manager =
            Arc::new(FileDiskManager::new(db_file.clone(), db_log_file.clone()).await);
        let disk_scheduler = Arc::new(TokioMutex::new(DiskScheduler::new(Arc::clone(
            &disk_manager,
        ))));
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
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

    async fn cleanup(&self) {
        let _ = tokio::fs::remove_file(&self.db_file).await;
        let _ = tokio::fs::remove_file(&self.db_log_file).await;
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        let db_file = self.db_file.clone();
        let db_log = self.db_log_file.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let _ = tokio::fs::remove_file(db_file).await;
                let _ = tokio::fs::remove_file(db_log).await;
            });
        })
        .join()
        .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn basic_tests() {
        let context = TestContext::new("basic_tests").await;
        let bpm = &context.bpm;

        let page0 = bpm.new_page().await.unwrap();
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
            let page2 = bpm.new_page().await.unwrap();
            let page2_arc = Arc::new(Mutex::new(page2));
            let _guard2 = ReadPageGuard::new(bpm.clone(), page2_arc.clone());
            // Pin count should increase to 1
            assert_eq!(1, page2_arc.lock().get_pin_count());
        } // _guard2 goes out of scope and should decrease pin count

        // Verify that the pin count has decreased
        assert_eq!(0, page0_arc.lock().get_pin_count());

        // Test WritePageGuard
        {
            let page3 = bpm.new_page().await.unwrap();
            let page3_arc = Arc::new(Mutex::new(page3));
            let page_guard3 = WritePageGuard::new(bpm.clone(), page3_arc.clone());
            // Pin count should increase to 1
            assert_eq!(1, page3_arc.lock().get_pin_count());
        } // _guard3 goes out of scope and should decrease pin count
    }
}
