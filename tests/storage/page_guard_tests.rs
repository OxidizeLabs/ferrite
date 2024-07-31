// #[cfg(test)]
// mod tests {
//     extern crate tkdb;
//
//     use spin::Mutex;
//     use std::sync::Arc;
//     use tokio::fs::remove_file;
//     use tkdb::buffer::buffer_pool_manager::BufferPoolManager;
//     // use tkdb::storage::disk::disk_manager_memory::DiskManagerUnlimitedMemory;
//     use tkdb::storage::page::page_guard::{BasicPageGuard, ReadPageGuard};
//     use tokio::test;
//     use tkdb::buffer::lru_k_replacer::LRUKReplacer;
//     use tkdb::storage::disk::disk_manager::DiskManager;
//     use tkdb::storage::disk::disk_scheduler::DiskScheduler;
//
//     struct TestContext {
//         disk_manager: Arc<DiskManager>,
//         disk_scheduler: Arc<DiskScheduler>,
//         db_file: String,
//         db_log: String,
//     }
//
//     impl TestContext {
//         fn new() -> Self {
//             let db_file = "test_page_guard.db";
//             let log_file = "test_page_guard.log";
//             let disk_manager = Arc::new(DiskManager::new(db_file, log_file));
//             let disk_scheduler = Arc::new(DiskScheduler::new(Arc::clone(&disk_manager)));
//             Self { disk_manager, disk_scheduler, db_file: db_file.to_string(), db_log: log_file.to_string() }
//         }
//
//         fn cleanup(&self) {
//             let _ = remove_file(&self.db_file);
//             let _ = remove_file(&self.db_log);
//         }
//     }
//
//     impl Drop for TestContext {
//         fn drop(&mut self) {
//             self.cleanup();
//         }
//     }
//
//     #[test]
//     async fn sample_test() {
//         const BUFFER_POOL_SIZE: usize = 5;
//         const K: usize = 2;
//
//         let disk_manager = Arc::new(DiskManager::new("test", "test"));
//         let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager.clone()));
//         let replacer = Arc::new(Mutex::new(LRUKReplacer::new(10, 2)));
//         let bpm = Arc::new(BufferPoolManager::new(BUFFER_POOL_SIZE, disk_scheduler, disk_manager.clone(), replacer.clone()));
//
//         let page0 = bpm.new_page().unwrap();
//         let mut guarded_page = BasicPageGuard::new(bpm.clone(), Arc::new(page0.clone()));
//
//         assert_eq!(page0.get_data(), guarded_page.get_data());
//         assert_eq!(page0.get_page_id(), guarded_page.page_id());
//         assert_eq!(1, page0.get_pin_count());
//
//         guarded_page.drop();
//
//         assert_eq!(0, page0.get_pin_count());
//
//         {
//             let page2 = bpm.new_page().expect("Failed to create new page");
//             let _guard2 = ReadPageGuard::new(bpm.clone(), Arc::new(page2));
//         }
//
//         // Shutdown the disk manager and remove the temporary file we created.
//         disk_manager.shut_down();
//     }
// }
