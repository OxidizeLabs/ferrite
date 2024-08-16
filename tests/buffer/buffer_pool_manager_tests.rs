use crate::test_setup::initialize_logger;
use chrono::Utc;
use log::info;
use rand::Rng;
use std::sync::{Arc, Mutex};
use std::thread;
use spin::RwLock;
use tkdb::buffer::buffer_pool_manager::BufferPoolManager;
use tkdb::buffer::lru_k_replacer::{LRUKReplacer, SystemTimeSource};
use tkdb::common::time::TimeSource;
use tkdb::storage::disk::disk_manager::FileDiskManager;
use tkdb::storage::disk::disk_scheduler::DiskScheduler;
use tkdb::common::config::{PageId, DB_PAGE_SIZE};
use std::any::Any;
use tkdb::buffer::lru_k_replacer::AccessType;


struct TestContext {
    bpm: Arc<BufferPoolManager>,
    db_file: String,
    db_log_file: String,
    buffer_pool_size: usize,
}

impl TestContext {
    fn new(test_name: &str) -> Self {
        initialize_logger();
        const BUFFER_POOL_SIZE: usize = 10;
        let time_source: Arc<dyn TimeSource> = Arc::new(SystemTimeSource);
        const K: usize = 2;
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
            buffer_pool_size: BUFFER_POOL_SIZE,
        }
    }

    fn cleanup(&self) {
        let _ = std::fs::remove_file(&self.db_file);
        let _ = std::fs::remove_file(&self.db_log_file);
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        let db_file = self.db_file.clone();
        let db_log = self.db_log_file.clone();
        thread::spawn(move || {
            let _ = std::fs::remove_file(db_file);
            let _ = std::fs::remove_file(db_log);
        })
            .join()
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_page_deadlock() {
        // Initialize the test context
        let ctx = TestContext::new("test_write_page_deadlock");
        let bpm = Arc::clone(&ctx.bpm);

        // Generate random data for writing
        let mut rng = rand::thread_rng();
        let data: [u8; DB_PAGE_SIZE] = rng.gen();

        // Mutex for synchronizing test completion
        let done = Arc::new(Mutex::new(false));
        let done_clone = Arc::clone(&done);

        // Create a list to hold thread handles
        let mut handles = vec![];

        // Spawn threads to perform concurrent write operations
        for i in 0..10 {
            let bpm_clone = Arc::clone(&bpm);
            let done_clone = Arc::clone(&done);

            let handle = thread::spawn(move || {
                // Create a new page
                let page_id = i as u32;
                bpm_clone.new_page().expect("Failed to create a new page");

                // Write data to the page
                bpm_clone.write_page(page_id as PageId, data);

                // Signal completion
                let mut done = done_clone.lock().unwrap();
                *done = true;
            });

            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("Thread failed");
        }

        // Check if the test finished without deadlocks
        let done = done.lock().unwrap();
        assert!(*done, "Test did not complete successfully, possible deadlock detected");

        // Cleanup the test context
        ctx.cleanup();
    }
}

#[cfg(test)]
mod extended_tests {
    use std::time::Duration;
    use std::thread::sleep;
    use super::*;

    #[test]
    fn concurrent_page_access_test() {
        let ctx = TestContext::new("concurrent_page_access_test");
        let bpm = Arc::new(RwLock::new(ctx.bpm.clone()));
        let page = bpm.write().new_page().unwrap();

        let page_id = page.read().get_page_id();
        let bpm_clone = bpm.clone();
        let mut handles = vec![];

        // Spawn multiple threads to simulate concurrent access to the same page
        for i in 0..10 {
            let bpm = bpm_clone.clone();
            let page_id = page_id;

            let handle = thread::spawn(move || {
                {
                    // First, fetch the page with a short-lived lock on the BPM
                    let page = {
                        let bpm = bpm.read(); // Use a read lock on the BPM
                        bpm.fetch_page(page_id).unwrap() // Fetch the page
                    };

                    // Then, modify the page with a write lock on the page itself
                    {
                        let mut page_guard = page.write(); // Acquire write lock on the Page
                        let data = page_guard.get_data_mut();
                        data[0] = i as u8; // Each thread writes its index as the first byte
                    }

                    // Finally, unpin the page
                    let mut bpm = bpm.write();
                    bpm.unpin_page(page_id, true, AccessType::Lookup); // Mark the page as dirty
                }
            });

            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("Thread failed");
        }

        // Verify the final state of the page
        {
            let bpm = bpm.read();
            let final_page = bpm.fetch_page(page_id).unwrap();
            let binding = final_page.read();
            let final_data = binding.get_data();
            assert!(
                final_data[0] < 10,
                "Final modification resulted in incorrect data: expected value < 10, got {}",
                final_data[0]
            );
        }

        // Clean up by unpinning the page
        bpm.write().unpin_page(page_id, false, AccessType::Lookup);
    }

    #[test]
    fn eviction_policy_test() {
        let ctx = TestContext::new("eviction_policy_test");
        let bpm = Arc::new(RwLock::new(ctx.bpm.clone()));

        // Fill the buffer pool completely
        let mut last_page_id = 0;
        for i in 0..ctx.buffer_pool_size {
            // Create a new page within a narrow lock scope
            let page_id = {
                let mut bpm = bpm.write(); // Acquire write lock
                let page = bpm.new_page();

                let page_id = page.unwrap().read().get_page_id();
                last_page_id = page_id;
                page_id
            }; // Release write lock immediately

            // Unpin the page in a separate lock scope
            bpm.write().unpin_page(page_id, false, AccessType::Lookup);
        }

        // Access the first page to make it recently used
        info!("Accessing page 0 to mark it as recently used");
        let page_0 = bpm.write().fetch_page(0);
        bpm.write().unpin_page(0, true, AccessType::Lookup);

        // Create a new page, forcing an eviction
        info!("Creating a new page to trigger eviction");
        let new_page = bpm.write().new_page();

        // Verify that the last page was evicted
        {
            let bpm = bpm.write(); // Acquire write lock
            let page_table = bpm.get_page_table(); // Acquire read lock on page_table
            let page_table = page_table.read();
            assert!(
                !page_table.contains_key(&last_page_id),
                "Last page should have been evicted"
            );
        }
    }

    #[test]
    fn repeated_fetch_and_modify_test() {
        let ctx = TestContext::new("repeated_fetch_and_modify_test");
        let bpm = Arc::new(RwLock::new(ctx.bpm.clone()));

        // Create and modify a page repeatedly
        let page = bpm.write().new_page().expect("Failed to create a new page");
        let page_id = page.read().get_page_id(); // Store page_id for later use

        for i in 0..100 {
            {
                // Fetch the page with a short-lived lock on the BufferPoolManager
                let page = {
                    let bpm = bpm.read(); // Use a read lock on the BPM to fetch the page
                    bpm.fetch_page(page_id).expect("Failed to fetch page")
                };

                // Modify the page with a write lock on the page itself
                {
                    let mut page_guard = page.write(); // Acquire write lock on the page
                    let data = page_guard.get_data_mut(); // Get a mutable reference to the data
                    data[0] = i as u8; // Modify the data
                }

                // Unpin the page
                let mut bpm = bpm.write(); // Acquire write lock to unpin the page
                bpm.unpin_page(page_id, true, AccessType::Lookup); // Mark the page as dirty
            } // The locks are dropped here to prevent deadlocks
        }

        // Verify the final modification
        {
            let bpm = bpm.read(); // Acquire read lock on BPM
            if let Some(page_guard) = bpm.fetch_page(page_id) {
                let page_guard = page_guard.read(); // Acquire read lock on the page
                let data = page_guard.get_data(); // Get a reference to the data
                assert_eq!(data[0], 99, "Final modification did not persist");
            } else {
                panic!("Failed to fetch page");
            }

            bpm.unpin_page(page_id, false, AccessType::Lookup); // Clean up by unpinning the page
        }
    }

    #[test]
    fn boundary_conditions_test() {
        let ctx = TestContext::new("boundary_conditions_test");
        let bpm = Arc::new(RwLock::new(ctx.bpm.clone()));

        // Create maximum number of pages
        for _ in 0..ctx.buffer_pool_size {
            bpm.write().new_page().expect("Failed to create a new page");
        }

        // Attempt to create more pages than the buffer pool can handle
        for _ in 0..10 {
            let new_page_result = bpm.write().new_page();
            if let Some(ref page) = new_page_result {
                bpm.write().unpin_page(page.read().get_page_id(), false, AccessType::Lookup);
                info!("Unexpectedly created page {}", page.read().get_page_id());
            }
            assert!(new_page_result.is_none(), "Should not be able to create more pages than the buffer pool size");
        }
    }
}
