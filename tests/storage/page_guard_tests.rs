use crate::test_setup::initialize_logger;
use chrono::Utc;
use spin::RwLock;
use std::sync::Arc;
use tkdb::buffer::buffer_pool_manager::BufferPoolManager;
use tkdb::buffer::buffer_pool_manager::NewPageType;
use tkdb::buffer::lru_k_replacer::LRUKReplacer;
use tkdb::storage::disk::disk_manager::FileDiskManager;
use tkdb::storage::disk::disk_scheduler::DiskScheduler;
use tkdb::storage::page::page::PageTrait;
use tkdb::storage::page::page_guard::PageGuard;
use tkdb::storage::page::page_types::extendable_hash_table_directory_page::ExtendableHTableDirectoryPage;

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

        let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
        let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
        let db_log_file = format!("tests/data/{}_{}.log", test_name, timestamp);

        let disk_manager = Arc::new(FileDiskManager::new(db_file.clone(), db_log_file.clone(), 100));
        let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
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
mod unit_tests {
    use super::*;

    #[test]
    fn basic_page_guard() {
        let ctx = TestContext::new("basic_page_guard");
        let bpm = Arc::clone(&ctx.bpm);

        // Create a new guarded page with bpm
        let page0 = bpm.new_page(NewPageType::Basic).unwrap();
        let basic_guard = PageGuard::new(Arc::clone(&bpm), Arc::clone(&page0), page0.read().as_page_trait().get_page_id());

        // Ensure page0 is pinned and the guard works as expected
        assert_eq!(page0.read().as_page_trait().get_pin_count(), 1); // Pin count should be 1 after creation
        assert_eq!(page0.read().as_page_trait().get_page_id(), basic_guard.get_page_id());

        // Ensure dropping the guard decreases the pin count
        drop(basic_guard);
        assert_eq!(page0.read().as_page_trait().get_pin_count(), 0, "Pin count should be 0 after dropping the WritePageGuard");
    }

    #[test]
    fn convert_into() {
        let ctx = TestContext::new("basic_page_guard");
        let bpm = Arc::clone(&ctx.bpm);

        // Create a new guarded page with bpm
        let page0 = bpm.new_page(NewPageType::Basic).unwrap();
        let page_guard = PageGuard::new(Arc::clone(&bpm), Arc::clone(&page0), page0.read().as_page_trait().get_page_id());

        if let Some(ext_guard) = page_guard.into_specific_type::<ExtendableHTableDirectoryPage>(/* fn(ExtendableHTableHeaderPage) -> PageType */) {
            let read_guard = ext_guard.read();
            read_guard.access(|page| {
                let directory_size = page.size();
                println!("Directory size: {}", directory_size);
            });

            let mut write_guard = ext_guard.write();
            write_guard.access_mut(|page| {
                page.set_dirty(true);
            });
        }
    }
}

#[cfg(test)]
mod basic_behaviour {
    use super::*;

    #[test]
    fn create_and_drop() {
        let ctx = TestContext::new("create_and_drop");
        let bpm = Arc::clone(&ctx.bpm);

        // Create a new guarded page with bpm
        let page0 = bpm.new_page(NewPageType::Basic).unwrap();
        let page_id = page0.read().as_page_trait().get_page_id();
        let basic_guarded_page = PageGuard::new(Arc::clone(&bpm), Arc::clone(&page0), page_id);

        // Ensure page0 is pinned and the guard works as expected
        assert_eq!(page0.read().as_page_trait().get_pin_count(), 1); // Pin count should be 1 after creation
        assert_eq!(page_id, basic_guarded_page.get_page_id());

        // Create another page and guard it with ReadPageGuard
        let page2 = bpm.new_page(NewPageType::Basic).unwrap();
        let page2_id = page2.read().as_page_trait().get_page_id();
        let read_guard = PageGuard::new(Arc::clone(&bpm), Arc::clone(&page2), page2_id);
        assert_eq!(page2.read().as_page_trait().get_pin_count(), 1); // Pin count should be 1 after creating the guard

        // Ensure dropping the guard decreases the pin count
        drop(read_guard);
        assert_eq!(page2.read().as_page_trait().get_pin_count(), 0); // Pin count should be 0 after dropping the guard

        // Test WritePageGuard functionality
        let page3 = bpm.new_page(NewPageType::Basic).unwrap();
        let page3_id = page3.read().as_page_trait().get_page_id();

        {
            let write_guard = PageGuard::new(Arc::clone(&bpm), Arc::clone(&page3), page3_id);
            assert_eq!(page3.read().as_page_trait().get_pin_count(), 1); // Pin count should be 1 after creating WritePageGuard
        }

        // Ensure pin count is reduced after dropping WritePageGuard
        assert_eq!(page3.read().as_page_trait().get_pin_count(), 0);
    }

    #[test]
    fn modify_data() {
        let ctx = TestContext::new("modify_data");
        let bpm = Arc::clone(&ctx.bpm);

        // Create a new guarded page with bpm
        let page0 = bpm.new_page(NewPageType::Basic).unwrap();
        let page0_id = page0.read().as_page_trait().get_page_id();
        {
            let basic_guard = PageGuard::new(Arc::clone(&bpm), Arc::clone(&page0), page0_id);

            // Ensure page0 is pinned and the guard works as expected
            assert_eq!(page0.read().as_page_trait().get_pin_count(), 1); // Pin count should be 1 after creation
            assert_eq!(page0.read().as_page_trait().get_page_id(), basic_guard.get_page_id());


            // Directly modify data in the page
            let mut write_guard = basic_guard.write();
            let data_mut = write_guard.as_page_trait_mut().get_data_mut();

            // Example of directly setting bytes (this could represent any structure)
            data_mut[0] = 3; // Equivalent to setting a 'depth' field
            data_mut[4..8].copy_from_slice(&42u32.to_ne_bytes()); // Example of setting a 'bucket_page_id'

            // Read back the data and verify it
            assert_eq!(data_mut[0], 3, "Depth should be set to 3");
            let bucket_page_id = u32::from_ne_bytes(data_mut[4..8].try_into().unwrap());
            assert_eq!(bucket_page_id, 42, "Bucket page ID should be set to 42");
        }
        // Ensure dropping the guard decreases the pin count

        assert_eq!(page0.read().as_page_trait().get_pin_count(), 0, "Pin count should be 0 after dropping the WritePageGuard");
    }
}

// #[cfg(test)]
// mod concurrency {
//     use super::*;
//
//     #[test]
//     fn reads() {
//         let ctx = TestContext::new("concurrent_reads");
//         let bpm = Arc::clone(&ctx.bpm);
//
//         // Create a shared page
//         let page = Arc::new(bpm.new_page().unwrap());
//
//         // Spawn multiple reader threads
//         let mut threads = Vec::new();
//         for _ in 0..3 {
//             let bpm_clone = Arc::clone(&bpm);
//             let page_clone = Arc::clone(&page);
//
//             threads.push(thread::spawn(move || {
//
//                 // Acquire read guard
//                 {
//                     let read_guard = ReadPageGuard::new(Arc::clone(&bpm_clone), Arc::clone(&page_clone));
//                     let data = read_guard.get_data();
//
//                     // Simulate a read operation by checking if the page ID matches
//                     assert_eq!(read_guard.get_page_id().unwrap(), page_clone.read().get_page_id());
//                     assert_eq!(data[0], 0); // Expecting initial byte value
//                 } // Lock released here when the guard goes out of scope
//
//             }));
//         }
//
//         // Wait for all reader threads to finish
//         for thread in threads {
//             thread.join().unwrap();
//         }
//     }
//
//     #[test]
//     fn modify_data_concurrently() {
//         use std::sync::{Arc, Barrier, RwLock};
//         use std::thread;
//
//         let ctx = TestContext::new("modify_data_concurrently");
//         let bpm = Arc::clone(&ctx.bpm);
//
//         // Create a new guarded page with bpm
//         let page0 = Arc::new(bpm.new_page().unwrap());
//
//         // Barrier to synchronize the start of all threads
//         let barrier = Arc::new(Barrier::new(3)); // 2 worker threads + main thread
//
//         let mut threads = Vec::new();
//
//         for i in 0..2 {
//             let bpm_clone = Arc::clone(&bpm);
//             let page0_clone = Arc::clone(&page0);
//             let barrier_clone = Arc::clone(&barrier);
//
//             threads.push(thread::spawn(move || {
//                 // Wait until all threads are ready
//                 barrier_clone.wait();
//
//                 // Create a basic guard
//                 let basic_guard = BasicPageGuard::new(Arc::clone(&bpm_clone), Arc::clone(&page0_clone));
//
//                 // Upgrade to WritePageGuard
//                 let mut write_guard = basic_guard.upgrade_write();
//
//                 // Perform different modifications in each thread
//                 let mut data_mut = write_guard.get_data_mut();
//                 data_mut[i * 4] = (i + 1) as u8; // Each thread writes at a different index
//                 data_mut[4 + i * 4..8 + i * 4].copy_from_slice(&(i as u32).to_ne_bytes());
//
//                 // Verify that the written data is correct for the current thread
//                 assert_eq!(data_mut[i * 4], (i + 1) as u8, "Value should match the thread index");
//                 let expected_value = u32::from_ne_bytes(data_mut[4 + i * 4..8 + i * 4].try_into().unwrap());
//                 assert_eq!(expected_value, i as u32, "Bucket page ID should match the thread index");
//
//                 // Drop the write guard to release the lock
//             }));
//         }
//
//         barrier.wait(); // Synchronize all threads
//
//         // Wait for all threads to finish
//         for thread in threads {
//             thread.join().unwrap();
//         }
//
//         // Final check after concurrent modifications
//         let final_guard = BasicPageGuard::new(Arc::clone(&bpm), Arc::clone(&page0));
//         let data = final_guard.get_data();
//
//         // Verify the data written by each thread
//         for i in 0..2 {
//             assert_eq!(data[i * 4], (i + 1) as u8, "Expected value not found at index {}", i * 4);
//             let bucket_page_id = u32::from_ne_bytes(data[4 + i * 4..8 + i * 4].try_into().unwrap());
//             assert_eq!(bucket_page_id, i as u32, "Expected bucket page ID not found for index {}", i);
//         }
//
//         // Ensure the pin count is zero after all threads are done
//         assert_eq!(page0.read().get_pin_count(), 0, "Pin count should be 0 after all guards are dropped");
//     }
//
//     #[test]
//     fn reads_and_writes() {
//         let ctx = TestContext::new("concurrent_reads_and_writes");
//         let bpm = Arc::clone(&ctx.bpm);
//
//         // Create a shared page
//         let page = bpm.new_page().unwrap();
//
//         // Spawn multiple threads for concurrent reads and writes
//         let mut threads = Vec::new();
//
//         // Writer threads
//         for i in 0..2 {
//             let bpm_clone = Arc::clone(&bpm);
//             let page_clone = Arc::clone(&page);
//
//             threads.push(thread::spawn(move || {
//                 // Perform the write operation
//                 let mut write_guard = WritePageGuard::new(Arc::clone(&bpm_clone), Arc::clone(&page_clone));
//                 let mut data_mut = write_guard.get_data_mut();
//
//                 // Each writer thread writes a value
//                 data_mut[i] = (i + 1) as u8;
//             }));
//         }
//
//         // Reader threads
//         for i in 0..2 {
//             let bpm_clone = Arc::clone(&bpm);
//             let page_clone = Arc::clone(&page);
//
//             threads.push(thread::spawn(move || {
//                 // Perform the read operation
//                 let read_guard = ReadPageGuard::new(Arc::clone(&bpm_clone), Arc::clone(&page_clone));
//                 let data = read_guard.get_data();
//
//                 // Simulate a read operation by checking if the page ID matches
//                 assert_eq!(read_guard.get_page_id().unwrap(), page_clone.read().get_page_id());
//                 // Reading values written by the writer threads
//                 assert_eq!(data[i], (i + 1) as u8, "Unexpected values in the data");
//             }));
//         }
//
//         // Wait for all threads to finish
//         for thread in threads {
//             thread.join().unwrap();
//         }
//
//         // Verify the page's content after concurrent reads and writes
//         let final_guard = WritePageGuard::new(Arc::clone(&bpm), Arc::clone(&page));
//         let data = final_guard.get_data();
//
//         assert!(data[0] <= 2 && data[1] <= 2, "Unexpected values in the data after concurrent operations");
//     }
// }
//
// #[cfg(test)]
// mod edge_cases {
//     use super::*;
//
//     #[test]
//     fn page_eviction_under_pressure() {
//         let ctx = TestContext::new("page_eviction_under_pressure");
//         let bpm = Arc::clone(&ctx.bpm);
//
//         // Fill the buffer pool to force eviction
//         let mut pages = Vec::new();
//         for _ in 0..5 {
//             let page = bpm.new_page().unwrap();
//             pages.push(page);
//         }
//
//         let page0_id;
//         {
//             // Access the first page and hold onto it
//             let page0 = Arc::clone(&pages[0]);
//             page0_id = page0.read().get_page_id(); // Store page ID before releasing the guard
//
//             // Keep the first page pinned
//             let guard0 = WritePageGuard::new(Arc::clone(&bpm), Arc::clone(&page0));
//             // Perform the assertion to ensure the page is accessible
//             assert_eq!(guard0.get_page_id().unwrap(), page0_id);
//         }
//
//         // Now create a new page that should cause an eviction due to the buffer pool size limit
//         let page_evicted = bpm.new_page().unwrap_or_else(|| panic!("Failed to create a new page, eviction didn't work as expected"));
//         let evict_guard = WritePageGuard::new(Arc::clone(&bpm), Arc::clone(&page_evicted));
//
//         // Ensure the first page was not evicted by checking the page ID directly
//         assert_eq!(pages[0].read().get_page_id(), page0_id, "The first page should not be evicted");
//
//         // Test if the evicted page is correctly unpinned
//         drop(evict_guard);
//         assert_eq!(page_evicted.read().get_pin_count(), 0, "Evicted page should be unpinned");
//
//         // Verify the replacer can now evict this page since it should be marked as evictable
//         // assert!(bpm.unpin_page(page0_id, false, AccessType::Unknown), "Page should be unpinned and made evictable");
//     }
//
//     #[test]
//     fn invalid_page_access_after_drop() {
//         let ctx = TestContext::new("invalid_page_access_after_drop");
//         let bpm = Arc::clone(&ctx.bpm);
//
//         // Create a page and drop the guard
//         let page = bpm.new_page().unwrap();
//         let guard = WritePageGuard::new(Arc::clone(&bpm), Arc::clone(&page));
//         drop(guard);
//
//         // Attempt to re-access the page after it’s been dropped
//         let result = bpm.fetch_page_write(page.read().get_page_id());
//         assert!(result.is_some(), "Accessing a dropped page should return an error");
//     }
// }

