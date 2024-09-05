use crate::test_setup::initialize_logger;
use chrono::Utc;
use env_logger;
use log::{error, info};
use spin::RwLock;
use std::mem::size_of;
use std::sync::Arc;
use std::thread;
use tkdb::buffer::buffer_pool_manager::BufferPoolManager;
use tkdb::buffer::buffer_pool_manager::NewPageType;
use tkdb::buffer::lru_k_replacer::LRUKReplacer;
use tkdb::common::config::{PageId, INVALID_PAGE_ID};
use tkdb::storage::disk::disk_manager::FileDiskManager;
use tkdb::storage::disk::disk_scheduler::DiskScheduler;
use tkdb::storage::page::page::PageTrait;
use tkdb::storage::page::page_types::extendable_hash_table_bucket_page::ExtendableHTableBucketPage;
use tkdb::storage::page::page_types::extendable_hash_table_directory_page::ExtendableHTableDirectoryPage;
use tkdb::storage::page::page_types::extendable_hash_table_header_page::ExtendableHTableHeaderPage;
use tkdb::types_db::integer_type::IntegerType;

const PAGE_ID_SIZE: usize = size_of::<PageId>();

struct TestContext {
    bpm: Arc<BufferPoolManager>,
    db_file: String,
    db_log_file: String,
    buffer_pool_size: usize,
}

impl TestContext {
    fn new(test_name: &str) -> Self {
        initialize_logger();
        let buffer_pool_size: usize = 5;
        const K: usize = 2;
        let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
        let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
        let db_log_file = format!("tests/data/{}_{}.log", test_name, timestamp);
        let disk_manager =
            Arc::new(FileDiskManager::new(db_file.clone(), db_log_file.clone(), 100));
        let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(
            &disk_manager,
        ))));
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(buffer_pool_size, K)));
        let bpm = Arc::new((BufferPoolManager::new(
            buffer_pool_size,
            disk_scheduler,
            disk_manager.clone(),
            replacer.clone(),
        )));
        Self {
            bpm,
            db_file,
            db_log_file,
            buffer_pool_size,
        }
    }

    fn cleanup(&self) {
        let _ = std::fs::remove_file(&self.db_file);
        let _ = std::fs::remove_file(&self.db_log_file);
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        self.cleanup()
    }
}

#[cfg(test)]
mod unit_tests {}

#[cfg(test)]
mod basic_behavior {
    use super::*;

    #[test]
    fn page_id_size() {
        assert_eq!(PAGE_ID_SIZE, 4, "PageId size is not 4 bytes");
    }

    #[test]
    fn basic_behaviour() {
        let ctx = TestContext::new("test_basic_behaviour");
        let bpm = &ctx.bpm;

        info!("Starting basic behaviour test");

        let mut bucket_page_ids = [INVALID_PAGE_ID; 4];

        // Create directory page
        let directory_guard = bpm.new_page_guarded(NewPageType::ExtendedHashTableDirectory).unwrap();
        let directory_page_id = directory_guard.get_page_id();
        info!("Created directory page with ID: {}", directory_page_id);

        if let Some(mut ext_dir_guard) = directory_guard.into_specific_type::<ExtendableHTableDirectoryPage, 8>() {
            ext_dir_guard.access_mut(|directory_page| {
                directory_page.init(3);
                info!("Initialized directory page with global depth: {}", directory_page.get_global_depth());

                // Create bucket pages
                for i in 0..4 {
                    let bucket_guard = bpm.new_page_guarded(NewPageType::ExtendedHashTableBucket).unwrap();
                    bucket_page_ids[i] = bucket_guard.get_page_id();
                    info!("Created bucket page {} with ID: {}", i, bucket_page_ids[i]);

                    if let Some(mut bucket_guard) = bucket_guard.into_specific_type::<ExtendableHTableBucketPage<IntegerType, 8>, 8>() {
                        bucket_guard.access_mut(|bucket_page| {
                            bucket_page.init(10);
                            info!("Initialized bucket page {} with max size: {}", i, bucket_page.get_size());
                        });
                    } else {
                        panic!("Failed to convert to ExtendableHTableBucketPage<8>");
                    }
                }

                directory_page.set_bucket_page_id(0, bucket_page_ids[0]);

                /*
                ======== DIRECTORY (global_depth_: 0) ========
                | bucket_idx | page_id | local_depth |
                |    0       |    1    |    0        |
                ================ END DIRECTORY ================
                */
                directory_page.print_directory();
                directory_page.verify_integrity();
                assert_eq!(directory_page.get_size(), 1);
                assert_eq!(directory_page.get_bucket_page_id(0), Some(bucket_page_ids[0]));

                // grow the directory, local depths should change!
                directory_page.set_local_depth(0, 1);
                directory_page.incr_global_depth();
                directory_page.set_bucket_page_id(1, bucket_page_ids[1]);
                directory_page.set_local_depth(1, 1);

                /*
                ======== DIRECTORY (global_depth_: 1) ========
                | bucket_idx | page_id | local_depth |
                |    0       |    1    |    1        |
                |    1       |    2    |    1        |
                ================ END DIRECTORY ================
                */

                directory_page.print_directory();
                directory_page.verify_integrity();
                assert_eq!(directory_page.get_size(), 2);
                assert_eq!(directory_page.get_bucket_page_id(0), Some(bucket_page_ids[0]));
                assert_eq!(directory_page.get_bucket_page_id(1), Some(bucket_page_ids[1]));

                for i in 0..100 {
                    assert_eq!(directory_page.hash_to_bucket_index(i), i % 2);
                }

                directory_page.set_local_depth(0, 2);
                directory_page.incr_global_depth();
                directory_page.set_bucket_page_id(2, bucket_page_ids[2]);

                /*
                ======== DIRECTORY (global_depth_: 2) ========
                | bucket_idx | page_id | local_depth |
                |    0       |    1    |    2        |
                |    1       |    2    |    1        |
                |    2       |    3    |    1        |
                |    3       |    2    |    1        |
                ================ END DIRECTORY ================
                */

                directory_page.print_directory();
                directory_page.verify_integrity();
                assert_eq!(directory_page.get_size(), 4);
                assert_eq!(directory_page.get_bucket_page_id(0), Some(bucket_page_ids[0]));
                assert_eq!(directory_page.get_bucket_page_id(1), Some(bucket_page_ids[1]));
                assert_eq!(directory_page.get_bucket_page_id(2), Some(bucket_page_ids[2]));
                assert_eq!(directory_page.get_bucket_page_id(3), Some(bucket_page_ids[1]));

                for i in 0..100 {
                    assert_eq!(directory_page.hash_to_bucket_index(i), i % 4);
                }

                directory_page.set_local_depth(0, 3);
                directory_page.incr_global_depth();
                directory_page.set_bucket_page_id(4, bucket_page_ids[3]);

                /*
                ======== DIRECTORY (global_depth_: 3) ========
                | bucket_idx | page_id | local_depth |
                |    0       |    1    |    3        |
                |    1       |    2    |    1        |
                |    2       |    3    |    2        |
                |    3       |    2    |    1        |
                |    4       |    4    |    3        |
                |    5       |    2    |    1        |
                |    6       |    3    |    2        |
                |    7       |    2    |    1        |
                ================ END DIRECTORY ================
                */

                directory_page.print_directory();
                directory_page.verify_integrity();
                assert_eq!(directory_page.get_size(), 8);
                assert_eq!(directory_page.get_bucket_page_id(0), Some(bucket_page_ids[0]));
                assert_eq!(directory_page.get_bucket_page_id(1), Some(bucket_page_ids[1]));
                assert_eq!(directory_page.get_bucket_page_id(2), Some(bucket_page_ids[2]));
                assert_eq!(directory_page.get_bucket_page_id(3), Some(bucket_page_ids[1]));
                assert_eq!(directory_page.get_bucket_page_id(4), Some(bucket_page_ids[3]));
                assert_eq!(directory_page.get_bucket_page_id(5), Some(bucket_page_ids[1]));
                assert_eq!(directory_page.get_bucket_page_id(6), Some(bucket_page_ids[2]));
                assert_eq!(directory_page.get_bucket_page_id(7), Some(bucket_page_ids[1]));

                for i in 0..100 {
                    assert_eq!(directory_page.hash_to_bucket_index(i), i % 8);
                }

                // uncommenting this code line below should cause an "Assertion failed"
                // since this would be exceeding the max depth we initialized
                // directory_page.incr_global_depth();

                // at this time, we cannot shrink the directory since we have ld = gd = 3
                directory_page.print_directory();
                assert_eq!(directory_page.can_shrink(), false);

                directory_page.set_local_depth(0, 2);
                directory_page.set_local_depth(4, 2);
                directory_page.set_bucket_page_id(0, bucket_page_ids[3]);

                /*
                ======== DIRECTORY (global_depth_: 3) ========
                | bucket_idx | page_id | local_depth |
                |    0       |    4    |    2        |
                |    1       |    2    |    1        |
                |    2       |    3    |    2        |
                |    3       |    2    |    1        |
                |    4       |    4    |    2        |
                |    5       |    2    |    1        |
                |    6       |    3    |    2        |
                |    7       |    2    |    1        |
                ================ END DIRECTORY ================
                */

                directory_page.print_directory();
                assert_eq!(directory_page.can_shrink(), true);
                directory_page.decr_global_depth();

                /*
                ======== DIRECTORY (global_depth_: 2) ========
                | bucket_idx | page_id | local_depth |
                |    0       |    4    |    2        |
                |    1       |    2    |    1        |
                |    2       |    3    |    2        |
                |    3       |    2    |    1        |
                ================ END DIRECTORY ================
                */

                directory_page.print_directory();
                directory_page.verify_integrity();
                assert_eq!(directory_page.get_size(), 4);
                assert_eq!(directory_page.can_shrink(), false);

                info!("Basic behaviour test completed successfully");
            });
        } else {
            panic!("Failed to convert to ExtendableHTableDirectoryPage");
        }
    }
}

// #[cfg(test)]
// mod concurrency {
//     use tkdb::types_db::integer_type::IntegerType;
//     use super::*;
//
//     #[test]
//     #[ignore]
//     fn concurrent_access() {
//         let ctx = Arc::new(TestContext::new("test_concurrent_access"));
//         let bpm = Arc::clone(&ctx.bpm);
//
//         info!("Starting concurrent access test");
//
//         let num_threads = 4;
//         let operations_per_thread = 100;
//
//         let mut handles = vec![];
//
//         for i in 0..num_threads {
//             let bpm_clone = Arc::clone(&bpm);
//             let handle = thread::spawn(move || {
//                 for j in 0..operations_per_thread {
//                     let bucket_guard = bpm_clone.new_page_guarded(NewPageType::ExtendedHashTableBucket).unwrap();
//                     let bucket_page_id = bucket_guard.get_page_id();
//                     info!("Thread {} created bucket page with ID: {} (operation {})", i, bucket_page_id, j);
//
//                     if let Some(mut bucket_guard) = bucket_guard.into_specific_type::<ExtendableHTableBucketPage<IntegerType, 8>, 8>() {
//                         bucket_guard.access_mut(|bucket_page| {
//                             bucket_page.init(10);
//                             assert_eq!(bucket_page.get_size(), 0, "Initial bucket size should be 0");
//                             info!("Thread {} initialized bucket page {} (operation {})", i, bucket_page_id, j);
//                         });
//                     } else {
//                         panic!("Thread {} failed to convert to ExtendableHTableBucketPage<8> (operation {})", i, j);
//                     }
//                 }
//             });
//             handles.push(handle);
//         }
//
//         for handle in handles {
//             handle.join().unwrap();
//         }
//
//         info!("Concurrent access test completed successfully");
//     }
// }
