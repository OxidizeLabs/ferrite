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
    #[ignore]
    fn page_id_size() {
        assert_eq!(PAGE_ID_SIZE, 4, "PageId size is not 4 bytes");
    }

    #[test]
    #[ignore]
    fn header_page_integrity() {
        let ctx = TestContext::new("header_page_integrity");
        let bpm = &ctx.bpm;

        info!("Creating ExtendedHashTableHeader page");
        let header_guard = bpm.new_page_guarded(NewPageType::ExtendedHashTableHeader).unwrap();
        info!("Created page type: {}", header_guard.get_page_type());

        match header_guard.into_specific_type::<ExtendableHTableHeaderPage>() {
            Some(mut ext_guard) => {
                info!("Successfully converted to ExtendableHTableHeaderPage");

                ext_guard.access(|page| {
                    info!("ExtendableHTableHeaderPage ID: {}", page.get_page_id());
                });

                // Initialize the header page with a max depth of 2
                ext_guard.access_mut(|page| {
                    page.init(2);
                    info!("Initialized header page with global depth: {}", page.global_depth());
                });

                // Test hashes that will produce different upper bits
                let hashes = [
                    0b00000000000000000000000000000000, // Should map to 0
                    0b01000000000000000000000000000000, // Should map to 1
                    0b10000000000000000000000000000000, // Should map to 2
                    0b11000000000000000000000000000000, // Should map to 3
                ];

                for (i, &hash) in hashes.iter().enumerate() {
                    ext_guard.access_mut(|page| {
                        let index = page.hash_to_directory_index(hash);
                        info!("Hash {:#034b} mapped to index {}", hash, index);
                        assert_eq!(index, i as u32, "Hash {:#034b} should map to index {}", hash, i);
                    });
                }
                info!("All hash to index mappings verified successfully");

                // Test with max_depth 0
                ext_guard.access_mut(|page| {
                    page.init(0);
                    for &hash in &hashes {
                        let index = page.hash_to_directory_index(hash);
                        info!("With max_depth 0, hash {:#034b} mapped to index {}", hash, index);
                        assert_eq!(index, 0, "With max_depth 0, all hashes should map to index 0");
                    }
                });
                info!("Max depth 0 test completed successfully");

                // Test with max_depth 31 (maximum allowed)
                ext_guard.access_mut(|page| {
                    page.init(31);
                    for (i, &hash) in hashes.iter().enumerate() {
                        let index = page.hash_to_directory_index(hash);
                        info!("With max_depth 31, hash {:#034b} mapped to index {}", hash, index);
                        assert_eq!(index, hash >> 1, "With max_depth 31, hash {:#034b} should map to index {}", hash, hash >> 1);
                    }
                });
                info!("Max depth 31 test completed successfully");
            }
            None => {
                panic!("Failed to convert to ExtendableHTableHeaderPage");
            }
        }

        info!("Header page test completed successfully");
    }

    #[test]
    #[ignore]
    fn directory_page_integrity() {
        let ctx = TestContext::new("directory_page_integrity");
        let bpm = &ctx.bpm;

        // DIRECTORY PAGE TEST
        let header_guard = bpm.new_page_guarded(NewPageType::ExtendedHashTableHeader).unwrap();

        // DIRECTORY PAGE TEST
        let directory_guard = bpm.new_page_guarded(NewPageType::ExtendedHashTableDirectory).unwrap();
        match directory_guard.into_specific_type::<ExtendableHTableDirectoryPage>() {
            Some(mut ext_guard) => {
                info!("Successfully converted to ExtendableHTableDirectoryPage");

                ext_guard.access(|page| {
                    info!("ExtendableHTableDirectoryPage ID: {}", page.get_page_id());
                });

                ext_guard.access_mut(|page| {
                    page.init(3);
                    info!("Initialized directory page with global depth: {}", page.get_global_depth());
                });
            }
            None => {
                error!("Failed to convert to ExtendableHTableDirectoryPage");
                panic!("Conversion to ExtendableHTableDirectoryPage failed");
            }
        }
    }

    #[test]
    #[ignore]
    fn bucket_page_integrity() {
        let ctx = TestContext::new("bucket_page_integrity");
        let bpm = &ctx.bpm;

        let bucket_guard = match bpm.new_page_guarded(NewPageType::ExtendedHashTableBucket) {
            Some(guard) => guard,
            None => {
                error!("Failed to create new bucket page");
                panic!("Failed to create new bucket page");
            }
        };
        let bucket_page_id = bucket_guard.get_page_id();
        match bucket_guard.into_specific_type::<ExtendableHTableBucketPage<8>>() {
            Some(mut bucket_guard_1) => {
                bucket_guard_1.access_mut(|page| {
                    page.init(10);
                    info!("Initialized bucket page with max size: {}", page.get_size());
                });
            }
            None => {
                error!("Failed to convert to ExtendableHTableBucketPage<8>");
                // Print more information about the page
                error!("Page ID: {}", bucket_page_id);
                // error!("Page type: {}", bucket_guard_1.get_page_type());
                panic!("Conversion to ExtendableHTableBucketPage<8> failed");
            }
        }
    }

    #[test]
    fn test_basic_behaviour() {
        let ctx = TestContext::new("test_basic_behaviour");
        let bpm = &ctx.bpm;

        info!("Starting basic behaviour test");

        let mut bucket_page_ids = [INVALID_PAGE_ID; 4];

        // Create directory page
        let directory_guard = bpm.new_page_guarded(NewPageType::ExtendedHashTableDirectory).unwrap();
        let directory_page_id = directory_guard.get_page_id();
        info!("Created directory page with ID: {}", directory_page_id);

        if let Some(mut ext_dir_guard) = directory_guard.into_specific_type::<ExtendableHTableDirectoryPage>() {
            ext_dir_guard.access_mut(|directory_page| {
                directory_page.init(3);
                info!("Initialized directory page with global depth: {}", directory_page.get_global_depth());

                // Create bucket pages
                for i in 0..4 {
                    let bucket_guard = bpm.new_page_guarded(NewPageType::ExtendedHashTableBucket).unwrap();
                    bucket_page_ids[i] = bucket_guard.get_page_id();
                    info!("Created bucket page {} with ID: {}", i, bucket_page_ids[i]);

                    if let Some(mut bucket_guard) = bucket_guard.into_specific_type::<ExtendableHTableBucketPage<8>>() {
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
                assert_eq!(directory_page.size(), 1);
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
                assert_eq!(directory_page.size(), 2);
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
                assert_eq!(directory_page.size(), 4);
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
                assert_eq!(directory_page.size(), 8);
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
                assert_eq!(directory_page.size(), 4);
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
//     use super::*;
//
//     #[test]
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
//                     if let Some(mut bucket_guard) = bucket_guard.into_specific_type::<ExtendableHTableBucketPage<8>>() {
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
//
// #[cfg(test)]
// mod edge_case {
//     use super::*;
//
//     #[test]
//     fn test_max_depth() {
//         let ctx = TestContext::new("test_max_depth");
//         let bpm = &ctx.bpm;
//
//         info!("Starting max depth test");
//
//         let directory_guard = bpm.new_page_guarded(NewPageType::ExtendedHashTableDirectory).unwrap();
//         let directory_page_id = directory_guard.get_page_id();
//         info!("Created directory page with ID: {}", directory_page_id);
//
//         if let Some(mut ext_dir_guard) = directory_guard.into_specific_type::<ExtendableHTableDirectoryPage>() {
//             ext_dir_guard.access_mut(|directory_page| {
//                 directory_page.init(30); // Start with a high global depth
//                 assert_eq!(directory_page.get_global_depth(), 30, "Initial global depth should be 30");
//
//                 // Try to increment beyond the maximum depth
//                 directory_page.incr_global_depth();
//                 assert_eq!(directory_page.get_global_depth(), 31, "Global depth should be 31 after increment");
//
//                 // This should not increase the global depth further
//                 directory_page.incr_global_depth();
//                 assert_eq!(directory_page.get_global_depth(), 31, "Global depth should remain 31 after attempting to exceed max depth");
//
//                 info!("Max depth test completed successfully");
//             });
//         } else {
//             panic!("Failed to convert to ExtendableHTableDirectoryPage");
//         }
//     }
//
//     #[test]
//     fn test_empty_directory() {
//         let ctx = TestContext::new("test_empty_directory");
//         let bpm = &ctx.bpm;
//
//         info!("Starting empty directory test");
//
//         let directory_guard = bpm.new_page_guarded(NewPageType::ExtendedHashTableDirectory).unwrap();
//         let directory_page_id = directory_guard.get_page_id();
//         info!("Created directory page with ID: {}", directory_page_id);
//
//         if let Some(mut ext_dir_guard) = directory_guard.into_specific_type::<ExtendableHTableDirectoryPage>() {
//             ext_dir_guard.access_mut(|directory_page| {
//                 directory_page.init(0); // Start with global depth 0
//                 assert_eq!(directory_page.get_global_depth(), 0, "Initial global depth should be 0");
//                 assert_eq!(directory_page.size(), 1, "Empty directory should have size 1");
//
//                 // Try to access a bucket
//                 assert_eq!(directory_page.get_bucket_page_id(0), Some(INVALID_PAGE_ID), "Empty directory should return INVALID_PAGE_ID");
//
//                 // Try to decrement global depth (should not change)
//                 directory_page.decr_global_depth();
//                 assert_eq!(directory_page.get_global_depth(), 0, "Global depth should remain 0 after attempted decrement");
//
//                 info!("Empty directory test completed successfully");
//             });
//         } else {
//             panic!("Failed to convert to ExtendableHTableDirectoryPage");
//         }
//     }
// }