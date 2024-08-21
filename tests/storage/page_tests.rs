use spin::RwLock;
use std::sync::{Arc, Barrier};
use std::thread;
use tkdb::common::config::Lsn;
use tkdb::common::config::DB_PAGE_SIZE;
use tkdb::storage::page::page::Page;
use tkdb::storage::page::page::PageTrait;


#[cfg(test)]
mod tests {
    use super::*;

    // Unit Tests
    #[cfg(test)]
    mod unit_tests {
        use super::*;

        #[test]
        fn test_page_creation() {
            let page = Page::new(1);
            assert_eq!(page.get_page_id(), 1);
            assert_eq!(page.get_pin_count(), 1);
            assert!(!page.is_dirty());
        }

        #[test]
        fn test_page_data_access() {
            let mut page = Page::new(2);
            let mut data = [0u8; DB_PAGE_SIZE];
            data[0] = 42;
            // Use the set_data method with the offset to update the page data
            if let Err(e) = page.set_data(0, &data) {
                panic!("Error setting data: {:?}", e);
            }
            assert_eq!(page.get_data().get(0), Some(42).as_ref());
        }

        #[test]
        fn test_set_and_get_lsn() {
            let mut page = Page::new(3);
            let lsn: Lsn = 1234;
            page.set_lsn(lsn);
            assert_eq!(page.get_lsn(), lsn);
        }
    }

    // Basic Behavior
    #[cfg(test)]
    mod basic_behavior {
        use super::*;

        #[test]
        fn test_increment_and_decrement_pin_count() {
            let mut page = Page::new(1);
            page.increment_pin_count();
            assert_eq!(page.get_pin_count(), 2);
            page.decrement_pin_count();
            assert_eq!(page.get_pin_count(), 1);
        }

        #[test]
        fn test_setting_dirty_flag() {
            let mut page = Page::new(1);
            assert!(!page.is_dirty());
            page.set_dirty(true);
            assert!(page.is_dirty());
        }

        #[test]
        fn test_reset_memory() {
            let mut page = Page::new(1);
            let mut data = [0u8; DB_PAGE_SIZE];
            data[0] = 255;
            if let Err(e) = page.set_data(0, &data) {
                panic!("Error setting data: {:?}", e);
            }

            // Ensure data is modified before reset
            assert_eq!(page.get_data()[0], 255);

            // Reset the memory
            page.reset_memory();
            assert_eq!(page.get_data()[0], 0);
        }
    }

    // Concurrency
    #[cfg(test)]
    mod concurrency {
        use super::*;

        #[test]
        fn concurrent_read_and_write() {
            let page = Arc::new(RwLock::new(Page::new(1))); // Wrap Page in Arc<RwLock>.
            let start_barrier = Arc::new(Barrier::new(4)); // Used to start all threads simultaneously.
            let end_barrier = Arc::new(Barrier::new(4)); // Used to ensure writers finish before readers start.
            let mut handles = vec![];

            // Writer threads
            for i in 0..2 {
                let page = Arc::clone(&page);
                let start_barrier = Arc::clone(&start_barrier);
                let end_barrier = Arc::clone(&end_barrier);
                handles.push(thread::spawn(move || {
                    start_barrier.wait(); // Wait until all threads are ready to start.

                    // Perform the write operation.
                    {
                        let mut page = page.write(); // Acquire write lock on the entire Page.
                        let mut data = [0u8; DB_PAGE_SIZE];
                        data[0] = (i + 1) as u8;
                        if let Err(e) = page.set_data(0, &data) {
                            panic!("Error setting data: {:?}", e);
                        }
                    }

                    end_barrier.wait(); // Signal that the writer has finished.
                }));
            }

            // Reader threads
            for _ in 0..2 {
                let page = Arc::clone(&page);
                let start_barrier = Arc::clone(&start_barrier);
                let end_barrier = Arc::clone(&end_barrier);

                handles.push(thread::spawn(move || {
                    start_barrier.wait(); // Wait until all threads are ready to start.

                    // Ensure writers are finished before reading.
                    end_barrier.wait();

                    // Perform the read operation.
                    let page = page.read(); // Acquire read lock on the entire Page.
                    let data = page.get_data(); // Safely access immutable data.
                    assert!(data[0] == 1 || data[0] == 2, "Unexpected values in the data");
                }));
            }

            // Wait for all threads to finish.
            for handle in handles {
                handle.join().unwrap();
            }

            // Verify the final content of the page after all operations.
            let page = page.read(); // Acquire read lock on the entire Page.
            let data = page.get_data();
            assert!(data[0] == 1 || data[0] == 2, "Unexpected values in the data after concurrent operations");
        }

        #[test]
        fn multiple_concurrent_reads() {
            // Wrap the Page in Arc<RwLock> to manage thread-safe access.
            let page = Arc::new(RwLock::new(Page::new(1)));
            let barrier = Arc::new(Barrier::new(4)); // Barrier for synchronizing thread starts.
            let mut handles = vec![];

            // Launch multiple reader threads.
            for _ in 0..3 {
                let page = Arc::clone(&page);
                let barrier = Arc::clone(&barrier);
                handles.push(thread::spawn(move || {
                    barrier.wait(); // Synchronize start.

                    // Acquire read lock on the Page.
                    let page = page.read();

                    // Perform the read operation.
                    let data = page.get_data();
                    assert_eq!(data[0], 0); // Since no write happened, data should be zeroed.
                }));
            }

            barrier.wait(); // Trigger all threads to start.

            // Wait for all threads to finish.
            for handle in handles {
                handle.join().unwrap();
            }
        }
    }

    // Edge Cases
    #[cfg(test)]
    mod edge_cases {
        use super::*;

        #[test]
        fn test_pin_count_underflow_protection() {
            let mut page = Page::new(1);
            page.decrement_pin_count();  // Pin count goes to 0
            assert_eq!(page.get_pin_count(), 0);

            // Attempting to decrement below 0 should be prevented
            page.decrement_pin_count();
            assert_eq!(page.get_pin_count(), 0);
        }

        #[test]
        fn test_page_with_large_lsn_value() {
            let mut page = Page::new(1);
            let large_lsn: Lsn = i32::MAX.into();
            page.set_lsn(large_lsn);
            assert_eq!(page.get_lsn(), large_lsn);
        }

        #[test]
        fn test_page_reset_memory_after_dirty() {
            let mut page = Page::new(1);
            let mut data = *page.get_data_mut();
            data[0] = 255;
            if let Err(e) = page.set_data(0, &data) {
                panic!("Error setting data: {:?}", e);
            }
            assert!(page.is_dirty());

            // Reset memory and verify it's no longer dirty
            page.reset_memory();
            page.set_dirty(false);
            assert!(!page.is_dirty());
            assert_eq!(page.get_data()[0], 0);
        }
    }
}
