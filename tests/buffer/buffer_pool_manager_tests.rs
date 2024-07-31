#[cfg(test)]
mod tests {
    use std::fs::remove_file;

    extern crate tkdb;

    use tkdb::buffer::buffer_pool_manager::BufferPoolManager;
    use tkdb::buffer::lru_k_replacer::{AccessType, LRUKReplacer};
    use tkdb::common::config::DB_PAGE_SIZE;
    use tkdb::storage::disk::disk_manager::DiskManager;
    use tkdb::storage::disk::disk_scheduler::DiskScheduler;
    use rand::Rng;
    use std::sync::{Arc, Mutex};
    use tkdb::common::util::helpers::format_slice;

    struct TestContext {
        disk_manager: Arc<DiskManager>,
        disk_scheduler: Arc<DiskScheduler>,
        replacer: Arc<Mutex<LRUKReplacer>>,
        db_file: String,
        db_log: String
    }

    impl TestContext {
        fn new() -> Self {
            let db_file = "test_bpm.db";
            let log_file = "test_bpm.log";
            let buffer_pool_size= 10;
            let disk_manager = Arc::new(DiskManager::new(db_file, log_file));
            let disk_scheduler = Arc::new(DiskScheduler::new(Arc::clone(&disk_manager)));
            let replacer = Arc::new(Mutex::new(LRUKReplacer::new(7, 2)));

            Self { disk_manager, disk_scheduler, replacer, db_file: db_file.to_string(), db_log: log_file.to_string() }
        }

        fn cleanup(&self) {
            let _ = remove_file(&self.db_file);
            let _ = remove_file(&self.db_log);
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            self.cleanup();
        }
    }

    #[test]
    fn binary_data_test() {
        let ctx = TestContext::new();
        let buffer_pool_size = 10;
        let disk_manager = &ctx.disk_manager;
        let disk_scheduler = &ctx.disk_scheduler;
        let replacer = &ctx.replacer;
        let bpm = BufferPoolManager::new(buffer_pool_size, disk_scheduler.clone(), disk_manager.clone(), replacer.clone());

        // Scenario: The buffer pool is empty. We should be able to create a new page.
        println!("Creating page 0...");
        let mut page0 = bpm.new_page().expect("Failed to create a new page");

        // Generate and fill random binary data
        let mut rng = rand::thread_rng();
        let mut random_binary_data = [0u8; DB_PAGE_SIZE];
        rng.fill(&mut random_binary_data);

        // Insert terminal characters both in the middle and at the end
        random_binary_data[DB_PAGE_SIZE / 2] = 0;
        random_binary_data[DB_PAGE_SIZE - 1] = 0;

        // Scenario: Once we have a page, we should be able to read and write content.
        let page_data = page0.get_data_mut();
        page_data.copy_from_slice(&random_binary_data);
        assert_eq!(&random_binary_data, page_data, "Data mismatch immediately after writing");

        // Write data to page in bpm
        bpm.write_page(page0.get_page_id(), random_binary_data.clone());

        // Scenario: We should be able to create new pages until we fill up the buffer pool.
        for i in 1..buffer_pool_size {
            println!("Creating page {}...", i + 1);
            assert!(bpm.new_page().is_some());
        }

        // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
        for i in buffer_pool_size..(buffer_pool_size * 2) {
            println!("Attempting to create page {}...", i + 1);
            let new_page_result = bpm.new_page();
            if let Some(ref page) = new_page_result {
                println!("Unexpectedly created page {}", page.get_page_id());
            }
            assert!(new_page_result.is_none());
        }

        // Scenario: After unpinning pages {0, 1, 2, 3, 4}, we should be able to create 5 new pages.
        for i in 0..5 {
            println!("Unpinning page {}...", i);
            assert!(bpm.unpin_page(i, true, AccessType::Lookup));
            bpm.flush_page(i).expect("Failed to flush page");
        }
        for i in 0..5 {
            println!("Creating new page after unpinning: {}...", i + 1);
            let new_page = bpm.new_page().expect("Failed to create a new page after unpinning");
            bpm.unpin_page(new_page.get_page_id(), false, AccessType::Lookup);
        }

        // Scenario: We should be able to fetch the data we wrote a while ago.
        if let Some(page0) = bpm.fetch_page(0) {
            let page0 = page0.read().unwrap();
            let fetched_data = page0.get_data();

            // Print the fetched data for debugging
            // println!("Fetched Data:    {}", format_slice(fetched_data));
            // println!("Expected Data:   {}", format_slice(&random_binary_data));
            assert_eq!(&random_binary_data, fetched_data, "Data mismatch after fetching");
        } else {
            panic!("Failed to fetch page 0");
        }

        bpm.unpin_page(0, true, AccessType::Lookup);

        // Shutdown the disk manager and remove the temporary file we created.
        ctx.disk_scheduler.shut_down();
        ctx.disk_manager.shut_down();
    }
}
