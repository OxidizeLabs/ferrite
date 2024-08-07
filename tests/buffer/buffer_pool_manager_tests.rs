use std::fs::remove_file;
use std::sync::Arc;
use chrono::Utc;
use spin::Mutex;
use rand::Rng;

use tkdb::buffer::buffer_pool_manager::BufferPoolManager;
use tkdb::buffer::lru_k_replacer::{AccessType, LRUKReplacer};
use tkdb::common::config::DB_PAGE_SIZE;
use tkdb::storage::disk::disk_manager::DiskManager;
use tkdb::storage::disk::disk_scheduler::DiskScheduler;
use crate::test_setup::initialize_logger;

extern crate tkdb;

struct TestContext {
    bpm: Arc<BufferPoolManager>,
    buffer_pool_size: usize,
    db_file: String,
    db_log_file: String,
}

impl TestContext {
    fn new() -> Self {
        initialize_logger();
        let timestamp = Utc::now().format("%Y%m%d%H%M%S").to_string();
        let db_file = format!("test_bpm_{}.db", timestamp);
        let db_log_file = format!("test_bpm_{}.log", timestamp);
        let buffer_pool_size = 10;
        let disk_manager = Arc::new(DiskManager::new(db_file.clone(), db_log_file.clone()));
        let disk_scheduler = Arc::new((DiskScheduler::new(Arc::clone(&disk_manager))));
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(7, 2)));
        let bpm = Arc::new(BufferPoolManager::new(buffer_pool_size, disk_scheduler.clone(), disk_manager.clone(), replacer.clone()));

        Self {
            bpm,
            buffer_pool_size,
            db_file,
            db_log_file
        }
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
    use log::info;
    use super::*;


    #[test]
    fn binary_data_test() {
        let ctx = TestContext::new();
        let bpm = &ctx.bpm;


        // Scenario: The buffer pool is empty. We should be able to create a new page.
        info!("Creating page 0...");
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
        for i in 1..ctx.buffer_pool_size {
            info!("Creating page {}...", i + 1);
            assert!(bpm.new_page().is_some());
        }

        // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
        for i in ctx.buffer_pool_size..(ctx.buffer_pool_size * 2) {
            info!("Attempting to create page {}...", i + 1);
            let new_page_result = bpm.new_page();
            if let Some(ref page) = new_page_result {
                info!("Unexpectedly created page {}", page.get_page_id());
            }
            assert!(new_page_result.is_none());
        }

        // Scenario: After unpinning pages {0, 1, 2, 3, 4}, we should be able to create 5 new pages.
        for i in 0..5 {
            info!("Unpinning page {}...", i);
            assert!(bpm.unpin_page(i, true, AccessType::Lookup));
            bpm.flush_page(i).expect("Failed to flush page");
        }
        for i in 0..5 {
            info!("Creating new page after unpinning: {}...", i + 1);
            let new_page = bpm.new_page().expect("Failed to create a new page after unpinning");
            bpm.unpin_page(new_page.get_page_id(), false, AccessType::Lookup);
        }

        // Scenario: We should be able to fetch the data we wrote a while ago.
        if let Some(page0) = bpm.fetch_page(0) {
            let page0 = page0.read();
            let fetched_data = page0.get_data();

            // Print the fetched data for debugging
            // info!("Fetched Data:    {}", format_slice(fetched_data));
            // info!("Expected Data:   {}", format_slice(&random_binary_data));
            assert_eq!(&random_binary_data, fetched_data, "Data mismatch after fetching");
        } else {
            panic!("Failed to fetch page 0");
        }

        bpm.unpin_page(0, true, AccessType::Lookup);
    }
}
