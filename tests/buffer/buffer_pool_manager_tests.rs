use crate::test_setup::initialize_logger;
use chrono::Utc;
use log::info;
use spin::Mutex;
use std::sync::Arc;
use tkdb::buffer::buffer_pool_manager::BufferPoolManager;
use tkdb::buffer::lru_k_replacer::LRUKReplacer;
use tkdb::storage::disk::disk_manager::FileDiskManager;
use tkdb::storage::disk::disk_scheduler::DiskScheduler;
use tokio::fs::remove_file;
use tokio::sync::Mutex as TokioMutex;

struct TestContext {
    bpm: Arc<BufferPoolManager>,
    db_file: String,
    db_log_file: String,
    buffer_pool_size: usize,
}

impl TestContext {
    async fn new(test_name: &str) -> Self {
        initialize_logger();
        let buffer_pool_size: usize = 5;
        const K: usize = 2;
        let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
        let db_file = format!("{}_{}.db", test_name, timestamp);
        let db_log_file = format!("{}_{}.log", test_name, timestamp);
        let disk_manager =
            Arc::new(FileDiskManager::new(db_file.clone(), db_log_file.clone()).await);
        let disk_scheduler = Arc::new(TokioMutex::new(DiskScheduler::new(Arc::clone(
            &disk_manager,
        ))));
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(buffer_pool_size, K)));
        let bpm = Arc::new(BufferPoolManager::new(
            buffer_pool_size,
            disk_scheduler,
            disk_manager.clone(),
            replacer.clone(),
        ));
        Self {
            bpm,
            db_file,
            db_log_file,
            buffer_pool_size,
        }
    }

    async fn cleanup(&self) {
        let _ = remove_file(&self.db_file).await;
        let _ = remove_file(&self.db_log_file).await;
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        let db_file = self.db_file.clone();
        let db_log = self.db_log_file.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let _ = remove_file(db_file).await;
                let _ = remove_file(db_log).await;
            });
        })
        .join()
        .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use tkdb::buffer::lru_k_replacer::AccessType;
    use tkdb::common::config::DB_PAGE_SIZE;
    use tokio;

    #[tokio::test]
    async fn binary_data_test() {
        let ctx = TestContext::new("binary_data_test").await;
        let bpm = &ctx.bpm;

        // Scenario: The buffer pool is empty. We should be able to create a new page.
        info!("Creating page 0...");
        let mut page0 = bpm.new_page().await.expect("Failed to create a new page");

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
        assert_eq!(
            &random_binary_data, page_data,
            "Data mismatch immediately after writing"
        );

        // Write data to page in bpm
        bpm.write_page(page0.get_page_id(), random_binary_data.clone());

        // Scenario: We should be able to create new pages until we fill up the buffer pool.
        for i in 1..ctx.buffer_pool_size {
            info!("Creating page {}...", i + 1);
            assert!(bpm.new_page().await.is_some());
        }

        // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
        for i in ctx.buffer_pool_size..(ctx.buffer_pool_size * 2) {
            info!("Attempting to create page {}...", i + 1);
            let new_page_result = bpm.new_page().await;
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
            let new_page = bpm
                .new_page()
                .await
                .expect("Failed to create a new page after unpinning");
            bpm.unpin_page(new_page.get_page_id(), false, AccessType::Lookup);
        }

        // Scenario: We should be able to fetch the data we wrote a while ago.
        if let Some(page0) = bpm.fetch_page(0).await {
            let page0 = page0.read();
            let fetched_data = page0.get_data();

            // Print the fetched data for debugging
            // info!("Fetched Data:    {}", format_slice(fetched_data));
            // info!("Expected Data:   {}", format_slice(&random_binary_data));
            assert_eq!(
                &random_binary_data, fetched_data,
                "Data mismatch after fetching"
            );
        } else {
            panic!("Failed to fetch page 0");
        }

        bpm.unpin_page(0, true, AccessType::Lookup);
    }
}
