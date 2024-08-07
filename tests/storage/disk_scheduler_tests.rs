use std::sync::Arc;
use chrono::Utc;
use tokio;
use tokio::fs::remove_file;
use tokio::sync::Mutex;
use tkdb::storage::disk::disk_manager::FileDiskManager;
use tkdb::storage::disk::disk_scheduler::DiskScheduler;
use crate::test_setup::initialize_logger;
use std::thread;
use log::info;
use tkdb::common::config::DB_PAGE_SIZE;

struct TestContext {
    disk_manager: Arc<FileDiskManager>,
    disk_scheduler: Arc<Mutex<DiskScheduler>>,
    db_file: String,
    db_log: String,
}

impl TestContext {
    async fn new(test_name: &str) -> Self {
        initialize_logger();
        let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
        let db_file = format!("{}_{}.db", test_name, timestamp);
        let db_log_file = format!("{}_{}.log", test_name, timestamp);
        let disk_manager = Arc::new(FileDiskManager::new(db_file.clone(), db_log_file.clone()).await);
        let disk_scheduler = Arc::new(Mutex::new(DiskScheduler::new(Arc::clone(&disk_manager))));
        Self { disk_manager, disk_scheduler, db_file, db_log: db_log_file }
    }

    async fn cleanup(&self) {
        let _ = remove_file(&self.db_file).await;
        let _ = remove_file(&self.db_log).await;
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        let db_file = self.db_file.clone();
        let db_log = self.db_log.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let _ = remove_file(db_file).await;
                let _ = remove_file(db_log).await;
            });
        }).join().unwrap();
    }
}


#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_disk_scheduler_write_and_read() {
        let ctx = TestContext::new("test_disk_scheduler_write_and_read").await;

        info!("Test started on thread: {:?}", thread::current().id());

        let disk_scheduler = ctx.disk_scheduler.clone();

        // Define page ID and data
        let mut write_data = [0u8; 4096];
        write_data[..11].copy_from_slice(b"test string");

        let data_arc = Arc::new(tokio::sync::Mutex::new(write_data));

        // Schedule a write operation
        let write_receiver = disk_scheduler.lock().await.schedule(true, Arc::clone(&data_arc), 0).await;
        info!("Scheduled write operation on thread: {:?}", thread::current().id());

        // Wait for the write operation to complete
        write_receiver.await.expect("Write operation failed");

        // Prepare buffer for reading
        let read_data = [0u8; 4096];
        let read_data_arc = Arc::new(tokio::sync::Mutex::new(read_data));

        // Schedule a read operation
        let read_receiver = disk_scheduler.lock().await.schedule(false, Arc::clone(&read_data_arc), 0).await;
        info!("Scheduled read operation on thread: {:?}", thread::current().id());

        // Wait for the read operation to complete
        read_receiver.await.expect("Read operation failed");

        // Retrieve the read data
        let read_data_locked = read_data_arc.lock().await;

        // Check if the read data matches the written data
        assert_eq!(&read_data_locked[..11], b"test string");

        // Clean up: shut down the scheduler
        disk_scheduler.lock().await.shut_down().await;
        ctx.disk_manager.shut_down().await;

        info!("Test completed on thread: {:?}", thread::current().id());
    }

    #[tokio::test]
    async fn schedule_write_read_page() {
        let ctx = TestContext::new("schedule_write_read_page").await;
        let disk_scheduler = ctx.disk_scheduler.clone();
        let buf = Arc::new(Mutex::new([0u8; DB_PAGE_SIZE]));
        let data = Arc::new(Mutex::new([0u8; DB_PAGE_SIZE]));

        {
            let mut data_guard = data.lock().await;
            let test_string = b"A test string.";
            data_guard[..test_string.len()].copy_from_slice(test_string);
        }

        let write_promise = disk_scheduler.lock().await.schedule(true, Arc::clone(&data), 0).await;
        let read_promise = disk_scheduler.lock().await.schedule(false, Arc::clone(&buf), 0).await;

        write_promise.await;
        read_promise.await;

        let buf_guard = buf.lock().await;
        let data_guard = data.lock().await;

        assert_eq!(buf_guard.clone(), data_guard.clone());

        disk_scheduler.lock().await.shut_down().await;
        ctx.disk_manager.shut_down().await;
    }
}
