use std::sync::Arc;
use chrono::Utc;
use spin::Mutex;
use tokio;
use tokio::fs::remove_file;

use tkdb::common::config::DB_PAGE_SIZE;
use tkdb::storage::disk::disk_manager::DiskManager;
use tkdb::storage::disk::disk_scheduler::DiskScheduler;
use crate::test_setup::initialize_logger;


struct TestContext {
    disk_manager: Arc<DiskManager>,
    disk_scheduler: Arc<DiskScheduler>,
    db_file: String,
    db_log: String,
}

impl TestContext {
    fn new() -> Self {
        initialize_logger();
        let timestamp = Utc::now().format("%Y%m%d%H%M%S").to_string();
        let db_file = format!("test_disk_scheduler_{}.db", timestamp);
        let db_log_file = format!("test_disk_scheduler_{}.log", timestamp);
        let disk_manager = Arc::new(DiskManager::new(db_file.clone(), db_log_file.clone()));
        let disk_scheduler = Arc::new(DiskScheduler::new(Arc::clone(&disk_manager)));
        Self { disk_manager, disk_scheduler, db_file: db_file.to_string(), db_log: db_log_file.to_string() }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn schedule_write_read_page() {
        let ctx = TestContext::new();
        let buf = Arc::new(Mutex::new([0u8; DB_PAGE_SIZE]));
        let data = Arc::new(Mutex::new([0u8; DB_PAGE_SIZE]));

        {
            let mut data_guard = data.lock();
            let test_string = b"A test string.";
            data_guard[..test_string.len()].copy_from_slice(test_string);
        }

        let write_promise = ctx.disk_scheduler.schedule(true, Arc::clone(&data), 0);
        let read_promise = ctx.disk_scheduler.schedule(false, Arc::clone(&buf), 0);

        write_promise.await.unwrap();
        read_promise.await.unwrap();

        let buf_guard = buf.lock();
        let data_guard = data.lock();

        assert_eq!(&*buf_guard, &*data_guard);

        ctx.disk_scheduler.shut_down();
        ctx.disk_manager.shut_down();
    }
}
