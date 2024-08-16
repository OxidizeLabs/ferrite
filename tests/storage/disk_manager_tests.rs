extern crate tkdb;

use chrono::Utc;
use spin::Mutex;
use std::sync::Arc;
use tkdb::common::config::DB_PAGE_SIZE;
use tkdb::storage::disk::disk_manager::{DiskIO, FileDiskManager};
use crate::test_setup::initialize_logger;
use std::fs;

struct TestContext {
    disk_manager: Arc<Mutex<FileDiskManager>>,
    db_file: String,
    db_log: String,
}

impl TestContext {
    fn new(test_name: &str) -> Self {
        initialize_logger();
        let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
        let db_file = format!("{}_{}.db", test_name, timestamp);
        let db_log_file = format!("{}_{}.log", test_name, timestamp);
        let disk_manager = Arc::new(Mutex::new(FileDiskManager::new(db_file.clone(), db_log_file.clone())));
        Self {
            disk_manager,
            db_file,
            db_log: db_log_file,
        }
    }

    fn cleanup(&self) {
        let _ = fs::remove_file(&self.db_file);
        let _ = fs::remove_file(&self.db_log);
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

    #[test]
    fn test_read_write_page() {
        let ctx = TestContext::new("test_read_write_page");
        let mut buf = [0u8; DB_PAGE_SIZE];
        let mut data = [0u8; DB_PAGE_SIZE];
        data[..14].copy_from_slice(b"A test string.");

        // Tolerate empty read
        ctx.disk_manager.lock().read_page(0, &mut buf);

        // Write and read page 0
        ctx.disk_manager.lock().write_page(0, &data);
        ctx.disk_manager.lock().read_page(0, &mut buf);
        assert_eq!(buf, data);

        // Write and read page 5
        buf.fill(0);
        ctx.disk_manager.lock().write_page(5, &data);
        ctx.disk_manager.lock().read_page(5, &mut buf);
        assert_eq!(buf, data);

        ctx.disk_manager.lock().shut_down();
    }

    #[test]
    fn test_read_write_log() {
        let ctx = TestContext::new("test_read_write_log");
        let mut buf = [0u8; 16];
        let mut data = [0u8; 16];
        data[..14].copy_from_slice(b"A test string.");

        // Tolerate empty read
        ctx.disk_manager.lock().read_log(&mut buf, 0);

        // Write and read log
        ctx.disk_manager.lock().write_log(&data);
        ctx.disk_manager.lock().read_log(&mut buf, 0);
        assert_eq!(buf, data);

        ctx.disk_manager.lock().shut_down();
    }
}
