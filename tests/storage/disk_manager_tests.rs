extern crate tkdb;

use std::sync::Arc;

use chrono::Utc;

use tkdb::common::config::DB_PAGE_SIZE;
use tkdb::storage::disk::disk_manager::{DiskIO, FileDiskManager};

use crate::test_setup::initialize_logger;

const BUSTUB_PAGE_SIZE: usize = 4096;

struct TestContext {
    disk_manager: Arc<FileDiskManager>,
    db_file: String,
    db_log: String,
}

impl TestContext {
    async fn new(test_name: &str) -> Self {
        initialize_logger();
        let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
        let db_file = format!("{}_{}.db", test_name, timestamp);
        let db_log_file = format!("{}_{}.log", test_name, timestamp);
        let disk_manager =
            Arc::new(FileDiskManager::new(db_file.clone(), db_log_file.clone()).await);
        Self {
            disk_manager,
            db_file,
            db_log: db_log_file,
        }
    }

    async fn cleanup(&self) {
        let _ = tokio::fs::remove_file(&self.db_file).await;
        let _ = tokio::fs::remove_file(&self.db_log).await;
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        let db_file = self.db_file.clone();
        let db_log = self.db_log.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let _ = tokio::fs::remove_file(db_file).await;
                let _ = tokio::fs::remove_file(db_log).await;
            });
        })
        .join()
        .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_read_write_page() {
        let ctx = TestContext::new("test_read_write_page").await;
        let mut buf = [0u8; DB_PAGE_SIZE];
        let mut data = [0u8; DB_PAGE_SIZE];
        data[..14].copy_from_slice(b"A test string.");

        // Tolerate empty read
        ctx.disk_manager.read_page(0, &mut buf).await;

        // Write and read page 0
        ctx.disk_manager.write_page(0, &data).await;
        ctx.disk_manager.read_page(0, &mut buf).await;
        assert_eq!(buf, data);

        // Write and read page 5
        buf.fill(0);
        ctx.disk_manager.write_page(5, &data).await;
        ctx.disk_manager.read_page(5, &mut buf).await;
        assert_eq!(buf, data);

        ctx.disk_manager.shut_down().await;
    }

    #[tokio::test]
    async fn test_read_write_log() {
        let ctx = TestContext::new("test_read_write_log").await;
        let mut buf = [0u8; 16];
        let mut data = [0u8; 16];
        data[..14].copy_from_slice(b"A test string.");

        // Tolerate empty read
        ctx.disk_manager.read_log(&mut buf, 0).await;

        // Write and read log
        ctx.disk_manager.write_log(&data).await;
        ctx.disk_manager.read_log(&mut buf, 0).await;
        assert_eq!(buf, data);

        ctx.disk_manager.shut_down().await;
    }

    // #[test]
    // fn test_throw_bad_file() {
    //     assert!(std::panic::catch_unwind(|| {
    //         FileDiskManager::new("/dev/null/foo/bar/baz/test_disk_manager.db", "test_disk_manager.log")
    //     })
    //     .is_err());
    // }
}
