#[cfg(test)]
mod tests {
    extern crate tkdb;

    use std::fs::remove_file;
    use std::sync::Arc;
    use chrono::Utc;
    use tkdb::common::config::DB_PAGE_SIZE;
    use tkdb::storage::disk::disk_manager::DiskManager;
    use crate::test_setup::initialize_logger;

    const BUSTUB_PAGE_SIZE: usize = 4096;

    struct TestContext {
        disk_manager: Arc<DiskManager>,
        db_file: String,
        db_log_file: String,
    }

    impl TestContext {
        fn new() -> Self {
            initialize_logger();
            let timestamp = Utc::now().format("%Y%m%d%H%M%S").to_string();
            let db_file = format!("test_disk_manager_{}.db", timestamp);
            let db_log_file = format!("test_disk_manager_{}.log", timestamp);
            let disk_manager = Arc::new(DiskManager::new(db_file.clone(), db_log_file.clone()));

            Self {
                disk_manager,
                db_file,
                db_log_file,
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


    #[test]
    fn test_read_write_page() {
        let ctx = TestContext::new();
        let mut buf = [0u8; DB_PAGE_SIZE];
        let mut data = [0u8; DB_PAGE_SIZE];
        data[..14].copy_from_slice(b"A test string.");

        // Tolerate empty read
        ctx.disk_manager.read_page(0, &mut buf);

        // Write and read page 0
        ctx.disk_manager.write_page(0, data);
        ctx.disk_manager.read_page(0, &mut buf);
        assert_eq!(buf, data);

        // Write and read page 5
        buf.fill(0);
        ctx.disk_manager.write_page(5, data);
        ctx.disk_manager.read_page(5, &mut buf);
        assert_eq!(buf, data);

        ctx.disk_manager.shut_down();
    }

    #[test]
    fn test_read_write_log() {
        let ctx = TestContext::new();
        let mut buf = [0u8; 16];
        let mut data = [0u8; 16];
        data[..14].copy_from_slice(b"A test string.");

        // Tolerate empty read
        ctx.disk_manager.read_log(&mut buf, 0);

        // Write and read log
        ctx.disk_manager.write_log(&data);
        ctx.disk_manager.read_log(&mut buf, 0);
        assert_eq!(buf, data);

        ctx.disk_manager.shut_down();
    }

    // #[test]
    // fn test_throw_bad_file() {
    //     assert!(std::panic::catch_unwind(|| {
    //         DiskManager::new("/dev/null/foo/bar/baz/test_disk_manager.db", "test_disk_manager.log")
    //     })
    //     .is_err());
    // }
}
