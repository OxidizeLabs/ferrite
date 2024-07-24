#[cfg(test)]
mod tests {
    use crate::disk::disk_manager::DiskManager;
    use std::fs::remove_file;
    use std::sync::Arc;

    const BUSTUB_PAGE_SIZE: usize = 4096;

    struct TestContext {
        disk_manager: Arc<DiskManager>,
    }

    impl TestContext {
        fn new() -> Self {
            let db_file = "test.db";
            let log_file = "test.log";
            let disk_manager = Arc::new(DiskManager::new(db_file, log_file));
            TestContext { disk_manager }
        }

        fn cleanup() {
            let _ = remove_file("test.db");
            let _ = remove_file("test.log");
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            Self::cleanup();
        }
    }

    #[test]
    fn test_read_write_page() {
        let ctx = TestContext::new();
        let mut buf = [0u8; BUSTUB_PAGE_SIZE];
        let mut data = [0u8; BUSTUB_PAGE_SIZE];
        data[..14].copy_from_slice(b"A test string.");

        ctx.disk_manager.read_page(0, &mut buf); // tolerate empty read

        ctx.disk_manager.write_page(0, data);
        ctx.disk_manager.read_page(0, &mut buf);
        assert_eq!(buf, data);

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

        ctx.disk_manager.read_log(&mut buf, 0); // tolerate empty read

        ctx.disk_manager.write_log(&data);
        ctx.disk_manager.read_log(&mut buf, 0);
        assert_eq!(buf, data);

        ctx.disk_manager.shut_down();
    }

    #[test]
    fn test_throw_bad_file() {
        assert!(std::panic::catch_unwind(|| {
            DiskManager::new("dev/null\\/foo/bar/baz/test.db", "test.log")
        })
        .is_err());
    }
}
