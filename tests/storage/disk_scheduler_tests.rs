#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use spin::Mutex;
    use tokio::test;
    use tokio::fs::remove_file;

    use tkdb::common::config::DB_PAGE_SIZE;
    use tkdb::storage::disk::disk_manager::DiskManager;
    use tkdb::storage::disk::disk_scheduler::DiskScheduler;

    extern crate tkdb;

    struct TestContext {
        disk_manager: Arc<DiskManager>,
        disk_scheduler: Arc<DiskScheduler>,
        db_file: String,
        db_log: String,
    }

    impl TestContext {
        fn new() -> Self {
            let db_file = "test_disk_scheduler.db";
            let log_file = "test_disk_scheduler.log";
            let disk_manager = Arc::new(DiskManager::new(db_file, log_file));
            let disk_scheduler = Arc::new(DiskScheduler::new(Arc::clone(&disk_manager)));
            Self { disk_manager, disk_scheduler, db_file: db_file.to_string(), db_log: log_file.to_string() }
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
    async fn schedule_write_read_page_test() {
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
