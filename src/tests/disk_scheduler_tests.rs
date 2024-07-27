#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::common::config::DB_PAGE_SIZE;
    use crate::disk::disk_manager::DiskManager;
    use crate::disk::disk_scheduler::DiskScheduler;

    #[tokio::test]
    async fn schedule_write_read_page_test() {
        let buf = Arc::new(Mutex::new([0u8; DB_PAGE_SIZE]));
        let data = Arc::new(Mutex::new([0u8; DB_PAGE_SIZE]));

        {
            let mut data_guard = data.lock().unwrap();
            let test_string = b"A test string.";
            data_guard[..test_string.len()].copy_from_slice(test_string);
        }

        let disk_manager = Arc::new(DiskManager::new("test.db", "test.log"));
        let disk_scheduler = DiskScheduler::new(Arc::clone(&disk_manager));

        let write_promise = disk_scheduler.schedule(true, Arc::clone(&data), 0);
        let read_promise = disk_scheduler.schedule(false, Arc::clone(&buf), 0);

        write_promise.await.unwrap();
        read_promise.await.unwrap();

        let buf_guard = buf.lock().unwrap();
        let data_guard = data.lock().unwrap();

        assert_eq!(&*buf_guard, &*data_guard);

        disk_scheduler.shut_down();
    }

}
