extern crate tkdb;

use chrono::Utc;
use log::info;
use spin::{Mutex, RwLock};
use std::fs;
use std::sync::{Arc, mpsc};
use std::thread;
use tkdb::common::config::DB_PAGE_SIZE;
use tkdb::storage::disk::disk_manager::FileDiskManager;
use tkdb::storage::disk::disk_scheduler::DiskScheduler;
use crate::test_setup::initialize_logger;

struct TestContext {
    disk_manager: Arc<FileDiskManager>,
    disk_scheduler: Arc<RwLock<DiskScheduler>>,
    db_file: String,
    db_log: String,
}

impl TestContext {
    fn new(test_name: &str) -> Self {
        initialize_logger();
        let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
        let db_file = format!("{}_{}.db", test_name, timestamp);
        let db_log_file = format!("{}_{}.log", test_name, timestamp);
        let disk_manager = Arc::new(FileDiskManager::new(db_file.clone(), db_log_file.clone()));
        let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
        Self {
            disk_manager,
            disk_scheduler,
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
    fn test_disk_scheduler_write_and_read() {
        let ctx = TestContext::new("test_disk_scheduler_write_and_read");

        info!("Test started on thread: {:?}", thread::current().id());

        let disk_scheduler = ctx.disk_scheduler.clone();

        // Define page ID and data
        let mut write_data = [0u8; 4096];
        write_data[..11].copy_from_slice(b"test string");

        let data_arc = Arc::new(RwLock::new(write_data));

        // Create a channel for synchronous communication
        let (tx, rx) = mpsc::channel();

        // Schedule a write operation
        disk_scheduler.write().schedule(true, Arc::clone(&data_arc), 0, tx);
        info!(
            "Scheduled write operation on thread: {:?}",
            thread::current().id()
        );

        // Wait for the write operation to complete
        rx.recv().expect("Write operation failed");

        // Prepare buffer for reading
        let mut read_data = [0u8; 4096];
        let read_data_arc = Arc::new(RwLock::new(read_data));

        // Create a new channel for reading
        let (tx, rx) = mpsc::channel();

        // Schedule a read operation
        disk_scheduler.write().schedule(false, Arc::clone(&read_data_arc), 0, tx);
        info!(
            "Scheduled read operation on thread: {:?}",
            thread::current().id()
        );

        // Wait for the read operation to complete
        rx.recv().expect("Read operation failed");

        // Retrieve the read data
        let read_data_locked = read_data_arc.read();

        // Check if the read data matches the written data
        assert_eq!(&read_data_locked[..11], b"test string");

        // Clean up: shut down the scheduler
        disk_scheduler.write().shut_down();
        ctx.disk_manager.shut_down();

        info!("Test completed on thread: {:?}", thread::current().id());
    }

    #[test]
    fn schedule_write_read_page() {
        let ctx = TestContext::new("schedule_write_read_page");
        let disk_scheduler = ctx.disk_scheduler.clone();
        let buf = Arc::new(RwLock::new([0u8; DB_PAGE_SIZE]));
        let data = Arc::new(RwLock::new([0u8; DB_PAGE_SIZE]));

        {
            let mut data_guard = data.write();
            let test_string = b"A test string.";
            data_guard[..test_string.len()].copy_from_slice(test_string);
        }

        // Create channels for synchronous communication
        let (write_tx, write_rx) = mpsc::channel();
        let (read_tx, read_rx) = mpsc::channel();

        disk_scheduler
            .write()
            .schedule(true, Arc::clone(&data), 0, write_tx);
        disk_scheduler
            .write()
            .schedule(false, Arc::clone(&buf), 0, read_tx);

        write_rx.recv().expect("Write operation failed");
        read_rx.recv().expect("Read operation failed");

        let buf_guard = buf.read();
        let data_guard = data.read();

        assert_eq!(*buf_guard, *data_guard);

        disk_scheduler.write().shut_down();
        ctx.disk_manager.shut_down();
    }
}

