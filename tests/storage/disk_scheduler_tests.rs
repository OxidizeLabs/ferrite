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
use std::thread::sleep;


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
        let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
        let db_log_file = format!("tests/data/{}_{}.log", test_name, timestamp);
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
mod unit_tests {
    use super::*;

    #[test]
    fn disk_scheduler_initialization() {
        let ctx = TestContext::new("disk_scheduler_initialization");
        let disk_scheduler = ctx.disk_scheduler.read();
        assert!(disk_scheduler.worker_thread.is_some());
        assert!(disk_scheduler.is_request_queue_empty());
    }
}

#[cfg(test)]
mod basic_behaviour {
    use super::*;

    #[test]
    fn schedule_write_and_read_operations() {
        let ctx = TestContext::new("schedule_write_read_operations");
        let disk_scheduler = ctx.disk_scheduler.clone();
        let buf = Arc::new(RwLock::new([0u8; DB_PAGE_SIZE]));
        let data = Arc::new(RwLock::new([0u8; DB_PAGE_SIZE]));

        // Preparing the data to be written
        {
            let mut data_guard = data.write();
            let test_string = b"Sample Data";
            data_guard[..test_string.len()].copy_from_slice(test_string);
        }

        let (write_tx, write_rx) = mpsc::channel();
        let (read_tx, read_rx) = mpsc::channel();

        // Schedule write and read
        disk_scheduler
            .write()
            .schedule(true, Arc::clone(&data), 1, write_tx);
        disk_scheduler
            .write()
            .schedule(false, Arc::clone(&buf), 1, read_tx);

        // Wait for operations to complete
        write_rx.recv().expect("Write operation failed");
        read_rx.recv().expect("Read operation failed");

        let buf_guard = buf.read();
        assert_eq!(buf_guard[..11], b"Sample Data"[..]);
    }

    #[test]
    fn scheduler_shutdown() {
        let ctx = TestContext::new("scheduler_shutdown");
        let disk_scheduler = ctx.disk_scheduler.clone();

        let data = Arc::new(RwLock::new([0u8; DB_PAGE_SIZE]));
        let (tx, _rx) = mpsc::channel();

        disk_scheduler.write().shut_down();
        assert!(disk_scheduler.write().worker_thread.is_none());

        // Attempting to schedule after shutdown
        disk_scheduler.write().schedule(true, Arc::clone(&data), 0, tx);
        assert!(!disk_scheduler.read().is_request_queue_empty());
    }
}

#[cfg(test)]
mod concurrency {
    use super::*;

    #[test]
    fn write_and_read_operations() {
        let ctx = TestContext::new("concurrent_operations");
        let disk_scheduler = ctx.disk_scheduler.clone();
        let mut threads = vec![];

        for i in 0..10 {
            let disk_scheduler = disk_scheduler.clone();
            threads.push(thread::spawn(move || {
                let data = Arc::new(RwLock::new([0u8; DB_PAGE_SIZE]));
                let buf = Arc::new(RwLock::new([0u8; DB_PAGE_SIZE]));
                let (write_tx, write_rx) = mpsc::channel();
                let (read_tx, read_rx) = mpsc::channel();

                disk_scheduler
                    .write()
                    .schedule(true, Arc::clone(&data), i, write_tx);
                disk_scheduler
                    .write()
                    .schedule(false, Arc::clone(&buf), i, read_tx);

                write_rx.recv().expect("Write operation failed");
                read_rx.recv().expect("Read operation failed");
            }));
        }

        for thread in threads {
            thread.join().unwrap();
        }
    }

    #[test]
    fn high_concurrency_load() {
        let ctx = TestContext::new("high_concurrency_load");
        let disk_scheduler = ctx.disk_scheduler.clone();

        let mut handles = vec![];
        for i in 0..100 {
            let disk_scheduler = disk_scheduler.clone();
            handles.push(thread::spawn(move || {
                let data = Arc::new(RwLock::new([0u8; DB_PAGE_SIZE]));
                let (tx, _rx) = mpsc::channel();
                disk_scheduler
                    .write()
                    .schedule(true, Arc::clone(&data), i, tx);
            }));
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }
        let milli = core::time::Duration::from_millis(100);
        sleep(milli);

        // Ensure all requests were processed
        assert_eq!(disk_scheduler.read().get_request_queue_length(), 0);
    }
}

#[cfg(test)]
mod edge_cases {
    use super::*;

    #[test]
    fn empty_queue_handling() {
        let ctx = TestContext::new("empty_queue_handling");
        let disk_scheduler = ctx.disk_scheduler.clone();

        // Directly trigger the worker thread notification without any scheduled task
        let _ = disk_scheduler.write().notifier.send(());

        // Worker thread should handle this gracefully without panicking
        assert!(disk_scheduler.read().is_request_queue_empty());
    }

    #[test]
    fn shutdown_while_processing() {
        let ctx = TestContext::new("shutdown_while_processing");
        let disk_scheduler = ctx.disk_scheduler.clone();

        let data = Arc::new(RwLock::new([0u8; DB_PAGE_SIZE]));
        let (tx, _rx) = mpsc::channel();

        for i in 0..5 {
            disk_scheduler.write().schedule(true, Arc::clone(&data), i, tx.clone());
        }

        // Initiate shutdown while requests are in the queue
        disk_scheduler.write().shut_down();

        // Ensure no pending requests are left in the queue
        assert_eq!(disk_scheduler.read().get_request_queue_length(), 0);
    }
}


