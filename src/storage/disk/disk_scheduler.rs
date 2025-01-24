use crate::common::config::PageId;
use crate::common::logger::initialize_logger;
use crate::storage::disk::disk_manager::{DiskIO, FileDiskManager, MockDiskIO};
use chrono::Utc;
use crossbeam::channel::{unbounded, Receiver, Sender};
use log::{error, info};
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::mpsc;
use std::sync::Arc;
use std::{fs, thread};

// Define DiskRequest struct
#[derive(Debug)]
pub struct DiskRequest {
    is_write: bool,
    data: Arc<RwLock<[u8; 4096]>>,
    page_id: PageId,
    sender: mpsc::Sender<()>,
}

// Define DiskScheduler struct
#[derive(Debug)]
pub struct DiskScheduler {
    disk_manager: Arc<FileDiskManager>,
    request_queue: Arc<RwLock<VecDeque<DiskRequest>>>,
    stop_flag: Arc<RwLock<bool>>,
    notifier: Sender<()>,
    worker_thread: Option<thread::JoinHandle<()>>,
}

impl DiskScheduler {
    pub fn new(disk_manager: Arc<FileDiskManager>) -> Self {
        let request_queue = Arc::new(RwLock::new(VecDeque::new()));
        let stop_flag = Arc::new(RwLock::new(false));
        let (notifier, receiver) = unbounded();
        let mut scheduler = DiskScheduler {
            disk_manager,
            request_queue: Arc::clone(&request_queue),
            stop_flag: Arc::clone(&stop_flag),
            notifier,
            worker_thread: None,
        };
        scheduler.start_worker_thread(receiver);
        scheduler
    }

    pub fn schedule(
        &self,
        is_write: bool,
        data: Arc<RwLock<[u8; 4096]>>,
        page_id: PageId,
        tx: mpsc::Sender<()>,
    ) {
        // Create a simple streaming channel

        let request = DiskRequest {
            is_write,
            data,
            page_id,
            sender: tx,
        };

        // Lock the queue to add a request
        {
            let mut queue = self.request_queue.write();
            queue.push_back(request);
            info!(
                "Request added to queue: is_write={}, page_id={}",
                is_write, page_id
            );
        }

        // Notify the worker thread
        if let Err(err) = self.notifier.send(()) {
            info!("Failed to send notification to worker thread: {:?}", err);
        } else {
            info!("Notifier sent to worker thread");
        }
    }

    pub fn start_worker_thread(&mut self, receiver: Receiver<()>) {
        let request_queue = Arc::clone(&self.request_queue);
        let disk_manager = Arc::clone(&self.disk_manager);
        let stop_flag = Arc::clone(&self.stop_flag);

        self.worker_thread = Some(thread::spawn(move || {
            info!(
                "Worker thread started on thread: {:?}",
                thread::current().id()
            );

            while {
                let stop_flag = stop_flag.read();
                !*stop_flag
            } {
                // Wait for notification
                info!(
                    "Worker thread waiting for notification on thread: {:?}",
                    thread::current().id()
                );
                if receiver.recv().is_err() {
                    info!(
                        "Worker thread notification receiver closed on thread: {:?}",
                        thread::current().id()
                    );
                    break;
                }
                info!(
                    "Worker thread received notification on thread: {:?}",
                    thread::current().id()
                );

                // Process request
                if let Some(request) = request_queue.write().pop_front() {
                    info!(
                        "Processing request: is_write={}, page_id={} on thread: {:?}",
                        request.is_write,
                        request.page_id,
                        thread::current().id()
                    );
                    let mut data = request.data.write();
                    if request.is_write {
                        info!("Writing to disk: page_id={}", request.page_id);
                        let _ = disk_manager.write_page(request.page_id, &data);
                    } else {
                        info!("Reading from disk: page_id={}", request.page_id);
                        let _ = disk_manager.read_page(request.page_id, &mut *data);
                    }
                    let _ = request.sender.send(());
                    info!(
                        "Request processed and response sent on thread: {:?}",
                        thread::current().id()
                    );
                }
            }

            // Process remaining requests before exiting
            while let Some(request) = request_queue.write().pop_front() {
                let mut data = request.data.write();
                if request.is_write {
                    info!("Writing to disk: page_id={}", request.page_id);
                    let _ = disk_manager.write_page(request.page_id, &data);
                } else {
                    info!("Reading from disk: page_id={}", request.page_id);
                    let _ = disk_manager.read_page(request.page_id, &mut *data);
                }
                let _ = request.sender.send(());
                info!("Request processed and response sent");
            }
        }));
    }

    pub fn shut_down(&mut self) {
        // Set the stop flag to indicate that the worker thread should stop
        {
            let mut stop_flag = self.stop_flag.write();
            *stop_flag = true; // Set the flag to true
        }

        // Notify the worker thread to exit
        if let Err(e) = self.notifier.send(()) {
            error!("Failed to send shutdown signal: {}", e);
        } else {
            info!("Shutdown signal sent");
        }

        // Wait for the worker thread to finish
        if let Some(handle) = self.worker_thread.take() {
            if let Err(e) = handle.join() {
                // Handle potential panics from the worker thread
                error!("Worker thread panicked during shutdown: {:?}", e);
            }
        }
    }

    pub fn get_request_queue_length(&self) -> usize {
        self.request_queue.read().len()
    }

    pub fn is_request_queue_empty(&self) -> bool {
        self.request_queue.read().is_empty()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use mockall::predicate::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub struct TestContext {
        disk_manager: Arc<FileDiskManager>,
        disk_scheduler: Arc<RwLock<DiskScheduler>>,
    }

    impl TestContext {
        fn new() -> Self {
            // Create disk manager with mock disk IO
            let disk_manager = Arc::new(FileDiskManager::new(
                "mock_db.db".to_string(),
                "mock_log.db".to_string(),
                10,
            ));
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            Self {
                disk_manager,
                disk_scheduler,
            }
        }

        fn set_mock_disk_io(&self, mock: MockDiskIO) {
            self.disk_manager.set_disk_io(Box::new(mock));
        }

        fn disk_scheduler(&self) -> Arc<RwLock<DiskScheduler>> {
            Arc::clone(&self.disk_scheduler)
        }
    }

    #[cfg(test)]
    mod unit_tests {
        use super::*;

        #[test]
        fn disk_scheduler_initialization() {
            let ctx = TestContext::new();
            let disk_scheduler = ctx.disk_scheduler.read();
            assert!(disk_scheduler.worker_thread.is_some());
            assert!(disk_scheduler.is_request_queue_empty());
        }
    }

    #[cfg(test)]
    mod basic_behaviour {
        use crate::common::config::DB_PAGE_SIZE;
        use super::*;

        #[test]
        fn schedule_write_and_read_operations() {
            let ctx = TestContext::new();
            let disk_scheduler = ctx.disk_scheduler.clone();
            
            // Create mock disk IO
            let mut mock_disk_io = MockDiskIO::new();
            
            // Setup write expectation
            mock_disk_io
                .expect_write_page()
                .with(eq(1), always())
                .times(1)
                .returning(|_, _| Ok(()));

            // Setup read expectation
            mock_disk_io
                .expect_read_page()
                .with(eq(1), always())
                .times(1)
                .returning(|_, buf| {
                    buf[..11].copy_from_slice(b"Sample Data");
                    Ok(())
                });

            ctx.set_mock_disk_io(mock_disk_io);

            let data = Arc::new(RwLock::new([0u8; DB_PAGE_SIZE as usize]));
            let buf = Arc::new(RwLock::new([0u8; DB_PAGE_SIZE as usize]));

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
            let ctx = TestContext::new();
            let disk_scheduler = ctx.disk_scheduler.clone();

            let mut mock_disk_io = MockDiskIO::new();
            mock_disk_io
                .expect_write_page()
                .returning(|_, _| Ok(()));
            ctx.set_mock_disk_io(mock_disk_io);

            let data = Arc::new(RwLock::new([0u8; DB_PAGE_SIZE as usize]));
            let (tx, _rx) = mpsc::channel();

            disk_scheduler.write().shut_down();
            assert!(disk_scheduler.write().worker_thread.is_none());

            // Attempting to schedule after shutdown
            disk_scheduler
                .write()
                .schedule(true, Arc::clone(&data), 0, tx);
            assert!(!disk_scheduler.read().is_request_queue_empty());
        }
    }

    #[cfg(test)]
    mod concurrency {
        use crate::common::config::DB_PAGE_SIZE;
        use super::*;

        #[test]
        fn write_and_read_operations() {
            let ctx = TestContext::new();
            let disk_scheduler = ctx.disk_scheduler.clone();
            
            let mut mock_disk_io = MockDiskIO::new();
            mock_disk_io
                .expect_write_page()
                .returning(|_, _| Ok(()))
                .times(10);
            mock_disk_io
                .expect_read_page()
                .returning(|_, buf| {
                    buf[0] = 42;
                    Ok(())
                })
                .times(10);
            
            ctx.set_mock_disk_io(mock_disk_io);

            let mut threads = vec![];

            for i in 0..10 {
                let disk_scheduler = disk_scheduler.clone();
                threads.push(thread::spawn(move || {
                    let data = Arc::new(RwLock::new([0u8; DB_PAGE_SIZE as usize]));
                    let buf = Arc::new(RwLock::new([0u8; DB_PAGE_SIZE as usize]));
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
            let ctx = TestContext::new();
            let disk_scheduler = ctx.disk_scheduler.clone();

            let mut mock_disk_io = MockDiskIO::new();
            mock_disk_io
                .expect_write_page()
                .returning(|_, _| Ok(()))
                .times(100);
            
            ctx.set_mock_disk_io(mock_disk_io);

            let mut handles = vec![];
            for i in 0..100 {
                let disk_scheduler = disk_scheduler.clone();
                handles.push(thread::spawn(move || {
                    let data = Arc::new(RwLock::new([0u8; DB_PAGE_SIZE as usize]));
                    let (tx, rx) = mpsc::channel();
                    disk_scheduler
                        .write()
                        .schedule(true, Arc::clone(&data), i, tx);
                    rx.recv().expect("Write operation failed");
                }));
            }

            for handle in handles {
                handle.join().expect("Thread panicked");
            }

            assert_eq!(disk_scheduler.read().get_request_queue_length(), 0);
        }
    }

    #[cfg(test)]
    mod edge_cases {
        use std::io::{Error, ErrorKind};
        use crate::common::config::DB_PAGE_SIZE;
        use super::*;

        #[test]
        fn empty_queue_handling() {
            let ctx = TestContext::new();
            let disk_scheduler = ctx.disk_scheduler.clone();

            // Directly trigger the worker thread notification without any scheduled task
            let _ = disk_scheduler.write().notifier.send(());

            // Worker thread should handle this gracefully without panicking
            assert!(disk_scheduler.read().is_request_queue_empty());
        }

        #[test]
        fn shutdown_while_processing() {
            let ctx = TestContext::new();
            let disk_scheduler = ctx.disk_scheduler.clone();

            let mut mock_disk_io = MockDiskIO::new();
            mock_disk_io
                .expect_write_page()
                .returning(|_, _| Ok(()))
                .times(5);
            
            ctx.set_mock_disk_io(mock_disk_io);

            let data = Arc::new(RwLock::new([0u8; DB_PAGE_SIZE as usize]));
            let (tx, rx) = mpsc::channel();

            for i in 0..5 {
                disk_scheduler
                    .write()
                    .schedule(true, Arc::clone(&data), i, tx.clone());
            }

            // Initiate shutdown while requests are in the queue
            disk_scheduler.write().shut_down();

            // Ensure no pending requests are left in the queue
            assert_eq!(disk_scheduler.read().get_request_queue_length(), 0);
        }

        #[test]
        fn disk_error_handling() {
            let ctx = TestContext::new();
            let disk_scheduler = ctx.disk_scheduler.clone();

            let mut mock_disk_io = MockDiskIO::new();
            mock_disk_io
                .expect_write_page()
                .returning(|_, _| Err(Error::new(ErrorKind::Other, "Simulated disk error")));
            
            ctx.set_mock_disk_io(mock_disk_io);

            let data = Arc::new(RwLock::new([0u8; DB_PAGE_SIZE as usize]));
            let (tx, rx) = mpsc::channel();

            disk_scheduler
                .write()
                .schedule(true, Arc::clone(&data), 0, tx);

            // Operation should complete despite the error
            rx.recv().expect("Operation should complete despite error");
        }
    }
}


