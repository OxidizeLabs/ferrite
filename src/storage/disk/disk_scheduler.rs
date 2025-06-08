use crate::common::config::PageId;
use crate::storage::disk::disk_manager::FileDiskManager;
use crossbeam_channel::{unbounded, Receiver, Sender};
use log::{error, info, debug, trace};
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

// Define DiskRequest struct
#[derive(Debug)]
pub struct DiskRequest {
    is_write: bool,
    data: Arc<RwLock<[u8; 4096]>>,
    page_id: PageId,
    sender: mpsc::Sender<()>,
}

// PERFORMANCE OPTIMIZATION: Batch request structure
#[derive(Debug)]
pub struct BatchDiskRequest {
    write_requests: Vec<(PageId, Arc<RwLock<[u8; 4096]>>, mpsc::Sender<()>)>,
    read_requests: Vec<(PageId, Arc<RwLock<[u8; 4096]>>, mpsc::Sender<()>)>,
}

impl BatchDiskRequest {
    fn new() -> Self {
        Self {
            write_requests: Vec::new(),
            read_requests: Vec::new(),
        }
    }

    fn add_request(&mut self, request: DiskRequest) {
        if request.is_write {
            self.write_requests.push((request.page_id, request.data, request.sender));
        } else {
            self.read_requests.push((request.page_id, request.data, request.sender));
        }
    }

    fn is_empty(&self) -> bool {
        self.write_requests.is_empty() && self.read_requests.is_empty()
    }

    fn total_requests(&self) -> usize {
        self.write_requests.len() + self.read_requests.len()
    }
}

// Define DiskScheduler struct
#[derive(Debug)]
pub struct DiskScheduler {
    disk_manager: Arc<FileDiskManager>,
    request_queue: Arc<RwLock<VecDeque<DiskRequest>>>,
    stop_flag: Arc<RwLock<bool>>,
    notifier: Sender<()>,
    worker_thread: Option<thread::JoinHandle<()>>,
    // PERFORMANCE OPTIMIZATION: Batch processing state
    batch_timeout: Duration,
    last_batch_time: Arc<RwLock<Instant>>,
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
            batch_timeout: Duration::from_secs(1),
            last_batch_time: Arc::new(RwLock::new(Instant::now())),
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
            trace!(
                "Request added to queue: is_write={}, page_id={}",
                is_write, page_id
            );
        }

        // Update last batch time
        *self.last_batch_time.write() = Instant::now();

        // Notify the worker thread
        if let Err(err) = self.notifier.send(()) {
            debug!("Failed to send notification to worker thread: {:?}", err);
        } else {
            trace!("Notifier sent to worker thread");
        }
    }

    /// PERFORMANCE OPTIMIZATION: Process requests in batches for better I/O performance
    fn process_batch_requests(&self, batch: BatchDiskRequest) {
        let start_time = Instant::now();
        
        // Process all write requests in batch
        if !batch.write_requests.is_empty() {
            debug!("Processing {} write requests in batch", batch.write_requests.len());
            
            for (page_id, data, sender) in batch.write_requests {
                let data_guard = data.write();
                if let Err(e) = self.disk_manager.write_page(page_id, &data_guard) {
                    error!("Batch write failed for page {}: {}", page_id, e);
                }
                let _ = sender.send(());
            }
        }

        // Process all read requests in batch  
        if !batch.read_requests.is_empty() {
            debug!("Processing {} read requests in batch", batch.read_requests.len());
            
            for (page_id, data, sender) in batch.read_requests {
                let mut data_guard = data.write();
                if let Err(e) = self.disk_manager.read_page(page_id, &mut data_guard) {
                    error!("Batch read failed for page {}: {}", page_id, e);
                }
                let _ = sender.send(());
            }
        }

        let duration = start_time.elapsed();
        debug!("Batch processing completed in {:?}", duration);
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
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::disk_manager::MockDiskIO;
    use mockall::predicate::*;
    use tempfile::TempDir;

    pub struct TestContext {
        disk_manager: Arc<FileDiskManager>,
        disk_scheduler: Arc<RwLock<DiskScheduler>>,
    }

    impl TestContext {
        fn new(name: &str) -> Self {
            initialize_logger();
            // Create temporary directory
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir
                .path()
                .join(format!("{name}.db"))
                .to_str()
                .unwrap()
                .to_string();
            let log_path = temp_dir
                .path()
                .join(format!("{name}.log"))
                .to_str()
                .unwrap()
                .to_string();

            // Create disk manager with mock disk IO
            let disk_manager = Arc::new(FileDiskManager::new(db_path, log_path, 10));
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            Self {
                disk_manager,
                disk_scheduler,
            }
        }

        fn set_mock_disk_io(&self, mock: MockDiskIO) {
            self.disk_manager.set_disk_io(Box::new(mock));
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
        use crate::common::config::DB_PAGE_SIZE;

        #[test]
        fn schedule_write_and_read_operations() {
            let ctx = TestContext::new("schedule_write_and_read_operations");
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
            let ctx = TestContext::new("scheduler_shutdown");
            let disk_scheduler = ctx.disk_scheduler.clone();

            let mut mock_disk_io = MockDiskIO::new();
            mock_disk_io.expect_write_page().returning(|_, _| Ok(()));
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
        use super::*;
        use crate::common::config::DB_PAGE_SIZE;

        #[test]
        fn write_and_read_operations() {
            let ctx = TestContext::new("write_and_read_operations");
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
            let ctx = TestContext::new("high_concurrency_load");
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
        use super::*;
        use crate::common::config::DB_PAGE_SIZE;
        use std::io::{Error, ErrorKind};

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

            let mut mock_disk_io = MockDiskIO::new();
            mock_disk_io
                .expect_write_page()
                .returning(|_, _| Ok(()))
                .times(5);

            ctx.set_mock_disk_io(mock_disk_io);

            let data = Arc::new(RwLock::new([0u8; DB_PAGE_SIZE as usize]));
            let (tx, _rx) = mpsc::channel();

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
            let ctx = TestContext::new("disk_error_handling");
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
