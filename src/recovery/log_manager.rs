use crate::common::config::{Lsn, INVALID_LSN, LOG_BUFFER_SIZE};
use crate::recovery::log_record::LogRecord;
use crate::storage::disk::disk_manager::FileDiskManager;
use log::{debug, error, info, trace, warn};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

/// LogManager maintains a separate thread that is awakened whenever the log buffer is full or whenever a timeout
/// happens. When the thread is awakened, the log buffer's content is written into the disk log file.
pub struct LogManager {
    next_lsn: AtomicU64,
    persistent_lsn: AtomicU64,
    log_buffer: Vec<u8>,
    flush_buffer: Vec<u8>,
    flush_thread: Option<thread::JoinHandle<()>>,
    stop_flag: Arc<RwLock<bool>>,
    disk_manager: Arc<FileDiskManager>,
}

impl LogManager {
    /// Creates a new `LogManager`.
    ///
    /// # Parameters
    /// - `disk_manager`: A reference to the disk manager.
    ///
    /// # Returns
    /// A new `LogManager` instance.
    pub fn new(disk_manager: Arc<FileDiskManager>) -> Self {
        debug!("Creating new LogManager instance");
        let manager = Self {
            next_lsn: AtomicU64::new(0),
            persistent_lsn: AtomicU64::new(INVALID_LSN),
            log_buffer: vec![0; LOG_BUFFER_SIZE as usize],
            flush_buffer: vec![0; LOG_BUFFER_SIZE as usize],
            stop_flag: Arc::new(RwLock::new(false)),
            flush_thread: None,
            disk_manager,
        };
        debug!("LogManager initialized with buffer size: {}", LOG_BUFFER_SIZE);
        manager
    }

    /// Runs the flush thread which writes the log buffer's content to the disk.
    pub fn run_flush_thread(&mut self) {
        debug!("Starting flush thread");
        let flush_buffer = self.flush_buffer.clone();
        let disk_manager = Arc::clone(&self.disk_manager);
        let stop_flag = Arc::clone(&self.stop_flag);

        self.flush_thread = Some(thread::spawn(move || {
            info!("Flush thread started on thread: {:?}", thread::current().id());
            debug!("Flush thread entering main loop");

            while {
                let stop_flag = stop_flag.read();
                !*stop_flag
            } {
                trace!("Flush thread performing disk write");
                match disk_manager.write_log(&flush_buffer) {
                    Ok(_) => trace!("Log buffer successfully flushed to disk"),
                    Err(e) => {
                        error!("Failed to write log to disk: {}", e);
                        // Continue running despite error - we'll retry on next iteration
                    }
                }
            }
            info!("Flush thread terminating");
        }));
        debug!("Flush thread spawned successfully");
    }

    pub fn shut_down(&mut self) {
        info!("Initiating LogManager shutdown");
        // Set the stop flag to indicate that the worker thread should stop
        {
            let mut stop_flag = self.stop_flag.write();
            debug!("Setting stop flag to true");
            *stop_flag = true;
        }

        // Wait for the worker thread to finish
        if let Some(handle) = self.flush_thread.take() {
            debug!("Waiting for flush thread to terminate");
            match handle.join() {
                Ok(_) => info!("Flush thread terminated successfully"),
                Err(e) => {
                    error!("Flush thread panicked during shutdown: {:?}", e);
                    warn!("Some log records may not have been written to disk");
                }
            }
        } else {
            debug!("No flush thread was running");
        }
        info!("LogManager shutdown complete");
    }

    /// Appends a log record to the log buffer.
    ///
    /// # Parameters
    /// - `log_record`: The log record to append.
    ///
    /// # Returns
    /// The log sequence number (LSN) of the appended log record.
    pub fn append_log_record(&mut self, log_record: &LogRecord) -> Lsn {
        trace!("Appending log record: {:?}", log_record);
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        debug!("Assigned LSN {} to log record", lsn);

        // Serialize log record and write to log buffer
        let log_record_bytes = log_record.to_string();
        let record_size = log_record_bytes.len();
        trace!("Serialized log record size: {} bytes", record_size);

        // Check if buffer has enough space
        if self.log_buffer.len() + record_size > LOG_BUFFER_SIZE as usize {
            warn!("Log buffer approaching capacity, may need to force flush");
        }

        self.log_buffer.extend_from_slice((&log_record_bytes).as_ref());
        debug!("Log record successfully appended to buffer");

        lsn
    }

    /// Returns the next log sequence number (LSN).
    pub fn get_next_lsn(&self) -> Lsn {
        let lsn = self.next_lsn.load(Ordering::SeqCst);
        trace!("Retrieved next LSN: {}", lsn);
        lsn
    }

    /// Returns the persistent log sequence number (LSN).
    pub fn get_persistent_lsn(&self) -> Lsn {
        let lsn = self.persistent_lsn.load(Ordering::SeqCst);
        trace!("Retrieved persistent LSN: {}", lsn);
        lsn
    }

    /// Sets the persistent log sequence number (LSN).
    ///
    /// # Parameters
    /// - `lsn`: The log sequence number to set.
    pub fn set_persistent_lsn(&mut self, lsn: Lsn) {
        debug!("Setting persistent LSN to {}", lsn);
        self.persistent_lsn.store(lsn, Ordering::SeqCst);
    }

    /// Returns a reference to the log buffer.
    pub fn get_log_buffer(&self) -> &[u8] {
        trace!("Accessing log buffer, current size: {}", self.log_buffer.len());
        &self.log_buffer
    }

    pub fn get_log_buffer_size(&self) -> usize {
        let size = self.log_buffer.len();
        trace!("Retrieved log buffer size: {}", size);
        size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::config::TxnId;
    use crate::recovery::log_record::{LogRecord, LogRecordType};
    use std::fs;
    use std::path::Path;
    use std::thread::sleep;
    use std::time::Duration;

    fn init_test_logger() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();
    }

    struct TestContext {
        log_file_path: String,
        log_manager: LogManager,
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            init_test_logger();
            debug!("Creating new test context for: {}", test_name);

            let log_file_path = format!("tests/data/{}_{}.log", test_name, chrono::Utc::now().timestamp());
            let disk_manager = Arc::new(FileDiskManager::new(
                "dummy.db".to_string(),
                log_file_path.clone(),
                100,
            ));
            let log_manager = LogManager::new(Arc::clone(&disk_manager));

            TestContext {
                log_file_path,
                log_manager,
            }
        }

        fn cleanup(&self) {
            debug!("Cleaning up test context");
            if Path::new(&self.log_file_path).exists() {
                if let Err(e) = fs::remove_file(&self.log_file_path) {
                    warn!("Failed to clean up log file: {}", e);
                }
            }
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            self.cleanup();
        }
    }

    #[test]
    fn test_log_manager_initialization() {
        let ctx = TestContext::new("init_test");

        assert_eq!(ctx.log_manager.get_next_lsn(), 0);
        assert_eq!(ctx.log_manager.get_persistent_lsn(), INVALID_LSN);
        assert_eq!(ctx.log_manager.get_log_buffer_size(), LOG_BUFFER_SIZE as usize);
    }

    #[test]
    fn test_append_log_record() {
        let mut ctx = TestContext::new("append_test");

        let txn_id: TxnId = 1;
        let prev_lsn = INVALID_LSN;

        // Create a begin transaction log record
        let log_record = LogRecord::new_transaction_record(
            txn_id,
            prev_lsn,
            LogRecordType::Begin,
        );

        // Append the log record
        let lsn = ctx.log_manager.append_log_record(&log_record);
        assert_eq!(lsn, 0); // First LSN should be 0

        // Verify LSN was incremented
        assert_eq!(ctx.log_manager.get_next_lsn(), 1);

        // Verify log buffer contains data
        assert!(!ctx.log_manager.get_log_buffer().is_empty());
    }

    #[test]
    fn test_multiple_append_operations() {
        let mut ctx = TestContext::new("multiple_append_test");

        // Append multiple log records
        for i in 0..5 {
            let log_record = LogRecord::new_transaction_record(
                i as TxnId,
                INVALID_LSN,
                LogRecordType::Begin,
            );
            let lsn = ctx.log_manager.append_log_record(&log_record);
            assert_eq!(lsn, i);
        }

        assert_eq!(ctx.log_manager.get_next_lsn(), 5);
    }

    #[test]
    fn test_persistent_lsn_management() {
        let mut ctx = TestContext::new("persistent_lsn_test");

        assert_eq!(ctx.log_manager.get_persistent_lsn(), INVALID_LSN);

        // Set persistent LSN
        let test_lsn = 42;
        ctx.log_manager.set_persistent_lsn(test_lsn);
        assert_eq!(ctx.log_manager.get_persistent_lsn(), test_lsn);
    }

    #[test]
    fn test_flush_thread_lifecycle() {
        let mut ctx = TestContext::new("flush_thread_test");

        // Start flush thread
        ctx.log_manager.run_flush_thread();

        // Allow some time for thread to start
        sleep(Duration::from_millis(100));

        // Append some log records
        for i in 0..3 {
            let log_record = LogRecord::new_transaction_record(
                i as TxnId,
                INVALID_LSN,
                LogRecordType::Begin,
            );
            ctx.log_manager.append_log_record(&log_record);
        }

        // Shutdown should complete cleanly
        ctx.log_manager.shut_down();
    }

    #[test]
    fn test_log_record_types() {
        let mut ctx = TestContext::new("record_types_test");

        // Test Begin record
        let begin_record = LogRecord::new_transaction_record(
            1,
            INVALID_LSN,
            LogRecordType::Begin,
        );
        let begin_lsn = ctx.log_manager.append_log_record(&begin_record);

        // Test Commit record
        let commit_record = LogRecord::new_transaction_record(
            1,
            begin_lsn,
            LogRecordType::Commit,
        );

        // Test Abort record
        let abort_record = LogRecord::new_transaction_record(
            2,
            INVALID_LSN,
            LogRecordType::Abort,
        );
        ctx.log_manager.append_log_record(&abort_record);

        assert_eq!(ctx.log_manager.get_next_lsn(), 3);
    }

    #[test]
    fn test_large_log_records() {
        let mut ctx = TestContext::new("large_records_test");

        // Create a large log record
        let log_record = LogRecord::new_transaction_record(
            1,
            INVALID_LSN,
            LogRecordType::Begin,
        );

        // Append multiple large records
        for _ in 0..5 {
            ctx.log_manager.append_log_record(&log_record);
        }

        assert!(ctx.log_manager.get_log_buffer().len() > 1000);
    }

    #[test]
    fn test_concurrent_log_appends() {
        let ctx = Arc::new(parking_lot::RwLock::new(TestContext::new("concurrent_test")));
        let thread_count = 5;
        let mut handles = vec![];

        for i in 0..thread_count {
            let ctx_clone = Arc::clone(&ctx);
            let handle = thread::spawn(move || {
                let log_record = LogRecord::new_transaction_record(
                    i as TxnId,
                    INVALID_LSN,
                    LogRecordType::Begin,
                );
                ctx_clone.write().log_manager.append_log_record(&log_record)
            });
            handles.push(handle);
        }

        // Collect all LSNs
        let lsns: Vec<Lsn> = handles.into_iter()
            .map(|h| h.join().unwrap())
            .collect();

        // Verify LSNs are unique and sequential
        let mut unique_lsns: Vec<Lsn> = lsns.clone();
        unique_lsns.sort();
        unique_lsns.dedup();
        assert_eq!(unique_lsns.len(), thread_count);

        assert_eq!(ctx.read().log_manager.get_next_lsn() as usize, thread_count);
    }

    #[test]
    fn test_shutdown_behavior() {
        let mut ctx = TestContext::new("shutdown_test");

        // Start flush thread
        ctx.log_manager.run_flush_thread();

        // Append some records
        for i in 0..3 {
            let log_record = LogRecord::new_transaction_record(
                i as TxnId,
                INVALID_LSN,
                LogRecordType::Begin,
            );
            ctx.log_manager.append_log_record(&log_record);
        }

        // Test multiple shutdown calls (should handle gracefully)
        ctx.log_manager.shut_down();
        ctx.log_manager.shut_down(); // Second shutdown should not panic
    }

    #[test]
    fn test_log_level_transitions() {
        let mut ctx = TestContext::new("log_level_test");

        // Test trace-level logging
        trace!("Testing trace-level logging");
        let log_record = LogRecord::new_transaction_record(1, INVALID_LSN, LogRecordType::Begin);
        ctx.log_manager.append_log_record(&log_record);

        // Test debug-level logging
        debug!("Testing debug-level logging");
        ctx.log_manager.set_persistent_lsn(42);

        // Test info-level logging
        info!("Testing info-level logging");
        ctx.log_manager.run_flush_thread();

        // Test warning-level logging
        warn!("Testing warning-level logging");
        for _ in 0..1000 {  // Fill buffer to trigger warning
            ctx.log_manager.append_log_record(&log_record);
        }

        // Test error-level logging
        error!("Testing error-level logging");
        ctx.log_manager.shut_down();
    }
}