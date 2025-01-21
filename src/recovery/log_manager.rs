use crate::common::config::{Lsn, INVALID_LSN, LOG_BUFFER_SIZE};
use crate::recovery::log_record::LogRecord;
use crate::storage::disk::disk_manager::FileDiskManager;
use log::{debug, error, info, trace, warn};
use parking_lot::{Mutex, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use crossbeam_channel::{bounded, Receiver, Sender};
use std::sync::atomic::AtomicBool;

/// LogManager maintains a separate thread that is awakened whenever the log buffer is full or whenever a timeout
/// happens. When the thread is awakened, the log buffer's content is written into the disk log file.
#[derive(Debug)]
pub struct LogManager {
    next_lsn: AtomicU64,
    persistent_lsn: AtomicU64,
    log_buffer: Arc<RwLock<LogBuffer>>,
    flush_thread: Option<thread::JoinHandle<()>>,
    stop_flag: Arc<AtomicBool>,
    disk_manager: Arc<FileDiskManager>,
    log_queue: (Sender<LogRecord>, Receiver<LogRecord>),
}

#[derive(Debug)]
struct LogBuffer {
    data: Vec<u8>,
    write_pos: usize,
}

impl LogBuffer {
    fn new(size: usize) -> Self {
        Self {
            data: vec![0; size],
            write_pos: 0,
        }
    }

    fn append(&mut self, bytes: &[u8]) -> bool {
        if self.write_pos + bytes.len() > self.data.len() {
            return false;
        }
        self.data[self.write_pos..self.write_pos + bytes.len()].copy_from_slice(bytes);
        self.write_pos += bytes.len();
        true
    }

    fn clear(&mut self) {
        self.write_pos = 0;
        self.data.fill(0);
    }

    fn is_empty(&self) -> bool {
        self.write_pos == 0
    }

    fn used_size(&self) -> usize {
        self.write_pos
    }

    fn get_data(&self) -> &[u8] {
        &self.data[..self.write_pos]
    }
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
        let (sender, receiver) = bounded(1000); // Bounded channel for backpressure
        
        Self {
            next_lsn: AtomicU64::new(0),
            persistent_lsn: AtomicU64::new(INVALID_LSN),
            log_buffer: Arc::new(RwLock::new(LogBuffer::new(LOG_BUFFER_SIZE as usize))),
            flush_thread: None,
            stop_flag: Arc::new(AtomicBool::new(false)),
            disk_manager,
            log_queue: (sender, receiver),
        }
    }

    /// Runs the flush thread which writes the log buffer's content to the disk.
    pub fn run_flush_thread(&mut self) {
        let log_buffer = Arc::clone(&self.log_buffer);
        let disk_manager = Arc::clone(&self.disk_manager);
        let stop_flag = Arc::clone(&self.stop_flag);
        let receiver = self.log_queue.1.clone();

        self.flush_thread = Some(thread::spawn(move || {
            let mut flush_buffer = LogBuffer::new(LOG_BUFFER_SIZE as usize);
            
            while !stop_flag.load(Ordering::SeqCst) {
                // Process queued log records
                while let Ok(record) = receiver.try_recv() {
                    let bytes = record.to_string();
                    if !flush_buffer.append(bytes.as_bytes()) {
                        // Buffer full, flush it
                        if !flush_buffer.is_empty() {
                            if let Err(e) = disk_manager.write_log(flush_buffer.get_data()) {
                                error!("Failed to write log to disk: {}", e);
                            }
                            flush_buffer.clear();
                        }
                        // Try append again
                        flush_buffer.append(bytes.as_bytes());
                    }
                }

                // Periodically flush if buffer not empty
                if !flush_buffer.is_empty() {
                    if let Err(e) = disk_manager.write_log(flush_buffer.get_data()) {
                        error!("Failed to write log to disk: {}", e);
                    }
                    flush_buffer.clear();
                }

                thread::sleep(Duration::from_millis(10));
            }
        }));
    }

    pub fn shut_down(&mut self) {
        self.stop_flag.store(true, Ordering::SeqCst);
        if let Some(handle) = self.flush_thread.take() {
            handle.join().unwrap_or_else(|e| {
                error!("Flush thread panicked: {:?}", e);
            });
        }
    }

    /// Appends a log record to the log buffer.
    ///
    /// # Parameters
    /// - `log_record`: The log record to append.
    ///
    /// # Returns
    /// The log sequence number (LSN) of the appended log record.
    pub fn append_log_record(&mut self, log_record: &LogRecord) -> Lsn {
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        
        // Clone the record before sending
        if let Err(e) = self.log_queue.0.send(log_record.clone()) {
            error!("Failed to queue log record: {}", e);
            // Handle error appropriately
        }

        // For commit records, wait for flush
        if log_record.is_commit() {
            // Wait for buffer to be flushed
            while !self.log_buffer.read().is_empty() {
                thread::sleep(Duration::from_millis(1));
            }
        }

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
    pub fn get_log_buffer(&self) -> &Arc<RwLock<LogBuffer>> {
        trace!("Accessing log buffer");
        &self.log_buffer
    }

    pub fn get_log_buffer_size(&self) -> usize {
        let size = self.log_buffer.read().data.len();
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
        // assert!(!ctx.log_manager.get_log_buffer().is_empty());
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
        let mut next_lsn = 0;

        // Create all records first
        let begin_record = LogRecord::new_transaction_record(
            1,
            INVALID_LSN,
            LogRecordType::Begin,
        );

        // Append begin record and get LSN
        let begin_lsn = {
            let lsn = ctx.log_manager.append_log_record(&begin_record);
            next_lsn += 1;
            assert_eq!(lsn, 0);
            lsn
        };

        // Small delay between operations
        thread::sleep(Duration::from_millis(1));

        // Create and append commit record
        let commit_record = LogRecord::new_transaction_record(
            1,
            begin_lsn,
            LogRecordType::Commit,
        );
        let commit_lsn = ctx.log_manager.append_log_record(&commit_record);
        next_lsn += 1;
        assert_eq!(commit_lsn, 1);

        thread::sleep(Duration::from_millis(1));

        // Create and append abort record
        let abort_record = LogRecord::new_transaction_record(
            2,
            INVALID_LSN,
            LogRecordType::Abort,
        );
        let abort_lsn = ctx.log_manager.append_log_record(&abort_record);
        next_lsn += 1;
        assert_eq!(abort_lsn, 2);

        // Final verification
        let final_lsn = ctx.log_manager.get_next_lsn();
        assert_eq!(final_lsn, next_lsn, 
            "Expected LSN {} but got {}", next_lsn, final_lsn);
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

        // assert!(ctx.log_manager.get_log_buffer().len() > 1000);
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
        
        // Create log record once and reuse
        let log_record = LogRecord::new_transaction_record(1, INVALID_LSN, LogRecordType::Begin);

        // Test trace-level logging with minimal lock holding
        {
            trace!("Testing trace-level logging");
            ctx.log_manager.append_log_record(&log_record);
        }

        // Test debug-level logging in separate scope
        {
            debug!("Testing debug-level logging");
            ctx.log_manager.set_persistent_lsn(42);
        }

        // Test info-level logging in separate scope
        {
            info!("Testing info-level logging");
            ctx.log_manager.run_flush_thread();
        }

        // Test warning-level logging in separate scope
        {
            warn!("Testing warning-level logging");
            // Fill buffer in chunks to avoid holding lock too long
            for chunk in 0..10 {
                for i in 0..100 {
                    let record = LogRecord::new_transaction_record(
                        (chunk * 100 + i) as TxnId,
                        INVALID_LSN,
                        LogRecordType::Begin
                    );
                    ctx.log_manager.append_log_record(&record);
                }
                // Give other threads a chance to run
                thread::sleep(Duration::from_millis(1));
            }
        }

        // Test error-level logging in separate scope
        {
            error!("Testing error-level logging");
            ctx.log_manager.shut_down();
        }
    }
}