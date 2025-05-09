use crate::common::config::{Lsn, INVALID_LSN, LOG_BUFFER_SIZE};
use crate::recovery::log_record::LogRecord;
use crate::storage::disk::disk_manager::FileDiskManager;
use crossbeam_channel::{bounded, Receiver, Sender};
use log::{debug, error, trace};
use parking_lot::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

/// LogManager maintains a separate thread awakened whenever the log buffer is full or whenever a timeout
/// happens. When the thread is awakened, the log buffer's content is written into the disk log file.
#[derive(Debug)]
pub struct LogManager {
    state: Arc<LogManagerState>,
}

#[derive(Debug)]
struct LogManagerState {
    next_lsn: AtomicU64,
    persistent_lsn: AtomicU64,
    log_buffer: RwLock<LogBuffer>,
    flush_thread: Mutex<Option<thread::JoinHandle<()>>>,
    stop_flag: AtomicBool,
    disk_manager: Arc<FileDiskManager>,
    log_queue: (Sender<Arc<LogRecord>>, Receiver<Arc<LogRecord>>),
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
            state: Arc::new(LogManagerState {
                next_lsn: AtomicU64::new(0),
                persistent_lsn: AtomicU64::new(INVALID_LSN),
                log_buffer: RwLock::new(LogBuffer::new(LOG_BUFFER_SIZE as usize)),
                flush_thread: Mutex::new(None),
                stop_flag: AtomicBool::new(false),
                disk_manager,
                log_queue: (sender, receiver),
            }),
        }
    }

    /// Runs the flush thread which writes the log buffer's content to the disk.
    pub fn run_flush_thread(&mut self) {
        let state = Arc::clone(&self.state);

        let thread = thread::spawn(move || {
            let mut flush_buffer = LogBuffer::new(LOG_BUFFER_SIZE as usize);
            let mut batch_start_lsn: Option<Lsn> = None;
            let mut max_lsn_in_batch: Lsn = INVALID_LSN;
            let mut last_flush_time = std::time::Instant::now();
            let flush_interval = Duration::from_millis(10);

            while !state.stop_flag.load(Ordering::SeqCst) {
                let mut records_processed = false;

                // Process queued log records
                while let Ok(record) = state.log_queue.1.try_recv() {
                    records_processed = true;
                    let record_lsn = record.get_lsn();

                    // Track highest LSN in this batch
                    if record_lsn > max_lsn_in_batch {
                        max_lsn_in_batch = record_lsn;
                    }

                    // Track first LSN in this batch if not set
                    if batch_start_lsn.is_none() {
                        batch_start_lsn = Some(record_lsn);
                    }

                    let bytes = record.to_string();
                    if !flush_buffer.append(bytes.as_bytes()) {
                        // Buffer full, flush it
                        if !flush_buffer.is_empty() {
                            if let Err(e) = state.disk_manager.write_log(flush_buffer.get_data()) {
                                error!("Failed to write log to disk: {}", e);
                            } else {
                                // Update persistent LSN after successful flush
                                state
                                    .persistent_lsn
                                    .store(max_lsn_in_batch, Ordering::SeqCst);
                                debug!(
                                    "Flushed log buffer, persistent LSN updated to {}",
                                    max_lsn_in_batch
                                );
                            }
                            flush_buffer.clear();
                            batch_start_lsn = Some(record_lsn);
                            max_lsn_in_batch = record_lsn;
                        }
                        // Try append again with fresh buffer
                        flush_buffer.append(bytes.as_bytes());
                    }

                    // Check if record is a commit - force flush if it is
                    if record.is_commit() {
                        if !flush_buffer.is_empty() {
                            if let Err(e) = state.disk_manager.write_log(flush_buffer.get_data()) {
                                error!("Failed to write log to disk on commit: {}", e);
                            } else {
                                state
                                    .persistent_lsn
                                    .store(max_lsn_in_batch, Ordering::SeqCst);
                                debug!(
                                    "Flushed log buffer for commit, persistent LSN updated to {}",
                                    max_lsn_in_batch
                                );
                            }
                            flush_buffer.clear();
                            batch_start_lsn = None;
                            max_lsn_in_batch = INVALID_LSN;
                        }
                    }
                }

                // Periodically flush if buffer not empty and either:
                // 1. It's been too long since last flush
                // 2. No new records but buffer has content
                let now = std::time::Instant::now();
                if !flush_buffer.is_empty()
                    && (now.duration_since(last_flush_time) >= flush_interval || !records_processed)
                {
                    if let Err(e) = state.disk_manager.write_log(flush_buffer.get_data()) {
                        error!("Failed to write log to disk in periodic flush: {}", e);
                    } else {
                        state
                            .persistent_lsn
                            .store(max_lsn_in_batch, Ordering::SeqCst);
                        debug!(
                            "Periodic flush, persistent LSN updated to {}",
                            max_lsn_in_batch
                        );
                    }
                    flush_buffer.clear();
                    batch_start_lsn = None;
                    max_lsn_in_batch = INVALID_LSN;
                    last_flush_time = now;
                }

                if !records_processed {
                    thread::sleep(Duration::from_millis(5));
                }
            }

            // Final flush before exiting
            if !flush_buffer.is_empty() {
                if let Err(e) = state.disk_manager.write_log(flush_buffer.get_data()) {
                    error!("Failed to write log to disk during shutdown: {}", e);
                } else if max_lsn_in_batch != INVALID_LSN {
                    state
                        .persistent_lsn
                        .store(max_lsn_in_batch, Ordering::SeqCst);
                }
            }
        });

        // Store the thread handle, handle potential mutex poisoning gracefully
        if let Ok(mut flush_thread) = self.state.flush_thread.lock() {
            *flush_thread = Some(thread);
        } else {
            error!("Failed to acquire flush thread mutex - it may be poisoned");
        }
    }

    pub fn shut_down(&mut self) {
        self.state.stop_flag.store(true, Ordering::SeqCst);

        // Handle mutex poisoning gracefully
        let thread_handle = if let Ok(mut guard) = self.state.flush_thread.lock() {
            guard.take()
        } else {
            error!("Failed to acquire flush thread mutex during shutdown - it may be poisoned");
            None
        };

        // Join the thread if we got a valid handle
        if let Some(handle) = thread_handle {
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
    pub fn append_log_record(&mut self, log_record: Arc<LogRecord>) -> Lsn {
        // Assign a new LSN atomically
        let lsn = self.state.next_lsn.fetch_add(1, Ordering::SeqCst);

        // Send log record to processing queue
        if let Err(e) = self.state.log_queue.0.send(log_record.clone()) {
            error!("Failed to queue log record: {}", e);
            // Consider implementing retry logic or returning an error
        }

        // For commit records, ensure durability by waiting for buffer to be flushed
        if log_record.is_commit() {
            // Signal the flush thread that this is a commit record
            // Wait for confirmation that it has been flushed
            trace!("Waiting for commit record with LSN {} to be flushed", lsn);

            // Wait for buffer to be flushed (observe persistent_lsn ≥ lsn)
            while self.state.persistent_lsn.load(Ordering::SeqCst) < lsn {
                thread::sleep(Duration::from_millis(1));
            }

            debug!("Commit record with LSN {} has been flushed", lsn);
        }

        lsn
    }

    /// Returns the next log sequence number (LSN).
    pub fn get_next_lsn(&self) -> Lsn {
        let lsn = self.state.next_lsn.load(Ordering::SeqCst);
        trace!("Retrieved next LSN: {}", lsn);
        lsn
    }

    /// Returns the persistent log sequence number (LSN).
    pub fn get_persistent_lsn(&self) -> Lsn {
        let lsn = self.state.persistent_lsn.load(Ordering::SeqCst);
        trace!("Retrieved persistent LSN: {}", lsn);
        lsn
    }

    /// Sets the persistent log sequence number (LSN).
    ///
    /// # Parameters
    /// - `lsn`: The log sequence number to set.
    pub fn set_persistent_lsn(&mut self, lsn: Lsn) {
        debug!("Setting persistent LSN to {}", lsn);
        self.state.persistent_lsn.store(lsn, Ordering::SeqCst);
    }

    pub fn get_log_buffer_size(&self) -> usize {
        let size = self.state.log_buffer.read().data.len();
        trace!("Retrieved log buffer size: {}", size);
        size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::config::TxnId;
    use crate::recovery::log_record::{LogRecord, LogRecordType};
    use log::{info, warn};
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
        disk_manager: Arc<FileDiskManager>,
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            // init_test_logger();
            debug!("Creating new test context for: {}", test_name);

            let log_file_path = format!(
                "tests/data/{}_{}.log",
                test_name,
                chrono::Utc::now().timestamp()
            );
            let disk_manager = Arc::new(FileDiskManager::new(
                "dummy.db".to_string(),
                log_file_path.clone(),
                100,
            ));
            let log_manager = LogManager::new(Arc::clone(&disk_manager));

            TestContext {
                log_file_path,
                log_manager,
                disk_manager,
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

    /// Basic functionality tests for LogManager
    mod basic_functionality {
        use super::*;

        #[test]
        fn test_log_manager_initialization() {
            let ctx = TestContext::new("init_test");

            assert_eq!(ctx.log_manager.get_next_lsn(), 0);
            assert_eq!(ctx.log_manager.get_persistent_lsn(), INVALID_LSN);
            assert_eq!(
                ctx.log_manager.get_log_buffer_size(),
                LOG_BUFFER_SIZE as usize
            );
        }

        #[test]
        fn test_append_log_record() {
            let mut ctx = TestContext::new("append_test");

            let txn_id: TxnId = 1;
            let prev_lsn = INVALID_LSN;

            // Create a begin transaction log record
            let log_record = Arc::new(LogRecord::new_transaction_record(
                txn_id,
                prev_lsn,
                LogRecordType::Begin,
            ));

            // Append the log record
            let lsn = ctx.log_manager.append_log_record(log_record);
            assert_eq!(lsn, 0); // First LSN should be 0

            // Verify LSN was incremented
            assert_eq!(ctx.log_manager.get_next_lsn(), 1);
        }

        #[test]
        fn test_multiple_append_operations() {
            let mut ctx = TestContext::new("multiple_append_test");

            // Append multiple log records
            for i in 0..5 {
                let log_record = Arc::new(LogRecord::new_transaction_record(
                    i as TxnId,
                    INVALID_LSN,
                    LogRecordType::Begin,
                ));
                let lsn = ctx.log_manager.append_log_record(log_record);
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
        fn test_log_record_types() {
            let mut ctx = TestContext::new("record_types_test");
            let mut next_lsn = 0;

            // Create all records first
            let begin_record = Arc::new(LogRecord::new_transaction_record(
                1,
                INVALID_LSN,
                LogRecordType::Begin,
            ));

            // Append begin record and get LSN
            let begin_lsn = {
                let lsn = ctx.log_manager.append_log_record(begin_record.clone());
                next_lsn += 1;
                assert_eq!(lsn, 0);
                lsn
            };

            // Small delay between operations
            thread::sleep(Duration::from_millis(1));

            // Create and append commit record
            let commit_record = Arc::new(LogRecord::new_transaction_record(
                1,
                begin_lsn,
                LogRecordType::Commit,
            ));
            let commit_lsn = ctx.log_manager.append_log_record(commit_record);
            next_lsn += 1;
            assert_eq!(commit_lsn, 1);

            thread::sleep(Duration::from_millis(1));

            // Create and append abort record
            let abort_record = Arc::new(LogRecord::new_transaction_record(
                2,
                INVALID_LSN,
                LogRecordType::Abort,
            ));
            let abort_lsn = ctx.log_manager.append_log_record(abort_record);
            next_lsn += 1;
            assert_eq!(abort_lsn, 2);

            // Final verification
            let final_lsn = ctx.log_manager.get_next_lsn();
            assert_eq!(
                final_lsn, next_lsn,
                "Expected LSN {} but got {}",
                next_lsn, final_lsn
            );
        }
    }

    /// Tests for threading and concurrency
    mod threading_tests {
        use super::*;

        #[test]
        fn test_flush_thread_lifecycle() {
            let mut ctx = TestContext::new("flush_thread_test");

            // Start flush thread
            ctx.log_manager.run_flush_thread();

            // Allow some time for thread to start
            sleep(Duration::from_millis(100));

            // Append some log records
            for i in 0..3 {
                let log_record = Arc::new(LogRecord::new_transaction_record(
                    i as TxnId,
                    INVALID_LSN,
                    LogRecordType::Begin,
                ));
                ctx.log_manager.append_log_record(log_record);
            }

            // Shutdown should complete cleanly
            ctx.log_manager.shut_down();
        }

        #[test]
        fn test_concurrent_log_appends() {
            let ctx = Arc::new(parking_lot::RwLock::new(TestContext::new(
                "concurrent_test",
            )));
            let thread_count = 5;
            let mut handles = vec![];

            for i in 0..thread_count {
                let ctx_clone = Arc::clone(&ctx);
                let handle = thread::spawn(move || {
                    let log_record = Arc::new(LogRecord::new_transaction_record(
                        i as TxnId,
                        INVALID_LSN,
                        LogRecordType::Begin,
                    ));
                    ctx_clone.write().log_manager.append_log_record(log_record)
                });
                handles.push(handle);
            }

            // Collect all LSNs
            let lsns: Vec<Lsn> = handles.into_iter().map(|h| h.join().unwrap()).collect();

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
                let log_record = Arc::new(LogRecord::new_transaction_record(
                    i as TxnId,
                    INVALID_LSN,
                    LogRecordType::Begin,
                ));
                ctx.log_manager.append_log_record(log_record);
            }

            // Test multiple shutdown calls (should handle gracefully)
            ctx.log_manager.shut_down();
            ctx.log_manager.shut_down(); // Second shutdown should not panic
        }

        #[test]
        fn test_concurrent_shutdown() {
            let ctx = Arc::new(parking_lot::RwLock::new(TestContext::new(
                "concurrent_shutdown_test",
            )));

            // Start flush thread
            ctx.write().log_manager.run_flush_thread();

            // Append some records
            for i in 0..3 {
                let log_record = Arc::new(LogRecord::new_transaction_record(
                    i as TxnId,
                    INVALID_LSN,
                    LogRecordType::Begin,
                ));
                ctx.write().log_manager.append_log_record(log_record);
            }

            // Try to shut down from multiple threads simultaneously
            let thread_count = 3;
            let mut handles = vec![];

            for _ in 0..thread_count {
                let ctx_clone = Arc::clone(&ctx);
                let handle = thread::spawn(move || {
                    ctx_clone.write().log_manager.shut_down();
                });
                handles.push(handle);
            }

            // Wait for all threads to complete
            for handle in handles {
                handle.join().unwrap();
            }

            // Verify we can still interact with the log manager
            ctx.write().log_manager.get_next_lsn();
        }
    }

    /// Tests for edge cases and unusual situations
    mod edge_cases {
        use super::*;

        #[test]
        fn test_large_log_records() {
            let mut ctx = TestContext::new("large_records_test");

            // Create a large log record
            let log_record = Arc::new(LogRecord::new_transaction_record(
                1,
                INVALID_LSN,
                LogRecordType::Begin,
            ));

            // Append multiple large records
            for _ in 0..5 {
                ctx.log_manager.append_log_record(log_record.clone());
            }
        }

        #[test]
        fn test_log_level_transitions() {
            let mut ctx = TestContext::new("log_level_test");

            // Create log record once and reuse
            let log_record = Arc::new(LogRecord::new_transaction_record(
                1,
                INVALID_LSN,
                LogRecordType::Begin,
            ));

            // Test trace-level logging with minimal lock holding
            {
                trace!("Testing trace-level logging");
                ctx.log_manager.append_log_record(log_record.clone());
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
                for chunk in 0..5 {
                    // Reduced from 10 for faster tests
                    for i in 0..10 {
                        // Reduced from 100 for faster tests
                        let record = Arc::new(LogRecord::new_transaction_record(
                            (chunk * 10 + i) as TxnId,
                            INVALID_LSN,
                            LogRecordType::Begin,
                        ));
                        ctx.log_manager.append_log_record(record);
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

        #[test]
        fn test_empty_buffer_flush() {
            let mut ctx = TestContext::new("empty_buffer_test");

            // Start flush thread with empty buffer
            ctx.log_manager.run_flush_thread();

            // Allow some time for thread to start
            sleep(Duration::from_millis(50));

            // Shutdown without appending any records
            ctx.log_manager.shut_down();

            // Verify state is still valid
            assert_eq!(ctx.log_manager.get_next_lsn(), 0);
            assert_eq!(ctx.log_manager.get_persistent_lsn(), INVALID_LSN);
        }

        #[test]
        fn test_max_lsn_value() {
            let mut ctx = TestContext::new("max_lsn_test");

            // Set LSN to near max value
            let near_max_lsn = u64::MAX - 10;
            ctx.log_manager.set_persistent_lsn(near_max_lsn);

            // Verify we can still append logs
            for i in 0..5 {
                let log_record = Arc::new(LogRecord::new_transaction_record(
                    i as TxnId,
                    near_max_lsn,
                    LogRecordType::Begin,
                ));
                ctx.log_manager.append_log_record(log_record);
            }

            // Verify next LSN is properly incremented
            assert!(ctx.log_manager.get_next_lsn() > near_max_lsn);
        }
    }

    /// Tests for transaction commit behavior
    mod transaction_tests {
        use super::*;

        #[test]
        fn test_commit_record_flushing() {
            let mut ctx = TestContext::new("commit_flush_test");

            // Start flush thread
            ctx.log_manager.run_flush_thread();

            // Create and append a commit record
            let commit_record = Arc::new(LogRecord::new_transaction_record(
                1,
                INVALID_LSN,
                LogRecordType::Commit,
            ));

            // Append and get LSN
            let commit_lsn = ctx.log_manager.append_log_record(commit_record);

            // Verify persistent LSN is equal to or greater than commit LSN
            assert!(ctx.log_manager.get_persistent_lsn() >= commit_lsn);

            ctx.log_manager.shut_down();
        }

        #[test]
        fn test_transaction_sequence() {
            let mut ctx = TestContext::new("txn_sequence_test");

            // Start flush thread
            ctx.log_manager.run_flush_thread();

            // Simulate a full transaction (begin, operations, commit)
            let txn_id: TxnId = 42;
            let mut prev_lsn = INVALID_LSN;

            // Begin transaction
            let begin_record = Arc::new(LogRecord::new_transaction_record(
                txn_id,
                prev_lsn,
                LogRecordType::Begin,
            ));
            prev_lsn = ctx.log_manager.append_log_record(begin_record);

            // Create a page record (simulating operation)
            let page_record = Arc::new(LogRecord::new_page_record(
                txn_id,
                prev_lsn,
                LogRecordType::NewPage,
                0,
                1,
            ));
            prev_lsn = ctx.log_manager.append_log_record(page_record);

            // Commit transaction
            let commit_record = Arc::new(LogRecord::new_transaction_record(
                txn_id,
                prev_lsn,
                LogRecordType::Commit,
            ));
            let commit_lsn = ctx.log_manager.append_log_record(commit_record);

            // Verify persistent LSN includes the commit
            assert!(ctx.log_manager.get_persistent_lsn() >= commit_lsn);

            ctx.log_manager.shut_down();
        }

        #[test]
        fn test_multiple_transactions() {
            let mut ctx = TestContext::new("multi_txn_test");

            // Start flush thread
            ctx.log_manager.run_flush_thread();

            // Create multiple transactions with interleaved operations
            let txn1_id: TxnId = 1;
            let txn2_id: TxnId = 2;

            // Begin transaction 1
            let begin_txn1 = Arc::new(LogRecord::new_transaction_record(
                txn1_id,
                INVALID_LSN,
                LogRecordType::Begin,
            ));
            let txn1_begin_lsn = ctx.log_manager.append_log_record(begin_txn1);

            // Begin transaction 2
            let begin_txn2 = Arc::new(LogRecord::new_transaction_record(
                txn2_id,
                INVALID_LSN,
                LogRecordType::Begin,
            ));
            let txn2_begin_lsn = ctx.log_manager.append_log_record(begin_txn2);

            // Operation in transaction 1
            let op_txn1 = Arc::new(LogRecord::new_page_record(
                txn1_id,
                txn1_begin_lsn,
                LogRecordType::NewPage,
                0,
                1,
            ));
            let txn1_op_lsn = ctx.log_manager.append_log_record(op_txn1);

            // Operation in transaction 2
            let op_txn2 = Arc::new(LogRecord::new_page_record(
                txn2_id,
                txn2_begin_lsn,
                LogRecordType::NewPage,
                0,
                2,
            ));
            let txn2_op_lsn = ctx.log_manager.append_log_record(op_txn2);

            // Commit transaction 1
            let commit_txn1 = Arc::new(LogRecord::new_transaction_record(
                txn1_id,
                txn1_op_lsn,
                LogRecordType::Commit,
            ));
            let txn1_commit_lsn = ctx.log_manager.append_log_record(commit_txn1);

            // Verify txn1 is committed (persistent LSN should be >= commit LSN)
            assert!(ctx.log_manager.get_persistent_lsn() >= txn1_commit_lsn);

            // Abort transaction 2
            let abort_txn2 = Arc::new(LogRecord::new_transaction_record(
                txn2_id,
                txn2_op_lsn,
                LogRecordType::Abort,
            ));
            let txn2_abort_lsn = ctx.log_manager.append_log_record(abort_txn2);

            // Verify both transactions are persisted
            assert!(ctx.log_manager.get_persistent_lsn() >= txn2_abort_lsn);

            ctx.log_manager.shut_down();
        }
    }
}
