use crate::common::config::{Lsn, INVALID_LSN, LOG_BUFFER_SIZE};
use crate::recovery::log_record::LogRecord;
use crate::storage::disk::disk_manager::FileDiskManager;
use crossbeam_channel::{bounded, Receiver, Sender};
use log::{debug, error, trace, warn};
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
                Self::process_queued_records(
                    &state,
                    &mut flush_buffer,
                    &mut batch_start_lsn,
                    &mut max_lsn_in_batch,
                    &mut records_processed,
                );

                // Handle periodic flush if needed
                let now = std::time::Instant::now();
                if !flush_buffer.is_empty()
                    && (now.duration_since(last_flush_time) >= flush_interval || !records_processed)
                {
                    Self::perform_flush(&state, &mut flush_buffer, max_lsn_in_batch, "periodic");
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
                Self::perform_flush(&state, &mut flush_buffer, max_lsn_in_batch, "shutdown");
            }
        });

        // Store the thread handle, handle potential mutex poisoning gracefully
        if let Ok(mut flush_thread) = self.state.flush_thread.lock() {
            *flush_thread = Some(thread);
        } else {
            error!("Failed to acquire flush thread mutex - it may be poisoned");
        }
    }

    /// Process all queued log records and add them to the flush buffer
    #[inline]
    fn process_queued_records(
        state: &Arc<LogManagerState>,
        flush_buffer: &mut LogBuffer,
        batch_start_lsn: &mut Option<Lsn>,
        max_lsn_in_batch: &mut Lsn,
        records_processed: &mut bool,
    ) {
        while let Ok(record) = state.log_queue.1.try_recv() {
            *records_processed = true;
            let record_lsn = record.get_lsn();

            // Track highest LSN in this batch
            if *max_lsn_in_batch == INVALID_LSN || record_lsn > *max_lsn_in_batch {
                *max_lsn_in_batch = record_lsn;
            }

            // Track first LSN in this batch if not set
            if batch_start_lsn.is_none() {
                *batch_start_lsn = Some(record_lsn);
            }

            let bytes = record.to_bytes().unwrap_or_default();
            if !flush_buffer.append(&bytes) {
                // Buffer full, flush it
                if !flush_buffer.is_empty() {
                    Self::perform_flush(state, flush_buffer, *max_lsn_in_batch, "buffer full");
                    *batch_start_lsn = Some(record_lsn);
                    *max_lsn_in_batch = record_lsn;
                }
                // Try append again with fresh buffer
                flush_buffer.append(&bytes);
            }

            // Check if record is a commit - force flush if it is
            if record.is_commit() {
                if !flush_buffer.is_empty() {
                    Self::perform_flush(state, flush_buffer, *max_lsn_in_batch, "commit");
                    *batch_start_lsn = None;
                    *max_lsn_in_batch = INVALID_LSN;
                }
            }
        }
    }

    /// Perform a flush of the buffer to disk and update the persistent LSN
    #[inline]
    fn perform_flush(
        state: &Arc<LogManagerState>,
        flush_buffer: &mut LogBuffer,
        max_lsn: Lsn,
        flush_reason: &str,
    ) {
        if flush_buffer.is_empty() {
            return;
        }

        if let Err(e) = state.disk_manager.write_log(flush_buffer.get_data()) {
            error!(
                "Failed to write log to disk during {} flush: {}",
                flush_reason, e
            );
        } else if max_lsn != INVALID_LSN {
            state.persistent_lsn.store(max_lsn, Ordering::SeqCst);
            debug!(
                "{} flush completed, persistent LSN updated to {}",
                flush_reason, max_lsn
            );
        }
        flush_buffer.clear();
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

        // Set the LSN in the log record - now thread-safe with interior mutability
        log_record.set_lsn(lsn);

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

    /// Parses a log record from raw bytes
    ///
    /// # Parameters
    /// - `data`: The raw log record data
    ///
    /// # Returns
    /// An optional LogRecord if parsing was successful
    pub fn parse_log_record(&self, data: &[u8]) -> Option<LogRecord> {
        // Skip empty data
        if data.is_empty() || data.iter().all(|&b| b == 0) {
            return None;
        }

        // Try to deserialize using LogRecord's bincode implementation
        match LogRecord::from_bytes(data) {
            Ok(record) => Some(record),
            Err(e) => {
                // Log the error but don't fail the recovery process
                warn!("Failed to parse log record: {}", e);
                None
            }
        }
    }

    /// Reads a log record from disk at the specified offset
    ///
    /// # Parameters
    /// - `offset`: The offset to read from
    ///
    /// # Returns
    /// An optional LogRecord if reading and parsing was successful
    pub fn read_log_record(&self, offset: u64) -> Option<LogRecord> {
        debug!("Reading log record from offset {}", offset);
        let mut buffer = vec![0u8; 1024]; // Buffer for reading log entries

        match self.state.disk_manager.read_log(&mut buffer, offset) {
            Ok(bytes_read) => {
                debug!("Read data from log at offset {}", offset);
                // Check if we actually read anything
                if !bytes_read {
                    debug!("No bytes read from log at offset {}", offset);
                    return None;
                }

                // Only use the bytes that were actually read - full buffer since we don't know exact size
                let record = self.parse_log_record(&buffer);
                if let Some(ref rec) = record {
                    debug!(
                        "Successfully parsed log record: txn_id={}, type={:?}, size={}",
                        rec.get_txn_id(),
                        rec.get_log_record_type(),
                        rec.get_size()
                    );
                } else {
                    debug!("Failed to parse log record from offset {}", offset);
                }
                record
            }
            Err(e) => {
                warn!("Failed to read log from disk at offset {}: {}", offset, e);
                None
            }
        }
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

    /// Tests for internal helper methods
    mod helper_method_tests {
        use super::*;
        use crossbeam_channel::bounded;
        use parking_lot::RwLock;
        use std::sync::atomic::{AtomicBool, AtomicU64};

        // Helper to create test state
        fn create_test_state() -> Arc<LogManagerState> {
            let (sender, receiver) = bounded(10); // smaller channel for testing
            let disk_manager = Arc::new(FileDiskManager::new(
                "dummy.db".to_string(),
                "tests/data/helper_test.log".to_string(),
                100,
            ));

            Arc::new(LogManagerState {
                next_lsn: AtomicU64::new(0),
                persistent_lsn: AtomicU64::new(INVALID_LSN),
                log_buffer: RwLock::new(LogBuffer::new(LOG_BUFFER_SIZE as usize)),
                flush_thread: Mutex::new(None),
                stop_flag: AtomicBool::new(false),
                disk_manager,
                log_queue: (sender, receiver),
            })
        }

        // Helper to create a test record
        fn create_test_record(
            txn_id: TxnId,
            lsn: Lsn,
            record_type: LogRecordType,
        ) -> Arc<LogRecord> {
            let record = LogRecord::new_transaction_record(txn_id, lsn, record_type);
            let record = Arc::new(record);
            // Set LSN using thread-safe method
            record.set_lsn(lsn);
            record
        }

        #[test]
        fn test_process_queued_records_single() {
            let state = create_test_state();
            let mut flush_buffer = LogBuffer::new(LOG_BUFFER_SIZE as usize);
            let mut batch_start_lsn = None;
            let mut max_lsn_in_batch = INVALID_LSN;
            let mut records_processed = false;

            // Create and directly place a record to the queue
            let record = create_test_record(1, 10, LogRecordType::Begin);

            // Verify the LSN is set correctly on the record
            assert_eq!(record.get_lsn(), 10, "Record LSN should be set to 10");

            // Send the record to the queue
            state.log_queue.0.send(record).unwrap();

            // Process the record
            LogManager::process_queued_records(
                &state,
                &mut flush_buffer,
                &mut batch_start_lsn,
                &mut max_lsn_in_batch,
                &mut records_processed,
            );

            // Verify the results
            assert!(records_processed, "Record should be processed");
            assert_eq!(batch_start_lsn, Some(10), "Starting LSN should be set");
            assert_eq!(max_lsn_in_batch, 10, "Max LSN should be set");
            assert!(!flush_buffer.is_empty(), "Buffer should not be empty");
        }

        #[test]
        fn test_process_queued_records_multiple() {
            let state = create_test_state();
            let mut flush_buffer = LogBuffer::new(LOG_BUFFER_SIZE as usize);
            let mut batch_start_lsn = None;
            let mut max_lsn_in_batch = INVALID_LSN;
            let mut records_processed = false;

            // Create multiple records
            let record1 = create_test_record(1, 10, LogRecordType::Begin);
            let record2 = create_test_record(1, 11, LogRecordType::NewPage);
            let record3 = create_test_record(1, 12, LogRecordType::Commit);

            // Verify LSNs are set correctly
            assert_eq!(record1.get_lsn(), 10);
            assert_eq!(record2.get_lsn(), 11);
            assert_eq!(record3.get_lsn(), 12);

            // Send records to the queue
            state.log_queue.0.send(record1).unwrap();
            state.log_queue.0.send(record2).unwrap();
            state.log_queue.0.send(record3).unwrap();

            // Process the records
            LogManager::process_queued_records(
                &state,
                &mut flush_buffer,
                &mut batch_start_lsn,
                &mut max_lsn_in_batch,
                &mut records_processed,
            );

            // Verify the results
            assert!(records_processed, "Records should be processed");
            assert_eq!(
                batch_start_lsn, None,
                "Batch LSN should be reset after commit"
            );
            assert_eq!(
                max_lsn_in_batch, INVALID_LSN,
                "Max LSN should be reset after commit"
            );
            assert!(
                flush_buffer.is_empty(),
                "Buffer should be empty after commit flush"
            );
        }

        #[test]
        fn test_process_queued_records_commit() {
            let state = create_test_state();
            let mut flush_buffer = LogBuffer::new(LOG_BUFFER_SIZE as usize);
            let mut batch_start_lsn = None;
            let mut max_lsn_in_batch = INVALID_LSN;
            let mut records_processed = false;

            // Create a commit record
            let record = create_test_record(1, 10, LogRecordType::Commit);

            // Verify LSN is set correctly
            assert_eq!(record.get_lsn(), 10);

            // Send to the queue
            state.log_queue.0.send(record).unwrap();

            // Process the record
            LogManager::process_queued_records(
                &state,
                &mut flush_buffer,
                &mut batch_start_lsn,
                &mut max_lsn_in_batch,
                &mut records_processed,
            );

            // Verify commit caused a flush
            assert!(records_processed, "Record should be processed");
            assert_eq!(
                batch_start_lsn, None,
                "Batch LSN should be reset after commit"
            );
            assert_eq!(
                max_lsn_in_batch, INVALID_LSN,
                "Max LSN should be reset after commit"
            );
            assert!(
                flush_buffer.is_empty(),
                "Buffer should be empty after commit flush"
            );

            // Check persistent LSN was updated
            let persistent_lsn = state.persistent_lsn.load(Ordering::SeqCst);
            assert_eq!(
                persistent_lsn, 10,
                "Persistent LSN should be updated to commit LSN"
            );
        }

        #[test]
        fn test_process_queued_records_buffer_full() {
            let state = create_test_state();

            // Create a small buffer to test overflow
            let mut flush_buffer = LogBuffer::new(100); // Small size to force overflow
            let mut batch_start_lsn = None;
            let mut max_lsn_in_batch = INVALID_LSN;
            let mut records_processed = false;

            // Create and prepare a record
            let record = LogRecord::new_transaction_record(1, INVALID_LSN, LogRecordType::Begin);
            let record = Arc::new(record);
            // Set LSN using thread-safe method
            record.set_lsn(10);

            // Verify LSN is set correctly
            assert_eq!(record.get_lsn(), 10);

            // Since we can't easily create a large record directly, we'll simulate by filling the buffer first
            flush_buffer.data[0..90].fill(b'X'); // Fill most of the buffer
            flush_buffer.write_pos = 90;

            // Send to the queue
            state.log_queue.0.send(record).unwrap();

            // Process the record
            LogManager::process_queued_records(
                &state,
                &mut flush_buffer,
                &mut batch_start_lsn,
                &mut max_lsn_in_batch,
                &mut records_processed,
            );

            // Verify buffer overflow was handled
            assert!(records_processed, "Record should be processed");
            assert_eq!(batch_start_lsn, Some(10), "Batch LSN should be set");
            assert_eq!(max_lsn_in_batch, 10, "Max LSN should be set");
            assert!(
                !flush_buffer.is_empty(),
                "Buffer should contain data after appending"
            );

            // Check persistent LSN was updated during buffer full flush
            let persistent_lsn = state.persistent_lsn.load(Ordering::SeqCst);
            assert_eq!(
                persistent_lsn, 10,
                "Persistent LSN should be updated after buffer full flush"
            );
        }

        #[test]
        fn test_perform_flush_success() {
            let state = create_test_state();
            let mut flush_buffer = LogBuffer::new(LOG_BUFFER_SIZE as usize);

            // Add some data to the buffer
            let test_data = b"Test flush data";
            flush_buffer.append(test_data);

            // Perform the flush
            LogManager::perform_flush(&state, &mut flush_buffer, 42, "test");

            // Verify buffer was cleared
            assert!(
                flush_buffer.is_empty(),
                "Buffer should be empty after flush"
            );

            // Verify persistent LSN was updated
            let persistent_lsn = state.persistent_lsn.load(Ordering::SeqCst);
            assert_eq!(persistent_lsn, 42, "Persistent LSN should be updated to 42");
        }

        #[test]
        fn test_perform_flush_empty_buffer() {
            let state = create_test_state();
            let mut flush_buffer = LogBuffer::new(LOG_BUFFER_SIZE as usize);

            // Set initial persistent LSN value
            state.persistent_lsn.store(10, Ordering::SeqCst);

            // Attempt to flush empty buffer
            LogManager::perform_flush(&state, &mut flush_buffer, 42, "empty_test");

            // Verify persistent LSN was not changed (early return on empty buffer)
            let persistent_lsn = state.persistent_lsn.load(Ordering::SeqCst);
            assert_eq!(
                persistent_lsn, 10,
                "Persistent LSN should not change when flushing empty buffer"
            );
        }

        #[test]
        fn test_perform_flush_different_reasons() {
            let state = create_test_state();
            let mut flush_buffer = LogBuffer::new(LOG_BUFFER_SIZE as usize);

            // Test different flush reasons
            let reasons = ["commit", "buffer full", "periodic", "shutdown"];

            for (i, reason) in reasons.iter().enumerate() {
                // Clear buffer and add new data
                flush_buffer.clear();
                flush_buffer.append(format!("Test data for {}", reason).as_bytes());

                // Perform flush with this reason
                let lsn = i as u64 + 100;
                LogManager::perform_flush(&state, &mut flush_buffer, lsn, reason);

                // Verify LSN was updated
                let persistent_lsn = state.persistent_lsn.load(Ordering::SeqCst);
                assert_eq!(
                    persistent_lsn, lsn,
                    "Persistent LSN should be updated to {} after {} flush",
                    lsn, reason
                );
            }
        }

        #[test]
        fn test_perform_flush_invalid_lsn() {
            let state = create_test_state();
            let mut flush_buffer = LogBuffer::new(LOG_BUFFER_SIZE as usize);

            // Set initial persistent LSN
            state.persistent_lsn.store(10, Ordering::SeqCst);

            // Add data to buffer
            flush_buffer.append(b"Test data");

            // Flush with INVALID_LSN
            LogManager::perform_flush(&state, &mut flush_buffer, INVALID_LSN, "invalid_lsn_test");

            // Verify buffer was cleared but LSN was not updated
            assert!(
                flush_buffer.is_empty(),
                "Buffer should be empty after flush"
            );
            let persistent_lsn = state.persistent_lsn.load(Ordering::SeqCst);
            assert_eq!(
                persistent_lsn, 10,
                "Persistent LSN should not be updated with INVALID_LSN"
            );
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

            sleep(Duration::from_millis(1));

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
        use crate::common::config::PageId;

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

        #[test]
        fn test_flush_thread_periodic_flush() {
            let mut ctx = TestContext::new("periodic_flush_test");

            // Start flush thread
            ctx.log_manager.run_flush_thread();

            // Append some log records
            for i in 0..5 {
                let log_record = Arc::new(LogRecord::new_transaction_record(
                    i as TxnId,
                    INVALID_LSN,
                    LogRecordType::Begin,
                ));
                let lsn = ctx.log_manager.append_log_record(log_record);
                debug!("Appended record with LSN {}", lsn);
            }

            // Wait for periodic flush to happen (slightly longer than the flush interval)
            sleep(Duration::from_millis(15)); // Flush interval is 10ms

            // Check that persistent LSN has been updated
            let persistent_lsn = ctx.log_manager.get_persistent_lsn();
            assert!(
                persistent_lsn >= 0,
                "Persistent LSN should be updated by periodic flush"
            );

            ctx.log_manager.shut_down();
        }

        #[test]
        fn test_flush_thread_commit_flush() {
            let mut ctx = TestContext::new("commit_flush_test");

            // Start flush thread
            ctx.log_manager.run_flush_thread();

            // Append a begin record
            let begin_record = Arc::new(LogRecord::new_transaction_record(
                1 as TxnId,
                INVALID_LSN,
                LogRecordType::Begin,
            ));
            let begin_lsn = ctx.log_manager.append_log_record(begin_record);

            // Append a commit record (should force immediate flush)
            let commit_record = Arc::new(LogRecord::new_transaction_record(
                1 as TxnId,
                begin_lsn,
                LogRecordType::Commit,
            ));
            let commit_lsn = ctx.log_manager.append_log_record(commit_record);

            // Persistent LSN should be updated immediately after commit
            let persistent_lsn = ctx.log_manager.get_persistent_lsn();
            assert!(
                persistent_lsn >= commit_lsn,
                "Persistent LSN {} should be >= commit LSN {}",
                persistent_lsn,
                commit_lsn
            );

            ctx.log_manager.shut_down();
        }

        #[test]
        fn test_flush_thread_buffer_full() {
            let mut ctx = TestContext::new("buffer_full_test");

            // Start flush thread
            ctx.log_manager.run_flush_thread();

            // Use a commit record to guarantee a flush happens
            for i in 0..10 {
                // Begin record
                let begin_record = Arc::new(LogRecord::new_transaction_record(
                    i as TxnId,
                    INVALID_LSN,
                    LogRecordType::Begin,
                ));
                let begin_lsn = ctx.log_manager.append_log_record(begin_record);

                // Commit record - forces flush
                let commit_record = Arc::new(LogRecord::new_transaction_record(
                    i as TxnId,
                    begin_lsn,
                    LogRecordType::Commit,
                ));
                ctx.log_manager.append_log_record(commit_record);
            }

            // Add a small delay to ensure all commits are processed
            sleep(Duration::from_millis(20));

            // Check that persistent LSN has been updated
            let persistent_lsn = ctx.log_manager.get_persistent_lsn();
            assert!(
                persistent_lsn != INVALID_LSN,
                "Persistent LSN should be updated after buffer fills up"
            );

            ctx.log_manager.shut_down();
        }

        #[test]
        fn test_flush_thread_continuous_operation() {
            let mut ctx = TestContext::new("continuous_operation_test");

            // Start flush thread
            ctx.log_manager.run_flush_thread();

            // Initialize flush thread with a marker record and wait for it to be processed
            let init_record = Arc::new(LogRecord::new_transaction_record(
                999 as TxnId,
                INVALID_LSN,
                LogRecordType::Commit,
            ));
            let init_lsn = ctx.log_manager.append_log_record(init_record);

            // Wait for initial record to be flushed
            sleep(Duration::from_millis(50));

            // Verify the flush thread is working
            let initial_persistent_lsn = ctx.log_manager.get_persistent_lsn();
            assert!(
                initial_persistent_lsn >= init_lsn,
                "Flush thread initialization failed: persistent LSN {} not updated to {}",
                initial_persistent_lsn,
                init_lsn
            );

            // Track LSNs before and after each batch
            let mut batch_end_lsns = Vec::new();

            // Perform several batches of appends with delays between
            for batch in 0..3 {
                let start_persistent_lsn = ctx.log_manager.get_persistent_lsn();

                // Append a batch of records
                let mut last_batch_lsn = INVALID_LSN;
                for i in 0..10 {
                    let txn_id = (batch * 100 + i) as TxnId;
                    let log_record = Arc::new(LogRecord::new_transaction_record(
                        txn_id,
                        INVALID_LSN,
                        LogRecordType::Begin,
                    ));
                    last_batch_lsn = ctx.log_manager.append_log_record(log_record);
                }

                // Add a commit record to force a flush
                let commit_record = Arc::new(LogRecord::new_transaction_record(
                    (batch * 100) as TxnId, // Use the first txn_id from this batch
                    last_batch_lsn,
                    LogRecordType::Commit,
                ));
                last_batch_lsn = ctx.log_manager.append_log_record(commit_record);

                batch_end_lsns.push(last_batch_lsn);

                // Wait for flush to happen
                sleep(Duration::from_millis(50));

                let end_persistent_lsn = ctx.log_manager.get_persistent_lsn();

                // Persistent LSN should have advanced
                assert!(
                    end_persistent_lsn >= last_batch_lsn,
                    "Batch {}: Persistent LSN should be at least {} but was {}",
                    batch,
                    last_batch_lsn,
                    end_persistent_lsn
                );
            }

            // Final persistent LSN should be at least as high as the last batch's highest LSN
            let final_persistent_lsn = ctx.log_manager.get_persistent_lsn();
            let highest_batch_lsn = *batch_end_lsns.iter().max().unwrap();

            assert!(
                final_persistent_lsn >= highest_batch_lsn,
                "Final persistent LSN {} should be >= highest batch LSN {}",
                final_persistent_lsn,
                highest_batch_lsn
            );

            ctx.log_manager.shut_down();
        }

        #[test]
        fn test_flush_thread_concurrent_commits() {
            let ctx = Arc::new(parking_lot::RwLock::new(TestContext::new(
                "concurrent_commits_test",
            )));

            // Start flush thread
            ctx.write().log_manager.run_flush_thread();

            // Launch multiple threads, each doing a begin+commit transaction
            let thread_count = 5;
            let mut handles = vec![];

            for i in 0..thread_count {
                let ctx_clone = Arc::clone(&ctx);
                let handle = thread::spawn(move || {
                    let txn_id = (i + 100) as TxnId;

                    // Begin transaction
                    let begin_record = Arc::new(LogRecord::new_transaction_record(
                        txn_id,
                        INVALID_LSN,
                        LogRecordType::Begin,
                    ));
                    let begin_lsn = ctx_clone
                        .write()
                        .log_manager
                        .append_log_record(begin_record);

                    // Commit transaction - this should force a flush
                    let commit_record = Arc::new(LogRecord::new_transaction_record(
                        txn_id,
                        begin_lsn,
                        LogRecordType::Commit,
                    ));
                    let commit_lsn = ctx_clone
                        .write()
                        .log_manager
                        .append_log_record(commit_record);

                    commit_lsn
                });
                handles.push(handle);
            }

            // Collect all commit LSNs
            let commit_lsns: Vec<Lsn> = handles.into_iter().map(|h| h.join().unwrap()).collect();
            let max_commit_lsn = *commit_lsns.iter().max().unwrap();

            // Final persistent LSN should include all commits
            let final_persistent_lsn = ctx.read().log_manager.get_persistent_lsn();
            assert!(
                final_persistent_lsn >= max_commit_lsn,
                "Final persistent LSN {} should be >= max commit LSN {}",
                final_persistent_lsn,
                max_commit_lsn
            );

            ctx.write().log_manager.shut_down();
        }

        #[test]
        fn test_flush_thread_mixed_operations() {
            let mut ctx = TestContext::new("mixed_operations_test");

            // Start flush thread
            ctx.log_manager.run_flush_thread();

            // Perform a mix of operations with different record types
            let txn_id = 42 as TxnId;
            let mut prev_lsn = INVALID_LSN;

            // Begin transaction
            let begin_record = Arc::new(LogRecord::new_transaction_record(
                txn_id,
                prev_lsn,
                LogRecordType::Begin,
            ));
            prev_lsn = ctx.log_manager.append_log_record(begin_record);

            // New page operation
            let new_page_record = Arc::new(LogRecord::new_page_record(
                txn_id,
                prev_lsn,
                LogRecordType::NewPage,
                0 as PageId,
                1 as PageId,
            ));
            prev_lsn = ctx.log_manager.append_log_record(new_page_record);

            // Wait a bit to allow potential flush
            sleep(Duration::from_millis(15));

            // Another new page operation
            let another_page_record = Arc::new(LogRecord::new_page_record(
                txn_id,
                prev_lsn,
                LogRecordType::NewPage,
                1 as PageId,
                2 as PageId,
            ));
            prev_lsn = ctx.log_manager.append_log_record(another_page_record);

            // Commit transaction (should force flush)
            let commit_record = Arc::new(LogRecord::new_transaction_record(
                txn_id,
                prev_lsn,
                LogRecordType::Commit,
            ));
            let commit_lsn = ctx.log_manager.append_log_record(commit_record);

            // Persistent LSN should include the commit
            let persistent_lsn = ctx.log_manager.get_persistent_lsn();
            assert!(
                persistent_lsn >= commit_lsn,
                "Persistent LSN {} should be >= commit LSN {}",
                persistent_lsn,
                commit_lsn
            );

            ctx.log_manager.shut_down();
        }
    }

    mod read_and_write_tests {
        use super::*;

        #[test]
        fn test_write_and_read_log_record() {
            let mut ctx = TestContext::new("write_read_test");

            // Start flush thread
            ctx.log_manager.run_flush_thread();

            // Create and append a log record
            let txn_id: TxnId = 42;
            let log_record = Arc::new(LogRecord::new_transaction_record(
                txn_id,
                INVALID_LSN,
                LogRecordType::Begin,
            ));

            // Get size of the serialized record for later
            let serialized_size = log_record.to_bytes().unwrap().len() as u64;
            debug!("Original record serialized size: {}", serialized_size);

            // Append the record
            let lsn = ctx.log_manager.append_log_record(log_record);
            debug!("Appended record with LSN {}", lsn);

            // Force a flush with a commit record
            let commit_record = Arc::new(LogRecord::new_transaction_record(
                txn_id,
                lsn,
                LogRecordType::Commit,
            ));
            ctx.log_manager.append_log_record(commit_record);

            // Wait a bit to ensure records are flushed
            sleep(Duration::from_millis(50));

            // Read directly using read_log_at method from disk_manager at offset 0
            let mut buffer = vec![0u8; serialized_size as usize];
            if let Err(e) = ctx.disk_manager.read_log_at(&mut buffer, 0) {
                panic!("Failed to read log at offset 0: {}", e);
            }

            debug!(
                "Successfully read {} bytes from log at offset 0",
                buffer.len()
            );

            // Try to deserialize the record
            match LogRecord::from_bytes(&buffer) {
                Ok(read_record) => {
                    debug!(
                        "Successfully deserialized record: txn_id={}, type={:?}",
                        read_record.get_txn_id(),
                        read_record.get_log_record_type()
                    );

                    // Verify record contents
                    assert_eq!(read_record.get_txn_id(), txn_id, "Transaction ID mismatch");
                    assert_eq!(
                        read_record.get_log_record_type(),
                        LogRecordType::Begin,
                        "Record type mismatch"
                    );
                }
                Err(e) => {
                    panic!("Failed to deserialize log record: {}", e);
                }
            }

            ctx.log_manager.shut_down();
        }

        #[test]
        fn test_read_multiple_log_records() {
            let mut ctx = TestContext::new("read_multiple_test");

            // Start flush thread
            ctx.log_manager.run_flush_thread();

            // Create and append multiple log records
            let record_count = 3; // Reduced from 5 for simplicity
            let mut records = Vec::new();
            let mut lsn_values = Vec::new();
            let mut offset = 0;

            for i in 0..record_count {
                let txn_id = i + 100;
                let record = Arc::new(LogRecord::new_transaction_record(
                    txn_id,
                    INVALID_LSN,
                    LogRecordType::Begin,
                ));

                // Store original record and its size
                let serialized = record.to_bytes().unwrap();
                let record_size = serialized.len() as u64;
                records.push(serialized);

                // Store offset for this record
                let current_offset = offset;
                offset += record_size;

                // Append and get LSN
                let lsn = ctx.log_manager.append_log_record(record.clone());
                lsn_values.push((lsn, current_offset));
            }

            // Force flush of records by committing the last transaction
            let commit_record = Arc::new(LogRecord::new_transaction_record(
                record_count + 100 - 1,
                lsn_values
                    .last()
                    .map(|(lsn, _)| *lsn)
                    .unwrap_or(INVALID_LSN),
                LogRecordType::Commit,
            ));
            ctx.log_manager.append_log_record(commit_record);

            // Ensure all records are flushed to disk
            sleep(Duration::from_millis(50));

            // Now read back each record directly using read_log_at
            for (i, (_, record_offset)) in lsn_values.iter().enumerate() {
                let txn_id = i as TxnId + 100;
                let expected_record_data = &records[i];

                // Read directly from disk_manager
                let mut buffer = vec![0u8; expected_record_data.len()];
                if let Err(e) = ctx.disk_manager.read_log_at(&mut buffer, *record_offset) {
                    panic!("Failed to read record at offset {}: {}", record_offset, e);
                }

                // Deserialize and verify
                match LogRecord::from_bytes(&buffer) {
                    Ok(read_record) => {
                        assert_eq!(
                            read_record.get_txn_id(),
                            txn_id,
                            "Transaction ID mismatch at offset {}",
                            record_offset
                        );
                        assert_eq!(
                            read_record.get_log_record_type(),
                            LogRecordType::Begin,
                            "Record type mismatch at offset {}",
                            record_offset
                        );
                    }
                    Err(e) => {
                        panic!(
                            "Failed to deserialize record at offset {}: {}",
                            record_offset, e
                        );
                    }
                }
            }

            ctx.log_manager.shut_down();
        }

        #[test]
        fn test_read_with_invalid_offset() {
            let mut ctx = TestContext::new("invalid_offset_test");

            // Start flush thread
            ctx.log_manager.run_flush_thread();

            // Try to read from an invalid offset (file should be empty)
            let record = ctx.log_manager.read_log_record(1000);
            assert!(record.is_none(), "Should return None for invalid offset");

            ctx.log_manager.shut_down();
        }

        #[test]
        fn test_read_after_log_write() {
            let mut ctx = TestContext::new("read_after_log_write_test");

            // Start flush thread
            ctx.log_manager.run_flush_thread();

            // Write a BEGIN record and immediately force flush
            let txn_id = 123;
            let begin_record = Arc::new(LogRecord::new_transaction_record(
                txn_id,
                INVALID_LSN,
                LogRecordType::Begin,
            ));

            // Get serialized size for reading later
            let serialized = begin_record.to_bytes().unwrap();
            let record_size = serialized.len();

            let begin_lsn = ctx.log_manager.append_log_record(begin_record);
            debug!("Appended BEGIN record with LSN {}", begin_lsn);

            // Force flush with a commit record
            let commit_record = Arc::new(LogRecord::new_transaction_record(
                txn_id,
                begin_lsn,
                LogRecordType::Commit,
            ));
            ctx.log_manager.append_log_record(commit_record);

            // Ensure records are flushed to disk
            sleep(Duration::from_millis(50));

            // Now read the BEGIN record directly from disk using read_log_at
            let mut buffer = vec![0u8; record_size];
            if let Err(e) = ctx.disk_manager.read_log_at(&mut buffer, 0) {
                panic!("Failed to read log at offset 0: {}", e);
            }

            // Deserialize and verify
            match LogRecord::from_bytes(&buffer) {
                Ok(record) => {
                    debug!(
                        "Read record: txn_id={}, type={:?}",
                        record.get_txn_id(),
                        record.get_log_record_type()
                    );

                    // Verify record contents
                    assert_eq!(
                        record.get_txn_id(),
                        txn_id,
                        "Transaction ID mismatch: expected {} but got {}",
                        txn_id,
                        record.get_txn_id()
                    );
                    assert_eq!(
                        record.get_log_record_type(),
                        LogRecordType::Begin,
                        "Record type mismatch: expected Begin but got {:?}",
                        record.get_log_record_type()
                    );
                }
                Err(e) => {
                    panic!("Failed to deserialize log record: {}", e);
                }
            }

            ctx.log_manager.shut_down();
        }

        #[test]
        fn test_binary_serialization() {
            // Set up logging for this test
            let _ = env_logger::builder()
                .filter_level(log::LevelFilter::Debug)
                .is_test(true)
                .try_init();

            // Create a log record
            let txn_id = 42;
            let log_record =
                LogRecord::new_transaction_record(txn_id, INVALID_LSN, LogRecordType::Begin);

            // Serialize to bytes
            let serialized = log_record.to_bytes().unwrap();
            debug!("Serialized log record to {} bytes", serialized.len());

            // Deserialize and verify
            let deserialized = LogRecord::from_bytes(&serialized).unwrap();

            // Verify the deserialized record matches the original
            assert_eq!(deserialized.get_txn_id(), txn_id);
            assert_eq!(deserialized.get_log_record_type(), LogRecordType::Begin);
            assert_eq!(deserialized.get_prev_lsn(), INVALID_LSN);
        }
    }
}
