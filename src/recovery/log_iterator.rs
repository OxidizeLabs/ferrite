use crate::recovery::log_record::{LogRecord, LogRecordType};
use log::{debug, warn};
use std::sync::Arc;
use crate::storage::disk::async_disk_manager::AsyncDiskManager;

/// `LogIterator` provides a robust way to iterate through log records in a log file.
/// It handles EOF detection, validation of records, and proper advancing through the file.
pub struct LogIterator {
    disk_manager: Arc<AsyncDiskManager>,
    current_offset: u64,
    reached_eof: bool,
    buffer_size: usize,
}

impl LogIterator {
    /// Creates a new `LogIterator` starting from the beginning of the log file.
    ///
    /// # Parameters
    /// - `disk_manager`: A reference to the disk manager.
    ///
    /// # Returns
    /// A new `LogIterator` instance.
    pub fn new(disk_manager: Arc<AsyncDiskManager>) -> Self {
        Self {
            disk_manager,
            current_offset: 0,
            reached_eof: false,
            buffer_size: 1024, // Default buffer size
        }
    }

    /// Creates a new `LogIterator` starting from a specific offset.
    ///
    /// # Parameters
    /// - `disk_manager`: A reference to the disk manager.
    /// - `start_offset`: The offset to start reading from.
    ///
    /// # Returns
    /// A new `LogIterator` instance.
    pub fn with_offset(disk_manager: Arc<AsyncDiskManager>, start_offset: u64) -> Self {
        Self {
            disk_manager,
            current_offset: start_offset,
            reached_eof: false,
            buffer_size: 1024,
        }
    }

    /// Gets the next log record.
    ///
    /// # Returns
    /// - `Some(LogRecord)` if a valid record was read.
    /// - `None` if EOF was reached or an error occurred.
    pub fn next(&mut self) -> Option<LogRecord> {
        if self.reached_eof {
            return None;
        }

        let mut buffer = vec![0u8; self.buffer_size];

        // Maximum number of attempts to find a valid record
        let max_attempts = 100;
        let mut attempts = 0;

        while attempts < max_attempts {
            match self.disk_manager.read_log(&mut buffer, self.current_offset) {
                Ok(true) => {
                    // Check if the buffer contains an empty/all-zeros record
                    // which might indicate we've reached the end of valid data
                    if buffer.iter().all(|&b| b == 0) {
                        debug!(
                            "Reached end of log file data at offset {}",
                            self.current_offset
                        );
                        self.reached_eof = true;
                        return None;
                    }

                    // Attempt to parse the log record
                    match LogRecord::from_bytes(&buffer) {
                        Ok(record) => {
                            let record_size = record.get_size();

                            // Additional validation for records to catch corrupted entries
                            // that somehow pass the basic parsing
                            if record_size <= 0
                                || record_size > 1_000_000
                                || record.get_log_record_type() == LogRecordType::Invalid
                            {
                                warn!(
                                    "Invalid record detected at offset {}: size={}, type={:?}, skipping",
                                    self.current_offset,
                                    record_size,
                                    record.get_log_record_type()
                                );
                                self.current_offset += 1;
                                attempts += 1;
                                continue;
                            }

                            debug!(
                                "Read valid log record: type={:?}, size={}, LSN={}, txn_id={}",
                                record.get_log_record_type(),
                                record_size,
                                record.get_lsn(),
                                record.get_txn_id()
                            );

                            // Advance to next record
                            self.current_offset += record_size as u64;
                            return Some(record);
                        }
                        Err(e) => {
                            warn!(
                                "Failed to parse log record at offset {}: {}",
                                self.current_offset, e
                            );

                            // Skip forward by 1 byte and try again
                            self.current_offset += 1;
                            attempts += 1;

                            // Continue to next attempt without returning
                            continue;
                        }
                    }
                }
                Ok(false) | Err(_) => {
                    // Error reading data or explicit EOF indication
                    debug!("Reached EOF or error at offset {}", self.current_offset);
                    self.reached_eof = true;
                    return None;
                }
            }
        }

        // If we've made too many attempts without finding a valid record,
        // assume we've reached the end of valid data
        warn!(
            "Made {} unsuccessful attempts to parse log records starting at offset {}, giving up",
            max_attempts, self.current_offset
        );
        self.reached_eof = true;
        None
    }

    /// Resets the iterator to the beginning of the log file.
    pub fn reset(&mut self) {
        self.current_offset = 0;
        self.reached_eof = false;
    }

    /// Sets the iterator to start at a specific offset.
    ///
    /// # Parameters
    /// - `offset`: The offset to start reading from.
    pub fn set_offset(&mut self, offset: u64) {
        self.current_offset = offset;
        self.reached_eof = false;
    }

    /// Gets the current offset of the iterator.
    ///
    /// # Returns
    /// The current offset.
    pub fn get_offset(&self) -> u64 {
        self.current_offset
    }

    /// Checks if the iterator has reached the end of the log file.
    ///
    /// # Returns
    /// `true` if the end of the log file has been reached, `false` otherwise.
    pub fn is_end(&self) -> bool {
        self.reached_eof
    }

    /// Sets the buffer size for reading log records.
    ///
    /// # Parameters
    /// - `size`: The buffer size in bytes.
    pub fn set_buffer_size(&mut self, size: usize) {
        self.buffer_size = size;
    }

    /// Collects all remaining log records into a vector.
    ///
    /// # Returns
    /// A vector of log records.
    pub fn collect(&mut self) -> Vec<LogRecord> {
        let mut records = Vec::new();
        while let Some(record) = self.next() {
            records.push(record);
        }
        records
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::config::{Lsn, TxnId, INVALID_LSN};
    use crate::recovery::log_record::{LogRecord, LogRecordType};
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::TempDir;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::async_disk_manager::DiskManagerConfig;

    struct TestContext {
        log_path: String,
        disk_manager: Arc<AsyncDiskManager>,
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 10;
            const K: usize = 2;

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

            // Create disk components
            let disk_manager =
                AsyncDiskManager::new(db_path, log_path.clone(), DiskManagerConfig::default())
                    .await;
            let disk_manager_arc = Arc::new(disk_manager.unwrap());

            Self {
                log_path,
                disk_manager: disk_manager_arc,
            }
        }

        fn cleanup(&self) {
            if Path::new(&self.log_path).exists() {
                fs::remove_file(&self.log_path).unwrap();
            }
        }

        fn write_record(&self, record: &LogRecord) {
            let bytes = record.to_bytes().unwrap();
            self.disk_manager.write_log(&bytes).unwrap();
        }

        fn create_test_begin_record(&self, txn_id: TxnId) -> LogRecord {
            let record =
                LogRecord::new_transaction_record(txn_id, INVALID_LSN, LogRecordType::Begin);
            // Set a dummy LSN
            record.set_lsn(txn_id as Lsn);
            record
        }

        fn create_test_commit_record(&self, txn_id: TxnId, prev_lsn: Lsn) -> LogRecord {
            let record = LogRecord::new_transaction_record(txn_id, prev_lsn, LogRecordType::Commit);
            // Set a dummy LSN
            record.set_lsn((txn_id + 1) as Lsn);
            record
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            self.cleanup();
        }
    }

    #[tokio::test]
    async fn test_empty_log_file() {
        let ctx = TestContext::new("empty_log_file").await;
        let mut iter = LogIterator::new(ctx.disk_manager.clone());

        // Should return None for empty file
        assert!(iter.next().is_none());
        assert!(iter.is_end());
    }

    #[tokio::test]
    async fn test_single_record() {
        let ctx = TestContext::new("single_record").await;

        // Write a single BEGIN record
        let record = ctx.create_test_begin_record(1);
        ctx.write_record(&record);

        // Create iterator and read the record
        let mut iter = LogIterator::new(ctx.disk_manager.clone());
        let read_record = iter.next();

        // Verify record was read correctly
        assert!(read_record.is_some());
        let read_record = read_record.unwrap();
        assert_eq!(read_record.get_txn_id(), 1);
        assert_eq!(read_record.get_log_record_type(), LogRecordType::Begin);

        // Should return None for second read
        assert!(iter.next().is_none());
        assert!(iter.is_end());
    }

    #[tokio::test]
    async fn test_multiple_records() {
        let ctx = TestContext::new("multiple_records").await;

        // Write multiple records
        for i in 1..=5 {
            let record = ctx.create_test_begin_record(i);
            ctx.write_record(&record);
        }

        // Create iterator and read all records
        let mut iter = LogIterator::new(ctx.disk_manager.clone());
        let mut records = Vec::new();

        while let Some(record) = iter.next() {
            records.push(record);
        }

        // Verify we read 5 records with the correct transaction IDs
        assert_eq!(records.len(), 5);
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.get_txn_id(), (i + 1) as TxnId);
            assert_eq!(record.get_log_record_type(), LogRecordType::Begin);
        }

        assert!(iter.is_end());
    }

    #[tokio::test]
    async fn test_reset_iterator() {
        let ctx = TestContext::new("reset_iterator").await;

        // Write records
        for i in 1..=3 {
            let record = ctx.create_test_begin_record(i);
            ctx.write_record(&record);
        }

        // Create iterator and read all records
        let mut iter = LogIterator::new(ctx.disk_manager.clone());
        let mut first_pass = Vec::new();

        while let Some(record) = iter.next() {
            first_pass.push(record);
        }

        assert_eq!(first_pass.len(), 3);
        assert!(iter.is_end());

        // Reset and read again
        iter.reset();
        assert!(!iter.is_end());

        let mut second_pass = Vec::new();
        while let Some(record) = iter.next() {
            second_pass.push(record);
        }

        // Both passes should have the same records
        assert_eq!(second_pass.len(), 3);
        for i in 0..3 {
            assert_eq!(first_pass[i].get_txn_id(), second_pass[i].get_txn_id());
        }
    }

    #[tokio::test]
    async fn test_set_offset() {
        let ctx = TestContext::new("set_offset").await;

        // Write records
        let mut offsets = Vec::new();
        let mut current_offset = 0;

        for i in 1..=5 {
            offsets.push(current_offset);
            let record = ctx.create_test_begin_record(i);
            let bytes = record.to_bytes().unwrap();
            ctx.disk_manager.write_log(&bytes).unwrap();
            current_offset += bytes.len() as u64;
        }

        // Start iterator at the third record (index 2)
        let mut iter = LogIterator::with_offset(ctx.disk_manager.clone(), offsets[2]);

        // Should get txn_id 3
        let record = iter.next().unwrap();
        assert_eq!(record.get_txn_id(), 3);

        // Set to first record
        iter.set_offset(offsets[0]);
        let record = iter.next().unwrap();
        assert_eq!(record.get_txn_id(), 1);
    }

    #[tokio::test]
    async fn test_transaction_sequence() {
        let ctx = TestContext::new("transaction_sequence").await;

        // Write a BEGIN and COMMIT pair
        let begin_record = ctx.create_test_begin_record(42);
        let begin_lsn = begin_record.get_lsn();
        ctx.write_record(&begin_record);

        let commit_record = ctx.create_test_commit_record(42, begin_lsn);
        ctx.write_record(&commit_record);

        // Read and verify the sequence
        let mut iter = LogIterator::new(ctx.disk_manager.clone());

        // We should get exactly two valid records
        let records = iter.collect();
        assert_eq!(
            records.len(),
            2,
            "Expected exactly 2 records, got {}",
            records.len()
        );

        // Verify the first record is BEGIN
        assert_eq!(records[0].get_txn_id(), 42);
        assert_eq!(records[0].get_log_record_type(), LogRecordType::Begin);

        // Verify the second record is COMMIT with proper prev_lsn
        assert_eq!(records[1].get_txn_id(), 42);
        assert_eq!(records[1].get_log_record_type(), LogRecordType::Commit);
        assert_eq!(records[1].get_prev_lsn(), begin_lsn);

        // The iterator should be at the end now
        assert!(iter.is_end(), "Iterator should be at the end");
    }

    #[tokio::test]
    async fn test_collect_method() {
        let ctx = TestContext::new("collect_method").await;

        // Write multiple records
        for i in 1..=5 {
            let record = ctx.create_test_begin_record(i);
            ctx.write_record(&record);
        }

        // Collect all records at once
        let mut iter = LogIterator::new(ctx.disk_manager.clone());
        let records = iter.collect();

        // Verify
        assert_eq!(records.len(), 5);
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.get_txn_id(), (i + 1) as TxnId);
        }

        assert!(iter.is_end());
    }

    #[tokio::test]
    async fn test_buffer_size_change() {
        let ctx = TestContext::new("buffer_size").await;

        // Write a record
        let record = ctx.create_test_begin_record(1);
        ctx.write_record(&record);

        // Use a very small buffer size that won't fit the record
        let mut iter = LogIterator::new(ctx.disk_manager.clone());
        iter.set_buffer_size(4); // Too small for any real record

        // This should still work because we'll skip ahead on parsing failure
        let result = iter.next();
        assert!(result.is_none()); // Will fail to parse with tiny buffer

        // Reset with proper buffer size
        iter.reset();
        iter.set_buffer_size(1024);

        // Now should succeed
        let record = iter.next();
        assert!(record.is_some());
        assert_eq!(record.unwrap().get_txn_id(), 1);
    }

    // This test writes corrupted data to the log file and verifies
    // the iterator can recover and continue reading valid records
    #[tokio::test]
    async fn test_recovery_from_corruption() {
        let ctx = TestContext::new("corruption_recovery").await;

        // Write a valid record
        let record1 = ctx.create_test_begin_record(1);
        ctx.write_record(&record1);

        // Write some corrupted bytes
        let corrupted = [0xFF; 20]; // Invalid data
        ctx.disk_manager.write_log(&corrupted).unwrap();

        // Write another valid record
        let record2 = ctx.create_test_begin_record(2);
        ctx.write_record(&record2);

        // Create iterator
        let mut iter = LogIterator::new(ctx.disk_manager.clone());

        // Should be able to read the first record
        let read1 = iter.next();
        assert!(read1.is_some());
        assert_eq!(read1.unwrap().get_txn_id(), 1);

        // With our improved corruption recovery, we should be able to
        // skip over the corrupted data and find the second record
        let read2 = iter.next();
        assert!(read2.is_some());
        assert_eq!(read2.unwrap().get_txn_id(), 2);

        // After reading all valid records, we should reach EOF
        assert!(iter.next().is_none());
        assert!(iter.is_end());
    }
}
