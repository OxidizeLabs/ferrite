use crate::recovery::log_record::{LogRecord, LogRecordType};
use log::{debug, warn};
use std::sync::Arc;
use crate::storage::disk::async_disk_manager::AsyncDiskManager;
use tokio::runtime::Handle;

/// `LogIterator` provides a robust way to iterate through log records in a log file.
/// It handles EOF detection, validation of records, and proper advancing through the file.
pub struct LogIterator {
    disk_manager: Arc<AsyncDiskManager>,
    current_offset: u64,
    reached_eof: bool,
    buffer_size: usize,
    runtime_handle: Handle,
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
            runtime_handle: Handle::current(),
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
            runtime_handle: Handle::current(),
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

        // Maximum number of attempts to find a valid record
        let max_attempts = 100;
        let mut attempts = 0;

        while attempts < max_attempts {
            let disk_manager = Arc::clone(&self.disk_manager);
            let offset = self.current_offset;

            match tokio::task::block_in_place(|| {
                self.runtime_handle.block_on(async move {
                    disk_manager.read_log(offset).await
                })
            }) {
                Ok(record) => {
                    // Additional validation for records to catch corrupted entries
                    let record_size = record.get_size();
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
                        "Failed to read log record at offset {}: {}",
                        self.current_offset, e
                    );

                    // Check if this is an EOF error
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        debug!("Reached EOF at offset {}", self.current_offset);
                        self.reached_eof = true;
                        return None;
                    }

                    // Skip forward by 1 byte and try again
                    self.current_offset += 1;
                    attempts += 1;
                    continue;
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

        async fn write_record(&self, record: &LogRecord) {
            self.disk_manager.write_log(record).await.unwrap();
        }

        fn create_test_begin_record(&self, txn_id: TxnId) -> LogRecord {
            let record =
                LogRecord::new_transaction_record(txn_id, INVALID_LSN, LogRecordType::Begin);
            // Set a dummy LSN
            record.set_lsn(txn_id as Lsn);
            record
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            self.cleanup();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_empty_log_file() {
        let ctx = TestContext::new("empty_log_file").await;
        let mut iter = LogIterator::new(ctx.disk_manager.clone());

        // Should return None for empty file
        assert!(iter.next().is_none());
        assert!(iter.is_end());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_single_record() {
        let ctx = TestContext::new("single_record").await;

        // Write a single BEGIN record
        let record = ctx.create_test_begin_record(1);
        ctx.write_record(&record).await;

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
}
