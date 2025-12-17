use crate::recovery::log_record::{LogRecord, LogRecordType};
use crate::storage::disk::async_disk::AsyncDiskManager;
use crate::storage::disk::direct_io::round_up_to_alignment;
use crate::common::config::DB_PAGE_SIZE;
use log::{debug, warn};
use std::sync::Arc;
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

        // Maximum number of attempts to find a valid record (for corruption resync).
        let max_attempts = 100;
        let mut attempts = 0;

        while attempts < max_attempts {
            let disk_manager = Arc::clone(&self.disk_manager);
            let offset = self.current_offset;

            match tokio::task::block_in_place(|| {
                self.runtime_handle
                    .block_on(async move { disk_manager.read_log_sized(offset, 4).await })
            }) {
                Ok(header_bytes) => {
                    if header_bytes.len() != 4 {
                        warn!(
                            "Short log header read at offset {}: {} bytes",
                            self.current_offset,
                            header_bytes.len()
                        );
                        self.reached_eof = true;
                        return None;
                    }

                    let size_bytes: [u8; 4] = header_bytes.as_slice().try_into().unwrap();
                    let record_size = i32::from_le_bytes(size_bytes);

                    // Basic validation.
                    if record_size <= 0 || record_size > 1_000_000 {
                        warn!(
                            "Invalid record size at offset {}: size={}, skipping",
                            self.current_offset, record_size
                        );

                        // If direct I/O is enabled, reads/writes are alignment-sensitive and we
                        // store records at alignment boundaries (with padding). Skip to the next
                        // aligned boundary to resync quickly.
                        if self.disk_manager.get_config().direct_io {
                            self.current_offset += DB_PAGE_SIZE;
                        } else {
                            self.current_offset += 1;
                        }

                        attempts += 1;
                        continue;
                    }

                    // Read the full record bytes (exact logical size).
                    let record_bytes = match tokio::task::block_in_place(|| {
                        let disk_manager = Arc::clone(&self.disk_manager);
                        self.runtime_handle.block_on(async move {
                            disk_manager.read_log_sized(offset, record_size as usize).await
                        })
                    }) {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            warn!(
                                "Failed to read full log record at offset {} (size={}): {}",
                                self.current_offset, record_size, e
                            );
                            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                                self.reached_eof = true;
                                return None;
                            }

                            if self.disk_manager.get_config().direct_io {
                                self.current_offset += DB_PAGE_SIZE;
                            } else {
                                self.current_offset += 1;
                            }
                            attempts += 1;
                            continue;
                        }
                    };

                    // Deserialize record bytes into LogRecord.
                    let record = match LogRecord::from_bytes(&record_bytes) {
                        Ok(record) => record,
                        Err(e) => {
                            warn!(
                                "Failed to deserialize log record at offset {}: {}",
                                self.current_offset, e
                            );

                            if self.disk_manager.get_config().direct_io {
                                self.current_offset += DB_PAGE_SIZE;
                            } else {
                                self.current_offset += 1;
                            }
                            attempts += 1;
                            continue;
                        }
                    };

                    // Additional validation for records to catch corrupted entries
                    if record.get_log_record_type() == LogRecordType::Invalid {
                        warn!(
                            "Invalid record detected at offset {}: size={}, type={:?}, skipping",
                            self.current_offset,
                            record_size,
                            record.get_log_record_type()
                        );
                        if self.disk_manager.get_config().direct_io {
                            self.current_offset += DB_PAGE_SIZE;
                        } else {
                            self.current_offset += 1;
                        }
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
                    if self.disk_manager.get_config().direct_io {
                        let physical_advance = round_up_to_alignment(
                            record_size as usize,
                            DB_PAGE_SIZE as usize,
                        ) as u64;
                        self.current_offset += physical_advance;
                    } else {
                        self.current_offset += record_size as u64;
                    }
                    return Some(record);
                }
                Err(e) => {
                    // Check if this is an EOF error
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        debug!("Reached EOF at offset {}", self.current_offset);
                        self.reached_eof = true;
                        return None;
                    }

                    warn!(
                        "Failed to read log record at offset {}: {}",
                        self.current_offset, e
                    );

                    // Resync on error.
                    if self.disk_manager.get_config().direct_io {
                        self.current_offset += DB_PAGE_SIZE;
                    } else {
                        self.current_offset += 1;
                    }
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
    use crate::common::config::{INVALID_LSN, Lsn, TxnId};
    use crate::common::logger::initialize_logger;
    use crate::recovery::log_record::{LogRecord, LogRecordType};
    use crate::storage::disk::async_disk::DiskManagerConfig;
    use std::sync::Arc;
    use tempfile::TempDir;

    struct TestContext {
        _temp_dir: TempDir,
        log_path: String,
        disk_manager: Arc<AsyncDiskManager>,
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
            // Default to buffered I/O for deterministic small-record tests.
            // (Direct I/O forces page-sized reads/writes and changes physical offsets.)
            Self::new_with_config(
                name,
                DiskManagerConfig {
                    direct_io: false,
                    ..DiskManagerConfig::default()
                },
            )
            .await
        }

        pub async fn new_with_config(name: &str, config: DiskManagerConfig) -> Self {
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
                AsyncDiskManager::new(db_path, log_path.clone(), config).await;
            let disk_manager_arc = Arc::new(disk_manager.unwrap());

            Self {
                _temp_dir: temp_dir,
                log_path,
                disk_manager: disk_manager_arc,
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

    fn record_logical_size_bytes(record: &LogRecord) -> u64 {
        record.to_bytes().unwrap().len() as u64
    }

    fn round_up_to_page(size: u64) -> u64 {
        round_up_to_alignment(size as usize, DB_PAGE_SIZE as usize) as u64
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_multiple_records_preserve_order_and_offsets_buffered_io() {
        let ctx = TestContext::new("multiple_records_buffered").await;

        // Write a few BEGIN records.
        let mut expected_sizes = Vec::new();
        let mut expected_txns = Vec::new();
        let mut expected_offsets = Vec::new();

        for txn_id in 1..=5 {
            let record = ctx.create_test_begin_record(txn_id);
            let size = record_logical_size_bytes(&record);
            expected_sizes.push(size);
            expected_txns.push(txn_id);

            // When direct I/O is disabled, `write_log` should append without padding.
            let offset = ctx.disk_manager.write_log(&record).await.unwrap();
            expected_offsets.push(offset);
        }

        // Invariant: contiguous layout with no padding when direct I/O is off.
        assert!(!ctx.disk_manager.get_config().direct_io);
        assert_eq!(expected_offsets[0], 0);
        let mut running = 0u64;
        for i in 0..expected_sizes.len() {
            assert_eq!(expected_offsets[i], running);
            running += expected_sizes[i];
        }

        // Read back and verify ordering + offset advancement invariants.
        let mut iter = LogIterator::new(ctx.disk_manager.clone());
        let mut current_expected_offset = 0u64;

        for (i, txn_id) in expected_txns.iter().copied().enumerate() {
            assert_eq!(iter.get_offset(), current_expected_offset);
            let rec = iter.next().expect("expected record");
            assert_eq!(rec.get_txn_id(), txn_id);
            assert_eq!(rec.get_lsn(), txn_id as Lsn);
            assert_eq!(rec.get_prev_lsn(), INVALID_LSN);
            assert_eq!(rec.get_log_record_type(), LogRecordType::Begin);

            current_expected_offset += expected_sizes[i];
            assert_eq!(iter.get_offset(), current_expected_offset);
        }

        assert!(iter.next().is_none());
        assert!(iter.is_end());

        // `reset` should allow re-reading from the start.
        iter.reset();
        assert!(!iter.is_end());
        assert_eq!(iter.get_offset(), 0);
        let first = iter.next().expect("expected first record after reset");
        assert_eq!(first.get_txn_id(), 1);

        // `set_offset` should also clear EOF state.
        while iter.next().is_some() {}
        assert!(iter.is_end());
        iter.set_offset(0);
        assert!(!iter.is_end());
        assert_eq!(iter.get_offset(), 0);
        assert!(iter.next().is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_with_offset_reads_from_record_boundary() {
        let ctx = TestContext::new("with_offset_boundary").await;

        let r1 = ctx.create_test_begin_record(10);
        let r2 = ctx.create_test_begin_record(20);
        let r3 = ctx.create_test_begin_record(30);

        let s1 = record_logical_size_bytes(&r1);
        let s2 = record_logical_size_bytes(&r2);
        let _s3 = record_logical_size_bytes(&r3);

        let o1 = ctx.disk_manager.write_log(&r1).await.unwrap();
        let o2 = ctx.disk_manager.write_log(&r2).await.unwrap();
        let o3 = ctx.disk_manager.write_log(&r3).await.unwrap();

        assert_eq!(o1, 0);
        assert_eq!(o2, s1);
        assert_eq!(o3, s1 + s2);

        // Start from the 2nd record boundary.
        let mut iter = LogIterator::with_offset(ctx.disk_manager.clone(), o2);
        let rec2 = iter.next().expect("expected second record");
        assert_eq!(rec2.get_txn_id(), 20);
        assert_eq!(iter.get_offset(), o2 + s2);

        let rec3 = iter.next().expect("expected third record");
        assert_eq!(rec3.get_txn_id(), 30);
        assert!(iter.next().is_none());
        assert!(iter.is_end());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_collect_matches_manual_iteration() {
        let ctx = TestContext::new("collect_matches_manual").await;

        for txn_id in 1..=4 {
            let record = ctx.create_test_begin_record(txn_id);
            ctx.write_record(&record).await;
        }

        let mut iter1 = LogIterator::new(ctx.disk_manager.clone());
        let mut iter2 = LogIterator::new(ctx.disk_manager.clone());

        let manual: Vec<(TxnId, Lsn, LogRecordType)> = std::iter::from_fn(|| iter1.next())
            .map(|r| (r.get_txn_id(), r.get_lsn(), r.get_log_record_type()))
            .collect();

        let collected: Vec<(TxnId, Lsn, LogRecordType)> = iter2
            .collect()
            .into_iter()
            .map(|r| (r.get_txn_id(), r.get_lsn(), r.get_log_record_type()))
            .collect();

        assert_eq!(manual, collected);
        assert_eq!(manual.len(), 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_direct_io_offsets_are_page_aligned_and_iterator_advances_physically() {
        let config = DiskManagerConfig {
            direct_io: true,
            ..DiskManagerConfig::default()
        };
        let ctx = TestContext::new_with_config("direct_io_offsets", config).await;
        assert!(ctx.disk_manager.get_config().direct_io);

        let r1 = ctx.create_test_begin_record(1);
        let r2 = ctx.create_test_begin_record(2);
        let s1 = record_logical_size_bytes(&r1);
        let s2 = record_logical_size_bytes(&r2);
        let p1 = round_up_to_page(s1);
        let p2 = round_up_to_page(s2);

        let o1 = ctx.disk_manager.write_log(&r1).await.unwrap();
        let o2 = ctx.disk_manager.write_log(&r2).await.unwrap();

        // Invariant: offsets reflect physical (aligned) layout.
        assert_eq!(o1, 0);
        assert_eq!(o2, p1);

        let mut iter = LogIterator::new(ctx.disk_manager.clone());
        let a = iter.next().expect("expected first record");
        assert_eq!(a.get_txn_id(), 1);
        assert_eq!(iter.get_offset(), p1);

        let b = iter.next().expect("expected second record");
        assert_eq!(b.get_txn_id(), 2);
        assert_eq!(iter.get_offset(), p1 + p2);

        assert!(iter.next().is_none());
        assert!(iter.is_end());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_direct_io_resync_skips_corrupt_page_boundary() {
        // Construct an on-disk layout that makes resync deterministic in direct I/O mode:
        // - Page 0 begins with an invalid size header (all zeros => size=0)
        // - Page 1 begins with a valid record, padded to a full page
        let config = DiskManagerConfig {
            direct_io: true,
            ..DiskManagerConfig::default()
        };

        // Create tempdir + file paths first so we can pre-populate the log file bytes
        initialize_logger();
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir
            .path()
            .join("direct_io_resync.db")
            .to_str()
            .unwrap()
            .to_string();
        let log_path = temp_dir
            .path()
            .join("direct_io_resync.log")
            .to_str()
            .unwrap()
            .to_string();

        let record = {
            let mut r = LogRecord::new_transaction_record(7, INVALID_LSN, LogRecordType::Begin);
            r.set_lsn(7);
            r
        };
        let record_bytes = record.to_bytes().unwrap();
        let mut page1 = vec![0u8; DB_PAGE_SIZE as usize];
        page1[..record_bytes.len()].copy_from_slice(&record_bytes);

        let mut file_bytes = vec![0u8; DB_PAGE_SIZE as usize];
        file_bytes.extend_from_slice(&page1);

        std::fs::write(&log_path, file_bytes).unwrap();

        // Now open the disk manager in direct I/O mode over that pre-populated file.
        let disk_manager =
            AsyncDiskManager::new(db_path, log_path.clone(), config).await.unwrap();
        let disk_manager = Arc::new(disk_manager);

        let mut iter = LogIterator::new(disk_manager);
        let rec = iter.next().expect("expected iterator to resync to page 1");
        assert_eq!(rec.get_txn_id(), 7);
        assert_eq!(rec.get_log_record_type(), LogRecordType::Begin);

        // Invariant: the iterator should have skipped page 0 and advanced one physical page.
        assert_eq!(iter.get_offset(), DB_PAGE_SIZE as u64 * 2);
    }
}
