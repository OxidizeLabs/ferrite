//! # I/O Operation Executor
//!
//! The `IOOperationExecutor` is responsible for the low-level execution of asynchronous I/O operations
//! against the database and log files. It serves as the "worker" component in the I/O pipeline,
//! translating high-level operation types into concrete `tokio::fs` calls.
//!
//! ## Architecture
//!
//! The executor manages access to shared file handles (`Arc<Mutex<File>>`) for the database file
//! (fixed-size pages) and the Write-Ahead Log (WAL, variable-length records). It supports both
//! buffered I/O and Direct I/O (O_DIRECT) modes.
//!
//! ## Key Features
//!
//! - **Direct I/O Support**: When enabled, uses `AlignedBuffer` to ensure memory alignment requirements
//!   are met for `O_DIRECT` operations (critical for bypassing OS page cache).
//! - **Operation Dispatch**: Centralized `execute_operation_type` method dispatching specific logic for:
//!     - `ReadPage` / `WritePage`: Fixed-size random access to database pages.
//!     - `AppendLog` / `ReadLog`: Sequential access for WAL records.
//!     - `Sync` / `SyncLog`: Durability primitives (fsync/fdatasync).
//! - **Thread Safety**: Safely shares file handles across concurrent async tasks using Tokio mutexes.
//! - **Metadata Logging**: Optional debug logging tracking file sizes and operation offsets for debugging.
//!
//! ## Usage
//!
//! This component is typically wrapped by the `AsyncDiskManager` or a worker pool. It is stateless
//! regarding the *content* of the I/O but stateful regarding the *configuration* (e.g., Direct I/O settings).
//!
//! ## Performance Considerations
//!
//! - **Direct I/O**: For the database file, Direct I/O avoids double-buffering in the OS page cache
//!   but requires strictly aligned memory buffers.
//! - **Buffered WAL**: The WAL always uses buffered I/O because its write patterns (small, variable appends)
//!   are poorly suited for the block-aligned restrictions of Direct I/O.
//!
//! This module contains the `IOOperationExecutor` struct.

use super::operations::IOOperationType;
use crate::common::config::{DB_PAGE_SIZE, PageId};
use crate::storage::disk::direct_io::{AlignedBuffer, DirectIOConfig};
use std::io::Result as IoResult;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

/// I/O operation executor responsible for executing operations
///
/// This component handles the actual file I/O operations including:
/// - Reading and writing database pages
/// - Reading and writing log entries
/// - Syncing files to disk
/// - Managing file handles safely across async operations
///
/// When direct I/O is enabled, all operations use properly aligned buffers
/// to ensure optimal performance and compatibility with O_DIRECT on Linux,
/// F_NOCACHE on macOS, and FILE_FLAG_NO_BUFFERING on Windows.
#[derive(Debug)]
pub struct IOOperationExecutor {
    /// Database file handle for page operations
    db_file: Arc<Mutex<File>>,

    /// Log file handle for log operations
    log_file: Arc<Mutex<File>>,

    /// Direct I/O configuration for the DB file only.
    ///
    /// WAL/log operations are intentionally OS-buffered: WAL is variable-length and append-heavy,
    /// and O_DIRECT/NO_BUFFERING alignment requirements are a poor fit.
    db_direct_io_config: DirectIOConfig,
}

impl IOOperationExecutor {
    /// Creates a new I/O operation executor
    ///
    /// # Arguments
    /// * `db_file` - Shared database file handle
    /// * `log_file` - Shared log file handle
    pub fn new(db_file: Arc<Mutex<File>>, log_file: Arc<Mutex<File>>) -> Self {
        Self {
            db_file,
            log_file,
            db_direct_io_config: DirectIOConfig::default(),
        }
    }

    /// Creates a new I/O operation executor with direct I/O configuration
    ///
    /// # Arguments
    /// * `db_file` - Shared database file handle
    /// * `log_file` - Shared log file handle
    /// * `direct_io_config` - Configuration for direct I/O operations
    pub fn with_config(
        db_file: Arc<Mutex<File>>,
        log_file: Arc<Mutex<File>>,
        db_direct_io_config: DirectIOConfig,
    ) -> Self {
        log::debug!(
            "Creating IOOperationExecutor with db_direct_io={}, db_alignment={}, wal_buffered=true",
            db_direct_io_config.enabled,
            db_direct_io_config.alignment
        );
        Self {
            db_file,
            log_file,
            db_direct_io_config,
        }
    }

    /// Returns whether direct I/O is enabled
    pub fn is_direct_io_enabled(&self) -> bool {
        self.db_direct_io_config.enabled
    }

    /// Returns the alignment requirement for direct I/O
    pub fn alignment(&self) -> usize {
        self.db_direct_io_config.alignment
    }

    /// Executes an I/O operation type and returns the resulting bytes.
    ///
    /// This is the main entry point used by the worker pipeline. Completion /
    /// cancellation is handled by higher-level components (e.g. `CompletionTracker`),
    /// not by this executor.
    pub async fn execute_operation_type(&self, operation_type: IOOperationType) -> IoResult<Vec<u8>> {
        match operation_type {
            IOOperationType::ReadPage { page_id } => self.execute_read_page(page_id).await,
            IOOperationType::WritePage { page_id, data } => {
                self.execute_write_page(page_id, &data).await?;
                Ok(data)
            }
            IOOperationType::ReadLog { offset, size } => self.execute_read_log(offset, size).await,
            IOOperationType::WriteLog { data, offset } => {
                self.execute_write_log(&data, offset).await?;
                Ok(data)
            }
            IOOperationType::AppendLog { data } => self
                .execute_append_log(&data)
                .await
                .map(|offset| offset.to_le_bytes().to_vec()),
            IOOperationType::Sync => {
                self.execute_sync().await?;
                Ok(Vec::new())
            }
            IOOperationType::SyncLog => {
                self.execute_sync_log().await?;
                Ok(Vec::new())
            }
        }
    }

    /// Reads a page from the database file
    ///
    /// When direct I/O is enabled, uses aligned buffers to ensure
    /// compatibility with O_DIRECT and similar mechanisms.
    ///
    /// # Arguments
    /// * `page_id` - The page identifier to read
    ///
    /// # Returns
    /// The page data as a vector of bytes
    async fn execute_read_page(&self, page_id: PageId) -> IoResult<Vec<u8>> {
        let offset = page_id * DB_PAGE_SIZE;
        let size = DB_PAGE_SIZE as usize;

        let mut file = self.db_file.lock().await;
        file.seek(std::io::SeekFrom::Start(offset)).await?;

        if self.db_direct_io_config.enabled {
            // Use aligned buffer for direct I/O
            let mut aligned_buf = AlignedBuffer::new(size, self.db_direct_io_config.alignment);
            file.read_exact(aligned_buf.as_mut_slice()).await?;
            Ok(aligned_buf.to_vec())
        } else {
            // Use standard buffer for buffered I/O
            let mut buffer = vec![0u8; size];
            file.read_exact(&mut buffer).await?;
            Ok(buffer)
        }
    }

    /// Writes a page to the database file
    ///
    /// When direct I/O is enabled, uses aligned buffers to ensure
    /// compatibility with O_DIRECT and similar mechanisms.
    ///
    /// # Arguments
    /// * `page_id` - The page identifier to write to
    /// * `data` - The page data to write (must be exactly DB_PAGE_SIZE bytes)
    ///
    /// # Errors
    /// Returns an error if data length doesn't match DB_PAGE_SIZE
    async fn execute_write_page(&self, page_id: PageId, data: &[u8]) -> IoResult<()> {
        // Validate page size to prevent partial writes
        if data.len() != DB_PAGE_SIZE as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Invalid page data size: expected {} bytes, got {} bytes",
                    DB_PAGE_SIZE,
                    data.len()
                ),
            ));
        }

        let offset = page_id * DB_PAGE_SIZE;

        let mut file = self.db_file.lock().await;

        // Debug logging with metadata (only pay syscall cost when debug is enabled)
        if log::log_enabled!(log::Level::Debug) {
            let file_size_before = file.metadata().await?.len();
            log::debug!(
                "execute_write_page: page_id={}, offset={}, data_len={}, file_size_before={}, db_direct_io={}",
                page_id,
                offset,
                data.len(),
                file_size_before,
                self.db_direct_io_config.enabled
            );
        }

        file.seek(std::io::SeekFrom::Start(offset)).await?;

        if self.db_direct_io_config.enabled {
            // Use aligned buffer for direct I/O
            // DB_PAGE_SIZE (4096) is already aligned, but we ensure the buffer is aligned
            let aligned_buf = AlignedBuffer::from_slice_with_size(
                data,
                DB_PAGE_SIZE as usize,
                self.db_direct_io_config.alignment,
            );
            file.write_all(aligned_buf.as_slice()).await?;
        } else {
            // Use standard write for buffered I/O
            file.write_all(data).await?;
        }

        // Flush to OS page cache (does NOT guarantee persistence - use sync_all for durability)
        file.flush().await?;

        // Debug logging with metadata (only pay syscall cost when debug is enabled)
        if log::log_enabled!(log::Level::Debug) {
            let file_size_after = file.metadata().await?.len();
            log::debug!(
                "execute_write_page: COMPLETED page_id={}, file_size_after={}",
                page_id,
                file_size_after
            );
        }

        Ok(())
    }

    /// Reads data from the log file at a specific offset
    ///
    /// When direct I/O is enabled, uses aligned buffers. Note that for
    /// log operations with non-aligned sizes, the buffer is allocated
    /// with aligned size but only the requested bytes are returned.
    ///
    /// # Arguments
    /// * `offset` - Byte offset in the log file
    /// * `size` - Number of bytes to read
    ///
    /// # Returns
    /// The log data as a vector of bytes
    async fn execute_read_log(&self, offset: u64, size: usize) -> IoResult<Vec<u8>> {
        if size == 0 {
            return Ok(Vec::new());
        }

        let mut file = self.log_file.lock().await;
        file.seek(std::io::SeekFrom::Start(offset)).await?;

        // WAL/log is OS-buffered: read exactly the requested number of bytes.
        let mut buffer = vec![0u8; size];
        file.read_exact(&mut buffer).await?;
        Ok(buffer)
    }

    /// Writes data to the log file at a specific offset
    ///
    /// When direct I/O is enabled, uses aligned buffers.
    ///
    /// # Arguments
    /// * `data` - The data to write
    /// * `offset` - Byte offset in the log file
    async fn execute_write_log(&self, data: &[u8], offset: u64) -> IoResult<()> {
        if data.is_empty() {
            return Ok(());
        }

        let mut file = self.log_file.lock().await;
        file.seek(std::io::SeekFrom::Start(offset)).await?;

        file.write_all(data).await?;

        // Flush to OS page cache (does NOT guarantee persistence - use sync_data/sync_all for durability)
        file.flush().await?;
        Ok(())
    }

    /// Appends data to the end of the log file
    ///
    /// When direct I/O is enabled, uses aligned buffers.
    ///
    /// # Arguments
    /// * `data` - The data to append
    ///
    /// # Returns
    /// The byte offset where the data was written
    async fn execute_append_log(&self, data: &[u8]) -> IoResult<u64> {
        let mut file = self.log_file.lock().await;
        let offset = file.seek(std::io::SeekFrom::End(0)).await?;

        if !data.is_empty() {
            file.write_all(data).await?;
            // Flush to OS page cache (does NOT guarantee persistence - use sync_data/sync_all for durability)
            file.flush().await?;
        }

        Ok(offset)
    }

    /// Syncs the database file to ensure data persistence (fsync)
    async fn execute_sync(&self) -> IoResult<()> {
        log::debug!("execute_sync: Starting sync_all() for database file");
        let file = self.db_file.lock().await;

        // Debug logging with metadata (only pay syscall cost when debug is enabled)
        if log::log_enabled!(log::Level::Debug) {
            let file_size = file.metadata().await?.len();
            log::debug!("execute_sync: file_size_before_sync={}", file_size);
        }

        let result = file.sync_all().await;

        log::debug!("execute_sync: sync_all() completed, result={:?}", result);
        result
    }

    /// Syncs the log file to ensure data persistence
    async fn execute_sync_log(&self) -> IoResult<()> {
        let file = self.log_file.lock().await;
        // Prefer fdatasync-style durability for WAL (data + required metadata like file length).
        file.sync_data().await
    }

    /// Gets the current size of the database file in bytes
    pub async fn get_db_file_size(&self) -> IoResult<u64> {
        let file = self.db_file.lock().await;
        let metadata = file.metadata().await?;
        Ok(metadata.len())
    }

    /// Gets a reference to the database file (for advanced use cases)
    pub fn db_file(&self) -> Arc<Mutex<File>> {
        Arc::clone(&self.db_file)
    }

    /// Gets a reference to the log file (for advanced use cases)
    pub fn log_file(&self) -> Arc<Mutex<File>> {
        Arc::clone(&self.log_file)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;

    async fn create_test_executor() -> (IOOperationExecutor, String, String) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique_id = COUNTER.fetch_add(1, Ordering::SeqCst);

        let db_path = format!(
            "/tmp/test_executor_db_{}_{}.dat",
            std::process::id(),
            unique_id
        );
        let log_path = format!(
            "/tmp/test_executor_log_{}_{}.dat",
            std::process::id(),
            unique_id
        );

        // Create and initialize test files
        let mut db_file = File::create(&db_path).await.unwrap();
        let mut log_file = File::create(&log_path).await.unwrap();

        // Write initial data
        db_file.write_all(&vec![0u8; 8192]).await.unwrap(); // 2 pages
        log_file.write_all(b"initial log").await.unwrap();

        drop(db_file);
        drop(log_file);

        // Reopen for executor
        let db_file = Arc::new(Mutex::new(
            File::options()
                .read(true)
                .write(true)
                .open(&db_path)
                .await
                .unwrap(),
        ));
        let log_file = Arc::new(Mutex::new(
            File::options()
                .read(true)
                .write(true)
                .open(&log_path)
                .await
                .unwrap(),
        ));

        let executor = IOOperationExecutor::new(db_file, log_file);
        (executor, db_path, log_path)
    }

    async fn create_large_test_executor() -> (IOOperationExecutor, String, String) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique_id = COUNTER.fetch_add(1, Ordering::SeqCst);

        let db_path = format!(
            "/tmp/test_executor_large_db_{}_{}.dat",
            std::process::id(),
            unique_id
        );
        let log_path = format!(
            "/tmp/test_executor_large_log_{}_{}.dat",
            std::process::id(),
            unique_id
        );

        // Create large test files
        let mut db_file = File::create(&db_path).await.unwrap();
        let mut log_file = File::create(&log_path).await.unwrap();

        // Initialize with 10 pages (40KB)
        db_file.write_all(&vec![0u8; 40960]).await.unwrap();
        log_file.write_all(b"large initial log").await.unwrap();

        drop(db_file);
        drop(log_file);

        // Reopen for executor
        let db_file = Arc::new(Mutex::new(
            File::options()
                .read(true)
                .write(true)
                .open(&db_path)
                .await
                .unwrap(),
        ));
        let log_file = Arc::new(Mutex::new(
            File::options()
                .read(true)
                .write(true)
                .open(&log_path)
                .await
                .unwrap(),
        ));

        let executor = IOOperationExecutor::new(db_file, log_file);
        (executor, db_path, log_path)
    }

    #[tokio::test]
    async fn test_page_operations() {
        let (executor, db_path, log_path) = create_test_executor().await;

        // Test page write
        let test_data = vec![42u8; 4096];
        executor.execute_write_page(0, &test_data).await.unwrap();

        // Test page read
        let read_data = executor.execute_read_page(0).await.unwrap();
        assert_eq!(test_data, read_data);

        // Test second page
        let test_data2 = vec![99u8; 4096];
        executor.execute_write_page(1, &test_data2).await.unwrap();
        let read_data2 = executor.execute_read_page(1).await.unwrap();
        assert_eq!(test_data2, read_data2);

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }

    #[tokio::test]
    async fn test_log_operations() {
        let (executor, db_path, log_path) = create_test_executor().await;

        // Test log append
        let log_data = b"test log entry";
        let offset = executor.execute_append_log(log_data).await.unwrap();

        // Test log read
        let read_data = executor
            .execute_read_log(offset, log_data.len())
            .await
            .unwrap();
        assert_eq!(log_data, &read_data[..]);

        // Test log write at specific offset
        let new_data = b"overwritten";
        executor.execute_write_log(new_data, offset).await.unwrap();

        let read_overwritten = executor
            .execute_read_log(offset, new_data.len())
            .await
            .unwrap();
        assert_eq!(new_data, &read_overwritten[..]);

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }

    #[tokio::test]
    async fn test_sync_operations() {
        let (executor, db_path, log_path) = create_test_executor().await;

        // Test database sync
        executor.execute_sync().await.unwrap();

        // Test log sync
        executor.execute_sync_log().await.unwrap();

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }

    #[tokio::test]
    async fn test_operation_type_execution_write_and_read() {
        let (executor, db_path, log_path) = create_test_executor().await;

        let test_data = vec![123u8; 4096];
        let write_result = executor
            .execute_operation_type(IOOperationType::WritePage {
                page_id: 0,
                data: test_data.clone(),
            })
            .await
            .unwrap();
        assert_eq!(write_result, test_data);

        let read_result = executor
            .execute_operation_type(IOOperationType::ReadPage { page_id: 0 })
            .await
            .unwrap();
        assert_eq!(read_result, test_data);

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }

    #[tokio::test]
    async fn test_page_boundary_conditions() {
        let (executor, db_path, log_path) = create_test_executor().await;

        // Test writing to page beyond current file size
        let test_data = vec![77u8; 4096];
        let result = executor.execute_write_page(10, &test_data).await;
        assert!(
            result.is_ok(),
            "Should be able to write beyond current file size"
        );

        // Test reading from that page
        let read_result = executor.execute_read_page(10).await;
        assert!(
            read_result.is_ok(),
            "Should be able to read from extended page"
        );

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }

    #[tokio::test]
    async fn test_log_sequential_operations() {
        let (executor, db_path, log_path) = create_test_executor().await;

        let mut expected_offset = 11; // After "initial log"

        // Append multiple log entries
        for i in 0..5 {
            let log_data = format!("log entry {}", i);
            let offset = executor
                .execute_append_log(log_data.as_bytes())
                .await
                .unwrap();
            assert_eq!(offset, expected_offset);
            expected_offset += log_data.len() as u64;
        }

        // Read back the log entries
        let mut current_offset = 11;
        for i in 0..5 {
            let expected_data = format!("log entry {}", i);
            let read_data = executor
                .execute_read_log(current_offset, expected_data.len())
                .await
                .unwrap();
            assert_eq!(read_data, expected_data.as_bytes());
            current_offset += expected_data.len() as u64;
        }

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }

    #[tokio::test]
    async fn test_concurrent_operations() {
        let (executor, db_path, log_path) = create_test_executor().await;
        let executor = Arc::new(executor);

        let mut handles = Vec::new();

        // Launch concurrent page writes
        for i in 0..10 {
            let executor_clone = Arc::clone(&executor);
            let handle = tokio::spawn(async move {
                let test_data = vec![i as u8; 4096];
                executor_clone
                    .execute_write_page(i, &test_data)
                    .await
                    .unwrap();
                (i, test_data)
            });
            handles.push(handle);
        }

        // Wait for all writes to complete
        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.unwrap());
        }

        // Verify all writes completed successfully
        for (page_id, expected_data) in results {
            let read_data = executor.execute_read_page(page_id).await.unwrap();
            assert_eq!(read_data, expected_data);
        }

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }

    #[tokio::test]
    async fn test_concurrent_log_operations() {
        let (executor, db_path, log_path) = create_test_executor().await;
        let executor = Arc::new(executor);

        let mut handles = Vec::new();

        // Launch concurrent log appends
        for i in 0..10 {
            let executor_clone = Arc::clone(&executor);
            let handle = tokio::spawn(async move {
                let log_data = format!("concurrent log {}", i);
                let offset = executor_clone
                    .execute_append_log(log_data.as_bytes())
                    .await
                    .unwrap();
                (offset, log_data)
            });
            handles.push(handle);
        }

        // Wait for all appends to complete
        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.unwrap());
        }

        // Verify all appends completed and data is readable
        for (offset, expected_data) in results {
            let read_data = executor
                .execute_read_log(offset, expected_data.len())
                .await
                .unwrap();
            assert_eq!(read_data, expected_data.as_bytes());
        }

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }

    #[tokio::test]
    async fn test_large_data_operations() {
        let (executor, db_path, log_path) = create_large_test_executor().await;

        // Test writing large page data
        let large_data = vec![0x55u8; 4096];
        executor.execute_write_page(5, &large_data).await.unwrap();

        let read_data = executor.execute_read_page(5).await.unwrap();
        assert_eq!(read_data, large_data);

        // Test large log data
        let large_log_data = vec![0xAAu8; 8192]; // 8KB log entry
        let offset = executor.execute_append_log(&large_log_data).await.unwrap();

        let read_log_data = executor
            .execute_read_log(offset, large_log_data.len())
            .await
            .unwrap();
        assert_eq!(read_log_data, large_log_data);

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }

    #[tokio::test]
    async fn test_data_integrity() {
        let (executor, db_path, log_path) = create_test_executor().await;

        // Write different patterns to multiple pages
        let patterns = [
            vec![0x00u8; 4096],
            vec![0xFFu8; 4096],
            vec![0x55u8; 4096],
            vec![0xAAu8; 4096],
        ];

        // Write patterns
        for (page_id, pattern) in patterns.iter().enumerate() {
            executor
                .execute_write_page(page_id as u64, pattern)
                .await
                .unwrap();
        }

        // Sync to ensure data is written
        executor.execute_sync().await.unwrap();

        // Read and verify patterns
        for (page_id, expected_pattern) in patterns.iter().enumerate() {
            let read_data = executor.execute_read_page(page_id as u64).await.unwrap();
            assert_eq!(
                read_data, *expected_pattern,
                "Pattern mismatch for page {}",
                page_id
            );
        }

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }

    #[tokio::test]
    async fn test_log_overwrite_integrity() {
        let (executor, db_path, log_path) = create_test_executor().await;

        // Write initial log entry
        let initial_data = b"initial log entry";
        let offset = executor.execute_append_log(initial_data).await.unwrap();

        // Overwrite with shorter data
        let shorter_data = b"short";
        executor
            .execute_write_log(shorter_data, offset)
            .await
            .unwrap();

        // Read back and verify
        let read_data = executor
            .execute_read_log(offset, shorter_data.len())
            .await
            .unwrap();
        assert_eq!(read_data, shorter_data);

        // Overwrite with longer data
        let longer_data = b"much longer log entry";
        executor
            .execute_write_log(longer_data, offset)
            .await
            .unwrap();

        // Read back and verify
        let read_data = executor
            .execute_read_log(offset, longer_data.len())
            .await
            .unwrap();
        assert_eq!(read_data, longer_data);

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }

    #[tokio::test]
    async fn test_file_handles_access() {
        let (executor, db_path, log_path) = create_test_executor().await;

        // Test that we can get references to file handles
        let db_handle = executor.db_file();
        let log_handle = executor.log_file();

        // Verify we can lock and use the handles
        {
            let _db_file = db_handle.lock().await;
            let _log_file = log_handle.lock().await;
        }

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }

    #[tokio::test]
    async fn test_edge_case_empty_operations() {
        let (executor, db_path, log_path) = create_test_executor().await;

        // Test writing empty data
        let empty_data = vec![];
        let result = executor.execute_write_log(&empty_data, 0).await;
        assert!(result.is_ok());

        // Test reading zero bytes
        let read_result = executor.execute_read_log(0, 0).await.unwrap();
        assert!(read_result.is_empty());

        // Test appending empty data
        let append_result = executor.execute_append_log(&empty_data).await;
        assert!(append_result.is_ok());

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }

    #[tokio::test]
    async fn test_stress_many_operations() {
        let (executor, db_path, log_path) = create_large_test_executor().await;
        let executor = Arc::new(executor);

        let num_operations = 100;
        let mut handles = Vec::new();

        // Launch many mixed operations
        for i in 0..num_operations {
            let executor_clone = Arc::clone(&executor);
            let handle = tokio::spawn(async move {
                match i % 4 {
                    0 => {
                        // Page write
                        let data = vec![i as u8; 4096];
                        executor_clone
                            .execute_write_page(i % 10, &data)
                            .await
                            .unwrap();
                    }
                    1 => {
                        // Page read
                        let _ = executor_clone.execute_read_page(i % 10).await.unwrap();
                    }
                    2 => {
                        // Log append
                        let log_data = format!("stress log {}", i);
                        let _ = executor_clone
                            .execute_append_log(log_data.as_bytes())
                            .await
                            .unwrap();
                    }
                    3 => {
                        // Sync
                        executor_clone.execute_sync().await.unwrap();
                    }
                    _ => unreachable!(),
                }
            });
            handles.push(handle);
        }

        // Wait for all operations to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify system is still functional
        let test_data = vec![0xDEu8; 4096];
        executor.execute_write_page(0, &test_data).await.unwrap();
        let read_data = executor.execute_read_page(0).await.unwrap();
        assert_eq!(read_data, test_data);

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }
}
