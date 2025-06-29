/// I/O Operation Executor Module
/// 
/// This module handles the actual execution of I/O operations,
/// including file operations for both database and log files.

use super::operations::{IOOperation, IOOperationType};
use crate::common::config::{PageId, DB_PAGE_SIZE};
use std::sync::Arc;
use tokio::fs::File;
use tokio::sync::Mutex;
use std::io::Result as IoResult;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

/// I/O operation executor responsible for executing operations
/// 
/// This component handles the actual file I/O operations including:
/// - Reading and writing database pages
/// - Reading and writing log entries
/// - Syncing files to disk
/// - Managing file handles safely across async operations
#[derive(Debug)]
pub struct IOOperationExecutor {
    /// Database file handle for page operations
    db_file: Arc<Mutex<File>>,
    
    /// Log file handle for log operations
    log_file: Arc<Mutex<File>>,
}

impl IOOperationExecutor {
    /// Creates a new I/O operation executor
    /// 
    /// # Arguments
    /// * `db_file` - Shared database file handle
    /// * `log_file` - Shared log file handle
    pub fn new(db_file: Arc<Mutex<File>>, log_file: Arc<Mutex<File>>) -> Self {
        Self { db_file, log_file }
    }

    /// Executes a single I/O operation
    /// 
    /// This is the main entry point for operation execution. It matches
    /// on the operation type and delegates to the appropriate method.
    /// The result is sent through the completion_sender and also returned.
    /// 
    /// # Arguments
    /// * `operation` - The I/O operation to execute
    /// 
    /// # Returns
    /// The result data as a vector of bytes, or an I/O error
    pub async fn execute_operation(&self, operation: IOOperation) -> IoResult<Vec<u8>> {
        let completion_sender = operation.completion_sender;
        
        let result = match operation.operation_type {
            IOOperationType::ReadPage { page_id } => {
                self.execute_read_page(page_id).await
            }
            IOOperationType::WritePage { page_id, data } => {
                self.execute_write_page(page_id, &data).await?;
                Ok(data) // Return the written data
            }
            IOOperationType::ReadLog { offset, size } => {
                self.execute_read_log(offset, size).await
            }
            IOOperationType::WriteLog { data, offset } => {
                self.execute_write_log(&data, offset).await?;
                Ok(data) // Return the written data
            }
            IOOperationType::AppendLog { data } => {
                let offset = self.execute_append_log(&data).await?;
                Ok(offset.to_le_bytes().to_vec()) // Return offset as bytes
            }
            IOOperationType::Sync => {
                self.execute_sync().await?;
                Ok(Vec::new()) // No data to return
            }
            IOOperationType::SyncLog => {
                self.execute_sync_log().await?;
                Ok(Vec::new()) // No data to return
            }
        };

        // Send result through completion channel, handling the Result properly
        match &result {
            Ok(data) => {
                // Send a copy of the successful data
                let _ = completion_sender.send(Ok(data.clone()));
            }
            Err(err) => {
                // Create a new error with the same details since Error doesn't implement Clone
                let error_msg = err.to_string();
                let error_kind = err.kind();
                let new_error = std::io::Error::new(error_kind, error_msg);
                let _ = completion_sender.send(Err(new_error));
            }
        }
        
        result
    }

    /// Reads a page from the database file
    /// 
    /// # Arguments
    /// * `page_id` - The page identifier to read
    /// 
    /// # Returns
    /// The page data as a vector of bytes
    async fn execute_read_page(&self, page_id: PageId) -> IoResult<Vec<u8>> {
        let offset = (page_id as u64) * DB_PAGE_SIZE;
        let mut buffer = vec![0u8; DB_PAGE_SIZE as usize];

        let mut file = self.db_file.lock().await;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        file.read_exact(&mut buffer).await?;
        Ok(buffer)
    }

    /// Writes a page to the database file
    /// 
    /// # Arguments
    /// * `page_id` - The page identifier to write to
    /// * `data` - The page data to write
    async fn execute_write_page(&self, page_id: PageId, data: &[u8]) -> IoResult<()> {
        let offset = (page_id as u64) * DB_PAGE_SIZE;

        let mut file = self.db_file.lock().await;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        file.write_all(data).await?;
        Ok(())
    }

    /// Reads data from the log file at a specific offset
    /// 
    /// # Arguments
    /// * `offset` - Byte offset in the log file
    /// * `size` - Number of bytes to read
    /// 
    /// # Returns
    /// The log data as a vector of bytes
    async fn execute_read_log(&self, offset: u64, size: usize) -> IoResult<Vec<u8>> {
        let mut buffer = vec![0u8; size];

        let mut file = self.log_file.lock().await;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        file.read_exact(&mut buffer).await?;
        Ok(buffer)
    }

    /// Writes data to the log file at a specific offset
    /// 
    /// # Arguments
    /// * `data` - The data to write
    /// * `offset` - Byte offset in the log file
    async fn execute_write_log(&self, data: &[u8], offset: u64) -> IoResult<()> {
        let mut file = self.log_file.lock().await;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        file.write_all(data).await?;
        Ok(())
    }

    /// Appends data to the end of the log file
    /// 
    /// # Arguments
    /// * `data` - The data to append
    /// 
    /// # Returns
    /// The byte offset where the data was written
    async fn execute_append_log(&self, data: &[u8]) -> IoResult<u64> {
        let mut file = self.log_file.lock().await;
        let offset = file.seek(std::io::SeekFrom::End(0)).await?;
        file.write_all(data).await?;
        Ok(offset)
    }

    /// Syncs the database file to ensure data persistence
    async fn execute_sync(&self) -> IoResult<()> {
        let file = self.db_file.lock().await;
        file.sync_all().await
    }

    /// Syncs the log file to ensure data persistence
    async fn execute_sync_log(&self) -> IoResult<()> {
        let file = self.log_file.lock().await;
        file.sync_all().await
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
    use tokio::sync::oneshot;
    use std::time::Instant;

    async fn create_test_executor() -> (IOOperationExecutor, String, String) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique_id = COUNTER.fetch_add(1, Ordering::SeqCst);
        
        let db_path = format!("/tmp/test_executor_db_{}_{}.dat", std::process::id(), unique_id);
        let log_path = format!("/tmp/test_executor_log_{}_{}.dat", std::process::id(), unique_id);

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
            File::options().read(true).write(true).open(&db_path).await.unwrap()
        ));
        let log_file = Arc::new(Mutex::new(
            File::options().read(true).write(true).open(&log_path).await.unwrap()
        ));

        let executor = IOOperationExecutor::new(db_file, log_file);
        (executor, db_path, log_path)
    }

    async fn create_large_test_executor() -> (IOOperationExecutor, String, String) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique_id = COUNTER.fetch_add(1, Ordering::SeqCst);
        
        let db_path = format!("/tmp/test_executor_large_db_{}_{}.dat", std::process::id(), unique_id);
        let log_path = format!("/tmp/test_executor_large_log_{}_{}.dat", std::process::id(), unique_id);

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
            File::options().read(true).write(true).open(&db_path).await.unwrap()
        ));
        let log_file = Arc::new(Mutex::new(
            File::options().read(true).write(true).open(&log_path).await.unwrap()
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
        let read_data = executor.execute_read_log(offset, log_data.len()).await.unwrap();
        assert_eq!(log_data, &read_data[..]);

        // Test log write at specific offset
        let new_data = b"overwritten";
        executor.execute_write_log(new_data, offset).await.unwrap();
        
        let read_overwritten = executor.execute_read_log(offset, new_data.len()).await.unwrap();
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
    async fn test_operation_execution() {
        let (executor, db_path, log_path) = create_test_executor().await;

        // Create a test operation
        let (sender, _receiver) = oneshot::channel();
        let operation = IOOperation {
            priority: 5,
            id: 1,
            operation_type: IOOperationType::WritePage {
                page_id: 0,
                data: vec![123u8; 4096],
            },
            completion_sender: sender,
            submitted_at: Instant::now(),
        };

        // Execute the operation
        let result = executor.execute_operation(operation).await.unwrap();
        assert_eq!(result, vec![123u8; 4096]);

        // Verify the write actually happened
        let read_data = executor.execute_read_page(0).await.unwrap();
        assert_eq!(read_data, vec![123u8; 4096]);

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }

    #[tokio::test]
    async fn test_all_operation_types() {
        let (executor, db_path, log_path) = create_test_executor().await;

        // Test ReadPage operation
        let (sender, receiver) = oneshot::channel();
        let read_op = IOOperation {
            priority: 1,
            id: 1,
            operation_type: IOOperationType::ReadPage { page_id: 0 },
            completion_sender: sender,
            submitted_at: Instant::now(),
        };
        let result = executor.execute_operation(read_op).await.unwrap();
        assert_eq!(result.len(), 4096);
        let _ = receiver.await;

        // Test WritePage operation
        let test_data = vec![42u8; 4096];
        let (sender, receiver) = oneshot::channel();
        let write_op = IOOperation {
            priority: 2,
            id: 2,
            operation_type: IOOperationType::WritePage {
                page_id: 0,
                data: test_data.clone(),
            },
            completion_sender: sender,
            submitted_at: Instant::now(),
        };
        let result = executor.execute_operation(write_op).await.unwrap();
        assert_eq!(result, test_data);
        let _ = receiver.await;

        // Test ReadLog operation
        let (sender, receiver) = oneshot::channel();
        let read_log_op = IOOperation {
            priority: 3,
            id: 3,
            operation_type: IOOperationType::ReadLog { offset: 0, size: 11 },
            completion_sender: sender,
            submitted_at: Instant::now(),
        };
        let result = executor.execute_operation(read_log_op).await.unwrap();
        assert_eq!(result, b"initial log");
        let _ = receiver.await;

        // Test WriteLog operation
        let log_data = b"new log data";
        let (sender, receiver) = oneshot::channel();
        let write_log_op = IOOperation {
            priority: 4,
            id: 4,
            operation_type: IOOperationType::WriteLog {
                data: log_data.to_vec(),
                offset: 0,
            },
            completion_sender: sender,
            submitted_at: Instant::now(),
        };
        let result = executor.execute_operation(write_log_op).await.unwrap();
        assert_eq!(result, log_data);
        let _ = receiver.await;

        // Test AppendLog operation
        let append_data = b"appended data";
        let (sender, receiver) = oneshot::channel();
        let append_log_op = IOOperation {
            priority: 5,
            id: 5,
            operation_type: IOOperationType::AppendLog {
                data: append_data.to_vec(),
            },
            completion_sender: sender,
            submitted_at: Instant::now(),
        };
        let result = executor.execute_operation(append_log_op).await.unwrap();
        assert_eq!(result.len(), 8); // offset as bytes
        let _ = receiver.await;

        // Test Sync operation
        let (sender, receiver) = oneshot::channel();
        let sync_op = IOOperation {
            priority: 6,
            id: 6,
            operation_type: IOOperationType::Sync,
            completion_sender: sender,
            submitted_at: Instant::now(),
        };
        let result = executor.execute_operation(sync_op).await.unwrap();
        assert!(result.is_empty());
        let _ = receiver.await;

        // Test SyncLog operation
        let (sender, receiver) = oneshot::channel();
        let sync_log_op = IOOperation {
            priority: 7,
            id: 7,
            operation_type: IOOperationType::SyncLog,
            completion_sender: sender,
            submitted_at: Instant::now(),
        };
        let result = executor.execute_operation(sync_log_op).await.unwrap();
        assert!(result.is_empty());
        let _ = receiver.await;

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }

    #[tokio::test]
    async fn test_completion_channel_success() {
        let (executor, db_path, log_path) = create_test_executor().await;

        let (sender, receiver) = oneshot::channel();
        let test_data = vec![55u8; 4096];
        let operation = IOOperation {
            priority: 1,
            id: 1,
            operation_type: IOOperationType::WritePage {
                page_id: 0,
                data: test_data.clone(),
            },
            completion_sender: sender,
            submitted_at: Instant::now(),
        };

        // Execute operation
        let direct_result = executor.execute_operation(operation).await.unwrap();
        
        // Check completion channel
        let channel_result = receiver.await.unwrap().unwrap();
        
        assert_eq!(direct_result, test_data);
        assert_eq!(channel_result, test_data);

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
        assert!(result.is_ok(), "Should be able to write beyond current file size");

        // Test reading from that page
        let read_result = executor.execute_read_page(10).await;
        assert!(read_result.is_ok(), "Should be able to read from extended page");

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
            let offset = executor.execute_append_log(log_data.as_bytes()).await.unwrap();
            assert_eq!(offset, expected_offset);
            expected_offset += log_data.len() as u64;
        }

        // Read back the log entries
        let mut current_offset = 11;
        for i in 0..5 {
            let expected_data = format!("log entry {}", i);
            let read_data = executor.execute_read_log(current_offset, expected_data.len()).await.unwrap();
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
                executor_clone.execute_write_page(i, &test_data).await.unwrap();
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
                let offset = executor_clone.execute_append_log(log_data.as_bytes()).await.unwrap();
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
            let read_data = executor.execute_read_log(offset, expected_data.len()).await.unwrap();
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
        
        let read_log_data = executor.execute_read_log(offset, large_log_data.len()).await.unwrap();
        assert_eq!(read_log_data, large_log_data);

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }

    #[tokio::test]
    async fn test_data_integrity() {
        let (executor, db_path, log_path) = create_test_executor().await;

        // Write different patterns to multiple pages
        let patterns = vec![
            vec![0x00u8; 4096],
            vec![0xFFu8; 4096],
            vec![0x55u8; 4096],
            vec![0xAAu8; 4096],
        ];

        // Write patterns
        for (page_id, pattern) in patterns.iter().enumerate() {
            executor.execute_write_page(page_id as u64, pattern).await.unwrap();
        }

        // Sync to ensure data is written
        executor.execute_sync().await.unwrap();

        // Read and verify patterns
        for (page_id, expected_pattern) in patterns.iter().enumerate() {
            let read_data = executor.execute_read_page(page_id as u64).await.unwrap();
            assert_eq!(read_data, *expected_pattern, "Pattern mismatch for page {}", page_id);
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
        executor.execute_write_log(shorter_data, offset).await.unwrap();

        // Read back and verify
        let read_data = executor.execute_read_log(offset, shorter_data.len()).await.unwrap();
        assert_eq!(read_data, shorter_data);

        // Overwrite with longer data
        let longer_data = b"much longer log entry";
        executor.execute_write_log(longer_data, offset).await.unwrap();

        // Read back and verify
        let read_data = executor.execute_read_log(offset, longer_data.len()).await.unwrap();
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
                        executor_clone.execute_write_page(i % 10, &data).await.unwrap();
                    }
                    1 => {
                        // Page read
                        let _ = executor_clone.execute_read_page(i % 10).await.unwrap();
                    }
                    2 => {
                        // Log append
                        let log_data = format!("stress log {}", i);
                        let _ = executor_clone.execute_append_log(log_data.as_bytes()).await.unwrap();
                    }
                    3 => {
                        // Sync
                        let _ = executor_clone.execute_sync().await.unwrap();
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