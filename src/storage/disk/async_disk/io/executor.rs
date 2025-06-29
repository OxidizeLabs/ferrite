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
    /// 
    /// # Arguments
    /// * `operation` - The I/O operation to execute
    /// 
    /// # Returns
    /// The result data as a vector of bytes, or an I/O error
    pub async fn execute_operation(&self, operation: IOOperation) -> IoResult<Vec<u8>> {
        match operation.operation_type {
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
        }
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

    async fn create_test_executor() -> (IOOperationExecutor, String, String) {
        let db_path = format!("/tmp/test_executor_db_{}.dat", std::process::id());
        let log_path = format!("/tmp/test_executor_log_{}.dat", std::process::id());

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
        use tokio::sync::oneshot;
        use std::time::Instant;

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
} 