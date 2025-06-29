// Async I/O Engine implementation
// Refactored from the original async_disk_manager.rs file

use crate::common::config::{PageId, DB_PAGE_SIZE};
use std::sync::Arc;
use tokio::fs::File;
use tokio::sync::Mutex;
use std::collections::BinaryHeap;
use tokio::task::JoinHandle;
use std::sync::atomic::AtomicU64;
use std::io::Result as IoResult;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use std::time::Instant;
use crate::storage::disk::async_disk::io::completion::CompletionTracker;
use crate::storage::disk::async_disk::io::metrics;

/// Priority queue for I/O operations
pub type PriorityQueue<T> = Arc<Mutex<BinaryHeap<T>>>;

// CompletionTracker is now imported from the completion module

/// I/O operation with priority
#[derive(Debug)]
pub struct IOOperation {
    // Simplified for now
    priority: u8,
    id: u64,
}

impl Ord for IOOperation {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority.cmp(&other.priority)
    }
}

impl PartialOrd for IOOperation {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for IOOperation {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for IOOperation {}

/// Async I/O engine with tokio
#[derive(Debug)]
pub struct AsyncIOEngine {
    // File handles with async I/O - wrapped in Mutex for safe mutable access
    db_file: Arc<Mutex<File>>,
    log_file: Arc<Mutex<File>>,

    // I/O operation queue with prioritization
    operation_queue: PriorityQueue<IOOperation>,

    // Async I/O workers
    worker_pool: Vec<JoinHandle<()>>,

    // I/O completion tracking
    completion_tracker: Arc<CompletionTracker>,

    // Operation ID counter
    next_operation_id: AtomicU64,
}

impl AsyncIOEngine {
    /// Creates a new async I/O engine
    pub fn new(db_file: Arc<Mutex<File>>, log_file: Arc<Mutex<File>>) -> IoResult<Self> {
        let operation_queue = Arc::new(Mutex::new(BinaryHeap::new()));
        let completion_tracker = Arc::new(CompletionTracker::new());

        Ok(Self {
            db_file,
            log_file,
            operation_queue,
            worker_pool: Vec::new(),
            completion_tracker,
            next_operation_id: AtomicU64::new(0),
        })
    }

    /// Reads a page from the database file
    pub async fn read_page(&self, page_id: PageId) -> IoResult<Vec<u8>> {
        let _start_time = Instant::now();
        let offset = (page_id as u64) * DB_PAGE_SIZE;
        let mut buffer = vec![0u8; DB_PAGE_SIZE as usize];

        // Start tracking this operation
        let (op_id, _receiver) = self.completion_tracker.start_operation(None).await;

        let result = async {
            // Lock the file for reading
            let mut file = self.db_file.lock().await;

            // Seek to the correct offset in the file
            file.seek(std::io::SeekFrom::Start(offset)).await?;

            // Read the page data into the buffer
            file.read_exact(&mut buffer).await?;

            Ok::<Vec<u8>, std::io::Error>(buffer)
        }.await;

        match result {
            Ok(data) => {
                // Complete the operation successfully
                let _ = self.completion_tracker.complete_operation(op_id, data.clone()).await;
                Ok(data)
            }
            Err(e) => {
                // Fail the operation
                let _ = self.completion_tracker.fail_operation(op_id, e.to_string()).await;
                Err(e)
            }
        }
    }

    /// Writes a page to the database file
    pub async fn write_page(&self, page_id: PageId, data: &[u8]) -> IoResult<()> {
        let _start_time = Instant::now();
        let offset = (page_id as u64) * DB_PAGE_SIZE;

        // Start tracking this operation
        let (op_id, _receiver) = self.completion_tracker.start_operation(None).await;

        let result = async {
            // Lock the file for writing
            let mut file = self.db_file.lock().await;

            // Seek to the correct offset in the file
            file.seek(std::io::SeekFrom::Start(offset)).await?;

            // Write the page data
            file.write_all(data).await?;

            Ok::<(), std::io::Error>(())
        }.await;

        match result {
            Ok(_) => {
                // Complete the operation successfully with the written data
                let _ = self.completion_tracker.complete_operation(op_id, data.to_vec()).await;
                Ok(())
            }
            Err(e) => {
                // Fail the operation
                let _ = self.completion_tracker.fail_operation(op_id, e.to_string()).await;
                Err(e)
            }
        }
    }

    /// Syncs the database file to disk
    pub async fn sync(&self) -> IoResult<()> {
        let file = self.db_file.lock().await;
        file.sync_all().await
    }

    /// Reads data from the log file at the specified offset
    pub async fn read_log(&self, offset: u64, size: usize) -> IoResult<Vec<u8>> {
        let mut buffer = vec![0u8; size];

        // Start tracking this operation
        let (op_id, _receiver) = self.completion_tracker.start_operation(None).await;

        let result = async {
            // Lock the log file for reading
            let mut file = self.log_file.lock().await;

            // Seek to the specified offset
            file.seek(std::io::SeekFrom::Start(offset)).await?;

            // Read the data
            file.read_exact(&mut buffer).await?;

            Ok::<Vec<u8>, std::io::Error>(buffer)
        }.await;

        match result {
            Ok(data) => {
                // Complete the operation successfully
                let _ = self.completion_tracker.complete_operation(op_id, data.clone()).await;
                Ok(data)
            }
            Err(e) => {
                // Fail the operation
                let _ = self.completion_tracker.fail_operation(op_id, e.to_string()).await;
                Err(e)
            }
        }
    }

    /// Writes data to the log file at the specified offset
    pub async fn write_log(&self, data: &[u8], offset: u64) -> IoResult<()> {
        // Start tracking this operation
        let (op_id, _receiver) = self.completion_tracker.start_operation(None).await;

        let result = async {
            // Lock the log file for writing
            let mut file = self.log_file.lock().await;

            // Seek to the specified offset
            file.seek(std::io::SeekFrom::Start(offset)).await?;

            // Write the data
            file.write_all(data).await?;

            Ok::<(), std::io::Error>(())
        }.await;

        match result {
            Ok(_) => {
                // Complete the operation successfully with the written data
                let _ = self.completion_tracker.complete_operation(op_id, data.to_vec()).await;
                Ok(())
            }
            Err(e) => {
                // Fail the operation
                let _ = self.completion_tracker.fail_operation(op_id, e.to_string()).await;
                Err(e)
            }
        }
    }

    /// Appends data to the log file and returns the offset where it was written
    pub async fn append_log(&self, data: &[u8]) -> IoResult<u64> {
        // Start tracking this operation
        let (op_id, _receiver) = self.completion_tracker.start_operation(None).await;

        let result = async {
            // Lock the log file for writing
            let mut file = self.log_file.lock().await;

            // Seek to the end of the file to get the current position
            let offset = file.seek(std::io::SeekFrom::End(0)).await?;

            // Write the data
            file.write_all(data).await?;

            Ok::<u64, std::io::Error>(offset)
        }.await;

        match result {
            Ok(offset) => {
                // Complete the operation successfully with the written data
                let _ = self.completion_tracker.complete_operation(op_id, data.to_vec()).await;
                Ok(offset)
            }
            Err(e) => {
                // Fail the operation
                let _ = self.completion_tracker.fail_operation(op_id, e.to_string()).await;
                Err(e)
            }
        }
    }

    /// Syncs the log file to ensure durability
    pub async fn sync_log(&self) -> IoResult<()> {
        let file = self.log_file.lock().await;
        file.sync_all().await
    }

    // Completion tracker access methods

    /// Gets the completion tracker for direct access to metrics and operation management
    pub fn completion_tracker(&self) -> Arc<CompletionTracker> {
        Arc::clone(&self.completion_tracker)
    }

    /// Gets the current I/O metrics
    pub fn metrics(&self) -> Arc<metrics::IOMetrics> {
        self.completion_tracker.metrics()
    }

    /// Gets the number of currently tracked operations
    pub async fn tracked_operations_count(&self) -> usize {
        self.completion_tracker.tracked_operations_count().await
    }

    /// Gets the number of pending operations
    pub async fn pending_operations_count(&self) -> usize {
        self.completion_tracker.pending_operations_count().await
    }

    /// Cancels all pending operations
    pub async fn cancel_all_pending_operations(&self, reason: &str) -> usize {
        self.completion_tracker.cancel_all_pending(reason.to_string()).await
    }

    /// Checks for and handles timed out operations
    pub async fn check_and_handle_timeouts(&self) -> usize {
        self.completion_tracker.check_timeouts().await
    }

    /// Prints current I/O statistics for monitoring
    pub async fn print_io_stats(&self) {
        let metrics = self.metrics();
        
        println!("=== AsyncIOEngine Statistics ===");
        println!("Total operations: {}", metrics.total_operations());
        println!("Completed operations: {}", metrics.completed_operations());
        println!("Failed operations: {}", metrics.failed_operations());
        println!("Cancelled operations: {}", metrics.cancelled_operations());
        println!("Timed out operations: {}", metrics.timed_out_operations());
        println!("Pending operations: {}", metrics.pending_operations());
        println!("Total bytes read: {}", metrics.total_bytes_read());
        println!("Total bytes written: {}", metrics.total_bytes_written());
        println!("Average duration: {:?}", metrics.average_duration());
        println!("Min duration: {:?}", metrics.min_duration());
        println!("Max duration: {:?}", metrics.max_duration());
        println!("Rolling average duration: {:?}", metrics.rolling_average_duration().await);
        println!("Operations per second: {:.2}", metrics.operations_per_second());
        println!("Throughput (bytes/sec): {:.2}", metrics.throughput_bytes_per_second());
        println!("Success rate: {:.2}%", metrics.success_rate());
        println!("===============================");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn test_async_io_engine_with_completion_tracking() {
        // Create temporary files for testing
        let db_path = "/tmp/test_db.dat";
        let log_path = "/tmp/test_log.dat";

        // Create test files
        let mut db_file = File::create(db_path).await.unwrap();
        let mut log_file = File::create(log_path).await.unwrap();
        
        // Write some initial data to ensure the files exist
        db_file.write_all(&vec![0u8; 4096]).await.unwrap();
        log_file.write_all(b"initial log data").await.unwrap();
        
        // Close and reopen files
        drop(db_file);
        drop(log_file);
        
        let db_file = Arc::new(Mutex::new(File::options().read(true).write(true).open(db_path).await.unwrap()));
        let log_file = Arc::new(Mutex::new(File::options().read(true).write(true).open(log_path).await.unwrap()));

        // Create the async I/O engine
        let engine = AsyncIOEngine::new(db_file, log_file).unwrap();

        // Test page read/write with completion tracking
        let test_data = vec![1u8; 4096];
        
        // Write a page
        engine.write_page(0, &test_data).await.unwrap();
        
        // Read the page back
        let read_data = engine.read_page(0).await.unwrap();
        assert_eq!(test_data, read_data);

        // Test log operations with completion tracking
        let log_data = b"test log entry";
        let offset = engine.append_log(log_data).await.unwrap();
        
        let read_log_data = engine.read_log(offset, log_data.len()).await.unwrap();
        assert_eq!(log_data, &read_log_data[..]);

        // Check metrics
        let metrics = engine.metrics();
        assert!(metrics.total_operations() > 0);
        assert!(metrics.completed_operations() > 0);
        assert_eq!(metrics.failed_operations(), 0);

        // Print statistics
        engine.print_io_stats().await;

        // Verify no pending operations
        assert_eq!(engine.pending_operations_count().await, 0);

        // Clean up test files
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }
}

/// Example function demonstrating AsyncIOEngine usage with completion tracking
/// 
/// This example demonstrates how to:
/// 1. Create an AsyncIOEngine with production-ready completion tracking
/// 2. Perform I/O operations with automatic tracking
/// 3. Monitor operation metrics and statistics
/// 4. Handle operation timeouts and cancellations
#[allow(dead_code)]
async fn example_usage() -> std::io::Result<()> {
    use tokio::fs::File;
    
    // Create file handles
    let db_file = Arc::new(Mutex::new(File::create("database.dat").await?));
    let log_file = Arc::new(Mutex::new(File::create("log.dat").await?));

    // Create the async I/O engine with completion tracking
    let engine = AsyncIOEngine::new(db_file, log_file)?;

    // Perform I/O operations - they're automatically tracked
    let data = vec![42u8; 4096];
    engine.write_page(0, &data).await?;
    let _read_data = engine.read_page(0).await?;

    // Monitor metrics
    let metrics = engine.metrics();
    println!("Operations completed: {}", metrics.completed_operations());
    println!("Average duration: {:?}", metrics.average_duration());
    println!("Success rate: {:.2}%", metrics.success_rate());

    // Print detailed statistics
    engine.print_io_stats().await;

    // Handle timeouts if needed
    let timed_out = engine.check_and_handle_timeouts().await;
    if timed_out > 0 {
        println!("Handled {} timed out operations", timed_out);
    }

    // Cancel all pending operations if shutting down
    let cancelled = engine.cancel_all_pending_operations("System shutdown").await;
    if cancelled > 0 {
        println!("Cancelled {} pending operations", cancelled);
    }

    Ok(())
}
