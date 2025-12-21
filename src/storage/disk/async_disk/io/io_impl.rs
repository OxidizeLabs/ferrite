//! # Async I/O Engine
//!
//! The `AsyncIOEngine` is the central coordinator for the asynchronous I/O subsystem. It ties together
//! all modular components (Queue, Workers, Executor, Completion Tracking) to provide a high-performance,
//! non-blocking I/O interface for the database.
//!
//! ## Architecture
//!
//! The engine implements a producer-consumer architecture:
//! - **Producers**: Database components call methods like `read_page` or `write_page`, which enqueue
//!   operations into a priority queue.
//! - **Consumers**: A pool of background worker threads (managed by `IOWorkerManager`) dequeue
//!   operations and execute them using the `IOOperationExecutor`.
//!
//! ## Key Features
//!
//! - **Priority Scheduling**: Operations are processed based on importance (e.g., WAL writes > Page writes > Prefetch reads).
//! - **Concurrency Control**: Limits the number of in-flight I/O requests to prevent system overload.
//! - **Direct I/O Support**: Configurable support for O_DIRECT to bypass OS page cache.
//! - **Completion Tracking**: Callers receive `oneshot` channels to await results asynchronously.
//! - **Graceful Shutdown**: Coordination for safe termination of worker threads and flushing of queues.
//!
//! ## Usage
//!
//! This struct is the main entry point for I/O in the `AsyncDiskManager`. It should be instantiated once
//! and shared across the application.

use crate::common::config::PageId;
use crate::storage::disk::async_disk::io::completion::CompletionTracker;
use crate::storage::disk::async_disk::io::metrics;
use crate::storage::disk::direct_io::DirectIOConfig;
use std::io::Result as IoResult;
use std::sync::Arc;
use tokio::fs::File;
use tokio::sync::Mutex;

use super::executor::IOOperationExecutor;
// Import our modular components
use super::operation_status::OperationResult;
use super::operations::{IOOperationType, priorities};
use super::queue::IOQueueManager;
use super::worker::IOWorkerManager;
use tokio::sync::oneshot;

// All the modular components are now in separate files

/// Async I/O engine with tokio
///
/// This engine supports direct I/O for bypassing the OS page cache,
/// which is essential for databases that manage their own caching.
#[derive(Debug)]
pub struct AsyncIOEngine {
    // Queue management - separated responsibility
    queue_manager: Arc<IOQueueManager>,

    // Operation execution - separated responsibility
    executor: Arc<IOOperationExecutor>,

    // Worker management - separated responsibility
    worker_manager: IOWorkerManager,

    // I/O completion tracking
    completion_tracker: Arc<CompletionTracker>,
}

impl AsyncIOEngine {
    /// Creates a new async I/O engine with default (buffered) I/O
    pub fn new(db_file: Arc<Mutex<File>>, log_file: Arc<Mutex<File>>) -> IoResult<Self> {
        Self::with_config(db_file, log_file, DirectIOConfig::default())
    }

    /// Creates a new async I/O engine with direct I/O configuration for the DB file.
    ///
    /// # Arguments
    /// * `db_file` - Shared database file handle (should be opened with direct I/O flags)
    /// * `log_file` - Shared WAL/log file handle (should be opened without direct I/O flags)
    /// * `direct_io_config` - Configuration for DB direct I/O operations
    pub fn with_config(
        db_file: Arc<Mutex<File>>,
        log_file: Arc<Mutex<File>>,
        direct_io_config: DirectIOConfig,
    ) -> IoResult<Self> {
        log::debug!(
            "Creating AsyncIOEngine with db_direct_io={}, db_alignment={}, wal_buffered=true",
            direct_io_config.enabled,
            direct_io_config.alignment
        );

        let queue_manager = Arc::new(IOQueueManager::new());
        let executor = Arc::new(IOOperationExecutor::with_config(
            db_file,
            log_file,
            direct_io_config,
        ));
        let worker_manager = IOWorkerManager::new(64); // Allow up to 64 concurrent I/O operations
        let completion_tracker = Arc::new(CompletionTracker::new());

        Ok(Self {
            queue_manager,
            executor,
            worker_manager,
            completion_tracker,
        })
    }

    /// Returns whether direct I/O is enabled for this engine
    pub fn is_direct_io_enabled(&self) -> bool {
        self.executor.is_direct_io_enabled()
    }

    /// Starts the I/O engine with specified number of worker threads
    pub fn start(&mut self, num_workers: usize) {
        self.worker_manager.start_workers(
            num_workers,
            Arc::clone(&self.queue_manager),
            Arc::clone(&self.executor),
            Arc::clone(&self.completion_tracker),
        );
    }

    /// Signals the I/O engine to begin shutdown (without requiring mutable access)
    /// This triggers the shutdown signals that workers are listening for
    pub fn signal_shutdown(&self) {
        self.worker_manager.signal_shutdown();
    }

    /// Stops the I/O engine and all worker threads
    pub async fn stop(&mut self) {
        self.worker_manager.shutdown().await;
    }

    /// Reads a page from the database file (now uses queue)
    pub async fn read_page(&self, page_id: PageId) -> IoResult<Vec<u8>> {
        self.read_page_with_priority(page_id, priorities::PAGE_READ)
            .await
    }

    /// Reads a page with specified priority
    pub async fn read_page_with_priority(
        &self,
        page_id: PageId,
        priority: u8,
    ) -> IoResult<Vec<u8>> {
        let (_op_id, receiver) = self
            .enqueue_tracked_operation(IOOperationType::ReadPage { page_id }, priority)
            .await;

        Self::await_operation(receiver).await
    }

    /// Writes a page to the database file (now uses queue)
    pub async fn write_page(&self, page_id: PageId, data: &[u8]) -> IoResult<()> {
        self.write_page_with_priority(page_id, data, priorities::PAGE_WRITE)
            .await
    }

    /// Writes a page with specified priority
    pub async fn write_page_with_priority(
        &self,
        page_id: PageId,
        data: &[u8],
        priority: u8,
    ) -> IoResult<()> {
        let (_op_id, receiver) = self
            .enqueue_tracked_operation(
                IOOperationType::WritePage {
                    page_id,
                    data: data.to_vec(),
                },
                priority,
            )
            .await;

        Self::await_operation(receiver).await.map(|_| ())
    }

    /// Syncs the database file to disk (now uses queue)
    pub async fn sync(&self) -> IoResult<()> {
        self.sync_with_priority(priorities::SYNC).await
    }

    /// Syncs with specified priority
    pub async fn sync_with_priority(&self, priority: u8) -> IoResult<()> {
        let (_op_id, receiver) = self
            .enqueue_tracked_operation(IOOperationType::Sync, priority)
            .await;

        Self::await_operation(receiver).await.map(|_| ())
    }

    /// Reads data from the log file at the specified offset (now uses queue)
    pub async fn read_log(&self, offset: u64, size: usize) -> IoResult<Vec<u8>> {
        self.read_log_with_priority(offset, size, priorities::LOG_READ)
            .await
    }

    /// Reads log with specified priority
    pub async fn read_log_with_priority(
        &self,
        offset: u64,
        size: usize,
        priority: u8,
    ) -> IoResult<Vec<u8>> {
        let (_op_id, receiver) = self
            .enqueue_tracked_operation(IOOperationType::ReadLog { offset, size }, priority)
            .await;

        Self::await_operation(receiver).await
    }

    /// Writes data to the log file at the specified offset (now uses queue)
    pub async fn write_log(&self, data: &[u8], offset: u64) -> IoResult<()> {
        self.write_log_with_priority(data, offset, priorities::LOG_WRITE)
            .await
    }

    /// Writes log with specified priority
    pub async fn write_log_with_priority(
        &self,
        data: &[u8],
        offset: u64,
        priority: u8,
    ) -> IoResult<()> {
        let (_op_id, receiver) = self
            .enqueue_tracked_operation(
                IOOperationType::WriteLog {
                    data: data.to_vec(),
                    offset,
                },
                priority,
            )
            .await;

        Self::await_operation(receiver).await.map(|_| ())
    }

    /// Appends data to the log file and returns the offset where it was written (now uses queue)
    pub async fn append_log(&self, data: &[u8]) -> IoResult<u64> {
        self.append_log_with_priority(data, priorities::LOG_APPEND)
            .await
    }

    /// Appends data to the log file by executing directly on the caller task.
    ///
    /// This bypasses the queue/worker pipeline. It's useful for components (like the
    /// recovery `LogManager` flush thread) that may call into async I/O via
    /// `Handle::block_on` and would otherwise risk deadlocking if the runtime has limited
    /// worker threads.
    pub async fn append_log_direct(&self, data: &[u8]) -> IoResult<u64> {
        let bytes = self
            .executor
            .execute_operation_type(IOOperationType::AppendLog {
                data: data.to_vec(),
            })
            .await?;
        if bytes.len() < 8 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid offset data returned from append operation",
            ));
        }
        let offset_bytes: [u8; 8] = bytes[0..8].try_into().expect("slice length checked");
        Ok(u64::from_le_bytes(offset_bytes))
    }

    /// Syncs the WAL/log file by executing directly on the caller task.
    ///
    /// This bypasses the queue/worker pipeline for the same reason as `append_log_direct`:
    /// callers that use `Handle::block_on` from a non-runtime thread (e.g. the WAL flush thread)
    /// must not depend on the async I/O workers to make progress.
    pub async fn sync_log_direct(&self) -> IoResult<()> {
        self.executor
            .execute_operation_type(IOOperationType::SyncLog)
            .await
            .map(|_| ())
    }

    /// Appends log with specified priority
    pub async fn append_log_with_priority(&self, data: &[u8], priority: u8) -> IoResult<u64> {
        let (_op_id, receiver) = self
            .enqueue_tracked_operation(
                IOOperationType::AppendLog {
                    data: data.to_vec(),
                },
                priority,
            )
            .await;

        let bytes = Self::await_operation(receiver).await?;
        if bytes.len() >= 8 {
            let offset_bytes: [u8; 8] = bytes[0..8].try_into().unwrap();
            Ok(u64::from_le_bytes(offset_bytes))
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid offset data returned from append operation",
            ))
        }
    }

    /// Syncs the log file to ensure durability (now uses queue)
    pub async fn sync_log(&self) -> IoResult<()> {
        self.sync_log_with_priority(priorities::SYNC).await
    }

    /// Syncs log with specified priority
    pub async fn sync_log_with_priority(&self, priority: u8) -> IoResult<()> {
        let (_op_id, receiver) = self
            .enqueue_tracked_operation(IOOperationType::SyncLog, priority)
            .await;

        Self::await_operation(receiver).await.map(|_| ())
    }

    /// Gets the current size of the database file in bytes
    pub async fn get_db_file_size(&self) -> IoResult<u64> {
        self.executor.get_db_file_size().await
    }

    // Queue management methods

    /// Gets the current queue size
    pub async fn queue_size(&self) -> usize {
        self.queue_manager.queue_size().await
    }

    /// Clears all pending operations from the queue
    pub async fn clear_queue(&self) -> usize {
        let drained = self.queue_manager.drain_queue().await;
        let mut cancelled = 0usize;
        for op in drained {
            if self
                .completion_tracker
                .cancel_operation(op.id, "Cleared from queue".to_string())
                .await
                .is_ok()
            {
                cancelled += 1;
            }
        }
        cancelled
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
        // First drain the queue so we don't waste I/O on work that hasn't started yet.
        let drained = self.queue_manager.drain_queue().await;
        let mut cancelled = 0usize;
        for op in drained {
            if self
                .completion_tracker
                .cancel_operation(op.id, reason.to_string())
                .await
                .is_ok()
            {
                cancelled += 1;
            }
        }

        // Then cancel any remaining pending operations (including in-flight ones).
        cancelled
            + self
                .completion_tracker
                .cancel_all_pending(reason.to_string())
                .await
    }

    /// Checks for and handles timed out operations
    pub async fn check_and_handle_timeouts(&self) -> usize {
        self.completion_tracker.check_timeouts().await
    }

    /// Prints current I/O statistics for monitoring
    pub async fn print_io_stats(&self) {
        let metrics = self.metrics();

        println!("=== AsyncIOEngine Statistics ===");
        println!("Queue size: {}", self.queue_size().await);
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
        println!(
            "Rolling average duration: {:?}",
            metrics.rolling_average_duration().await
        );
        println!(
            "Operations per second: {:.2}",
            metrics.operations_per_second()
        );
        println!(
            "Throughput (bytes/sec): {:.2}",
            metrics.throughput_bytes_per_second()
        );
        println!("Success rate: {:.2}%", metrics.success_rate());
        println!("===============================");
    }

    async fn enqueue_tracked_operation(
        &self,
        operation_type: IOOperationType,
        priority: u8,
    ) -> (u64, oneshot::Receiver<OperationResult>) {
        let (op_id, receiver) = self.completion_tracker.start_operation(None).await;
        self.queue_manager
            .enqueue_operation(operation_type, priority, op_id)
            .await;
        (op_id, receiver)
    }

    async fn await_operation(receiver: oneshot::Receiver<OperationResult>) -> IoResult<Vec<u8>> {
        match receiver.await {
            Ok(OperationResult::Success(bytes)) => Ok(bytes),
            Ok(OperationResult::Error(msg)) => Err(std::io::Error::other(msg)),
            Err(_) => Err(std::io::Error::other(
                "Operation was cancelled or failed to complete",
            )),
        }
    }
}

/// Example function demonstrating AsyncIOEngine usage with completion tracking and priority queues
///
/// This example demonstrates how to:
/// 1. Create an AsyncIOEngine with production-ready completion tracking
/// 2. Start worker threads to process I/O operations
/// 3. Perform I/O operations with different priorities
/// 4. Monitor operation metrics and statistics
/// 5. Handle operation timeouts and cancellations
/// 6. Properly shut down the engine
#[allow(dead_code)]
async fn example_usage() -> std::io::Result<()> {
    use tokio::fs::File;

    // Create file handles
    let db_file = Arc::new(Mutex::new(File::create("database.dat").await?));
    let log_file = Arc::new(Mutex::new(File::create("log.dat").await?));

    // Create the async I/O engine with completion tracking
    let mut engine = AsyncIOEngine::new(db_file, log_file)?;

    // Start the engine with 4 worker threads
    engine.start(4);

    // Perform I/O operations with different priorities - they're automatically queued and tracked
    let data = vec![42u8; 4096];

    // High priority write
    engine
        .write_page_with_priority(0, &data, priorities::SYNC)
        .await?;

    // Normal priority read
    let _read_data = engine.read_page(0).await?;

    // Critical log append
    let _offset = engine
        .append_log_with_priority(b"critical log entry", priorities::LOG_APPEND)
        .await?;

    // Low priority sync
    engine.sync_with_priority(priorities::BACKGROUND).await?;

    // Monitor metrics
    let metrics = engine.metrics();
    println!("Operations completed: {}", metrics.completed_operations());
    println!("Average duration: {:?}", metrics.average_duration());
    println!("Success rate: {:.2}%", metrics.success_rate());
    println!("Current queue size: {}", engine.queue_size().await);

    // Print detailed statistics
    engine.print_io_stats().await;

    // Handle timeouts if needed
    let timed_out = engine.check_and_handle_timeouts().await;
    if timed_out > 0 {
        println!("Handled {} timed out operations", timed_out);
    }

    // Cancel all pending operations if shutting down
    let cancelled = engine
        .cancel_all_pending_operations("System shutdown")
        .await;
    if cancelled > 0 {
        println!("Cancelled {} pending operations", cancelled);
    }

    // Clear the queue if needed
    let cleared = engine.clear_queue().await;
    if cleared > 0 {
        println!("Cleared {} operations from queue", cleared);
    }

    // Stop the engine and wait for all workers to finish
    engine.stop().await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn test_async_io_engine_with_completion_tracking() {
        // Create temporary files for testing
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique_id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let db_path = format!(
            "/tmp/test_db_{}_{}.dat",
            std::process::id(),
            unique_id
        );
        let log_path = format!(
            "/tmp/test_log_{}_{}.dat",
            std::process::id(),
            unique_id
        );

        // Create test files
        let mut db_file = File::create(&db_path).await.unwrap();
        let mut log_file = File::create(&log_path).await.unwrap();

        // Write some initial data to ensure the files exist
        db_file.write_all(&vec![0u8; 4096]).await.unwrap();
        log_file.write_all(b"initial log data").await.unwrap();

        // Close and reopen files
        drop(db_file);
        drop(log_file);

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

        // Create the async I/O engine
        let mut engine = AsyncIOEngine::new(db_file, log_file).unwrap();

        // Start the engine with 2 worker threads
        engine.start(2);

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

        // Test priority operations
        let high_priority_data = vec![2u8; 4096];
        engine
            .write_page_with_priority(1, &high_priority_data, priorities::SYNC)
            .await
            .unwrap();
        let read_high_priority = engine
            .read_page_with_priority(1, priorities::SYNC)
            .await
            .unwrap();
        assert_eq!(high_priority_data, read_high_priority);

        // Check metrics
        let metrics = engine.metrics();
        assert!(metrics.total_operations() > 0);
        assert!(metrics.completed_operations() > 0);
        assert_eq!(metrics.failed_operations(), 0);

        // Print statistics
        engine.print_io_stats().await;

        // Verify no pending operations
        assert_eq!(engine.pending_operations_count().await, 0);

        // Test queue management
        let queue_size = engine.queue_size().await;
        println!("Current queue size: {}", queue_size);

        // Stop the engine
        engine.stop().await;

        // Clean up test files
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }

    #[tokio::test]
    async fn test_io_queue_manager() {
        let queue_manager = IOQueueManager::new();

        // Test enqueuing operations with different priorities
        queue_manager
            .enqueue_operation(IOOperationType::ReadPage { page_id: 1 }, 5, 1)
            .await;

        queue_manager
            .enqueue_operation(
                IOOperationType::WritePage {
                    page_id: 2,
                    data: vec![1u8; 4096],
                },
                10, // Higher priority
                2,
            )
            .await;

        queue_manager
            .enqueue_operation(
                IOOperationType::ReadPage { page_id: 3 },
                3, // Lower priority
                3,
            )
            .await;

        // Queue should have 3 operations
        assert_eq!(queue_manager.queue_size().await, 3);

        // Dequeue operations - should come out in priority order
        let op1 = queue_manager.dequeue_operation().await.unwrap();
        assert_eq!(op1.priority, 10); // Highest priority first

        let op2 = queue_manager.dequeue_operation().await.unwrap();
        assert_eq!(op2.priority, 5);

        let op3 = queue_manager.dequeue_operation().await.unwrap();
        assert_eq!(op3.priority, 3); // Lowest priority last

        // Queue should be empty now
        assert_eq!(queue_manager.queue_size().await, 0);
        assert!(queue_manager.dequeue_operation().await.is_none());
    }
}
