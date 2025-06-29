/// I/O Worker Management Module
/// 
/// This module handles the worker thread lifecycle and operation processing,
/// including worker spawning, shutdown, and operation execution coordination.

use super::operations::IOOperation;
use super::queue::IOQueueManager;
use super::executor::IOOperationExecutor;
use crate::storage::disk::async_disk::io::completion::CompletionTracker;
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio::sync::Semaphore;

/// Worker task manager responsible for managing worker threads
/// 
/// This component handles all worker-related operations including:
/// - Starting and stopping worker threads
/// - Coordinating operation processing
/// - Managing graceful shutdown
/// - Handling worker failures and recovery
/// - Limiting concurrent I/O operations via semaphore
#[derive(Debug)]
pub struct IOWorkerManager {
    /// JoinSet for managing all running worker tasks
    worker_tasks: JoinSet<()>,
    
    /// Signal used to coordinate worker shutdown
    shutdown_signal: Arc<tokio::sync::Notify>,
    
    /// Semaphore to limit concurrent I/O operations across all workers
    concurrency_limiter: Arc<Semaphore>,
}

impl IOWorkerManager {
    /// Creates a new I/O worker manager with specified concurrency limit
    /// 
    /// # Arguments
    /// * `max_concurrent_operations` - Maximum number of I/O operations that can run concurrently
    pub fn new(max_concurrent_operations: usize) -> Self {
        Self {
            worker_tasks: JoinSet::new(),
            shutdown_signal: Arc::new(tokio::sync::Notify::new()),
            concurrency_limiter: Arc::new(Semaphore::new(max_concurrent_operations)),
        }
    }

    /// Creates a new I/O worker manager with default concurrency limit
    /// 
    /// Default limit is set to a reasonable value for most workloads
    pub fn new_with_default_concurrency() -> Self {
        Self::new(32) // Default to 32 concurrent operations
    }

    /// Starts worker tasks that process operations from the queue
    /// 
    /// This method spawns the specified number of worker tasks that will
    /// continuously process operations from the queue until shutdown.
    /// 
    /// # Arguments
    /// * `num_workers` - Number of worker threads to spawn
    /// * `queue_manager` - Shared queue manager for dequeuing operations
    /// * `executor` - Shared executor for running operations
    /// * `completion_tracker` - Shared completion tracker for metrics
    pub fn start_workers(
        &mut self,
        num_workers: usize,
        queue_manager: Arc<IOQueueManager>,
        executor: Arc<IOOperationExecutor>,
        completion_tracker: Arc<CompletionTracker>,
    ) {
        for worker_id in 0..num_workers {
            let queue_manager_clone = Arc::clone(&queue_manager);
            let executor_clone = Arc::clone(&executor);
            let completion_tracker_clone = Arc::clone(&completion_tracker);
            let shutdown_signal_clone = Arc::clone(&self.shutdown_signal);
            let concurrency_limiter_clone = Arc::clone(&self.concurrency_limiter);

            self.worker_tasks.spawn(async move {
                Self::worker_loop(
                    worker_id,
                    queue_manager_clone,
                    executor_clone,
                    completion_tracker_clone,
                    shutdown_signal_clone,
                    concurrency_limiter_clone,
                ).await;
            });
        }
        
        log::info!("Started {} I/O worker threads", num_workers);
    }

    /// Main worker loop that continuously processes operations
    /// 
    /// Each worker runs this loop independently, checking for:
    /// - Shutdown signals
    /// - Available operations in the queue
    /// - Processing operations when available
    /// 
    /// # Arguments
    /// * `worker_id` - Unique identifier for this worker
    /// * `queue_manager` - Queue manager for getting operations
    /// * `executor` - Executor for running operations
    /// * `completion_tracker` - Tracker for operation metrics
    /// * `shutdown_signal` - Signal for coordinated shutdown
    /// * `concurrency_limiter` - Semaphore for limiting concurrent operations
    async fn worker_loop(
        worker_id: usize,
        queue_manager: Arc<IOQueueManager>,
        executor: Arc<IOOperationExecutor>,
        completion_tracker: Arc<CompletionTracker>,
        shutdown_signal: Arc<tokio::sync::Notify>,
        concurrency_limiter: Arc<Semaphore>,
    ) {
        log::debug!("Worker {} started", worker_id);
        
        loop {
            tokio::select! {
                // Check for shutdown signal
                _ = shutdown_signal.notified() => {
                    log::debug!("Worker {} received shutdown signal", worker_id);
                    break;
                }
                
                // Try to get an operation from the queue
                operation = queue_manager.dequeue_operation() => {
                    if let Some(operation) = operation {
                        log::trace!("Worker {} processing operation {}", worker_id, operation.id);
                        Self::process_operation(operation, &executor, &completion_tracker, &concurrency_limiter).await;
                    } else {
                        // Queue is empty, yield control to allow other tasks to run
                        tokio::task::yield_now().await;
                    }
                }
            }
        }
        
        log::debug!("Worker {} shutting down", worker_id);
    }

    /// Processes a single operation with semaphore-based concurrency control
    /// 
    /// This method handles the complete lifecycle of operation processing:
    /// - Acquiring semaphore permit to limit concurrent operations
    /// - Executing the operation
    /// - Recording completion/failure metrics
    /// - Sending results back to the caller
    /// - Releasing semaphore permit automatically when done
    /// 
    /// # Arguments
    /// * `operation` - The operation to process
    /// * `executor` - The executor to run the operation
    /// * `completion_tracker` - Tracker for recording metrics
    /// * `concurrency_limiter` - Semaphore for limiting concurrent operations
    async fn process_operation(
        operation: IOOperation,
        executor: &IOOperationExecutor,
        completion_tracker: &CompletionTracker,
        concurrency_limiter: &Semaphore,
    ) {
        // Acquire semaphore permit to limit concurrent I/O operations
        // This provides backpressure and prevents overwhelming the disk subsystem
        let _permit = match concurrency_limiter.acquire().await {
            Ok(permit) => permit,
            Err(_) => {
                log::error!("Failed to acquire concurrency permit for operation - semaphore closed");
                return;
            }
        };
        
        let op_id = operation.id;
        let start_time = std::time::Instant::now();
        
        // Record that we started processing this operation
        completion_tracker.metrics().record_operation_start();
        
        // Execute the operation - this consumes the operation including its completion_sender
        let result = executor.execute_operation(operation).await;
        
        let duration = start_time.elapsed();
        
        // Record completion metrics based on the result
        match &result {
            Ok(data) => {
                // Record successful completion
                let data_size = data.len() as u64;
                completion_tracker.metrics().record_operation_complete(duration, data_size, true).await;
                log::trace!("Operation {} completed successfully in {:?}", op_id, duration);
            }
            Err(e) => {
                // Record failure
                completion_tracker.metrics().record_operation_failed();
                log::warn!("Operation {} failed after {:?}: {}", op_id, duration, e);
            }
        }
        
        // Note: The executor.execute_operation() method handles sending the result
        // back to the caller via the completion_sender, so we don't need to do it here.
        // The operation is fully consumed by the executor.
    }

    /// Signals all workers to shutdown gracefully
    /// 
    /// This method:
    /// 1. Sends shutdown signal to all workers
    /// 2. Waits for all workers to complete current operations
    /// 3. Cleans up worker tasks
    pub async fn shutdown(&mut self) {
        if self.worker_tasks.is_empty() {
            return;
        }

        log::info!("Shutting down {} I/O workers", self.worker_tasks.len());
        
        // Signal all workers to shutdown
        self.shutdown_signal.notify_waiters();

        // Wait for all workers to complete using JoinSet
        let mut worker_id = 0;
        while let Some(result) = self.worker_tasks.join_next().await {
            match result {
                Ok(_) => log::debug!("Worker {} shut down successfully", worker_id),
                Err(e) => log::error!("Worker {} failed to shut down: {}", worker_id, e),
            }
            worker_id += 1;
        }

        log::info!("All I/O workers shut down");
    }

    /// Gets the number of active worker threads
    pub fn worker_count(&self) -> usize {
        self.worker_tasks.len()
    }

    /// Checks if any workers are currently running
    pub fn has_active_workers(&self) -> bool {
        !self.worker_tasks.is_empty()
    }

    /// Forces immediate shutdown of all workers (non-graceful)
    /// 
    /// This should only be used in emergency situations as it may
    /// cause operations to be interrupted.
    pub async fn force_shutdown(&mut self) {
        if self.worker_tasks.is_empty() {
            return;
        }

        log::warn!("Force shutting down {} I/O workers", self.worker_tasks.len());
        
        // Abort all worker tasks using JoinSet
        self.worker_tasks.abort_all();
        
        // Drain all aborted tasks to update the count
        while let Some(result) = self.worker_tasks.join_next().await {
            match result {
                Ok(()) => log::debug!("Worker task completed during force shutdown"),
                Err(e) if e.is_cancelled() => log::debug!("Worker task was cancelled during force shutdown"),
                Err(e) => log::warn!("Worker task failed during force shutdown: {}", e),
            }
        }

        log::warn!("All I/O workers force shut down");
    }
}

impl Default for IOWorkerManager {
    fn default() -> Self {
        Self::new_with_default_concurrency()
    }
}

// Ensure proper cleanup on drop
impl Drop for IOWorkerManager {
    fn drop(&mut self) {
        if !self.worker_tasks.is_empty() {
            log::warn!("IOWorkerManager dropped with {} active workers - they will be aborted", 
                      self.worker_tasks.len());
            
            // Abort any remaining workers using JoinSet
            self.worker_tasks.abort_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::async_disk::io::operations::{IOOperationType, priorities};
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    async fn create_test_components() -> (IOQueueManager, IOOperationExecutor, CompletionTracker, String, String) {
        // Use a more unique identifier to prevent test interference
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique_id = COUNTER.fetch_add(1, Ordering::SeqCst);
        
        let db_path = format!("/tmp/test_worker_db_{}_{}.dat", std::process::id(), unique_id);
        let log_path = format!("/tmp/test_worker_log_{}_{}.dat", std::process::id(), unique_id);

        // Create test files
        let mut db_file = File::create(&db_path).await.unwrap();
        let mut log_file = File::create(&log_path).await.unwrap();
        
        db_file.write_all(&vec![0u8; 8192]).await.unwrap();
        log_file.write_all(b"initial").await.unwrap();
        
        // Ensure files are flushed before closing
        db_file.flush().await.unwrap();
        log_file.flush().await.unwrap();
        
        drop(db_file);
        drop(log_file);
        
        let db_file = Arc::new(Mutex::new(
            File::options().read(true).write(true).open(&db_path).await.unwrap()
        ));
        let log_file = Arc::new(Mutex::new(
            File::options().read(true).write(true).open(&log_path).await.unwrap()
        ));

        let queue_manager = IOQueueManager::new();
        let executor = IOOperationExecutor::new(db_file, log_file);
        let completion_tracker = CompletionTracker::new();

        (queue_manager, executor, completion_tracker, db_path, log_path)
    }

    async fn cleanup_test_files(db_path: &str, log_path: &str) {
        let _ = tokio::fs::remove_file(db_path).await;
        let _ = tokio::fs::remove_file(log_path).await;
    }

    #[tokio::test]
    async fn test_worker_lifecycle() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) = create_test_components().await;
        
        let mut worker_manager = IOWorkerManager::new(16);
        
        // Initially no workers
        assert_eq!(worker_manager.worker_count(), 0);
        assert!(!worker_manager.has_active_workers());
        
        // Start workers
        worker_manager.start_workers(
            2,
            Arc::new(queue_manager),
            Arc::new(executor),
            Arc::new(completion_tracker),
        );
        
        assert_eq!(worker_manager.worker_count(), 2);
        assert!(worker_manager.has_active_workers());
        
        // Shutdown workers
        worker_manager.shutdown().await;
        
        assert_eq!(worker_manager.worker_count(), 0);
        assert!(!worker_manager.has_active_workers());

        cleanup_test_files(&db_path, &log_path).await;
    }

    #[tokio::test]
    async fn test_operation_processing() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) = create_test_components().await;
        
        let queue_manager = Arc::new(queue_manager);
        let executor = Arc::new(executor);
        let completion_tracker = Arc::new(completion_tracker);
        
        let mut worker_manager = IOWorkerManager::new(16);
        worker_manager.start_workers(1, Arc::clone(&queue_manager), Arc::clone(&executor), Arc::clone(&completion_tracker));
        
        // Enqueue an operation
        let test_data = vec![42u8; 4096];
        let (_op_id, receiver) = queue_manager.enqueue_operation(
            IOOperationType::WritePage { page_id: 0, data: test_data.clone() },
            priorities::PAGE_WRITE,
        ).await;
        
        // Wait for operation to complete
        let result = receiver.await.unwrap().unwrap();
        assert_eq!(result, test_data);
        
        // Verify operation was processed by checking metrics
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        let metrics = completion_tracker.metrics();
        assert!(metrics.total_operations() > 0);
        assert!(metrics.completed_operations() > 0);
        
        worker_manager.shutdown().await;

        cleanup_test_files(&db_path, &log_path).await;
    }

    #[tokio::test]
    async fn test_multiple_workers() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) = create_test_components().await;
        
        let queue_manager = Arc::new(queue_manager);
        let executor = Arc::new(executor);
        let completion_tracker = Arc::new(completion_tracker);
        
        let mut worker_manager = IOWorkerManager::new(16);
        worker_manager.start_workers(3, Arc::clone(&queue_manager), Arc::clone(&executor), Arc::clone(&completion_tracker));
        
        // Enqueue multiple operations
        let mut receivers = Vec::new();
        for i in 0..5 {
            let test_data = vec![i as u8; 4096];
            let (_op_id, receiver) = queue_manager.enqueue_operation(
                IOOperationType::WritePage { page_id: i, data: test_data },
                priorities::PAGE_WRITE,
            ).await;
            receivers.push(receiver);
        }
        
        // Wait for all operations to complete
        for receiver in receivers {
            let _result = receiver.await.unwrap().unwrap();
        }
        
        // Verify all operations were processed
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let metrics = completion_tracker.metrics();
        assert!(metrics.completed_operations() >= 5);
        
        worker_manager.shutdown().await;

        cleanup_test_files(&db_path, &log_path).await;
    }

    #[tokio::test]
    async fn test_force_shutdown() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) = create_test_components().await;
        
        let mut worker_manager = IOWorkerManager::new(16);
        worker_manager.start_workers(
            2,
            Arc::new(queue_manager),
            Arc::new(executor),
            Arc::new(completion_tracker),
        );
        
        assert_eq!(worker_manager.worker_count(), 2);
        
        // Force shutdown
        worker_manager.force_shutdown().await;
        
        assert_eq!(worker_manager.worker_count(), 0);

        cleanup_test_files(&db_path, &log_path).await;
    }

    #[tokio::test]
    async fn test_operation_error_handling() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) = create_test_components().await;
        
        let queue_manager = Arc::new(queue_manager);
        let executor = Arc::new(executor);
        let completion_tracker = Arc::new(completion_tracker);
        
        let mut worker_manager = IOWorkerManager::new(16);
        worker_manager.start_workers(1, Arc::clone(&queue_manager), Arc::clone(&executor), Arc::clone(&completion_tracker));
        
        // Try to read from a very large page that doesn't exist (would cause EOF error)
        let (_op_id, receiver) = queue_manager.enqueue_operation(
            IOOperationType::ReadPage { page_id: 1000 }, // Page that doesn't exist in our small test file
            priorities::PAGE_READ,
        ).await;
        
        // Wait for operation to complete
        let result = receiver.await.unwrap();
        assert!(result.is_err(), "Reading from non-existent page should fail");
        
        // Verify error was recorded in metrics
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        let metrics = completion_tracker.metrics();
        assert!(metrics.total_operations() > 0);
        assert!(metrics.failed_operations() > 0);
        
        worker_manager.shutdown().await;

        cleanup_test_files(&db_path, &log_path).await;
    }

    #[tokio::test]
    async fn test_concurrency_limiting() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) = create_test_components().await;
        
        let queue_manager = Arc::new(queue_manager);
        let executor = Arc::new(executor);
        let completion_tracker = Arc::new(completion_tracker);
        
        // Create worker manager with very limited concurrency
        let mut worker_manager = IOWorkerManager::new(1);
        worker_manager.start_workers(2, Arc::clone(&queue_manager), Arc::clone(&executor), Arc::clone(&completion_tracker));
        
        // Enqueue multiple operations
        let mut receivers = Vec::new();
        for i in 0..3 {
            let test_data = vec![i as u8; 4096];
            let (_op_id, receiver) = queue_manager.enqueue_operation(
                IOOperationType::WritePage { page_id: i, data: test_data },
                priorities::PAGE_WRITE,
            ).await;
            receivers.push(receiver);
        }
        
        // Operations should still complete despite concurrency limiting
        for receiver in receivers {
            let _result = receiver.await.unwrap().unwrap();
        }
        
        worker_manager.shutdown().await;

        cleanup_test_files(&db_path, &log_path).await;
    }
} 