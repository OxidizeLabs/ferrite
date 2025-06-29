/// I/O Worker Management Module
/// 
/// This module handles the worker thread lifecycle and operation processing,
/// including worker spawning, shutdown, and operation execution coordination.

use super::operations::IOOperation;
use super::queue::IOQueueManager;
use super::executor::IOOperationExecutor;
use crate::storage::disk::async_disk::io::completion::CompletionTracker;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::sync::oneshot;

/// Worker task manager responsible for managing worker threads
/// 
/// This component handles all worker-related operations including:
/// - Starting and stopping worker threads
/// - Coordinating operation processing
/// - Managing graceful shutdown
/// - Handling worker failures and recovery
#[derive(Debug)]
pub struct IOWorkerManager {
    /// Handles to all running worker tasks
    worker_handles: Vec<JoinHandle<()>>,
    
    /// Signal used to coordinate worker shutdown
    shutdown_signal: Arc<tokio::sync::Notify>,
}

impl IOWorkerManager {
    /// Creates a new I/O worker manager
    pub fn new() -> Self {
        Self {
            worker_handles: Vec::new(),
            shutdown_signal: Arc::new(tokio::sync::Notify::new()),
        }
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

            let handle = tokio::spawn(async move {
                Self::worker_loop(
                    worker_id,
                    queue_manager_clone,
                    executor_clone,
                    completion_tracker_clone,
                    shutdown_signal_clone,
                ).await;
            });

            self.worker_handles.push(handle);
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
    async fn worker_loop(
        worker_id: usize,
        queue_manager: Arc<IOQueueManager>,
        executor: Arc<IOOperationExecutor>,
        completion_tracker: Arc<CompletionTracker>,
        shutdown_signal: Arc<tokio::sync::Notify>,
    ) {
        log::debug!("Worker {} started", worker_id);
        
        loop {
            tokio::select! {
                // Check for shutdown signal
                _ = shutdown_signal.notified() => {
                    log::debug!("Worker {} received shutdown signal", worker_id);
                    break;
                }
                
                // Process operations from the queue
                operation = async {
                    loop {
                        if let Some(op) = queue_manager.dequeue_operation().await {
                            return Some(op);
                        }
                        // Small delay to prevent busy waiting when queue is empty
                        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                    }
                } => {
                    if let Some(operation) = operation {
                        log::trace!("Worker {} processing operation {}", worker_id, operation.id);
                        Self::process_operation(operation, &executor, &completion_tracker).await;
                    }
                }
            }
        }
        
        log::debug!("Worker {} shutting down", worker_id);
    }

    /// Processes a single operation
    /// 
    /// This method handles the complete lifecycle of operation processing:
    /// - Starting completion tracking
    /// - Executing the operation
    /// - Recording completion/failure metrics
    /// - Sending results back to the caller
    /// 
    /// # Arguments
    /// * `operation` - The operation to process
    /// * `executor` - The executor to run the operation
    /// * `completion_tracker` - Tracker for recording metrics
    async fn process_operation(
        operation: IOOperation,
        executor: &IOOperationExecutor,
        completion_tracker: &CompletionTracker,
    ) {
        // Destructure the operation completely to avoid partial moves
        let IOOperation {
            priority,
            id: op_id,
            operation_type,
            completion_sender,
            submitted_at,
        } = operation;

        // Reconstruct operation for execution (without completion_sender)
        let exec_operation = IOOperation {
            priority,
            id: op_id,
            operation_type,
            completion_sender: {
                let (dummy_sender, _) = oneshot::channel();
                dummy_sender
            },
            submitted_at,
        };

        // Start tracking the operation
        let (_tracked_op_id, _receiver) = completion_tracker.start_operation(None).await;

        // Execute the operation
        let result = executor.execute_operation(exec_operation).await;

        // Complete tracking and send result
        match &result {
            Ok(data) => {
                let _ = completion_tracker.complete_operation(op_id, data.clone()).await;
                log::trace!("Operation {} completed successfully", op_id);
            }
            Err(e) => {
                let _ = completion_tracker.fail_operation(op_id, e.to_string()).await;
                log::warn!("Operation {} failed: {}", op_id, e);
            }
        }

        // Send result back to the caller
        if let Err(_) = completion_sender.send(result) {
            log::warn!("Failed to send result for operation {} - receiver dropped", op_id);
        }
    }

    /// Signals all workers to shutdown gracefully
    /// 
    /// This method:
    /// 1. Sends shutdown signal to all workers
    /// 2. Waits for all workers to complete current operations
    /// 3. Cleans up worker handles
    pub async fn shutdown(&mut self) {
        if self.worker_handles.is_empty() {
            return;
        }

        log::info!("Shutting down {} I/O workers", self.worker_handles.len());
        
        // Signal all workers to shutdown
        self.shutdown_signal.notify_waiters();

        // Wait for all workers to complete
        let handles = std::mem::take(&mut self.worker_handles);
        for (worker_id, handle) in handles.into_iter().enumerate() {
            match handle.await {
                Ok(_) => log::debug!("Worker {} shut down successfully", worker_id),
                Err(e) => log::error!("Worker {} failed to shut down: {}", worker_id, e),
            }
        }

        log::info!("All I/O workers shut down");
    }

    /// Gets the number of active worker threads
    pub fn worker_count(&self) -> usize {
        self.worker_handles.len()
    }

    /// Checks if any workers are currently running
    pub fn has_active_workers(&self) -> bool {
        !self.worker_handles.is_empty()
    }

    /// Forces immediate shutdown of all workers (non-graceful)
    /// 
    /// This should only be used in emergency situations as it may
    /// cause operations to be interrupted.
    pub async fn force_shutdown(&mut self) {
        if self.worker_handles.is_empty() {
            return;
        }

        log::warn!("Force shutting down {} I/O workers", self.worker_handles.len());
        
        // Abort all worker tasks
        let handles = std::mem::take(&mut self.worker_handles);
        for handle in handles {
            handle.abort();
        }

        log::warn!("All I/O workers force shut down");
    }
}

impl Default for IOWorkerManager {
    fn default() -> Self {
        Self::new()
    }
}

// Ensure proper cleanup on drop
impl Drop for IOWorkerManager {
    fn drop(&mut self) {
        if !self.worker_handles.is_empty() {
            log::warn!("IOWorkerManager dropped with {} active workers - they will be aborted", 
                      self.worker_handles.len());
            
            // Abort any remaining workers
            for handle in &self.worker_handles {
                handle.abort();
            }
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
        let db_path = format!("/tmp/test_worker_db_{}.dat", std::process::id());
        let log_path = format!("/tmp/test_worker_log_{}.dat", std::process::id());

        // Create test files
        let mut db_file = File::create(&db_path).await.unwrap();
        let mut log_file = File::create(&log_path).await.unwrap();
        
        db_file.write_all(&vec![0u8; 8192]).await.unwrap();
        log_file.write_all(b"initial").await.unwrap();
        
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

    #[tokio::test]
    async fn test_worker_lifecycle() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) = create_test_components().await;
        
        let mut worker_manager = IOWorkerManager::new();
        
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

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }

    #[tokio::test]
    async fn test_operation_processing() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) = create_test_components().await;
        
        let queue_manager = Arc::new(queue_manager);
        let executor = Arc::new(executor);
        let completion_tracker = Arc::new(completion_tracker);
        
        let mut worker_manager = IOWorkerManager::new();
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
        
        // Verify operation was processed
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let metrics = completion_tracker.metrics();
        assert!(metrics.total_operations() > 0);
        assert!(metrics.completed_operations() > 0);
        
        worker_manager.shutdown().await;

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }

    #[tokio::test]
    async fn test_multiple_workers() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) = create_test_components().await;
        
        let queue_manager = Arc::new(queue_manager);
        let executor = Arc::new(executor);
        let completion_tracker = Arc::new(completion_tracker);
        
        let mut worker_manager = IOWorkerManager::new();
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
        assert_eq!(metrics.completed_operations(), 5);
        
        worker_manager.shutdown().await;

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }

    #[tokio::test]
    async fn test_force_shutdown() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) = create_test_components().await;
        
        let mut worker_manager = IOWorkerManager::new();
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

        // Cleanup
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(log_path);
    }
} 