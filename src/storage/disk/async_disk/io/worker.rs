//! # I/O Worker Management
//!
//! This module provides `IOWorkerManager`, which handles the lifecycle of background Tokio tasks
//! that process I/O operations. It bridges the priority queue and execution logic, providing
//! concurrency control and graceful shutdown.
//!
//! ## Architecture
//!
//! ```text
//!   AsyncDiskManager
//!   ═══════════════════════════════════════════════════════════════════════════
//!          │
//!          │ start_workers(num_workers, queue, executor, tracker)
//!          ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                       IOWorkerManager                                   │
//!   │                                                                         │
//!   │   ┌─────────────────────────────────────────────────────────────────┐   │
//!   │   │  worker_tasks: JoinSet<()>                                      │   │
//!   │   │                                                                 │   │
//!   │   │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐             │   │
//!   │   │  │Worker 0 │  │Worker 1 │  │Worker 2 │  │  ...    │             │   │
//!   │   │  │ (task)  │  │ (task)  │  │ (task)  │  │         │             │   │
//!   │   │  └────┬────┘  └────┬────┘  └────┬────┘  └─────────┘             │   │
//!   │   │       │            │            │                               │   │
//!   │   │       └────────────┼────────────┘                               │   │
//!   │   │                    │                                            │   │
//!   │   │                    ▼                                            │   │
//!   │   │       ┌────────────────────────────┐                            │   │
//!   │   │       │ concurrency_limiter        │                            │   │
//!   │   │       │ (Semaphore, max_ops)       │                            │   │
//!   │   │       └────────────────────────────┘                            │   │
//!   │   └─────────────────────────────────────────────────────────────────┘   │
//!   │                                                                         │
//!   │   ┌─────────────────────────────────────────────────────────────────┐   │
//!   │   │  Shutdown Coordination                                          │   │
//!   │   │                                                                 │   │
//!   │   │  shutdown_signal: Arc<Notify>   ← Wake waiting workers          │   │
//!   │   │  shutdown_flag: Arc<AtomicBool> ← Reliable shutdown detection   │   │
//!   │   └─────────────────────────────────────────────────────────────────┘   │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Worker Loop Flow
//!
//! ```text
//!   worker_loop(worker_id)
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │ Loop Start                                                              │
//!   └───────────────────────────────┬─────────────────────────────────────────┘
//!                                   │
//!                                   ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │ Check shutdown_flag                                                     │
//!   │                                                                         │
//!   │   shutdown_flag.load()?                                                 │
//!   │        │                                                                │
//!   │        ├── true:  Break loop (exit worker)                              │
//!   │        └── false: Continue                                              │
//!   └───────────────────────────────┬─────────────────────────────────────────┘
//!                                   │
//!                                   ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │ tokio::select! (race between shutdown and work)                         │
//!   │                                                                         │
//!   │   ┌─────────────────────────┐    ┌─────────────────────────┐            │
//!   │   │ shutdown_signal.notified│ OR │ queue.dequeue_operation │            │
//!   │   └───────────┬─────────────┘    └───────────┬─────────────┘            │
//!   │               │                              │                          │
//!   │               ▼                              ▼                          │
//!   │         Break loop                    Got operation?                    │
//!   │                                              │                          │
//!   │                              ┌───────────────┼───────────────┐          │
//!   │                              │ Some(op)      │ None          │          │
//!   │                              ▼               ▼               │          │
//!   │                        process_operation  sleep(1ms)         │          │
//!   │                              │               │               │          │
//!   │                              └───────────────┴───────────────┘          │
//!   └───────────────────────────────┬─────────────────────────────────────────┘
//!                                   │
//!                                   └──► Continue loop
//! ```
//!
//! ## Operation Processing
//!
//! ```text
//!   process_operation(operation, executor, tracker, semaphore)
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │ Step 1: CHECK IF STILL PENDING                                          │
//!   │                                                                         │
//!   │   tracker.is_operation_pending(op_id)?                                  │
//!   │        │                                                                │
//!   │        ├── false: Skip (already cancelled/timed out)                    │
//!   │        └── true:  Continue                                              │
//!   └───────────────────────────────┬─────────────────────────────────────────┘
//!                                   │
//!                                   ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │ Step 2: ACQUIRE SEMAPHORE PERMIT                                        │
//!   │                                                                         │
//!   │   semaphore.acquire().await                                             │
//!   │        │                                                                │
//!   │        ├── Err: Semaphore closed → return (shutdown)                    │
//!   │        └── Ok(permit): Hold permit for duration of operation            │
//!   │                                                                         │
//!   │   ⚠️ Backpressure: Only max_concurrent_operations can run at once       │
//!   └───────────────────────────────┬─────────────────────────────────────────┘
//!                                   │
//!                                   ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │ Step 3: EXECUTE OPERATION                                               │
//!   │                                                                         │
//!   │   executor.execute_operation_type(op.operation_type).await              │
//!   │        │                                                                │
//!   │        ├── Ok(data):  Complete with OperationResult::Success(data)      │
//!   │        └── Err(err):  Complete with OperationResult::Error(msg)         │
//!   └───────────────────────────────┬─────────────────────────────────────────┘
//!                                   │
//!                                   ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │ Step 4: REPORT COMPLETION                                               │
//!   │                                                                         │
//!   │   tracker.complete_operation_with_result(op_id, result).await           │
//!   │        │                                                                │
//!   │        └── If Err: Operation was cancelled, just log and continue       │
//!   └─────────────────────────────────────────────────────────────────────────┘
//!                                   │
//!                                   ▼
//!   permit dropped automatically (RAII) → releases semaphore slot
//! ```
//!
//! ## Key Components
//!
//! | Component             | Description                                        |
//! |-----------------------|----------------------------------------------------|
//! | `IOWorkerManager`     | Manages worker lifecycle and shutdown coordination |
//! | `worker_tasks`        | `JoinSet<()>` holding all spawned worker tasks     |
//! | `shutdown_signal`     | `Arc<Notify>` to wake workers waiting on queue     |
//! | `shutdown_flag`       | `Arc<AtomicBool>` for reliable shutdown detection  |
//! | `concurrency_limiter` | `Arc<Semaphore>` limiting concurrent I/O operations|
//!
//! ## Core Operations
//!
//! | Method                        | Description                                 |
//! |-------------------------------|---------------------------------------------|
//! | `new()`                       | Create with specific concurrency limit      |
//! | `new_with_default_concurrency()`| Create with default limit (32)            |
//! | `start_workers()`             | Spawn worker tasks with shared components   |
//! | `signal_shutdown()`           | Signal shutdown without waiting (`&self`)   |
//! | `shutdown()`                  | Graceful shutdown, waits for completion     |
//! | `force_shutdown()`            | Abort all workers immediately               |
//! | `worker_count()`              | Get number of active workers                |
//! | `has_active_workers()`        | Check if any workers are running            |
//!
//! ## Shutdown Strategies
//!
//! | Strategy           | Behavior                                            |
//! |--------------------|-----------------------------------------------------|
//! | `signal_shutdown()`| Sets flag + notifies, returns immediately (`&self`) |
//! | `shutdown()`       | Sets flag + notifies, awaits all workers to finish  |
//! | `force_shutdown()` | Aborts all tasks immediately (may drop in-flight)   |
//! | Drop               | Logs warning, aborts remaining workers              |
//!
//! ## Concurrency Control
//!
//! ```text
//!   Workers: 4              Semaphore permits: 2
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   Worker 0  ─────────────────────►  Acquire permit ✓
//!                                     [Executing I/O...]
//!
//!   Worker 1  ─────────────────────►  Acquire permit ✓
//!                                     [Executing I/O...]
//!
//!   Worker 2  ─────────────────────►  Waiting for permit...
//!                                     (blocked until Worker 0 or 1 finishes)
//!
//!   Worker 3  ─────────────────────►  Waiting for permit...
//!                                     (blocked until permit available)
//!
//!   Benefit: Prevents overwhelming disk subsystem even with many workers
//! ```
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::storage::disk::async_disk::io::worker::IOWorkerManager;
//! use std::sync::Arc;
//!
//! // Create worker manager with concurrency limit of 16
//! let mut worker_manager = IOWorkerManager::new(16);
//!
//! // Start 4 worker tasks
//! worker_manager.start_workers(
//!     4,
//!     Arc::clone(&queue_manager),
//!     Arc::clone(&executor),
//!     Arc::clone(&completion_tracker),
//! );
//!
//! assert_eq!(worker_manager.worker_count(), 4);
//! assert!(worker_manager.has_active_workers());
//!
//! // ... process operations ...
//!
//! // Graceful shutdown (waits for in-flight operations)
//! worker_manager.shutdown().await;
//!
//! assert_eq!(worker_manager.worker_count(), 0);
//!
//! // Can restart workers after shutdown
//! worker_manager.start_workers(2, queue, executor, tracker);
//! ```
//!
//! ## Shared Components
//!
//! ```text
//!   start_workers() receives:
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                                                                         │
//!   │   Arc<IOQueueManager>     ← Priority queue of pending operations        │
//!   │   Arc<IOOperationExecutor>← Executes reads/writes on files              │
//!   │   Arc<CompletionTracker>  ← Tracks operation state and metrics          │
//!   │                                                                         │
//!   └─────────────────────────────────────────────────────────────────────────┘
//!
//!   Each worker gets Arc::clone() of all components
//!   → Safe concurrent access from multiple workers
//! ```
//!
//! ## Thread Safety
//!
//! - `JoinSet`: Tokio's built-in task management (task-safe)
//! - `shutdown_signal`: `Arc<Notify>` for async wake-up
//! - `shutdown_flag`: `Arc<AtomicBool>` with `Ordering::Relaxed` (sufficient for flags)
//! - `concurrency_limiter`: `Arc<Semaphore>` for fair permit distribution
//! - All shared components (`queue`, `executor`, `tracker`) wrapped in `Arc`
//!
//! ## Drop Behavior
//!
//! If `IOWorkerManager` is dropped with active workers:
//! - Logs a warning with the count of remaining workers
//! - Calls `abort_all()` on the `JoinSet` to stop them immediately
//! - Does NOT wait for graceful shutdown (use `shutdown()` before drop)

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use super::executor::IOOperationExecutor;
use super::operations::IOOperation;
use super::queue::IOQueueManager;
use crate::storage::disk::async_disk::io::completion::CompletionTracker;
use crate::storage::disk::async_disk::io::operation_status::OperationResult;

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

    /// Atomic flag for reliable shutdown signaling
    shutdown_flag: Arc<AtomicBool>,

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
            shutdown_flag: Arc::new(AtomicBool::new(false)),
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
            let shutdown_flag_clone = Arc::clone(&self.shutdown_flag);
            let concurrency_limiter_clone = Arc::clone(&self.concurrency_limiter);

            self.worker_tasks.spawn(async move {
                Self::worker_loop(
                    worker_id,
                    queue_manager_clone,
                    executor_clone,
                    completion_tracker_clone,
                    shutdown_signal_clone,
                    shutdown_flag_clone,
                    concurrency_limiter_clone,
                )
                .await;
            });
        }

        log::info!("Started {} I/O worker threads", num_workers);
    }

    /// Signals all workers to begin shutdown without waiting for them.
    ///
    /// This is useful when callers only have `&self` access (e.g. initiating shutdown
    /// from a different component) and will later call `shutdown()` to await completion.
    pub fn signal_shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::Relaxed);
        self.shutdown_signal.notify_waiters();
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
    /// * `shutdown_flag` - Atomic flag for reliable shutdown detection
    /// * `concurrency_limiter` - Semaphore for limiting concurrent operations
    async fn worker_loop(
        worker_id: usize,
        queue_manager: Arc<IOQueueManager>,
        executor: Arc<IOOperationExecutor>,
        completion_tracker: Arc<CompletionTracker>,
        shutdown_signal: Arc<tokio::sync::Notify>,
        shutdown_flag: Arc<AtomicBool>,
        concurrency_limiter: Arc<Semaphore>,
    ) {
        log::debug!("Worker {} started", worker_id);

        loop {
            // Check shutdown flag at the beginning of each iteration
            if shutdown_flag.load(Ordering::Relaxed) {
                log::debug!("Worker {} detected shutdown flag", worker_id);
                break;
            }

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
                        // Queue is empty, check shutdown flag again before sleeping
                        if shutdown_flag.load(Ordering::Relaxed) {
                            log::debug!("Worker {} detected shutdown flag while queue empty", worker_id);
                            break;
                        }
                        // Sleep briefly to avoid tight polling loop
                        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
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
        let op_id = operation.id;

        // If the operation has already been cancelled/timed out, skip executing the I/O.
        if !completion_tracker.is_operation_pending(op_id).await {
            log::trace!(
                "Skipping execution for operation {} (no longer pending)",
                op_id
            );
            return;
        }

        // Acquire semaphore permit to limit concurrent I/O operations
        // This provides backpressure and prevents overwhelming the disk subsystem
        let _permit = match concurrency_limiter.acquire().await {
            Ok(permit) => permit,
            Err(_) => {
                log::error!(
                    "Failed to acquire concurrency permit for operation - semaphore closed"
                );
                return;
            },
        };

        // Execute the operation and complete it via the tracker.
        let result = executor
            .execute_operation_type(operation.operation_type)
            .await;

        match result {
            Ok(data) => {
                if let Err(e) = completion_tracker
                    .complete_operation_with_result(op_id, OperationResult::Success(data))
                    .await
                {
                    log::debug!(
                        "Operation {} completed but could not be recorded (likely cancelled): {}",
                        op_id,
                        e
                    );
                }
            },
            Err(err) => {
                let msg = format!("{:?}: {}", err.kind(), err);
                if let Err(e) = completion_tracker
                    .complete_operation_with_result(op_id, OperationResult::Error(msg))
                    .await
                {
                    log::debug!(
                        "Operation {} failed but could not be recorded (likely cancelled): {}",
                        op_id,
                        e
                    );
                }
            },
        }
    }

    /// Signals all workers to shutdown gracefully
    ///
    /// This method:
    /// 1. Sets shutdown flag and sends shutdown signal to all workers
    /// 2. Waits for all workers to complete current operations
    /// 3. Cleans up worker tasks
    pub async fn shutdown(&mut self) {
        if self.worker_tasks.is_empty() {
            return;
        }

        log::info!("Shutting down {} I/O workers", self.worker_tasks.len());

        // Set shutdown flag for reliable signaling
        self.shutdown_flag.store(true, Ordering::Relaxed);

        // Also notify any workers waiting on the signal
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

        // Reset shutdown flag for potential reuse
        self.shutdown_flag.store(false, Ordering::Relaxed);

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

        log::warn!(
            "Force shutting down {} I/O workers",
            self.worker_tasks.len()
        );

        // Set shutdown flag
        self.shutdown_flag.store(true, Ordering::Relaxed);

        // Abort all worker tasks using JoinSet
        self.worker_tasks.abort_all();

        // Drain all aborted tasks to update the count
        while let Some(result) = self.worker_tasks.join_next().await {
            match result {
                Ok(()) => log::debug!("Worker task completed during force shutdown"),
                Err(e) if e.is_cancelled() => {
                    log::debug!("Worker task was cancelled during force shutdown")
                },
                Err(e) => log::warn!("Worker task failed during force shutdown: {}", e),
            }
        }

        // Reset shutdown flag
        self.shutdown_flag.store(false, Ordering::Relaxed);

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
            log::warn!(
                "IOWorkerManager dropped with {} active workers - they will be aborted",
                self.worker_tasks.len()
            );

            // Abort any remaining workers using JoinSet
            self.worker_tasks.abort_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;
    use tokio::sync::Mutex;

    use super::*;
    use crate::common::config::DB_PAGE_SIZE;
    use crate::storage::disk::async_disk::io::operations::{IOOperationType, priorities};

    async fn create_test_components(
        name: &str,
    ) -> (
        IOQueueManager,
        IOOperationExecutor,
        CompletionTracker,
        String,
        String,
    ) {
        // Create a temporary directory
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

        println!("Test files: db: {}, log: {}", db_path, log_path);

        // Create test files
        let mut db_file = File::create(&db_path).await.unwrap();
        let mut log_file = File::create(&log_path).await.unwrap();

        db_file.write_all(&vec![0u8; 8192]).await.unwrap();
        log_file.write_all(b"initial").await.unwrap();

        println!("Test files created");

        // Ensure files are flushed before closing
        db_file.flush().await.unwrap();
        log_file.flush().await.unwrap();

        println!("Test files flushed");

        drop(db_file);
        drop(log_file);

        println!("Test files dropped");

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

        println!("Test files opened");

        let queue_manager = IOQueueManager::new();
        let executor = IOOperationExecutor::new(db_file, log_file);
        let completion_tracker = CompletionTracker::new();

        println!("Test components created");

        (
            queue_manager,
            executor,
            completion_tracker,
            db_path,
            log_path,
        )
    }

    async fn cleanup_test_files(db_path: &str, log_path: &str) {
        let _ = tokio::fs::remove_file(db_path).await;
        let _ = tokio::fs::remove_file(log_path).await;
    }

    #[tokio::test]
    async fn test_worker_lifecycle() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) =
            create_test_components("test_worker_lifecycle").await;

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
        let (queue_manager, executor, completion_tracker, db_path, log_path) =
            create_test_components("test_operation_processing").await;

        let queue_manager = Arc::new(queue_manager);
        let executor = Arc::new(executor);
        let completion_tracker = Arc::new(completion_tracker);

        let mut worker_manager = IOWorkerManager::new(16);
        worker_manager.start_workers(
            1,
            Arc::clone(&queue_manager),
            Arc::clone(&executor),
            Arc::clone(&completion_tracker),
        );

        // Enqueue an operation
        let test_data = vec![42u8; 4096];
        let (op_id, receiver) = completion_tracker.start_operation(None).await;
        queue_manager
            .enqueue_operation(
                IOOperationType::WritePage {
                    page_id: 0,
                    data: test_data.clone(),
                },
                priorities::PAGE_WRITE,
                op_id,
            )
            .await;

        // Wait for operation to complete
        let result = receiver.await.unwrap();
        match result {
            OperationResult::Success(bytes) => assert_eq!(bytes, test_data),
            OperationResult::Error(err) => panic!("Expected success, got error: {}", err),
        }

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
        let (queue_manager, executor, completion_tracker, db_path, log_path) =
            create_test_components("test_multiple_workers").await;

        let queue_manager = Arc::new(queue_manager);
        let executor = Arc::new(executor);
        let completion_tracker = Arc::new(completion_tracker);

        let mut worker_manager = IOWorkerManager::new(16);
        worker_manager.start_workers(
            3,
            Arc::clone(&queue_manager),
            Arc::clone(&executor),
            Arc::clone(&completion_tracker),
        );

        // Enqueue multiple operations
        let mut receivers = Vec::new();
        for i in 0..5 {
            let test_data = vec![i as u8; 4096];
            let (op_id, receiver) = completion_tracker.start_operation(None).await;
            queue_manager
                .enqueue_operation(
                    IOOperationType::WritePage {
                        page_id: i,
                        data: test_data,
                    },
                    priorities::PAGE_WRITE,
                    op_id,
                )
                .await;
            receivers.push(receiver);
        }

        // Wait for all operations to complete
        for receiver in receivers {
            let result = receiver.await.unwrap();
            match result {
                OperationResult::Success(_) => {},
                OperationResult::Error(err) => panic!("Expected success, got error: {}", err),
            }
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
        let (queue_manager, executor, completion_tracker, db_path, log_path) =
            create_test_components("test_force_shutdown").await;

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
        let (queue_manager, executor, completion_tracker, db_path, log_path) =
            create_test_components("test_operation_error_handling").await;

        let queue_manager = Arc::new(queue_manager);
        let executor = Arc::new(executor);
        let completion_tracker = Arc::new(completion_tracker);

        let mut worker_manager = IOWorkerManager::new(16);
        worker_manager.start_workers(
            1,
            Arc::clone(&queue_manager),
            Arc::clone(&executor),
            Arc::clone(&completion_tracker),
        );

        // Try to read from a very large page that doesn't exist (would cause EOF error)
        let (op_id, receiver) = completion_tracker.start_operation(None).await;
        queue_manager
            .enqueue_operation(
                IOOperationType::ReadPage { page_id: 1000 }, // Page that doesn't exist in our small test file
                priorities::PAGE_READ,
                op_id,
            )
            .await;

        // Wait for operation to complete
        let result = receiver.await.unwrap();
        assert!(
            matches!(result, OperationResult::Error(_)),
            "Reading from non-existent page should fail"
        );

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
        let (queue_manager, executor, completion_tracker, db_path, log_path) =
            create_test_components("test_concurrency_limiting").await;

        let queue_manager = Arc::new(queue_manager);
        let executor = Arc::new(executor);
        let completion_tracker = Arc::new(completion_tracker);

        // Create worker manager with very limited concurrency
        let mut worker_manager = IOWorkerManager::new(1);
        worker_manager.start_workers(
            2,
            Arc::clone(&queue_manager),
            Arc::clone(&executor),
            Arc::clone(&completion_tracker),
        );

        // Enqueue multiple operations
        let mut receivers = Vec::new();
        for i in 0..3 {
            let test_data = vec![i as u8; 4096];
            let (op_id, receiver) = completion_tracker.start_operation(None).await;
            queue_manager
                .enqueue_operation(
                    IOOperationType::WritePage {
                        page_id: i,
                        data: test_data,
                    },
                    priorities::PAGE_WRITE,
                    op_id,
                )
                .await;
            receivers.push(receiver);
        }

        // Operations should still complete despite concurrency limiting
        for receiver in receivers {
            let result = receiver.await.unwrap();
            match result {
                OperationResult::Success(_) => {},
                OperationResult::Error(err) => panic!("Expected success, got error: {}", err),
            }
        }

        worker_manager.shutdown().await;

        cleanup_test_files(&db_path, &log_path).await;
    }

    #[tokio::test]
    async fn test_queue_overflow_handling() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) =
            create_test_components("test_queue_overflow_handling").await;

        let queue_manager = Arc::new(queue_manager);
        let executor = Arc::new(executor);
        let completion_tracker = Arc::new(completion_tracker);

        // Create worker manager with minimal concurrency to create backpressure
        let mut worker_manager = IOWorkerManager::new(1);
        worker_manager.start_workers(
            1,
            Arc::clone(&queue_manager),
            Arc::clone(&executor),
            Arc::clone(&completion_tracker),
        );

        // Enqueue many operations quickly to test queue handling
        let mut receivers = Vec::new();
        for i in 0..50 {
            let test_data = vec![i as u8; DB_PAGE_SIZE as usize];
            let (op_id, receiver) = completion_tracker.start_operation(None).await;
            queue_manager
                .enqueue_operation(
                    IOOperationType::WritePage {
                        page_id: i,
                        data: test_data,
                    },
                    priorities::PAGE_WRITE,
                    op_id,
                )
                .await;
            receivers.push(receiver);
        }

        // All operations should eventually complete
        for receiver in receivers {
            let result = receiver.await.unwrap();
            match result {
                OperationResult::Success(_) => {},
                OperationResult::Error(err) => panic!("Expected success, got error: {}", err),
            }
        }

        // Verify all operations were processed
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let metrics = completion_tracker.metrics();
        assert!(metrics.completed_operations() >= 50);

        worker_manager.shutdown().await;
        cleanup_test_files(&db_path, &log_path).await;
    }

    #[tokio::test]
    async fn test_priority_handling() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) =
            create_test_components("test_priority_handling").await;

        let queue_manager = Arc::new(queue_manager);
        let executor = Arc::new(executor);
        let completion_tracker = Arc::new(completion_tracker);

        let mut worker_manager = IOWorkerManager::new(16);
        worker_manager.start_workers(
            1,
            Arc::clone(&queue_manager),
            Arc::clone(&executor),
            Arc::clone(&completion_tracker),
        );

        // Enqueue operations with different priorities
        let mut receivers = Vec::new();

        // Low priority operations
        for i in 0..3 {
            let test_data = vec![i as u8; DB_PAGE_SIZE as usize];
            let (op_id, receiver) = completion_tracker.start_operation(None).await;
            queue_manager
                .enqueue_operation(
                    IOOperationType::WritePage {
                        page_id: i,
                        data: test_data,
                    },
                    priorities::PAGE_WRITE, // Normal priority
                    op_id,
                )
                .await;
            receivers.push(receiver);
        }

        // High priority operations
        for i in 10..13 {
            let test_data = vec![i as u8; DB_PAGE_SIZE as usize];
            let (op_id, receiver) = completion_tracker.start_operation(None).await;
            queue_manager
                .enqueue_operation(
                    IOOperationType::WritePage {
                        page_id: i,
                        data: test_data,
                    },
                    priorities::LOG_WRITE, // Higher priority
                    op_id,
                )
                .await;
            receivers.push(receiver);
        }

        // All operations should complete regardless of priority
        for receiver in receivers {
            let result = receiver.await.unwrap();
            match result {
                OperationResult::Success(_) => {},
                OperationResult::Error(err) => panic!("Expected success, got error: {}", err),
            }
        }

        worker_manager.shutdown().await;
        cleanup_test_files(&db_path, &log_path).await;
    }

    #[tokio::test]
    async fn test_empty_queue_handling() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) =
            create_test_components("test_empty_queue_handling").await;

        let queue_manager = Arc::new(queue_manager);
        let executor = Arc::new(executor);
        let completion_tracker = Arc::new(completion_tracker);

        let mut worker_manager = IOWorkerManager::new(16);
        worker_manager.start_workers(
            2,
            Arc::clone(&queue_manager),
            Arc::clone(&executor),
            Arc::clone(&completion_tracker),
        );

        // Let workers run with empty queue for a short time
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Workers should still be active
        assert_eq!(worker_manager.worker_count(), 2);
        assert!(worker_manager.has_active_workers());

        // Add a single operation to verify workers are still responsive
        let test_data = vec![42u8; DB_PAGE_SIZE as usize];
        let (op_id, receiver) = completion_tracker.start_operation(None).await;
        queue_manager
            .enqueue_operation(
                IOOperationType::WritePage {
                    page_id: 0,
                    data: test_data.clone(),
                },
                priorities::PAGE_WRITE,
                op_id,
            )
            .await;

        let result = receiver.await.unwrap();
        match result {
            OperationResult::Success(bytes) => assert_eq!(bytes, test_data),
            OperationResult::Error(err) => panic!("Expected success, got error: {}", err),
        }

        worker_manager.shutdown().await;
        cleanup_test_files(&db_path, &log_path).await;
    }

    #[tokio::test]
    async fn test_shutdown_during_operations() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) =
            create_test_components("test_shutdown_during_operations").await;

        let queue_manager = Arc::new(queue_manager);
        let executor = Arc::new(executor);
        let completion_tracker = Arc::new(completion_tracker);

        let mut worker_manager = IOWorkerManager::new(16);
        worker_manager.start_workers(
            2,
            Arc::clone(&queue_manager),
            Arc::clone(&executor),
            Arc::clone(&completion_tracker),
        );

        // Enqueue operations
        let mut receivers = Vec::new();
        for i in 0..5 {
            let test_data = vec![i as u8; 4096];
            let (op_id, receiver) = completion_tracker.start_operation(None).await;
            queue_manager
                .enqueue_operation(
                    IOOperationType::WritePage {
                        page_id: i,
                        data: test_data,
                    },
                    priorities::PAGE_WRITE,
                    op_id,
                )
                .await;
            receivers.push(receiver);
        }

        // Start shutdown immediately without waiting for operations to complete
        let shutdown_task = tokio::spawn(async move {
            worker_manager.shutdown().await;
        });

        // Race the shutdown against operation completion with timeout
        let operation_task = tokio::spawn(async move {
            let mut completed_count = 0;
            for receiver in receivers {
                // Use timeout to avoid hanging indefinitely
                match tokio::time::timeout(tokio::time::Duration::from_millis(500), receiver).await
                {
                    Ok(Ok(OperationResult::Success(_))) => completed_count += 1,
                    Ok(Ok(OperationResult::Error(_))) => (), // acceptable during shutdown
                    Ok(Err(_)) => (),                        // receiver dropped
                    Err(_) => (), // Timeout - operation didn't complete in time due to shutdown
                }
            }
            completed_count
        });

        // Wait for both tasks to complete
        let (shutdown_result, operation_result) = tokio::join!(shutdown_task, operation_task);

        shutdown_result.unwrap();
        let completed_count = operation_result.unwrap();

        // Some operations may have completed before shutdown
        println!("Completed operations during shutdown: {}", completed_count);

        cleanup_test_files(&db_path, &log_path).await;
    }

    #[tokio::test]
    async fn test_different_operation_types() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) =
            create_test_components("test_different_operation_types").await;

        let queue_manager = Arc::new(queue_manager);
        let executor = Arc::new(executor);
        let completion_tracker = Arc::new(completion_tracker);

        let mut worker_manager = IOWorkerManager::new(16);
        worker_manager.start_workers(
            2,
            Arc::clone(&queue_manager),
            Arc::clone(&executor),
            Arc::clone(&completion_tracker),
        );

        // Test different operation types
        let mut receivers = Vec::new();

        // Write operations
        for i in 0..3 {
            let test_data = vec![i as u8; DB_PAGE_SIZE as usize];
            let (op_id, receiver) = completion_tracker.start_operation(None).await;
            queue_manager
                .enqueue_operation(
                    IOOperationType::WritePage {
                        page_id: i,
                        data: test_data,
                    },
                    priorities::PAGE_WRITE,
                    op_id,
                )
                .await;
            receivers.push(receiver);
        }

        // Read operations
        for i in 0..3 {
            let (op_id, receiver) = completion_tracker.start_operation(None).await;
            queue_manager
                .enqueue_operation(
                    IOOperationType::ReadPage { page_id: i },
                    priorities::PAGE_READ,
                    op_id,
                )
                .await;
            receivers.push(receiver);
        }

        // Log operations
        let log_data = b"test log entry".to_vec();
        let (op_id, receiver) = completion_tracker.start_operation(None).await;
        queue_manager
            .enqueue_operation(
                IOOperationType::AppendLog { data: log_data },
                priorities::LOG_WRITE,
                op_id,
            )
            .await;
        receivers.push(receiver);

        // Wait for all operations to complete
        for receiver in receivers {
            let _result = receiver.await.unwrap(); // Some may fail, which is OK
        }

        // Verify operations were processed
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let metrics = completion_tracker.metrics();
        assert!(metrics.total_operations() > 0);

        worker_manager.shutdown().await;
        cleanup_test_files(&db_path, &log_path).await;
    }

    #[tokio::test]
    async fn test_worker_restart_after_shutdown() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) =
            create_test_components("test_worker_restart_after_shutdown").await;

        let queue_manager = Arc::new(queue_manager);
        let executor = Arc::new(executor);
        let completion_tracker = Arc::new(completion_tracker);

        let mut worker_manager = IOWorkerManager::new(16);

        // Start workers
        worker_manager.start_workers(
            2,
            Arc::clone(&queue_manager),
            Arc::clone(&executor),
            Arc::clone(&completion_tracker),
        );
        assert_eq!(worker_manager.worker_count(), 2);

        // Shutdown workers
        worker_manager.shutdown().await;
        assert_eq!(worker_manager.worker_count(), 0);

        // Restart workers
        worker_manager.start_workers(
            3,
            Arc::clone(&queue_manager),
            Arc::clone(&executor),
            Arc::clone(&completion_tracker),
        );
        assert_eq!(worker_manager.worker_count(), 3);

        // Verify workers are functional
        let test_data = vec![42u8; DB_PAGE_SIZE as usize];
        let (op_id, receiver) = completion_tracker.start_operation(None).await;
        queue_manager
            .enqueue_operation(
                IOOperationType::WritePage {
                    page_id: 0,
                    data: test_data.clone(),
                },
                priorities::PAGE_WRITE,
                op_id,
            )
            .await;

        let result = receiver.await.unwrap();
        match result {
            OperationResult::Success(bytes) => assert_eq!(bytes, test_data),
            OperationResult::Error(err) => panic!("Expected success, got error: {}", err),
        }

        worker_manager.shutdown().await;
        cleanup_test_files(&db_path, &log_path).await;
    }

    #[tokio::test]
    async fn test_metrics_accuracy() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) =
            create_test_components("test_metrics_accuracy").await;

        let queue_manager = Arc::new(queue_manager);
        let executor = Arc::new(executor);
        let completion_tracker = Arc::new(completion_tracker);

        let mut worker_manager = IOWorkerManager::new(16);
        worker_manager.start_workers(
            1,
            Arc::clone(&queue_manager),
            Arc::clone(&executor),
            Arc::clone(&completion_tracker),
        );

        let initial_metrics = completion_tracker.metrics();
        let initial_total = initial_metrics.total_operations();
        let initial_completed = initial_metrics.completed_operations();

        // Enqueue successful operations
        let mut receivers = Vec::new();
        for i in 0..5 {
            let test_data = vec![i as u8; DB_PAGE_SIZE as usize];
            let (op_id, receiver) = completion_tracker.start_operation(None).await;
            queue_manager
                .enqueue_operation(
                    IOOperationType::WritePage {
                        page_id: i,
                        data: test_data,
                    },
                    priorities::PAGE_WRITE,
                    op_id,
                )
                .await;
            receivers.push(receiver);
        }

        // Wait for all operations to complete
        for receiver in receivers {
            let result = receiver.await.unwrap();
            match result {
                OperationResult::Success(_) => {},
                OperationResult::Error(err) => panic!("Expected success, got error: {}", err),
            }
        }

        // Enqueue an operation that should fail
        let (op_id, receiver) = completion_tracker.start_operation(None).await;
        queue_manager
            .enqueue_operation(
                IOOperationType::ReadPage { page_id: 1000 }, // Non-existent page
                priorities::PAGE_READ,
                op_id,
            )
            .await;

        let result = receiver.await.unwrap();
        assert!(matches!(result, OperationResult::Error(_)));

        // Wait for metrics to update
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let final_metrics = completion_tracker.metrics();

        // Verify metrics are accurate
        assert_eq!(final_metrics.total_operations(), initial_total + 6);
        assert_eq!(final_metrics.completed_operations(), initial_completed + 5);
        assert!(final_metrics.failed_operations() > 0);

        worker_manager.shutdown().await;
        cleanup_test_files(&db_path, &log_path).await;
    }

    #[tokio::test]
    async fn test_semaphore_exhaustion() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) =
            create_test_components("test_semaphore_exhaustion").await;

        let queue_manager = Arc::new(queue_manager);
        let executor = Arc::new(executor);
        let completion_tracker = Arc::new(completion_tracker);

        // Create worker manager with very limited concurrency
        let mut worker_manager = IOWorkerManager::new(1);
        worker_manager.start_workers(
            3,
            Arc::clone(&queue_manager),
            Arc::clone(&executor),
            Arc::clone(&completion_tracker),
        );

        // Enqueue more operations than semaphore permits
        let mut receivers = Vec::new();
        for i in 0..10 {
            let test_data = vec![i as u8; DB_PAGE_SIZE as usize];
            let (op_id, receiver) = completion_tracker.start_operation(None).await;
            queue_manager
                .enqueue_operation(
                    IOOperationType::WritePage {
                        page_id: i,
                        data: test_data,
                    },
                    priorities::PAGE_WRITE,
                    op_id,
                )
                .await;
            receivers.push(receiver);
        }

        // All operations should eventually complete despite semaphore limiting
        for receiver in receivers {
            let result = receiver.await.unwrap();
            match result {
                OperationResult::Success(_) => {},
                OperationResult::Error(err) => panic!("Expected success, got error: {}", err),
            }
        }

        // Verify all operations were processed
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let metrics = completion_tracker.metrics();
        assert!(metrics.completed_operations() >= 10);

        worker_manager.shutdown().await;
        cleanup_test_files(&db_path, &log_path).await;
    }

    #[tokio::test]
    async fn test_default_constructor() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) =
            create_test_components("test_default_constructor").await;

        let mut worker_manager = IOWorkerManager::default();

        // Should have default concurrency limit
        assert_eq!(worker_manager.worker_count(), 0);
        assert!(!worker_manager.has_active_workers());

        // Should be able to start workers
        worker_manager.start_workers(
            2,
            Arc::new(queue_manager),
            Arc::new(executor),
            Arc::new(completion_tracker),
        );

        assert_eq!(worker_manager.worker_count(), 2);
        assert!(worker_manager.has_active_workers());

        worker_manager.shutdown().await;
        cleanup_test_files(&db_path, &log_path).await;
    }

    #[tokio::test]
    async fn test_concurrent_shutdown_calls() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) =
            create_test_components("test_concurrent_shutdown_calls").await;

        let queue_manager = Arc::new(queue_manager);
        let executor = Arc::new(executor);
        let completion_tracker = Arc::new(completion_tracker);

        let mut worker_manager = IOWorkerManager::new(16);
        worker_manager.start_workers(
            2,
            Arc::clone(&queue_manager),
            Arc::clone(&executor),
            Arc::clone(&completion_tracker),
        );

        // Call shutdown multiple times concurrently
        let worker_manager = Arc::new(Mutex::new(worker_manager));

        let mut shutdown_tasks = Vec::new();
        for _ in 0..3 {
            let worker_manager_clone = Arc::clone(&worker_manager);
            let task = tokio::spawn(async move {
                let mut manager = worker_manager_clone.lock().await;
                manager.shutdown().await;
            });
            shutdown_tasks.push(task);
        }

        // Wait for all shutdown calls to complete
        for task in shutdown_tasks {
            task.await.unwrap();
        }

        // Verify all workers are shut down
        let manager = worker_manager.lock().await;
        assert_eq!(manager.worker_count(), 0);

        cleanup_test_files(&db_path, &log_path).await;
    }

    #[tokio::test]
    async fn test_large_data_operations() {
        let (queue_manager, executor, completion_tracker, db_path, log_path) =
            create_test_components("test_large_data_operations").await;

        let queue_manager = Arc::new(queue_manager);
        let executor = Arc::new(executor);
        let completion_tracker = Arc::new(completion_tracker);

        let mut worker_manager = IOWorkerManager::new(16);
        worker_manager.start_workers(
            2,
            Arc::clone(&queue_manager),
            Arc::clone(&executor),
            Arc::clone(&completion_tracker),
        );

        // Test with large data via log operations (log ops support variable sizes)
        let large_data = vec![0xFFu8; 64 * 1024]; // 64KB
        let (op_id, receiver) = completion_tracker.start_operation(None).await;
        queue_manager
            .enqueue_operation(
                IOOperationType::AppendLog {
                    data: large_data.clone(),
                },
                priorities::LOG_APPEND,
                op_id,
            )
            .await;

        let offset_bytes = receiver.await.unwrap();
        let offset_bytes = match offset_bytes {
            OperationResult::Success(bytes) => bytes,
            OperationResult::Error(err) => panic!("Expected success, got error: {}", err),
        };
        let offset: u64 = u64::from_le_bytes(
            offset_bytes
                .as_slice()
                .try_into()
                .expect("AppendLog must return 8-byte offset"),
        );

        let (op_id, receiver) = completion_tracker.start_operation(None).await;
        queue_manager
            .enqueue_operation(
                IOOperationType::ReadLog {
                    offset,
                    size: large_data.len(),
                },
                priorities::LOG_READ,
                op_id,
            )
            .await;

        let read_back = receiver.await.unwrap();
        let read_back = match read_back {
            OperationResult::Success(bytes) => bytes,
            OperationResult::Error(err) => panic!("Expected success, got error: {}", err),
        };
        assert_eq!(read_back, large_data);

        // Verify metrics recorded the large data size
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        let metrics = completion_tracker.metrics();
        assert!(metrics.total_operations() > 0);
        assert!(metrics.completed_operations() > 0);

        worker_manager.shutdown().await;
        cleanup_test_files(&db_path, &log_path).await;
    }
}
