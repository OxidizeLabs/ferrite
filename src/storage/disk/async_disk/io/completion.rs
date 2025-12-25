//! # I/O Completion Tracking
//!
//! This module provides `CompletionTracker`, a robust mechanism for tracking the lifecycle and
//! status of asynchronous I/O operations. It bridges low-level async I/O execution with high-level
//! request management.
//!
//! ## Architecture
//!
//! ```text
//!   Caller (AsyncIOEngine)                       Worker (IOWorkerManager)
//!   ═══════════════════════════════════════════════════════════════════════════
//!          │                                              │
//!          │ start_operation(timeout)                     │
//!          ▼                                              │
//!   ┌─────────────────────────────────────────────────────┼─────────────────────┐
//!   │                      CompletionTracker              │                     │
//!   │                                                     │                     │
//!   │   ┌─────────────────────────────────────────────────┼─────────────────┐   │
//!   │   │  operations: Arc<RwLock<HashMap<OpId, Status>>> │                 │   │
//!   │   │                                                 │                 │   │
//!   │   │  ┌─────┬────────────────────────────────────────┼──────────────┐  │   │
//!   │   │  │ ID  │ OperationStatus                        │              │  │   │
//!   │   │  ├─────┼────────────────────────────────────────┼──────────────┤  │   │
//!   │   │  │  1  │ Pending { notifier: tx1, timeout }     │◄─────────────┼──┼───┤
//!   │   │  │  2  │ Completed { result, timing }           │              │  │   │
//!   │   │  │  3  │ Cancelled { reason, timing }           │              │  │   │
//!   │   │  └─────┴────────────────────────────────────────┼──────────────┘  │   │
//!   │   └─────────────────────────────────────────────────┼─────────────────┘   │
//!   │                                                     │                     │
//!   │   ┌─────────────────────────────────────────────────┼─────────────────┐   │
//!   │   │  Broadcast Channel (event_sender)               │                 │   │
//!   │   │                                                 │                 │   │
//!   │   │  For multi-listener notifications               │                 │   │
//!   │   │  (monitoring, wait_for_operations, etc.)        │                 │   │
//!   │   └─────────────────────────────────────────────────┼─────────────────┘   │
//!   │                                                     │                     │
//!   │   ┌─────────────────────────────────────────────────┼─────────────────┐   │
//!   │   │  Background Cleanup Task                        │                 │   │
//!   │   │                                                 │                 │   │
//!   │   │  • Check timeouts periodically                  │                 │   │
//!   │   │  • Purge old completed/cancelled operations     │                 │   │
//!   │   └─────────────────────────────────────────────────┼─────────────────┘   │
//!   └─────────────────────────────────────────────────────┼─────────────────────┘
//!          │                                              │
//!          │ Returns (OpId, oneshot::Receiver)            │ complete_operation_with_result()
//!          ▼                                              │
//!   ┌─────────────────────────────────────────────────────┴─────────────────────┐
//!   │  Caller awaits receiver.await → OperationResult::Success/Error            │
//!   └───────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Operation Lifecycle
//!
//! ```text
//!   start_operation()
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │                         Pending                                        │
//!   │                                                                        │
//!   │  • Operation is registered in HashMap                                  │
//!   │  • Oneshot sender stored for direct notification                       │
//!   │  • Optional timeout configured                                         │
//!   │  • Metrics: record_operation_start()                                   │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ├─────────────────┬─────────────────┬─────────────────┐
//!        │                 │                 │                 │
//!        │ complete_op()   │ fail_op()       │ cancel_op()     │ timeout
//!        ▼                 ▼                 ▼                 ▼
//!   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
//!   │  Completed  │  │  Completed  │  │  Cancelled  │  │  Completed  │
//!   │  (Success)  │  │  (Error)    │  │             │  │  (Timeout)  │
//!   │             │  │             │  │             │  │             │
//!   │ notifier.   │  │ notifier.   │  │ (notifier   │  │ notifier.   │
//!   │ send(data)  │  │ send(err)   │  │  dropped)   │  │ send(err)   │
//!   └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘
//!        │                 │                 │                 │
//!        └─────────────────┴─────────────────┴─────────────────┘
//!                                   │
//!                                   ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  Background Cleanup                                                    │
//!   │                                                                        │
//!   │  After completed_operation_ttl (default 5 min):                        │
//!   │  • Remove from HashMap to free memory                                  │
//!   └────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Notification System
//!
//! ```text
//!   Two notification mechanisms:
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   1. DIRECT (oneshot channel) - Primary mechanism
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │                                                                        │
//!   │  start_operation() → (op_id, oneshot::Receiver)                        │
//!   │                                                                        │
//!   │  • Single consumer only                                                │
//!   │  • Efficient: no cloning, no broadcast overhead                        │
//!   │  • Typical usage: caller awaits specific operation result              │
//!   │                                                                        │
//!   │  Caller:  receiver.await → OperationResult                             │
//!   └────────────────────────────────────────────────────────────────────────┘
//!
//!   2. BROADCAST (broadcast channel) - Secondary mechanism
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │                                                                        │
//!   │  subscribe_to_events() → broadcast::Receiver<OperationEvent>           │
//!   │                                                                        │
//!   │  • Multiple consumers                                                  │
//!   │  • Used by: wait_for_operations(), monitoring tools                    │
//!   │  • OperationEvent { operation_id, result }                             │
//!   │                                                                        │
//!   │  Monitors:  event_rx.recv() → OperationEvent                           │
//!   └────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Components
//!
//! | Component                   | Description                                   |
//! |-----------------------------|-----------------------------------------------|
//! | `CompletionTracker`         | Main tracker managing operation lifecycle     |
//! | `CompletionTrackerConfig`   | Configuration (TTL, cleanup interval, etc.)   |
//! | `OperationEvent`            | Broadcast event with op_id and result         |
//! | `OperationStats`            | Snapshot of pending/completed/cancelled counts|
//!
//! ## Configuration Options
//!
//! | Field                      | Default       | Description                        |
//! |----------------------------|---------------|------------------------------------|
//! | `completed_operation_ttl`  | 5 minutes     | How long to keep completed ops     |
//! | `cleanup_interval`         | 1 minute      | How often to run cleanup task      |
//! | `max_tracked_operations`   | 10,000        | Capacity before forced cleanup     |
//! | `default_timeout`          | 30 seconds    | Default timeout for new operations |
//!
//! ## Core Operations
//!
//! | Method                           | Description                             |
//! |----------------------------------|-----------------------------------------|
//! | `new()`                          | Create with default configuration       |
//! | `with_config()`                  | Create with custom configuration        |
//! | `start_operation()`              | Register new op, get (id, receiver)     |
//! | `complete_operation()`           | Complete with success data              |
//! | `complete_operation_with_result()`| Complete with specific result          |
//! | `fail_operation()`               | Complete with error message             |
//! | `cancel_operation()`             | Cancel with reason                      |
//! | `wait_for_operation()`           | Await single op with timeout            |
//! | `wait_for_operations()`          | Await multiple ops with timeout         |
//!
//! ## Status & Metrics Methods
//!
//! | Method                       | Description                                |
//! |------------------------------|--------------------------------------------|
//! | `get_operation_status()`     | Check if operation is completed            |
//! | `is_operation_pending()`     | Check if operation is still pending        |
//! | `tracked_operations_count()` | Get total tracked operations               |
//! | `pending_operations_count()` | Get pending operations only                |
//! | `get_operation_stats()`      | Get detailed stats snapshot                |
//! | `metrics()`                  | Get `Arc<IOMetrics>` for I/O statistics    |
//!
//! ## Administrative Methods
//!
//! | Method                 | Description                                     |
//! |------------------------|-------------------------------------------------|
//! | `cancel_all_pending()` | Cancel all pending ops with reason              |
//! | `check_timeouts()`     | Manually check and fail timed-out ops           |
//! | `subscribe_to_events()`| Get broadcast receiver for monitoring           |
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::storage::disk::async_disk::io::completion::{
//!     CompletionTracker, CompletionTrackerConfig, OperationEvent,
//! };
//! use crate::storage::disk::async_disk::io::operation_status::OperationResult;
//! use std::time::Duration;
//! use std::sync::Arc;
//!
//! // Create with custom configuration
//! let config = CompletionTrackerConfig {
//!     completed_operation_ttl: Duration::from_secs(600),
//!     cleanup_interval: Duration::from_secs(30),
//!     max_tracked_operations: 50_000,
//!     default_timeout: Some(Duration::from_secs(60)),
//! };
//! let tracker = Arc::new(CompletionTracker::with_config(config));
//!
//! // Start an operation
//! let (op_id, receiver) = tracker.start_operation(None).await;
//!
//! // In worker: complete the operation
//! let tracker_clone = Arc::clone(&tracker);
//! tokio::spawn(async move {
//!     // ... perform I/O ...
//!     let data = vec![1, 2, 3, 4];
//!     tracker_clone.complete_operation(op_id, data).await.unwrap();
//! });
//!
//! // In caller: await the result
//! match receiver.await {
//!     Ok(OperationResult::Success(data)) => {
//!         println!("Got {} bytes", data.len());
//!     }
//!     Ok(OperationResult::Error(msg)) => {
//!         eprintln!("I/O error: {}", msg);
//!     }
//!     Err(_) => {
//!         eprintln!("Operation cancelled");
//!     }
//! }
//!
//! // Wait for multiple operations
//! let (op1, _) = tracker.start_operation(None).await;
//! let (op2, _) = tracker.start_operation(None).await;
//!
//! // ... workers complete ops ...
//!
//! let results = tracker.wait_for_operations(
//!     &[op1, op2],
//!     Duration::from_secs(5),
//! ).await?;
//!
//! for (id, result) in results {
//!     println!("Op {}: {:?}", id, result);
//! }
//!
//! // Monitor all completions
//! let mut event_rx = tracker.subscribe_to_events();
//! while let Ok(event) = event_rx.recv().await {
//!     println!("Op {} completed: {:?}", event.operation_id, event.result);
//! }
//! ```
//!
//! ## Background Cleanup Task
//!
//! ```text
//!   Runs every cleanup_interval (default 1 minute)
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │ Phase 1: TIMEOUT DETECTION                                             │
//!   │                                                                        │
//!   │   For each Pending operation:                                          │
//!   │     if started_at + timeout < now:                                     │
//!   │       • Transition to Completed(Error("timed out"))                    │
//!   │       • Send broadcast event                                           │
//!   │       • Record timeout metric                                          │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │ Phase 2: OLD OPERATION CLEANUP                                         │
//!   │                                                                        │
//!   │   For each Completed/Cancelled operation:                              │
//!   │     if completed_at/cancelled_at + TTL < now:                          │
//!   │       • Remove from HashMap                                            │
//!   │                                                                        │
//!   │   Keeps HashMap size bounded                                           │
//!   │   Prevents memory leaks from accumulating old operations               │
//!   └────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## OperationStats Fields
//!
//! | Field            | Description                                        |
//! |------------------|----------------------------------------------------|
//! | `total_count`    | Total operations in HashMap                        |
//! | `pending_count`  | Operations still waiting                           |
//! | `completed_count`| Successfully completed or failed operations        |
//! | `cancelled_count`| Cancelled operations                               |
//! | `timed_out_count`| Pending operations that exceeded timeout           |
//!
//! ## Thread Safety
//!
//! - `operations`: `Arc<RwLock<HashMap<...>>>` - concurrent read/write access
//! - `metrics`: `Arc<IOMetrics>` - atomic counters for stats
//! - `next_id`: `AtomicU64` - lock-free ID generation
//! - `event_sender`: `broadcast::Sender` - multi-producer multi-consumer
//! - `cleanup_task`: `Mutex<Option<JoinHandle>>` - single background task
//! - All public methods take `&self`, safe for concurrent calls
//!
//! ## Drop Behavior
//!
//! When `CompletionTracker` is dropped:
//! - Background cleanup task is aborted
//! - Pending operations' receivers will receive `RecvError` (sender dropped)

use super::metrics::IOMetrics;
use super::operation_status::{OperationId, OperationResult, OperationStatus};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock, broadcast, oneshot};
use tokio::time::sleep;

/// Notification event for operation completion
#[derive(Debug, Clone)]
pub struct OperationEvent {
    pub operation_id: OperationId,
    pub result: OperationResult,
}

/// Completion tracker for I/O operations with production-ready features
#[derive(Debug)]
pub struct CompletionTracker {
    /// Map of operation ID to operation status
    operations: Arc<RwLock<HashMap<OperationId, OperationStatus>>>,

    /// Metrics collector for observability
    metrics: Arc<IOMetrics>,

    /// Operation ID counter
    next_id: AtomicU64,

    /// Background task handle for cleanup
    cleanup_task: Mutex<Option<tokio::task::JoinHandle<()>>>,

    /// Configuration
    config: CompletionTrackerConfig,

    /// Broadcast channel for operation completion notifications
    event_sender: broadcast::Sender<OperationEvent>,
}

/// Configuration for the completion tracker
#[derive(Debug, Clone)]
pub struct CompletionTrackerConfig {
    /// How long to keep completed operations for queries
    completed_operation_ttl: Duration,

    /// How often to run cleanup of old operations
    cleanup_interval: Duration,

    /// Maximum number of operations to track simultaneously
    max_tracked_operations: usize,

    /// Default timeout for operations
    default_timeout: Option<Duration>,
}

impl Default for CompletionTrackerConfig {
    fn default() -> Self {
        Self {
            completed_operation_ttl: Duration::from_secs(300), // 5 minutes
            cleanup_interval: Duration::from_secs(60),         // 1 minute
            max_tracked_operations: 10_000,
            default_timeout: Some(Duration::from_secs(30)), // 30 seconds
        }
    }
}

impl CompletionTracker {
    /// Creates a new completion tracker with default configuration
    pub fn new() -> Self {
        Self::with_config(CompletionTrackerConfig::default())
    }

    /// Creates a new completion tracker with custom configuration
    pub fn with_config(config: CompletionTrackerConfig) -> Self {
        // Create broadcast channel for operation events
        let (event_sender, _) = broadcast::channel(1024);

        let tracker = Self {
            operations: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(IOMetrics::default()),
            next_id: AtomicU64::new(1),
            cleanup_task: Mutex::new(None),
            config,
            event_sender,
        };

        // Start background cleanup task
        tracker.start_cleanup_task();

        tracker
    }

    /// Starts a new I/O operation and returns its ID and a receiver for the result
    pub async fn start_operation(
        &self,
        timeout: Option<Duration>,
    ) -> (OperationId, oneshot::Receiver<OperationResult>) {
        let op_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let operation_timeout = timeout.or(self.config.default_timeout);

        let (status, receiver) = OperationStatus::new_pending(operation_timeout);

        // Track the operation
        {
            let mut operations = self.operations.write().await;

            // Check if we're at capacity
            if operations.len() >= self.config.max_tracked_operations {
                // Remove oldest completed/cancelled operations to make space
                self.cleanup_old_operations(&mut operations).await;
            }

            operations.insert(op_id, status);
        }

        // Record metrics
        self.metrics.record_operation_start();

        (op_id, receiver)
    }

    /// Completes an operation with a successful result
    pub async fn complete_operation(
        &self,
        op_id: OperationId,
        result: Vec<u8>,
    ) -> Result<(), String> {
        self.complete_operation_with_result(op_id, OperationResult::Success(result))
            .await
    }

    /// Completes an operation with a specific result (success or error)
    pub async fn complete_operation_with_result(
        &self,
        op_id: OperationId,
        result: OperationResult,
    ) -> Result<(), String> {
        let (duration, data_size, is_success) = {
            let mut operations = self.operations.write().await;

            if let Some(status) = operations.get_mut(&op_id) {
                if !status.is_pending() {
                    return Err(format!("Operation {} is not pending", op_id));
                }

                let duration = status.elapsed();
                let data_size = match &result {
                    OperationResult::Success(data) => data.len() as u64,
                    OperationResult::Error(_) => 0,
                };
                let is_success = matches!(result, OperationResult::Success(_));

                // Complete the operation in-place (this sends notification via oneshot)
                let old_status = std::mem::replace(
                    status,
                    OperationStatus::Cancelled {
                        started_at: Instant::now(),
                        cancelled_at: Instant::now(),
                        reason: "temporary".to_string(),
                    },
                );

                *status = old_status.complete(result.clone());

                (duration, data_size, is_success)
            } else {
                return Err(format!("Operation {} not found", op_id));
            }
        };

        // Send broadcast notification for waiters by ID
        let event = OperationEvent {
            operation_id: op_id,
            result: result.clone(),
        };

        // Ignore broadcast errors (no receivers is OK)
        let _ = self.event_sender.send(event);

        // Record metrics
        if is_success {
            self.metrics
                .record_operation_complete(duration, data_size, true)
                .await;
        } else {
            self.metrics.record_operation_failed();
        }

        Ok(())
    }

    /// Fails an operation with an error
    pub async fn fail_operation(&self, op_id: OperationId, error: String) -> Result<(), String> {
        self.complete_operation_with_result(op_id, OperationResult::Error(error))
            .await
    }

    /// Cancels an operation
    pub async fn cancel_operation(&self, op_id: OperationId, reason: String) -> Result<(), String> {
        {
            let mut operations = self.operations.write().await;

            if let Some(status) = operations.get_mut(&op_id) {
                if !status.is_pending() {
                    return Err(format!("Operation {} is not pending", op_id));
                }

                // Cancel the operation in-place
                let old_status = std::mem::replace(
                    status,
                    OperationStatus::Cancelled {
                        started_at: Instant::now(),
                        cancelled_at: Instant::now(),
                        reason: "temporary".to_string(),
                    },
                );

                *status = old_status.cancel(reason.clone());
            } else {
                return Err(format!("Operation {} not found", op_id));
            }
        }

        // Send broadcast notification for waiters by ID
        let event = OperationEvent {
            operation_id: op_id,
            result: OperationResult::Error(format!("Operation cancelled: {}", reason)),
        };

        // Ignore broadcast errors (no receivers is OK)
        let _ = self.event_sender.send(event);

        // Record metrics
        self.metrics.record_operation_cancelled();

        Ok(())
    }

    /// Gets the status of an operation
    pub async fn get_operation_status(&self, op_id: OperationId) -> Option<bool> {
        let operations = self.operations.read().await;
        operations.get(&op_id).map(|status| status.is_completed())
    }

    /// Returns true if the operation exists and is still pending.
    pub async fn is_operation_pending(&self, op_id: OperationId) -> bool {
        let operations = self.operations.read().await;
        matches!(
            operations.get(&op_id),
            Some(OperationStatus::Pending { .. })
        )
    }

    /// Waits for an operation to complete with a timeout
    pub async fn wait_for_operation(
        &self,
        op_id: OperationId,
        wait_timeout: Duration,
    ) -> Result<OperationResult, String> {
        // First check if the operation is already completed
        {
            let operations = self.operations.read().await;
            match operations.get(&op_id) {
                Some(OperationStatus::Completed { result, .. }) => {
                    return Ok(result.clone());
                },
                Some(OperationStatus::Cancelled { reason, .. }) => {
                    return Err(format!("Operation was cancelled: {}", reason));
                },
                Some(OperationStatus::Pending { .. }) => {
                    // Continue to wait
                },
                None => {
                    return Err(format!("Operation {} not found", op_id));
                },
            }
        }

        // Subscribe to operation completion events
        let mut event_receiver = self.event_sender.subscribe();

        // Use tokio::select to wait for either the operation completion or timeout
        tokio::select! {
            // Wait for operation completion event
            result = async {
                while let Ok(event) = event_receiver.recv().await {
                    if event.operation_id == op_id {
                        return event.result;
                    }
                }
                // If the broadcast channel is closed, check final status
                let operations = self.operations.read().await;
                match operations.get(&op_id) {
                    Some(OperationStatus::Completed { result, .. }) => result.clone(),
                    Some(OperationStatus::Cancelled { reason, .. }) => {
                        OperationResult::Error(format!("Operation was cancelled: {}", reason))
                    }
                    _ => OperationResult::Error("Operation completed without notification".to_string()),
                }
            } => {
                Ok(result)
            }

            // Timeout
            _ = sleep(wait_timeout) => {
                Err("Timeout waiting for operation".to_string())
            }
        }
    }

    /// Gets the current metrics
    pub fn metrics(&self) -> Arc<IOMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Creates a receiver for operation events (useful for monitoring multiple operations)
    pub fn subscribe_to_events(&self) -> broadcast::Receiver<OperationEvent> {
        self.event_sender.subscribe()
    }

    /// Gets the number of currently tracked operations
    pub async fn tracked_operations_count(&self) -> usize {
        let operations = self.operations.read().await;
        operations.len()
    }

    /// Gets the number of pending operations
    pub async fn pending_operations_count(&self) -> usize {
        let operations = self.operations.read().await;
        operations
            .values()
            .filter(|status| status.is_pending())
            .count()
    }

    /// Cancels all pending operations
    pub async fn cancel_all_pending(&self, reason: String) -> usize {
        let pending_ids = {
            let operations = self.operations.read().await;
            operations
                .iter()
                .filter_map(
                    |(id, status)| {
                        if status.is_pending() { Some(*id) } else { None }
                    },
                )
                .collect::<Vec<_>>()
        };

        let mut cancelled_count = 0;

        for op_id in pending_ids {
            if self.cancel_operation(op_id, reason.clone()).await.is_ok() {
                cancelled_count += 1;
            }
        }

        cancelled_count
    }

    /// Checks for timed out operations and marks them as failed
    pub async fn check_timeouts(&self) -> usize {
        let timed_out_ids = {
            let operations = self.operations.read().await;
            operations
                .iter()
                .filter_map(|(id, status)| {
                    if status.is_timed_out() {
                        Some(*id)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };

        let mut timed_out_count = 0;

        for op_id in timed_out_ids {
            if self
                .fail_operation(op_id, "Operation timed out".to_string())
                .await
                .is_ok()
            {
                timed_out_count += 1;

                // Record timeout-specific metric
                self.metrics.record_operation_timed_out();
            }
        }

        timed_out_count
    }

    /// Starts the background cleanup task
    fn start_cleanup_task(&self) {
        let operations = Arc::clone(&self.operations);
        let config = self.config.clone();
        let event_sender = self.event_sender.clone();
        let metrics = Arc::clone(&self.metrics);

        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.cleanup_interval);

            loop {
                interval.tick().await;

                // Check for timeouts first and handle them
                let timed_out_count = {
                    let timed_out_ids = {
                        let ops = operations.read().await;
                        ops.iter()
                            .filter_map(|(id, status)| {
                                if status.is_timed_out() {
                                    Some(*id)
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<_>>()
                    };

                    let mut count = 0;
                    for op_id in timed_out_ids {
                        let mut ops = operations.write().await;
                        if let Some(status) = ops.get_mut(&op_id)
                            && status.is_pending()
                            && status.is_timed_out()
                        {
                            let old_status = std::mem::replace(
                                status,
                                OperationStatus::Cancelled {
                                    started_at: Instant::now(),
                                    cancelled_at: Instant::now(),
                                    reason: "temporary".to_string(),
                                },
                            );

                            *status = old_status.complete(OperationResult::Error(
                                "Operation timed out".to_string(),
                            ));
                            count += 1;

                            // Send notification
                            let event = OperationEvent {
                                operation_id: op_id,
                                result: OperationResult::Error("Operation timed out".to_string()),
                            };
                            let _ = event_sender.send(event);

                            // Record metrics
                            metrics.record_operation_timed_out();
                        }
                    }
                    count
                };

                if timed_out_count > 0 {
                    println!("DEBUG: Timed out {} operations", timed_out_count);
                }

                // Then clean up old operations
                let mut ops = operations.write().await;
                let before_count = ops.len();

                // Remove old completed/cancelled operations
                let cutoff_time = Instant::now() - config.completed_operation_ttl;
                ops.retain(|_, status| {
                    match status {
                        OperationStatus::Completed { completed_at, .. } => {
                            *completed_at > cutoff_time
                        },
                        OperationStatus::Cancelled { cancelled_at, .. } => {
                            *cancelled_at > cutoff_time
                        },
                        OperationStatus::Pending { .. } => true, // Keep pending operations
                    }
                });

                let removed_count = before_count - ops.len();
                if removed_count > 0 {
                    // Log cleanup - using println for now since tracing is not available
                    println!("DEBUG: Cleaned up {} old operations", removed_count);
                }
            }
        });

        // Store the task handle
        if let Ok(mut cleanup_task) = self.cleanup_task.try_lock() {
            *cleanup_task = Some(task);
        }
    }

    /// Helper method to cleanup old operations when at capacity
    async fn cleanup_old_operations(&self, operations: &mut HashMap<OperationId, OperationStatus>) {
        let cutoff_time = Instant::now() - self.config.completed_operation_ttl;

        operations.retain(|_, status| {
            match status {
                OperationStatus::Completed { completed_at, .. } => *completed_at > cutoff_time,
                OperationStatus::Cancelled { cancelled_at, .. } => *cancelled_at > cutoff_time,
                OperationStatus::Pending { .. } => true, // Always keep pending operations
            }
        });
    }

    /// Waits for multiple operations to complete
    pub async fn wait_for_operations(
        &self,
        op_ids: &[OperationId],
        wait_timeout: Duration,
    ) -> Result<Vec<(OperationId, OperationResult)>, String> {
        if op_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        let mut remaining_ids: std::collections::HashSet<OperationId> =
            op_ids.iter().copied().collect();

        // Check for already completed operations
        {
            let operations = self.operations.read().await;
            for &op_id in op_ids {
                match operations.get(&op_id) {
                    Some(OperationStatus::Completed { result, .. }) => {
                        results.push((op_id, result.clone()));
                        remaining_ids.remove(&op_id);
                    },
                    Some(OperationStatus::Cancelled { reason, .. }) => {
                        results.push((
                            op_id,
                            OperationResult::Error(format!("Operation was cancelled: {}", reason)),
                        ));
                        remaining_ids.remove(&op_id);
                    },
                    Some(OperationStatus::Pending { .. }) => {
                        // Will wait for this one
                    },
                    None => {
                        return Err(format!("Operation {} not found", op_id));
                    },
                }
            }
        }

        if remaining_ids.is_empty() {
            return Ok(results);
        }

        // Subscribe to events for remaining operations
        let mut event_receiver = self.event_sender.subscribe();

        tokio::select! {
            // Wait for all remaining operations
            _ = async {
                while !remaining_ids.is_empty() {
                    if let Ok(event) = event_receiver.recv().await {
                        if remaining_ids.contains(&event.operation_id) {
                            results.push((event.operation_id, event.result));
                            remaining_ids.remove(&event.operation_id);
                        }
                    } else {
                        // Channel closed, break
                        break;
                    }
                }
            } => {}

            // Timeout
            _ = sleep(wait_timeout) => {
                return Err(format!("Timeout waiting for {} operations", remaining_ids.len()));
            }
        }

        Ok(results)
    }

    /// Gets detailed statistics about operations
    pub async fn get_operation_stats(&self) -> OperationStats {
        let operations = self.operations.read().await;
        let mut stats = OperationStats::default();

        for status in operations.values() {
            match status {
                OperationStatus::Pending { .. } => {
                    stats.pending_count += 1;
                    if status.is_timed_out() {
                        stats.timed_out_count += 1;
                    }
                },
                OperationStatus::Completed { .. } => {
                    stats.completed_count += 1;
                },
                OperationStatus::Cancelled { .. } => {
                    stats.cancelled_count += 1;
                },
            }
        }

        stats.total_count = operations.len();
        stats
    }
}

/// Detailed statistics about tracked operations
#[derive(Debug, Default, Clone)]
pub struct OperationStats {
    pub total_count: usize,
    pub pending_count: usize,
    pub completed_count: usize,
    pub cancelled_count: usize,
    pub timed_out_count: usize,
}

impl Default for CompletionTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for CompletionTracker {
    fn drop(&mut self) {
        // Cancel the cleanup task
        if let Ok(mut cleanup_task) = self.cleanup_task.try_lock()
            && let Some(task) = cleanup_task.take()
        {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;
    use tokio::time::{sleep, timeout};

    #[tokio::test]
    async fn test_basic_operation_lifecycle() {
        let tracker = CompletionTracker::new();

        // Start an operation
        let (op_id, receiver) = tracker.start_operation(Some(Duration::from_secs(10))).await;
        assert!(op_id > 0);

        // Check initial state
        assert_eq!(tracker.pending_operations_count().await, 1);
        assert_eq!(tracker.tracked_operations_count().await, 1);

        // Complete the operation
        let test_data = vec![1, 2, 3, 4, 5];
        tracker
            .complete_operation(op_id, test_data.clone())
            .await
            .unwrap();

        // Check final state
        assert_eq!(tracker.pending_operations_count().await, 0);
        assert_eq!(tracker.tracked_operations_count().await, 1);

        // Verify the result via receiver
        let result = receiver.await.unwrap();
        match result {
            OperationResult::Success(data) => assert_eq!(data, test_data),
            OperationResult::Error(_) => panic!("Expected success result"),
        }
    }

    #[tokio::test]
    async fn test_operation_failure() {
        let tracker = CompletionTracker::new();

        let (op_id, receiver) = tracker.start_operation(None).await;

        // Fail the operation
        let error_msg = "Test error".to_string();
        tracker
            .fail_operation(op_id, error_msg.clone())
            .await
            .unwrap();

        // Verify the result
        let result = receiver.await.unwrap();
        match result {
            OperationResult::Error(err) => assert_eq!(err, error_msg),
            OperationResult::Success(_) => panic!("Expected error result"),
        }
    }

    #[tokio::test]
    async fn test_operation_cancellation() {
        let tracker = CompletionTracker::new();

        let (op_id, _receiver) = tracker.start_operation(None).await;

        // Cancel the operation
        let cancel_reason = "Test cancellation".to_string();
        tracker
            .cancel_operation(op_id, cancel_reason.clone())
            .await
            .unwrap();

        // Check state
        assert_eq!(tracker.pending_operations_count().await, 0);

        let stats = tracker.get_operation_stats().await;
        assert_eq!(stats.cancelled_count, 1);
    }

    #[tokio::test]
    async fn test_wait_for_operation() {
        let tracker = Arc::new(CompletionTracker::new());

        let (op_id, _receiver) = tracker.start_operation(None).await;

        // Spawn a task to complete the operation after a delay
        let tracker_clone = Arc::clone(&tracker);
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;
            let _ = tracker_clone.complete_operation(op_id, vec![42]).await;
        });

        // Wait for the operation
        let result = tracker
            .wait_for_operation(op_id, Duration::from_secs(1))
            .await
            .unwrap();
        match result {
            OperationResult::Success(data) => assert_eq!(data, vec![42]),
            OperationResult::Error(_) => panic!("Expected success result"),
        }
    }

    #[tokio::test]
    async fn test_wait_for_operation_timeout() {
        let tracker = CompletionTracker::new();

        let (op_id, _receiver) = tracker.start_operation(None).await;

        // Wait with a very short timeout
        let result = tracker
            .wait_for_operation(op_id, Duration::from_millis(10))
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Timeout"));
    }

    #[tokio::test]
    async fn test_wait_for_multiple_operations() {
        let tracker = Arc::new(CompletionTracker::new());

        // Start multiple operations
        let mut op_ids = Vec::new();
        for i in 0..5 {
            let (op_id, _receiver) = tracker.start_operation(None).await;
            op_ids.push(op_id);

            // Spawn tasks to complete operations with different delays
            let tracker_clone = Arc::clone(&tracker);
            tokio::spawn(async move {
                sleep(Duration::from_millis(20 * (i + 1))).await;
                let data = vec![i as u8; (i + 1) as usize];
                let _ = tracker_clone.complete_operation(op_id, data).await;
            });
        }

        // Wait for all operations
        let results = tracker
            .wait_for_operations(&op_ids, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(results.len(), 5);

        // Check all operations completed successfully
        for (i, (op_id, result)) in results.iter().enumerate() {
            assert!(op_ids.contains(op_id));
            match result {
                OperationResult::Success(data) => {
                    assert_eq!(data.len(), i + 1);
                    assert_eq!(data[0], i as u8);
                },
                OperationResult::Error(_) => panic!("Expected success result"),
            }
        }
    }

    #[tokio::test]
    async fn test_operation_already_completed() {
        let tracker = CompletionTracker::new();

        let (op_id, _receiver) = tracker.start_operation(None).await;

        // Complete the operation
        tracker.complete_operation(op_id, vec![1]).await.unwrap();

        // Try to complete again - should fail
        let result = tracker.complete_operation(op_id, vec![2]).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not pending"));
    }

    #[tokio::test]
    async fn test_operation_not_found() {
        let tracker = CompletionTracker::new();

        // Try to complete non-existent operation
        let result = tracker.complete_operation(999, vec![1]).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));

        // Try to cancel non-existent operation
        let result = tracker.cancel_operation(999, "test".to_string()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[tokio::test]
    async fn test_event_subscription() {
        let tracker = Arc::new(CompletionTracker::new());
        let mut event_receiver = tracker.subscribe_to_events();

        let (op_id, _receiver) = tracker.start_operation(None).await;

        // Complete the operation
        let tracker_clone = Arc::clone(&tracker);
        tokio::spawn(async move {
            sleep(Duration::from_millis(10)).await;
            let _ = tracker_clone.complete_operation(op_id, vec![42]).await;
        });

        // Wait for the event
        let event = timeout(Duration::from_secs(1), event_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.operation_id, op_id);
        match event.result {
            OperationResult::Success(data) => assert_eq!(data, vec![42]),
            OperationResult::Error(_) => panic!("Expected success result"),
        }
    }

    #[tokio::test]
    async fn test_cancel_all_pending() {
        let tracker = CompletionTracker::new();

        // Start multiple operations
        let mut op_ids = Vec::new();
        for _ in 0..5 {
            let (op_id, _receiver) = tracker.start_operation(None).await;
            op_ids.push(op_id);
        }

        assert_eq!(tracker.pending_operations_count().await, 5);

        // Cancel all pending operations
        let cancelled_count = tracker
            .cancel_all_pending("Test cancellation".to_string())
            .await;
        assert_eq!(cancelled_count, 5);
        assert_eq!(tracker.pending_operations_count().await, 0);

        let stats = tracker.get_operation_stats().await;
        assert_eq!(stats.cancelled_count, 5);
    }

    #[tokio::test]
    async fn test_operation_statistics() {
        let tracker = CompletionTracker::new();

        // Create operations in different states
        let (op1, _) = tracker.start_operation(None).await;
        let (op2, _) = tracker.start_operation(None).await;
        let (op3, _) = tracker.start_operation(None).await;

        // Complete one successfully
        tracker.complete_operation(op1, vec![1]).await.unwrap();

        // Fail one
        tracker
            .fail_operation(op2, "Test error".to_string())
            .await
            .unwrap();

        // Cancel one
        tracker
            .cancel_operation(op3, "Test cancellation".to_string())
            .await
            .unwrap();

        let stats = tracker.get_operation_stats().await;
        assert_eq!(stats.total_count, 3);
        assert_eq!(stats.pending_count, 0);
        assert_eq!(stats.completed_count, 2); // Both successful and failed are "completed"
        assert_eq!(stats.cancelled_count, 1);
    }

    #[tokio::test]
    async fn test_operation_timeout_detection() {
        let config = CompletionTrackerConfig {
            completed_operation_ttl: Duration::from_secs(300),
            cleanup_interval: Duration::from_secs(10), // Long interval to prevent background cleanup
            max_tracked_operations: 1000,
            default_timeout: Some(Duration::from_millis(50)), // Very short timeout
        };

        let tracker = CompletionTracker::with_config(config);

        let (_op_id, _receiver) = tracker.start_operation(None).await;

        // Wait for timeout to occur
        sleep(Duration::from_millis(100)).await;

        // Manually check for timeouts (since background cleanup is disabled)
        let timed_out = tracker.check_timeouts().await;
        assert!(timed_out > 0);

        // Verify the operation is no longer pending
        assert_eq!(tracker.pending_operations_count().await, 0);
    }

    #[tokio::test]
    async fn test_custom_configuration() {
        let config = CompletionTrackerConfig {
            completed_operation_ttl: Duration::from_secs(600),
            cleanup_interval: Duration::from_secs(30),
            max_tracked_operations: 50_000,
            default_timeout: Some(Duration::from_secs(60)),
        };

        let tracker = CompletionTracker::with_config(config);

        // Test that operations work with custom config
        let (op_id, receiver) = tracker.start_operation(None).await;
        tracker
            .complete_operation(op_id, vec![1, 2, 3])
            .await
            .unwrap();

        let result = receiver.await.unwrap();
        match result {
            OperationResult::Success(data) => assert_eq!(data, vec![1, 2, 3]),
            OperationResult::Error(_) => panic!("Expected success result"),
        }
    }

    #[tokio::test]
    async fn test_high_concurrency() {
        let tracker = Arc::new(CompletionTracker::new());
        let num_operations: usize = 100;

        let mut handles = Vec::new();
        let mut expected_results = HashSet::new();

        // Start many concurrent operations
        for i in 0..num_operations {
            let tracker_clone = Arc::clone(&tracker);
            expected_results.insert(i as u8);

            let handle = tokio::spawn(async move {
                let (op_id, receiver) = tracker_clone.start_operation(None).await;

                // Complete after a random delay
                sleep(Duration::from_millis((i % 10) as u64)).await;
                let data = vec![i as u8];
                tracker_clone
                    .complete_operation(op_id, data.clone())
                    .await
                    .unwrap();

                // Return the result
                let result = receiver.await.unwrap();
                match result {
                    OperationResult::Success(data) => data[0],
                    OperationResult::Error(_) => panic!("Expected success result"),
                }
            });

            handles.push(handle);
        }

        // Wait for all operations to complete
        let mut actual_results = HashSet::new();
        for handle in handles {
            let result = handle.await.unwrap();
            actual_results.insert(result);
        }

        // Verify all operations completed successfully
        assert_eq!(actual_results.len(), num_operations);
        assert_eq!(actual_results, expected_results);

        // Check final state
        assert_eq!(tracker.pending_operations_count().await, 0);
        assert_eq!(tracker.tracked_operations_count().await, num_operations);
    }

    #[tokio::test]
    async fn test_metrics_integration() {
        let tracker = CompletionTracker::new();
        let metrics = tracker.metrics();

        let initial_started = metrics.total_operations();

        // Start and complete some operations
        let (op1, _) = tracker.start_operation(None).await;
        let (op2, _) = tracker.start_operation(None).await;

        tracker.complete_operation(op1, vec![1]).await.unwrap();
        tracker
            .fail_operation(op2, "Test error".to_string())
            .await
            .unwrap();

        // Check metrics were updated
        let final_started = metrics.total_operations();
        assert_eq!(final_started, initial_started + 2);
    }

    #[tokio::test]
    async fn test_operation_capacity_limit() {
        let config = CompletionTrackerConfig {
            completed_operation_ttl: Duration::from_millis(50),
            cleanup_interval: Duration::from_millis(10),
            max_tracked_operations: 5, // Very small limit
            default_timeout: None,
        };

        let tracker = CompletionTracker::with_config(config);

        // Fill up to capacity
        let mut operations = Vec::new();
        for _ in 0..5 {
            let (op_id, _) = tracker.start_operation(None).await;
            operations.push(op_id);
        }

        assert_eq!(tracker.tracked_operations_count().await, 5);

        // Complete some operations
        tracker
            .complete_operation(operations[0], vec![1])
            .await
            .unwrap();
        tracker
            .complete_operation(operations[1], vec![2])
            .await
            .unwrap();

        // Wait for cleanup to potentially kick in
        sleep(Duration::from_millis(100)).await;

        // Should still be able to add more operations
        let (new_op_id, _) = tracker.start_operation(None).await;
        tracker
            .complete_operation(new_op_id, vec![3])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_wait_for_already_completed_operations() {
        let tracker = CompletionTracker::new();

        // Start and complete operations immediately
        let (op1, _) = tracker.start_operation(None).await;
        let (op2, _) = tracker.start_operation(None).await;

        tracker.complete_operation(op1, vec![1]).await.unwrap();
        tracker
            .fail_operation(op2, "Test error".to_string())
            .await
            .unwrap();

        // Wait for already completed operations
        let results = tracker
            .wait_for_operations(&[op1, op2], Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(results.len(), 2);

        // Check results
        let results_map: std::collections::HashMap<_, _> = results.into_iter().collect();

        match &results_map[&op1] {
            OperationResult::Success(data) => assert_eq!(data, &vec![1]),
            OperationResult::Error(_) => panic!("Expected success result"),
        }

        match &results_map[&op2] {
            OperationResult::Error(err) => assert_eq!(err, "Test error"),
            OperationResult::Success(_) => panic!("Expected error result"),
        }
    }

    #[tokio::test]
    async fn test_empty_wait_for_operations() {
        let tracker = CompletionTracker::new();

        // Wait for empty list of operations
        let results = tracker
            .wait_for_operations(&[], Duration::from_secs(1))
            .await
            .unwrap();
        assert!(results.is_empty());
    }
}
