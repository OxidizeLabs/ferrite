//! # I/O Operation Status
//!
//! This module defines the state machine and result types for tracking individual I/O operations.
//! It allows callers to query the state of an asynchronous request or await its result via
//! oneshot channels.
//!
//! ## Architecture
//!
//! ```text
//!   Caller                                     Worker
//!   ═══════════════════════════════════════════════════════════════════════════
//!          │
//!          │ start_operation(timeout)
//!          ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │  CompletionTracker creates OperationStatus::Pending                     │
//!   │                                                                         │
//!   │  ┌───────────────────────────────────────────────────────────────────┐  │
//!   │  │  OperationStatus::Pending                                         │  │
//!   │  │                                                                   │  │
//!   │  │  started_at: Instant::now()                                       │  │
//!   │  │  timeout: Option<Duration>                                        │  │
//!   │  │  notifier: oneshot::Sender<OperationResult> ──────────────────────┼──┼──┐
//!   │  └───────────────────────────────────────────────────────────────────┘  │  │
//!   └─────────────────────────────────────────────────────────────────────────┘  │
//!          │                                                                     │
//!          │ Returns (OperationId, Receiver)                                     │
//!          ▼                                                                     │
//!   ┌─────────────────────────────────────────────────────────────────────────┐  │
//!   │  Caller holds oneshot::Receiver<OperationResult>                        │  │
//!   │                                                                         │  │
//!   │  receiver.await  ← blocks until notifier.send() is called               │  │
//!   └─────────────────────────────────────────────────────────────────────────┘  │
//!                                                                                │
//!          ┌─────────────────────────────────────────────────────────────────────┘
//!          │
//!          │ Worker executes I/O, calls complete(result)
//!          ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │  OperationStatus transitions:                                           │
//!   │                                                                         │
//!   │  Pending ──► complete(result) ──► Completed { result, timing }          │
//!   │          └──► cancel(reason)  ──► Cancelled { reason, timing }          │
//!   │                                                                         │
//!   │  On complete(): notifier.send(result) wakes the caller's receiver       │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## State Machine
//!
//! ```text
//!   ┌──────────────────────────────────────────────────────────────────────────┐
//!   │                          OperationStatus                                 │
//!   │                                                                          │
//!   │                        ┌─────────────┐                                   │
//!   │                        │   Pending   │                                   │
//!   │                        │             │                                   │
//!   │                        │ started_at  │                                   │
//!   │                        │ timeout     │                                   │
//!   │                        │ notifier    │                                   │
//!   │                        └──────┬──────┘                                   │
//!   │                               │                                          │
//!   │              ┌────────────────┼────────────────┐                         │
//!   │              │ complete()     │ cancel()       │                         │
//!   │              ▼                ▼                │ (already final)         │
//!   │       ┌─────────────┐  ┌─────────────┐         │                         │
//!   │       │  Completed  │  │  Cancelled  │         │                         │
//!   │       │             │  │             │         │                         │
//!   │       │ started_at  │  │ started_at  │◄────────┘                         │
//!   │       │ completed_at│  │ cancelled_at│                                   │
//!   │       │ result      │  │ reason      │                                   │
//!   │       └─────────────┘  └─────────────┘                                   │
//!   │                                                                          │
//!   │       [Terminal]       [Terminal]                                        │
//!   └──────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Types
//!
//! | Type              | Description                                          |
//! |-------------------|------------------------------------------------------|
//! | `OperationId`     | Type alias for `u64`, unique operation identifier    |
//! | `OperationResult` | Outcome: `Success(Vec<u8>)` or `Error(String)`       |
//! | `OperationStatus` | State machine: Pending → Completed or Cancelled      |
//!
//! ## OperationStatus Variants
//!
//! | Variant     | Fields                                  | Description                    |
//! |-------------|-----------------------------------------|--------------------------------|
//! | `Pending`   | `started_at`, `timeout`, `notifier`     | In queue or executing          |
//! | `Completed` | `started_at`, `completed_at`, `result`  | Finished (success or error)    |
//! | `Cancelled` | `started_at`, `cancelled_at`, `reason`  | Aborted before completion      |
//!
//! ## OperationResult Variants
//!
//! | Variant   | Data           | Description                              |
//! |-----------|----------------|------------------------------------------|
//! | `Success` | `Vec<u8>`      | Operation succeeded, contains result data|
//! | `Error`   | `String`       | Operation failed, contains error message |
//!
//! ## Core Operations
//!
//! | Method          | Description                                        |
//! |-----------------|----------------------------------------------------|
//! | `new_pending()` | Create Pending status with optional timeout        |
//! | `complete()`    | Transition to Completed, notify waiters            |
//! | `cancel()`      | Transition to Cancelled with reason                |
//! | `is_pending()`  | Check if still in Pending state                    |
//! | `is_completed()`| Check if in Completed state                        |
//! | `elapsed()`     | Get duration since operation started               |
//! | `is_timed_out()`| Check if pending and exceeded timeout              |
//!
//! ## Timeout Detection
//!
//! ```text
//!   is_timed_out()
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │ OperationStatus::Pending { timeout: Some(5s), started_at, ... }        │
//!   └────────────────────────────────────────────────────────────────────────┘
//!
//!   Timeline:
//!   ────────────────────────────────────────────────────────────────────────►
//!   │                     │                                │
//!   started_at            │                                now
//!                         │
//!                    started_at + 5s (timeout)
//!
//!   if now > started_at + timeout:
//!       is_timed_out() = true
//!   else:
//!       is_timed_out() = false
//!
//!   Note: Returns false for Completed, Cancelled, or Pending without timeout
//! ```
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::storage::disk::async_disk::io::operation_status::{
//!     OperationStatus, OperationResult,
//! };
//! use std::time::Duration;
//!
//! // Create a pending operation with 5-second timeout
//! let (status, receiver) = OperationStatus::new_pending(Some(Duration::from_secs(5)));
//!
//! assert!(status.is_pending());
//! assert!(!status.is_timed_out()); // Just started
//!
//! // In another task: await the result
//! tokio::spawn(async move {
//!     match receiver.await {
//!         Ok(OperationResult::Success(data)) => {
//!             println!("Read {} bytes", data.len());
//!         }
//!         Ok(OperationResult::Error(msg)) => {
//!             eprintln!("I/O error: {}", msg);
//!         }
//!         Err(_) => {
//!             eprintln!("Operation was cancelled (sender dropped)");
//!         }
//!     }
//! });
//!
//! // Worker completes the operation
//! let completed_status = status.complete(OperationResult::Success(vec![1, 2, 3, 4]));
//! assert!(completed_status.is_completed());
//!
//! // Or cancel it
//! let (status2, _rx) = OperationStatus::new_pending(None);
//! let cancelled = status2.cancel("Shutdown requested".to_string());
//! assert!(!cancelled.is_pending());
//! ```
//!
//! ## Timing Information
//!
//! ```text
//!   Completed Operation Timing
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │ OperationStatus::Completed                                              │
//!   │                                                                         │
//!   │   started_at   ─────────────────────────► completed_at                  │
//!   │        │                                        │                       │
//!   │        └──────── operation_duration ────────────┘                       │
//!   │                                                                         │
//!   │   elapsed() returns: Instant::now() - started_at                        │
//!   │   (useful for latency metrics)                                          │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Thread Safety
//!
//! - `OperationStatus` is not `Sync` (contains `oneshot::Sender`)
//! - `OperationStatus` is `Send` (can be moved between threads)
//! - `OperationResult` is `Clone`, `Send`, and `Sync`
//! - Oneshot channel provides safe single-value communication across tasks
//! - State transitions consume `self`, preventing concurrent mutations

use std::time::{Duration, Instant};

use tokio::sync::oneshot;

/// Unique identifier for I/O operations
pub type OperationId = u64;

/// Result of an I/O operation
#[derive(Debug, Clone)]
pub enum OperationResult {
    Success(Vec<u8>),
    Error(String),
}

/// Status of an I/O operation
#[derive(Debug)]
pub enum OperationStatus {
    Pending {
        started_at: Instant,
        timeout: Option<Duration>,
        notifier: oneshot::Sender<OperationResult>,
    },
    Completed {
        started_at: Instant,
        completed_at: Instant,
        result: OperationResult,
    },
    Cancelled {
        started_at: Instant,
        cancelled_at: Instant,
        reason: String,
    },
}

impl OperationStatus {
    /// Creates a new pending operation
    pub fn new_pending(timeout: Option<Duration>) -> (Self, oneshot::Receiver<OperationResult>) {
        let (tx, rx) = oneshot::channel();
        let status = Self::Pending {
            started_at: Instant::now(),
            timeout,
            notifier: tx,
        };
        (status, rx)
    }

    /// Completes the operation with a result
    pub fn complete(self, result: OperationResult) -> Self {
        match self {
            Self::Pending {
                started_at,
                notifier,
                ..
            } => {
                // Try to notify waiting tasks (ignore if receiver is dropped)
                let _ = notifier.send(result.clone());

                Self::Completed {
                    started_at,
                    completed_at: Instant::now(),
                    result,
                }
            },
            other => other, // Already completed or cancelled
        }
    }

    /// Cancels the operation
    pub fn cancel(self, reason: String) -> Self {
        match self {
            Self::Pending { started_at, .. } => Self::Cancelled {
                started_at,
                cancelled_at: Instant::now(),
                reason,
            },
            other => other, // Already completed or cancelled
        }
    }

    /// Returns true if the operation is still pending
    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending { .. })
    }

    /// Returns true if the operation has completed successfully
    pub fn is_completed(&self) -> bool {
        matches!(self, Self::Completed { .. })
    }

    /// Returns the duration since the operation started
    pub fn elapsed(&self) -> Duration {
        let started_at = match self {
            Self::Pending { started_at, .. } => *started_at,
            Self::Completed { started_at, .. } => *started_at,
            Self::Cancelled { started_at, .. } => *started_at,
        };
        started_at.elapsed()
    }

    /// Checks if the operation has timed out
    pub fn is_timed_out(&self) -> bool {
        match self {
            Self::Pending {
                started_at,
                timeout: Some(timeout),
                ..
            } => started_at.elapsed() > *timeout,
            _ => false,
        }
    }
}
