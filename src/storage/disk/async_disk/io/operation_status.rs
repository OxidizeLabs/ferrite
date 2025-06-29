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
            Self::Pending { started_at, notifier, .. } => {
                // Try to notify waiting tasks (ignore if receiver is dropped)
                let _ = notifier.send(result.clone());
                
                Self::Completed {
                    started_at,
                    completed_at: Instant::now(),
                    result,
                }
            }
            other => other, // Already completed or cancelled
        }
    }

    /// Cancels the operation
    pub fn cancel(self, reason: String) -> Self {
        match self {
            Self::Pending { started_at, .. } => {
                Self::Cancelled {
                    started_at,
                    cancelled_at: Instant::now(),
                    reason,
                }
            }
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

    /// Returns true if the operation was cancelled
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled { .. })
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
            Self::Pending { started_at, timeout: Some(timeout), .. } => {
                started_at.elapsed() > *timeout
            }
            _ => false,
        }
    }
} 