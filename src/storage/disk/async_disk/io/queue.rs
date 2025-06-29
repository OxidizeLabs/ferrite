/// I/O Queue Management Module
/// 
/// This module handles the priority queue for I/O operations,
/// including enqueueing, dequeuing, and queue management operations.

use super::operations::{IOOperation, IOOperationType};
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::{Mutex, oneshot};
use std::io::Result as IoResult;

/// Priority queue type for I/O operations
pub type PriorityQueue<T> = Arc<Mutex<BinaryHeap<T>>>;

/// Queue manager responsible for managing I/O operation queue
/// 
/// This component handles all queue-related operations including:
/// - Enqueueing operations with priority
/// - Dequeuing highest priority operations
/// - Queue size management
/// - Operation ID generation
#[derive(Debug)]
pub struct IOQueueManager {
    /// The priority queue holding pending I/O operations
    queue: PriorityQueue<IOOperation>,
    
    /// Atomic counter for generating unique operation IDs
    next_operation_id: AtomicU64,
}

impl IOQueueManager {
    /// Creates a new I/O queue manager
    pub fn new() -> Self {
        Self {
            queue: Arc::new(Mutex::new(BinaryHeap::new())),
            next_operation_id: AtomicU64::new(0),
        }
    }

    /// Enqueues an I/O operation with specified priority
    /// 
    /// Returns the operation ID and a receiver channel that will receive
    /// the operation result when it completes.
    /// 
    /// # Arguments
    /// * `operation_type` - The type of I/O operation to perform
    /// * `priority` - Priority level (higher numbers = higher priority)
    /// 
    /// # Returns
    /// A tuple of (operation_id, result_receiver)
    pub async fn enqueue_operation(
        &self,
        operation_type: IOOperationType,
        priority: u8,
    ) -> (u64, oneshot::Receiver<IoResult<Vec<u8>>>) {
        let id = self.next_operation_id.fetch_add(1, Ordering::SeqCst);
        let (sender, receiver) = oneshot::channel();

        let operation = IOOperation {
            priority,
            id,
            operation_type,
            completion_sender: sender,
            submitted_at: Instant::now(),
        };

        {
            let mut queue = self.queue.lock().await;
            queue.push(operation);
        }

        (id, receiver)
    }

    /// Dequeues the highest priority operation from the queue
    /// 
    /// Returns `Some(operation)` if an operation is available,
    /// or `None` if the queue is empty.
    pub async fn dequeue_operation(&self) -> Option<IOOperation> {
        let mut queue = self.queue.lock().await;
        queue.pop()
    }

    /// Gets the current number of operations in the queue
    pub async fn queue_size(&self) -> usize {
        let queue = self.queue.lock().await;
        queue.len()
    }

    /// Clears all pending operations from the queue
    /// 
    /// Returns the number of operations that were cleared.
    /// Note: This will cause all pending operations to fail
    /// when their completion channels are dropped.
    pub async fn clear_queue(&self) -> usize {
        let mut queue = self.queue.lock().await;
        let count = queue.len();
        queue.clear();
        count
    }

    /// Checks if the queue is empty
    pub async fn is_empty(&self) -> bool {
        let queue = self.queue.lock().await;
        queue.is_empty()
    }

    /// Gets the next operation ID that will be assigned
    /// (useful for testing and debugging)
    pub fn next_operation_id(&self) -> u64 {
        self.next_operation_id.load(Ordering::SeqCst)
    }

    /// Peeks at the highest priority operation without removing it
    /// (useful for debugging and monitoring)
    pub async fn peek_highest_priority(&self) -> Option<u8> {
        let queue = self.queue.lock().await;
        queue.peek().map(|op| op.priority)
    }
}

impl Default for IOQueueManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_queue_priority_ordering() {
        let queue_manager = IOQueueManager::new();

        // Enqueue operations with different priorities
        let (_id1, _receiver1) = queue_manager.enqueue_operation(
            IOOperationType::ReadPage { page_id: 1 },
            5,
        ).await;

        let (_id2, _receiver2) = queue_manager.enqueue_operation(
            IOOperationType::WritePage { page_id: 2, data: vec![1u8; 4096] },
            10, // Higher priority
        ).await;

        let (_id3, _receiver3) = queue_manager.enqueue_operation(
            IOOperationType::ReadPage { page_id: 3 },
            3, // Lower priority
        ).await;

        // Queue should have 3 operations
        assert_eq!(queue_manager.queue_size().await, 3);
        assert!(!queue_manager.is_empty().await);

        // Check highest priority
        assert_eq!(queue_manager.peek_highest_priority().await, Some(10));

        // Dequeue operations - should come out in priority order
        let op1 = queue_manager.dequeue_operation().await.unwrap();
        assert_eq!(op1.priority, 10); // Highest priority first

        let op2 = queue_manager.dequeue_operation().await.unwrap();
        assert_eq!(op2.priority, 5);

        let op3 = queue_manager.dequeue_operation().await.unwrap();
        assert_eq!(op3.priority, 3); // Lowest priority last

        // Queue should be empty now
        assert_eq!(queue_manager.queue_size().await, 0);
        assert!(queue_manager.is_empty().await);
        assert!(queue_manager.dequeue_operation().await.is_none());
        assert_eq!(queue_manager.peek_highest_priority().await, None);
    }

    #[tokio::test]
    async fn test_queue_clear() {
        let queue_manager = IOQueueManager::new();

        // Add some operations
        for i in 0..5 {
            let (_id, _receiver) = queue_manager.enqueue_operation(
                IOOperationType::ReadPage { page_id: i },
                5,
            ).await;
        }

        assert_eq!(queue_manager.queue_size().await, 5);

        // Clear the queue
        let cleared_count = queue_manager.clear_queue().await;
        assert_eq!(cleared_count, 5);
        assert_eq!(queue_manager.queue_size().await, 0);
        assert!(queue_manager.is_empty().await);
    }

    #[tokio::test]
    async fn test_operation_id_generation() {
        let queue_manager = IOQueueManager::new();

        let (id1, _receiver1) = queue_manager.enqueue_operation(
            IOOperationType::Sync,
            10,
        ).await;

        let (id2, _receiver2) = queue_manager.enqueue_operation(
            IOOperationType::SyncLog,
            10,
        ).await;

        // IDs should be sequential
        assert_eq!(id2, id1 + 1);
        
        // Next ID should be greater
        assert!(queue_manager.next_operation_id() > id2);
    }
} 