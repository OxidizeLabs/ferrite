//! # I/O Operation Queue
//!
//! This module provides `IOQueueManager`, a thread-safe, priority-based scheduling queue for
//! pending I/O operations. It ensures critical database operations (like WAL writes) are
//! processed before background tasks (like prefetching).
//!
//! ## Architecture
//!
//! ```text
//!   AsyncDiskManager / Callers
//!   ═══════════════════════════════════════════════════════════════════════════
//!          │
//!          │ enqueue_operation(op_type, priority, id)
//!          ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                        IOQueueManager                                   │
//!   │                                                                         │
//!   │   ┌─────────────────────────────────────────────────────────────────┐   │
//!   │   │  queue: Arc<Mutex<BinaryHeap<IOOperation>>>                     │   │
//!   │   │                                                                 │   │
//!   │   │  Priority ordering (max-heap):                                  │   │
//!   │   │                                                                 │   │
//!   │   │         ┌─────────────────────────────────────────┐             │   │
//!   │   │  Top ─► │ priority=255 (WAL sync)                 │             │   │
//!   │   │         ├─────────────────────────────────────────┤             │   │
//!   │   │         │ priority=200 (log write)                │             │   │
//!   │   │         ├─────────────────────────────────────────┤             │   │
//!   │   │         │ priority=100 (page write)               │             │   │
//!   │   │         ├─────────────────────────────────────────┤             │   │
//!   │   │         │ priority=50 (page read)                 │             │   │
//!   │   │         ├─────────────────────────────────────────┤             │   │
//!   │   │  Bot ─► │ priority=10 (prefetch)                  │             │   │
//!   │   │         └─────────────────────────────────────────┘             │   │
//!   │   │                                                                 │   │
//!   │   └─────────────────────────────────────────────────────────────────┘   │
//!   └─────────────────────────────────────────────────────────────────────────┘
//!          │
//!          │ dequeue_operation() → highest priority first
//!          ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │              IOWorkerManager (workers)                                  │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## IOOperation Structure
//!
//! ```text
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │  IOOperation                                                            │
//!   │                                                                         │
//!   │  ┌────────────────┬────────────────────────────────────────────────┐    │
//!   │  │ Field          │ Purpose                                        │    │
//!   │  ├────────────────┼────────────────────────────────────────────────┤    │
//!   │  │ priority: u8   │ Ordering key (higher = dequeued first)         │    │
//!   │  │ id: u64        │ Unique operation ID (for tracking/completion)  │    │
//!   │  │ operation_type │ ReadPage, WritePage, Sync, etc.                │    │
//!   │  │ submitted_at   │ Instant when enqueued (FIFO tie-breaking)      │    │
//!   │  └────────────────┴────────────────────────────────────────────────┘    │
//!   │                                                                         │
//!   │  Ord implementation: Compares by (priority DESC, submitted_at ASC)      │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Priority Ordering
//!
//! ```text
//!   Priority Values (higher = more urgent)
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   255 ─┬─ Maximum priority (WAL sync, critical durability)
//!        │
//!   200 ─┼─ Log operations (append, read)
//!        │
//!   150 ─┼─ Page writes (dirty page flush)
//!        │
//!   100 ─┼─ Page reads (user-initiated)
//!        │
//!    50 ─┼─ Background operations
//!        │
//!     0 ─┴─ Minimum priority (prefetch, speculative reads)
//!
//!   Tie-breaking: When priorities are equal, earlier submitted_at wins
//! ```
//!
//! ## Key Components
//!
//! | Component         | Description                                         |
//! |-------------------|-----------------------------------------------------|
//! | `IOQueueManager`  | Thread-safe queue wrapper with priority ordering    |
//! | `PriorityQueue<T>`| Type alias for `Arc<Mutex<BinaryHeap<T>>>`          |
//! | `IOOperation`     | Operation with priority, id, type, and timestamp    |
//!
//! ## Core Operations
//!
//! | Method                  | Description                                    |
//! |-------------------------|------------------------------------------------|
//! | `new()`                 | Create empty queue manager                     |
//! | `enqueue_operation()`   | Add operation with priority and ID             |
//! | `dequeue_operation()`   | Remove and return highest-priority operation   |
//! | `queue_size()`          | Get current number of pending operations       |
//! | `is_empty()`            | Check if queue has no operations               |
//! | `peek_highest_priority()`| Get priority of next operation (no removal)   |
//! | `clear_queue()`         | Remove all operations, return count            |
//! | `drain_queue()`         | Remove all operations, return them as Vec      |
//!
//! ## Producer-Consumer Pattern
//!
//! ```text
//!   Producers (multiple)                     Consumer (workers)
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   ┌─────────────┐
//!   │ Caller A    │──┐
//!   └─────────────┘  │
//!                    │  enqueue_operation()
//!   ┌─────────────┐  │           ┌────────────────────┐
//!   │ Caller B    │──┼──────────►│   IOQueueManager   │
//!   └─────────────┘  │           │                    │
//!                    │           │  ┌──────────────┐  │  dequeue_operation()
//!   ┌─────────────┐  │           │  │ BinaryHeap   │  │◄──────────────────────┐
//!   │ Caller C    │──┘           │  │ (max-heap)   │  │                       │
//!   └─────────────┘              │  └──────────────┘  │       ┌─────────────┐ │
//!                                └────────────────────┘       │ Worker 0    │─┘
//!                                                             ├─────────────┤
//!   Thread-safe via tokio::sync::Mutex                        │ Worker 1    │
//!   • Lock acquired for each operation                        ├─────────────┤
//!   • Short critical sections                                 │ Worker 2    │
//!                                                             └─────────────┘
//! ```
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::storage::disk::async_disk::io::queue::IOQueueManager;
//! use crate::storage::disk::async_disk::io::operations::{IOOperationType, priorities};
//!
//! // Create queue manager
//! let queue_manager = IOQueueManager::new();
//!
//! // Enqueue operations with different priorities
//! queue_manager.enqueue_operation(
//!     IOOperationType::ReadPage { page_id: 42 },
//!     priorities::PAGE_READ,
//!     1, // operation ID
//! ).await;
//!
//! queue_manager.enqueue_operation(
//!     IOOperationType::SyncLog,
//!     priorities::LOG_SYNC,  // Higher priority
//!     2,
//! ).await;
//!
//! // Check queue state
//! println!("Queue size: {}", queue_manager.queue_size().await);
//! println!("Highest priority: {:?}", queue_manager.peek_highest_priority().await);
//!
//! // Dequeue highest-priority operation (SyncLog comes first)
//! if let Some(op) = queue_manager.dequeue_operation().await {
//!     println!("Processing: {:?} with priority {}", op.operation_type, op.priority);
//! }
//!
//! // Drain all remaining operations (for shutdown)
//! let remaining = queue_manager.drain_queue().await;
//! for op in remaining {
//!     // Cancel or complete each operation
//! }
//! ```
//!
//! ## Drain vs Clear
//!
//! ```text
//!   clear_queue()                          drain_queue()
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   Returns: usize (count of removed)      Returns: Vec<IOOperation>
//!
//!   Use when:                              Use when:
//!   • Just need to empty the queue         • Need to process/cancel each operation
//!   • Don't care about dropped operations  • Want to complete pending receivers
//!   • Quick cleanup                        • Graceful shutdown
//! ```
//!
//! ## Thread Safety
//!
//! - `queue`: `Arc<Mutex<BinaryHeap<IOOperation>>>` (tokio async mutex)
//! - All operations acquire the lock for the duration of the operation
//! - Short critical sections minimize contention
//! - Safe for concurrent enqueue from multiple producers
//! - Safe for concurrent dequeue from multiple workers
//! - Implements `Default` for easy construction

use super::operations::{IOOperation, IOOperationType};
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

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
}

impl IOQueueManager {
    /// Creates a new I/O queue manager
    pub fn new() -> Self {
        Self {
            queue: Arc::new(Mutex::new(BinaryHeap::new())),
        }
    }

    /// Enqueues an I/O operation with specified priority and id.
    ///
    /// # Arguments
    /// * `operation_type` - The type of I/O operation to perform
    /// * `priority` - Priority level (higher numbers = higher priority)
    /// * `id` - Unique operation identifier (typically allocated by a tracker)
    ///
    pub async fn enqueue_operation(
        &self,
        operation_type: IOOperationType,
        priority: u8,
        id: u64,
    ) {

        let operation = IOOperation {
            priority,
            id,
            operation_type,
            submitted_at: Instant::now(),
        };

        {
            let mut queue = self.queue.lock().await;
            queue.push(operation);
        }
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
    pub async fn clear_queue(&self) -> usize {
        self.drain_queue().await.len()
    }

    /// Drains all pending operations from the queue.
    ///
    /// This is useful for cancellation/shutdown flows where callers want to
    /// explicitly complete/cancel drained operations rather than dropping them.
    pub async fn drain_queue(&self) -> Vec<IOOperation> {
        let mut queue = self.queue.lock().await;
        let count = queue.len();
        let mut drained = Vec::with_capacity(count);
        while let Some(op) = queue.pop() {
            drained.push(op);
        }
        drained
    }

    /// Checks if the queue is empty
    pub async fn is_empty(&self) -> bool {
        let queue = self.queue.lock().await;
        queue.is_empty()
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
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_queue_priority_ordering() {
        let queue_manager = IOQueueManager::new();

        // Enqueue operations with different priorities
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
            queue_manager
                .enqueue_operation(IOOperationType::ReadPage { page_id: i }, 5, i + 1)
                .await;
        }

        assert_eq!(queue_manager.queue_size().await, 5);

        // Clear the queue
        let cleared_count = queue_manager.clear_queue().await;
        assert_eq!(cleared_count, 5);
        assert_eq!(queue_manager.queue_size().await, 0);
        assert!(queue_manager.is_empty().await);
    }

    #[tokio::test]
    async fn test_all_operation_types() {
        let queue_manager = IOQueueManager::new();
        let test_data = vec![0u8; 4096];

        // Test all operation types can be enqueued
        let operations = vec![
            IOOperationType::ReadPage { page_id: 1 },
            IOOperationType::WritePage {
                page_id: 2,
                data: test_data.clone(),
            },
            IOOperationType::Sync,
            IOOperationType::SyncLog,
        ];

        for (i, op_type) in operations.into_iter().enumerate() {
            queue_manager
                .enqueue_operation(op_type, (i + 1) as u8, (i + 1) as u64)
                .await;
        }

        assert_eq!(queue_manager.queue_size().await, 4);

        // Dequeue all operations (should be in priority order: 4, 3, 2, 1)
        for expected_priority in [4u8, 3, 2, 1] {
            let op = queue_manager.dequeue_operation().await.unwrap();
            assert_eq!(op.priority, expected_priority);
        }

        assert!(queue_manager.is_empty().await);
    }

    #[tokio::test]
    async fn test_same_priority_operations() {
        let queue_manager = IOQueueManager::new();

        // Enqueue multiple operations with same priority
        let mut operation_ids = Vec::new();
        for i in 0..5 {
            let id = (i + 1) as u64;
            queue_manager
                .enqueue_operation(
                    IOOperationType::ReadPage { page_id: i },
                    5, // Same priority
                    id,
                )
                .await;
            operation_ids.push(id);
        }

        assert_eq!(queue_manager.queue_size().await, 5);

        // With same priority, operations should maintain some order
        // (BinaryHeap with same priority may not guarantee FIFO, but IDs should be unique)
        let mut dequeued_ids = Vec::new();
        while let Some(op) = queue_manager.dequeue_operation().await {
            assert_eq!(op.priority, 5);
            dequeued_ids.push(op.id);
        }

        assert_eq!(dequeued_ids.len(), 5);
        // All IDs should be unique
        dequeued_ids.sort();
        operation_ids.sort();
        assert_eq!(dequeued_ids, operation_ids);
    }

    #[tokio::test]
    async fn test_concurrent_enqueue() {
        let queue_manager = Arc::new(IOQueueManager::new());
        let num_tasks = 100;
        let num_ops_per_task = 10;

        // Spawn multiple tasks that enqueue operations concurrently
        let handles: Vec<_> = (0..num_tasks)
            .map(|task_id| {
                let qm = Arc::clone(&queue_manager);
                tokio::spawn(async move {
                    let mut receivers = Vec::new();
                    for op_id in 0..num_ops_per_task {
                        let id = (task_id * num_ops_per_task + op_id + 1) as u64;
                        qm
                            .enqueue_operation(
                                IOOperationType::ReadPage {
                                    page_id: (task_id * num_ops_per_task + op_id) as u64,
                                },
                                ((task_id + op_id) % 10) as u8, // Varying priorities
                                id,
                            )
                            .await;
                        receivers.push(id);
                    }
                    receivers
                })
            })
            .collect();

        // Wait for all tasks to complete
        let mut all_receivers = Vec::new();
        for handle in handles {
            let receivers = handle.await.unwrap();
            all_receivers.extend(receivers);
        }

        // Verify total number of operations
        let expected_total = num_tasks * num_ops_per_task;
        assert_eq!(queue_manager.queue_size().await, expected_total);
        assert_eq!(all_receivers.len(), expected_total);

        // Verify all operation IDs are unique
        let mut ids: Vec<_> = all_receivers.iter().copied().collect();
        ids.sort();
        let unique_ids: std::collections::HashSet<_> = ids.iter().cloned().collect();
        assert_eq!(unique_ids.len(), expected_total);
    }

    #[tokio::test]
    async fn test_concurrent_dequeue() {
        let queue_manager = Arc::new(IOQueueManager::new());
        let num_operations = 50usize;

        // Enqueue operations first
        for i in 0..num_operations {
            queue_manager
                .enqueue_operation(
                    IOOperationType::ReadPage { page_id: i as u64 },
                    (i % 10) as u8,
                    (i + 1) as u64,
                )
                .await;
        }

        assert_eq!(queue_manager.queue_size().await, num_operations);

        // Spawn multiple tasks that dequeue operations concurrently
        let num_workers = 5;
        let handles: Vec<_> = (0..num_workers)
            .map(|_| {
                let qm = Arc::clone(&queue_manager);
                tokio::spawn(async move {
                    let mut dequeued = Vec::new();
                    while let Some(op) = qm.dequeue_operation().await {
                        dequeued.push(op.id);
                    }
                    dequeued
                })
            })
            .collect();

        // Wait for all workers to complete
        let mut all_dequeued = Vec::new();
        for handle in handles {
            let dequeued = handle.await.unwrap();
            all_dequeued.extend(dequeued);
        }

        // Verify all operations were dequeued exactly once
        assert_eq!(all_dequeued.len(), num_operations);
        assert!(queue_manager.is_empty().await);

        // Verify all IDs are unique (no operation was dequeued twice)
        let unique_ids: std::collections::HashSet<_> = all_dequeued.into_iter().collect();
        assert_eq!(unique_ids.len(), num_operations);
    }

    #[tokio::test]
    async fn test_queue_operations_under_load() {
        let queue_manager = Arc::new(IOQueueManager::new());
        let duration = Duration::from_millis(100);
        let start_time = Instant::now();

        // Spawn producer task
        let producer_qm = Arc::clone(&queue_manager);
        let producer = tokio::spawn(async move {
            let mut count = 0usize;
            while start_time.elapsed() < duration {
                producer_qm
                    .enqueue_operation(
                        IOOperationType::ReadPage {
                            page_id: count as u64,
                        },
                        (count % 256) as u8,
                        (count + 1) as u64,
                    )
                    .await;
                count += 1;
                tokio::task::yield_now().await;
            }
            count
        });

        // Spawn consumer task
        let consumer_qm = Arc::clone(&queue_manager);
        let consumer = tokio::spawn(async move {
            let mut count = 0usize;
            while start_time.elapsed() < duration {
                if let Some(_op) = consumer_qm.dequeue_operation().await {
                    count += 1;
                }
                tokio::task::yield_now().await;
            }
            count
        });

        let (produced, consumed) = tokio::join!(producer, consumer);
        let produced = produced.unwrap();
        let consumed = consumed.unwrap();

        println!("Produced: {}, Consumed: {}", produced, consumed);
        assert!(produced > 0, "Should have produced some operations");

        // Final queue size should be the difference
        let final_size = queue_manager.queue_size().await;
        assert_eq!(final_size, produced - consumed);
    }

    #[tokio::test]
    async fn test_empty_queue_operations() {
        let queue_manager = IOQueueManager::new();

        // Test operations on empty queue
        assert!(queue_manager.is_empty().await);
        assert_eq!(queue_manager.queue_size().await, 0);
        assert!(queue_manager.dequeue_operation().await.is_none());
        assert_eq!(queue_manager.peek_highest_priority().await, None);
        assert_eq!(queue_manager.clear_queue().await, 0);
    }

    #[tokio::test]
    async fn test_operation_timestamps() {
        let queue_manager = IOQueueManager::new();

        let start_time = Instant::now();

        // Enqueue operation
        queue_manager
            .enqueue_operation(IOOperationType::ReadPage { page_id: 1 }, 5, 1)
            .await;

        // Small delay
        tokio::time::sleep(Duration::from_millis(1)).await;

        // Dequeue operation
        let operation = queue_manager.dequeue_operation().await.unwrap();

        // Check that the timestamp is reasonable
        assert!(operation.submitted_at >= start_time);
        assert!(operation.submitted_at <= Instant::now());
    }

    #[tokio::test]
    async fn test_large_data_operations() {
        let queue_manager = IOQueueManager::new();

        // Test with large data payload
        let large_data = vec![42u8; 1024 * 1024]; // 1MB
        queue_manager
            .enqueue_operation(
                IOOperationType::WritePage {
                    page_id: 1,
                    data: large_data.clone(),
                },
                5,
                1,
            )
            .await;

        let operation = queue_manager.dequeue_operation().await.unwrap();
        match operation.operation_type {
            IOOperationType::WritePage { page_id, data } => {
                assert_eq!(page_id, 1);
                assert_eq!(data.len(), 1024 * 1024);
                assert_eq!(data, large_data);
            }
            _ => panic!("Expected WritePage operation"),
        }
    }

    #[tokio::test]
    async fn test_priority_boundaries() {
        let queue_manager = IOQueueManager::new();

        // Test with min and max priority values
        queue_manager
            .enqueue_operation(
                IOOperationType::ReadPage { page_id: 1 },
                u8::MIN, // Minimum priority
                1,
            )
            .await;

        queue_manager
            .enqueue_operation(
                IOOperationType::ReadPage { page_id: 2 },
                u8::MAX, // Maximum priority
                2,
            )
            .await;

        // Max priority should come first
        let op1 = queue_manager.dequeue_operation().await.unwrap();
        assert_eq!(op1.priority, u8::MAX);

        let op2 = queue_manager.dequeue_operation().await.unwrap();
        assert_eq!(op2.priority, u8::MIN);
    }

    #[tokio::test]
    async fn test_queue_manager_default() {
        let queue_manager = IOQueueManager::default();

        // Should start with empty queue
        assert!(queue_manager.is_empty().await);

        // Should work normally
        queue_manager.enqueue_operation(IOOperationType::Sync, 5, 1).await;
        assert!(!queue_manager.is_empty().await);
    }

    #[tokio::test]
    async fn test_mixed_operations_priority_order() {
        let queue_manager = IOQueueManager::new();

        // Mix of different operation types with different priorities
        let operations = vec![
            (IOOperationType::ReadPage { page_id: 1 }, 3u8),
            (
                IOOperationType::WritePage {
                    page_id: 2,
                    data: vec![1; 100],
                },
                1u8,
            ),
            (IOOperationType::Sync, 5u8),
            (IOOperationType::SyncLog, 2u8),
            (IOOperationType::ReadPage { page_id: 3 }, 4u8),
        ];

        // Enqueue all operations
        for (i, (op_type, priority)) in operations.into_iter().enumerate() {
            queue_manager
                .enqueue_operation(op_type, priority, (i + 1) as u64)
                .await;
        }

        // Dequeue and verify priority order: 5, 4, 3, 2, 1
        let expected_priorities = vec![5u8, 4, 3, 2, 1];
        for expected_priority in expected_priorities {
            let op = queue_manager.dequeue_operation().await.unwrap();
            assert_eq!(op.priority, expected_priority);
        }

        assert!(queue_manager.is_empty().await);
    }

    #[tokio::test]
    async fn test_timeout_scenarios() {
        let queue_manager = IOQueueManager::new();

        // Test that queue operations don't hang
        let result = timeout(Duration::from_millis(100), async {
            // These should complete quickly
            assert!(queue_manager.is_empty().await);
            assert_eq!(queue_manager.queue_size().await, 0);
            assert!(queue_manager.dequeue_operation().await.is_none());

            queue_manager.enqueue_operation(IOOperationType::Sync, 5, 1).await;

            assert!(!queue_manager.is_empty().await);
            assert_eq!(queue_manager.queue_size().await, 1);
        })
        .await;

        assert!(result.is_ok(), "Queue operations should not timeout");
    }
}
