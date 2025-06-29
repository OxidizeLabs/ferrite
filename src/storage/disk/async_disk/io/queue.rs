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
    use std::time::Duration;
    use tokio::time::timeout;

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

    #[tokio::test]
    async fn test_all_operation_types() {
        let queue_manager = IOQueueManager::new();
        let test_data = vec![0u8; 4096];

        // Test all operation types can be enqueued
        let operations = vec![
            IOOperationType::ReadPage { page_id: 1 },
            IOOperationType::WritePage { page_id: 2, data: test_data.clone() },
            IOOperationType::Sync,
            IOOperationType::SyncLog,
        ];

        let mut receivers = Vec::new();
        for (i, op_type) in operations.into_iter().enumerate() {
            let (id, receiver) = queue_manager.enqueue_operation(op_type, (i + 1) as u8).await;
            assert_eq!(id, i as u64);
            receivers.push(receiver);
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
            let (id, _receiver) = queue_manager.enqueue_operation(
                IOOperationType::ReadPage { page_id: i },
                5, // Same priority
            ).await;
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
                        let (id, receiver) = qm.enqueue_operation(
                            IOOperationType::ReadPage { page_id: (task_id * num_ops_per_task + op_id) as u64 },
                            ((task_id + op_id) % 10) as u8, // Varying priorities
                        ).await;
                        receivers.push((id, receiver));
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
        let mut ids: Vec<_> = all_receivers.iter().map(|(id, _)| *id).collect();
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
            let (_id, _receiver) = queue_manager.enqueue_operation(
                IOOperationType::ReadPage { page_id: i as u64 },
                (i % 10) as u8,
            ).await;
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
                let (_id, _receiver) = producer_qm.enqueue_operation(
                    IOOperationType::ReadPage { page_id: count as u64 },
                    (count % 256) as u8,
                ).await;
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
    async fn test_completion_channels() {
        let queue_manager = IOQueueManager::new();

        // Enqueue an operation and get the receiver
        let (id, receiver) = queue_manager.enqueue_operation(
            IOOperationType::ReadPage { page_id: 42 },
            10,
        ).await;

        // Dequeue the operation
        let operation = queue_manager.dequeue_operation().await.unwrap();
        assert_eq!(operation.id, id);
        assert_eq!(operation.priority, 10);

        // Simulate completion by sending result through the channel
        let test_data = vec![1, 2, 3, 4];
        operation.completion_sender.send(Ok(test_data.clone())).unwrap();

        // Receiver should get the result
        let received_result = receiver.await.unwrap();
        assert_eq!(received_result.unwrap(), test_data);
    }

    #[tokio::test]
    async fn test_completion_channel_dropped() {
        let queue_manager = IOQueueManager::new();

        // Enqueue an operation
        let (_id, receiver) = queue_manager.enqueue_operation(
            IOOperationType::ReadPage { page_id: 42 },
            10,
        ).await;

        // Dequeue and drop the operation (simulating worker crash)
        let operation = queue_manager.dequeue_operation().await.unwrap();
        drop(operation); // This drops the completion_sender

        // Receiver should detect the channel was closed
        let result = receiver.await;
        assert!(result.is_err()); // Should be RecvError
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
        let (_id, _receiver) = queue_manager.enqueue_operation(
            IOOperationType::ReadPage { page_id: 1 },
            5,
        ).await;

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
        let (_id, _receiver) = queue_manager.enqueue_operation(
            IOOperationType::WritePage { page_id: 1, data: large_data.clone() },
            5,
        ).await;

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
        let (_id1, _receiver1) = queue_manager.enqueue_operation(
            IOOperationType::ReadPage { page_id: 1 },
            u8::MIN, // Minimum priority
        ).await;

        let (_id2, _receiver2) = queue_manager.enqueue_operation(
            IOOperationType::ReadPage { page_id: 2 },
            u8::MAX, // Maximum priority
        ).await;

        // Max priority should come first
        let op1 = queue_manager.dequeue_operation().await.unwrap();
        assert_eq!(op1.priority, u8::MAX);

        let op2 = queue_manager.dequeue_operation().await.unwrap();
        assert_eq!(op2.priority, u8::MIN);
    }

    #[tokio::test]
    async fn test_queue_manager_default() {
        let queue_manager = IOQueueManager::default();
        
        // Should start with empty queue and ID 0
        assert!(queue_manager.is_empty().await);
        assert_eq!(queue_manager.next_operation_id(), 0);

        // Should work normally
        let (id, _receiver) = queue_manager.enqueue_operation(
            IOOperationType::Sync,
            5,
        ).await;
        assert_eq!(id, 0);
        assert!(!queue_manager.is_empty().await);
    }

    #[tokio::test]
    async fn test_mixed_operations_priority_order() {
        let queue_manager = IOQueueManager::new();

        // Mix of different operation types with different priorities
        let operations = vec![
            (IOOperationType::ReadPage { page_id: 1 }, 3u8),
            (IOOperationType::WritePage { page_id: 2, data: vec![1; 100] }, 1u8),
            (IOOperationType::Sync, 5u8),
            (IOOperationType::SyncLog, 2u8),
            (IOOperationType::ReadPage { page_id: 3 }, 4u8),
        ];

        // Enqueue all operations
        for (op_type, priority) in operations {
            let (_id, _receiver) = queue_manager.enqueue_operation(op_type, priority).await;
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
            
            let (_id, _receiver) = queue_manager.enqueue_operation(
                IOOperationType::Sync,
                5,
            ).await;
            
            assert!(!queue_manager.is_empty().await);
            assert_eq!(queue_manager.queue_size().await, 1);
        }).await;

        assert!(result.is_ok(), "Queue operations should not timeout");
    }
} 