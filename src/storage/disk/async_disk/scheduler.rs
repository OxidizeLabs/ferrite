//! I/O Scheduler Implementation
//!
//! This module provides scheduling mechanisms for asynchronous I/O operations within the
//! `AsyncDiskManager`. It implements two types of schedulers:
//!
//! - **WorkStealingScheduler**: A scalable scheduler that distributes tasks across multiple
//!   worker threads. It uses a round-robin approach for initial assignment and allows idle
//!   workers to "steal" tasks from busy workers, ensuring balanced load distribution.
//!
//! - **PriorityTaskScheduler**: A priority-based scheduler that manages tasks across
//!   different priority levels (Critical/High, Normal, Low). This ensures that critical
//!   operations like WAL writes or metadata updates are processed before background tasks like prefetching.
//!
//! The module also defines the `IOTask` structure and `IOTaskType` enum, which encapsulate
//! the unit of work for the I/O system.

use crate::common::config::PageId;
use crate::storage::disk::async_disk::config::IOPriority;
use std::io::Result as IoResult;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::sync::{Mutex, Notify, mpsc, oneshot};

/// Advanced work-stealing I/O scheduler
#[derive(Debug)]
pub struct WorkStealingScheduler {
    worker_queues: Vec<mpsc::Sender<IOTask>>,
    receivers: Vec<Mutex<mpsc::Receiver<IOTask>>>,
    worker_count: usize,
    round_robin_counter: AtomicUsize,
}

/// I/O task for work-stealing scheduler
#[derive(Debug)]
pub struct IOTask {
    pub task_type: IOTaskType,
    pub priority: IOPriority,
    pub creation_time: Instant,
    pub completion_callback: Option<oneshot::Sender<IoResult<Vec<u8>>>>,
}

/// Types of I/O tasks
#[derive(Debug)]
pub enum IOTaskType {
    Read(PageId),
    Write(PageId, Vec<u8>),
    BatchRead(Vec<PageId>),
    BatchWrite(Vec<(PageId, Vec<u8>)>),
    Prefetch(Vec<PageId>),
    Flush,
    Sync,
}

impl WorkStealingScheduler {
    pub fn new(worker_count: usize) -> Self {
        let mut worker_queues = Vec::new();
        let mut receivers = Vec::new();
        // Use a reasonable bound to prevent OOM
        let queue_capacity = 256;

        for _ in 0..worker_count {
            let (sender, receiver) = mpsc::channel(queue_capacity);
            worker_queues.push(sender);
            receivers.push(Mutex::new(receiver));
        }

        Self {
            worker_queues,
            receivers,
            worker_count,
            round_robin_counter: AtomicUsize::new(0),
        }
    }

    /// Submits a task to the scheduler
    pub async fn submit_task(&self, task: IOTask) -> Result<(), mpsc::error::SendError<IOTask>> {
        // Use round-robin for now, could be improved with load balancing
        let worker_idx =
            self.round_robin_counter.fetch_add(1, Ordering::Relaxed) % self.worker_count;
        self.worker_queues[worker_idx].send(task).await
    }

    /// Tries to submit a task without waiting (useful for prefetching)
    pub fn try_submit_task(&self, task: IOTask) -> Result<(), mpsc::error::TrySendError<IOTask>> {
        let worker_idx =
            self.round_robin_counter.fetch_add(1, Ordering::Relaxed) % self.worker_count;
        self.worker_queues[worker_idx].try_send(task)
    }

    /// Tries to steal work from another worker's queue
    pub fn try_steal_work(&self, worker_id: usize) -> Option<IOTask> {
        for i in 1..self.worker_count {
            let steal_from = (worker_id + i) % self.worker_count;
            if let Ok(mut rx) = self.receivers[steal_from].try_lock()
                && let Ok(task) = rx.try_recv() {
                    return Some(task);
                }
        }
        None
    }

    /// Gets the receiver for a specific worker
    pub fn get_worker_receiver(&self, worker_id: usize) -> &Mutex<mpsc::Receiver<IOTask>> {
        &self.receivers[worker_id]
    }
}

/// Task scheduler with priority queues
#[derive(Debug)]
pub struct PriorityTaskScheduler {
    high_priority_queue: mpsc::Sender<IOTask>,
    normal_priority_queue: mpsc::Sender<IOTask>,
    low_priority_queue: mpsc::Sender<IOTask>,
    high_receiver: Mutex<mpsc::Receiver<IOTask>>,
    normal_receiver: Mutex<mpsc::Receiver<IOTask>>,
    low_receiver: Mutex<mpsc::Receiver<IOTask>>,
    notify: Arc<Notify>,
}

impl Default for PriorityTaskScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl PriorityTaskScheduler {
    pub fn new() -> Self {
        let capacity = 1024;
        let (high_sender, high_receiver) = mpsc::channel(capacity);
        let (normal_sender, normal_receiver) = mpsc::channel(capacity);
        let (low_sender, low_receiver) = mpsc::channel(capacity);

        Self {
            high_priority_queue: high_sender,
            normal_priority_queue: normal_sender,
            low_priority_queue: low_sender,
            high_receiver: Mutex::new(high_receiver),
            normal_receiver: Mutex::new(normal_receiver),
            low_receiver: Mutex::new(low_receiver),
            notify: Arc::new(Notify::new()),
        }
    }

    pub async fn submit_task(&self, task: IOTask) -> Result<(), mpsc::error::SendError<IOTask>> {
        let result = match task.priority {
            IOPriority::Critical | IOPriority::High => self.high_priority_queue.send(task).await,
            IOPriority::Normal => self.normal_priority_queue.send(task).await,
            IOPriority::Low => self.low_priority_queue.send(task).await,
        };

        if result.is_ok() {
            self.notify.notify_one();
        }

        result
    }

    pub fn get_next_task(&self) -> Option<IOTask> {
        // Try high priority first, then normal, then low
        if let Ok(mut rx) = self.high_receiver.try_lock()
            && let Ok(task) = rx.try_recv() {
                return Some(task);
            }

        if let Ok(mut rx) = self.normal_receiver.try_lock()
            && let Ok(task) = rx.try_recv() {
                return Some(task);
            }

        if let Ok(mut rx) = self.low_receiver.try_lock()
            && let Ok(task) = rx.try_recv() {
                return Some(task);
            }

        None
    }

    /// Waits asynchronously for the next task
    pub async fn next_task(&self) -> IOTask {
        loop {
            if let Some(task) = self.get_next_task() {
                return task;
            }
            self.notify.notified().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_work_stealing_scheduler() {
        let scheduler = WorkStealingScheduler::new(4);

        // Create a task
        let (sender, _receiver) = oneshot::channel();
        let task = IOTask {
            task_type: IOTaskType::Read(1),
            priority: IOPriority::Normal,
            creation_time: Instant::now(),
            completion_callback: Some(sender),
        };

        // Submit the task
        scheduler.submit_task(task).await.unwrap();

        // Try to steal work
        let stolen = scheduler.try_steal_work(1);
        assert!(stolen.is_some());
    }

    #[tokio::test]
    async fn test_priority_scheduler() {
        let scheduler = PriorityTaskScheduler::new();

        // Create tasks with different priorities
        let (sender1, _receiver1) = oneshot::channel();
        let high_task = IOTask {
            task_type: IOTaskType::Read(1),
            priority: IOPriority::High,
            creation_time: Instant::now(),
            completion_callback: Some(sender1),
        };

        let (sender2, _receiver2) = oneshot::channel();
        let low_task = IOTask {
            task_type: IOTaskType::Read(2),
            priority: IOPriority::Low,
            creation_time: Instant::now(),
            completion_callback: Some(sender2),
        };

        // Submit tasks in reverse priority order
        scheduler.submit_task(low_task).await.unwrap();
        scheduler.submit_task(high_task).await.unwrap();

        // High priority task should be returned first
        let next_task = scheduler.get_next_task().unwrap();
        if let IOTaskType::Read(page_id) = next_task.task_type {
            assert_eq!(page_id, 1);
        } else {
            panic!("Expected Read task");
        }
    }

    #[tokio::test]
    async fn test_round_robin_distribution_across_workers() {
        let scheduler = WorkStealingScheduler::new(3);

        // Submit 9 tasks which should distribute evenly across 3 workers (3 each)
        for i in 0..9u64 {
            let (sender, _receiver) = oneshot::channel();
            let task = IOTask {
                task_type: IOTaskType::Read(i as PageId),
                priority: IOPriority::Normal,
                creation_time: Instant::now(),
                completion_callback: Some(sender),
            };
            scheduler.submit_task(task).await.unwrap();
        }

        let mut counts = vec![0usize; 3];
        for (worker_id, item) in counts.iter_mut().enumerate().take(3) {
            let rx = scheduler.get_worker_receiver(worker_id);
            let mut rx_guard = rx.lock().await;
            while let Ok(_task) = rx_guard.try_recv() {
                *item += 1;
            }
        }

        assert_eq!(counts, vec![3, 3, 3]);
    }

    #[tokio::test]
    async fn test_try_steal_when_other_worker_has_tasks() {
        let scheduler = WorkStealingScheduler::new(2);

        // First task should go to worker 0 via round-robin
        let (sender, _receiver) = oneshot::channel();
        let task = IOTask {
            task_type: IOTaskType::Read(42),
            priority: IOPriority::Normal,
            creation_time: Instant::now(),
            completion_callback: Some(sender),
        };
        scheduler.submit_task(task).await.unwrap();

        // Worker 1 attempts to steal and should get the task from worker 0
        let stolen = scheduler.try_steal_work(1);
        assert!(stolen.is_some());

        // Original worker 0 queue should now be empty
        let rx0 = scheduler.get_worker_receiver(0);
        let mut rx0_guard = rx0.lock().await;
        assert!(rx0_guard.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_try_steal_returns_none_when_no_tasks_available() {
        let scheduler = WorkStealingScheduler::new(2);
        let stolen = scheduler.try_steal_work(0);
        assert!(stolen.is_none());
    }

    #[tokio::test]
    async fn test_priority_scheduler_fifo_within_same_priority() {
        let scheduler = PriorityTaskScheduler::new();

        // Two high-priority tasks should be returned in FIFO order
        let (s1, _r1) = oneshot::channel();
        let high1 = IOTask {
            task_type: IOTaskType::Read(10),
            priority: IOPriority::High,
            creation_time: Instant::now(),
            completion_callback: Some(s1),
        };
        let (s2, _r2) = oneshot::channel();
        let high2 = IOTask {
            task_type: IOTaskType::Read(11),
            priority: IOPriority::High,
            creation_time: Instant::now(),
            completion_callback: Some(s2),
        };

        scheduler.submit_task(high1).await.unwrap();
        scheduler.submit_task(high2).await.unwrap();

        let next1 = scheduler.get_next_task().unwrap();
        match next1.task_type {
            IOTaskType::Read(id) => assert_eq!(id, 10),
            _ => panic!("Expected Read task"),
        }

        let next2 = scheduler.get_next_task().unwrap();
        match next2.task_type {
            IOTaskType::Read(id) => assert_eq!(id, 11),
            _ => panic!("Expected Read task"),
        }
    }

    #[tokio::test]
    async fn test_priority_levels_order_high_then_normal_then_low() {
        let scheduler = PriorityTaskScheduler::new();

        // Enqueue normal and low first, then high
        let (sn, _rn) = oneshot::channel();
        let normal = IOTask {
            task_type: IOTaskType::Read(20),
            priority: IOPriority::Normal,
            creation_time: Instant::now(),
            completion_callback: Some(sn),
        };
        let (sl, _rl) = oneshot::channel();
        let low = IOTask {
            task_type: IOTaskType::Read(21),
            priority: IOPriority::Low,
            creation_time: Instant::now(),
            completion_callback: Some(sl),
        };
        let (sh, _rh) = oneshot::channel();
        let high = IOTask {
            task_type: IOTaskType::Read(22),
            priority: IOPriority::High,
            creation_time: Instant::now(),
            completion_callback: Some(sh),
        };

        scheduler.submit_task(normal).await.unwrap();
        scheduler.submit_task(low).await.unwrap();
        scheduler.submit_task(high).await.unwrap();

        // High should preempt normal and low
        let first = scheduler.get_next_task().unwrap();
        match first.task_type {
            IOTaskType::Read(id) => assert_eq!(id, 22),
            _ => panic!("Expected Read task"),
        }

        // Then Normal
        let second = scheduler.get_next_task().unwrap();
        match second.task_type {
            IOTaskType::Read(id) => assert_eq!(id, 20),
            _ => panic!("Expected Read task"),
        }

        // Then Low
        let third = scheduler.get_next_task().unwrap();
        match third.task_type {
            IOTaskType::Read(id) => assert_eq!(id, 21),
            _ => panic!("Expected Read task"),
        }
    }

    #[tokio::test]
    async fn test_priority_scheduler_empty_returns_none() {
        let scheduler = PriorityTaskScheduler::new();
        assert!(scheduler.get_next_task().is_none());
    }
}
