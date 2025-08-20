//! Work-stealing I/O scheduler for the Async Disk Manager
//! 
//! This module contains the work-stealing scheduler for I/O tasks.

use crate::common::config::PageId;
use crate::storage::disk::async_disk::config::IOPriority;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::sync::oneshot;
use std::io::Result as IoResult;

/// Advanced work-stealing I/O scheduler
#[derive(Debug)]
pub struct WorkStealingScheduler {
    worker_queues: Vec<Arc<crossbeam_channel::Sender<IOTask>>>,
    receivers: Vec<crossbeam_channel::Receiver<IOTask>>,
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

        for _ in 0..worker_count {
            let (sender, receiver) = crossbeam_channel::unbounded();
            worker_queues.push(Arc::new(sender));
            receivers.push(receiver);
        }

        Self {
            worker_queues,
            receivers,
            worker_count,
            round_robin_counter: AtomicUsize::new(0),
        }
    }

    /// Submits a task to the scheduler
    pub fn submit_task(&self, task: IOTask) -> Result<(), crossbeam_channel::SendError<IOTask>> {
        // Use round-robin for now, could be improved with load balancing
        let worker_idx = self.round_robin_counter.fetch_add(1, Ordering::Relaxed) % self.worker_count;
        self.worker_queues[worker_idx].send(task)
    }

    /// Tries to steal work from another worker's queue
    pub fn try_steal_work(&self, worker_id: usize) -> Option<IOTask> {
        for i in 1..self.worker_count {
            let steal_from = (worker_id + i) % self.worker_count;
            if let Ok(task) = self.receivers[steal_from].try_recv() {
                return Some(task);
            }
        }
        None
    }

    /// Gets the receiver for a specific worker
    pub fn get_worker_receiver(&self, worker_id: usize) -> &crossbeam_channel::Receiver<IOTask> {
        &self.receivers[worker_id]
    }
}

/// Task scheduler with priority queues
pub struct PriorityTaskScheduler {
    high_priority_queue: crossbeam_channel::Sender<IOTask>,
    normal_priority_queue: crossbeam_channel::Sender<IOTask>,
    low_priority_queue: crossbeam_channel::Sender<IOTask>,
    high_receiver: crossbeam_channel::Receiver<IOTask>,
    normal_receiver: crossbeam_channel::Receiver<IOTask>,
    low_receiver: crossbeam_channel::Receiver<IOTask>,
}

impl PriorityTaskScheduler {
    pub fn new() -> Self {
        let (high_sender, high_receiver) = crossbeam_channel::unbounded();
        let (normal_sender, normal_receiver) = crossbeam_channel::unbounded();
        let (low_sender, low_receiver) = crossbeam_channel::unbounded();
        
        Self {
            high_priority_queue: high_sender,
            normal_priority_queue: normal_sender,
            low_priority_queue: low_sender,
            high_receiver,
            normal_receiver,
            low_receiver,
        }
    }
    
    pub fn submit_task(&self, task: IOTask) -> Result<(), crossbeam_channel::SendError<IOTask>> {
        match task.priority {
            IOPriority::Critical | IOPriority::High => self.high_priority_queue.send(task),
            IOPriority::Normal => self.normal_priority_queue.send(task),
            IOPriority::Low => self.low_priority_queue.send(task),
        }
    }
    
    pub fn get_next_task(&self) -> Option<IOTask> {
        // Try high priority first, then normal, then low
        if let Ok(task) = self.high_receiver.try_recv() {
            return Some(task);
        }
        
        if let Ok(task) = self.normal_receiver.try_recv() {
            return Some(task);
        }
        
        if let Ok(task) = self.low_receiver.try_recv() {
            return Some(task);
        }
        
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_work_stealing_scheduler() {
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
        scheduler.submit_task(task).unwrap();
        
        // Try to steal work
        let stolen = scheduler.try_steal_work(1);
        assert!(stolen.is_some());
    }
    
    #[test]
    fn test_priority_scheduler() {
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
        scheduler.submit_task(low_task).unwrap();
        scheduler.submit_task(high_task).unwrap();
        
        // High priority task should be returned first
        let next_task = scheduler.get_next_task().unwrap();
        if let IOTaskType::Read(page_id) = next_task.task_type {
            assert_eq!(page_id, 1);
        } else {
            panic!("Expected Read task");
        }
    }

    #[test]
    fn test_round_robin_distribution_across_workers() {
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
            scheduler.submit_task(task).unwrap();
        }

        let mut counts = vec![0usize; 3];
        for worker_id in 0..3 {
            let rx = scheduler.get_worker_receiver(worker_id);
            while let Ok(_task) = rx.try_recv() {
                counts[worker_id] += 1;
            }
        }

        assert_eq!(counts, vec![3, 3, 3]);
    }

    #[test]
    fn test_try_steal_when_other_worker_has_tasks() {
        let scheduler = WorkStealingScheduler::new(2);

        // First task should go to worker 0 via round-robin
        let (sender, _receiver) = oneshot::channel();
        let task = IOTask {
            task_type: IOTaskType::Read(42),
            priority: IOPriority::Normal,
            creation_time: Instant::now(),
            completion_callback: Some(sender),
        };
        scheduler.submit_task(task).unwrap();

        // Worker 1 attempts to steal and should get the task from worker 0
        let stolen = scheduler.try_steal_work(1);
        assert!(stolen.is_some());

        // Original worker 0 queue should now be empty
        let rx0 = scheduler.get_worker_receiver(0);
        assert!(rx0.try_recv().is_err());
    }

    #[test]
    fn test_try_steal_returns_none_when_no_tasks_available() {
        let scheduler = WorkStealingScheduler::new(2);
        let stolen = scheduler.try_steal_work(0);
        assert!(stolen.is_none());
    }

    #[test]
    fn test_priority_scheduler_fifo_within_same_priority() {
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

        scheduler.submit_task(high1).unwrap();
        scheduler.submit_task(high2).unwrap();

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

    #[test]
    fn test_priority_levels_order_high_then_normal_then_low() {
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

        scheduler.submit_task(normal).unwrap();
        scheduler.submit_task(low).unwrap();
        scheduler.submit_task(high).unwrap();

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

    #[test]
    fn test_priority_scheduler_empty_returns_none() {
        let scheduler = PriorityTaskScheduler::new();
        assert!(scheduler.get_next_task().is_none());
    }
}