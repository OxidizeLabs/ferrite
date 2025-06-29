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
}