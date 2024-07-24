use crate::common::config::PageId;
use futures::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Condvar};
use std::thread;

use crate::disk::disk_manager::DiskManager;

/// Represents a Write or Read request for the DiskManager to execute.
pub struct DiskRequest {
    /// Flag indicating whether the request is a write or a read.
    is_write: bool,

    /// Arc to a Mutex that protects the data being read/written.
    data: Arc<Mutex<[u8; 4096]>>,

    /// ID of the page being read from / written to disk.
    page_id: PageId,

    /// Callback used to signal to the request issuer when the request has been completed.
    callback: DiskSchedulerPromise,
}

/// A future that completes when a DiskRequest is done.
pub struct DiskSchedulerPromise {
    shared_state: Arc<Mutex<SharedState>>,
}

pub struct SharedState {
    completed: bool,
    waker: Option<Waker>,
}

impl Future for DiskSchedulerPromise {
    type Output = bool;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut shared_state = self.shared_state.lock().unwrap();
        if shared_state.completed {
            Poll::Ready(true)
        } else {
            shared_state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl DiskSchedulerPromise {
    fn new() -> Self {
        DiskSchedulerPromise {
            shared_state: Arc::new(Mutex::new(SharedState {
                completed: false,
                waker: None,
            })),
        }
    }

    fn complete(&self) {
        let mut shared_state = self.shared_state.lock().unwrap();
        shared_state.completed = true;
        if let Some(waker) = shared_state.waker.take() {
            waker.wake();
        }
    }
}

impl DiskRequest {
    fn new(is_write: bool, data: [u8; 4096], page_id: PageId) -> Self {
        DiskRequest {
            is_write,
            data: Arc::new(Mutex::new(data)),
            page_id,
            callback: DiskSchedulerPromise::new(),
        }
    }
}

/// The DiskScheduler schedules disk read and write operations.
pub struct DiskScheduler {
    disk_manager: Arc<DiskManager>,
    request_queue: Arc<(Mutex<VecDeque<DiskRequest>>, Condvar)>,
    background_thread: Option<thread::JoinHandle<()>>,
}

impl DiskScheduler {
    pub fn new(disk_manager: Arc<DiskManager>) -> Self {
        let request_queue = Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));
        let mut scheduler = DiskScheduler {
            disk_manager,
            request_queue: Arc::clone(&request_queue),
            background_thread: None,
        };
        scheduler.start_worker_thread();
        scheduler
    }

    /// Schedules a request for the DiskManager to execute.
    pub fn schedule(&self, r: DiskRequest) {
        let (lock, cvar) = &*self.request_queue;
        let mut queue = lock.lock().unwrap();
        queue.push_back(r);
        cvar.notify_one();
    }

    /// Background worker thread function that processes scheduled requests.
    fn start_worker_thread(&mut self) {
        let request_queue = Arc::clone(&self.request_queue);
        let disk_manager = Arc::clone(&self.disk_manager);

        self.background_thread = Some(thread::spawn(move || {
            let (lock, cvar) = &*request_queue;
            loop {
                let mut queue = lock.lock().unwrap();
                while queue.is_empty() {
                    queue = cvar.wait(queue).unwrap();
                }
                if let Some(request) = queue.pop_front() {
                    let mut data = request.data.lock().unwrap();
                    if request.is_write {
                        disk_manager.write_page(request.page_id, *data);
                    } else {
                        disk_manager.read_page(request.page_id, &mut *data);
                    }
                    request.callback.complete();
                }
            }
        }));
    }

    pub fn create_promise() -> DiskSchedulerPromise {
        DiskSchedulerPromise::new()
    }
}

impl Drop for DiskScheduler {
    fn drop(&mut self) {
        if let Some(handle) = self.background_thread.take() {
            handle.join().unwrap();
        }
    }
}
