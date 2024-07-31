use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use tokio::sync::oneshot;

use crate::common::config::PageId;
use crate::storage::disk::disk_manager::DiskManager;

pub struct DiskRequest {
    is_write: bool,
    data: Arc<Mutex<[u8; 4096]>>,
    page_id: PageId,
    sender: oneshot::Sender<()>,
}

pub struct DiskScheduler {
    disk_manager: Arc<DiskManager>,
    request_queue: Arc<(Mutex<VecDeque<DiskRequest>>, Condvar)>,
    stop_flag: Arc<(Mutex<bool>, Condvar)>,
    worker_thread: Option<thread::JoinHandle<()>>,
}

impl DiskScheduler {
    pub fn new(disk_manager: Arc<DiskManager>) -> Self {
        let request_queue = Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));
        let stop_flag = Arc::new((Mutex::new(false), Condvar::new()));
        let mut scheduler = DiskScheduler {
            disk_manager,
            request_queue: Arc::clone(&request_queue),
            stop_flag: Arc::clone(&stop_flag),
            worker_thread: None,
        };
        scheduler.start_worker_thread();
        scheduler
    }

    pub fn schedule(
        &self,
        is_write: bool,
        data: Arc<Mutex<[u8; 4096]>>,
        page_id: PageId,
    ) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        let request = DiskRequest {
            is_write,
            data,
            page_id,
            sender,
        };

        let (lock, cvar) = &*self.request_queue;
        let mut queue = lock.lock().unwrap();
        queue.push_back(request);
        cvar.notify_one();

        receiver
    }

    fn start_worker_thread(&mut self) {
        let request_queue = Arc::clone(&self.request_queue);
        let disk_manager = Arc::clone(&self.disk_manager);
        let stop_flag = Arc::clone(&self.stop_flag);

        self.worker_thread = Some(thread::spawn(move || {
            let (lock, cvar) = &*request_queue;
            let (stop_lock, stop_cvar) = &*stop_flag;
            loop {
                let mut queue = lock.lock().unwrap();

                // Check for stop condition
                if *stop_lock.lock().unwrap() && queue.is_empty() {
                    break;
                }

                while queue.is_empty() {
                    queue = cvar.wait(queue).unwrap();

                    // Re-check for stop condition after waking up
                    if *stop_lock.lock().unwrap() && queue.is_empty() {
                        return;
                    }
                }

                if let Some(request) = queue.pop_front() {
                    drop(queue);

                    let mut data = request.data.lock().unwrap();
                    if request.is_write {
                        disk_manager.write_page(request.page_id, *data);
                    } else {
                        disk_manager.read_page(request.page_id, &mut *data);
                    }
                    let _ = request.sender.send(());

                    // queue = lock.lock().unwrap();
                }
            }

            stop_cvar.notify_all();
        }));
    }

    pub fn shut_down(&self) {
        let (stop_lock, stop_cvar) = &*self.stop_flag;
        let mut stop = stop_lock.lock().unwrap();
        *stop = true;
        stop_cvar.notify_all();

        // Notify the request queue to unblock worker thread
        let (_lock, cvar) = &*self.request_queue;
        let _ = cvar.notify_all();
    }
}

impl Drop for DiskScheduler {
    fn drop(&mut self) {
        self.shut_down();
        if let Some(handle) = self.worker_thread.take() {
            handle.join().unwrap();
        }
    }
}
