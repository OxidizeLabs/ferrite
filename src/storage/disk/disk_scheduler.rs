use std::collections::VecDeque;
use std::sync::Arc;
use std::thread;
use spin::Mutex;
use tokio::sync::oneshot;
use crossbeam::channel::{unbounded, Receiver, Sender};
use log::info;
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
    request_queue: Arc<Mutex<VecDeque<DiskRequest>>>,
    stop_flag: Arc<Mutex<bool>>,
    notifier: Sender<()>,
    worker_thread: Option<thread::JoinHandle<()>>,
}

impl DiskScheduler {
    pub fn new(disk_manager: Arc<DiskManager>) -> Self {
        let request_queue = Arc::new(Mutex::new(VecDeque::new()));
        let stop_flag = Arc::new(Mutex::new(false));
        let (notifier, receiver) = unbounded();
        let mut scheduler = DiskScheduler {
            disk_manager,
            request_queue: Arc::clone(&request_queue),
            stop_flag: Arc::clone(&stop_flag),
            notifier,
            worker_thread: None,
        };
        scheduler.start_worker_thread(receiver);
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

        {
            let mut queue = self.request_queue.lock();
            queue.push_back(request);
        }

        // If the send fails, the receiver is probably already dropped, which is fine.
        let _ = self.notifier.send(());

        receiver
    }

    fn start_worker_thread(&mut self, receiver: Receiver<()>) {
        let request_queue = Arc::clone(&self.request_queue);
        let disk_manager = Arc::clone(&self.disk_manager);
        let stop_flag = Arc::clone(&self.stop_flag);

        self.worker_thread = Some(thread::spawn(move || {
            loop {
                // Wait for notification
                if receiver.recv().is_err() {
                    break;
                }

                // Check for stop condition
                if *stop_flag.lock() {
                    let queue = request_queue.lock();
                    if queue.is_empty() {
                        break;
                    }
                }

                let request = {
                    let mut queue = request_queue.lock();
                    queue.pop_front()
                };

                if let Some(request) = request {
                    let mut data = request.data.lock();
                    if request.is_write {
                        disk_manager.write_page(request.page_id, *data);
                    } else {
                        disk_manager.read_page(request.page_id, &mut *data);
                    }
                    let _ = request.sender.send(());
                }
            }
        }));
    }

    pub fn shut_down(&self) {
        {
            let mut stop = self.stop_flag.lock();
            *stop = true;
        }
        // If the send fails, the receiver is probably already dropped, which is fine.
        let _ = self.notifier.send(()); // Notify the worker thread to exit
    }
}

impl Drop for DiskScheduler {
    fn drop(&mut self) {
        self.shut_down();
        if let Some(handle) = self.worker_thread.take() {
            if let Err(e) = handle.join() {
                info!("Failed to join worker thread: {:?}", e);
            }
        }
    }
}
