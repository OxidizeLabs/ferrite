use crate::common::config::PageId;
use crate::storage::disk::disk_manager::DiskIO;
use crate::storage::disk::disk_manager::FileDiskManager;
use crossbeam::channel::{unbounded, Receiver, Sender};
use log::{debug, error, info};
use std::collections::VecDeque;
use std::sync::Arc;
use spin::RwLock;
use std::thread;

// Define DiskRequest struct
pub struct DiskRequest {
    is_write: bool,
    data: Arc<RwLock<[u8; 4096]>>,
    page_id: PageId,
    sender: tokio::sync::oneshot::Sender<()>,
}

// Define DiskScheduler struct
pub struct DiskScheduler {
    disk_manager: Arc<FileDiskManager>,
    request_queue: Arc<RwLock<VecDeque<DiskRequest>>>,
    stop_flag: Arc<RwLock<bool>>,
    notifier: Sender<()>,
    worker_thread: Option<thread::JoinHandle<()>>,
}

impl DiskScheduler {
    pub fn new(disk_manager: Arc<FileDiskManager>) -> Self {
        let request_queue = Arc::new(RwLock::new(VecDeque::new()));
        let stop_flag = Arc::new(RwLock::new(false));
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
        data: Arc<RwLock<[u8; 4096]>>,
        page_id: PageId,
    ) -> tokio::sync::oneshot::Receiver<()> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let request = DiskRequest {
            is_write,
            data,
            page_id,
            sender,
        };

        // Lock the queue to add a request
        {
            let mut queue = self.request_queue.write();
            queue.push_back(request);
            info!(
                "Request added to queue: is_write={}, page_id={}",
                is_write, page_id
            );
        }

        // Notify the worker thread
        if let Err(err) = self.notifier.send(()) {
            info!("Failed to send notification to worker thread: {:?}", err);
        } else {
            info!("Notifier sent to worker thread");
        }

        receiver
    }

    pub fn start_worker_thread(&mut self, receiver: Receiver<()>) {
        let request_queue = Arc::clone(&self.request_queue);
        let disk_manager = Arc::clone(&self.disk_manager);
        let stop_flag = Arc::clone(&self.stop_flag);

        self.worker_thread = Some(thread::spawn(move || {
            info!(
                "Worker thread started on thread: {:?}",
                thread::current().id()
            );

            while {
                let stop_flag = stop_flag.read();
                !*stop_flag
            } {
                // Wait for notification
                info!(
                    "Worker thread waiting for notification on thread: {:?}",
                    thread::current().id()
                );
                if receiver.recv().is_err() {
                    info!(
                        "Worker thread notification receiver closed on thread: {:?}",
                        thread::current().id()
                    );
                    break;
                }
                info!(
                    "Worker thread received notification on thread: {:?}",
                    thread::current().id()
                );

                // Process request
                if let Some(request) = request_queue.write().pop_front() {
                    info!(
                        "Processing request: is_write={}, page_id={} on thread: {:?}",
                        request.is_write,
                        request.page_id,
                        thread::current().id()
                    );
                    let mut data = request.data.write();
                    if request.is_write {
                        info!("Writing to disk: page_id={}", request.page_id);
                        disk_manager.write_page(request.page_id, &data);
                    } else {
                        info!("Reading from disk: page_id={}", request.page_id);
                        disk_manager.read_page(request.page_id, &mut *data);
                    }
                    let _ = request.sender.send(());
                    info!(
                        "Request processed and response sent on thread: {:?}",
                        thread::current().id()
                    );
                }
            }

            // Process remaining requests before exiting
            while let Some(request) = request_queue.write().pop_front() {
                let mut data = request.data.write();
                if request.is_write {
                    info!("Writing to disk: page_id={}", request.page_id);
                    disk_manager.write_page(request.page_id, &data);
                } else {
                    info!("Reading from disk: page_id={}", request.page_id);
                    disk_manager.read_page(request.page_id, &mut *data);
                }
                let _ = request.sender.send(());
                info!("Request processed and response sent");
            }
        }));
    }

    pub fn shut_down(&mut self) {
        // Set the stop flag to indicate that the worker thread should stop
        {
            let mut stop_flag = self.stop_flag.write(); // Assuming stop_flag is an async lock
            *stop_flag = true; // Set the flag to true
        }

        // Notify the worker thread to exit
        if let Err(e) = self.notifier.send(()) {
            error!("Failed to send shutdown signal: {}", e);
        } else {
            info!("Shutdown signal sent");
        }

        // Wait for the worker thread to finish
        if let Some(handle) = self.worker_thread.take() {
            if let Err(e) = handle.join() {
                // Handle potential panics from the worker thread
                error!("Worker thread panicked during shutdown: {:?}", e);
            }
        }
    }
}
