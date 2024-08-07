use crate::common::config::PageId;
use crate::storage::disk::disk_manager::DiskIO;
use crate::storage::disk::disk_manager::FileDiskManager;
use crossbeam::channel::{unbounded, Receiver, Sender};
use log::{debug, info};
use std::collections::VecDeque;
use std::sync::Arc;
use std::thread;
use tokio::sync::Mutex;

pub struct DiskRequest {
    is_write: bool,
    data: Arc<Mutex<[u8; 4096]>>,
    page_id: PageId,
    sender: tokio::sync::oneshot::Sender<()>,
}

pub struct DiskScheduler {
    disk_manager: Arc<FileDiskManager>,
    request_queue: Arc<Mutex<VecDeque<DiskRequest>>>,
    stop_flag: Arc<Mutex<bool>>,
    notifier: Sender<()>,
    worker_thread: Option<std::thread::JoinHandle<()>>,
}

impl DiskScheduler {
    pub fn new(disk_manager: Arc<FileDiskManager>) -> Self {
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

    pub async fn schedule(
        &self,
        is_write: bool,
        data: Arc<Mutex<[u8; 4096]>>,
        page_id: PageId,
    ) -> tokio::sync::oneshot::Receiver<()> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let request = DiskRequest {
            is_write,
            data,
            page_id,
            sender,
        };

        {
            let mut queue = self.request_queue.lock().await;
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

        self.worker_thread = Some(std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async move {
                info!(
                    "Worker thread started on thread: {:?}",
                    std::thread::current().id()
                );
                while !*stop_flag.lock().await {
                    // Wait for notification
                    info!(
                        "Worker thread waiting for notification on thread: {:?}",
                        std::thread::current().id()
                    );
                    if receiver.recv().is_err() {
                        info!(
                            "Worker thread notification receiver closed on thread: {:?}",
                            std::thread::current().id()
                        );
                        break;
                    }
                    info!(
                        "Worker thread received notification on thread: {:?}",
                        std::thread::current().id()
                    );

                    // Process request
                    if let Some(request) = request_queue.lock().await.pop_front() {
                        info!(
                            "Processing request: is_write={}, page_id={} on thread: {:?}",
                            request.is_write,
                            request.page_id,
                            std::thread::current().id()
                        );
                        let mut data = request.data.lock().await;
                        if request.is_write {
                            info!("Writing to disk: page_id={}", request.page_id);
                            disk_manager.write_page(request.page_id, &data).await;
                        } else {
                            info!("Reading from disk: page_id={}", request.page_id);
                            disk_manager.read_page(request.page_id, &mut *data).await;
                        }
                        let _ = request.sender.send(());
                        info!(
                            "Request processed and response sent on thread: {:?}",
                            std::thread::current().id()
                        );
                    }
                }

                // Process remaining requests before exiting
                while let Some(request) = request_queue.lock().await.pop_front() {
                    let mut data = request.data.lock().await;
                    if request.is_write {
                        info!("Writing to disk: page_id={}", request.page_id);
                        disk_manager.write_page(request.page_id, &data).await;
                    } else {
                        info!("Reading from disk: page_id={}", request.page_id);
                        disk_manager.read_page(request.page_id, &mut *data).await;
                    }
                    let _ = request.sender.send(());
                    info!("Request processed and response sent");
                }
            });
        }));
    }

    pub async fn shut_down(&mut self) {
        {
            let mut stop = self.stop_flag.lock().await;
            *stop = true;
        }
        // If the send fails, the receiver is probably already dropped, which is fine.
        let _ = self.notifier.send(()); // Notify the worker thread to exit
        info!("Shutdown signal sent");

        // Wait for the worker thread to finish
        if let Some(handle) = self.worker_thread.take() {
            handle.join().unwrap();
        }
    }
}
