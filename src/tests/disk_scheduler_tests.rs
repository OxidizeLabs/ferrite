use std::sync::Arc;
use std::sync::Mutex;

use crate::common::config::{DB_PAGE_SIZE};
use crate::disk::disk_manager::DiskManager;
use crate::disk::disk_scheduler::{DiskRequest, DiskScheduler, DiskSchedulerPromise};


#[tokio::test]
async fn schedule_write_read_page_test() {
    let buf = Arc::new(Mutex::new([0u8; DB_PAGE_SIZE]));
    let data = Arc::new(Mutex::new([0u8; DB_PAGE_SIZE]));

    {
        let mut data_guard = data.lock().unwrap();
        data_guard.copy_from_slice(b"A test string.");
    }

    let dm = Arc::new(DiskManager::new("test.db", "test.log"));
    let disk_scheduler = DiskScheduler::new(Arc::clone(&dm));

    let (promise1, future1) = DiskSchedulerPromise::;
    let (promise2, future2) = DiskSchedulerPromise::channel();

    disk_scheduler.schedule(DiskRequest {
        is_write: true,
        data: Arc::clone(&data),
        page_id: 0,
        callback: promise1,
    });

    disk_scheduler.schedule(DiskRequest {
        is_write: false,
        data: Arc::clone(&buf),
        page_id: 0,
        callback: promise2,
    });

    assert!(future1.await.is_ok());
    assert!(future2.await.is_ok());

    let buf_guard = buf.lock().await;
    let data_guard = data.lock().await;

    assert_eq!(&*buf_guard, &*data_guard);

    // Call the DiskScheduler destructor to finish all scheduled jobs.
    drop(disk_scheduler);
    dm.shutdown();
}

