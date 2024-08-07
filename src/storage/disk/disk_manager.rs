use std::future::Future;
use std::io::SeekFrom;
use log::{info, debug, trace, warn, error};
use std::path::Path;
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex as AsyncMutex;

use crate::common::config::{DB_PAGE_SIZE, PageId};
use crate::common::util::helpers::format_slice;

#[async_trait]
pub trait DiskIO: Send + Sync {
    async fn write_page(&self, page_id: PageId, page_data: &[u8; 4096]);
    async fn read_page(&self, page_id: PageId, page_data: &mut [u8; 4096]);
}

pub struct FileDiskManager {
    file_name: String,
    log_name: String,
    db_io: AsyncMutex<tokio::fs::File>,
    log_io: AsyncMutex<tokio::fs::File>,
    num_flushes: Mutex<i32>,
    num_writes: Mutex<i32>,
    flush_log: Mutex<bool>,
    flush_log_f: Mutex<Option<Box<dyn Future<Output = ()> + Send>>>,
}

impl FileDiskManager {
    pub async fn new(db_file: String, log_file: String) -> Self {
        let db_io = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_file.clone())
            .await
            .unwrap();
        let log_io = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(log_file.clone())
            .await
            .unwrap();

        Self {
            file_name: db_file,
            log_name: log_file,
            db_io: AsyncMutex::new(db_io),
            log_io: AsyncMutex::new(log_io),
            num_flushes: Mutex::new(0),
            num_writes: Mutex::new(0),
            flush_log: Mutex::new(false),
            flush_log_f: Mutex::new(None),
        }
    }

    pub async fn shut_down(&self) {
        if let Err(e) = self.db_io.lock().await.sync_all().await {
            warn!("Failed to sync db file during shutdown: {}", e);
        }
        if let Err(e) = self.log_io.lock().await.sync_all().await {
            warn!("Failed to sync log file during shutdown: {}", e);
        }
        info!("Shutdown complete");
    }

    pub async fn write_log(&self, log_data: &[u8]) {
        let mut log_io = self.log_io.lock().await;
        if let Err(e) = log_io.write_all(log_data).await {
            error!("Failed to write log data: {}", e);
            return;
        }
        if let Err(e) = log_io.flush().await {
            error!("Failed to flush log data: {}", e);
            return;
        }

        let mut num_flushes = self.num_flushes.lock().unwrap();
        *num_flushes += 1;
        debug!("Log data written and flushed");
    }

    pub async fn read_log(&self, log_data: &mut [u8], offset: u64) -> bool {
        let mut log_io = self.log_io.lock().await;
        if let Err(e) = log_io.seek(SeekFrom::Start(offset)).await {
            error!("Failed to seek log file to offset {}: {}", offset, e);
            return false;
        }
        match log_io.read_exact(log_data).await {
            Ok(_) => {
                debug!("Log data read from offset {}", offset);
                true
            }
            Err(e) => {
                warn!("Failed to read log data from offset {}: {}", offset, e);
                false
            }
        }
    }

    pub fn get_num_flushes(&self) -> i32 {
        *self.num_flushes.lock().unwrap()
    }

    pub fn get_flush_state(&self) -> bool {
        *self.flush_log.lock().unwrap()
    }

    pub fn get_num_writes(&self) -> i32 {
        *self.num_writes.lock().unwrap()
    }

    pub fn set_flush_log_future(&self, f: Box<dyn Future<Output = ()> + Send>) {
        let mut flush_log_f = self.flush_log_f.lock().unwrap();
        *flush_log_f = Some(f);
    }

    pub fn has_flush_log_future(&self) -> bool {
        self.flush_log_f.lock().unwrap().is_some()
    }

    fn get_file_size(file_name: &str) -> u64 {
        let path = Path::new(file_name);
        path.metadata().unwrap().len()
    }
}

#[async_trait]
impl DiskIO for FileDiskManager {
    async fn write_page(&self, page_id: PageId, page_data: &[u8; 4096]) {
        let offset = page_id as u64 * 4096;
        trace!("Writing page {} at offset {}", page_id, offset);
        trace!("Page data being written: {:?}", &page_data[..64]);

        let mut db_io = self.db_io.lock().await;
        if let Err(e) = db_io.seek(SeekFrom::Start(offset)).await {
            error!("Failed to seek to offset {}: {}", offset, e);
            return;
        }
        if let Err(e) = db_io.write_all(page_data).await {
            error!("Failed to write data for page {}: {}", page_id, e);
        } else {
            debug!("Successfully wrote data for page {}", page_id);
        }
    }

    async fn read_page(&self, page_id: PageId, page_data: &mut [u8; 4096]) {
        let offset = page_id as u64 * 4096;
        trace!("Reading page {} at offset {}", page_id, offset);

        let mut db_io = self.db_io.lock().await;
        if let Err(e) = db_io.seek(SeekFrom::Start(offset)).await {
            error!("Failed to seek to offset {}: {}", offset, e);
            return;
        }
        match db_io.read_exact(page_data).await {
            Ok(_) => {
                trace!("Page data read: {:?}", &page_data[..64]);
                debug!("Successfully read data for page {}", page_id);
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                page_data.fill(0);
                warn!("Page data read incomplete (EOF reached), filling with zeroes");
            }
            Err(e) => {
                error!("Unexpected error reading page: {}", e);
            }
        }
    }
}
