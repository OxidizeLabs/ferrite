use async_trait::async_trait;
use log::{debug, error, info, trace, warn};
use std::fs::OpenOptions;
use std::io::{SeekFrom, Read, Write, Seek};
use std::path::Path;
use spin::Mutex;
use std::sync::Arc;
use std::io::BufReader;
use std::io::BufWriter;
use std::fs::File;
use std::future::Future;
use futures::AsyncReadExt;
use crate::common::config::{PageId, DB_PAGE_SIZE};

pub trait DiskIO: Send + Sync {
    fn write_page(&self, page_id: PageId, page_data: &[u8; 4096]);
    fn read_page(&self, page_id: PageId, page_data: &mut [u8; 4096]);
}

pub struct FileDiskManager {
    file_name: String,
    log_name: String,
    db_io: Arc<Mutex<BufWriter<File>>>,
    log_io: Arc<Mutex<BufReader<File>>>,
    num_flushes: Arc<Mutex<i32>>,
    num_writes: Arc<Mutex<i32>>,
    flush_log: Arc<Mutex<bool>>,
    flush_log_f: Arc<Mutex<Option<Box<dyn Future<Output = ()> + Send>>>>,
}

impl FileDiskManager {
    pub fn new(db_file: String, log_file: String) -> Self {
        let db_io = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_file.clone())
            .unwrap();
        let log_io = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(log_file.clone())
            .unwrap();

        Self {
            file_name: db_file,
            log_name: log_file,
            db_io: Arc::new(Mutex::new(BufWriter::new(db_io))),
            log_io: Arc::new(Mutex::new(BufReader::new(log_io))),
            num_flushes: Arc::new(Mutex::new(0)),
            num_writes: Arc::new(Mutex::new(0)),
            flush_log: Arc::new(Mutex::new(false)),
            flush_log_f: Arc::new(Mutex::new(None)),
        }
    }

    pub fn shut_down(&self) {
        let mut db_io = self.db_io.lock();
        if let Err(e) = db_io.flush() {
            warn!("Failed to flush db file during shutdown: {}", e);
        }
        let mut log_io = self.log_io.lock();
        // BufReader does not need to be explicitly flushed
        info!("Shutdown complete");
    }

    pub fn write_log(&self, log_data: &[u8]) {
        let mut log_io = self.log_io.lock();
        let mut log_io_writer = log_io.get_mut(); // Access the underlying writer
        if let Err(e) = log_io_writer.write_all(log_data) {
            error!("Failed to write log data: {}", e);
            return;
        }
        if let Err(e) = log_io_writer.flush() {
            error!("Failed to flush log data: {}", e);
            return;
        }

        let mut num_flushes = self.num_flushes.lock();
        *num_flushes += 1;
        debug!("Log data written and flushed");
    }

    pub fn read_log(&self, log_data: &mut [u8], offset: u64) -> bool {
        let mut log_io = self.log_io.lock();
        let mut log_io_reader = log_io.get_mut(); // Access the underlying reader
        if let Err(e) = log_io_reader.seek(SeekFrom::Start(offset)) {
            error!("Failed to seek log file to offset {}: {}", offset, e);
            return false;
        }
        match log_io_reader.read_exact(log_data) {
            Ok(_) => {
                debug!("Log data read from offset {}", offset);
                true
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    log_data.fill(0);
                    warn!("Log data read incomplete (EOF reached), filling with zeroes");
                } else {
                    error!("Failed to read log data from offset {}: {}", offset, e);
                }
                false
            }
        }
    }

    pub fn get_num_flushes(&self) -> i32 {
        *self.num_flushes.lock()
    }

    pub fn get_flush_state(&self) -> bool {
        *self.flush_log.lock()
    }

    pub fn get_num_writes(&self) -> i32 {
        *self.num_writes.lock()
    }

    pub fn set_flush_log_future(&self, f: Box<dyn Future<Output = ()> + Send>) {
        let mut flush_log_f = self.flush_log_f.lock();
        *flush_log_f = Some(f);
    }

    pub fn has_flush_log_future(&self) -> bool {
        self.flush_log_f.lock().is_some()
    }

    fn get_file_size(file_name: &str) -> u64 {
        let path = Path::new(file_name);
        path.metadata().unwrap().len()
    }
}

#[async_trait::async_trait]
impl DiskIO for FileDiskManager {
    fn write_page(&self, page_id: PageId, page_data: &[u8; 4096]) {
        let offset = page_id as u64 * 4096;
        trace!("Writing page {} at offset {}", page_id, offset);
        trace!("Page data being written: {:?}", &page_data[..64]);

        let mut db_io = self.db_io.lock();
        let mut db_io_writer = db_io.get_mut(); // Access the underlying writer
        if let Err(e) = db_io_writer.seek(SeekFrom::Start(offset)) {
            error!("Failed to seek to offset {}: {}", offset, e);
            return;
        }
        if let Err(e) = db_io_writer.write_all(page_data) {
            error!("Failed to write data for page {}: {}", page_id, e);
        } else {
            debug!("Successfully wrote data for page {}", page_id);
        }
    }

    fn read_page(&self, page_id: PageId, page_data: &mut [u8; 4096]) {
        let offset = page_id as u64 * 4096;
        trace!("Reading page {} at offset {}", page_id, offset);

        let mut db_io = self.db_io.lock();
        let mut db_io_reader = db_io.get_mut(); // Access the underlying reader
        if let Err(e) = db_io_reader.seek(SeekFrom::Start(offset)) {
            error!("Failed to seek to offset {}: {}", offset, e);
            return;
        }
        match db_io_reader.read_exact(page_data) {
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
