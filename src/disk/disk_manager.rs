use crate::common::config::PageId;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

pub struct DiskManager {
    file_name: String,
    log_name: String,
    db_io: Mutex<File>,
    log_io: Mutex<File>,
    num_flushes: Mutex<i32>,
    num_writes: Mutex<i32>,
    flush_log: Mutex<bool>,
    flush_log_f: Mutex<Option<Box<dyn Future<Output = ()> + Send>>>,
}

impl DiskManager {
    pub fn new(db_file: &str, log_file: &str) -> Self {
        let db_io = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_file)
            .unwrap();
        let log_io = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(log_file)
            .unwrap();

        Self {
            file_name: db_file.to_string(),
            log_name: log_file.to_string(),
            db_io: Mutex::new(db_io),
            log_io: Mutex::new(log_io),
            num_flushes: Mutex::new(0),
            num_writes: Mutex::new(0),
            flush_log: Mutex::new(false),
            flush_log_f: Mutex::new(None),
        }
    }

    pub fn shut_down(&self) {
        let _ = self.db_io.lock().unwrap().sync_all();
        let _ = self.log_io.lock().unwrap().sync_all();
    }

    pub fn write_page(&self, page_id: PageId, page_data: [u8; 4096]) {
        let mut db_io = self.db_io.lock().unwrap();
        let offset = page_id as u64 * page_data.len() as u64;
        db_io.seek(SeekFrom::Start(offset)).unwrap();
        db_io.write_all(page_data.as_ref()).unwrap();
        db_io.flush().unwrap();

        let mut num_writes = self.num_writes.lock().unwrap();
        *num_writes += 1;
    }

    pub fn read_page(&self, page_id: PageId, page_data: &mut [u8]) {
        let mut db_io = self.db_io.lock().unwrap();
        let offset = page_id as u64 * page_data.len() as u64;
        db_io.seek(SeekFrom::Start(offset)).unwrap();
        match db_io.read_exact(page_data) {
            Ok(_) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // If there's an UnexpectedEof error, we simply fill the page_data with zeroes.
                page_data.fill(0);
            }
            Err(e) => panic!("Unexpected error reading page: {}", e),
        }
    }

    pub fn write_log(&self, log_data: &[u8]) {
        let mut log_io = self.log_io.lock().unwrap();
        log_io.write_all(log_data).unwrap();
        log_io.flush().unwrap();

        let mut num_flushes = self.num_flushes.lock().unwrap();
        *num_flushes += 1;
    }

    pub fn read_log(&self, log_data: &mut [u8], offset: u64) -> bool {
        let mut log_io = self.log_io.lock().unwrap();
        log_io.seek(SeekFrom::Start(offset)).unwrap();
        log_io.read_exact(log_data).is_ok()
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
