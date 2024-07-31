use std::sync::{Arc, Condvar, Mutex};
use std::sync::atomic::AtomicI64;
use std::thread;

use crate::storage::disk::disk_manager::DiskManager;

const LOG_BUFFER_SIZE: usize = 4096;  // Adjust as necessary

/// LogManager maintains a separate thread that is awakened whenever the log buffer is full or whenever a timeout
/// happens. When the thread is awakened, the log buffer's content is written into the disk log file.
pub struct LogManager {
    next_lsn: AtomicI64,
    persistent_lsn: AtomicI64,
    log_buffer: Vec<u8>,
    flush_buffer: Vec<u8>,
    latch: Mutex<()>,
    cv: Condvar,
    flush_thread: Option<thread::JoinHandle<()>>,
    disk_manager: Arc<Mutex<DiskManager>>,
}

// impl LogManager {
//     /// Creates a new `LogManager`.
//     ///
//     /// # Parameters
//     /// - `disk_manager`: A reference to the disk manager.
//     ///
//     /// # Returns
//     /// A new `LogManager` instance.
//     pub fn new(disk_manager: Arc<Mutex<DiskManager>>) -> Self {
//         Self {
//             next_lsn: AtomicI64::new(0),
//             persistent_lsn: AtomicI64::new(INVALID_LSN),
//             log_buffer: vec![0; LOG_BUFFER_SIZE],
//             flush_buffer: vec![0; LOG_BUFFER_SIZE],
//             latch: Mutex::new(()),
//             cv: Condvar::new(),
//             flush_thread: None,
//             disk_manager,
//         }
//     }
//
//     /// Runs the flush thread which writes the log buffer's content to the disk.
//     pub fn run_flush_thread(&mut self) {
//         let disk_manager = Arc::clone(&self.disk_manager);
//         let cv = &self.cv;
//         let latch = &self.latch;
//         let log_buffer = self.log_buffer.clone();
//         let flush_buffer = self.flush_buffer.clone();
//
//         self.flush_thread = Some(thread::spawn(move || {
//             loop {
//                 let _lock = latch.lock().unwrap();
//                 cv.wait(_lock).unwrap();
//
//                 // Perform flush to disk
//                 let disk_manager = disk_manager.lock().unwrap();
//                 disk_manager.write_log(&flush_buffer);
//             }
//         }));
//     }
//
//     /// Stops the flush thread.
//     pub fn stop_flush_thread(&mut self) {
//         if let Some(handle) = self.flush_thread.take() {
//             handle.join().unwrap();
//         }
//     }
//
//     /// Appends a log record to the log buffer.
//     ///
//     /// # Parameters
//     /// - `log_record`: The log record to append.
//     ///
//     /// # Returns
//     /// The log sequence number (LSN) of the appended log record.
//     pub fn append_log_record(&mut self, log_record: &LogRecord) -> Lsn {
//         let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
//         log_record.set_lsn(lsn);
//
//         // Serialize log record and write to log buffer
//         let log_record_bytes = log_record.to_string();
//         self.log_buffer.extend_from_slice(&log_record_bytes);
//
//         lsn
//     }
//
//     /// Returns the next log sequence number (LSN).
//     pub fn get_next_lsn(&self) -> Lsn {
//         self.next_lsn.load(Ordering::SeqCst)
//     }
//
//     /// Returns the persistent log sequence number (LSN).
//     pub fn get_persistent_lsn(&self) -> Lsn {
//         self.persistent_lsn.load(Ordering::SeqCst)
//     }
//
//     /// Sets the persistent log sequence number (LSN).
//     ///
//     /// # Parameters
//     /// - `lsn`: The log sequence number to set.
//     pub fn set_persistent_lsn(&mut self, lsn: Lsn) {
//         self.persistent_lsn.store(lsn, Ordering::SeqCst);
//     }
//
//     /// Returns a reference to the log buffer.
//     pub fn get_log_buffer(&self) -> &[u8] {
//         &self.log_buffer
//     }
// }
