use crate::common::config::{Lsn, INVALID_LSN, LOG_BUFFER_SIZE};
use crate::recovery::log_record::LogRecord;
use crate::storage::disk::disk_manager::FileDiskManager;
use log::{error, info};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

/// LogManager maintains a separate thread that is awakened whenever the log buffer is full or whenever a timeout
/// happens. When the thread is awakened, the log buffer's content is written into the disk log file.
pub struct LogManager {
    next_lsn: AtomicU64,
    persistent_lsn: AtomicU64,
    log_buffer: Vec<u8>,
    flush_buffer: Vec<u8>,
    flush_thread: Option<thread::JoinHandle<()>>,
    stop_flag: Arc<RwLock<bool>>,
    disk_manager: Arc<FileDiskManager>,
}

impl LogManager {
    /// Creates a new `LogManager`.
    ///
    /// # Parameters
    /// - `disk_manager`: A reference to the disk manager.
    ///
    /// # Returns
    /// A new `LogManager` instance.
    pub fn new(disk_manager: Arc<FileDiskManager>) -> Self {
        Self {
            next_lsn: AtomicU64::new(0),
            persistent_lsn: AtomicU64::new(INVALID_LSN),
            log_buffer: vec![0; LOG_BUFFER_SIZE as usize],
            flush_buffer: vec![0; LOG_BUFFER_SIZE as usize],
            stop_flag: Arc::new(RwLock::new(false)),
            flush_thread: None,
            disk_manager,
        }
    }

    /// Runs the flush thread which writes the log buffer's content to the disk.
    pub fn run_flush_thread(&mut self) {
        let flush_buffer = self.flush_buffer.clone();
        let disk_manager = Arc::clone(&self.disk_manager);
        let stop_flag = Arc::clone(&self.stop_flag);

        self.flush_thread = Some(thread::spawn(move || {
            info!(
                "Flush thread started on thread: {:?}",
                thread::current().id()
            );

            while {
                let stop_flag = stop_flag.read();
                !*stop_flag
            } {
                // Perform flush to disk
                disk_manager
                    .write_log(&flush_buffer)
                    .expect("Failed to write log");
            }
        }));
    }

    pub fn shut_down(&mut self) {
        // Set the stop flag to indicate that the worker thread should stop
        {
            let mut stop_flag = self.stop_flag.write();
            *stop_flag = true; // Set the flag to true
        }

        // Wait for the worker thread to finish
        if let Some(handle) = self.flush_thread.take() {
            if let Err(e) = handle.join() {
                // Handle potential panics from the worker thread
                error!("Flush thread panicked during shutdown: {:?}", e);
            }
        }
    }

    /// Appends a log record to the log buffer.
    ///
    /// # Parameters
    /// - `log_record`: The log record to append.
    ///
    /// # Returns
    /// The log sequence number (LSN) of the appended log record.
    pub fn append_log_record(&mut self, log_record: &LogRecord) -> Lsn {
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);

        // Serialize log record and write to log buffer
        let log_record_bytes = log_record.to_string();
        self.log_buffer
            .extend_from_slice((&log_record_bytes).as_ref());

        lsn
    }

    /// Returns the next log sequence number (LSN).
    pub fn get_next_lsn(&self) -> Lsn {
        self.next_lsn.load(Ordering::SeqCst)
    }

    /// Returns the persistent log sequence number (LSN).
    pub fn get_persistent_lsn(&self) -> Lsn {
        self.persistent_lsn.load(Ordering::SeqCst)
    }

    /// Sets the persistent log sequence number (LSN).
    ///
    /// # Parameters
    /// - `lsn`: The log sequence number to set.
    pub fn set_persistent_lsn(&mut self, lsn: Lsn) {
        self.persistent_lsn.store(lsn, Ordering::SeqCst);
    }

    /// Returns a reference to the log buffer.
    pub fn get_log_buffer(&self) -> &[u8] {
        &self.log_buffer
    }
}
