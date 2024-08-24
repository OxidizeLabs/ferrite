use crate::common::config::{PageId, DB_PAGE_SIZE};
use log::{debug, error, info, trace, warn};
use spin::{Mutex, RwLock};
use std::fs::File;
use std::fs::OpenOptions;
use std::future::Future;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Result as IoResult;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

/// The `DiskIO` trait defines the basic operations for interacting with disk storage.
/// Implementers of this trait must provide methods to write and read pages.
pub trait DiskIO: Send + Sync {
    fn write_page(&self, page_id: PageId, page_data: &[u8; DB_PAGE_SIZE]) -> Result<(), std::io::Error>;
    fn read_page(&self, page_id: PageId, page_data: &mut [u8; DB_PAGE_SIZE]) -> Result<(), std::io::Error>;
}

/// The `FileDiskManager` is responsible for managing disk I/O operations,
/// including reading and writing pages and managing log files.
pub struct FileDiskManager {
    file_name: String,
    log_name: String,
    db_io: Arc<RwLock<BufWriter<File>>>,
    log_io: Arc<RwLock<BufReader<File>>>,
    num_flushes: AtomicI32,
    num_writes: AtomicI32,
    flush_log: Arc<Mutex<bool>>,
    flush_log_f: Arc<Mutex<Option<Box<dyn Future<Output=()> + Send>>>>,
}

impl FileDiskManager {
    /// Creates a new instance of the `FileDiskManager`.
    ///
    /// # Arguments
    ///
    /// * `db_file` - The path to the database file.
    /// * `log_file` - The path to the log file.
    ///
    /// # Returns
    ///
    /// A new `FileDiskManager` instance.
    pub fn new(db_file: String, log_file: String, buffer_size: usize) -> Self {
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
            db_io: Arc::new(RwLock::new(BufWriter::with_capacity(buffer_size, db_io))),
            log_io: Arc::new(RwLock::new(BufReader::new(log_io))),
            num_flushes: AtomicI32::new(0),
            num_writes: AtomicI32::new(0),
            flush_log: Arc::new(Mutex::new(false)),
            flush_log_f: Arc::new(Mutex::new(None)),
        }
    }

    /// Shuts down the `FileDiskManager` by flushing any buffered data to disk.
    ///
    /// This method ensures that any pending writes are completed before shutdown.
    pub fn shut_down(&self) -> IoResult<()> {
        let mut db_io = self.db_io.write();
        db_io.flush()?;
        let _log_io = self.log_io.write();
        info!("Shutdown complete");
        Ok(())
    }

    /// Writes log data to the log file and flushes it to disk.
    ///
    /// # Arguments
    ///
    /// * `log_data` - The log data to be written.
    pub fn write_log(&self, log_data: &[u8]) -> IoResult<()> {
        let mut log_io = self.log_io.write();
        let mut log_io_writer = log_io.get_mut();
        log_io_writer.write_all(log_data)?;
        log_io_writer.flush()?;
        log_io_writer.sync_all()?;

        self.num_flushes.fetch_add(1, Ordering::SeqCst);
        debug!("Log data written and flushed");
        Ok(())
    }

    /// Reads log data from the log file at a specified offset.
    ///
    /// # Arguments
    ///
    /// * `log_data` - A mutable buffer to store the read log data.
    /// * `offset` - The offset from which to start reading.
    ///
    /// # Returns
    ///
    /// `true` if the read was successful, `false` otherwise.
    pub fn read_log(&self, log_data: &mut [u8], offset: u64) -> IoResult<bool> {
        let mut log_io = self.log_io.write();
        let mut log_io_reader = log_io.get_mut();

        log_io_reader.seek(SeekFrom::Start(offset))?;
        match log_io_reader.read(log_data) {
            Ok(bytes_read) => {
                if bytes_read == log_data.len() {
                    debug!("Log data read from offset {}", offset);
                } else {
                    log_data[bytes_read..].fill(0);
                    warn!("Log data read incomplete (EOF reached), partially filled buffer with zeroes");
                }
                Ok(true)
            }
            Err(e) => {
                error!("Failed to read log data from offset {}: {}", offset, e);
                Ok(false)
            }
        }
    }

    /// Retrieves the number of times data has been flushed to disk.
    ///
    /// # Returns
    ///
    /// The number of flushes.
    pub fn get_num_flushes(&self) -> i32 {
        self.num_flushes.load(Ordering::SeqCst)
    }

    /// Checks whether a log flush is currently pending.
    ///
    /// # Returns
    ///
    /// `true` if a flush is pending, `false` otherwise.
    pub fn get_flush_state(&self) -> bool {
        *self.flush_log.lock()
    }

    /// Retrieves the number of writes performed by the disk manager.
    ///
    /// # Returns
    ///
    /// The number of writes.
    pub fn get_num_writes(&self) -> i32 {
        self.num_writes.load(Ordering::SeqCst)
    }

    /// Sets a future that will be executed when a log flush is triggered.
    ///
    /// # Arguments
    ///
    /// * `f` - A future to be executed when a log flush occurs.
    pub fn set_flush_log_future(&self, f: Box<dyn Future<Output=()> + Send>) {
        let mut flush_log_f = self.flush_log_f.lock();
        *flush_log_f = Some(f);
    }

    /// Checks whether a flush log future is currently set.
    ///
    /// # Returns
    ///
    /// `true` if a flush log future is set, `false` otherwise.
    pub fn has_flush_log_future(&self) -> bool {
        self.flush_log_f.lock().is_some()
    }

    /// Retrieves the size of a specified file.
    ///
    /// # Arguments
    ///
    /// * `file_name` - The name of the file.
    ///
    /// # Returns
    ///
    /// The size of the file in bytes.
    pub fn get_file_size(file_name: &str) -> IoResult<u64> {
        let path = Path::new(file_name);
        Ok(path.metadata()?.len())
    }
}

impl DiskIO for FileDiskManager {
    fn write_page(&self, page_id: PageId, page_data: &[u8; DB_PAGE_SIZE]) -> Result<(), std::io::Error> {
        let offset = page_id as u64 * DB_PAGE_SIZE as u64;
        trace!("Writing page {} at offset {}", page_id, offset);

        let mut db_io = self.db_io.write();
        let mut db_io_writer = db_io.get_mut();
        db_io_writer.seek(SeekFrom::Start(offset))?;
        db_io_writer.write_all(page_data)?;
        db_io_writer.flush()?;
        self.num_writes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn read_page(&self, page_id: PageId, page_data: &mut [u8; DB_PAGE_SIZE]) -> IoResult<()> {
        let offset = page_id as u64 * DB_PAGE_SIZE as u64;
        let mut db_io = self.db_io.write();
        let mut db_io_reader = db_io.get_mut();

        db_io_reader.seek(SeekFrom::Start(offset))?;

        match db_io_reader.read_exact(page_data) {
            Ok(_) => {
                // Successfully read the entire page.
                Ok(())
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // If EOF is reached, fill the remaining buffer with zeros.
                let bytes_read = db_io_reader.stream_position()? - offset;
                page_data[bytes_read as usize..].fill(0);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}
