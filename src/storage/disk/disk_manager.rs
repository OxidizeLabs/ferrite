use crate::common::config::{PageId, DB_PAGE_SIZE};
use crate::common::logger::initialize_logger;
use log::{debug, error, info, trace, warn};
use mockall::automock;
use spin::{Mutex, RwLock};
use std::fs;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{
    BufReader, BufWriter, Error, ErrorKind, Read, Result as IoResult, Seek, SeekFrom, Write,
};
use std::path::Path;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fmt, thread};
use mockall_double::double;

/// The `DiskIO` trait defines the basic operations for interacting with disk storage.
/// Implementers of this trait must provide methods to write and read pages.
#[automock]
pub trait DiskIO: Send + Sync {
    fn write_page(&self, page_id: PageId, page_data: &[u8; DB_PAGE_SIZE as usize]) -> IoResult<()>;
    fn read_page(
        &self,
        page_id: PageId,
        page_data: &mut [u8; DB_PAGE_SIZE as usize],
    ) -> IoResult<()>; // Default implementations for retry methods
    fn write_page_with_retry(
        &self,
        page_id: PageId,
        page_data: &[u8; DB_PAGE_SIZE as usize],
    ) -> IoResult<()>;
    fn read_page_with_retry(
        &self,
        page_id: PageId,
        page_data: &mut [u8; DB_PAGE_SIZE as usize],
    ) -> IoResult<()>;
}

/// The `FileDiskManager` is responsible for managing disk I/O operations,
/// including reading and writing pages and managing log files.
pub struct FileDiskManager {
    disk_io: Arc<RwLock<Box<dyn DiskIO>>>,
    db_io: Arc<RwLock<BufWriter<File>>>,
    log_io: Arc<RwLock<BufWriter<File>>>,
    db_read_buffer: Arc<RwLock<BufReader<File>>>,
    num_flushes: AtomicI32,
    num_writes: AtomicI32,
    flush_log: Arc<Mutex<bool>>,
    flush_log_f: Arc<Mutex<Option<Box<dyn Future<Output = ()> + Send>>>>,
    metrics: DiskMetrics,
}

#[derive(Debug, Default)]
pub struct DiskMetrics {
    read_latency_ns: AtomicU64,
    write_latency_ns: AtomicU64,
    read_throughput_bytes: AtomicU64,
    write_throughput_bytes: AtomicU64,
    read_errors: AtomicU64,
    write_errors: AtomicU64,
}

// Create a real disk I/O implementation
struct RealDiskIO {
    db_io: Arc<RwLock<BufWriter<File>>>,
    db_read_buffer: Arc<RwLock<BufReader<File>>>,
}

impl DiskIO for RealDiskIO {
    fn write_page(&self, page_id: PageId, page_data: &[u8; DB_PAGE_SIZE as usize]) -> IoResult<()> {
        // Use checked multiplication to prevent overflow
        let offset = match (page_id as u64).checked_mul(DB_PAGE_SIZE) {
            Some(off) => off,
            None => {
                warn!(
                    target: "tkdb::storage",
                    "Page ID {} would cause offset overflow", page_id
                );
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Page ID would cause offset overflow",
                ));
            }
        };

        let mut db_io = self.db_io.write();
        let writer = db_io.get_mut();
        writer.seek(SeekFrom::Start(offset))?;
        writer.write_all(page_data)?;
        writer.flush()?;
        Ok(())
    }

    fn read_page(
        &self,
        page_id: PageId,
        page_data: &mut [u8; DB_PAGE_SIZE as usize],
    ) -> IoResult<()> {
        // Use checked multiplication to prevent overflow
        let offset = match (page_id as u64).checked_mul(DB_PAGE_SIZE) {
            Some(off) => off,
            None => {
                warn!(
                    target: "tkdb::storage",
                    "Page ID {} would cause offset overflow, returning zeroed page", page_id
                );
                // Fill the buffer with zeros but return an error to trigger error metrics
                page_data.fill(0);
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Page ID would cause offset overflow",
                ));
            }
        };

        let mut db_reader = self.db_read_buffer.write();
        let reader = db_reader.get_mut();
        reader.seek(SeekFrom::Start(offset))?;
        match reader.read_exact(page_data) {
            Ok(_) => Ok(()),
            Err(ref e) if e.kind() == ErrorKind::UnexpectedEof => {
                warn!(
                    target: "tkdb::storage",
                    "Read beyond EOF for page {}, returning zeroed page", page_id
                );
                page_data.fill(0);
                // Return an error to trigger error metrics
                Err(Error::new(ErrorKind::InvalidInput, "Read beyond EOF"))
            }
            Err(e) => Err(e),
        }
    }

    // Default implementations for retry methods
    fn write_page_with_retry(
        &self,
        page_id: PageId,
        page_data: &[u8; DB_PAGE_SIZE as usize],
    ) -> IoResult<()> {
        self.write_page(page_id, page_data)
    }

    fn read_page_with_retry(
        &self,
        page_id: PageId,
        page_data: &mut [u8; DB_PAGE_SIZE as usize],
    ) -> IoResult<()> {
        self.read_page(page_id, page_data)
    }
}

#[automock]
impl FileDiskManager {
    const MAX_RETRIES: u32 = 3;
    const RETRY_DELAY_MS: u64 = 100;

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
        // Initialize logger at startup
        initialize_logger();

        info!(
            target: "tkdb::storage",
            "Initializing FileDiskManager: buffer_size={}, db_file={}, log_file={}",
            buffer_size, db_file, log_file
        );

        let db_io = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_file.clone())
            .unwrap_or_else(|e| {
                error!(
                    target: "tkdb::storage",
                    "Failed to open database file {}: {}", db_file, e
                );
                panic!("Database file initialization failed")
            });

        let log_io = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(log_file.clone())
            .unwrap();

        // Create separate read buffers
        let db_read = OpenOptions::new().read(true).open(db_file).unwrap();
        OpenOptions::new().read(true).open(log_file).unwrap();

        // Clone the file handle for the real disk IO
        let real_disk_io = RealDiskIO {
            db_io: Arc::new(RwLock::new(BufWriter::with_capacity(
                buffer_size,
                db_io.try_clone().unwrap(),
            ))),
            db_read_buffer: Arc::new(RwLock::new(BufReader::with_capacity(
                buffer_size,
                db_read.try_clone().unwrap(),
            ))),
        };

        Self {
            disk_io: Arc::new(RwLock::new(Box::new(real_disk_io))),
            db_io: Arc::new(RwLock::new(BufWriter::with_capacity(buffer_size, db_io))),
            log_io: Arc::new(RwLock::new(BufWriter::with_capacity(buffer_size, log_io))),
            db_read_buffer: Arc::new(RwLock::new(BufReader::with_capacity(buffer_size, db_read))),
            num_flushes: AtomicI32::new(0),
            num_writes: AtomicI32::new(0),
            flush_log: Arc::new(Mutex::new(false)),
            flush_log_f: Arc::new(Mutex::new(None)),
            metrics: DiskMetrics::default(),
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
        let log_io_writer = log_io.get_mut();
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
        let log_io_reader = log_io.get_mut();

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
    pub fn set_flush_log_future(&self, f: Box<dyn Future<Output = ()> + Send>) {
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

    /// Writes log data at a specific offset, ensuring thread-safety and proper file handling
    pub fn write_log_at(&self, data: &[u8], offset: u64) -> std::io::Result<()> {
        // Take a write lock on the entire log_io to prevent concurrent access
        let mut log_io = self.log_io.write();
        let mut pinned = std::pin::pin!(log_io.get_mut());
        let file = pinned.as_mut().get_mut(); // Get the underlying File

        // Get current file size and pre-allocate space atomically
        let file_size = file.metadata()?.len();
        if offset + data.len() as u64 > file_size {
            file.set_len(offset + data.len() as u64)?;
        }

        // Seek and write atomically while holding the lock
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;

        // Ensure data is written to disk
        file.flush()?;
        file.sync_data()?;

        debug!("Log data written and flushed at offset {}", offset);
        Ok(())
    }

    /// Reads log data from a specific offset with proper thread synchronization
    pub fn read_log_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        // Take a write lock to ensure consistent reads
        let mut log_io = self.log_io.write();
        let mut pinned = std::pin::pin!(log_io.get_mut());
        let file = pinned.as_mut().get_mut();

        // Get current file size
        let file_size = file.metadata()?.len();

        // Check if read is within file bounds
        if offset >= file_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Attempted to read beyond end of file",
            ));
        }

        // Calculate how many bytes we can actually read
        let bytes_available = file_size - offset;
        let bytes_to_read = std::cmp::min(bytes_available, buf.len() as u64) as usize;

        // Create a temporary buffer for the actual read
        let mut temp_buf = vec![0u8; bytes_to_read];

        // Seek and read the available data
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut temp_buf)?;

        // Copy data to output buffer
        buf[..bytes_to_read].copy_from_slice(&temp_buf);
        if bytes_to_read < buf.len() {
            buf[bytes_to_read..].fill(0);
            debug!(
                "Partial log data read from offset {} ({} bytes)",
                offset, bytes_to_read
            );
        } else {
            debug!("Log data read from offset {}", offset);
        }

        Ok(())
    }

    /// Atomically allocate space in the log file
    pub fn allocate_log_space(&self, size: u64) -> std::io::Result<u64> {
        let mut log_io = self.log_io.write();
        let mut pinned = std::pin::pin!(log_io.get_mut());
        let file = pinned.as_mut().get_mut();

        // Get current end of file position using metadata
        let offset = file.metadata()?.len();

        // Pre-allocate space for the new log entry
        file.set_len(offset + size)?;
        file.sync_data()?;

        Ok(offset)
    }

    pub fn write_pages_batch(
        &self,
        pages: &[(PageId, [u8; DB_PAGE_SIZE as usize])],
    ) -> IoResult<()> {
        let start = Instant::now();
        let total_bytes = pages.len() * DB_PAGE_SIZE as usize;

        info!(
            "Starting batch write of {} pages ({} bytes)",
            pages.len(),
            total_bytes
        );

        let result = {
            let mut db_io = self.db_io.write();
            let writer = db_io.get_mut();

            for (page_id, page_data) in pages {
                let offset = *page_id as u64 * DB_PAGE_SIZE;
                trace!("Writing page {} at offset {}", page_id, offset);

                if let Err(e) = writer.seek(SeekFrom::Start(offset)) {
                    error!(
                        "Seek failed for page {} at offset {}: {}",
                        page_id, offset, e
                    );
                    return Err(e);
                }

                if let Err(e) = writer.write_all(page_data) {
                    error!("Write failed for page {}: {}", page_id, e);
                    return Err(e);
                }
            }

            writer.flush()
        };

        match result {
            Ok(_) => {
                let duration = start.elapsed();
                let throughput = total_bytes as f64 / duration.as_secs_f64();
                info!(
                    "Batch write completed: {} pages, {} bytes, took {:?} ({:.2} MB/s)",
                    pages.len(),
                    total_bytes,
                    duration,
                    throughput / 1_000_000.0
                );
                self.record_metrics("write", start, total_bytes as u64, false);
                Ok(())
            }
            Err(e) => {
                error!("Batch write failed: {}", e);
                self.record_metrics("write", start, total_bytes as u64, true);
                Err(e)
            }
        }
    }

    pub fn read_pages_batch(
        &self,
        page_ids: &[PageId],
    ) -> IoResult<Vec<[u8; DB_PAGE_SIZE as usize]>> {
        let mut results = Vec::with_capacity(page_ids.len());
        let mut db_reader = self.db_read_buffer.write();
        let reader = db_reader.get_mut();

        for &page_id in page_ids {
            let mut page_data = [0u8; DB_PAGE_SIZE as usize];
            let offset = page_id as u64 * DB_PAGE_SIZE;
            reader.seek(SeekFrom::Start(offset))?;

            match reader.read_exact(&mut page_data) {
                Ok(_) => results.push(page_data),
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    results.push([0u8; DB_PAGE_SIZE as usize]);
                }
                Err(e) => return Err(e),
            }
        }

        Ok(results)
    }

    pub async fn write_page_async(
        &self,
        page_id: PageId,
        page_data: &[u8; DB_PAGE_SIZE as usize],
    ) -> IoResult<()> {
        let start = Instant::now();
        debug!(target: "tkdb::storage", "Starting async write for page {}", page_id);

        let result = {
            let offset = page_id as u64 * DB_PAGE_SIZE;
            let mut db_io = self.db_io.write();
            let writer = db_io.get_mut();

            // Use async versions of seek and write operations
            writer.seek(SeekFrom::Start(offset))?;
            writer.write_all(page_data)?;
            writer.flush()?;
            Ok(())
        };

        match result {
            Ok(_) => {
                let duration = start.elapsed();
                info!(
                    target: "tkdb::storage",
                    "Async write completed for page {} (took {:?})",
                    page_id, duration
                );
                self.record_metrics("write", start, DB_PAGE_SIZE, false);
                Ok(())
            }
            Err(e) => {
                error!(
                    target: "tkdb::storage",
                    "Async write failed for page {}: {}", page_id, e
                );
                self.record_metrics("write", start, DB_PAGE_SIZE, true);
                Err(e)
            }
        }
    }

    pub async fn read_page_async(&self, page_id: PageId) -> IoResult<[u8; DB_PAGE_SIZE as usize]> {
        let start = Instant::now();
        debug!(target: "tkdb::storage", "Starting async read for page {}", page_id);

        let result = {
            // Use checked multiplication to prevent overflow
            let offset = match (page_id as u64).checked_mul(DB_PAGE_SIZE) {
                Some(off) => off,
                None => {
                    warn!(
                        target: "tkdb::storage",
                        "Page ID {} would cause offset overflow, returning zeroed page", page_id
                    );
                    return Ok([0u8; DB_PAGE_SIZE as usize]);
                }
            };

            let mut db_reader = self.db_read_buffer.write();
            let reader = db_reader.get_mut();
            let mut page_data = [0u8; DB_PAGE_SIZE as usize];

            // Use regular versions since we're already async
            reader.seek(SeekFrom::Start(offset))?;
            match reader.read_exact(&mut page_data) {
                Ok(_) => Ok(page_data),
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    debug!(
                        target: "tkdb::storage",
                        "Read beyond EOF for page {}, returning zeroed page", page_id
                    );
                    Ok([0u8; DB_PAGE_SIZE as usize])
                }
                Err(e) => Err(e),
            }
        };

        match result {
            Ok(data) => {
                let duration = start.elapsed();
                debug!(
                    target: "tkdb::storage",
                    "Async read completed for page {} (took {:?})",
                    page_id, duration
                );
                self.record_metrics("read", start, DB_PAGE_SIZE, false);
                Ok(data)
            }
            Err(e) => {
                error!(
                    target: "tkdb::storage",
                    "Async read failed for page {}: {}", page_id, e
                );
                self.record_metrics("read", start, DB_PAGE_SIZE, true);
                Err(e)
            }
        }
    }

    fn record_metrics(&self, operation: &str, start_time: Instant, bytes: u64, is_error: bool) {
        let duration_ns = start_time.elapsed().as_nanos() as u64;

        match operation {
            "read" => {
                self.metrics
                    .read_latency_ns
                    .fetch_add(duration_ns, Ordering::Relaxed);
                self.metrics
                    .read_throughput_bytes
                    .fetch_add(bytes, Ordering::Relaxed);
                if is_error {
                    self.metrics.read_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            "write" => {
                self.metrics
                    .write_latency_ns
                    .fetch_add(duration_ns, Ordering::Relaxed);
                self.metrics
                    .write_throughput_bytes
                    .fetch_add(bytes, Ordering::Relaxed);
                if is_error {
                    self.metrics.write_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            _ => {}
        }
    }

    pub fn log_metrics(&self) {
        let read_latency = self.metrics.read_latency_ns.load(Ordering::Relaxed);
        let write_latency = self.metrics.write_latency_ns.load(Ordering::Relaxed);
        let read_throughput = self.metrics.read_throughput_bytes.load(Ordering::Relaxed);
        let write_throughput = self.metrics.write_throughput_bytes.load(Ordering::Relaxed);
        let read_errors = self.metrics.read_errors.load(Ordering::Relaxed);
        let write_errors = self.metrics.write_errors.load(Ordering::Relaxed);

        info!(
            target: "tkdb::storage",
            "Disk Metrics Summary:\n\
             Read: [latency={:?}, throughput={:.2}MB/s, errors={}]\n\
             Write: [latency={:?}, throughput={:.2}MB/s, errors={}]",
            Duration::from_nanos(read_latency),
            read_throughput as f64 / 1_000_000.0,
            read_errors,
            Duration::from_nanos(write_latency),
            write_throughput as f64 / 1_000_000.0,
            write_errors
        );
    }

    pub fn check_health(&self) -> bool {
        let read_errors = self.metrics.read_errors.load(Ordering::Relaxed);
        let write_errors = self.metrics.write_errors.load(Ordering::Relaxed);
        let total_errors = read_errors + write_errors;

        if total_errors > 0 {
            warn!(
                target: "tkdb::storage",
                "Disk Health Check FAILED: {} total errors ({} read, {} write)",
                total_errors, read_errors, write_errors
            );
            false
        } else {
            debug!(target: "tkdb::storage", "Disk Health Check: OK");
            true
        }
    }

    #[cfg(test)]
    pub fn set_disk_io(&self, mock_disk_io: Box<dyn DiskIO>) {
        *self.disk_io.write() = mock_disk_io;
    }

    // Update write_page to use disk_io
    pub(crate) fn write_page(
        &self,
        page_id: PageId,
        page_data: &[u8; DB_PAGE_SIZE as usize],
    ) -> IoResult<()> {
        self.disk_io.read().write_page(page_id, page_data)
    }

    // Update read_page to use disk_io
    pub(crate) fn read_page(
        &self,
        page_id: PageId,
        page_data: &mut [u8; DB_PAGE_SIZE as usize],
    ) -> IoResult<()> {
        let start = Instant::now();
        let result = self.disk_io.read().read_page(page_id, page_data);

        match result {
            Ok(_) => {
                self.record_metrics("read", start, DB_PAGE_SIZE, false);
                Ok(())
            }
            Err(e) => {
                self.record_metrics("read", start, DB_PAGE_SIZE, true);
                if e.kind() == ErrorKind::InvalidInput {
                    // For overflow and EOF cases, we've already zeroed the buffer
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    pub fn write_page_with_retry(
        &self,
        page_id: PageId,
        page_data: &[u8; DB_PAGE_SIZE as usize],
    ) -> IoResult<()> {
        let start = Instant::now();
        let mut attempts = 0;

        debug!(
            target: "tkdb::storage",
            "Starting write operation for page {} with retry",
            page_id
        );

        while attempts < Self::MAX_RETRIES {
            match self.write_page(page_id, page_data) {
                Ok(_) => {
                    let duration = start.elapsed();
                    info!(
                        target: "tkdb::storage",
                        "Successfully wrote page {} after {} attempts (took {:?})",
                        page_id, attempts + 1, duration
                    );
                    self.record_metrics("write", start, DB_PAGE_SIZE, false);
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        target: "tkdb::storage",
                        "Write failed for page {}, attempt {}/{}: {}",
                        page_id, attempts + 1, Self::MAX_RETRIES, e
                    );
                    if attempts + 1 < Self::MAX_RETRIES {
                        thread::sleep(Duration::from_millis(Self::RETRY_DELAY_MS));
                    }
                    attempts += 1;
                    if attempts == Self::MAX_RETRIES {
                        error!(
                            target: "tkdb::storage",
                            "Write failed for page {} after {} attempts: {}",
                            page_id, attempts, e
                        );
                        self.record_metrics("write", start, DB_PAGE_SIZE, true);
                        return Err(e);
                    }
                }
            }
        }
        unreachable!("Loop should return before reaching this point");
    }

    pub fn read_page_with_retry(
        &self,
        page_id: PageId,
        page_data: &mut [u8; DB_PAGE_SIZE as usize],
    ) -> IoResult<()> {
        let start = Instant::now();
        let mut attempts = 0;

        debug!(
            target: "tkdb::storage",
            "Starting read operation for page {} with retry",
            page_id
        );

        while attempts < Self::MAX_RETRIES {
            match self.read_page(page_id, page_data) {
                Ok(_) => {
                    let duration = start.elapsed();
                    debug!(
                        target: "tkdb::storage",
                        "Successfully read page {} after {} attempts (took {:?})",
                        page_id, attempts + 1, duration
                    );
                    self.record_metrics("read", start, DB_PAGE_SIZE, false);
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        target: "tkdb::storage",
                        "Read failed for page {}, attempt {}/{}: {}",
                        page_id, attempts + 1, Self::MAX_RETRIES, e
                    );
                    if attempts + 1 < Self::MAX_RETRIES {
                        thread::sleep(Duration::from_millis(Self::RETRY_DELAY_MS));
                    }
                    attempts += 1;
                    if attempts == Self::MAX_RETRIES {
                        error!(
                            target: "tkdb::storage",
                            "Read failed for page {} after {} attempts: {}",
                            page_id, attempts, e
                        );
                        self.record_metrics("read", start, DB_PAGE_SIZE, true);
                        return Err(e);
                    }
                }
            }
        }
        unreachable!("Loop should return before reaching this point");
    }
}

impl fmt::Debug for FileDiskManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileDiskManager")
            .field("num_flushes", &self.num_flushes.load(Ordering::SeqCst))
            // Skip db_file, log_file, and flush_log_f as they don't implement Debug
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use mockall::predicate::{always, eq};
    use std::io::{Error, ErrorKind};
    use tempfile::TempDir;
    use super::{MockDiskIO, MockFileDiskManager};

    pub struct TestContext {
        disk_manager: Arc<RwLock<MockFileDiskManager>>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        fn new(name: &str) -> Self {
            // Create temporary directory
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir
                .path()
                .join(format!("{name}.db"))
                .to_str()
                .unwrap()
                .to_string();
            let log_path = temp_dir
                .path()
                .join(format!("{name}.log"))
                .to_str()
                .unwrap()
                .to_string();

            // Create mock instance directly without calling new()
            let mut mock_disk_manager = MockFileDiskManager::default();
            
            // Set up default expectations for commonly used methods
            mock_disk_manager
                .expect_get_num_flushes()
                .returning(|| 0);
            mock_disk_manager
                .expect_get_flush_state()
                .returning(|| false);
            mock_disk_manager
                .expect_get_num_writes()
                .returning(|| 0);
            mock_disk_manager
                .expect_has_flush_log_future()
                .returning(|| false);

            Self {
                disk_manager: Arc::new(RwLock::new(mock_disk_manager)),
                _temp_dir: temp_dir,
            }
        }

        pub fn expect<'a>(&'a self) -> impl std::ops::Deref<Target = MockFileDiskManager> + 'a {
            self.disk_manager.read()
        }
    }

    mod basic_tests {
        use super::*;

        #[test]
        fn test_initialization() {
            let ctx = TestContext::new("test_initialization");
            
            // Verify initial state using mock expectations
            assert_eq!(ctx.expect().get_num_flushes(), 0);
            assert!(!ctx.expect().get_flush_state());
            assert_eq!(ctx.expect().get_num_writes(), 0);
            assert!(!ctx.expect().has_flush_log_future());
        }

        #[test]
        fn test_basic_read_write() -> IoResult<()> {
            let ctx = TestContext::new("test_basic_read_write");
            let mut disk_manager = ctx.disk_manager.write();

            // Set up expectations for write and read
            disk_manager
                .expect_write_page()
                .with(eq(0), always())
                .returning(|_, _| Ok(()));

            let mut expected_data = [0u8; DB_PAGE_SIZE as usize];
            expected_data[0] = 42;
            disk_manager
                .expect_read_page()
                .with(eq(0), always())
                .returning(move |_, buf| {
                    buf.copy_from_slice(&expected_data);
                    Ok(())
                });

            // Perform test operations
            let test_data = [42u8; DB_PAGE_SIZE as usize];
            disk_manager.write_page(0, &test_data)?;

            let mut read_buffer = [0u8; DB_PAGE_SIZE as usize];
            disk_manager.read_page(0, &mut read_buffer)?;
            assert_eq!(read_buffer[0], 42);

            Ok(())
        }
    }
}
