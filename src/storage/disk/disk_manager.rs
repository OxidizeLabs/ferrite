use crate::common::config::{PageId, DB_PAGE_SIZE};
use crate::common::logger::initialize_logger;
use log::{debug, error, info, trace, warn};
use mockall::automock;
use spin::{Mutex, RwLock};
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
    flush_log_f: Arc<Mutex<Option<Box<dyn Future<Output=()> + Send>>>>,
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
    pub fn write_page(
        &self,
        page_id: PageId,
        page_data: &[u8; DB_PAGE_SIZE as usize],
    ) -> IoResult<()> {
        self.disk_io.read().write_page(page_id, page_data)
    }

    // Update read_page to use disk_io
    pub fn read_page(
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
    use mockall::predicate::{always, eq};
    use std::io::{Error, ErrorKind};
    use tempfile::TempDir;

    // Shared test context and utilities
    pub struct TestContext {
        disk_manager: Arc<RwLock<FileDiskManager>>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        fn new(name: &str) -> Self {
            initialize_logger();
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

            // Create disk components
            let disk_manager = Arc::new(RwLock::new(FileDiskManager::new(db_path, log_path, 10)));

            Self {
                disk_manager,
                _temp_dir: temp_dir,
            }
        }

        // Helper methods for common test operations
        fn write_test_page(&self, page_id: PageId, value: u8) -> IoResult<()> {
            let mut data = [0u8; DB_PAGE_SIZE as usize];
            data[0] = value;
            self.disk_manager.write().write_page(page_id, &data)
        }

        fn read_test_page(&self, page_id: PageId) -> IoResult<[u8; DB_PAGE_SIZE as usize]> {
            let mut data = [0u8; DB_PAGE_SIZE as usize];
            self.disk_manager.write().read_page(page_id, &mut data)?;
            Ok(data)
        }

        // Add Clone implementation helper
        fn clone_disk_manager(&self) -> Arc<RwLock<FileDiskManager>> {
            Arc::clone(&self.disk_manager)
        }
    }

    // Basic functionality tests
    mod basic_tests {
        use super::*;

        #[test]
        fn test_initialization() {
            let ctx = TestContext::new("test_initialization");
            let disk_manager = ctx.disk_manager.read();

            assert_eq!(disk_manager.get_num_flushes(), 0);
            assert_eq!(disk_manager.get_num_writes(), 0);
            assert!(!disk_manager.get_flush_state());
            assert!(!disk_manager.has_flush_log_future());
        }

        #[test]
        fn test_basic_read_write() -> IoResult<()> {
            let ctx = TestContext::new("test_basic_read_write");

            // Write test data
            ctx.write_test_page(0, 42)?;

            // Read and verify
            let data = ctx.read_test_page(0)?;
            assert_eq!(data[0], 42, "Read data doesn't match written data");

            Ok(())
        }
    }

    // Batch operation tests
    mod batch_tests {
        use super::*;

        #[test]
        fn test_batch_operations() {
            let ctx = TestContext::new("test_batch_operations");
            let disk_manager = ctx.disk_manager.write();

            // Test data preparation and batch operations
            let test_pages: Vec<_> = (0..5)
                .map(|i| {
                    let mut data = [0u8; DB_PAGE_SIZE as usize];
                    data[0] = i as u8;
                    (i as PageId, data)
                })
                .collect();

            assert!(disk_manager.write_pages_batch(&test_pages).is_ok());

            // Verify batch read
            let page_ids: Vec<_> = test_pages.iter().map(|(id, _)| *id).collect();
            let read_results = disk_manager.read_pages_batch(&page_ids).unwrap();

            for (i, page_data) in read_results.iter().enumerate() {
                assert_eq!(page_data[0], i as u8);
            }
        }
    }

    // Async operation tests
    mod async_tests {
        use super::*;

        #[tokio::test]
        async fn test_async_operations() {
            let ctx = TestContext::new("test_async_operations");
            let disk_manager = ctx.disk_manager.write();

            let mut test_data = [0u8; DB_PAGE_SIZE as usize];
            test_data[0] = 42;

            // Test async write
            assert!(disk_manager.write_page_async(0, &test_data).await.is_ok());

            // Test async read
            let read_data = disk_manager.read_page_async(0).await.unwrap();
            assert_eq!(
                read_data[0], 42,
                "Async read data doesn't match written data"
            );

            // Test error handling
            let result = disk_manager.read_page_async(PageId::MAX).await;
            assert!(
                result.is_ok(),
                "Should handle invalid page reads gracefully"
            );
            assert_eq!(
                result.unwrap(),
                [0u8; DB_PAGE_SIZE as usize],
                "Should return zeroed buffer for invalid page"
            );
        }
    }

    // Concurrency tests
    mod concurrency_tests {
        use super::*;

        #[test]
        fn test_concurrent_access() {
            let ctx = TestContext::new("test_concurrent_access");
            let thread_count = 10;
            let mut handles = vec![];
            let disk_manager = ctx.clone_disk_manager();

            for i in 0..thread_count {
                let dm = Arc::clone(&disk_manager);
                handles.push(thread::spawn(move || {
                    let mut data = [0u8; DB_PAGE_SIZE as usize];
                    data[0] = i as u8;

                    // Write data
                    {
                        let disk_mgr = dm.write();
                        assert!(disk_mgr.write_page(i as PageId, &data).is_ok());
                    }

                    // Read and verify data
                    {
                        let mut read_buffer = [0u8; DB_PAGE_SIZE as usize];
                        let disk_mgr = dm.write();
                        assert!(disk_mgr.read_page(i as PageId, &mut read_buffer).is_ok());
                        assert_eq!(
                            read_buffer[0], i as u8,
                            "Data mismatch in concurrent operation"
                        );
                    }
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }
        }

        #[test]
        fn test_concurrent_batch_operations() {
            let ctx = TestContext::new("test_concurrent_batch_operations");
            let disk_manager = ctx.clone_disk_manager();
            let batch_size = 5;
            let thread_count = 4;
            let mut handles = vec![];

            for t in 0..thread_count {
                let dm = Arc::clone(&disk_manager);
                handles.push(thread::spawn(move || {
                    // Prepare batch data
                    let start_page = t * batch_size;
                    let test_pages: Vec<_> = (0..batch_size)
                        .map(|i| {
                            let page_id = start_page + i;
                            let mut data = [0u8; DB_PAGE_SIZE as usize];
                            data[0] = page_id as u8;
                            (page_id as PageId, data)
                        })
                        .collect();

                    // Write batch
                    {
                        let disk_mgr = dm.write();
                        assert!(disk_mgr.write_pages_batch(&test_pages).is_ok());
                    }

                    // Read and verify batch
                    {
                        let disk_mgr = dm.write();
                        let page_ids: Vec<_> = test_pages.iter().map(|(id, _)| *id).collect();
                        let read_results = disk_mgr.read_pages_batch(&page_ids).unwrap();

                        for (i, page_data) in read_results.iter().enumerate() {
                            assert_eq!(
                                page_data[0],
                                (start_page + i) as u8,
                                "Batch data mismatch in thread {}",
                                t
                            );
                        }
                    }
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }
        }
    }

    // Error handling and edge cases
    mod error_tests {
        use super::*;

        #[test]
        fn test_invalid_page_reads() {
            let ctx = TestContext::new("test_invalid_page_reads");
            let disk_manager = ctx.disk_manager.write();
            let mut buffer = [0u8; DB_PAGE_SIZE as usize];

            // Test reading PageId::MAX (should return zeroed buffer)
            let result = disk_manager.read_page(PageId::MAX, &mut buffer);
            assert!(result.is_ok(), "Should handle overflow gracefully");
            assert_eq!(
                buffer, [0u8; DB_PAGE_SIZE as usize],
                "Should return zeroed buffer for invalid page"
            );

            // Test reading beyond EOF
            let result = disk_manager.read_page(1_000_000, &mut buffer);
            assert!(result.is_ok(), "Should handle EOF gracefully");
            assert_eq!(
                buffer, [0u8; DB_PAGE_SIZE as usize],
                "Should return zeroed buffer for EOF"
            );
        }

        #[test]
        fn test_invalid_page_writes() {
            let ctx = TestContext::new("test_invalid_page_writes");
            let disk_manager = ctx.disk_manager.write();
            let test_data = [42u8; DB_PAGE_SIZE as usize];

            // Test writing to PageId::MAX (should fail)
            let result = disk_manager.write_page(PageId::MAX, &test_data);
            assert!(result.is_err(), "Should fail on overflow");
            assert_eq!(
                result.unwrap_err().kind(),
                ErrorKind::InvalidInput,
                "Should return InvalidInput error for overflow"
            );
        }

        #[test]
        fn test_mock_disk_errors() {
            let mut mock_disk_io = MockDiskIO::new();
            mock_disk_io
                .expect_write_page()
                .with(eq(0), always())
                .returning(|_, _| Err(Error::new(ErrorKind::WriteZero, "Disk full")));

            let result = mock_disk_io.write_page(0, &[0u8; DB_PAGE_SIZE as usize]);
            assert!(result.is_err());
            assert_eq!(result.unwrap_err().kind(), ErrorKind::WriteZero);
        }
    }

    // Metrics tests
    mod metrics_tests {
        use super::*;

        #[test]
        fn test_metrics_recording() {
            let ctx = TestContext::new("test_metrics_recording");
            let disk_manager = ctx.disk_manager.write();

            let start_time = Instant::now();
            disk_manager.record_metrics("read", start_time, 1024, false);
            disk_manager.record_metrics("write", start_time, 2048, true);

            assert_eq!(
                disk_manager
                    .metrics
                    .read_throughput_bytes
                    .load(Ordering::Relaxed),
                1024
            );
            assert_eq!(
                disk_manager
                    .metrics
                    .write_throughput_bytes
                    .load(Ordering::Relaxed),
                2048
            );
            assert_eq!(disk_manager.metrics.write_errors.load(Ordering::Relaxed), 1);
            assert_eq!(disk_manager.metrics.read_errors.load(Ordering::Relaxed), 0);
        }
    }

    // Logging tests
    mod logging_tests {
        use super::*;
        use std::sync::Once;

        static INIT: Once = Once::new();

        fn setup() {
            INIT.call_once(|| {
                initialize_logger();
            });
        }

        #[test]
        fn test_metrics_logging() {
            setup();
            let ctx = TestContext::new("test_metrics_logging");
            let disk_manager = ctx.disk_manager.write();

            // Generate some activity
            let mut test_data = [0u8; DB_PAGE_SIZE as usize];
            test_data[0] = 42;

            // Write some pages
            for i in 0..5 {
                assert!(disk_manager.write_page(i, &test_data).is_ok());
            }

            // Read some pages
            let mut read_buffer = [0u8; DB_PAGE_SIZE as usize];
            for i in 0..5 {
                assert!(disk_manager.read_page(i, &mut read_buffer).is_ok());
            }

            // Force an error by reading an invalid page
            assert!(disk_manager
                .read_page(PageId::MAX, &mut read_buffer)
                .is_ok());

            // Log metrics
            disk_manager.log_metrics();

            // Verify that we have at least one error (from the invalid page read)
            assert_eq!(
                disk_manager.metrics.read_errors.load(Ordering::Relaxed),
                1,
                "Should have recorded one read error from invalid page access"
            );

            // Check health (should fail due to the error we just verified)
            assert!(
                !disk_manager.check_health(),
                "Health check should fail due to recorded errors"
            );
        }

        #[test]
        fn test_performance_logging() {
            setup();
            let ctx = TestContext::new("test_performance_logging");
            let disk_manager = ctx.disk_manager.write();

            // Prepare batch operation
            let test_pages: Vec<_> = (0..10)
                .map(|i| {
                    let mut data = [0u8; DB_PAGE_SIZE as usize];
                    data[0] = i as u8;
                    (i as PageId, data)
                })
                .collect();

            // Execute batch write with performance logging
            assert!(disk_manager.write_pages_batch(&test_pages).is_ok());

            // Verify metrics were recorded
            assert!(
                disk_manager
                    .metrics
                    .write_throughput_bytes
                    .load(Ordering::Relaxed)
                    > 0
            );
        }
    }

    // Retry tests
    mod retry_tests {
        use super::*;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        #[test]
        fn test_write_with_retry_success() {
            let ctx = TestContext::new("test_write_with_retry_success");
            let disk_manager = ctx.disk_manager.write();
            let attempts = Arc::new(AtomicUsize::new(0));
            let attempts_clone = attempts.clone();

            // Create a mock that fails twice then succeeds
            let mut mock = MockDiskIO::new();
            mock.expect_write_page().times(3).returning(move |_, _| {
                let current_attempt = attempts_clone.fetch_add(1, Ordering::SeqCst);
                if current_attempt < 2 {
                    Err(Error::new(ErrorKind::Other, "Temporary error"))
                } else {
                    Ok(())
                }
            });

            // Set the mock implementation
            disk_manager.set_disk_io(Box::new(mock));

            // Test the retry mechanism
            let test_data = [42u8; DB_PAGE_SIZE as usize];
            let result = disk_manager.write_page_with_retry(0, &test_data);

            assert!(result.is_ok(), "Write should eventually succeed");
            assert_eq!(
                attempts.load(Ordering::SeqCst),
                3,
                "Should have attempted 3 times"
            );
        }

        #[test]
        fn test_write_with_retry_failure() {
            let ctx = TestContext::new("test_write_with_retry_failure");
            let disk_manager = ctx.disk_manager.write();
            let attempts = Arc::new(AtomicUsize::new(0));
            let attempts_clone = attempts.clone();

            // Create a mock that always fails
            let mut mock = MockDiskIO::new();
            mock.expect_write_page()
                .times(FileDiskManager::MAX_RETRIES as usize)
                .returning(move |_, _| {
                    attempts_clone.fetch_add(1, Ordering::SeqCst);
                    Err(Error::new(ErrorKind::Other, "Persistent error"))
                });

            // Set the mock implementation
            disk_manager.set_disk_io(Box::new(mock));

            // Test the retry mechanism
            let test_data = [42u8; DB_PAGE_SIZE as usize];
            let result = disk_manager.write_page_with_retry(0, &test_data);

            assert!(result.is_err(), "Write should fail after max retries");
            assert_eq!(
                attempts.load(Ordering::SeqCst),
                FileDiskManager::MAX_RETRIES as usize,
                "Should have attempted exactly MAX_RETRIES times"
            );
        }

        #[test]
        fn test_read_with_retry_success() {
            let ctx = TestContext::new("test_read_with_retry_success");
            let disk_manager = ctx.disk_manager.write();
            let attempts = Arc::new(AtomicUsize::new(0));
            let attempts_clone = attempts.clone();

            // Create a mock that fails once then succeeds
            let mut mock = MockDiskIO::new();
            mock.expect_read_page().times(2).returning(move |_, buf| {
                let current_attempt = attempts_clone.fetch_add(1, Ordering::SeqCst);
                if current_attempt == 0 {
                    Err(Error::new(ErrorKind::Other, "Temporary read error"))
                } else {
                    buf[0] = 42; // Set test data
                    Ok(())
                }
            });

            // Set the mock implementation
            disk_manager.set_disk_io(Box::new(mock));

            // Test the retry mechanism
            let mut read_buffer = [0u8; DB_PAGE_SIZE as usize];
            let result = disk_manager.read_page_with_retry(0, &mut read_buffer);

            assert!(result.is_ok(), "Read should eventually succeed");
            assert_eq!(read_buffer[0], 42, "Should have read correct data");
            assert_eq!(
                attempts.load(Ordering::SeqCst),
                2,
                "Should have attempted twice"
            );
        }

        #[test]
        fn test_read_with_retry_metrics() {
            let ctx = TestContext::new("test_read_with_retry_metrics");
            let disk_manager = ctx.disk_manager.write();
            let attempts = Arc::new(AtomicUsize::new(0));
            let attempts_clone = attempts.clone();

            // Create a mock that fails once then succeeds
            let mut mock = MockDiskIO::new();
            mock.expect_read_page().times(2).returning(move |_, buf| {
                let current_attempt = attempts_clone.fetch_add(1, Ordering::SeqCst);
                if current_attempt == 0 {
                    Err(Error::new(ErrorKind::Other, "Simulated read error"))
                } else {
                    buf[0] = 42; // Set test data
                    Ok(())
                }
            });

            // Set the mock implementation
            disk_manager.set_disk_io(Box::new(mock));

            // Test the retry mechanism
            let mut read_buffer = [0u8; DB_PAGE_SIZE as usize];
            let result = disk_manager.read_page_with_retry(0, &mut read_buffer);

            assert!(result.is_ok(), "Read should eventually succeed");
            assert_eq!(read_buffer[0], 42, "Should have read correct data");

            // Verify metrics
            assert_eq!(
                disk_manager.metrics.read_errors.load(Ordering::Relaxed),
                1,
                "Should record one read error"
            );
            assert!(
                disk_manager.metrics.read_latency_ns.load(Ordering::Relaxed) > 0,
                "Should record read latency"
            );
        }

        #[test]
        fn test_retry_delay() {
            let ctx = TestContext::new("test_retry_delay");
            let disk_manager = ctx.disk_manager.write();
            let start_time = Instant::now();

            // Create a mock that fails twice then succeeds
            let mut mock = MockDiskIO::new();
            mock.expect_write_page()
                .times(FileDiskManager::MAX_RETRIES as usize)
                .returning(|_, _| Err(Error::new(ErrorKind::Other, "Temporary error")));

            // Set the mock implementation
            disk_manager.set_disk_io(Box::new(mock));

            // Attempt write that will fail
            let test_data = [42u8; DB_PAGE_SIZE as usize];
            let result = disk_manager.write_page_with_retry(0, &test_data);

            // Verify that appropriate time has elapsed
            let elapsed = start_time.elapsed();
            assert!(result.is_err(), "Write should fail after retries");
            assert!(
                elapsed
                    >= Duration::from_millis(
                    FileDiskManager::RETRY_DELAY_MS * (FileDiskManager::MAX_RETRIES - 1) as u64
                ),
                "Should have waited appropriate retry delay time"
            );
        }
    }
}
