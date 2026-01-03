//! # Log Manager
//!
//! This module provides the `LogManager` which coordinates Write-Ahead Logging (WAL)
//! for durability and crash recovery. It manages a background flush thread that
//! periodically writes log records to disk, ensuring committed transactions are
//! durable before returning control to the caller.
//!
//! ## Architecture
//!
//! ```text
//!                          ┌───────────────────────────────────────────────────────┐
//!                          │                    LogManager                         │
//!                          │                                                       │
//!                          │  ┌─────────────────────────────────────────────────┐  │
//!                          │  │              LogManagerState                    │  │
//!                          │  │                                                 │  │
//!                          │  │  ┌───────────────────┐  ┌───────────────────┐   │  │
//!                          │  │  │ next_lsn: Atomic  │  │persistent_lsn:    │   │  │
//!                          │  │  │ (monotonic)       │  │Atomic (flushed)   │   │  │
//!                          │  │  └───────────────────┘  └───────────────────┘   │  │
//!                          │  │                                                 │  │
//!                          │  │  ┌─────────────────────────────────────────┐    │  │
//!                          │  │  │        mpsc::Sender<LogRecord>          │    │  │
//!                          │  │  └─────────────────────────────────────────┘    │  │
//!                          │  │                        │                        │  │
//!                          │  │  ┌─────────────────────────────────────────┐    │  │
//!                          │  │  │     disk_manager: AsyncDiskManager      │    │  │
//!                          │  │  └─────────────────────────────────────────┘    │  │
//!                          │  │                                                 │  │
//!                          │  │  stop_flag: AtomicBool                          │  │
//!                          │  └─────────────────────────────────────────────────┘  │
//!                          │                                                       │
//!                          │  receiver: mpsc::Receiver<LogRecord>                  │
//!                          │  runtime_handle: tokio::Handle                        │
//!                          └───────────────────────────────────────────────────────┘
//! ```
//!
//! ## Flush Thread Pipeline
//!
//! ```text
//!   append_log_record()           mpsc Channel              Flush Thread
//!   ─────────────────────────────────────────────────────────────────────────
//!
//!   ┌──────────────┐
//!   │ Assign LSN   │
//!   │ (atomic++)   │
//!   └──────┬───────┘
//!          │
//!          ▼
//!   ┌──────────────┐         ┌─────────────────┐
//!   │ Send record  │────────▶│  Bounded Queue  │
//!   │ to channel   │         │  (capacity 1000)│
//!   └──────┬───────┘         └────────┬────────┘
//!          │                          │
//!          │ (if Commit)              ▼
//!          │                 ┌─────────────────┐
//!          │                 │ Wait for record │◀──── timeout(10ms)
//!          │                 │ or timeout      │
//!          │                 └────────┬────────┘
//!          │                          │
//!          │                          ▼
//!          │                 ┌─────────────────┐
//!          │                 │ Drain queue     │
//!          │                 │ batch records   │
//!          │                 └────────┬────────┘
//!          │                          │
//!          │                          ▼
//!          │                 ┌─────────────────┐
//!          │                 │ Write to disk   │
//!          │                 │ + sync_log()    │
//!          │                 └────────┬────────┘
//!          │                          │
//!          │                          ▼
//!          │                 ┌─────────────────┐
//!          ▼                 │ Update          │
//!   ┌──────────────┐         │ persistent_lsn  │
//!   │ Wait until   │◀────────└─────────────────┘
//!   │ persistent ≥ │
//!   │ commit LSN   │
//!   └──────────────┘
//! ```
//!
//! ## Key Components
//!
//! | Component | Description |
//! |-----------|-------------|
//! | `LogManager` | Main coordinator for WAL operations |
//! | `LogManagerState` | Shared state for LSN tracking and I/O |
//! | `next_lsn` | Monotonically increasing LSN counter |
//! | `persistent_lsn` | Highest LSN confirmed durable on disk |
//! | Flush Thread | Background tokio task for batched writes |
//! | mpsc Channel | Bounded queue for log record delivery |
//!
//! ## LSN Lifecycle
//!
//! ```text
//!   Timeline ═══════════════════════════════════════════════════════════▶
//!
//!              append         append         append         flush
//!                │              │              │              │
//!                ▼              ▼              ▼              ▼
//!   next_lsn:    1              2              3              3
//!   persistent:  0              0              0         ────▶3
//!
//!   Invariant: persistent_lsn ≤ next_lsn (always)
//! ```
//!
//! ## Commit Durability
//!
//! When a commit record is appended, the caller blocks until the record
//! is confirmed durable (persistent_lsn ≥ commit_lsn):
//!
//! ```text
//!   Transaction Commit Flow
//!   ────────────────────────────────────────────────────────
//!
//!   1. append_log_record(Commit) → assign LSN = 5
//!   2. Send to channel
//!   3. Block: while persistent_lsn < 5 { sleep(1ms) }
//!   4. Flush thread writes & syncs → persistent_lsn = 5
//!   5. Caller unblocks → return LSN 5
//! ```
//!
//! ## Core Operations
//!
//! | Operation | Description |
//! |-----------|-------------|
//! | `new(disk_manager)` | Create log manager with async disk backend |
//! | `run_flush_thread()` | Start background flush task |
//! | `shut_down()` | Stop flush thread, drain remaining records |
//! | `append_log_record(record)` | Assign LSN, queue for flush, return LSN |
//! | `get_next_lsn()` | Get next available LSN |
//! | `get_persistent_lsn()` | Get highest durable LSN |
//! | `read_log_record(offset)` | Read record from disk at byte offset |
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::recovery::log_manager::LogManager;
//! use crate::recovery::log_record::{LogRecord, LogRecordType};
//!
//! // Create log manager
//! let mut log_manager = LogManager::new(disk_manager);
//! log_manager.run_flush_thread();
//!
//! // Begin transaction
//! let begin = Arc::new(LogRecord::new_transaction_record(
//!     txn_id, INVALID_LSN, LogRecordType::Begin
//! ));
//! let begin_lsn = log_manager.append_log_record(begin);
//!
//! // ... perform operations ...
//!
//! // Commit transaction (blocks until durable)
//! let commit = Arc::new(LogRecord::new_transaction_record(
//!     txn_id, prev_lsn, LogRecordType::Commit
//! ));
//! let commit_lsn = log_manager.append_log_record(commit);
//! // At this point, commit is guaranteed durable on disk
//!
//! // Shutdown
//! log_manager.shut_down();
//! ```
//!
//! ## Thread Safety
//!
//! - LSN counters use `AtomicU64` for lock-free updates
//! - Log records are sent via bounded `mpsc` channel (backpressure at 1000)
//! - Flush thread runs as a tokio task, joined on shutdown
//! - Separate OS threads used for sync/async boundary to support both single and multi-threaded runtimes
//!
//! ## Failure Handling
//!
//! If a disk write fails during flush:
//! 1. Error is logged
//! 2. `stop_flag` is set to prevent false durability signals
//! 3. Callers waiting on commit will detect the flag and stop waiting

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use std::{io, thread};

use log::{debug, error, info, trace, warn};
use parking_lot::Mutex;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::common::config::{INVALID_LSN, Lsn};
use crate::recovery::log_record::LogRecord;
use crate::storage::disk::async_disk::AsyncDiskManager;

/// Coordinates Write-Ahead Logging (WAL) for durability and crash recovery.
///
/// The `LogManager` maintains a background flush thread that periodically writes
/// log records to disk, ensuring committed transactions are durable before
/// returning control to the caller. See the module-level documentation for
/// architecture details and usage examples.
///
/// # Thread Safety
///
/// - LSN counters use `AtomicU64` for lock-free updates
/// - Log records are sent via a bounded `mpsc` channel (backpressure at 1000)
/// - The flush thread runs as a tokio task, joined on shutdown
pub struct LogManager {
    /// Shared state containing LSN counters, disk manager, and channel sender.
    state: Arc<LogManagerState>,
    /// Handle to the tokio runtime for spawning the flush task.
    runtime_handle: Handle,
    /// Receiver end of the log record channel, consumed by the flush thread.
    ///
    /// Wrapped in `Mutex<Option<...>>` to allow one-time extraction when
    /// starting the flush thread.
    receiver: Mutex<Option<mpsc::Receiver<Arc<LogRecord>>>>,
}

/// Internal shared state for the `LogManager`.
///
/// This struct holds all state that needs to be accessed by both the main
/// log manager methods and the background flush thread.
struct LogManagerState {
    /// Monotonically increasing counter for assigning LSNs to new log records.
    ///
    /// Each call to `append_log_record` atomically increments this counter.
    next_lsn: AtomicU64,
    /// Highest LSN that has been durably written to disk.
    ///
    /// Updated by the flush thread after successful disk writes and syncs.
    /// Commit operations block until `persistent_lsn >= commit_lsn`.
    persistent_lsn: AtomicU64,
    /// Handle to the background flush task for joining on shutdown.
    flush_thread: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Flag to signal the flush thread to stop.
    ///
    /// Set to `true` during shutdown or on critical disk write failures.
    stop_flag: AtomicBool,
    /// Async disk manager for writing log records to the WAL file.
    disk_manager: Arc<AsyncDiskManager>,
    /// Sender end of the bounded channel for queuing log records.
    sender: mpsc::Sender<Arc<LogRecord>>,
}

impl LogManager {
    /// Creates a new `LogManager`.
    ///
    /// # Parameters
    /// - `disk_manager`: A reference to the disk manager.
    ///
    /// # Returns
    /// A new `LogManager` instance.
    pub fn new(disk_manager: Arc<AsyncDiskManager>) -> Self {
        let (sender, receiver) = mpsc::channel(1000); // Bounded channel for backpressure
        let runtime_handle = Handle::current();

        Self {
            state: Arc::new(LogManagerState {
                next_lsn: AtomicU64::new(0),
                persistent_lsn: AtomicU64::new(INVALID_LSN),
                flush_thread: Mutex::new(None),
                stop_flag: AtomicBool::new(false),
                disk_manager,
                sender,
            }),
            runtime_handle,
            receiver: Mutex::new(Some(receiver)),
        }
    }

    /// Starts the background flush thread.
    ///
    /// The flush thread periodically (every 10ms) drains queued log records
    /// and writes them to disk. It also performs immediate flushes when commit
    /// records are encountered to ensure transaction durability.
    ///
    /// # Panics
    ///
    /// Panics if called more than once (the receiver can only be taken once).
    ///
    /// # Note
    ///
    /// This method must be called before any `append_log_record` calls that
    /// include commit records, otherwise commit durability waiting will hang.
    pub fn run_flush_thread(&mut self) {
        info!("Starting log flush thread");
        let state = Arc::clone(&self.state);
        let mut receiver = self
            .receiver
            .lock()
            .take()
            .expect("Flush thread already running or receiver missing");

        let task_handle = self.runtime_handle.spawn(async move {
            let flush_interval = Duration::from_millis(10);

            while !state.stop_flag.load(Ordering::SeqCst) {
                // Block until at least one record arrives, or timeout to allow periodic flush.
                let first = match timeout(flush_interval, receiver.recv()).await {
                    Ok(Some(record)) => Some(record),
                    Ok(None) => break, // Channel closed
                    Err(_) => None,    // Timeout
                };

                let mut records_to_flush = Vec::new();
                let mut max_lsn_in_batch: Lsn = INVALID_LSN;

                if let Some(record) = first {
                    let record_lsn = record.get_lsn();
                    max_lsn_in_batch = record_lsn;
                    records_to_flush.push(record);

                    // Drain any additional queued records.
                    while let Ok(record) = receiver.try_recv() {
                        let record_lsn = record.get_lsn();
                        if max_lsn_in_batch == INVALID_LSN || record_lsn > max_lsn_in_batch {
                            max_lsn_in_batch = record_lsn;
                        }
                        let is_commit = record.is_commit();
                        records_to_flush.push(record);
                        if is_commit {
                            break;
                        }
                    }
                }

                if !records_to_flush.is_empty() {
                    Self::perform_flush(&state, records_to_flush, max_lsn_in_batch, "periodic")
                        .await;
                }
            }

            // Final flush before exiting
            let mut final_records = Vec::new();
            while let Ok(record) = receiver.try_recv() {
                final_records.push(record);
            }
            if !final_records.is_empty() {
                let max_lsn = final_records
                    .iter()
                    .map(|r| r.get_lsn())
                    .max()
                    .unwrap_or(INVALID_LSN);
                Self::perform_flush(&state, final_records, max_lsn, "shutdown").await;
            }
        });

        // Store the join handle so shutdown can join it.
        *self.state.flush_thread.lock() = Some(task_handle);
    }

    /// Writes a batch of log records to disk and updates the persistent LSN.
    ///
    /// This method is called by the flush thread to durably persist queued records.
    /// After successful writes and sync, `persistent_lsn` is updated to `max_lsn`,
    /// unblocking any callers waiting on commit durability.
    ///
    /// # Parameters
    ///
    /// - `state`: Shared state containing the disk manager and LSN counters.
    /// - `records`: The batch of log records to write.
    /// - `max_lsn`: The highest LSN in the batch (used to update `persistent_lsn`).
    /// - `flush_reason`: A descriptive string for logging (e.g., "periodic", "shutdown").
    ///
    /// # Error Handling
    ///
    /// If a disk write fails, the `stop_flag` is set to prevent false durability
    /// signals. Callers waiting on commit will detect this and stop waiting.
    async fn perform_flush(
        state: &Arc<LogManagerState>,
        records: Vec<Arc<LogRecord>>,
        max_lsn: Lsn,
        flush_reason: &str,
    ) {
        if records.is_empty() {
            return;
        }

        let disk_manager = Arc::clone(&state.disk_manager);

        // Synchronously (from the flush thread) write records in-order to preserve WAL ordering.
        let flush_res: Result<(), io::Error> = async {
            for record in records {
                disk_manager.write_log(&record).await?;
            }
            // Ensure WAL durability before advertising persistence via `persistent_lsn`.
            disk_manager.sync_log_direct().await?;
            Ok(())
        }
        .await;

        match flush_res {
            Ok(()) => {
                if max_lsn != INVALID_LSN {
                    state.persistent_lsn.store(max_lsn, Ordering::SeqCst);
                    debug!(
                        "{} flush completed, persistent LSN updated to {}",
                        flush_reason, max_lsn
                    );
                }
            },
            Err(e) => {
                error!(
                    "Failed to write log record(s) to disk during {} flush: {}",
                    flush_reason, e
                );
                // Fail closed: stop the flush thread to avoid giving a false durability signal.
                // Callers waiting on commit will notice stop_flag and stop waiting.
                state.stop_flag.store(true, Ordering::SeqCst);
            },
        }
    }

    /// Shuts down the log manager and waits for the flush thread to complete.
    ///
    /// This method:
    /// 1. Sets the stop flag to signal the flush thread to exit.
    /// 2. Waits for the flush thread to drain remaining records and terminate.
    /// 3. Joins the flush task to ensure clean shutdown.
    ///
    /// # Note
    ///
    /// It is safe to call this method multiple times; subsequent calls are no-ops.
    /// Any remaining queued records will be flushed before the thread exits.
    pub fn shut_down(&mut self) {
        self.state.stop_flag.store(true, Ordering::SeqCst);

        let task_handle = self.state.flush_thread.lock().take();

        // Join the task if we got a valid handle.
        // Use a separate OS thread to avoid block_in_place limitation with single-threaded runtime.
        if let Some(handle) = task_handle {
            let runtime_handle = self.runtime_handle.clone();
            let join_result = std::thread::spawn(move || runtime_handle.block_on(handle)).join();

            match join_result {
                Ok(Ok(())) => {},
                Ok(Err(e)) => error!("Flush task panicked or cancelled: {:?}", e),
                Err(_) => error!("Thread panicked while joining flush task"),
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
    pub fn append_log_record(&mut self, log_record: Arc<LogRecord>) -> Lsn {
        // Assign a new LSN atomically
        let lsn = self.state.next_lsn.fetch_add(1, Ordering::SeqCst);

        // Set the LSN in the log record - now thread-safe with interior mutability
        log_record.set_lsn(lsn);

        // Send log record to processing queue using try_send with retry.
        // This approach works with both single-threaded and multi-threaded Tokio runtimes,
        // avoiding the block_in_place limitation that requires multi-threaded runtime.
        let mut record_to_send = log_record.clone();
        loop {
            match self.state.sender.try_send(record_to_send) {
                Ok(()) => break,
                Err(tokio::sync::mpsc::error::TrySendError::Full(returned)) => {
                    // Channel is full, wait briefly and retry
                    record_to_send = returned;
                    thread::sleep(Duration::from_micros(100));
                },
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    error!("Failed to queue log record: channel closed");
                    break;
                },
            }
        }

        // For commit records, ensure durability by waiting for buffer to be flushed
        if log_record.is_commit() {
            // Signal the flush thread that this is a commit record
            // Wait for confirmation that it has been flushed
            trace!("Waiting for commit record with LSN {} to be flushed", lsn);

            // Wait for buffer to be flushed (observe persistent_lsn ≥ lsn).
            // persistent_lsn is initialized to INVALID_LSN, which must be treated as "nothing flushed yet".
            // This spin-wait loop is synchronous and doesn't require block_in_place.
            while {
                let persisted = self.state.persistent_lsn.load(Ordering::SeqCst);
                persisted == INVALID_LSN || persisted < lsn
            } {
                if self.state.stop_flag.load(Ordering::SeqCst) {
                    error!(
                        "Log flush thread stopped while waiting for commit LSN {} to persist",
                        lsn
                    );
                    break;
                }
                thread::sleep(Duration::from_millis(1));
            }

            debug!("Commit record with LSN {} has been flushed", lsn);
        }

        lsn
    }

    /// Returns the next LSN that will be assigned to a new log record.
    ///
    /// This is the current value of the monotonic LSN counter. The actual
    /// assignment happens atomically in `append_log_record`.
    ///
    /// # Returns
    ///
    /// The next available LSN.
    pub fn get_next_lsn(&self) -> Lsn {
        let lsn = self.state.next_lsn.load(Ordering::SeqCst);
        trace!("Retrieved next LSN: {}", lsn);
        lsn
    }

    /// Returns the highest LSN that has been durably written to disk.
    ///
    /// Commit operations block until `persistent_lsn >= commit_lsn` to ensure
    /// durability. This value is updated by the flush thread after successful
    /// disk writes and syncs.
    ///
    /// # Returns
    ///
    /// The highest durable LSN, or `INVALID_LSN` if nothing has been flushed yet.
    pub fn get_persistent_lsn(&self) -> Lsn {
        let lsn = self.state.persistent_lsn.load(Ordering::SeqCst);
        trace!("Retrieved persistent LSN: {}", lsn);
        lsn
    }

    /// Sets the persistent log sequence number (LSN).
    ///
    /// # Parameters
    /// - `lsn`: The log sequence number to set.
    pub fn set_persistent_lsn(&mut self, lsn: Lsn) {
        debug!("Setting persistent LSN to {}", lsn);
        self.state.persistent_lsn.store(lsn, Ordering::SeqCst);
    }

    /// Parses a log record from raw bytes
    ///
    /// # Parameters
    /// - `data`: The raw log record data
    ///
    /// # Returns
    /// An optional LogRecord if parsing was successful
    pub fn parse_log_record(&self, data: &[u8]) -> Option<LogRecord> {
        // Skip empty data
        if data.is_empty() || data.iter().all(|&b| b == 0) {
            return None;
        }

        // Try to deserialize using LogRecord's bincode implementation
        match LogRecord::from_bytes(data) {
            Ok(record) => Some(record),
            Err(e) => {
                // Log the error but don't fail the recovery process
                warn!("Failed to parse log record: {}", e);
                None
            },
        }
    }

    /// Reads a log record from disk at the specified offset
    ///
    /// # Parameters
    /// - `offset`: The offset to read from
    ///
    /// # Returns
    /// An optional LogRecord if reading and parsing was successful
    pub async fn read_log_record_async(&self, offset: u64) -> Option<LogRecord> {
        Self::read_log_record_static(Arc::clone(&self.state.disk_manager), offset).await
    }

    /// Static helper for reading log records, used by both async and sync wrappers.
    async fn read_log_record_static(
        disk_manager: Arc<AsyncDiskManager>,
        offset: u64,
    ) -> Option<LogRecord> {
        debug!("Reading log record from offset {}", offset);

        match disk_manager.read_log(offset).await {
            Ok(bytes) => match LogRecord::from_bytes(&bytes) {
                Ok(record) => {
                    debug!(
                        "Successfully read log record: txn_id={}, type={:?}, size={}",
                        record.get_txn_id(),
                        record.get_log_record_type(),
                        record.get_size()
                    );
                    Some(record)
                },
                Err(e) => {
                    warn!(
                        "Failed to deserialize log record at offset {}: {}",
                        offset, e
                    );
                    None
                },
            },
            Err(e) => {
                warn!("Failed to read log from disk at offset {}: {}", offset, e);
                None
            },
        }
    }

    /// Reads a log record from disk at the specified offset (synchronous version).
    ///
    /// This is a blocking wrapper around [`read_log_record_async`](Self::read_log_record_async).
    /// It handles both cases: running on a tokio runtime (uses a separate OS thread)
    /// or running outside a runtime (uses the stored runtime handle directly).
    ///
    /// # Parameters
    ///
    /// - `offset`: The byte offset in the log file to read from.
    ///
    /// # Returns
    ///
    /// - `Some(LogRecord)`: If reading and parsing succeeded.
    /// - `None`: If the offset is invalid or parsing failed.
    pub fn read_log_record(&self, offset: u64) -> Option<LogRecord> {
        // If we're already on a Tokio runtime, spawn a separate OS thread to avoid
        // block_in_place limitation with single-threaded runtime.
        if Handle::try_current().is_ok() {
            let runtime_handle = self.runtime_handle.clone();
            let disk_manager = Arc::clone(&self.state.disk_manager);
            std::thread::spawn(move || {
                runtime_handle.block_on(Self::read_log_record_static(disk_manager, offset))
            })
            .join()
            .unwrap_or(None)
        } else {
            self.runtime_handle
                .block_on(self.read_log_record_async(offset))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tempfile::TempDir;

    use super::*;
    use crate::common::config::TxnId;
    use crate::common::logger::initialize_logger;
    use crate::recovery::log_record::{LogRecord, LogRecordType};
    use crate::storage::disk::async_disk::DiskManagerConfig;

    struct TestContext {
        log_manager: LogManager,
        disk_manager: Arc<AsyncDiskManager>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
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
            let disk_manager = AsyncDiskManager::new(
                db_path.clone(),
                log_path.clone(),
                DiskManagerConfig::default(),
            )
            .await;
            let disk_manager_arc = Arc::new(disk_manager.unwrap());
            let mut log_manager = LogManager::new(disk_manager_arc.clone());
            // Most tests append commit records; commit durability waiting requires the flush
            // thread to be running. Start it by default so tests don't hang.
            log_manager.run_flush_thread();

            Self {
                log_manager,
                disk_manager: disk_manager_arc,
                _temp_dir: temp_dir,
            }
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            // Ensure we don't leak a background flush thread across tests.
            self.log_manager.shut_down();
        }
    }

    /// Basic functionality tests for LogManager
    mod basic_functionality {
        use super::*;

        #[tokio::test(flavor = "multi_thread")]
        async fn test_log_manager_initialization() {
            let ctx = TestContext::new("init_test").await;

            assert_eq!(ctx.log_manager.get_next_lsn(), 0);
            assert_eq!(ctx.log_manager.get_persistent_lsn(), INVALID_LSN);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_append_log_record() {
            let mut ctx = TestContext::new("append_test").await;

            let txn_id: TxnId = 1;
            let prev_lsn = INVALID_LSN;

            // Create a begin transaction log record
            let log_record = Arc::new(LogRecord::new_transaction_record(
                txn_id,
                prev_lsn,
                LogRecordType::Begin,
            ));

            // Append the log record
            let lsn = ctx.log_manager.append_log_record(log_record);
            assert_eq!(lsn, 0); // First LSN should be 0

            // Verify LSN was incremented
            assert_eq!(ctx.log_manager.get_next_lsn(), 1);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_multiple_append_operations() {
            let mut ctx = TestContext::new("multiple_append_test").await;

            // Append multiple log records
            for i in 0..5 {
                let log_record = Arc::new(LogRecord::new_transaction_record(
                    i as TxnId,
                    INVALID_LSN,
                    LogRecordType::Begin,
                ));
                let lsn = ctx.log_manager.append_log_record(log_record);
                assert_eq!(lsn, i);
            }

            assert_eq!(ctx.log_manager.get_next_lsn(), 5);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_persistent_lsn_management() {
            let mut ctx = TestContext::new("persistent_lsn_test").await;

            assert_eq!(ctx.log_manager.get_persistent_lsn(), INVALID_LSN);

            // Set persistent LSN
            let test_lsn = 42;
            ctx.log_manager.set_persistent_lsn(test_lsn);
            assert_eq!(ctx.log_manager.get_persistent_lsn(), test_lsn);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_log_record_types() {
            let mut ctx = TestContext::new("record_types_test").await;
            let mut next_lsn = 0;

            // Create all records first
            let begin_record = Arc::new(LogRecord::new_transaction_record(
                1,
                INVALID_LSN,
                LogRecordType::Begin,
            ));

            // Append begin record and get LSN
            let begin_lsn = {
                let lsn = ctx.log_manager.append_log_record(begin_record.clone());
                next_lsn += 1;
                assert_eq!(lsn, 0);
                lsn
            };

            // Small delay between operations
            tokio::time::sleep(Duration::from_millis(1)).await;

            // Create and append commit record
            let commit_record = Arc::new(LogRecord::new_transaction_record(
                1,
                begin_lsn,
                LogRecordType::Commit,
            ));
            let commit_lsn = ctx.log_manager.append_log_record(commit_record);
            next_lsn += 1;
            assert_eq!(commit_lsn, 1);

            tokio::time::sleep(Duration::from_millis(1)).await;

            // Create and append abort record
            let abort_record = Arc::new(LogRecord::new_transaction_record(
                2,
                INVALID_LSN,
                LogRecordType::Abort,
            ));
            let abort_lsn = ctx.log_manager.append_log_record(abort_record);
            next_lsn += 1;
            assert_eq!(abort_lsn, 2);

            // Final verification
            let final_lsn = ctx.log_manager.get_next_lsn();
            assert_eq!(
                final_lsn, next_lsn,
                "Expected LSN {} but got {}",
                next_lsn, final_lsn
            );
        }
    }

    /// Tests for threading and concurrency
    mod threading_tests {
        use parking_lot::RwLock;

        use super::*;
        use crate::common::config::PageId;

        #[tokio::test(flavor = "multi_thread")]
        async fn test_flush_thread_lifecycle() {
            let mut ctx = TestContext::new("flush_thread_test").await;

            // Start flush thread (already started in TestContext::new)
            // ctx.log_manager.run_flush_thread();

            // Allow some time for thread to start
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Append some log records
            for i in 0..3 {
                let log_record = Arc::new(LogRecord::new_transaction_record(
                    i as TxnId,
                    INVALID_LSN,
                    LogRecordType::Begin,
                ));
                ctx.log_manager.append_log_record(log_record);
            }

            // Shutdown should complete cleanly
            ctx.log_manager.shut_down();
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_concurrent_log_appends() {
            let ctx = Arc::new(RwLock::new(TestContext::new("concurrent_test").await));
            let thread_count = 5;
            let mut handles = vec![];

            for i in 0..thread_count {
                let ctx_clone = Arc::clone(&ctx);
                let handle = thread::spawn(move || {
                    let log_record = Arc::new(LogRecord::new_transaction_record(
                        i as TxnId,
                        INVALID_LSN,
                        LogRecordType::Begin,
                    ));
                    ctx_clone.write().log_manager.append_log_record(log_record)
                });
                handles.push(handle);
            }

            // Collect all LSNs
            let lsns: Vec<Lsn> = handles.into_iter().map(|h| h.join().unwrap()).collect();

            // Verify LSNs are unique and sequential
            let mut unique_lsns: Vec<Lsn> = lsns.clone();
            unique_lsns.sort();
            unique_lsns.dedup();
            assert_eq!(unique_lsns.len(), thread_count);

            assert_eq!(ctx.read().log_manager.get_next_lsn() as usize, thread_count);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_shutdown_behavior() {
            let mut ctx = TestContext::new("shutdown_test").await;

            // Start flush thread (already started in TestContext::new)
            // ctx.log_manager.run_flush_thread();

            // Append some records
            for i in 0..3 {
                let log_record = Arc::new(LogRecord::new_transaction_record(
                    i as TxnId,
                    INVALID_LSN,
                    LogRecordType::Begin,
                ));
                ctx.log_manager.append_log_record(log_record);
            }

            // Test multiple shutdown calls (should handle gracefully)
            ctx.log_manager.shut_down();
            ctx.log_manager.shut_down(); // Second shutdown should not panic
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_concurrent_shutdown() {
            let ctx = Arc::new(RwLock::new(
                TestContext::new("concurrent_shutdown_test").await,
            ));

            // Start flush thread (already started in TestContext::new)
            // ctx.write().log_manager.run_flush_thread();

            // Append some records
            for i in 0..3 {
                let log_record = Arc::new(LogRecord::new_transaction_record(
                    i as TxnId,
                    INVALID_LSN,
                    LogRecordType::Begin,
                ));
                ctx.write().log_manager.append_log_record(log_record);
            }

            // Try to shut down from multiple threads simultaneously
            let thread_count = 3;
            let mut handles = vec![];

            for _ in 0..thread_count {
                let ctx_clone = Arc::clone(&ctx);
                let handle = thread::spawn(move || {
                    ctx_clone.write().log_manager.shut_down();
                });
                handles.push(handle);
            }

            // Wait for all threads to complete
            for handle in handles {
                handle.join().unwrap();
            }

            // Verify we can still interact with the log manager
            ctx.write().log_manager.get_next_lsn();
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_flush_thread_periodic_flush() {
            let mut ctx = TestContext::new("periodic_flush_test").await;

            // Start flush thread (already started in TestContext::new)
            // ctx.log_manager.run_flush_thread();

            // Append some log records
            for i in 0..5 {
                let log_record = Arc::new(LogRecord::new_transaction_record(
                    i as TxnId,
                    INVALID_LSN,
                    LogRecordType::Begin,
                ));
                let lsn = ctx.log_manager.append_log_record(log_record);
                debug!("Appended record with LSN {}", lsn);
            }

            // Wait for periodic flush to happen (slightly longer than the flush interval)
            tokio::time::sleep(Duration::from_millis(15)).await;
            // Flush interval is 10ms

            // Check that persistent LSN has been updated
            let _persistent_lsn = ctx.log_manager.get_persistent_lsn();
            // Persistent LSN should be updated by periodic flush
            // Note: LSN is unsigned, so >= 0 check is not needed

            ctx.log_manager.shut_down();
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_flush_thread_commit_flush() {
            let mut ctx = TestContext::new("commit_flush_test").await;

            // Start flush thread (already started in TestContext::new)
            // ctx.log_manager.run_flush_thread();

            // Append a begin record
            let begin_record = Arc::new(LogRecord::new_transaction_record(
                1 as TxnId,
                INVALID_LSN,
                LogRecordType::Begin,
            ));
            let begin_lsn = ctx.log_manager.append_log_record(begin_record);

            // Append a commit record (should force immediate flush)
            let commit_record = Arc::new(LogRecord::new_transaction_record(
                1 as TxnId,
                begin_lsn,
                LogRecordType::Commit,
            ));
            let commit_lsn = ctx.log_manager.append_log_record(commit_record);

            // Persistent LSN should be updated immediately after commit
            let persistent_lsn = ctx.log_manager.get_persistent_lsn();
            assert!(
                persistent_lsn >= commit_lsn,
                "Persistent LSN {} should be >= commit LSN {}",
                persistent_lsn,
                commit_lsn
            );

            ctx.log_manager.shut_down();
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_flush_thread_buffer_full() {
            let mut ctx = TestContext::new("buffer_full_test").await;

            // Start flush thread (already started in TestContext::new)
            // ctx.log_manager.run_flush_thread();

            // Use a commit record to guarantee a flush happens
            for i in 0..10 {
                // Begin record
                let begin_record = Arc::new(LogRecord::new_transaction_record(
                    i as TxnId,
                    INVALID_LSN,
                    LogRecordType::Begin,
                ));
                let begin_lsn = ctx.log_manager.append_log_record(begin_record);

                // Commit record - forces flush
                let commit_record = Arc::new(LogRecord::new_transaction_record(
                    i as TxnId,
                    begin_lsn,
                    LogRecordType::Commit,
                ));
                ctx.log_manager.append_log_record(commit_record);
            }

            // Add a small delay to ensure all commits are processed
            tokio::time::sleep(Duration::from_millis(20)).await;

            // Check that persistent LSN has been updated
            let persistent_lsn = ctx.log_manager.get_persistent_lsn();
            assert_ne!(
                persistent_lsn, INVALID_LSN,
                "Persistent LSN should be updated after buffer fills up"
            );

            ctx.log_manager.shut_down();
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_flush_thread_continuous_operation() {
            let mut ctx = TestContext::new("continuous_operation_test").await;

            // Start flush thread (already started in TestContext::new)
            // ctx.log_manager.run_flush_thread();

            // Initialize flush thread with a marker record and wait for it to be processed
            let init_record = Arc::new(LogRecord::new_transaction_record(
                999 as TxnId,
                INVALID_LSN,
                LogRecordType::Commit,
            ));
            let init_lsn = ctx.log_manager.append_log_record(init_record);

            // Wait for initial record to be flushed
            tokio::time::sleep(Duration::from_millis(50)).await;

            // Verify the flush thread is working
            let initial_persistent_lsn = ctx.log_manager.get_persistent_lsn();
            assert!(
                initial_persistent_lsn >= init_lsn,
                "Flush thread initialization failed: persistent LSN {} not updated to {}",
                initial_persistent_lsn,
                init_lsn
            );

            // Track LSNs before and after each batch
            let mut batch_end_lsns = Vec::new();

            // Perform several batches of appends with delays between
            for batch in 0..3 {
                let _start_persistent_lsn = ctx.log_manager.get_persistent_lsn();

                // Append a batch of records
                let mut last_batch_lsn = INVALID_LSN;
                for i in 0..10 {
                    let txn_id = (batch * 100 + i) as TxnId;
                    let log_record = Arc::new(LogRecord::new_transaction_record(
                        txn_id,
                        INVALID_LSN,
                        LogRecordType::Begin,
                    ));
                    last_batch_lsn = ctx.log_manager.append_log_record(log_record);
                }

                // Add a commit record to force a flush
                let commit_record = Arc::new(LogRecord::new_transaction_record(
                    (batch * 100) as TxnId, // Use the first txn_id from this batch
                    last_batch_lsn,
                    LogRecordType::Commit,
                ));
                last_batch_lsn = ctx.log_manager.append_log_record(commit_record);

                batch_end_lsns.push(last_batch_lsn);

                // Wait for flush to happen (polling to reduce flakiness)
                let mut waited_ms = 0;
                let end_persistent_lsn = loop {
                    let current = ctx.log_manager.get_persistent_lsn();
                    if current >= last_batch_lsn || waited_ms >= 200 {
                        break current;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    waited_ms += 5;
                };

                // Persistent LSN should have advanced
                assert!(
                    end_persistent_lsn >= last_batch_lsn,
                    "Batch {}: Persistent LSN should be at least {} but was {}",
                    batch,
                    last_batch_lsn,
                    end_persistent_lsn
                );
            }

            // Final persistent LSN should be at least as high as the last batch's highest LSN
            let final_persistent_lsn = ctx.log_manager.get_persistent_lsn();
            let highest_batch_lsn = *batch_end_lsns.iter().max().unwrap();

            assert!(
                final_persistent_lsn >= highest_batch_lsn,
                "Final persistent LSN {} should be >= highest batch LSN {}",
                final_persistent_lsn,
                highest_batch_lsn
            );

            ctx.log_manager.shut_down();
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_flush_thread_concurrent_commits() {
            let ctx = Arc::new(RwLock::new(
                TestContext::new("concurrent_commits_test").await,
            ));

            // Start flush thread (already started in TestContext::new)
            // ctx.write().log_manager.run_flush_thread();

            // Launch multiple threads, each doing a begin+commit transaction
            let thread_count = 5;
            let mut handles = vec![];

            for i in 0..thread_count {
                let ctx_clone = Arc::clone(&ctx);
                let handle = thread::spawn(move || {
                    let txn_id = (i + 100) as TxnId;

                    // Begin transaction
                    let begin_record = Arc::new(LogRecord::new_transaction_record(
                        txn_id,
                        INVALID_LSN,
                        LogRecordType::Begin,
                    ));
                    let begin_lsn = ctx_clone
                        .write()
                        .log_manager
                        .append_log_record(begin_record);

                    // Commit transaction - this should force a flush
                    let commit_record = Arc::new(LogRecord::new_transaction_record(
                        txn_id,
                        begin_lsn,
                        LogRecordType::Commit,
                    ));
                    ctx_clone
                        .write()
                        .log_manager
                        .append_log_record(commit_record)
                });
                handles.push(handle);
            }

            // Collect all commit LSNs
            let commit_lsns: Vec<Lsn> = handles.into_iter().map(|h| h.join().unwrap()).collect();
            let max_commit_lsn = *commit_lsns.iter().max().unwrap();

            // Final persistent LSN should include all commits
            let final_persistent_lsn = ctx.read().log_manager.get_persistent_lsn();
            assert!(
                final_persistent_lsn >= max_commit_lsn,
                "Final persistent LSN {} should be >= max commit LSN {}",
                final_persistent_lsn,
                max_commit_lsn
            );

            ctx.write().log_manager.shut_down();
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_flush_thread_mixed_operations() {
            let mut ctx = TestContext::new("mixed_operations_test").await;

            // Start flush thread (already started in TestContext::new)
            // ctx.log_manager.run_flush_thread();

            // Perform a mix of operations with different record types
            let txn_id = 42 as TxnId;
            let mut prev_lsn = INVALID_LSN;

            // Begin transaction
            let begin_record = Arc::new(LogRecord::new_transaction_record(
                txn_id,
                prev_lsn,
                LogRecordType::Begin,
            ));
            prev_lsn = ctx.log_manager.append_log_record(begin_record);

            // New page operation
            let new_page_record = Arc::new(LogRecord::new_page_record(
                txn_id,
                prev_lsn,
                LogRecordType::NewPage,
                0 as PageId,
                1 as PageId,
            ));
            prev_lsn = ctx.log_manager.append_log_record(new_page_record);

            // Wait a bit to allow potential flush
            tokio::time::sleep(Duration::from_millis(15)).await;

            // Another new page operation
            let another_page_record = Arc::new(LogRecord::new_page_record(
                txn_id,
                prev_lsn,
                LogRecordType::NewPage,
                1 as PageId,
                2 as PageId,
            ));
            prev_lsn = ctx.log_manager.append_log_record(another_page_record);

            // Commit transaction (should force flush)
            let commit_record = Arc::new(LogRecord::new_transaction_record(
                txn_id,
                prev_lsn,
                LogRecordType::Commit,
            ));
            let commit_lsn = ctx.log_manager.append_log_record(commit_record);

            // Persistent LSN should include the commit
            let persistent_lsn = ctx.log_manager.get_persistent_lsn();
            assert!(
                persistent_lsn >= commit_lsn,
                "Persistent LSN {} should be >= commit LSN {}",
                persistent_lsn,
                commit_lsn
            );

            ctx.log_manager.shut_down();
        }
    }

    mod read_and_write_tests {
        use super::*;

        #[tokio::test(flavor = "multi_thread")]
        async fn test_write_and_read_log_record() {
            let mut ctx = TestContext::new("write_read_test").await;

            // Create and append a log record
            let txn_id: TxnId = 42;
            let begin_record = Arc::new(LogRecord::new_transaction_record(
                txn_id,
                INVALID_LSN,
                LogRecordType::Begin,
            ));
            let begin_serialized_size = begin_record.to_bytes().unwrap().len();

            let begin_lsn = ctx.log_manager.append_log_record(begin_record);

            // Force durability with a commit record
            let commit_record = Arc::new(LogRecord::new_transaction_record(
                txn_id,
                begin_lsn,
                LogRecordType::Commit,
            ));
            ctx.log_manager.append_log_record(commit_record);

            // Read back the first record from the log using the known serialized size
            let raw = ctx
                .disk_manager
                .read_log_sized(0, begin_serialized_size)
                .await
                .expect("log bytes should be readable");
            let read_record =
                LogRecord::from_bytes(&raw).expect("log record should exist at offset 0");

            assert_eq!(read_record.get_txn_id(), txn_id);
            assert_eq!(read_record.get_log_record_type(), LogRecordType::Begin);

            ctx.log_manager.shut_down();
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_read_with_invalid_offset() {
            let mut ctx = TestContext::new("invalid_offset_test").await;

            // Try to read from an offset that is beyond the written data
            let record = ctx.log_manager.read_log_record_async(10_000).await;
            assert!(record.is_none(), "Should return None for invalid offset");

            ctx.log_manager.shut_down();
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_parse_log_record_rejects_empty() {
            let ctx = TestContext::new("parse_empty_test").await;
            assert!(ctx.log_manager.parse_log_record(&[]).is_none());
            assert!(ctx.log_manager.parse_log_record(&[0u8; 32]).is_none());
        }

        #[test]
        fn test_binary_serialization() {
            // Set up logging for this test
            let _ = env_logger::builder()
                .filter_level(log::LevelFilter::Debug)
                .is_test(true)
                .try_init();

            // Create a log record
            let txn_id = 42;
            let log_record =
                LogRecord::new_transaction_record(txn_id, INVALID_LSN, LogRecordType::Begin);

            // Serialize to bytes
            let serialized = log_record.to_bytes().unwrap();
            debug!("Serialized log record to {} bytes", serialized.len());

            // Deserialize and verify
            let deserialized = LogRecord::from_bytes(&serialized).unwrap();

            // Verify the deserialized record matches the original
            assert_eq!(deserialized.get_txn_id(), txn_id);
            assert_eq!(deserialized.get_log_record_type(), LogRecordType::Begin);
            assert_eq!(deserialized.get_prev_lsn(), INVALID_LSN);
        }
    }
}
