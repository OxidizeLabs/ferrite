//! # Async Disk Manager
//!
//! This module provides `AsyncDiskManager`, a high-performance asynchronous disk management
//! system designed for database workloads. It integrates caching, write buffering, work-stealing
//! scheduling, and comprehensive metrics to optimize I/O operations.
//!
//! ## Architecture
//!
//! ```text
//!   Buffer Pool Manager / Query Execution
//!   ═══════════════════════════════════════════════════════════════════════════
//!                          │
//!                          ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                        AsyncDiskManager                                 │
//!   │                                                                         │
//!   │  ┌──────────────────────────────────────────────────────────────────┐   │
//!   │  │                    Read/Write Request                            │   │
//!   │  └───────────────────────────┬──────────────────────────────────────┘   │
//!   │                              │                                          │
//!   │         ┌────────────────────┼────────────────────┐                     │
//!   │         ▼                    ▼                    ▼                     │
//!   │  ┌─────────────┐     ┌─────────────┐     ┌───────────────┐              │
//!   │  │CacheManager │     │WriteManager │     │WorkStealing   │              │
//!   │  │             │     │             │     │Scheduler      │              │
//!   │  │ Hot/Warm/   │     │ Buffer &    │     │               │              │
//!   │  │ Cold Tiers  │     │ Batch       │     │ Priority      │              │
//!   │  │ (LRU-based) │     │ Writes      │     │ Task Queues   │              │
//!   │  └──────┬──────┘     └──────┬──────┘     └───────┬───────┘              │
//!   │         │                   │                    │                      │
//!   │         └───────────────────┼────────────────────┘                      │
//!   │                             ▼                                           │
//!   │  ┌──────────────────────────────────────────────────────────────────┐   │
//!   │  │                      AsyncIOEngine                               │   │
//!   │  │  • Worker threads for async I/O                                  │   │
//!   │  │  • Direct I/O support (O_DIRECT / F_NOCACHE)                     │   │
//!   │  │  • Aligned buffer management                                     │   │
//!   │  └───────────────────────────┬──────────────────────────────────────┘   │
//!   │                              │                                          │
//!   │  ┌──────────────────────────────────────────────────────────────────┐   │
//!   │  │                    MetricsCollector                              │   │
//!   │  │  • Latency, throughput, IOPS tracking                            │   │
//!   │  │  • Health scoring & alerts                                       │   │
//!   │  │  • Prometheus export                                             │   │
//!   │  └──────────────────────────────────────────────────────────────────┘   │
//!   └─────────────────────────────────────────────────────────────────────────┘
//!                              │
//!                              ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                    Disk (DB File + WAL File)                            │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Read Path
//!
//! ```text
//!   read_page(page_id)
//!        │
//!        ▼
//!   ┌────────────────┐     ┌────────────────┐
//!   │ Check Cache    │────►│ Cache Hit?     │
//!   └────────────────┘     └───────┬────────┘
//!                                  │
//!                    ┌─────────────┴─────────────┐
//!                    │ Yes                       │ No
//!                    ▼                           ▼
//!             ┌─────────────┐           ┌────────────────────┐
//!             │ Return      │           │ work_stealing?     │
//!             │ cached data │           └─────────┬──────────┘
//!             └─────────────┘                     │
//!                                   ┌─────────────┴─────────────┐
//!                                   │ Yes                       │ No
//!                                   ▼                           ▼
//!                          ┌────────────────┐          ┌────────────────┐
//!                          │ Submit to      │          │ Direct read    │
//!                          │ Scheduler      │          │ via IOEngine   │
//!                          └───────┬────────┘          └───────┬────────┘
//!                                  │                           │
//!                                  └───────────┬───────────────┘
//!                                              ▼
//!                                    ┌─────────────────┐
//!                                    │ Store in cache  │
//!                                    │ Return data     │
//!                                    └─────────────────┘
//! ```
//!
//! ## Write Path
//!
//! ```text
//!   write_page(page_id, data)
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  WriteManager.buffer_write()                                           │
//!   │                                                                        │
//!   │  ┌──────────────────┐     ┌──────────────────┐                         │
//!   │  │ Add to write     │────►│ Buffer full?     │                         │
//!   │  │ buffer           │     └─────────┬────────┘                         │
//!   │  └──────────────────┘               │                                  │
//!   │                       ┌─────────────┴─────────────┐                    │
//!   │                       │ Yes                       │ No                 │
//!   │                       ▼                           ▼                    │
//!   │              ┌────────────────┐          ┌────────────────┐            │
//!   │              │ Return pages   │          │ Return None    │            │
//!   │              │ to flush       │          │ (buffered)     │            │
//!   │              └───────┬────────┘          └────────────────┘            │
//!   └──────────────────────┼─────────────────────────────────────────────────┘
//!                          │
//!                          ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  write_pages_to_disk()                                                 │
//!   │                                                                        │
//!   │  For each (page_id, data):                                             │
//!   │    IOEngine.write_page(page_id, data)                                  │
//!   │                                                                        │
//!   │  Apply durability policy (fsync based on FsyncPolicy)                  │
//!   └────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Components
//!
//! | Component             | Description                                          |
//! |-----------------------|------------------------------------------------------|
//! | `AsyncDiskManager`    | Main entry point for all disk I/O operations        |
//! | `AsyncIOEngine`       | Low-level async I/O with worker threads             |
//! | `CacheManager`        | Multi-tier LRU cache (hot/warm/cold)                 |
//! | `WriteManager`        | Write buffering and batch coalescing                 |
//! | `WorkStealingScheduler`| Priority-based task scheduling                      |
//! | `MetricsCollector`    | Performance metrics and health monitoring            |
//!
//! ## Core Operations
//!
//! | Method                    | Description                                      |
//! |---------------------------|--------------------------------------------------|
//! | `new()`                   | Create manager with DB and log file paths        |
//! | `read_page()`             | Read page (from cache or disk)                   |
//! | `write_page()`            | Buffer write for later flush                     |
//! | `read_pages_batch()`      | Read multiple pages in one operation             |
//! | `write_pages_batch()`     | Write multiple pages in one operation            |
//! | `flush()`                 | Flush buffered writes to disk                    |
//! | `sync()`                  | Force fsync on DB file                           |
//! | `sync_log()`              | Force fsync on WAL file                          |
//! | `read_log()` / `write_log()` | WAL operations for recovery                   |
//! | `shutdown()`              | Graceful shutdown with flush                     |
//!
//! ## Configuration (`DiskManagerConfig`)
//!
//! | Option                  | Default | Description                              |
//! |-------------------------|---------|------------------------------------------|
//! | `io_threads`            | 4       | Number of I/O worker threads             |
//! | `cache_size_mb`         | 64      | Cache size in MB (0 = disabled)          |
//! | `batch_size`            | 32      | Pages per batch for batch operations     |
//! | `direct_io`             | false   | Enable O_DIRECT / F_NOCACHE              |
//! | `work_stealing_enabled` | true    | Use work-stealing scheduler              |
//! | `prefetch_enabled`      | true    | Enable read-ahead prefetching            |
//! | `fsync_policy`          | OnFlush | When to call fsync (Never/PerWrite/etc.) |
//! | `compression_enabled`   | false   | Enable page compression                  |
//!
//! ## Fsync Policies
//!
//! ```text
//!   FsyncPolicy::Never      → No fsync (fastest, no durability guarantee)
//!   FsyncPolicy::PerWrite   → fsync after every write (slowest, safest)
//!   FsyncPolicy::OnFlush    → fsync on explicit flush() calls
//!   FsyncPolicy::Periodic(n)→ fsync every n milliseconds
//! ```
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
//!
//! // Create with default configuration
//! let config = DiskManagerConfig {
//!     io_threads: 4,
//!     cache_size_mb: 128,
//!     direct_io: true,
//!     ..Default::default()
//! };
//!
//! let mut manager = AsyncDiskManager::new(
//!     "data.db".to_string(),
//!     "wal.log".to_string(),
//!     config,
//! ).await?;
//!
//! // Write a page
//! let page_data = vec![0u8; 4096];
//! manager.write_page(42, page_data).await?;
//!
//! // Ensure durability
//! manager.flush().await?;
//!
//! // Read back
//! let data = manager.read_page(42).await?;
//!
//! // Batch operations
//! let pages = vec![(1, data1), (2, data2), (3, data3)];
//! manager.write_pages_batch(pages).await?;
//!
//! // Graceful shutdown
//! manager.shutdown().await?;
//! ```
//!
//! ## Metrics and Monitoring
//!
//! ```text
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                        Metrics Available                                │
//!   │                                                                         │
//!   │  Performance:           Cache:              Storage:                    │
//!   │  • Read latency         • Hit ratio         • Buffer utilization        │
//!   │  • Write latency        • Hot/warm/cold     • Compression ratio         │
//!   │  • IOPS                 • Promotions        • Flush frequency           │
//!   │  • Throughput (MB/s)    • Evictions         • Write amplification       │
//!   │                                                                         │
//!   │  Export: get_metrics(), export_prometheus_metrics()                     │
//!   │  Health: health_check(), get_health_report(), get_dashboard_data()      │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Thread Safety
//!
//! - `AsyncDiskManager` is designed for concurrent access via `Arc<AsyncDiskManager>`
//! - Internal components use `RwLock` and `Mutex` for synchronization
//! - I/O engine uses `Arc<RwLock<AsyncIOEngine>>` for thread-safe access
//! - Write manager uses internal locking for buffer operations
//! - Safe to call from multiple tokio tasks concurrently
//!
//! ## WAL Handling
//!
//! The WAL (Write-Ahead Log) file uses **buffered I/O** (not direct I/O) because:
//! - Append-heavy workload with variable-length records
//! - O_DIRECT alignment constraints are impractical for WAL
//! - OS buffer cache is beneficial for sequential writes
//! - Durability is ensured via explicit `sync_log()` calls

use super::config::DiskManagerConfig;
use crate::common::config::{DB_PAGE_SIZE, PageId};
use crate::recovery::log_record::LogRecord;
use crate::storage::disk::async_disk::cache::cache_manager::CacheManager;
use crate::storage::disk::async_disk::cache::cache_manager::CacheStatistics;
use crate::storage::disk::async_disk::config::{FsyncPolicy, IOPriority};
use crate::storage::disk::async_disk::io::AsyncIOEngine;
use crate::storage::disk::async_disk::memory::{
    DurabilityProvider, WriteBufferStats, WriteManager,
};
use crate::storage::disk::async_disk::metrics::alerts::AlertSummary;
use crate::storage::disk::async_disk::metrics::collector::MetricsCollector;
use crate::storage::disk::async_disk::metrics::dashboard::{
    CacheDashboard, ComponentHealth, DashboardData, HealthReport, PerformanceDashboard,
    StorageDashboard,
};
use crate::storage::disk::async_disk::metrics::prediction::TrendData;
use crate::storage::disk::async_disk::metrics::snapshot::MetricsSnapshot;
use crate::storage::disk::async_disk::scheduler::{IOTask, IOTaskType, WorkStealingScheduler};
use log::{debug, error, info, trace, warn};
use std::future::Future;
use std::io;
use std::io::Result as IoResult;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::sync::oneshot;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;

/// Asynchronous Disk Manager that provides high-performance I/O operations
/// with advanced features like caching, compression, and metrics collection.
#[derive(Debug)]
pub struct AsyncDiskManager {
    // Core components
    io_engine: Arc<RwLock<AsyncIOEngine>>,
    write_manager: Arc<WriteManager>,
    cache_manager: Arc<CacheManager>,

    // Configuration
    config: DiskManagerConfig,

    // Optimization components
    scheduler: Arc<WorkStealingScheduler>,

    // Metrics and monitoring
    metrics_collector: Arc<MetricsCollector>,

    // State tracking
    is_shutting_down: Arc<AtomicBool>,
    background_tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl DurabilityProvider for AsyncDiskManager {
    fn sync_data(&self) -> impl Future<Output = IoResult<()>> + Send {
        self.sync()
    }

    fn sync_log(&self) -> impl Future<Output = IoResult<()>> + Send {
        self.sync_log()
    }
}

impl AsyncDiskManager {
    /// Creates a new AsyncDiskManager with the specified database and log files
    pub async fn new(
        db_file_path: String,
        log_file_path: String,
        config: DiskManagerConfig,
    ) -> IoResult<Self> {
        debug!(
            "Creating AsyncDiskManager with db_file: {}, log_file: {}",
            db_file_path, log_file_path
        );
        debug!(
            "Config - io_threads: {}, max_concurrent_ops: {}, batch_size: {}",
            config.io_threads, config.max_concurrent_ops, config.batch_size
        );

        // Create direct I/O configuration
        // Use DB_PAGE_SIZE for alignment to ensure optimal direct I/O performance
        let db_direct_io_config = crate::storage::disk::direct_io::DirectIOConfig {
            enabled: config.direct_io,
            alignment: DB_PAGE_SIZE as usize,
        };
        // WAL/log should remain OS-buffered: it is append-heavy and variable-length, and
        // O_DIRECT/NO_BUFFERING-style constraints (alignment of offset/size/pointer) are a poor fit.
        let log_direct_io_config = crate::storage::disk::direct_io::DirectIOConfig {
            enabled: false,
            alignment: DB_PAGE_SIZE as usize,
        };

        // Open database and log files with direct I/O support
        debug!(
            "Opening database file with direct_io={}: {}",
            config.direct_io, db_file_path
        );
        let db_file_std = crate::storage::disk::direct_io::open_direct_io(
            &db_file_path,
            true,
            true,
            true,
            &db_direct_io_config,
        )?;
        let db_file = Arc::new(Mutex::new(File::from_std(db_file_std)));

        debug!(
            "Opening log file with OS-buffered I/O (direct_io=false): {}",
            log_file_path
        );
        let log_file_std = crate::storage::disk::direct_io::open_direct_io(
            &log_file_path,
            true,
            true,
            true,
            &log_direct_io_config,
        )?;
        let log_file = Arc::new(Mutex::new(File::from_std(log_file_std)));

        // Create IO engine with direct I/O configuration
        debug!(
            "Initializing AsyncIOEngine with db_direct_io={}, db_alignment={}, wal_buffered=true",
            db_direct_io_config.enabled, db_direct_io_config.alignment
        );
        let mut io_engine_instance =
            AsyncIOEngine::with_config(db_file.clone(), log_file.clone(), db_direct_io_config)?;

        // Start the IO engine with configured number of worker threads
        debug!(
            "Starting AsyncIOEngine with {} worker threads",
            config.io_threads
        );
        io_engine_instance.start(config.io_threads);

        let io_engine = Arc::new(RwLock::new(io_engine_instance));

        // Create metrics collector
        debug!("Initializing MetricsCollector");
        let metrics_collector = Arc::new(MetricsCollector::new(&config));

        // Create cache manager
        debug!("Initializing CacheManager");
        let cache_manager = Arc::new(CacheManager::new(&config));

        // Create write manager
        debug!("Initializing WriteManager");
        let write_manager = Arc::new(WriteManager::new(&config));

        // Create scheduler
        debug!(
            "Initializing WorkStealingScheduler with {} threads",
            config.io_threads
        );
        let scheduler = Arc::new(WorkStealingScheduler::new(config.io_threads));

        // Create AsyncDiskManager
        let manager = Self {
            io_engine,
            write_manager,
            cache_manager,
            config,
            scheduler,
            metrics_collector,
            is_shutting_down: Arc::new(AtomicBool::new(false)),
            background_tasks: Mutex::new(Vec::new()),
        };

        info!(
            "AsyncDiskManager successfully initialized with {} worker threads",
            manager.config.io_threads
        );
        // Start background tasks

        Ok(manager)
    }

    /// Reads a page from disk or cache
    pub async fn read_page(&self, page_id: PageId) -> IoResult<Vec<u8>> {
        self.read_page_with_priority(page_id, IOPriority::Normal)
            .await
    }

    /// Reads a page with specified priority using scheduler
    pub async fn read_page_with_priority(
        &self,
        page_id: PageId,
        priority: IOPriority,
    ) -> IoResult<Vec<u8>> {
        trace!("Reading page: {} with priority: {:?}", page_id, priority);
        let start_time = Instant::now();

        // Try to get page from cache if enabled
        if self.config.cache_size_mb > 0
            && let Some(data) = self
                .cache_manager
                .get_page_with_metrics(page_id, &self.metrics_collector)
        {
            let elapsed = start_time.elapsed();
            debug!(
                "Cache hit for page {} (size: {} bytes, elapsed: {:?})",
                page_id,
                data.len(),
                elapsed
            );
            let elapsed_ns = elapsed.as_nanos() as u64;
            self.metrics_collector
                .record_read(elapsed_ns, data.len() as u64, true);
            return Ok(data);
        }

        // Page not in cache, schedule read operation
        debug!(
            "Cache miss for page {}, scheduling disk read with priority {:?}",
            page_id, priority
        );

        // Use scheduler if work-stealing is enabled
        if self.config.work_stealing_enabled {
            self.schedule_read_operation(page_id, priority, start_time)
                .await
        } else {
            // Direct read without scheduling
            self.direct_read_operation(page_id, start_time).await
        }
    }

    /// Schedules a read operation through the work-stealing scheduler
    async fn schedule_read_operation(
        &self,
        page_id: PageId,
        priority: IOPriority,
        start_time: Instant,
    ) -> IoResult<Vec<u8>> {
        let (sender, receiver) = oneshot::channel();

        let task = IOTask {
            task_type: IOTaskType::Read(page_id),
            priority,
            creation_time: start_time,
            completion_callback: Some(sender),
        };

        debug!("Submitting read task for page {} to scheduler", page_id);
        if let Err(e) = self.scheduler.submit_task(task).await {
            error!("Failed to submit read task for page {}: {:?}", page_id, e);
            return Err(std::io::Error::other(format!(
                "Failed to submit read task: {:?}",
                e
            )));
        }

        // Wait for completion
        match receiver.await {
            Ok(Ok(data)) => {
                let elapsed = start_time.elapsed();
                debug!(
                    "Scheduled read completed for page {} (size: {} bytes, elapsed: {:?})",
                    page_id,
                    data.len(),
                    elapsed
                );
                let elapsed_ns = elapsed.as_nanos() as u64;
                self.metrics_collector
                    .record_read(elapsed_ns, data.len() as u64, false);

                // Store in cache for future reads if caching is enabled
                if self.config.cache_size_mb > 0 {
                    trace!("Storing page {} in cache", page_id);
                    self.cache_manager.store_page(page_id, data.clone());
                }

                Ok(data)
            },
            Ok(Err(e)) => {
                error!("Scheduled read failed for page {}: {}", page_id, e);
                Err(e)
            },
            Err(_) => {
                error!("Read task was cancelled for page {}", page_id);
                Err(std::io::Error::other("Read task was cancelled"))
            },
        }
    }

    /// Performs a direct read operation without scheduling
    async fn direct_read_operation(
        &self,
        page_id: PageId,
        start_time: Instant,
    ) -> IoResult<Vec<u8>> {
        let data = self.io_engine.read().await.read_page(page_id).await?;

        // Update metrics
        let elapsed = start_time.elapsed();
        debug!(
            "Direct read completed for page {} (size: {} bytes, elapsed: {:?})",
            page_id,
            data.len(),
            elapsed
        );
        let elapsed_ns = elapsed.as_nanos() as u64;
        self.metrics_collector
            .record_read(elapsed_ns, data.len() as u64, false);

        // Store in cache for future reads if caching is enabled
        if self.config.cache_size_mb > 0 {
            trace!("Storing page {} in cache", page_id);
            self.cache_manager.store_page(page_id, data.clone());
        }

        Ok(data)
    }

    #[inline]
    fn validate_page_data_len(len: usize) -> IoResult<()> {
        if len != DB_PAGE_SIZE as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Invalid page data size: expected {} bytes, got {} bytes",
                    DB_PAGE_SIZE, len
                ),
            ));
        }
        Ok(())
    }

    /// Writes a page to the write buffer and eventually to disk
    pub async fn write_page(&self, page_id: PageId, data: Vec<u8>) -> IoResult<()> {
        self.write_page_with_priority(page_id, data, IOPriority::Normal)
            .await
    }

    /// Writes a page with specified priority
    pub async fn write_page_with_priority(
        &self,
        page_id: PageId,
        data: Vec<u8>,
        priority: IOPriority,
    ) -> IoResult<()> {
        Self::validate_page_data_len(data.len())?;
        trace!(
            "Writing page: {} (size: {} bytes) with priority: {:?}",
            page_id,
            data.len(),
            priority
        );
        let start_time = Instant::now();

        // Buffer the write using write manager
        debug!("Buffering write for page {}", page_id);
        let flushed_pages = self
            .write_manager
            .buffer_write(page_id, data.clone(), self)
            .await?;

        // Update cache if caching is enabled
        if self.config.cache_size_mb > 0 {
            trace!("Updating cache for page {}", page_id);
            self.cache_manager.store_page(page_id, data.clone());
        }

        // Write any flushed pages to disk
        if let Some(pages) = flushed_pages {
            debug!(
                "Writing {} flushed pages to disk from buffer_write",
                pages.len()
            );
            self.write_pages_to_disk(pages).await?;
        }

        // Update metrics
        let elapsed = start_time.elapsed();
        debug!(
            "Write buffered for page {} (elapsed: {:?})",
            page_id, elapsed
        );
        let elapsed_ns = elapsed.as_nanos() as u64;
        self.metrics_collector
            .record_write(elapsed_ns, data.len() as u64);

        // Check if we need to flush based on configuration (this should no longer trigger since we handled flushed pages above)
        if self.should_trigger_flush().await {
            debug!("Write buffer threshold reached, triggering flush");
            self.flush_writes_with_durability().await?;
        }

        Ok(())
    }

    /// Determines if a flush should be triggered based on configuration
    async fn should_trigger_flush(&self) -> bool {
        // Check write buffer threshold
        if self.write_manager.should_flush().await {
            return true;
        }

        // Check if we have enough dirty pages to warrant batch processing
        let stats = self.write_manager.get_buffer_stats().await;
        if stats.dirty_pages >= self.config.batch_size {
            debug!(
                "Batch size threshold reached: {} >= {}",
                stats.dirty_pages, self.config.batch_size
            );
            return true;
        }

        false
    }

    /// Flushes all buffered writes to disk and applies durability policies
    pub async fn flush_writes_with_durability(&self) -> IoResult<()> {
        debug!("Flushing writes with durability");
        let start_time = std::time::Instant::now();

        // Get buffered pages to flush
        let pages = self.write_manager.force_flush(self).await?;

        if !pages.is_empty() {
            debug!("Flushing {} pages to disk", pages.len());
            self.write_pages_to_disk(pages.clone()).await?;
        }

        // Apply durability based on configuration
        // Note: write_manager.force_flush(self) already applies durability if configured,
        // but we might want to double check here if the pages were empty (no flush triggered internally)
        // or if there are other pending writes.
        match self.config.fsync_policy {
            FsyncPolicy::OnFlush | FsyncPolicy::PerWrite | FsyncPolicy::Periodic(_) => {
                debug!(
                    "Applying sync based on fsync policy: {:?}",
                    self.config.fsync_policy
                );
                self.sync().await?;
            },
            FsyncPolicy::Never => {
                debug!("Skipping sync due to fsync policy: Never");
            },
        }

        // Record flush metrics
        let elapsed_ns = start_time.elapsed().as_nanos() as u64;
        self.metrics_collector.record_flush(elapsed_ns, pages.len());

        Ok(())
    }

    /// Schedules a batch write operation through the scheduler
    async fn schedule_batch_write_operation(&self, pages: Vec<(PageId, Vec<u8>)>) -> IoResult<()> {
        let (sender, receiver) = oneshot::channel();

        let task = IOTask {
            task_type: IOTaskType::BatchWrite(pages.clone()),
            priority: IOPriority::High, // Flush operations are high priority
            creation_time: Instant::now(),
            completion_callback: Some(sender),
        };

        debug!(
            "Submitting batch write task for {} pages to scheduler",
            pages.len()
        );
        if let Err(e) = self.scheduler.submit_task(task).await {
            error!("Failed to submit batch write task: {:?}", e);
            return Err(std::io::Error::other(format!(
                "Failed to submit batch write task: {:?}",
                e
            )));
        }

        // Wait for completion
        match receiver.await {
            Ok(Ok(_)) => {
                debug!("Scheduled batch write completed for {} pages", pages.len());
                Ok(())
            },
            Ok(Err(e)) => {
                error!("Scheduled batch write failed: {}", e);
                Err(e)
            },
            Err(_) => {
                error!("Batch write task was cancelled");
                Err(std::io::Error::other("Batch write task was cancelled"))
            },
        }
    }

    /// Writes multiple pages to disk
    async fn write_pages_to_disk(&self, pages: Vec<(PageId, Vec<u8>)>) -> IoResult<()> {
        debug!("Writing {} pages to disk", pages.len());
        let start_time = Instant::now();
        let mut total_bytes = 0;

        let io_engine = self.io_engine.read().await;

        for (page_id, data) in &pages {
            trace!(
                "Writing page {} to disk (size: {} bytes)",
                page_id,
                data.len()
            );
            io_engine.write_page(*page_id, data).await?;
            total_bytes += data.len() as u64;
        }

        // Apply durability policy if needed
        debug!("Applying durability policy for {} pages", pages.len());
        self.write_manager.apply_durability(&pages, self).await?;

        // Update metrics
        let elapsed = start_time.elapsed();
        debug!(
            "Disk write completed for {} pages (total: {} bytes, elapsed: {:?})",
            pages.len(),
            total_bytes,
            elapsed
        );
        let elapsed_ns = elapsed.as_nanos() as u64;
        self.update_batch_metrics(pages.len(), total_bytes, elapsed_ns);

        Ok(())
    }

    /// Reads multiple pages in a batch operation
    pub async fn read_pages_batch(&self, page_ids: Vec<PageId>) -> IoResult<Vec<Vec<u8>>> {
        debug!("Reading batch of {} pages", page_ids.len());
        let start_time = Instant::now();

        // Use configured batch size to process in chunks
        let batch_size = self.config.batch_size;
        let mut results = Vec::with_capacity(page_ids.len());
        let mut total_bytes = 0;

        for chunk in page_ids.chunks(batch_size) {
            let chunk_results = if self.config.work_stealing_enabled && chunk.len() > 1 {
                self.schedule_batch_read_operation(chunk.to_vec()).await?
            } else {
                self.direct_batch_read_operation(chunk.to_vec()).await?
            };

            // Calculate total bytes for metrics
            for data in &chunk_results {
                total_bytes += data.len() as u64;
            }

            results.extend(chunk_results);
        }

        // Update batch read metrics
        let elapsed = start_time.elapsed();
        let elapsed_ns = elapsed.as_nanos() as u64;
        self.update_batch_read_metrics(results.len(), total_bytes, elapsed_ns);

        debug!(
            "Batch read completed for {} pages (total: {} bytes, elapsed: {:?})",
            results.len(),
            total_bytes,
            elapsed
        );
        Ok(results)
    }

    /// Schedules a batch read operation through the scheduler
    async fn schedule_batch_read_operation(&self, page_ids: Vec<PageId>) -> IoResult<Vec<Vec<u8>>> {
        let start_time = Instant::now();
        let (sender, receiver) = oneshot::channel();

        let task = IOTask {
            task_type: IOTaskType::BatchRead(page_ids.clone()),
            priority: IOPriority::Normal,
            creation_time: start_time,
            completion_callback: Some(sender),
        };

        debug!(
            "Submitting batch read task for {} pages to scheduler",
            page_ids.len()
        );
        if let Err(e) = self.scheduler.submit_task(task).await {
            error!("Failed to submit batch read task: {:?}", e);
            return Err(std::io::Error::other(format!(
                "Failed to submit batch read task: {:?}",
                e
            )));
        }

        // Wait for completion
        match receiver.await {
            Ok(Ok(data)) => {
                debug!(
                    "Scheduled batch read completed for {} pages",
                    page_ids.len()
                );

                // Parse the combined data back into individual pages
                // This is a simplified approach - in practice, you'd need proper serialization
                let chunk_size = data.len() / page_ids.len();
                let mut results = Vec::new();
                for i in 0..page_ids.len() {
                    let start = i * chunk_size;
                    let end = if i == page_ids.len() - 1 {
                        data.len()
                    } else {
                        start + chunk_size
                    };
                    results.push(data[start..end].to_vec());
                }

                // Record individual metrics for this chunk
                let elapsed = start_time.elapsed();
                let elapsed_ns = elapsed.as_nanos() as u64;
                self.update_batch_read_metrics(results.len(), data.len() as u64, elapsed_ns);

                Ok(results)
            },
            Ok(Err(e)) => {
                error!("Scheduled batch read failed: {}", e);
                Err(e)
            },
            Err(_) => {
                error!("Batch read task was cancelled");
                Err(std::io::Error::other("Batch read task was cancelled"))
            },
        }
    }

    /// Performs direct batch read operation without scheduling
    async fn direct_batch_read_operation(&self, page_ids: Vec<PageId>) -> IoResult<Vec<Vec<u8>>> {
        let start_time = Instant::now();
        let mut results = Vec::with_capacity(page_ids.len());
        let mut total_bytes = 0;

        for page_id in page_ids.iter() {
            let data = self.read_page(*page_id).await?;
            total_bytes += data.len() as u64;
            results.push(data);
        }

        // Record metrics for this direct batch read
        let elapsed = start_time.elapsed();
        let elapsed_ns = elapsed.as_nanos() as u64;
        self.update_batch_read_metrics(results.len(), total_bytes, elapsed_ns);

        Ok(results)
    }

    /// Writes multiple pages in a batch operation
    pub async fn write_pages_batch(&self, pages: Vec<(PageId, Vec<u8>)>) -> IoResult<()> {
        debug!("Writing batch of {} pages", pages.len());
        let start_time = Instant::now();
        let mut total_bytes = 0;

        // Use configured batch size to process in chunks
        let batch_size = self.config.batch_size;

        for chunk in pages.chunks(batch_size) {
            // Buffer all writes in this chunk
            for (page_id, data) in chunk {
                Self::validate_page_data_len(data.len())?;
                trace!("Buffering page {} in batch write", page_id);
                let flushed_pages = self
                    .write_manager
                    .buffer_write(*page_id, data.clone(), self)
                    .await?;

                // Write any flushed pages to disk immediately
                if let Some(pages) = flushed_pages {
                    debug!(
                        "Writing {} flushed pages to disk from batch write",
                        pages.len()
                    );
                    self.write_pages_to_disk(pages).await?;
                }

                // Update cache if caching is enabled
                if self.config.cache_size_mb > 0 {
                    self.cache_manager.store_page(*page_id, data.clone());
                }

                total_bytes += data.len() as u64;
            }
        }

        // Flush if needed
        if self.should_trigger_flush().await {
            debug!("Batch write triggered flush");
            self.flush_writes_with_durability().await?;
        }

        // Update metrics
        let elapsed = start_time.elapsed();
        debug!(
            "Batch write completed for {} pages (total: {} bytes, elapsed: {:?})",
            pages.len(),
            total_bytes,
            elapsed
        );
        let elapsed_ns = elapsed.as_nanos() as u64;
        self.update_batch_metrics(pages.len(), total_bytes, elapsed_ns);

        Ok(())
    }

    /// Updates metrics for batch operations
    fn update_batch_metrics(&self, page_count: usize, total_bytes: u64, latency_ns: u64) {
        debug!(
            "Updating batch metrics: {} pages, {} bytes, {} ns",
            page_count, total_bytes, latency_ns
        );
        // Use the new batch write method since most batch operations are writes in this context
        self.metrics_collector
            .record_batch_write(page_count, latency_ns, total_bytes);
    }

    /// Updates metrics for batch read operations specifically
    fn update_batch_read_metrics(&self, page_count: usize, total_bytes: u64, latency_ns: u64) {
        debug!(
            "Updating batch read metrics: {} pages, {} bytes, {} ns",
            page_count, total_bytes, latency_ns
        );
        self.metrics_collector
            .record_batch_read(page_count, latency_ns, total_bytes);
    }

    /// Flushes all pending writes to disk
    pub async fn flush(&self) -> IoResult<()> {
        debug!("Explicit flush requested");
        let start_time = Instant::now();

        // Flush write buffer
        self.flush_writes_with_durability().await?;

        // Sync to ensure durability based on configuration
        if matches!(
            self.config.fsync_policy,
            super::config::FsyncPolicy::OnFlush | super::config::FsyncPolicy::PerWrite
        ) {
            debug!(
                "Syncing to ensure durability per fsync policy: {:?}",
                self.config.fsync_policy
            );
            self.sync().await?;
        }

        // Update metrics
        let elapsed = start_time.elapsed();
        debug!("Flush completed (elapsed: {:?})", elapsed);
        let elapsed_ns = elapsed.as_nanos() as u64;
        let _ = elapsed_ns; // Use elapsed for future metrics if needed
        self.metrics_collector
            .get_live_metrics()
            .flush_count
            .fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Syncs all pending writes to disk
    pub async fn sync(&self) -> IoResult<()> {
        debug!("Syncing all pending writes to disk");
        let io_engine = self.io_engine.read().await;
        io_engine.sync().await
    }

    /// Syncs the WAL/log file to ensure durability.
    ///
    /// This uses a "direct" execution path (bypassing the queue/worker pipeline) so the
    /// recovery log flush thread can safely call it via `Handle::block_on` without risking
    /// deadlock.
    pub async fn sync_log_direct(&self) -> IoResult<()> {
        // Honor the configured durability policy.
        if matches!(self.config.fsync_policy, FsyncPolicy::Never) {
            return Ok(());
        }
        debug!(
            "Syncing WAL/log file (direct path) per fsync policy: {:?}",
            self.config.fsync_policy
        );
        let io_engine = self.io_engine.read().await;
        io_engine.sync_log_direct().await
    }

    /// Syncs the log file to disk (standard path)
    pub async fn sync_log(&self) -> IoResult<()> {
        debug!("Syncing WAL/log file");
        let io_engine = self.io_engine.read().await;
        io_engine.sync_log().await
    }

    /// Gets current metrics snapshot
    pub fn get_metrics(&self) -> MetricsSnapshot {
        debug!("Creating metrics snapshot");
        self.metrics_collector.create_metrics_snapshot()
    }

    /// Gets the metrics collector for direct access to raw metrics
    pub fn get_metrics_collector(&self) -> &Arc<MetricsCollector> {
        &self.metrics_collector
    }

    /// Gets write buffer statistics
    pub async fn get_write_buffer_stats(&self) -> WriteBufferStats {
        debug!("Getting write buffer statistics");
        self.write_manager.get_buffer_stats().await
    }

    /// Forces flush of all pending writes
    pub async fn force_flush_all(&self) -> IoResult<()> {
        warn!("Force flush all requested");
        let pages = self.write_manager.force_flush(self).await?;

        if !pages.is_empty() {
            debug!("Force flushing {} pages to disk", pages.len());

            // Use scheduler for large batches if work-stealing is enabled
            if self.config.work_stealing_enabled && pages.len() > self.config.batch_size {
                self.schedule_batch_write_operation(pages).await?;
            } else {
                self.write_pages_to_disk(pages).await?;
            }
        }

        // Force sync regardless of policy
        debug!("Force syncing to disk");
        self.sync().await?;

        info!("Force flush all completed");
        Ok(())
    }

    /// Gets cache statistics
    pub async fn get_cache_stats(&self) -> (u64, u64, f64) {
        debug!("Getting cache statistics");
        let stats = self.cache_manager.get_cache_statistics();
        debug!(
            "Cache stats - promotions: {}, demotions: {}, hit_ratio: {:.2}%",
            stats.promotion_count,
            stats.demotion_count,
            stats.overall_hit_ratio * 100.0
        );
        (
            stats.promotion_count,
            stats.demotion_count,
            stats.overall_hit_ratio,
        )
    }

    /// Performs a health check
    pub fn health_check(&self) -> bool {
        debug!("Performing health check");
        let health_score = self.metrics_collector.calculate_health_score();
        debug!("Health score: {}", health_score);

        // Use configurable health threshold if available, otherwise default
        let health_threshold = 50; // Could be made configurable
        let is_healthy = health_score > health_threshold;

        if is_healthy {
            debug!("System health: OK");
        } else {
            warn!(
                "System health: DEGRADED (score: {} < threshold: {})",
                health_score, health_threshold
            );
        }
        is_healthy
    }

    /// Starts the metrics monitoring system
    pub async fn start_monitoring(&self) -> IoResult<JoinHandle<()>> {
        info!("Starting metrics monitoring system");
        let handle = self.metrics_collector.start_monitoring();

        let mut tasks = self.background_tasks.lock().await;
        let handle_clone = tokio::spawn(async {
            debug!("Background monitoring task started");
            // Background monitoring task placeholder
        });
        tasks.push(handle_clone);

        Ok(handle)
    }

    /// Gets dashboard data for monitoring
    pub fn get_dashboard_data(&self) -> DashboardData {
        debug!("Generating dashboard data");
        let health_score = self.metrics_collector.calculate_health_score() as f64;

        // Use configuration to determine which metrics to include
        let iops = if self.config.metrics_enabled {
            self.calculate_iops()
        } else {
            0.0
        };

        // Get cache statistics if caching is enabled
        let (overall_hit_ratio, hot_cache_hit_ratio, warm_cache_hit_ratio, cold_cache_hit_ratio) =
            if self.config.cache_size_mb > 0 {
                let cache_stats = self.cache_manager.get_cache_statistics();
                (
                    self.calculate_cache_hit_ratio("overall", &cache_stats),
                    self.calculate_cache_hit_ratio("hot", &cache_stats),
                    self.calculate_cache_hit_ratio("warm", &cache_stats),
                    self.calculate_cache_hit_ratio("cold", &cache_stats),
                )
            } else {
                (0.0, 0.0, 0.0, 0.0)
            };

        DashboardData {
            timestamp: Instant::now().into(),
            health_score,
            performance: PerformanceDashboard {
                avg_read_latency_ms: 0.0,
                avg_write_latency_ms: 0.0,
                throughput_mb_sec: 0.0,
                iops,
                queue_depth: 0,
            },
            cache: CacheDashboard {
                overall_hit_ratio,
                hot_cache_hit_ratio,
                warm_cache_hit_ratio,
                cold_cache_hit_ratio,
                memory_usage_mb: 0,
                promotions_per_sec: 0.0,
                evictions_per_sec: 0.0,
            },
            storage: StorageDashboard {
                buffer_utilization: 0.0,
                compression_ratio: if self.config.compression_enabled {
                    1.0
                } else {
                    0.0
                },
                flush_frequency: 0.0,
                bytes_written_mb: 0.0,
                write_amplification: self.calculate_write_amplification(),
            },
            alerts: vec![],
        }
    }

    /// Gets trend data for a specific time range
    pub fn get_trend_data(&self, time_range: Duration) -> TrendData {
        debug!("Generating trend data for range: {:?}", time_range);
        // Implementation would depend on historical metrics storage
        TrendData {
            time_range,
            latency_trend: vec![],
            throughput_trend: vec![],
            cache_hit_ratio_trend: vec![],
            error_rate_trend: vec![],
            predictions: Vec::new(), // Simplified
        }
    }

    /// Gets a health report
    pub fn get_health_report(&self) -> HealthReport {
        debug!("Generating health report");
        let _metrics = self.metrics_collector.create_metrics_snapshot();

        HealthReport {
            overall_health: self.metrics_collector.calculate_health_score() as f64,
            component_health: ComponentHealth {
                io_engine: 0.0,
                cache_manager: 0.0,
                write_manager: 0.0,
                storage_engine: 0.0,
            },
            recommendations: Vec::new(), // Simplified
            uptime_seconds: 0,
            bottlenecks: vec![],
            last_error: None,
        }
    }

    /// Gets active alerts
    pub fn get_active_alerts(&self) -> Vec<AlertSummary> {
        debug!("Getting active alerts");
        Vec::new() // Simplified
    }

    /// Calculates current IOPS (I/O operations per second)
    fn calculate_iops(&self) -> f64 {
        let metrics = self.metrics_collector.get_live_metrics();
        let elapsed_sec = metrics.uptime_seconds.load(Ordering::Relaxed) as f64;

        if elapsed_sec > 0.0 {
            let read_ops = metrics.read_ops_count.load(Ordering::Relaxed) as f64;
            let iops = read_ops / elapsed_sec;
            trace!("Calculated IOPS: {:.2}", iops);
            iops
        } else {
            0.0
        }
    }

    /// Calculates cache hit ratio for a specific cache level
    fn calculate_cache_hit_ratio(&self, cache_level: &str, cache_stats: &CacheStatistics) -> f64 {
        let ratio = match cache_level {
            "hot" => cache_stats.hot_cache_hit_ratio,
            "warm" => cache_stats.warm_cache_hit_ratio,
            "cold" => cache_stats.cold_cache_hit_ratio,
            _ => cache_stats.overall_hit_ratio,
        };
        trace!("Cache hit ratio for {}: {:.2}%", cache_level, ratio * 100.0);
        ratio
    }

    /// Calculates write amplification
    fn calculate_write_amplification(&self) -> f64 {
        let metrics = self.metrics_collector.get_live_metrics();
        let logical_writes = metrics.write_ops_count.load(Ordering::Relaxed) as f64;
        let physical_writes = metrics.write_amplification.load(Ordering::Relaxed) as f64;

        let amplification = if logical_writes > 0.0 {
            physical_writes / logical_writes
        } else {
            0.0
        };
        trace!("Write amplification: {:.2}x", amplification);
        amplification
    }

    /// Exports metrics in Prometheus format
    pub fn export_prometheus_metrics(&self) -> String {
        debug!("Exporting metrics in Prometheus format");
        let metrics = self.metrics_collector.create_metrics_snapshot();
        let mut output = String::new();

        // Include configuration info in metrics
        output.push_str("# HELP ferrite_config_io_threads Number of configured I/O threads\n");
        output.push_str("# TYPE ferrite_config_io_threads gauge\n");
        output.push_str(&format!(
            "ferrite_config_io_threads {}\n",
            self.config.io_threads
        ));

        output.push_str("# HELP ferrite_config_cache_size_mb Cache size in MB\n");
        output.push_str("# TYPE ferrite_config_cache_size_mb gauge\n");
        output.push_str(&format!(
            "ferrite_config_cache_size_mb {}\n",
            self.config.cache_size_mb
        ));

        // Basic metrics
        output.push_str("# HELP ferrite_read_operations Total number of read operations\n");
        output.push_str("# TYPE ferrite_read_operations counter\n");
        output.push_str(&format!(
            "ferrite_read_operations {}\n",
            metrics.retry_count
        ));

        output.push_str("# HELP ferrite_write_operations Total number of write operations\n");
        output.push_str("# TYPE ferrite_write_operations counter\n");
        output.push_str(&format!(
            "ferrite_write_operations {}\n",
            metrics.retry_count
        ));

        // More metrics would be added here
        debug!("Prometheus metrics exported ({} bytes)", output.len());
        output
    }

    /// Reads log data from the log file
    pub async fn read_log(&self, offset: u64) -> IoResult<Vec<u8>> {
        debug!("Reading log data at offset: {}", offset);
        // Use configurable log read size
        let log_read_size = 1024; // Could be made configurable
        let result = self
            .io_engine
            .read()
            .await
            .read_log(offset, log_read_size)
            .await;
        match &result {
            Ok(data) => debug!(
                "Log read successful: {} bytes at offset {}",
                data.len(),
                offset
            ),
            Err(e) => error!("Log read failed at offset {}: {}", offset, e),
        }
        result
    }

    /// Reads an exact number of bytes from the log file at the specified offset.
    ///
    /// This is the primitive that higher-level WAL utilities (like `LogIterator`) should use.
    /// Note: the underlying I/O layer uses `read_exact`, so this will return `UnexpectedEof`
    /// if the requested range extends past the end of the file.
    pub async fn read_log_sized(&self, offset: u64, size: usize) -> IoResult<Vec<u8>> {
        debug!("Reading log data at offset: {} (size={})", offset, size);
        self.io_engine.read().await.read_log(offset, size).await
    }

    /// Writes a log record to the log file
    pub async fn write_log(&self, log_record: &LogRecord) -> IoResult<u64> {
        debug!("Writing log record: {:?}", log_record);
        // Serialize using the storage bincode config (LogRecord::to_bytes)
        let bytes = log_record.to_bytes().map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to serialize log record: {e}"),
            )
        })?;

        // Append to the log file and return the offset.
        // Note: when direct I/O is enabled, the I/O layer may pad the write to alignment.
        // Readers should use the record's own `size` (logical) and advance offsets with
        // the same alignment policy.
        let offset = self
            .io_engine
            .read()
            .await
            .append_log_direct(&bytes)
            .await?;
        Ok(offset)
    }

    /// Gets configuration information
    pub fn get_config(&self) -> &DiskManagerConfig {
        &self.config
    }

    /// Gets the current size of the database file in bytes
    pub async fn get_db_file_size(&self) -> IoResult<u64> {
        debug!("Getting database file size");
        let io_engine = self.io_engine.read().await;
        io_engine.get_db_file_size().await
    }

    /// Updates configuration at runtime (for supported settings)
    pub fn update_runtime_config(&mut self, new_config: DiskManagerConfig) {
        debug!("Updating runtime configuration");

        // Only update certain runtime-configurable parameters
        if self.config.metrics_enabled != new_config.metrics_enabled {
            info!(
                "Metrics enabled changed: {} -> {}",
                self.config.metrics_enabled, new_config.metrics_enabled
            );
            self.config.metrics_enabled = new_config.metrics_enabled;
        }

        if self.config.prefetch_enabled != new_config.prefetch_enabled {
            info!(
                "Prefetch enabled changed: {} -> {}",
                self.config.prefetch_enabled, new_config.prefetch_enabled
            );
            self.config.prefetch_enabled = new_config.prefetch_enabled;
        }

        if self.config.prefetch_distance != new_config.prefetch_distance {
            info!(
                "Prefetch distance changed: {} -> {}",
                self.config.prefetch_distance, new_config.prefetch_distance
            );
            self.config.prefetch_distance = new_config.prefetch_distance;
        }

        // Note: Some settings like io_threads, cache_size_mb require restart
        if self.config.io_threads != new_config.io_threads {
            warn!(
                "IO threads change requires restart: {} -> {}",
                self.config.io_threads, new_config.io_threads
            );
        }
    }

    /// Shuts down the disk manager
    pub async fn shutdown(&mut self) -> IoResult<()> {
        info!("Shutting down AsyncDiskManager");
        self.is_shutting_down.store(true, Ordering::SeqCst);

        // Flush all pending writes before shutdown
        debug!("Flushing all pending writes before shutdown");
        self.force_flush_all().await?;

        // Signal the IO engine to stop gracefully to prevent worker abort warnings
        debug!("Signaling AsyncIOEngine to stop gracefully");
        {
            let mut io_engine = self.io_engine.write().await;
            // Cancel all pending operations to allow workers to exit cleanly
            let cancelled = io_engine
                .cancel_all_pending_operations("System shutdown")
                .await;
            if cancelled > 0 {
                debug!("Cancelled {} pending operations during shutdown", cancelled);
            }
            // Clear the queue to prevent new operations from being processed
            let cleared = io_engine.clear_queue().await;
            if cleared > 0 {
                debug!("Cleared {} operations from queue during shutdown", cleared);
            }

            // Stop the engine and wait for workers to exit
            debug!("Stopping AsyncIOEngine and waiting for workers to exit");
            io_engine.stop().await;
        }

        // Cancel background tasks
        debug!("Cancelling background tasks");
        let mut tasks = self.background_tasks.lock().await;
        for (i, task) in tasks.drain(..).enumerate() {
            debug!("Aborting background task {}", i);
            task.abort();
        }

        info!("AsyncDiskManager shutdown completed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::async_disk::compression::CompressionAlgorithm;
    use crate::storage::disk::async_disk::config::{
        DiskManagerConfig, DurabilityLevel, FsyncPolicy,
    };
    use tempfile::TempDir;

    fn page_with_prefix(prefix: &[u8]) -> Vec<u8> {
        let mut page = vec![0u8; DB_PAGE_SIZE as usize];
        let n = std::cmp::min(prefix.len(), page.len());
        page[..n].copy_from_slice(&prefix[..n]);
        page
    }

    fn page_filled(byte: u8) -> Vec<u8> {
        vec![byte; DB_PAGE_SIZE as usize]
    }

    /// Helper function to create a test AsyncDiskManager with temporary files
    async fn create_test_manager() -> (AsyncDiskManager, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let db_path = temp_dir.path().join("test.db");
        let log_path = temp_dir.path().join("test.log");

        let config = DiskManagerConfig {
            io_threads: 2,
            cache_size_mb: 64,
            batch_size: 4,
            work_stealing_enabled: true,
            prefetch_enabled: true,
            prefetch_distance: 2,
            metrics_enabled: true,
            ..Default::default()
        };

        let manager = AsyncDiskManager::new(
            db_path.to_string_lossy().to_string(),
            log_path.to_string_lossy().to_string(),
            config,
        )
        .await
        .expect("Failed to create AsyncDiskManager");

        (manager, temp_dir)
    }

    /// Helper function to create a test manager with custom config
    async fn create_test_manager_with_config(
        config: DiskManagerConfig,
    ) -> (AsyncDiskManager, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let db_path = temp_dir.path().join("test.db");
        let log_path = temp_dir.path().join("test.log");

        let manager = AsyncDiskManager::new(
            db_path.to_string_lossy().to_string(),
            log_path.to_string_lossy().to_string(),
            config,
        )
        .await
        .expect("Failed to create AsyncDiskManager");

        (manager, temp_dir)
    }

    #[tokio::test]
    async fn test_manager_creation() {
        let (manager, _temp_dir) = create_test_manager().await;
        assert_eq!(manager.config.io_threads, 2);
        assert_eq!(manager.config.cache_size_mb, 64);
        assert_eq!(manager.config.batch_size, 4);
        assert!(manager.config.work_stealing_enabled);
        assert!(manager.config.prefetch_enabled);
    }

    #[tokio::test]
    async fn test_basic_read_write() {
        let (mut manager, _temp_dir) = create_test_manager().await;

        let page_id = 1;
        let test_data = page_with_prefix(&[1, 2, 3, 4, 5, 6, 7, 8]);

        // Write a page
        manager
            .write_page(page_id, test_data.clone())
            .await
            .expect("Failed to write page");

        // Flush to ensure data is written
        manager.flush().await.expect("Failed to flush");

        // Read the page back
        let read_data = manager
            .read_page(page_id)
            .await
            .expect("Failed to read page");

        assert_eq!(read_data, test_data);

        // Properly shutdown the manager to avoid hanging
        manager
            .shutdown()
            .await
            .expect("Failed to shutdown manager");
    }

    #[tokio::test]
    async fn test_priority_operations() {
        let (manager, _temp_dir) = create_test_manager().await;

        let page_id = 1;
        let test_data = page_with_prefix(&[1, 2, 3, 4, 5, 6, 7, 8]);

        // Write with high priority
        manager
            .write_page_with_priority(page_id, test_data.clone(), IOPriority::High)
            .await
            .expect("Failed to write page with priority");

        // Flush to ensure data is written
        manager.flush().await.expect("Failed to flush");

        // Read with critical priority
        let read_data = manager
            .read_page_with_priority(page_id, IOPriority::Critical)
            .await
            .expect("Failed to read page with priority");

        assert_eq!(read_data, test_data);
    }

    #[tokio::test]
    async fn test_batch_operations() {
        let (manager, _temp_dir) = create_test_manager().await;

        let page_ids = [1, 2, 3, 4, 5];
        let test_data = [
            page_with_prefix(&[1, 2, 3, 4]),
            page_with_prefix(&[5, 6, 7, 8]),
            page_with_prefix(&[9, 10, 11, 12]),
            page_with_prefix(&[13, 14, 15, 16]),
            page_with_prefix(&[17, 18, 19, 20]),
        ];

        // Prepare batch write data
        let batch_data: Vec<(PageId, Vec<u8>)> = page_ids
            .iter()
            .zip(test_data.iter())
            .map(|(&id, data)| (id, data.clone()))
            .collect();

        // Write batch
        manager
            .write_pages_batch(batch_data)
            .await
            .expect("Failed to write batch");

        assert!(
            manager
                .get_db_file_size()
                .await
                .expect("Failed to get database file size")
                > 0
        );
    }

    #[tokio::test]
    async fn test_cache_functionality() {
        let (manager, _temp_dir) = create_test_manager().await;

        let page_id = 1;
        let test_data = page_with_prefix(&[1, 2, 3, 4, 5, 6, 7, 8]);

        // Write a page
        manager
            .write_page(page_id, test_data.clone())
            .await
            .expect("Failed to write page");

        // First, read should be from cache (since we store in cache on write)
        let read_data1 = manager
            .read_page(page_id)
            .await
            .expect("Failed to read page");

        // Second, read should also be from cache
        let read_data2 = manager
            .read_page(page_id)
            .await
            .expect("Failed to read page");

        assert_eq!(read_data1, test_data);
        assert_eq!(read_data2, test_data);

        // Check cache statistics
        let (_promotions, _demotions, hit_ratio) = manager.get_cache_stats().await;
        assert!(hit_ratio > 0.0, "Cache hit ratio should be greater than 0");
    }

    #[tokio::test]
    async fn test_configuration_driven_behavior() {
        // Test with caching disabled
        let config_no_cache = DiskManagerConfig {
            cache_size_mb: 0, // Disable caching
            work_stealing_enabled: false,
            prefetch_enabled: false,
            metrics_enabled: false,
            ..Default::default()
        };

        let (manager, _temp_dir) = create_test_manager_with_config(config_no_cache).await;

        // Use page_id = 0 to write at offset 0, since reads expect full DB_PAGE_SIZE bytes
        let page_id = 0;
        // Write a full page of data to avoid early EOF when reading
        let test_data = vec![42u8; 4096];

        // Write a page
        manager
            .write_page(page_id, test_data.clone())
            .await
            .expect("Failed to write page");

        // Flush to ensure data is written
        manager.flush().await.expect("Failed to flush");

        // Read should work even without caching
        let read_data = manager
            .read_page(page_id)
            .await
            .expect("Failed to read page");

        assert_eq!(read_data, test_data);

        // Cache stats should show no activity
        let (_promotions, _demotions, hit_ratio) = manager.get_cache_stats().await;
        assert_eq!(
            hit_ratio, 0.0,
            "Cache hit ratio should be 0 when caching is disabled"
        );
    }

    #[tokio::test]
    async fn test_flush_operations() {
        let (mut manager, _temp_dir) = create_test_manager().await;

        let page_ids = [1, 2, 3];
        let test_data = vec![
            page_with_prefix(&[1, 2, 3, 4]),
            page_with_prefix(&[5, 6, 7, 8]),
            page_with_prefix(&[9, 10, 11, 12]),
        ];

        // Write multiple pages
        for (page_id, data) in page_ids.iter().zip(test_data.iter()) {
            manager
                .write_page(*page_id, data.clone())
                .await
                .expect("Failed to write page");
        }

        // Get buffer stats before flush
        let stats_before = manager.get_write_buffer_stats().await;
        assert!(stats_before.dirty_pages > 0);

        // Flush all writes
        manager.flush().await.expect("Failed to flush");

        // Get buffer stats after flush
        let stats_after = manager.get_write_buffer_stats().await;
        assert_eq!(stats_after.dirty_pages, 0);

        // Verify data can be read back
        for (page_id, expected_data) in page_ids.iter().zip(test_data.iter()) {
            let read_data = manager
                .read_page(*page_id)
                .await
                .expect("Failed to read page");
            assert_eq!(read_data, *expected_data);
        }

        manager
            .shutdown()
            .await
            .expect("Failed to shutdown manager");
    }

    #[tokio::test]
    async fn test_force_flush() {
        let (manager, _temp_dir) = create_test_manager().await;

        let page_id = 1;
        let test_data = page_with_prefix(&[1, 2, 3, 4, 5, 6, 7, 8]);

        // Write a page
        manager
            .write_page(page_id, test_data.clone())
            .await
            .expect("Failed to write page");

        // Force flush all
        manager
            .force_flush_all()
            .await
            .expect("Failed to force flush");

        // Verify data persistence
        let read_data = manager
            .read_page(page_id)
            .await
            .expect("Failed to read page");
        assert_eq!(read_data, test_data);
    }

    #[tokio::test]
    async fn test_health_check() {
        let (manager, _temp_dir) = create_test_manager().await;

        // Health check should return true for a new manager
        let is_healthy = manager.health_check();
        assert!(is_healthy, "New manager should be healthy");
    }

    #[tokio::test]
    async fn test_metrics() {
        let (manager, _temp_dir) = create_test_manager().await;

        let page_id = 1;
        let test_data = page_with_prefix(&[1, 2, 3, 4, 5, 6, 7, 8]);

        // Perform some operations
        manager
            .write_page(page_id, test_data.clone())
            .await
            .expect("Failed to write page");
        manager.flush().await.expect("Failed to flush");
        manager
            .read_page(page_id)
            .await
            .expect("Failed to read page");

        // Get dashboard data
        let dashboard = manager.get_dashboard_data();
        assert!(dashboard.health_score > 0.0);

        // Get write buffer stats
        let buffer_stats = manager.get_write_buffer_stats().await;
        assert!(buffer_stats.max_buffer_size > 0);
    }

    #[tokio::test]
    async fn test_prometheus_metrics() {
        let (manager, _temp_dir) = create_test_manager().await;

        // Export metrics
        let metrics_str = manager.export_prometheus_metrics();

        // Should contain configuration metrics
        assert!(metrics_str.contains("ferrite_config_io_threads"));
        assert!(metrics_str.contains("ferrite_config_cache_size_mb"));
        assert!(metrics_str.contains("ferrite_read_operations"));
        assert!(metrics_str.contains("ferrite_write_operations"));
    }

    #[tokio::test]
    async fn test_configuration_access() {
        let (manager, _temp_dir) = create_test_manager().await;

        // Get configuration
        let config = manager.get_config();
        assert_eq!(config.io_threads, 2);
        assert_eq!(config.cache_size_mb, 64);
        assert_eq!(config.batch_size, 4);
    }

    #[tokio::test]
    async fn test_runtime_config_update() {
        let (mut manager, _temp_dir) = create_test_manager().await;

        // Update some runtime configurations
        let mut new_config = manager.config.clone();
        new_config.prefetch_enabled = false;
        new_config.metrics_enabled = false;
        new_config.prefetch_distance = 5;

        manager.update_runtime_config(new_config);

        // Verify changes
        assert!(!manager.config.prefetch_enabled);
        assert!(!manager.config.metrics_enabled);
        assert_eq!(manager.config.prefetch_distance, 5);
    }

    #[tokio::test]
    async fn test_fsync_policy_behavior() {
        // Test with different fsync policies
        let config_sync_on_flush = DiskManagerConfig {
            fsync_policy: FsyncPolicy::OnFlush,
            durability_level: DurabilityLevel::Sync,
            ..Default::default()
        };

        let (manager, _temp_dir) = create_test_manager_with_config(config_sync_on_flush).await;

        let page_id = 1;
        let test_data = page_with_prefix(&[1, 2, 3, 4, 5, 6, 7, 8]);

        // Write and flush
        manager
            .write_page(page_id, test_data.clone())
            .await
            .expect("Failed to write page");
        manager.flush().await.expect("Failed to flush");

        // Data should be persisted
        let read_data = manager
            .read_page(page_id)
            .await
            .expect("Failed to read page");
        assert_eq!(read_data, test_data);
    }

    #[tokio::test]
    async fn test_batch_size_limits() {
        let config_small_batch = DiskManagerConfig {
            batch_size: 2,                // Small batch size
            cache_size_mb: 64,            // Enable cache so reads come from cache
            work_stealing_enabled: false, // Disable to avoid scheduler hanging
            ..Default::default()
        };

        let (manager, _temp_dir) = create_test_manager_with_config(config_small_batch).await;

        // Write more pages than batch size - use full page size (4096 bytes)
        // Start from page 0 to write at offset 0
        let pages: Vec<(PageId, Vec<u8>)> = (0..5).map(|i| (i, vec![i as u8; 4096])).collect();

        manager
            .write_pages_batch(pages.clone())
            .await
            .expect("Failed to write batch");

        manager.flush().await.expect("Failed to flush");

        // Read all pages back (will come from cache since caching is enabled)
        let page_ids: Vec<PageId> = pages.iter().map(|(id, _)| *id).collect();
        let read_data = manager
            .read_pages_batch(page_ids)
            .await
            .expect("Failed to read batch");

        let expected_data: Vec<Vec<u8>> = pages.iter().map(|(_, data)| data.clone()).collect();
        assert_eq!(read_data, expected_data);
    }

    #[tokio::test]
    async fn test_work_stealing_enabled_vs_disabled() {
        // Test with work stealing enabled
        let config_enabled = DiskManagerConfig {
            work_stealing_enabled: true,
            batch_size: 2,
            ..Default::default()
        };

        let (manager_enabled, _temp_dir1) = create_test_manager_with_config(config_enabled).await;

        // Test with work stealing disabled
        let config_disabled = DiskManagerConfig {
            work_stealing_enabled: false,
            batch_size: 2,
            ..Default::default()
        };

        let (manager_disabled, _temp_dir2) = create_test_manager_with_config(config_disabled).await;

        let test_data = page_with_prefix(&[1, 2, 3, 4, 5, 6, 7, 8]);

        // Both should work with same functionality
        manager_enabled
            .write_page(1, test_data.clone())
            .await
            .expect("Failed to write with scheduler enabled");
        manager_enabled.flush().await.expect("Failed to flush");

        manager_disabled
            .write_page(1, test_data.clone())
            .await
            .expect("Failed to write with scheduler disabled");
        manager_disabled.flush().await.expect("Failed to flush");

        // Both should read back the same data
        let read_enabled = manager_enabled
            .read_page(1)
            .await
            .expect("Failed to read with scheduler enabled");
        let read_disabled = manager_disabled
            .read_page(1)
            .await
            .expect("Failed to read with scheduler disabled");

        assert_eq!(read_enabled, test_data);
        assert_eq!(read_disabled, test_data);
    }

    #[tokio::test]
    async fn test_error_handling() {
        // Use config without work-stealing to ensure direct error propagation
        let config = DiskManagerConfig {
            work_stealing_enabled: false,
            cache_size_mb: 0, // Disable cache to force disk read
            ..Default::default()
        };
        let (mut manager, _temp_dir) = create_test_manager_with_config(config).await;

        // Test reading non-existent page - should return an error
        // because the file doesn't have data at offset 999 * DB_PAGE_SIZE
        let result = manager.read_page(999).await;
        assert!(
            result.is_err(),
            "Reading non-existent page should return an error"
        );

        // Verify the error is related to EOF/missing data
        let error = result.unwrap_err();
        let error_str = error.to_string().to_lowercase();
        assert!(
            error_str.contains("eof") || error_str.contains("unexpected"),
            "Error should indicate missing data: {}",
            error
        );

        // Properly shutdown the manager to avoid hanging
        manager
            .shutdown()
            .await
            .expect("Failed to shutdown manager");
    }

    #[tokio::test]
    async fn test_concurrent_operations() {
        let (manager, _temp_dir) = create_test_manager().await;
        let manager = Arc::new(manager);

        // Spawn multiple concurrent write tasks
        let mut handles = vec![];
        for i in 1..=10 {
            let manager_clone = Arc::clone(&manager);
            let handle = tokio::spawn(async move {
                let data = page_filled(i as u8);
                manager_clone.write_page(i, data).await
            });
            handles.push(handle);
        }

        // Wait for all writes to complete
        for handle in handles {
            handle.await.unwrap().expect("Failed to write page");
        }

        // Flush all writes
        manager.flush().await.expect("Failed to flush");

        // Spawn multiple concurrent read tasks
        let mut handles = vec![];
        for i in 1..=10 {
            let manager_clone = Arc::clone(&manager);
            let handle = tokio::spawn(async move { manager_clone.read_page(i).await });
            handles.push(handle);
        }

        // Wait for all reads to complete
        for (i, handle) in handles.into_iter().enumerate() {
            let page_id = (i + 1) as PageId;
            let read_data = handle.await.unwrap().expect("Failed to read page");
            let expected_data = page_filled(page_id as u8);
            assert_eq!(read_data, expected_data);
        }
    }

    #[tokio::test]
    async fn test_monitoring_and_alerts() {
        let (manager, _temp_dir) = create_test_manager().await;

        // Start monitoring
        let _monitor_handle = manager
            .start_monitoring()
            .await
            .expect("Failed to start monitoring");

        // Perform some operations
        manager
            .write_page(1, page_with_prefix(&[1, 2, 3, 4]))
            .await
            .expect("Failed to write page");
        manager.flush().await.expect("Failed to flush");

        // Get health report
        let health_report = manager.get_health_report();
        assert!(health_report.overall_health >= 0.0);

        // Get trend data
        let trend_data = manager.get_trend_data(Duration::from_secs(60));
        assert_eq!(trend_data.time_range, Duration::from_secs(60));

        // Get active alerts
        let alerts = manager.get_active_alerts();
        // Should be empty for a healthy system
        assert!(alerts.is_empty());
    }

    #[tokio::test]
    async fn test_shutdown() {
        let (mut manager, _temp_dir) = create_test_manager().await;

        // Write some data
        manager
            .write_page(1, page_with_prefix(&[1, 2, 3, 4]))
            .await
            .expect("Failed to write page");

        // Shutdown should flush and complete successfully
        manager.shutdown().await.expect("Failed to shutdown");

        // Check that shutdown flag is set
        assert!(manager.is_shutting_down.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_large_data_handling() {
        let (mut manager, _temp_dir) = create_test_manager().await;

        // Large data belongs in the log, not a single fixed-size page.
        let large_data = vec![42u8; 1024 * 1024];

        let offset = manager
            .io_engine
            .read()
            .await
            .append_log(&large_data)
            .await
            .expect("Failed to append large log data");

        let read_back = manager
            .read_log_sized(offset, large_data.len())
            .await
            .expect("Failed to read back large log data");

        assert_eq!(read_back, large_data);

        // Properly shutdown the manager to avoid hanging
        manager
            .shutdown()
            .await
            .expect("Failed to shutdown manager");
    }

    #[tokio::test]
    async fn test_log_operations() {
        let (mut manager, _temp_dir) = create_test_manager().await;

        // Test log write using proper LogRecord constructor
        use crate::recovery::log_record::{LogRecord, LogRecordType};

        let log_record = LogRecord::new_transaction_record(
            1, // txn_id
            0, // prev_lsn
            LogRecordType::Begin,
        );

        // This should not panic
        let _result = manager.write_log(&log_record).await;

        // Test log read
        let _log_data = manager.read_log(0).await;
        // Should not panic regardless of success/failure

        // Properly shutdown the manager to avoid hanging
        manager
            .shutdown()
            .await
            .expect("Failed to shutdown manager");
    }

    #[tokio::test]
    async fn test_compression_configuration() {
        let config = DiskManagerConfig {
            compression_enabled: true,
            compression_algorithm: CompressionAlgorithm::LZ4,
            ..Default::default()
        };

        let (manager, _temp_dir) = create_test_manager_with_config(config).await;

        assert!(manager.config.compression_enabled);
        assert!(manager.config.compression_algorithm == CompressionAlgorithm::LZ4);

        // Test write with compression
        let test_data = vec![0x42; DB_PAGE_SIZE as usize]; // Compressible data
        let result = manager.write_page(100, test_data.clone()).await;
        assert!(result.is_ok(), "Write with compression should succeed");

        // Test read
        let read_result = manager.read_page(100).await;
        assert!(read_result.is_ok(), "Read with compression should succeed");
        let read_data = read_result.unwrap();
        assert_eq!(
            read_data, test_data,
            "Data should be correctly decompressed"
        );
    }

    #[tokio::test]
    async fn test_metrics_recording_integration() {
        let (manager, _temp_dir) = create_test_manager().await;

        // Perform read operation
        let test_data = vec![0x42; DB_PAGE_SIZE as usize];
        let write_result = manager.write_page(200, test_data.clone()).await;
        assert!(write_result.is_ok(), "Write should succeed");

        let read_result = manager.read_page(200).await;
        assert!(read_result.is_ok(), "Read should succeed");

        // Verify that metrics were recorded
        let live_metrics = manager.get_metrics_collector().get_live_metrics();
        let write_ops = live_metrics.write_ops_count.load(Ordering::Relaxed);

        // Note: Depending on caching, the read might come from cache or disk
        assert!(
            write_ops > 0,
            "Write operations should be recorded in metrics"
        );

        // Verify latency sums are populated
        let write_latency_sum = live_metrics.write_latency_sum.load(Ordering::Relaxed);

        assert!(write_latency_sum > 0, "Write latency should be recorded");
        // Read latency might be 0 if served from cache, which is acceptable

        // Test batch operations metrics
        let pages = vec![
            (300, vec![0x01; DB_PAGE_SIZE as usize]),
            (301, vec![0x02; DB_PAGE_SIZE as usize]),
            (302, vec![0x03; DB_PAGE_SIZE as usize]),
        ];

        let batch_write_result = manager.write_pages_batch(pages).await;
        assert!(batch_write_result.is_ok(), "Batch write should succeed");

        // Verify batch operations were recorded
        let batch_ops = live_metrics.batch_ops_count.load(Ordering::Relaxed);
        assert!(batch_ops > 0, "Batch operations should be recorded");

        // Test flush metrics
        let flush_result = manager.flush().await;
        assert!(flush_result.is_ok(), "Flush should succeed");

        // Force flush to ensure metrics are recorded
        let force_flush_result = manager.force_flush_all().await;
        assert!(force_flush_result.is_ok(), "Force flush should succeed");

        // Create metrics snapshot and verify it's populated correctly
        let snapshot = manager.get_metrics();
        assert!(
            snapshot.io_throughput_mb_per_sec >= 0.0,
            "Throughput should be non-negative"
        );
    }

    #[tokio::test]
    async fn test_cache_metrics_integration() {
        let config = DiskManagerConfig {
            cache_size_mb: 64, // Enable cache
            ..Default::default()
        };

        let (manager, _temp_dir) = create_test_manager_with_config(config).await;

        let test_data = vec![0x55; DB_PAGE_SIZE as usize];

        // First write and read (should be cache miss)
        let write_result = manager.write_page(400, test_data.clone()).await;
        assert!(write_result.is_ok(), "Write should succeed");

        let read_result1 = manager.read_page(400).await;
        assert!(read_result1.is_ok(), "First read should succeed");

        // Second read (should be cache hit)
        let read_result2 = manager.read_page(400).await;
        assert!(read_result2.is_ok(), "Second read should succeed");

        // Verify cache metrics were recorded
        let live_metrics = manager.get_metrics_collector().get_live_metrics();
        let cache_hits = live_metrics.cache_hits.load(Ordering::Relaxed);
        let cache_misses = live_metrics.cache_misses.load(Ordering::Relaxed);

        // We should have at least some cache activity
        let total_cache_ops = cache_hits + cache_misses;
        assert!(total_cache_ops > 0, "Should have cache operations recorded");

        // Create metrics snapshot and verify cache hit ratio calculation
        let snapshot = manager.get_metrics();
        assert!(
            snapshot.cache_hit_ratio >= 0.0 && snapshot.cache_hit_ratio <= 1.0,
            "Cache hit ratio should be between 0 and 1"
        );
    }

    #[tokio::test]
    async fn test_time_based_metrics_integration() {
        let (manager, _temp_dir) = create_test_manager().await;

        // Perform several operations over time
        for i in 0..5 {
            let data = vec![i as u8; DB_PAGE_SIZE as usize];
            let write_result = manager.write_page(500 + i, data).await;
            assert!(write_result.is_ok(), "Write {} should succeed", i);
        }

        // Wait at least 1 second for time-based metrics (update_performance_counters uses as_secs())
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Force an update of performance counters
        let collector = manager.get_metrics_collector();
        collector.update_performance_counters();

        // Verify time-based metrics are calculated
        let live_metrics = collector.get_live_metrics();
        let uptime = live_metrics.uptime_seconds.load(Ordering::Relaxed);

        assert!(uptime > 0, "Uptime should be tracked");

        // Create snapshot and verify time-based calculations
        let snapshot = manager.get_metrics();
        assert!(
            snapshot.io_throughput_mb_per_sec >= 0.0,
            "Throughput should be non-negative"
        );
        assert!(
            snapshot.flush_frequency_per_sec >= 0.0,
            "Flush frequency should be non-negative"
        );
        assert!(
            snapshot.error_rate_per_sec >= 0.0,
            "Error rate should be non-negative"
        );
    }

    #[tokio::test]
    async fn test_health_monitoring_integration() {
        let (manager, _temp_dir) = create_test_manager().await;

        // Perform normal operations (should maintain good health)
        for i in 0..3 {
            let data = vec![i as u8; DB_PAGE_SIZE as usize];
            let write_result = manager.write_page(600 + i, data).await;
            assert!(write_result.is_ok(), "Write should succeed");
        }

        // Check health
        let is_healthy = manager.health_check();
        assert!(
            is_healthy,
            "Manager should be healthy after normal operations"
        );

        // Get health score
        let health_score = manager.get_metrics_collector().calculate_health_score();
        assert!(
            health_score >= 50,
            "Health score should be reasonable: {}",
            health_score
        );

        // Get health report
        let health_report = manager.get_health_report();
        assert!(
            health_report.overall_health >= 0.0,
            "Health report should be valid"
        );
    }
}
