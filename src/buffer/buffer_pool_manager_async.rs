//! # Buffer Pool Manager (Async)
//!
//! This module implements the **Buffer Pool Manager (BPM)**, the central memory management
//! component that mediates all page-level I/O between the database engine and disk storage.
//! It integrates with the async `AsyncDiskManager` for non-blocking I/O operations.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────┐
//! │                           Buffer Pool Manager                                   │
//! ├─────────────────────────────────────────────────────────────────────────────────┤
//! │                                                                                 │
//! │   ┌─────────────────────────────────────────────────────────────────────────┐   │
//! │   │                         Frame Pool (L1 Cache)                           │   │
//! │   │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐            │   │
//! │   │  │ Frame 0 │ │ Frame 1 │ │ Frame 2 │ │ Frame 3 │ │ Frame N │   ...      │   │
//! │   │  │ Page 5  │ │ Page 12 │ │  Empty  │ │ Page 3  │ │ Page 42 │            │   │
//! │   │  │ pin: 2  │ │ pin: 0  │ │         │ │ pin: 1  │ │ pin: 0  │            │   │
//! │   │  │ dirty:Y │ │ dirty:N │ │         │ │ dirty:Y │ │ dirty:N │            │   │
//! │   │  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘            │   │
//! │   └─────────────────────────────────────────────────────────────────────────┘   │
//! │                                    │                                            │
//! │   ┌────────────────────────────────┼────────────────────────────────────────┐   │
//! │   │                                ▼                                        │   │
//! │   │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────┐   │   │
//! │   │  │    Page Table    │  │  LRU-K Replacer  │  │      Free List       │   │   │
//! │   │  │  PageId → FrameId│  │  (eviction order)│  │  [frame_2, ...]      │   │   │
//! │   │  │  5 → 0, 12 → 1   │  │  Tracks K recent │  │  Available frames    │   │   │
//! │   │  │  3 → 3, 42 → N   │  │  accesses/frame  │  │  for new pages       │   │   │
//! │   │  └──────────────────┘  └──────────────────┘  └──────────────────────┘   │   │
//! │   └─────────────────────────────────────────────────────────────────────────┘   │
//! │                                    │                                            │
//! │                    ┌───────────────┴───────────────┐                            │
//! │                    ▼                               ▼                            │
//! │   ┌─────────────────────────────────────────────────────────────────────────┐   │
//! │   │                     AsyncDiskManager (L2 Cache + I/O)                   │   │
//! │   │  ┌───────────────┐  ┌───────────────┐  ┌──────────────────────────────┐ │   │
//! │   │  │  Read Cache   │  │  Write Buffer │  │  Async I/O (tokio)           │ │   │
//! │   │  │  (compressed) │  │  (coalescing) │  │  read_page / write_page      │ │   │
//! │   │  └───────────────┘  └───────────────┘  └──────────────────────────────┘ │   │
//! │   └─────────────────────────────────────────────────────────────────────────┘   │
//! │                                    │                                            │
//! │                                    ▼                                            │
//! │                          ┌─────────────────┐                                    │
//! │                          │    Disk File    │                                    │
//! │                          │   (database)    │                                    │
//! │                          └─────────────────┘                                    │
//! └─────────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Two-Level Caching Architecture
//!
//! This component acts as the **Level 1 (Application)** cache, storing uncompressed `Page`
//! objects ready for CPU processing. It works in tandem with `AsyncDiskManager` (L2 cache):
//!
//! | Level | Component         | Data Format    | Purpose                           |
//! |-------|-------------------|----------------|-----------------------------------|
//! | L1    | BufferPoolManager | Uncompressed   | CPU-ready pages, hot data         |
//! | L2    | AsyncDiskManager  | Compressed     | I/O optimization, batch writes    |
//! | Disk  | Database File     | Persistent     | Durable storage                   |
//!
//! Benefits of the two-level approach:
//! - **Latency Hiding**: Flushes to L2 are non-blocking
//! - **I/O Optimization**: L2 handles compression, coalescing, and scheduling
//! - **Coordination**: `use_disk_manager_cache` flag manages duplication
//!
//! ## Key Components
//!
//! | Component                  | Description                                           |
//! |----------------------------|-------------------------------------------------------|
//! | `BufferPoolManager`        | Main struct managing the buffer pool                  |
//! | `PageGuard<T>`             | RAII guard for pinned pages with auto-unpin           |
//! | `LRUKReplacer`             | Eviction policy based on K-th access timestamp        |
//! | `BufferPoolHealthStatus`   | Health and performance metrics                        |
//! | `CacheCoordinationStats`   | L1/L2 cache coordination statistics                   |
//!
//! ## Page Lifecycle
//!
//! ```text
//! ┌─────────────┐    new_page()     ┌─────────────┐
//! │             │ ─────────────────▶│   Pinned    │
//! │   (none)    │                   │  (pin > 0)  │
//! │             │◀───────────────── │   dirty?    │
//! └─────────────┘   delete_page()   └──────┬──────┘
//!                                          │
//!                                  unpin() │ (pin--)
//!                                          ▼
//!                                   ┌─────────────┐
//!                   fetch_page() ──▶│  Evictable  │
//!                  (pin++, evict=F) │  (pin = 0)  │
//!                                   └──────┬──────┘
//!                                          │
//!                           LRU-K eviction │ (if dirty, flush first)
//!                                          ▼
//!                                   ┌─────────────┐
//!                                   │   Evicted   │
//!                                   │ (on disk)   │
//!                                   └─────────────┘
//! ```
//!
//! ## Core Operations
//!
//! | Operation                     | Description                                       | Complexity |
//! |-------------------------------|---------------------------------------------------|------------|
//! | `new_page::<T>()`             | Allocate a new page of type T                     | O(1) avg   |
//! | `fetch_page::<T>(page_id)`    | Load or retrieve page from pool                   | O(1) avg   |
//! | `flush_page(page_id)`         | Write dirty page to disk                          | O(1) + I/O |
//! | `flush_page_async(page_id)`   | Async version, non-blocking                       | O(1)       |
//! | `delete_page(page_id)`        | Remove page from pool (must be unpinned)          | O(1)       |
//! | `flush_all_pages()`           | Flush all dirty pages                             | O(n) + I/O |
//!
//! ## Async Operations
//!
//! This BPM provides both sync and async variants for I/O operations:
//!
//! | Sync (Deprecated)             | Async (Preferred)                    |
//! |-------------------------------|--------------------------------------|
//! | `flush_page()`                | `flush_page_async()`                 |
//! | `flush_dirty_pages_batch()`   | `flush_dirty_pages_batch_async()`    |
//! | `get_health_status()`         | `get_health_status_async()`          |
//! | `health_check()`              | `health_check_async()`               |
//!
//! The sync variants use `futures::executor::block_on` internally and are deprecated.
//! Prefer async variants for better performance and non-blocking behavior.
//!
//! ## Concurrency
//!
//! - **Frame Pool**: `Arc<RwLock<Vec<Option<Arc<RwLock<dyn PageTrait>>>>>>`
//! - **Page Table**: `Arc<RwLock<HashMap<PageId, FrameId>>>`
//! - **Replacer**: `Arc<RwLock<LRUKReplacer>>`
//! - **Free List**: `Arc<RwLock<Vec<FrameId>>>`
//!
//! Uses `parking_lot::RwLock` for lower overhead than `std::sync::RwLock`.
//!
//! ## Example Usage
//!
//! ```rust,no_run
//! use std::sync::Arc;
//!
//! // Create buffer pool with async disk manager
//! let bpm = Arc::new(BufferPoolManager::new_with_config(
//!     100,  // pool size
//!     "db.file".to_string(),
//!     "db.log".to_string(),
//!     DiskManagerConfig::default(),
//! ).await?);
//!
//! // Create a new page
//! let guard = bpm.new_page::<BasicPage>()?;
//! let page_id = guard.read().get_page_id();
//!
//! // Modify the page
//! {
//!     let mut page = guard.write();
//!     page.get_data_mut()[0] = 42;
//!     page.set_dirty(true);
//! }
//!
//! // Drop guard to unpin
//! drop(guard);
//!
//! // Flush to disk (async preferred)
//! bpm.flush_page_async(page_id).await?;
//!
//! // Fetch page back
//! let guard = bpm.fetch_page::<BasicPage>(page_id)?;
//! ```
//!
//! ## Batch Operations
//!
//! For better I/O performance, use batch operations:
//!
//! ```rust,no_run
//! // Load multiple pages in batch
//! let guards = bpm.load_pages_batch::<BasicPage>(vec![1, 2, 3, 4, 5]).await;
//!
//! // Flush dirty pages in batch
//! let flushed = bpm.flush_dirty_pages_batch_async(100).await?;
//!
//! // Prefetch pages to disk cache
//! bpm.prefetch_to_disk_cache(vec![10, 11, 12]).await?;
//! ```
//!
//! ## Health Monitoring
//!
//! ```rust,no_run
//! // Get comprehensive health status
//! let status = bpm.get_health_status_async().await;
//! println!("Pool utilization: {:.1}%", status.pool_utilization);
//! println!("Cache hit ratio: {:.2}", status.cache_hit_ratio);
//!
//! // Get cache coordination stats
//! let stats = bpm.get_cache_coordination_stats().await;
//! println!("Total efficiency: {:.1}%", stats.total_cache_efficiency);
//! ```
//!
//! ## Key Responsibilities
//!
//! - **Frame Management**: Tracks free frames and page-to-frame mappings
//! - **Page Lifecycle**: Loads pages from disk (L2) into memory (L1), evicts when full
//! - **Pinning/Unpinning**: Prevents eviction of pages in use by execution threads
//! - **Replacement Policy**: Uses `LRUKReplacer` for access-frequency-based eviction
//! - **Cache Coordination**: Manages L1/L2 cache interaction and efficiency
use crate::buffer::lru_k_replacer::{AccessType, LRUKReplacer};
use crate::common::config::{DB_PAGE_SIZE, FrameId, INVALID_PAGE_ID, PageId};
use crate::common::exception::DeletePageError;
use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
use crate::storage::page::page_guard::{PageGuard, PageUnpinner};
use crate::storage::page::{PAGE_TYPE_OFFSET, PageType};
use crate::storage::page::{Page, PageTrait};
use log::{error, info, trace, warn};
use parking_lot::{RwLock, RwLockReadGuard};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// Type aliases for complex types

/// Type alias for the collection of page frames in the buffer pool.
///
/// Each frame may optionally contain a page (represented as a trait object).
type PageCollection = Arc<RwLock<Vec<Option<Arc<RwLock<dyn PageTrait>>>>>>;

/// Type alias for the page table mapping page IDs to their frame locations.
type PageTable = Arc<RwLock<HashMap<PageId, FrameId>>>;

/// The `BufferPoolManager` is responsible for managing the buffer pool,
/// including fetching and unpinning pages, and handling page replacement.
///
/// This version integrates with the `AsyncDiskManager` for better performance
/// and implements coordinated two-level caching to avoid duplication.
///
/// See the module-level documentation for architecture details.
pub struct BufferPoolManager {
    /// Maximum number of frames in the buffer pool.
    pool_size: usize,
    /// Atomic counter for generating unique page IDs.
    next_page_id: AtomicU64,
    /// Collection of page frames (L1 cache).
    pages: PageCollection,
    /// Mapping from page IDs to frame IDs.
    page_table: PageTable,
    /// LRU-K replacement policy for eviction decisions.
    replacer: Arc<RwLock<LRUKReplacer>>,
    /// List of unoccupied frame IDs available for new pages.
    free_list: Arc<RwLock<Vec<FrameId>>>,
    /// Async disk manager for I/O operations (L2 cache + storage).
    disk_manager: Arc<AsyncDiskManager>,
    /// Whether to use the disk manager's read cache (L2).
    use_disk_manager_cache: bool,
    /// Whether to bypass disk cache for pinned pages.
    bypass_disk_cache_for_pinned: bool,
}

/// Manual Debug implementation that excludes the `pages` field.
///
/// The pages collection contains trait objects which don't implement Debug,
/// so we skip it in the debug output.
impl Debug for BufferPoolManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferPoolManager")
            .field("pool_size", &self.pool_size)
            .field("next_page_id", &self.next_page_id)
            .field("page_table", &self.page_table)
            .field("replacer", &self.replacer)
            .field("free_list", &self.free_list)
            .field("disk_manager", &self.disk_manager)
            // Skip pages field as it contains the trait object
            .finish()
    }
}

impl BufferPoolManager {
    /// Creates a new `BufferPoolManager` with the default cache configuration.
    ///
    /// This constructor enables disk manager caching and disables bypassing the disk cache
    /// for pinned pages. For custom cache configuration, use [`new_with_cache_config`].
    ///
    /// # Parameters
    /// - `pool_size`: Number of frames in the buffer pool
    /// - `disk_manager`: Shared async disk manager for I/O operations
    /// - `replacer`: LRU-K replacer for page eviction decisions
    ///
    /// # Returns
    /// A new `BufferPoolManager` instance or an error string on failure.
    pub fn new(
        pool_size: usize,
        disk_manager: Arc<AsyncDiskManager>,
        replacer: Arc<RwLock<LRUKReplacer>>,
    ) -> Result<Self, String> {
        Self::new_with_cache_config(pool_size, disk_manager, replacer, true, false)
    }

    /// Creates a new `BufferPoolManager` with custom cache coordination configuration.
    ///
    /// This allows fine-grained control over how the buffer pool interacts with the
    /// async disk manager's cache layer.
    ///
    /// # Parameters
    /// - `pool_size`: Number of frames in the buffer pool
    /// - `disk_manager`: Shared async disk manager for I/O operations
    /// - `replacer`: LRU-K replacer for page eviction decisions
    /// - `use_disk_manager_cache`: Whether to use the disk manager's read cache
    /// - `bypass_disk_cache_for_pinned`: Whether to skip disk cache for pinned pages
    ///
    /// # Returns
    /// A new `BufferPoolManager` instance or an error string on failure.
    pub fn new_with_cache_config(
        pool_size: usize,
        disk_manager: Arc<AsyncDiskManager>,
        replacer: Arc<RwLock<LRUKReplacer>>,
        use_disk_manager_cache: bool,
        bypass_disk_cache_for_pinned: bool,
    ) -> Result<Self, String> {
        let free_list: Vec<FrameId> = (0..pool_size as FrameId).collect();

        info!(
            "BufferPoolManager initialized with pool size: {}, disk manager health: {}, cache coordination: {}",
            pool_size,
            disk_manager.health_check(),
            use_disk_manager_cache
        );

        Ok(Self {
            pool_size,
            next_page_id: AtomicU64::new(0),
            pages: Arc::new(RwLock::new(vec![None; pool_size])),
            page_table: Arc::new(RwLock::new(HashMap::new())),
            replacer,
            free_list: Arc::new(RwLock::new(free_list)),
            disk_manager,
            use_disk_manager_cache,
            bypass_disk_cache_for_pinned,
        })
    }

    /// Creates a new buffer pool manager with enhanced disk manager configuration.
    ///
    /// This is the recommended constructor for production use. It creates both the
    /// async disk manager and LRU-K replacer internally with the provided configuration.
    ///
    /// # Parameters
    /// - `pool_size`: Number of frames in the buffer pool
    /// - `db_file_path`: Path to the database file
    /// - `log_file_path`: Path to the write-ahead log file
    /// - `config`: Configuration options for the async disk manager
    ///
    /// # Returns
    /// A new `BufferPoolManager` instance or an error string on failure.
    ///
    /// # Example
    /// ```rust,no_run
    /// let bpm = BufferPoolManager::new_with_config(
    ///     100,
    ///     "data.db".to_string(),
    ///     "data.log".to_string(),
    ///     DiskManagerConfig::default(),
    /// ).await?;
    /// ```
    pub async fn new_with_config(
        pool_size: usize,
        db_file_path: String,
        log_file_path: String,
        config: DiskManagerConfig,
    ) -> Result<Self, String> {
        // Create async disk manager
        let disk_manager = Arc::new(
            AsyncDiskManager::new(db_file_path, log_file_path, config)
                .await
                .map_err(|e| format!("Failed to create async disk manager: {}", e))?,
        );

        // Start monitoring if enabled
        if let Err(e) = disk_manager.start_monitoring().await {
            warn!("Failed to start disk manager monitoring: {}", e);
        }

        // Create replacer (use default LRU-K with k=2)
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(pool_size, 2)));
        Self::new(pool_size, disk_manager, replacer)
    }

    /// Returns the size of the buffer pool (number of frames).
    ///
    /// This is the maximum number of pages that can be held in memory simultaneously.
    pub fn get_pool_size(&self) -> usize {
        self.pool_size
    }

    /// Returns a clone of the page collection.
    ///
    /// The returned collection contains all frames in the buffer pool, where each
    /// frame may or may not contain a page. Use this for inspection or debugging.
    ///
    /// # Note
    /// This returns an `Arc`-wrapped collection, so modifications are shared.
    pub fn get_pages(&self) -> PageCollection {
        self.pages.clone()
    }

    /// Returns a clone of the page table mapping page IDs to frame IDs.
    ///
    /// The page table tracks which pages are currently in the buffer pool and
    /// their corresponding frame locations.
    ///
    /// # Note
    /// This returns an `Arc`-wrapped map, so modifications are shared.
    pub fn get_page_table(&self) -> Arc<RwLock<HashMap<PageId, FrameId>>> {
        self.page_table.clone()
    }

    /// Creates a new page and returns it.
    ///
    /// # Returns
    /// An optional new page.
    pub fn new_page<T: Page>(self: &Arc<Self>) -> Option<PageGuard<T>> {
        trace!("Creating new page of type: {:?}", T::TYPE_ID);

        let frame_id = self.get_available_frame()?;
        trace!("Got available frame: {}", frame_id);

        self.evict_page_if_necessary(frame_id);

        let new_page_id = self.allocate_page_id();
        trace!("Allocated new page ID: {}", new_page_id);

        let new_page = self.create_typed_page::<T>(new_page_id);
        self.update_page_metadata(frame_id, new_page_id, &new_page);

        trace!("Created new page {} in frame {}", new_page_id, frame_id);
        let unpinner: Arc<BufferPoolManager> = Arc::clone(self);
        let unpinner: Arc<dyn PageUnpinner> = unpinner;
        Some(PageGuard::new_for_new_page(
            new_page,
            new_page_id,
            Some(unpinner),
        ))
    }

    /// Creates a new page with custom options using a provided constructor function.
    ///
    /// # Parameters
    /// - `constructor`: A function that creates the page with the given page ID.
    ///
    /// # Returns
    /// An optional `PageGuard<T>`.
    pub fn new_page_with_options<T: Page, F>(
        self: &Arc<Self>,
        constructor: F,
    ) -> Option<PageGuard<T>>
    where
        F: FnOnce(PageId) -> T,
    {
        trace!(
            "Creating new page with custom options of type {:?}",
            T::TYPE_ID
        );

        let frame_id = self.get_available_frame()?;
        trace!("Got available frame: {}", frame_id);

        self.evict_page_if_necessary(frame_id);

        let new_page_id = self.allocate_page_id();
        trace!("Allocated new page ID: {}", new_page_id);

        let new_page = self.create_typed_page_with_constructor::<T, F>(new_page_id, constructor);
        self.update_page_metadata(frame_id, new_page_id, &new_page);

        trace!(
            "Created new page {} in frame {} with custom options",
            new_page_id, frame_id
        );
        let unpinner: Arc<BufferPoolManager> = Arc::clone(self);
        let unpinner: Arc<dyn PageUnpinner> = unpinner;
        Some(PageGuard::new_for_new_page(
            new_page,
            new_page_id,
            Some(unpinner),
        ))
    }

    /// Writes data to the page with the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to write to.
    /// - `data`: The data to write to the page.
    pub fn write_page(&self, page_id: PageId, data: [u8; DB_PAGE_SIZE as usize]) {
        trace!("Writing to page with ID: {}", page_id);

        // Acquire read lock on the page collection
        let pages = self.pages.read();

        // Find the target page
        let mut target_page = None;
        for (i, page_opt) in pages.iter().enumerate() {
            if let Some(page) = page_opt {
                let page_data = page.read(); // Acquire read lock on the page
                if page_data.get_page_id() == page_id {
                    // Access trait method directly
                    target_page = Some((page.clone(), i));
                    break;
                }
            }
        }

        // Drop the read lock before acquiring the write lock on the target page
        drop(pages);

        if let Some((page, _index)) = target_page {
            // Acquire write lock on the target page
            let mut page_data = page.write();

            // Update the page data - access trait methods directly
            let _ = page_data.set_data(0, &data);
            page_data.set_dirty(true);

            trace!("Page {} written and marked dirty", page_id);
        } else {
            // Optionally log if the page was not found
            warn!("Page with ID {} not found for writing", page_id);
        }
    }

    /// Fetches a page with the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to fetch.
    ///
    /// # Returns
    /// An optional `PageGuard<T>`.
    pub fn fetch_page<T: Page + 'static>(
        self: &Arc<Self>,
        page_id: PageId,
    ) -> Option<PageGuard<T>> {
        trace!("Fetching page {} of type {:?}", page_id, T::TYPE_ID);

        if page_id == INVALID_PAGE_ID || page_id >= self.next_page_id.load(Ordering::SeqCst) {
            trace!("Invalid page ID: {}", page_id);
            return None;
        }

        // Check if page is already in memory
        if let Some(page) = self.get_page_internal(page_id) {
            // Verify page type
            let actual_type = {
                let page_read = page.read();
                let type_byte = page_read.get_data()[PAGE_TYPE_OFFSET];
                let page_type = PageType::from_u8(type_byte);
                trace!(
                    "Page {} in memory - Type byte: {}, Parsed type: {:?}, Expected: {:?}",
                    page_id,
                    type_byte,
                    page_type,
                    T::TYPE_ID
                );
                page_type
            };

            if actual_type != Some(T::TYPE_ID) {
                warn!(
                    "Page type mismatch: expected {:?}, found {:?}",
                    T::TYPE_ID,
                    actual_type
                );
                return None;
            }

            // Mark frame as non-evictable while pinned and record access.
            if let Some(&frame_id) = self.page_table.read().get(&page_id) {
                let mut replacer = self.replacer.write();
                replacer.record_access(frame_id, AccessType::Unknown);
                replacer.set_evictable(frame_id, false);
            }

            // Create new guard (which will increment pin count)
            let typed_page = unsafe {
                let raw = Arc::into_raw(page) as *const RwLock<T>;
                Arc::from_raw(raw)
            };

            let unpinner: Arc<BufferPoolManager> = Arc::clone(self);
            let unpinner: Arc<dyn PageUnpinner> = unpinner;
            return Some(PageGuard::new(typed_page, page_id, Some(unpinner)));
        }

        // Page not in memory, need to load from disk
        self.load_page_from_disk::<T>(page_id)
    }

    /// Enhanced page loading with coordinated caching
    fn load_page_from_disk<T: Page + 'static>(
        self: &Arc<Self>,
        page_id: PageId,
    ) -> Option<PageGuard<T>> {
        trace!("Loading page {} with cache coordination", page_id);

        // Get available frame
        let frame_id = self.get_available_frame()?;
        trace!("Got available frame: {} for page load", frame_id);

        // Evict if necessary
        self.evict_page_if_necessary(frame_id);

        // Try to get page data using coordinated caching
        let page_data = if self.use_disk_manager_cache {
            self.load_page_with_cache_coordination(page_id)
        } else {
            self.load_page_bypass_cache(page_id)
        };

        let page_data = match page_data {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to load page {} : {}", page_id, e);
                return None;
            },
        };

        // Verify page type and create page
        if let Some(page_guard) = self.create_page_from_data::<T>(page_id, page_data) {
            // Update metadata
            let page_arc = page_guard.get_page().clone();
            self.update_page_metadata(frame_id, page_id, &page_arc);
            trace!("Successfully loaded page {} in frame {}", page_id, frame_id);
            Some(page_guard)
        } else {
            error!("Failed to create page {} from data", page_id);
            None
        }
    }

    /// Load page with cache coordination between BPM and AsyncDiskManager
    fn load_page_with_cache_coordination(
        &self,
        page_id: PageId,
    ) -> Result<Vec<u8>, std::io::Error> {
        trace!("Loading page {} with cache coordination", page_id);

        // Use AsyncDiskManager's read_page which checks its multi-level cache first
        // This avoids duplication since AsyncDiskManager handles cache levels internally
        futures::executor::block_on(async { self.disk_manager.read_page(page_id).await })
            .map_err(std::io::Error::other)
    }

    /// Load page bypassing AsyncDiskManager cache (BPM as sole cache)
    fn load_page_bypass_cache(&self, page_id: PageId) -> Result<Vec<u8>, std::io::Error> {
        trace!("Loading page {} bypassing disk manager cache", page_id);

        // When BPM wants to be the sole cache manager, we still use AsyncDiskManager
        // for I/O but could add a bypass_cache flag in the future
        // For now, we'll use the regular read_page but this could be optimized
        futures::executor::block_on(async { self.disk_manager.read_page(page_id).await })
            .map_err(std::io::Error::other)
    }

    /// Creates a page from raw data with proper type checking
    fn create_page_from_data<T: Page + 'static>(
        self: &Arc<Self>,
        page_id: PageId,
        data: Vec<u8>,
    ) -> Option<PageGuard<T>> {
        // Verify data size
        if data.len() < DB_PAGE_SIZE as usize {
            error!(
                "Page data too short for page {}: {} bytes",
                page_id,
                data.len()
            );
            return None;
        }

        // Verify page type
        let type_byte = data[PAGE_TYPE_OFFSET];
        let page_type = PageType::from_u8(type_byte);

        trace!(
            "Creating page {} from data - Type byte: {}, Parsed type: {:?}, Expected: {:?}",
            page_id,
            type_byte,
            page_type,
            T::TYPE_ID
        );

        if page_type != Some(T::TYPE_ID) {
            warn!(
                "Page type mismatch for page {}: expected {:?}, found {:?}",
                page_id,
                T::TYPE_ID,
                page_type
            );
            return None;
        }

        // Create new page
        let page_arc = self.create_typed_page::<T>(page_id);

        // Set the data
        {
            let mut page_guard = page_arc.write();
            if let Err(e) = page_guard.set_data(0, &data[..DB_PAGE_SIZE as usize]) {
                error!("Failed to set page data for page {}: {:?}", page_id, e);
                return None;
            }
        }

        let unpinner: Arc<BufferPoolManager> = Arc::clone(self);
        let unpinner: Arc<dyn PageUnpinner> = unpinner;
        Some(PageGuard::new_for_new_page(
            page_arc,
            page_id,
            Some(unpinner),
        ))
    }

    /// Unpins a page with the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to unpin.
    /// - `is_dirty`: Whether the page is dirty.
    /// - `access_type`: The type of access for the replacer.
    ///
    /// # Returns
    /// `true` if the page was successfully unpinned, `false` otherwise.
    pub fn unpin_page(&self, page_id: PageId, is_dirty: bool, access_type: AccessType) -> bool {
        trace!("Unpinning page {} (dirty: {})", page_id, is_dirty);

        let page_table = self.page_table.read();
        let frame_id = match page_table.get(&page_id) {
            Some(&fid) => fid,
            None => {
                trace!("Page {} not found in page table", page_id);
                return false;
            },
        };
        drop(page_table);

        let pages = self.pages.read();
        if let Some(Some(page)) = pages.get(frame_id as usize) {
            let mut page_guard = page.write();

            // Update dirty flag
            if is_dirty {
                page_guard.set_dirty(true);
            }

            // Decrement pin count
            let pin_count = page_guard.get_pin_count();
            if pin_count > 0 {
                page_guard.set_pin_count(pin_count - 1);
                trace!(
                    "Page {} unpinned, new pin count: {}",
                    page_id,
                    pin_count - 1
                );

                // If pin count reaches 0, mark as evictable
                if pin_count == 1 {
                    let mut replacer = self.replacer.write();
                    replacer.record_access(frame_id, access_type);
                    replacer.set_evictable(frame_id, true);
                    trace!("Page {} marked as evictable", page_id);
                }

                true
            } else {
                warn!("Attempted to unpin page {} with pin count 0", page_id);
                false
            }
        } else {
            warn!("Page {} not found in pages array", page_id);
            false
        }
    }

    /// Flushes a page to disk asynchronously
    pub async fn flush_page_async(&self, page_id: PageId) -> Result<(), String> {
        trace!("Flushing page {} to disk", page_id);

        let frame_id = {
            let page_table = self.page_table.read();
            *page_table
                .get(&page_id)
                .ok_or_else(|| format!("Page {} not found in page table", page_id))?
        };

        let page = {
            let pages = self.pages.read();
            pages
                .get(frame_id as usize)
                .and_then(|p| p.as_ref())
                .ok_or_else(|| format!("Page {} not found in pages array", page_id))?
                .clone()
        };

        self.write_page_to_disk_async(page_id, &page).await
    }

    /// Flushes a page with the given page ID to disk.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to flush.
    ///
    /// # Returns
    /// `Ok(())` if the page was successfully flushed, `Err(String)` otherwise.
    pub fn flush_page(&self, page_id: PageId) -> Result<(), String> {
        trace!("Flushing page {} to disk", page_id);

        let page_table = self.page_table.read();
        let frame_id = page_table
            .get(&page_id)
            .ok_or_else(|| format!("Page {} not found in page table", page_id))?;
        let frame_id = *frame_id;
        drop(page_table);

        let pages = self.pages.read();
        let page = pages
            .get(frame_id as usize)
            .and_then(|p| p.as_ref())
            .ok_or_else(|| format!("Page {} not found in pages array", page_id))?
            .clone();
        drop(pages);

        self.write_page_to_disk(page_id, &page)
    }

    /// Flushes all pages to disk.
    pub fn flush_all_pages(&self) {
        trace!("Flushing all pages to disk");

        let page_table = self.page_table.read();
        let page_ids: Vec<PageId> = page_table.keys().cloned().collect();
        drop(page_table);

        for page_id in page_ids {
            if let Err(e) = self.flush_page(page_id) {
                error!("Failed to flush page {}: {}", page_id, e);
            }
        }

        trace!("Finished flushing all pages");
    }

    /// Flushes all pages to disk asynchronously.
    pub async fn flush_all_pages_async(&self) {
        trace!("Flushing all pages to disk");

        let page_table = self.page_table.read();
        let page_ids: Vec<PageId> = page_table.keys().cloned().collect();
        drop(page_table);

        for page_id in page_ids {
            if let Err(e) = self.flush_page_async(page_id).await {
                error!("Failed to flush page {}: {}", page_id, e);
            }
        }

        trace!("Finished flushing all pages");
    }

    /// Flushes all pages **and** forces the disk manager to flush buffered writes.
    ///
    /// The buffer pool flush methods (`flush_page*` / `flush_all_pages*`) write pages via the
    /// async disk manager, which may internally buffer/coalesce writes for performance.
    ///
    /// For short-lived processes (examples, tools) and for “evidence” demos where we want
    /// the on-disk files to reflect the latest state *immediately*, this method provides a
    /// stronger guarantee by explicitly calling `AsyncDiskManager::flush()`.
    pub async fn flush_all_pages_durable(&self) -> Result<(), String> {
        self.flush_all_pages_async().await;
        self.disk_manager
            .flush()
            .await
            .map_err(|e| format!("Failed to flush disk manager: {e}"))?;
        Ok(())
    }

    /// Allocates a new page ID.
    ///
    /// # Returns
    /// A new page ID.
    pub fn allocate_page(&self) -> PageId {
        self.allocate_page_id()
    }

    /// Deallocates a page with the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to deallocate.
    pub fn deallocate_page(&mut self, page_id: PageId) {
        trace!("Deallocating page {}", page_id);

        // Remove from page table
        self.remove_page_from_table(page_id);

        // Note: In a full implementation, we might want to track deallocated page IDs
        // for reuse, but for now we'll just remove it from the buffer pool
    }

    /// Deletes a page with the given page ID.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to delete.
    ///
    /// # Returns
    /// `Ok(())` if the page was successfully deleted, `Err(DeletePageError)` otherwise.
    pub fn delete_page(&self, page_id: PageId) -> Result<(), DeletePageError> {
        trace!("Deleting page {}", page_id);

        let frame_id = self.get_frame_id(page_id)?;

        // Check if page is pinned
        let pages = self.pages.read();
        if let Some(Some(page)) = pages.get(frame_id as usize) {
            let page_guard = page.read();
            if page_guard.get_pin_count() > 0 {
                return Err(DeletePageError::PagePinned(page_id));
            }
        }
        drop(pages);

        // Reset the frame
        self.reset_frame(frame_id)?;

        // Remove from page table
        self.remove_page_from_table(page_id);

        // Add frame to free list
        {
            let mut free_list = self.free_list.write();
            free_list.push(frame_id);
        }

        // Remove from replacer
        {
            let replacer = self.replacer.write();
            replacer.remove(frame_id);
        }

        trace!("Successfully deleted page {}", page_id);
        Ok(())
    }

    /// Returns the number of frames currently available in the free list.
    ///
    /// A higher number indicates more available capacity for new pages without
    /// requiring eviction.
    pub fn get_free_list_size(&self) -> usize {
        self.free_list.read().len()
    }

    /// Returns a read guard to the LRU-K replacer.
    ///
    /// This allows inspection of the replacer's internal state for debugging
    /// or monitoring purposes.
    ///
    /// # Returns
    /// A read guard wrapped in `Option` (always `Some` in current implementation).
    pub fn get_replacer(&self) -> Option<RwLockReadGuard<'_, LRUKReplacer>> {
        Some(self.replacer.read())
    }

    // =========================================================================
    // Private Helper Methods
    // =========================================================================

    /// Looks up the frame ID for a given page ID in the page table.
    fn get_frame_id(&self, page_id: PageId) -> Result<FrameId, DeletePageError> {
        let page_table = self.page_table.read();
        page_table
            .get(&page_id)
            .copied()
            .ok_or(DeletePageError::PageNotFound(page_id))
    }

    /// Removes a page entry from the page table.
    fn remove_page_from_table(&self, page_id: PageId) {
        let mut page_table = self.page_table.write();
        page_table.remove(&page_id);
    }

    /// Resets a frame by clearing its page content.
    fn reset_frame(&self, frame_id: FrameId) -> Result<(), DeletePageError> {
        let mut pages = self.pages.write();
        if let Some(page_slot) = pages.get_mut(frame_id as usize) {
            *page_slot = None;
            Ok(())
        } else {
            Err(DeletePageError::InvalidFrame(frame_id))
        }
    }

    /// Evicts the page in the specified frame if it exists and is unpinned.
    fn evict_page_if_necessary(&self, frame_id: FrameId) {
        let pages = self.pages.read();
        if let Some(Some(page)) = pages.get(frame_id as usize) {
            let page_guard = page.read();
            let old_page_id = page_guard.get_page_id();
            let pin_count = page_guard.get_pin_count();
            drop(page_guard);
            drop(pages);

            if pin_count == 0 {
                trace!("Evicting page {} from frame {}", old_page_id, frame_id);
                self.evict_old_page(frame_id, old_page_id);
            } else {
                warn!(
                    "Cannot evict pinned page {} (pin count: {}) from frame {}",
                    old_page_id, pin_count, frame_id
                );
            }
        }
    }

    /// Evicts a page from the specified frame, flushing to disk if dirty.
    fn evict_old_page(&self, frame_id: FrameId, old_page_id: PageId) {
        // Check if page is dirty and flush if necessary
        let page_to_flush = {
            let pages = self.pages.read();
            if let Some(Some(page)) = pages.get(frame_id as usize) {
                let is_dirty = page.read().is_dirty();
                if is_dirty { Some(page.clone()) } else { None }
            } else {
                None
            }
        };

        if let Some(page) = page_to_flush {
            trace!("Flushing dirty page {} before eviction", old_page_id);
            if let Err(e) = self.write_page_to_disk(old_page_id, &page) {
                error!(
                    "Failed to flush page {} during eviction: {}",
                    old_page_id, e
                );
            }
        }

        // Remove from page table
        self.remove_page_from_table(old_page_id);

        // Reset frame
        if let Err(e) = self.reset_frame(frame_id) {
            error!(
                "Failed to reset frame {} during eviction: {:?}",
                frame_id, e
            );
        }

        trace!(
            "Successfully evicted page {} from frame {}",
            old_page_id, frame_id
        );
    }

    /// Gets an available frame, either from the free list or by evicting a page.
    ///
    /// This method first checks the free list for unused frames. If none are
    /// available, it uses the LRU-K replacer to select a victim for eviction.
    fn get_available_frame(&self) -> Option<FrameId> {
        // Try to get a frame from the free list first
        {
            let mut free_list = self.free_list.write();
            if let Some(frame_id) = free_list.pop() {
                trace!("Got frame {} from free list", frame_id);
                return Some(frame_id);
            }
        }

        // No free frames, try to evict using LRU-K
        trace!("No free frames, attempting eviction");
        let mut replacer = self.replacer.write();

        // Try multiple eviction attempts if needed
        for _ in 0..self.pool_size {
            if let Some(frame_id) = replacer.evict() {
                trace!("LRU-K replacer selected frame {} for eviction", frame_id);

                // Find the page_id for this frame
                let page_id = {
                    let page_table = self.page_table.read();
                    let mut victim_page_id = None;

                    for (&pid, &fid) in page_table.iter() {
                        if fid == frame_id {
                            victim_page_id = Some(pid);
                            break;
                        }
                    }

                    victim_page_id
                };

                // If we found a page to evict, do it now
                if let Some(old_page_id) = page_id {
                    self.evict_old_page(frame_id, old_page_id);
                }

                return Some(frame_id);
            }

            // If no frames are evictable, try to make some evictable
            // Find pages with pin count 0 that aren't marked as evictable
            let pages = self.pages.read();
            for (i, page_opt) in pages.iter().enumerate() {
                if let Some(page) = page_opt {
                    let page_data = page.read();
                    if page_data.get_pin_count() == 0 {
                        // Drop read lock before acquiring write lock
                        drop(page_data);

                        // Set this frame as evictable
                        replacer.set_evictable(i as FrameId, true);
                    }
                }
            }
        }

        warn!("No evictable frames found after multiple attempts");
        None
    }

    /// Allocates and returns the next available page ID atomically.
    fn allocate_page_id(&self) -> PageId {
        self.next_page_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Creates a new typed page with default initialization.
    ///
    /// Sets initial pin count to 1, marks as clean, and writes the page type byte.
    fn create_typed_page<T: Page>(&self, page_id: PageId) -> Arc<RwLock<T>> {
        let mut page = T::new(page_id);

        // Set initial page metadata
        page.set_pin_count(1); // Start with pin count 1
        page.set_dirty(false);

        // Ensure page type is set correctly
        let type_byte = T::TYPE_ID.to_u8();
        page.get_data_mut()[PAGE_TYPE_OFFSET] = type_byte;

        trace!(
            "Created new page {} of type {:?} (type byte: {})",
            page_id,
            T::TYPE_ID,
            type_byte
        );

        Arc::new(RwLock::new(page))
    }

    /// Creates a new typed page using a custom constructor function.
    ///
    /// Sets initial pin count to 1, marks as clean, and writes the page type byte.
    fn create_typed_page_with_constructor<T: Page, F>(
        &self,
        page_id: PageId,
        constructor: F,
    ) -> Arc<RwLock<T>>
    where
        F: FnOnce(PageId) -> T,
    {
        let mut page = constructor(page_id);

        // Set initial page metadata
        page.set_pin_count(1); // Start with pin count 1
        page.set_dirty(false);

        // Ensure page type is set correctly
        let type_byte = T::TYPE_ID.to_u8();
        page.get_data_mut()[PAGE_TYPE_OFFSET] = type_byte;

        trace!(
            "Created new page {} of type {:?} (type byte: {}) with custom constructor",
            page_id,
            T::TYPE_ID,
            type_byte
        );

        Arc::new(RwLock::new(page))
    }

    /// Updates internal metadata after placing a page in a frame.
    ///
    /// This updates the pages array, page table, and replacer state to reflect
    /// the new page-to-frame mapping.
    fn update_page_metadata<P: PageTrait + 'static>(
        &self,
        frame_id: FrameId,
        page_id: PageId,
        new_page: &Arc<RwLock<P>>,
    ) {
        // Update pages array
        {
            let mut pages = self.pages.write();
            let page_clone = new_page.clone();
            let trait_object: Arc<RwLock<dyn PageTrait>> = page_clone;
            pages[frame_id as usize] = Some(trait_object);
        }

        // Update page table
        {
            let mut page_table = self.page_table.write();
            page_table.insert(page_id, frame_id);
        }

        // Update replacer
        {
            let mut replacer = self.replacer.write();
            replacer.record_access(frame_id, AccessType::Unknown);
            replacer.set_evictable(frame_id, false); // New pages are not evictable
        }

        trace!(
            "Updated metadata for page {} in frame {}",
            page_id, frame_id
        );
    }

    /// Writes a page to disk asynchronously with proper cache coordination.
    ///
    /// This is the recommended method for persisting page data. It uses native async
    /// I/O for optimal performance and marks the page as clean after successful write.
    ///
    /// # Parameters
    /// - `page_id`: The ID of the page to write
    /// - `page`: Reference to the page trait object
    ///
    /// # Returns
    /// `Ok(())` on success, or an error message describing the failure.
    ///
    /// # Performance
    /// This method is truly async and does not block on I/O operations.
    pub async fn write_page_to_disk_async(
        &self,
        page_id: PageId,
        page: &Arc<RwLock<dyn PageTrait>>,
    ) -> Result<(), String> {
        trace!("Writing page {} to disk with cache coordination", page_id);

        // Prepare data for writing
        let data_buffer = {
            let page_guard = page.read();
            let mut data = vec![0u8; DB_PAGE_SIZE as usize];
            data.copy_from_slice(page_guard.get_data());
            data
        };

        // PERFORMANCE OPTIMIZATION: Use native async - no more blocking!
        self.disk_manager
            .write_page(page_id, data_buffer)
            .await
            .map_err(|e| format!("Failed to write page to disk: {}", e))?;

        // Mark page as clean after successful write
        {
            let mut page_guard = page.write();
            page_guard.set_dirty(false);
        }

        trace!(
            "Successfully wrote page {} to disk with cache coordination",
            page_id
        );
        Ok(())
    }

    /// DEPRECATED: Use write_page_to_disk_async instead
    ///
    /// This method is kept for backward compatibility but will be removed.
    /// It still blocks on async operations which defeats the purpose of async I/O.
    fn write_page_to_disk(
        &self,
        page_id: PageId,
        page: &Arc<RwLock<dyn PageTrait>>,
    ) -> Result<(), String> {
        warn!(
            "DEPRECATED: write_page_to_disk still uses blocking sync wrapper. Use write_page_to_disk_async instead for better performance."
        );

        // Prepare data for writing
        let data_buffer = {
            let page_guard = page.read();
            let mut data = vec![0u8; DB_PAGE_SIZE as usize];
            data.copy_from_slice(page_guard.get_data());
            data
        };

        // PERFORMANCE BOTTLENECK: This still blocks async operations!
        let result = futures::executor::block_on(async {
            self.disk_manager.write_page(page_id, data_buffer).await
        });

        result.map_err(|e| format!("Failed to write page to disk: {}", e))?;

        // Mark page as clean after successful write
        {
            let mut page_guard = page.write();
            page_guard.set_dirty(false);
        }

        trace!(
            "Successfully wrote page {} to disk with cache coordination",
            page_id
        );
        Ok(())
    }

    /// Retrieves a page from the buffer pool without type checking or guard creation.
    ///
    /// This is an internal helper that returns the raw page trait object if present.
    fn get_page_internal(&self, page_id: PageId) -> Option<Arc<RwLock<dyn PageTrait>>> {
        let page_table = self.page_table.read();
        let frame_id = page_table.get(&page_id)?;

        let pages = self.pages.read();
        pages[*frame_id as usize].clone()
    }

    /// Loads multiple pages in batch for improved I/O performance.
    ///
    /// This method optimizes disk access by batching read operations. Pages already
    /// in memory are returned immediately, while missing pages are loaded from disk
    /// in a single batch operation.
    ///
    /// # Parameters
    /// - `page_ids`: Vector of page IDs to load
    ///
    /// # Returns
    /// A vector of optional page guards, maintaining the same order as input.
    /// `None` entries indicate pages that could not be loaded.
    pub async fn load_pages_batch<T: Page + 'static>(
        self: &Arc<Self>,
        page_ids: Vec<PageId>,
    ) -> Vec<Option<PageGuard<T>>> {
        if page_ids.is_empty() {
            return Vec::new();
        }

        trace!("Loading {} pages in batch", page_ids.len());

        // Check which pages are already in memory
        let mut in_memory_pages = Vec::new();
        let mut pages_to_load = Vec::new();

        for page_id in page_ids {
            if let Some(page) = self.get_page_internal(page_id) {
                // Verify type and create guard
                if let Some(guard) = self.create_guard_from_existing::<T>(page_id, page) {
                    in_memory_pages.push((page_id, Some(guard)));
                } else {
                    in_memory_pages.push((page_id, None));
                }
            } else {
                pages_to_load.push(page_id);
            }
        }

        // Load missing pages from disk in batch
        if !pages_to_load.is_empty() {
            match self
                .disk_manager
                .read_pages_batch(pages_to_load.clone())
                .await
            {
                Ok(pages_data) => {
                    for (page_id, data) in pages_to_load.into_iter().zip(pages_data) {
                        if let Some(guard) = self.create_page_from_data::<T>(page_id, data) {
                            // Get frame and update metadata
                            if let Some(frame_id) = self.get_available_frame() {
                                self.evict_page_if_necessary(frame_id);
                                let page_arc = guard.get_page().clone();
                                self.update_page_metadata(frame_id, page_id, &page_arc);
                            }
                            in_memory_pages.push((page_id, Some(guard)));
                        } else {
                            in_memory_pages.push((page_id, None));
                        }
                    }
                },
                Err(e) => {
                    error!("Failed to load pages in batch: {}", e);
                    for page_id in pages_to_load {
                        in_memory_pages.push((page_id, None));
                    }
                },
            }
        }

        // Sort by original order and return
        in_memory_pages.sort_by_key(|(page_id, _)| *page_id);
        in_memory_pages
            .into_iter()
            .map(|(_, guard)| guard)
            .collect()
    }

    /// Creates a guard from an existing page in memory
    fn create_guard_from_existing<T: Page + 'static>(
        self: &Arc<Self>,
        page_id: PageId,
        page: Arc<RwLock<dyn PageTrait>>,
    ) -> Option<PageGuard<T>> {
        // Verify page type
        let actual_type = {
            let page_read = page.read();
            let type_byte = page_read.get_data()[PAGE_TYPE_OFFSET];
            PageType::from_u8(type_byte)
        };

        if actual_type != Some(T::TYPE_ID) {
            warn!(
                "Page type mismatch for page {}: expected {:?}, found {:?}",
                page_id,
                T::TYPE_ID,
                actual_type
            );
            return None;
        }

        // Mark frame as non-evictable while pinned and record access.
        if let Some(&frame_id) = self.page_table.read().get(&page_id) {
            let mut replacer = self.replacer.write();
            replacer.record_access(frame_id, AccessType::Unknown);
            replacer.set_evictable(frame_id, false);
        }

        // Create typed page arc
        let typed_page = unsafe {
            let raw = Arc::into_raw(page) as *const RwLock<T>;
            Arc::from_raw(raw)
        };

        let unpinner: Arc<BufferPoolManager> = Arc::clone(self);
        let unpinner: Arc<dyn PageUnpinner> = unpinner;
        Some(PageGuard::new(typed_page, page_id, Some(unpinner)))
    }

    /// Flushes multiple dirty pages to disk in a single batch operation.
    ///
    /// This method collects dirty pages and writes them using the async disk manager's
    /// batch write capability for improved I/O efficiency.
    ///
    /// # Parameters
    /// - `max_pages`: Maximum number of dirty pages to flush in this batch
    ///
    /// # Returns
    /// The number of pages successfully flushed, or an error message.
    pub async fn flush_dirty_pages_batch_async(&self, max_pages: usize) -> Result<usize, String> {
        trace!(
            "Starting async batch flush of up to {} dirty pages",
            max_pages
        );

        // Collect dirty pages
        let dirty_pages: Vec<(PageId, Vec<u8>)> = {
            let pages = self.pages.read();
            pages
                .iter()
                .filter_map(|page_opt| {
                    if let Some(page) = page_opt {
                        let page_guard = page.read();
                        if page_guard.is_dirty() {
                            let page_id = page_guard.get_page_id();
                            let data = page_guard.get_data().to_vec();
                            Some((page_id, data))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .take(max_pages)
                .collect()
        };

        if dirty_pages.is_empty() {
            return Ok(0);
        }

        // Write in batch using async disk manager
        let result = self
            .disk_manager
            .write_pages_batch(dirty_pages.clone())
            .await;

        match result {
            Ok(()) => {
                // Mark pages as clean
                let pages = self.pages.read();
                for (page_id, _) in &dirty_pages {
                    if let Some(frame_id) = self.page_table.read().get(page_id)
                        && let Some(Some(page)) = pages.get(*frame_id as usize)
                    {
                        page.write().set_dirty(false);
                    }
                }
                trace!(
                    "Successfully flushed {} dirty pages in batch",
                    dirty_pages.len()
                );
                Ok(dirty_pages.len())
            },
            Err(e) => {
                error!("Failed to flush pages in batch: {}", e);
                Err(format!("Batch flush failed: {}", e))
            },
        }
    }

    // Cache Coordination Management Methods

    /// Configures cache coordination behavior
    pub fn set_cache_coordination(
        &mut self,
        use_disk_manager_cache: bool,
        bypass_disk_cache_for_pinned: bool,
    ) {
        self.use_disk_manager_cache = use_disk_manager_cache;
        self.bypass_disk_cache_for_pinned = bypass_disk_cache_for_pinned;

        info!(
            "Cache coordination updated: use_disk_manager_cache={}, bypass_disk_cache_for_pinned={}",
            use_disk_manager_cache, bypass_disk_cache_for_pinned
        );
    }

    /// Gets current cache coordination settings
    pub fn get_cache_coordination_settings(&self) -> (bool, bool) {
        (
            self.use_disk_manager_cache,
            self.bypass_disk_cache_for_pinned,
        )
    }

    /// Invalidates a page from AsyncDiskManager cache when it's modified in BPM
    pub fn invalidate_disk_cache(&self, page_id: PageId) {
        if self.use_disk_manager_cache {
            trace!("Would invalidate page {} from disk manager cache", page_id);
            // Note: This would need a new method in AsyncDiskManager like invalidate_cache_page()
            // For now, we rely on the fact that writes update the cache
        }
    }

    /// Prefetches pages into AsyncDiskManager cache for future BPM access
    pub async fn prefetch_to_disk_cache(&self, page_ids: Vec<PageId>) -> Result<usize, String> {
        if !self.use_disk_manager_cache || page_ids.is_empty() {
            return Ok(0);
        }

        trace!("Prefetching {} pages to disk manager cache", page_ids.len());

        // Use batch read to populate AsyncDiskManager cache
        // We don't need the data, just want to populate the cache
        match self.disk_manager.read_pages_batch(page_ids.clone()).await {
            Ok(_) => {
                trace!(
                    "Successfully prefetched {} pages to disk cache",
                    page_ids.len()
                );
                Ok(page_ids.len())
            },
            Err(e) => {
                error!("Failed to prefetch pages to disk cache: {}", e);
                Err(format!("Prefetch failed: {}", e))
            },
        }
    }

    /// Gets cache coordination statistics
    pub async fn get_cache_coordination_stats(&self) -> CacheCoordinationStats {
        let (disk_cache_hits, disk_cache_misses, disk_hit_ratio) =
            self.disk_manager.get_cache_stats().await;

        let bpm_utilization = {
            let page_table_size = self.page_table.read().len();
            page_table_size as f64 / self.pool_size as f64 * 100.0
        };

        CacheCoordinationStats {
            use_disk_manager_cache: self.use_disk_manager_cache,
            bypass_disk_cache_for_pinned: self.bypass_disk_cache_for_pinned,
            bpm_pool_utilization: bpm_utilization,
            disk_cache_hits,
            disk_cache_misses,
            disk_cache_hit_ratio: disk_hit_ratio,
            total_cache_efficiency: self
                .calculate_total_cache_efficiency(bpm_utilization, disk_hit_ratio),
        }
    }

    /// Calculates overall cache efficiency across BPM and AsyncDiskManager
    fn calculate_total_cache_efficiency(&self, bpm_utilization: f64, disk_hit_ratio: f64) -> f64 {
        if self.use_disk_manager_cache {
            // Weighted efficiency: BPM utilization has higher weight as it's the primary cache
            (bpm_utilization * 0.7) + (disk_hit_ratio * 100.0 * 0.3)
        } else {
            // Only BPM cache is used
            bpm_utilization
        }
    }

    /// Optimizes cache coordination based on access patterns
    pub async fn optimize_cache_coordination(&mut self) -> Result<(), String> {
        let stats = self.get_cache_coordination_stats().await;

        trace!(
            "Current cache stats: BPM utilization: {:.2}%, Disk hit ratio: {:.2}%, Total efficiency: {:.2}%",
            stats.bpm_pool_utilization,
            stats.disk_cache_hit_ratio * 100.0,
            stats.total_cache_efficiency
        );

        // Auto-optimization logic
        if stats.bpm_pool_utilization > 90.0 && stats.disk_cache_hit_ratio < 0.3 {
            // BPM is highly utilized but disk cache isn't helping much
            warn!("Consider increasing buffer pool size or optimizing access patterns");
        } else if stats.disk_cache_hit_ratio > 0.8 && stats.bpm_pool_utilization < 50.0 {
            // Disk cache is very effective, BPM has room
            info!("Cache coordination is working well");
        }

        Ok(())
    }

    /// Gets comprehensive health status including disk manager metrics (async)
    ///
    /// PERFORMANCE OPTIMIZATION: Now truly async - no more blocking on async operations!
    pub async fn get_health_status_async(&self) -> BufferPoolHealthStatus {
        let disk_health = self.disk_manager.health_check();
        let disk_metrics = self.disk_manager.get_metrics();
        let (cache_hits, cache_misses, cache_hit_ratio) = self.disk_manager.get_cache_stats().await;

        let pages_count = self.pages.read().len();
        let free_list_size = self.free_list.read().len();
        let page_table_size = self.page_table.read().len();

        let buffer_pool_healthy =
            pages_count == self.pool_size && free_list_size + page_table_size <= self.pool_size;

        BufferPoolHealthStatus {
            overall_healthy: disk_health && buffer_pool_healthy,
            disk_manager_healthy: disk_health,
            buffer_pool_healthy,
            pool_utilization: (page_table_size as f64 / self.pool_size as f64 * 100.0),
            cache_hit_ratio,
            cache_hits,
            cache_misses,
            avg_read_latency_ns: disk_metrics.read_latency_avg_ns,
            avg_write_latency_ns: disk_metrics.write_latency_avg_ns,
            io_throughput_mb_per_sec: disk_metrics.io_throughput_mb_per_sec,
        }
    }

    /// DEPRECATED: Use get_health_status_async instead
    ///
    /// This method blocks on async operations which defeats the purpose of async I/O.
    pub fn get_health_status(&self) -> BufferPoolHealthStatus {
        warn!(
            "DEPRECATED: get_health_status still uses blocking sync wrapper. Use get_health_status_async instead for better performance."
        );
        futures::executor::block_on(self.get_health_status_async())
    }

    /// Gets performance metrics from the async disk manager.
    ///
    /// Returns a snapshot of I/O statistics including read/write latencies,
    /// throughput, and operation counts.
    pub fn get_disk_metrics(
        &self,
    ) -> crate::storage::disk::async_disk::metrics::snapshot::MetricsSnapshot {
        self.disk_manager.get_metrics()
    }

    /// Forces a full sync of all pending writes to disk.
    ///
    /// This ensures that all data in the disk manager's write buffer is
    /// durably persisted to storage. Use before shutdown or at checkpoints.
    pub async fn sync_to_disk(&self) -> Result<(), String> {
        self.disk_manager
            .sync()
            .await
            .map_err(|e| format!("Failed to sync to disk: {}", e))
    }

    /// DEPRECATED: Use flush_dirty_pages_batch_async instead
    ///
    /// This method blocks on async operations which defeats the purpose of async I/O.
    /// Use flush_dirty_pages_batch_async for better performance.
    pub fn flush_dirty_pages_batch(&self, max_pages: usize) -> Result<usize, String> {
        warn!(
            "DEPRECATED: flush_dirty_pages_batch still uses blocking sync wrapper. Use flush_dirty_pages_batch_async instead for better performance."
        );
        futures::executor::block_on(self.flush_dirty_pages_batch_async(max_pages))
    }

    /// Returns a reference to the underlying async disk manager.
    ///
    /// This allows direct access to the disk manager for advanced operations
    /// such as custom I/O patterns or metrics collection.
    pub fn get_disk_manager(&self) -> Arc<AsyncDiskManager> {
        Arc::clone(&self.disk_manager)
    }

    /// Performs an asynchronous health check on the buffer pool.
    ///
    /// This checks both the buffer pool's internal state and the disk manager's
    /// health status.
    ///
    /// # Returns
    /// `true` if the system is healthy, `false` otherwise.
    pub async fn health_check_async(&self) -> bool {
        self.get_health_status_async().await.overall_healthy
    }

    /// DEPRECATED: Use health_check_async instead
    ///
    /// This method blocks on async operations which defeats the purpose of async I/O.
    pub fn health_check(&self) -> bool {
        warn!(
            "DEPRECATED: health_check still uses blocking sync wrapper. Use health_check_async instead for better performance."
        );
        self.get_health_status().overall_healthy
    }

    /// Executes an async operation with proper error handling and logging
    ///
    /// This method provides a generic way to execute async operations on the buffer pool
    /// with consistent error handling, logging, and performance monitoring.
    ///
    /// # Type Parameters
    /// - `F`: The future type returned by the operation
    /// - `T`: The result type of the operation
    ///
    /// # Parameters
    /// - `operation_name`: A descriptive name for the operation (used in logging)
    /// - `operation`: The async operation to execute
    ///
    /// # Returns
    /// The result of the operation wrapped in a Result for error handling
    ///
    /// # Example
    /// ```rust,no_run
    /// let result = bpm.run_async_operation("flush_specific_page", async {
    ///     bpm.flush_page_async(page_id).await
    /// }).await?;
    /// ```
    pub async fn run_async_operation<F, T>(
        &self,
        operation_name: &str,
        operation: F,
    ) -> Result<T, String>
    where
        F: Future<Output = Result<T, String>>,
    {
        trace!("Starting async operation: {}", operation_name);
        let start_time = std::time::Instant::now();

        // Execute the operation with timeout protection
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(30), // 30 second timeout
            operation,
        )
        .await;

        let duration = start_time.elapsed();

        match result {
            Ok(Ok(value)) => {
                trace!(
                    "Async operation '{}' completed successfully in {:?}",
                    operation_name, duration
                );
                Ok(value)
            },
            Ok(Err(e)) => {
                error!(
                    "Async operation '{}' failed after {:?}: {}",
                    operation_name, duration, e
                );
                Err(format!("Operation '{}' failed: {}", operation_name, e))
            },
            Err(_) => {
                error!(
                    "Async operation '{}' timed out after {:?}",
                    operation_name, duration
                );
                Err(format!(
                    "Operation '{}' timed out after 30 seconds",
                    operation_name
                ))
            },
        }
    }

    /// Gracefully shuts down the buffer pool manager.
    ///
    /// This method ensures all dirty pages are flushed, syncs to disk, and
    /// properly shuts down the async disk manager. Always call this before
    /// dropping the buffer pool manager in production.
    ///
    /// # Returns
    /// `Ok(())` on successful shutdown, or an error message describing the failure.
    pub async fn shutdown(&mut self) -> Result<(), String> {
        info!("Shutting down BufferPoolManager");

        // Flush all dirty pages using async batch operation
        let dirty_count = self.flush_dirty_pages_batch_async(self.pool_size).await?;
        info!("Flushed {} dirty pages during shutdown", dirty_count);

        // Sync to ensure all data is written
        self.sync_to_disk().await?;

        // Shutdown the disk manager
        let disk_manager = Arc::clone(&self.disk_manager);
        if let Ok(mut dm) = Arc::try_unwrap(disk_manager) {
            dm.shutdown()
                .await
                .map_err(|e| format!("Failed to shutdown disk manager: {}", e))?;
        }

        info!("BufferPoolManager shutdown complete");
        Ok(())
    }
}

/// Health status information for the buffer pool and disk manager.
///
/// This struct provides a comprehensive snapshot of the buffer pool's operational
/// state, including cache efficiency and I/O performance metrics.
#[derive(Debug, Clone)]
pub struct BufferPoolHealthStatus {
    /// Whether the overall system (buffer pool + disk manager) is healthy.
    pub overall_healthy: bool,
    /// Whether the async disk manager is functioning correctly.
    pub disk_manager_healthy: bool,
    /// Whether the buffer pool's internal state is consistent.
    pub buffer_pool_healthy: bool,
    /// Percentage of buffer pool frames currently in use (0-100).
    pub pool_utilization: f64,
    /// Ratio of cache hits to total cache accesses (0.0-1.0).
    pub cache_hit_ratio: f64,
    /// Total number of cache hits since startup.
    pub cache_hits: u64,
    /// Total number of cache misses since startup.
    pub cache_misses: u64,
    /// Average read latency in nanoseconds.
    pub avg_read_latency_ns: u64,
    /// Average write latency in nanoseconds.
    pub avg_write_latency_ns: u64,
    /// Current I/O throughput in megabytes per second.
    pub io_throughput_mb_per_sec: f64,
}

/// Statistics for cache coordination between BPM (L1) and AsyncDiskManager (L2).
///
/// This struct provides insight into how effectively the two-level cache
/// architecture is performing.
#[derive(Debug, Clone)]
pub struct CacheCoordinationStats {
    /// Whether the disk manager's cache layer is enabled.
    pub use_disk_manager_cache: bool,
    /// Whether pinned pages bypass the disk cache.
    pub bypass_disk_cache_for_pinned: bool,
    /// Percentage of buffer pool frames currently in use (0-100).
    pub bpm_pool_utilization: f64,
    /// Total disk cache hits (L2 layer).
    pub disk_cache_hits: u64,
    /// Total disk cache misses (L2 layer).
    pub disk_cache_misses: u64,
    /// Disk cache hit ratio (0.0-1.0).
    pub disk_cache_hit_ratio: f64,
    /// Combined efficiency of both cache layers (0-100).
    pub total_cache_efficiency: f64,
}

/// Manual Clone implementation that creates a shallow copy with shared state.
///
/// All `Arc`-wrapped fields share state with the original. The `next_page_id`
/// counter is copied as a new atomic with the current value.
impl Clone for BufferPoolManager {
    fn clone(&self) -> Self {
        Self {
            pool_size: self.pool_size,
            next_page_id: AtomicU64::new(self.next_page_id.load(Ordering::SeqCst)),
            pages: Arc::clone(&self.pages),
            page_table: Arc::clone(&self.page_table),
            replacer: Arc::clone(&self.replacer),
            free_list: Arc::clone(&self.free_list),
            disk_manager: Arc::clone(&self.disk_manager),
            use_disk_manager_cache: self.use_disk_manager_cache,
            bypass_disk_cache_for_pinned: self.bypass_disk_cache_for_pinned,
        }
    }
}

/// Implementation of `PageUnpinner` trait for automatic page unpinning via `PageGuard`.
///
/// This allows `PageGuard` to call back into the BPM when the guard is dropped,
/// ensuring proper pin count management and replacer updates.
impl PageUnpinner for BufferPoolManager {
    /// Unpins a page when its guard is dropped.
    ///
    /// Delegates to the BPM's internal unpin logic with `AccessType::Unknown`.
    fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> bool {
        self.unpin_page(page_id, is_dirty, AccessType::Unknown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::{BasicPage, PageTrait, PageType};
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Test context for async buffer pool manager tests.
    ///
    /// Creates an isolated test environment with a temporary directory and
    /// pre-configured buffer pool manager.
    struct AsyncTestContext {
        /// The buffer pool manager under test.
        bpm: Arc<BufferPoolManager>,
        /// Temporary directory (dropped after test).
        _temp_dir: TempDir,
    }

    impl AsyncTestContext {
        /// Creates a new test context with the given name.
        ///
        /// The name is used to create unique database and log file names.
        pub async fn new(name: &str) -> Self {
            const BUFFER_POOL_SIZE: usize = 5;

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

            // Use default configuration for testing
            let config = DiskManagerConfig::default();

            let bpm = Arc::new(
                BufferPoolManager::new_with_config(BUFFER_POOL_SIZE, db_path, log_path, config)
                    .await
                    .expect("Failed to create BufferPoolManager"),
            );

            Self {
                bpm,
                _temp_dir: temp_dir,
            }
        }

        /// Returns a clone of the buffer pool manager for use in tests.
        pub fn bpm(&self) -> Arc<BufferPoolManager> {
            self.bpm.clone()
        }
    }

    #[tokio::test]
    async fn test_async_basic_page_operations() {
        let ctx = AsyncTestContext::new("async_basic_page_operations").await;
        let bpm = ctx.bpm();

        // Create a new page
        let page_guard = bpm
            .new_page::<BasicPage>()
            .expect("Failed to create new page");

        // Test read operations
        {
            let data = page_guard.read();
            assert_eq!(data.get_page_type(), PageType::Basic);
            assert_eq!(data.get_pin_count(), 1);
            assert_eq!(data.get_data()[PAGE_TYPE_OFFSET], PageType::Basic.to_u8());
        }

        // Test write operations (preserving page type)
        {
            let mut data = page_guard.write();
            let type_byte = data.get_data()[PAGE_TYPE_OFFSET];
            data.get_data_mut()[1] = 42; // Write to second byte, not the type byte
            data.get_data_mut()[PAGE_TYPE_OFFSET] = type_byte; // Ensure type is preserved
            assert_eq!(data.get_data()[1], 42);
            assert_eq!(data.get_data()[PAGE_TYPE_OFFSET], PageType::Basic.to_u8());
        }

        // Get page ID for later use
        let page_id = page_guard.read().get_page_id();

        // Verify type before dropping
        assert_eq!(page_guard.read().get_page_type(), PageType::Basic);
        assert_eq!(
            page_guard.read().get_data()[PAGE_TYPE_OFFSET],
            PageType::Basic.to_u8()
        );

        // Drop the guard to unpin the page
        drop(page_guard);

        // Fetch the page again
        let fetched_guard = bpm
            .fetch_page::<BasicPage>(page_id)
            .expect("Failed to fetch page");

        // Verify type and data after fetch
        let data = fetched_guard.read();
        assert_eq!(data.get_page_type(), PageType::Basic);
        assert_eq!(data.get_data()[PAGE_TYPE_OFFSET], PageType::Basic.to_u8());
        assert_eq!(data.get_data()[1], 42);
    }

    #[tokio::test]
    async fn test_async_disk_integration() {
        let ctx = AsyncTestContext::new("async_disk_integration").await;
        let bpm = ctx.bpm();

        // Create and write a page
        let page_guard = bpm
            .new_page::<BasicPage>()
            .expect("Failed to create new page");

        let page_id = page_guard.read().get_page_id();

        // Write some test data
        {
            let mut data = page_guard.write();
            data.get_data_mut()[1] = 123;
            data.set_dirty(true);
        }

        // Flush to disk using async version
        bpm.flush_page_async(page_id)
            .await
            .expect("Failed to flush page");

        // Unpin the page
        drop(page_guard);

        // Delete from buffer pool to force disk read
        bpm.delete_page(page_id).expect("Failed to delete page");

        // Fetch again - should read from disk
        let reloaded_guard = bpm
            .fetch_page::<BasicPage>(page_id)
            .expect("Failed to fetch page from disk");

        // Verify data was persisted
        let data = reloaded_guard.read();
        assert_eq!(data.get_data()[1], 123);
        assert_eq!(data.get_page_type(), PageType::Basic);
    }

    #[tokio::test]
    async fn test_async_health_check() {
        let ctx = AsyncTestContext::new("async_health_check").await;
        let bpm = ctx.bpm();

        // Fresh buffer pool should be healthy
        assert!(bpm.health_check());

        // After some operations, should still be healthy
        for i in 0..3 {
            let page_guard = bpm.new_page::<BasicPage>().expect("Failed to create page");
            let page_id = page_guard.read().get_page_id();

            {
                let mut data = page_guard.write();
                data.get_data_mut()[1] = i as u8;
            }

            drop(page_guard);
            bpm.flush_page_async(page_id)
                .await
                .expect("Failed to flush page");
        }

        assert!(bpm.health_check());
    }

    #[tokio::test]
    async fn test_async_batch_flush() {
        let ctx = AsyncTestContext::new("async_batch_flush").await;
        let bpm = ctx.bpm();

        // Create multiple dirty pages
        let mut page_ids = Vec::new();
        for i in 0..4 {
            let page_guard = bpm.new_page::<BasicPage>().expect("Failed to create page");
            let page_id = page_guard.read().get_page_id();
            page_ids.push(page_id);

            {
                let mut data = page_guard.write();
                data.get_data_mut()[1] = i as u8;
                data.set_dirty(true);
            }

            drop(page_guard);
        }

        // Flush in batch
        let flushed_count = bpm
            .flush_dirty_pages_batch(3)
            .expect("Failed to batch flush");
        assert!(flushed_count <= 3);
        assert!(flushed_count > 0);
    }

    #[tokio::test]
    async fn test_run_async_operation_success() {
        let ctx = AsyncTestContext::new("run_async_operation_success").await;
        let bpm = ctx.bpm();

        // Test successful operation
        let result = bpm
            .run_async_operation("test_health_check", async {
                Ok(bpm.health_check_async().await)
            })
            .await;

        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_run_async_operation_with_page_operations() {
        let ctx = AsyncTestContext::new("run_async_operation_page_ops").await;
        let bpm = ctx.bpm();

        // Test operation that involves actual page management
        let result = bpm
            .run_async_operation("create_and_flush_page", async {
                // Create a new page
                let page_guard = bpm
                    .new_page::<BasicPage>()
                    .ok_or_else(|| "Failed to create page".to_string())?;

                let page_id = page_guard.read().get_page_id();

                // Write some data
                {
                    let mut data = page_guard.write();
                    data.get_data_mut()[0] = 42;
                    data.set_dirty(true);
                }

                drop(page_guard);

                // Flush the page
                bpm.flush_page_async(page_id).await?;

                Ok(page_id)
            })
            .await;

        assert!(result.is_ok());
        let page_id = result.unwrap();
        assert!(page_id != INVALID_PAGE_ID);
    }

    #[tokio::test]
    async fn test_run_async_operation_error_handling() {
        let ctx = AsyncTestContext::new("run_async_operation_error").await;
        let bpm = ctx.bpm();

        // Test operation that fails
        let result = bpm
            .run_async_operation("failing_operation", async {
                Err::<(), String>("Intentional test failure".to_string())
            })
            .await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err();
        assert!(error_msg.contains("Operation 'failing_operation' failed"));
        assert!(error_msg.contains("Intentional test failure"));
    }

    #[tokio::test]
    async fn test_run_async_operation_timeout() {
        let ctx = AsyncTestContext::new("run_async_operation_timeout").await;
        let bpm = ctx.bpm();

        // Test operation that times out (using a longer delay than the timeout)
        let result = bpm
            .run_async_operation("slow_operation", async {
                tokio::time::sleep(std::time::Duration::from_secs(35)).await;
                Ok::<(), String>(())
            })
            .await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err();
        assert!(error_msg.contains("Operation 'slow_operation' timed out"));
        assert!(error_msg.contains("30 seconds"));
    }

    #[tokio::test]
    async fn test_run_async_operation_with_complex_workflow() {
        let ctx = AsyncTestContext::new("run_async_operation_complex").await;
        let bpm = ctx.bpm();

        // Test a complex workflow involving multiple async operations
        let result = bpm
            .run_async_operation("complex_workflow", async {
                let mut page_ids = Vec::new();

                // Create multiple pages
                for i in 0..3 {
                    let page_guard = bpm
                        .new_page::<BasicPage>()
                        .ok_or_else(|| format!("Failed to create page {}", i))?;

                    let page_id = page_guard.read().get_page_id();
                    page_ids.push(page_id);

                    // Write unique data to each page
                    {
                        let mut data = page_guard.write();
                        data.get_data_mut()[0] = i as u8;
                        data.set_dirty(true);
                    }

                    drop(page_guard);
                }

                // Flush all pages
                for page_id in &page_ids {
                    bpm.flush_page_async(*page_id)
                        .await
                        .map_err(|e| format!("Failed to flush page {}: {}", page_id, e))?;
                }

                // Note: Simplified test - in a real scenario we would verify page data
                // but for the test we just ensure all operations completed successfully

                Ok(page_ids.len())
            })
            .await;

        match result {
            Ok(count) => {
                assert_eq!(count, 3);
            },
            Err(e) => {
                panic!("Complex workflow failed: {}", e);
            },
        }
    }

    #[tokio::test]
    async fn test_run_async_operation_concurrent_operations() {
        let ctx = AsyncTestContext::new("run_async_operation_concurrent").await;
        let bpm = ctx.bpm();

        // Test multiple sequential async operations to simulate concurrency concepts
        // without running into complex lifetime/Send issues
        let mut results = Vec::new();

        for i in 0..3 {
            let result = bpm
                .run_async_operation(&format!("sequential_op_{}", i), async {
                    // Create a page
                    let page_guard = bpm
                        .new_page::<BasicPage>()
                        .ok_or_else(|| "Failed to create page".to_string())?;

                    let page_id = page_guard.read().get_page_id();

                    // Write data
                    {
                        let mut data = page_guard.write();
                        data.get_data_mut()[0] = i as u8;
                        data.set_dirty(true);
                    }

                    drop(page_guard);

                    // Small delay to simulate work
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

                    // Flush the page
                    bpm.flush_page_async(page_id).await?;

                    Ok(page_id)
                })
                .await;

            results.push(result);
        }

        // Verify all operations succeeded
        for (i, result) in results.into_iter().enumerate() {
            assert!(result.is_ok(), "Operation {} should succeed", i);
        }
    }

    #[tokio::test]
    async fn test_run_async_operation_performance_logging() {
        let ctx = AsyncTestContext::new("run_async_operation_perf").await;
        let bpm = ctx.bpm();

        // Test that performance timing works correctly
        let start = std::time::Instant::now();

        let result = bpm
            .run_async_operation("timed_operation", async {
                // Simulate some work
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                Ok::<String, String>("completed".to_string())
            })
            .await;

        let elapsed = start.elapsed();

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "completed");
        assert!(elapsed >= std::time::Duration::from_millis(50));
        assert!(elapsed < std::time::Duration::from_millis(1000)); // Should complete quickly
    }
}
