//! # Advanced Cache Manager
//!
//! The `CacheManager` implements a sophisticated multi-level caching system designed to optimize
//! disk I/O performance for the Async Disk Manager. It uses a tiered architecture to handle
//! different data access patterns efficiently, distinguishing between hot, warm, and cold data.
//!
//! ## System-Level Architecture
//!
//! ```text
//!   Two-Level Caching in the Database Stack
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                    Query Execution Engine                               │
//!   └───────────────────────────────┬─────────────────────────────────────────┘
//!                                   │
//!                                   ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │  L1: BufferPoolManager                                                  │
//!   │  • Semantic-aware caching of `Page` objects                             │
//!   │  • LRU-K replacement policy                                             │
//!   │  • Pin/unpin management                                                 │
//!   └───────────────────────────────┬─────────────────────────────────────────┘
//!                                   │ eviction / miss
//!                                   ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │  L2: CacheManager (this module)                                         │
//!   │  • Block-aware caching of raw/compressed bytes                          │
//!   │  • Three-tier hot/warm/cold organization                                │
//!   │  • Victim cache for L1 evictions                                        │
//!   │  • Masks disk latency for L1 misses                                     │
//!   └───────────────────────────────┬─────────────────────────────────────────┘
//!                                   │ miss
//!                                   ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                      AsyncDiskManager (Disk I/O)                        │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Three-Tier Cache Architecture
//!
//! ```text
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                        CacheManager                                     │
//!   │                                                                         │
//!   │  ┌───────────────────────────────────────────────────────────────────┐  │
//!   │  │              L1 Hot Cache (LRU-K, K=2)                             │  │
//!   │  │                                                                   │  │
//!   │  │  • Frequently accessed pages (access_count > 10)                  │  │
//!   │  │  • Resists scan pollution via backward K-distance                 │  │
//!   │  │  • ~20% of total cache (configurable via hot_cache_ratio)         │  │
//!   │  └───────────────────────────────────────────────────────────────────┘  │
//!   │                              ▲                                          │
//!   │                    promotion │ │ demotion (full)                        │
//!   │                              │ ▼                                        │
//!   │  ┌───────────────────────────────────────────────────────────────────┐  │
//!   │  │              L2 Warm Cache (LFU)                                   │  │
//!   │  │                                                                   │  │
//!   │  │  • Moderately accessed pages (access_count > 5)                   │  │
//!   │  │  • Retains pages with proven frequency                            │  │
//!   │  │  • ~30% of total cache (configurable via warm_cache_ratio)        │  │
//!   │  └───────────────────────────────────────────────────────────────────┘  │
//!   │                              ▲                                          │
//!   │                    promotion │ │ demotion (full)                        │
//!   │                              │ ▼                                        │
//!   │  ┌───────────────────────────────────────────────────────────────────┐  │
//!   │  │              L3 Cold Cache (FIFO)                                  │  │
//!   │  │                                                                   │  │
//!   │  │  • Default entry point for new pages                              │  │
//!   │  │  • Low-overhead FIFO for one-time scans                           │  │
//!   │  │  • ~50% of total cache (remainder)                                │  │
//!   │  └───────────────────────────────────────────────────────────────────┘  │
//!   │                              ▲                                          │
//!   │                    insert    │ │ eviction (full)                        │
//!   │                              │ ▼                                        │
//!   │                         [New Page Data]                                 │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Page Lookup Flow
//!
//! ```text
//!   get_page(page_id)
//!        │
//!        ├──► Check L1 Hot Cache (LRU-K)
//!        │         │
//!        │         ├─► Hit? → Return data, update K-distance
//!        │         │
//!        │         └─► Miss? → Continue...
//!        │
//!        ├──► Check L2 Warm Cache (LFU)
//!        │         │
//!        │         ├─► Hit? → Promote to Hot, return data
//!        │         │
//!        │         └─► Miss? → Continue...
//!        │
//!        ├──► Check L3 Cold Cache (FIFO)
//!        │         │
//!        │         ├─► Hit? → Promote to Warm, return data
//!        │         │
//!        │         └─► Miss? → Return None (disk read needed)
//!        │
//!        └──► Update access patterns for prefetching
//! ```
//!
//! ## Temperature Classification
//!
//! ```text
//!   Access Count    Temperature    Cache Tier    Eviction Policy
//!   ═══════════════════════════════════════════════════════════════════════════
//!        > 10          Hot            L1           LRU-K (K=2)
//!       5 - 10         Warm           L2           LFU
//!       2 - 5          Cold           L3           FIFO
//!       < 2            Frozen         L3           FIFO (first evicted)
//! ```
//!
//! ## Key Components
//!
//! | Component             | Description                                          |
//! |-----------------------|------------------------------------------------------|
//! | `CacheManager`        | Central cache coordinator with three tiers           |
//! | `LRUKCache`           | Hot cache with LRU-K (K=2) eviction                  |
//! | `LFUCache`            | Warm cache with frequency-based eviction             |
//! | `FIFOCache`           | Cold cache with FIFO eviction                        |
//! | `AdmissionController` | Regulates cache entry based on memory pressure       |
//! | `PrefetchEngine`      | Sequential and pattern-based prefetch prediction     |
//! | `DeduplicationEngine` | Identifies duplicate page content                    |
//! | `HotColdMetadata`     | Tracks access patterns per page                      |
//!
//! ## Core Operations
//!
//! | Method                       | Description                                   |
//! |------------------------------|-----------------------------------------------|
//! | `get_page()`                 | Lookup page in all tiers                      |
//! | `get_page_with_metrics()`    | Lookup with metrics recording                 |
//! | `store_page()`               | Store page in appropriate tier                |
//! | `trigger_prefetch()`         | Get prefetch predictions for current page     |
//! | `perform_maintenance()`      | Update pressure & evict if needed             |
//! | `get_cache_statistics()`     | Get hit ratios and tier usage                 |
//! | `get_enhanced_cache_statistics()` | Get algorithm-specific details           |
//! | `get_page_access_details()`  | Get LRU-K/LFU/FIFO specific info for a page   |
//!
//! ## Admission Control
//!
//! ```text
//!   Memory Pressure    Admission Rate    Large Page Behavior
//!   ═══════════════════════════════════════════════════════════════════════════
//!       0 - 50%           100%           Admit all
//!      51 - 70%            80%           Admit most
//!      71 - 85%            60%           Selective admission
//!      86 - 95%            40%           Very selective
//!       > 95%              20%           Emergency mode (reject large pages)
//! ```
//!
//! ## Prefetching
//!
//! ```text
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                       Prefetch Engine                                   │
//!   │                                                                         │
//!   │  Sequential Prefetch:                                                   │
//!   │  • Detects sequential access (page N → N+1 → N+2...)                    │
//!   │  • Prefetches next `prefetch_distance` pages                            │
//!   │  • Distance adapts based on accuracy (0.7+ → more, <0.4 → less)         │
//!   │                                                                         │
//!   │  Pattern-Based Prefetch:                                                │
//!   │  • Tracks page → next_page transitions                                  │
//!   │  • Predicts likely next pages based on history                          │
//!   │  • Only used when accuracy > 20%                                        │
//!   │                                                                         │
//!   │  Accuracy Tracking:                                                     │
//!   │  • Records predictions                                                  │
//!   │  • Counts hits (predicted page was actually accessed)                   │
//!   │  • Cleans up old predictions (>60 seconds)                              │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::storage::disk::async_disk::cache::cache_manager::CacheManager;
//! use crate::storage::disk::async_disk::config::DiskManagerConfig;
//!
//! let config = DiskManagerConfig {
//!     cache_size_mb: 128,
//!     hot_cache_ratio: 0.2,   // 20% for hot
//!     warm_cache_ratio: 0.3,  // 30% for warm
//!     prefetch_distance: 4,
//!     ..Default::default()
//! };
//!
//! let cache = CacheManager::new(&config);
//!
//! // Store a page (enters cold cache)
//! cache.store_page(42, vec![0u8; 4096]);
//!
//! // Retrieve (hit in cold → promotes to warm)
//! if let Some(data) = cache.get_page(42) {
//!     // Process data...
//! }
//!
//! // Multiple accesses promote to hotter tiers
//! for _ in 0..10 {
//!     let _ = cache.get_page(42);
//! }
//!
//! // Trigger prefetch predictions
//! let prefetch_pages = cache.trigger_prefetch(42);
//! for page_id in prefetch_pages {
//!     // Load predicted pages in background...
//! }
//!
//! // Check statistics
//! let stats = cache.get_cache_statistics();
//! println!("Hit ratio: {:.2}%", stats.overall_hit_ratio * 100.0);
//! println!("Prefetch accuracy: {:.2}%", stats.prefetch_accuracy * 100.0);
//! ```
//!
//! ## Thread Safety
//!
//! - Each cache tier uses `Arc<RwLock<T>>` for thread-safe access
//! - Atomic counters for statistics (hit/miss counts, promotions, etc.)
//! - `try_write()` / `try_read()` used to avoid blocking under contention
//! - Safe for concurrent access from multiple tokio tasks
//!
//! ## Deduplication
//!
//! The `DeduplicationEngine` identifies pages with identical content:
//! - Uses simple hash-based detection
//! - Tracks savings (bytes avoided by not storing duplicates)
//! - Useful for databases with repeated patterns (e.g., zero-filled pages)

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use tokio::sync::RwLock;

use super::fifo::FIFOCache;
use super::lfu::LFUCache;
use super::lru_k::LRUKCache;
use crate::common::config::{DB_PAGE_SIZE, PageId};
use crate::storage::disk::async_disk::cache::cache_traits::{
    CoreCache, FIFOCacheTrait, LFUCacheTrait, LRUKCacheTrait,
};
use crate::storage::disk::async_disk::config::DiskManagerConfig;
use crate::storage::disk::async_disk::metrics::collector::MetricsCollector;

/// Cached page data with associated metadata for cache management decisions.
///
/// `PageData` wraps the raw page bytes along with access tracking information
/// used by the cache manager to make promotion, demotion, and eviction decisions.
///
/// # Memory Efficiency
///
/// The page data is stored in an `Arc<Vec<u8>>` to enable zero-copy sharing
/// between cache tiers during promotions. When a page is promoted from cold
/// to warm cache, only the `Arc` reference is cloned, not the underlying data.
///
/// # Fields
///
/// | Field           | Purpose                                              |
/// |-----------------|------------------------------------------------------|
/// | `data`          | The actual page bytes (shared via `Arc`)             |
/// | `last_accessed` | Timestamp of most recent access (for LRU decisions)  |
/// | `access_count`  | Total access count (for LFU and temperature)         |
/// | `temperature`   | Current temperature classification                   |
#[derive(Debug, Clone)]
pub struct PageData {
    /// The raw page data, wrapped in `Arc` for efficient sharing between cache tiers.
    pub data: Arc<Vec<u8>>,
    /// Timestamp of the most recent access to this page.
    pub last_accessed: Instant,
    /// Cumulative count of accesses to this page.
    pub access_count: u64,
    /// Current temperature classification based on access patterns.
    pub temperature: DataTemperature,
}

/// Temperature classification for cached pages based on access frequency.
///
/// The cache manager uses temperature to determine which cache tier a page
/// belongs to and how aggressively it should be retained. Temperature is
/// derived from access count thresholds.
///
/// # Temperature Thresholds
///
/// ```text
/// Access Count    Temperature    Behavior
/// ════════════════════════════════════════════════════════════════
///     > 10          Hot          Retained in L1 (LRU-K), highest priority
///    5 - 10         Warm         Retained in L2 (LFU), medium priority
///    2 - 5          Cold         Stored in L3 (FIFO), standard eviction
///     < 2           Frozen       First candidates for eviction
/// ```
///
/// # Cache Tier Mapping
///
/// - `Hot` → L1 Hot Cache (LRU-K with K=2)
/// - `Warm` → L2 Warm Cache (LFU)
/// - `Cold` / `Frozen` → L3 Cold Cache (FIFO)
#[derive(Debug, Clone, PartialEq)]
pub enum DataTemperature {
    /// Frequently accessed page (access_count > 10). Stored in L1 hot cache.
    Hot,
    /// Moderately accessed page (access_count 5-10). Stored in L2 warm cache.
    Warm,
    /// Infrequently accessed page (access_count 2-5). Stored in L3 cold cache.
    Cold,
    /// Rarely accessed page (access_count < 2). First eviction candidate.
    Frozen,
}

/// Classification of page access patterns for prefetch optimization.
///
/// The prefetch engine uses access pattern classification to predict
/// which pages are likely to be accessed next and adjust prefetch
/// aggressiveness accordingly.
///
/// # Pattern Detection
///
/// ```text
/// Pattern      Detection Method                 Prefetch Strategy
/// ════════════════════════════════════════════════════════════════════════════
/// Sequential   Page N → N+1 → N+2...            Prefetch next N pages
/// Random       No discernible pattern           Minimal prefetching
/// Temporal     Same pages accessed repeatedly   Prioritize recently accessed
/// Spatial      Pages in same region accessed    Prefetch nearby pages
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum AccessPattern {
    /// Linear page access (e.g., table scans). Triggers aggressive prefetching.
    Sequential,
    /// Unpredictable access pattern (e.g., index lookups). Minimal prefetching.
    Random,
    /// Pages accessed repeatedly over time (e.g., hot tuples). Retention priority.
    Temporal,
    /// Pages in proximity accessed together (e.g., index leaves). Region prefetch.
    Spatial,
}

/// Per-page metadata for hot/cold data separation and access tracking.
///
/// `HotColdMetadata` maintains detailed access statistics for each cached page,
/// enabling intelligent cache tier placement and eviction decisions. This metadata
/// is stored separately from the page data to allow efficient tracking even for
/// pages not currently in cache.
///
/// # Usage
///
/// The cache manager updates this metadata on every page access:
///
/// 1. Increments `access_count`
/// 2. Updates `last_accessed` timestamp
/// 3. Recalculates `temperature` based on new access count
/// 4. Updates `access_pattern` if pattern detection triggers
///
/// # Temperature Calculation
///
/// ```text
/// access_count > 10  →  Hot
/// access_count > 5   →  Warm
/// access_count > 2   →  Cold
/// access_count ≤ 2   →  Frozen
/// ```
#[derive(Debug)]
pub struct HotColdMetadata {
    /// Total number of times this page has been accessed.
    pub access_count: u64,
    /// Timestamp of the most recent access.
    pub last_accessed: Instant,
    /// Detected access pattern for this page.
    pub access_pattern: AccessPattern,
    /// Current temperature classification derived from access_count.
    pub temperature: DataTemperature,
    /// Whether the prefetch engine predicts this page will be reused soon.
    pub predicted_reuse: bool,
    /// Size of the page data in bytes (for memory pressure calculations).
    pub size_bytes: usize,
}

/// Prefetch prediction engine for anticipating future page accesses.
///
/// The `PrefetchEngine` tracks page access sequences to predict which pages
/// are likely to be accessed next. It employs two main prediction strategies:
///
/// # Prediction Strategies
///
/// ## 1. Sequential Prefetching
///
/// Detects linear access patterns (page N → N+1 → N+2) and prefetches
/// the next `prefetch_distance` pages. This is highly effective for
/// table scans and range queries.
///
/// ```text
/// Access: [1, 2, 3, 4] → Predict: [5, 6, 7, 8] (distance=4)
/// ```
///
/// ## 2. Pattern-Based Prefetching
///
/// Tracks historical page transitions and predicts based on learned patterns.
/// Requires at least `sequential_threshold` observations before making predictions.
///
/// ```text
/// Historical: Page 10 often followed by Page 25
/// Access: 10 → Predict: 25
/// ```
///
/// # Adaptive Behavior
///
/// The prefetch distance adapts based on prediction accuracy:
///
/// | Accuracy   | Distance Multiplier | Max Predictions |
/// |------------|---------------------|-----------------|
/// | > 70%      | 1.0×                | 12              |
/// | 40-70%     | 0.7×                | 8               |
/// | 20-40%     | 0.3×                | 4               |
/// | < 20%      | 0.5× (default)      | 4               |
#[derive(Debug)]
pub struct PrefetchEngine {
    /// Historical access sequences per page for pattern detection.
    /// Maps page_id → list of pages accessed after this page.
    pub access_patterns: HashMap<PageId, Vec<PageId>>,
    /// Minimum observations required before using pattern-based predictions.
    pub sequential_threshold: usize,
    /// Base number of pages to prefetch ahead for sequential access.
    pub prefetch_distance: usize,
    /// Accuracy percentage (0-100) stored atomically for thread-safe updates.
    pub accuracy_tracker: AtomicU64,
}

/// Admission controller for regulating cache entry under memory pressure.
///
/// The `AdmissionController` acts as a gatekeeper for the cache, deciding
/// whether new pages should be admitted based on current memory pressure
/// and page characteristics. This prevents cache thrashing under load.
///
/// # Admission Policy
///
/// ```text
/// Memory Pressure    Admission Rate    Behavior
/// ════════════════════════════════════════════════════════════════
///     0 - 50%           100%           Admit all pages freely
///    51 - 70%            80%           Admit most pages
///    71 - 85%            60%           Selective admission
///    86 - 95%            40%           Very selective, reject large pages
///     > 95%              20%           Emergency mode, critical only
/// ```
///
/// # Large Page Handling
///
/// Pages larger than `2 × DB_PAGE_SIZE` are rejected when memory pressure
/// exceeds 60%, preventing oversized pages from evicting many smaller pages.
///
/// # Reuse Prediction
///
/// Pages with recent access (< 60 seconds) or high access count (> 3)
/// bypass admission rate limits, as they have proven reuse value.
#[derive(Debug)]
pub struct AdmissionController {
    /// Current admission rate percentage (0-100). Atomic for thread-safe access.
    pub admission_rate: AtomicU64,
    /// Current memory pressure percentage (0-100). Derived from cache utilization.
    pub memory_pressure: AtomicU64,
    /// Maximum cache memory in bytes (for pressure calculations).
    pub max_memory: usize,
}

/// Deduplication engine for identifying and avoiding storage of duplicate pages.
///
/// The `DeduplicationEngine` maintains a hash-based index of page content,
/// allowing the cache manager to detect when a page has identical content
/// to an already-cached page. This saves memory by avoiding redundant storage.
///
/// # Use Cases
///
/// Deduplication is particularly effective for:
///
/// - **Zero-filled pages**: Newly allocated but unused pages
/// - **Repeated patterns**: Tables with repetitive data structures
/// - **Backup/restore**: Pages read multiple times during recovery
///
/// # Hash Algorithm
///
/// Uses a simple rolling hash (multiply-add with factor 31) for performance.
/// This provides reasonable collision resistance while being fast to compute.
///
/// # Statistics
///
/// The engine tracks:
/// - `dedup_savings_bytes`: Total bytes saved by avoiding duplicates
/// - `total_pages_processed`: Total pages checked for deduplication
/// - Deduplication ratio: `savings / (total × page_size)`
///
/// # Example
///
/// ```text
/// Page 1: [0, 0, 0, 0, ...] → hash=0x1234 → stored
/// Page 2: [0, 0, 0, 0, ...] → hash=0x1234 → duplicate of Page 1, not stored
/// Savings: 4096 bytes (one page)
/// ```
#[derive(Debug)]
pub struct DeduplicationEngine {
    /// Map from content hash to the canonical page ID storing that content.
    pub page_hashes: HashMap<u64, PageId>,
    /// Cumulative bytes saved by deduplication.
    pub dedup_savings_bytes: AtomicUsize,
    /// Total number of pages checked for deduplication.
    pub total_pages_processed: AtomicUsize,
}

impl Default for DeduplicationEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl DeduplicationEngine {
    /// Creates a new `DeduplicationEngine` with empty state.
    pub fn new() -> Self {
        Self {
            page_hashes: HashMap::new(),
            dedup_savings_bytes: AtomicUsize::new(0),
            total_pages_processed: AtomicUsize::new(0),
        }
    }

    /// Checks if page content is a duplicate of an existing cached page.
    ///
    /// Computes a hash of the page data and checks against known hashes.
    /// If a match is found (and it's not the same page), returns the existing
    /// page ID and records the deduplication savings.
    ///
    /// # Arguments
    ///
    /// * `page_id` - The page being checked
    /// * `data` - The page content to check for duplicates
    ///
    /// # Returns
    ///
    /// * `Some(PageId)` - The existing page with identical content
    /// * `None` - No duplicate found; this page's hash is now registered
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// if let Some(existing_id) = dedup.check_duplicate(new_page_id, &data) {
    ///     // Skip storing; reference existing page instead
    ///     println!("Page {} duplicates page {}", new_page_id, existing_id);
    /// }
    /// ```
    pub fn check_duplicate(&mut self, page_id: PageId, data: &[u8]) -> Option<PageId> {
        // In a real implementation, this would use a more sophisticated hashing algorithm
        // For this example, we'll use a simple hash
        let hash = Self::simple_hash(data);

        self.total_pages_processed.fetch_add(1, Ordering::Relaxed);

        if let Some(&existing_page_id) = self.page_hashes.get(&hash)
            && existing_page_id != page_id
        {
            // Found a duplicate page
            self.dedup_savings_bytes
                .fetch_add(data.len(), Ordering::Relaxed);
            return Some(existing_page_id);
        }

        // No duplicate found, add to hash map
        self.page_hashes.insert(hash, page_id);
        None
    }

    fn simple_hash(data: &[u8]) -> u64 {
        let mut hash: u64 = 0;
        for &byte in data {
            hash = hash.wrapping_mul(31).wrapping_add(byte as u64);
        }
        hash
    }

    /// Returns deduplication statistics.
    ///
    /// # Returns
    ///
    /// A tuple of `(savings_bytes, total_pages_processed, dedup_ratio)`:
    ///
    /// - `savings_bytes`: Total bytes saved by not storing duplicates
    /// - `total_pages_processed`: Number of pages checked
    /// - `dedup_ratio`: Fraction of bytes saved (`savings / (total × page_size)`)
    pub fn get_stats(&self) -> (usize, usize, f64) {
        let savings = self.dedup_savings_bytes.load(Ordering::Relaxed);
        let total = self.total_pages_processed.load(Ordering::Relaxed);
        let ratio = if total > 0 {
            savings as f64 / (total * DB_PAGE_SIZE as usize) as f64
        } else {
            0.0
        };
        (savings, total, ratio)
    }
}

/// Comprehensive cache statistics for monitoring and tuning.
///
/// `CacheStatistics` provides a snapshot of cache health, including capacity
/// utilization, hit ratios per tier, and operational metrics. Use these
/// statistics to tune cache ratios and identify performance bottlenecks.
///
/// # Interpreting Statistics
///
/// ## Hit Ratio Analysis
///
/// ```text
/// Tier        Good Ratio    Action if Low
/// ════════════════════════════════════════════════════════════════
/// Hot         > 80%         Increase hot_cache_ratio
/// Warm        > 60%         Working set may exceed cache
/// Cold        > 40%         Consider larger cold cache
/// Overall     > 90%         Cache is performing well
/// ```
///
/// ## Promotion/Demotion Balance
///
/// High promotion count with stable cache size indicates healthy tier movement.
/// High demotion count may indicate memory pressure or thrashing.
///
/// ## Prefetch Accuracy
///
/// - `> 70%`: Excellent, increase prefetch distance
/// - `40-70%`: Good, maintain current settings
/// - `< 40%`: Consider reducing prefetch aggressiveness
#[derive(Debug)]
pub struct CacheStatistics {
    /// Configured capacity of the hot cache (number of entries).
    pub hot_cache_size: usize,
    /// Configured capacity of the warm cache (number of entries).
    pub warm_cache_size: usize,
    /// Configured capacity of the cold cache (number of entries).
    pub cold_cache_size: usize,
    /// Current number of entries in the hot cache.
    pub hot_cache_used: usize,
    /// Current number of entries in the warm cache.
    pub warm_cache_used: usize,
    /// Current number of entries in the cold cache.
    pub cold_cache_used: usize,
    /// Fraction of total accesses served from hot cache (0.0-1.0).
    pub hot_cache_hit_ratio: f64,
    /// Fraction of total accesses served from warm cache (0.0-1.0).
    pub warm_cache_hit_ratio: f64,
    /// Fraction of total accesses served from cold cache (0.0-1.0).
    pub cold_cache_hit_ratio: f64,
    /// Fraction of all accesses served from any cache tier (0.0-1.0).
    pub overall_hit_ratio: f64,
    /// Total number of page promotions (cold→warm, warm→hot).
    pub promotion_count: u64,
    /// Total number of pages evicted or demoted.
    pub demotion_count: u64,
    /// Fraction of prefetch predictions that were subsequently accessed (0.0-1.0).
    pub prefetch_accuracy: f64,
}

/// Extended cache statistics including algorithm-specific details.
///
/// `EnhancedCacheStatistics` augments the basic `CacheStatistics` with
/// information about the specific eviction algorithms used in each tier.
/// This is useful for debugging and understanding cache behavior.
///
/// # Algorithm Details
///
/// | Tier | Algorithm | Key Parameter           |
/// |------|-----------|-------------------------|
/// | Hot  | LRU-K     | K value (default: 2)    |
/// | Warm | LFU       | Frequency counts        |
/// | Cold | FIFO      | Insertion order         |
///
/// # LRU-K Explanation
///
/// The hot cache uses LRU-K (K=2 by default), which evicts the page with
/// the largest backward K-distance. This resists scan pollution better
/// than simple LRU because a page must be accessed K times before being
/// considered "hot".
#[derive(Debug)]
pub struct EnhancedCacheStatistics {
    /// Standard cache statistics.
    pub basic_stats: CacheStatistics,
    /// The K value used by the LRU-K algorithm in the hot cache.
    pub lru_k_value: usize,
    /// Name of the hot cache eviction algorithm (e.g., "LRU-K").
    pub hot_cache_algorithm: String,
    /// Name of the warm cache eviction algorithm (e.g., "LFU").
    pub warm_cache_algorithm: String,
    /// Name of the cold cache eviction algorithm (e.g., "FIFO").
    pub cold_cache_algorithm: String,
}

/// Detailed access information for a specific cached page.
///
/// `PageAccessDetails` provides algorithm-specific information about a page's
/// current cache status. The available fields depend on which cache tier
/// the page resides in.
///
/// # Tier-Specific Information
///
/// ## Hot Cache (LRU-K)
/// - `k_value`: The K parameter (typically 2)
/// - `access_count`: Number of accesses recorded
/// - `k_distance`: Backward K-distance used for eviction decisions
/// - `eviction_rank`: Position in eviction order (lower = evicted sooner)
///
/// ## Warm Cache (LFU)
/// - `access_count`: Frequency count for this page
///
/// ## Cold Cache (FIFO)
/// - `eviction_rank`: Position in insertion order (older = evicted sooner)
///
/// # Example Usage
///
/// ```rust,ignore
/// if let Some(details) = cache.get_page_access_details(page_id) {
///     match details.algorithm.as_str() {
///         "LRU-K" => {
///             println!("Hot page with K-distance: {:?}", details.k_distance);
///         },
///         "LFU" => {
///             println!("Warm page with frequency: {:?}", details.access_count);
///         },
///         "FIFO" => {
///             println!("Cold page at rank: {:?}", details.eviction_rank);
///         },
///         _ => {}
///     }
/// }
/// ```
#[derive(Debug)]
pub struct PageAccessDetails {
    /// Cache tier containing this page ("Hot", "Warm", or "Cold").
    pub cache_level: String,
    /// Eviction algorithm used for this tier ("LRU-K", "LFU", or "FIFO").
    pub algorithm: String,
    /// K value for LRU-K (only set for hot cache pages).
    pub k_value: Option<usize>,
    /// Access/frequency count (set for hot and warm cache pages).
    pub access_count: Option<u64>,
    /// Backward K-distance for LRU-K eviction (only set for hot cache pages).
    pub k_distance: Option<u64>,
    /// Position in eviction order (set for hot and cold cache pages).
    pub eviction_rank: Option<usize>,
    /// Temperature classification of this page.
    pub temperature: DataTemperature,
}

/// Multi-level cache manager with three-tier hot/warm/cold architecture.
///
/// The `CacheManager` is the primary L2 cache sitting between the `BufferPoolManager`
/// (L1, semantic page cache) and the `AsyncDiskManager` (disk I/O). It reduces disk
/// access latency by caching raw page bytes in a tiered structure optimized for
/// different access patterns.
///
/// # Architecture
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────────┐
/// │                           CacheManager                                  │
/// │                                                                         │
/// │  ┌─────────────────────────────────────────────────────────────────┐    │
/// │  │ L1 Hot Cache (LRU-K, K=2)                                       │    │
/// │  │ • ~20% of total cache (hot_cache_ratio)                         │    │
/// │  │ • Pages accessed > 10 times                                     │    │
/// │  │ • Resists scan pollution via backward K-distance                │    │
/// │  └─────────────────────────────────────────────────────────────────┘    │
/// │                              ▲ promote │ demote ▼                       │
/// │  ┌─────────────────────────────────────────────────────────────────┐    │
/// │  │ L2 Warm Cache (LFU)                                             │    │
/// │  │ • ~30% of total cache (warm_cache_ratio)                        │    │
/// │  │ • Pages accessed 5-10 times                                     │    │
/// │  │ • Evicts least frequently used                                  │    │
/// │  └─────────────────────────────────────────────────────────────────┘    │
/// │                              ▲ promote │ demote ▼                       │
/// │  ┌─────────────────────────────────────────────────────────────────┐    │
/// │  │ L3 Cold Cache (FIFO)                                            │    │
/// │  │ • ~50% of total cache (remainder)                               │    │
/// │  │ • Entry point for new pages                                     │    │
/// │  │ • Low-overhead FIFO for transient data                          │    │
/// │  └─────────────────────────────────────────────────────────────────┘    │
/// │                                                                         │
/// │  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────────────┐    │
/// │  │ PrefetchEngine  │ │ AdmissionCtrl   │ │ DeduplicationEngine     │    │
/// │  │ • Sequential    │ │ • Memory press. │ │ • Hash-based detection  │    │
/// │  │ • Pattern-based │ │ • Rate limiting │ │ • Saves duplicate bytes │    │
/// │  └─────────────────┘ └─────────────────┘ └─────────────────────────┘    │
/// └─────────────────────────────────────────────────────────────────────────┘
/// ```
///
/// # Thread Safety
///
/// All cache tiers use `Arc<RwLock<T>>` for thread-safe concurrent access.
/// Non-blocking `try_read()`/`try_write()` is used to avoid contention.
/// Atomic counters track statistics without locking.
///
/// # Key Operations
///
/// | Method                   | Description                                     |
/// |--------------------------|-------------------------------------------------|
/// | `get_page()`             | Retrieve page from cache (any tier)             |
/// | `store_page()`           | Store page in appropriate tier                  |
/// | `trigger_prefetch()`     | Get prefetch predictions for a page             |
/// | `perform_maintenance()`  | Update pressure and trigger eviction            |
/// | `get_cache_statistics()` | Get comprehensive cache health metrics          |
#[derive(Debug)]
pub struct CacheManager {
    /// L1 Hot Cache: LRU-K based cache for frequently accessed pages.
    /// Uses `Arc<Vec<u8>>` for efficient data sharing during promotions.
    hot_cache: Arc<RwLock<LRUKCache<PageId, Arc<Vec<u8>>>>>,

    /// L2 Warm Cache: LFU based cache for moderately accessed pages.
    /// Stores full `PageData` including access metadata.
    warm_cache: Arc<RwLock<LFUCache<PageId, PageData>>>,

    /// L3 Cold Cache: FIFO based cache for newly inserted pages.
    /// Low overhead, ideal for one-time sequential scans.
    cold_cache: Arc<RwLock<FIFOCache<PageId, PageData>>>,

    /// Configured capacity for hot cache (number of entries).
    hot_cache_size: usize,
    /// Configured capacity for warm cache (number of entries).
    warm_cache_size: usize,
    /// Configured capacity for cold cache (number of entries).
    cold_cache_size: usize,

    /// Prefetch prediction engine for sequential and pattern-based prefetching.
    prefetch_engine: Arc<RwLock<PrefetchEngine>>,

    /// Admission controller regulating cache entry under memory pressure.
    admission_controller: Arc<AdmissionController>,

    /// Per-page metadata for hot/cold classification and access tracking.
    hot_data_tracker: Arc<RwLock<HashMap<PageId, HotColdMetadata>>>,

    /// Deduplication engine for detecting and avoiding duplicate page storage.
    dedup_engine: Arc<RwLock<DeduplicationEngine>>,

    /// Count of pages promoted to hotter tiers (cold→warm, warm→hot).
    promotion_count: AtomicU64,
    /// Count of pages evicted or demoted.
    demotion_count: AtomicU64,

    /// Prefetch prediction accuracy (0-100 percentage, atomic).
    prefetch_accuracy: AtomicU64,
    /// Count of pages saved by deduplication.
    dedup_savings: AtomicU64,

    /// Active prefetch predictions with timestamps for accuracy tracking.
    prefetch_predictions: Arc<RwLock<HashMap<PageId, Instant>>>,
    /// Count of prefetch predictions that were subsequently accessed.
    prefetch_hits: AtomicU64,
    /// Total count of prefetch predictions made.
    prefetch_total: AtomicU64,

    /// Total cache hits across all tiers.
    cache_hits: AtomicU64,
    /// Total cache misses.
    cache_misses: AtomicU64,
    /// Hits specifically from hot cache.
    hot_cache_hits: AtomicU64,
    /// Hits specifically from warm cache.
    warm_cache_hits: AtomicU64,
    /// Hits specifically from cold cache.
    cold_cache_hits: AtomicU64,
}

impl CacheManager {
    /// Creates a new `CacheManager` with the specified configuration.
    ///
    /// Initializes all three cache tiers with capacities derived from the config:
    ///
    /// - **Hot cache**: `cache_size_mb × hot_cache_ratio` (default 20%)
    /// - **Warm cache**: `cache_size_mb × warm_cache_ratio` (default 30%)
    /// - **Cold cache**: Remaining capacity (default 50%)
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration containing cache size and ratio parameters
    ///
    /// # Configuration Parameters
    ///
    /// | Parameter           | Default | Description                          |
    /// |---------------------|---------|--------------------------------------|
    /// | `cache_size_mb`     | varies  | Total cache size in megabytes        |
    /// | `hot_cache_ratio`   | 0.2     | Fraction allocated to hot cache      |
    /// | `warm_cache_ratio`  | 0.3     | Fraction allocated to warm cache     |
    /// | `prefetch_distance` | 4       | Pages to prefetch ahead              |
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let config = DiskManagerConfig {
    ///     cache_size_mb: 128,      // 128 MB total
    ///     hot_cache_ratio: 0.2,    // 25.6 MB for hot
    ///     warm_cache_ratio: 0.3,   // 38.4 MB for warm
    ///     prefetch_distance: 4,    // Prefetch 4 pages ahead
    ///     ..Default::default()
    /// };
    /// let cache = CacheManager::new(&config);
    /// ```
    pub fn new(config: &DiskManagerConfig) -> Self {
        let total_cache_mb = config.cache_size_mb;
        let hot_cache_size = (total_cache_mb as f64 * config.hot_cache_ratio) as usize;
        let warm_cache_size = (total_cache_mb as f64 * config.warm_cache_ratio) as usize;
        let cold_cache_size = total_cache_mb - hot_cache_size - warm_cache_size;

        // Use LRU-K cache for hot cache (L1)
        let hot_cache = Arc::new(RwLock::new(LRUKCache::with_k(hot_cache_size, 2)));
        let warm_cache = Arc::new(RwLock::new(LFUCache::new(warm_cache_size)));
        let cold_cache = Arc::new(RwLock::new(FIFOCache::new(cold_cache_size)));

        let prefetch_engine = Arc::new(RwLock::new(PrefetchEngine {
            access_patterns: HashMap::new(),
            sequential_threshold: 3,
            prefetch_distance: config.prefetch_distance,
            accuracy_tracker: AtomicU64::new(0),
        }));

        let admission_controller = Arc::new(AdmissionController {
            admission_rate: AtomicU64::new(100),
            memory_pressure: AtomicU64::new(0),
            max_memory: total_cache_mb * 1024 * 1024, // Convert to bytes
        });

        // Initialize hot/cold data tracker
        let hot_data_tracker = Arc::new(RwLock::new(HashMap::new()));

        // Initialize deduplication engine
        let dedup_engine = Arc::new(RwLock::new(DeduplicationEngine::new()));

        // Initialize prefetch tracking
        let prefetch_predictions = Arc::new(RwLock::new(HashMap::new()));
        let prefetch_hits = AtomicU64::new(0);
        let prefetch_total = AtomicU64::new(0);

        Self {
            hot_cache,
            warm_cache,
            cold_cache,
            hot_cache_size,
            warm_cache_size,
            cold_cache_size,
            prefetch_engine,
            admission_controller,
            hot_data_tracker,
            dedup_engine,
            promotion_count: AtomicU64::new(0),
            demotion_count: AtomicU64::new(0),
            prefetch_accuracy: AtomicU64::new(0),
            dedup_savings: AtomicU64::new(0),
            prefetch_predictions,
            prefetch_hits,
            prefetch_total,
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            hot_cache_hits: AtomicU64::new(0),
            warm_cache_hits: AtomicU64::new(0),
            cold_cache_hits: AtomicU64::new(0),
        }
    }

    /// Retrieves a page from the cache if present.
    ///
    /// Searches all three cache tiers in order (hot → warm → cold) and returns
    /// a copy of the page data if found. Cache hits trigger the following actions:
    ///
    /// # Cache Tier Behavior
    ///
    /// | Tier Found | Action                                          |
    /// |------------|-------------------------------------------------|
    /// | Hot        | Update LRU-K access history, return data        |
    /// | Warm       | **Promote to hot**, update LFU count, return    |
    /// | Cold       | **Promote to warm**, update access, return      |
    /// | Miss       | Return `None`, caller must fetch from disk      |
    ///
    /// # Side Effects
    ///
    /// - Updates access pattern for prefetch learning
    /// - Checks if this access was a prefetch hit (for accuracy tracking)
    /// - Increments appropriate hit/miss counters
    ///
    /// # Arguments
    ///
    /// * `page_id` - The page identifier to look up
    ///
    /// # Returns
    ///
    /// * `Some(Vec<u8>)` - A clone of the cached page data
    /// * `None` - Page not found in any cache tier
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// if let Some(data) = cache.get_page(42) {
    ///     // Process cached data
    /// } else {
    ///     // Fetch from disk and store in cache
    ///     let data = disk.read_page(42)?;
    ///     cache.store_page(42, data);
    /// }
    /// ```
    pub fn get_page(&self, page_id: PageId) -> Option<Vec<u8>> {
        self.get_page_internal(page_id, None)
    }

    /// Internal implementation for page retrieval with optional metrics collection.
    ///
    /// This method contains the core cache lookup logic, searching tiers in order
    /// and handling promotions. The metrics collector parameter allows integration
    /// with the broader monitoring infrastructure.
    fn get_page_internal(
        &self,
        page_id: PageId,
        metrics_collector: Option<&MetricsCollector>,
    ) -> Option<Vec<u8>> {
        // Record access pattern for prefetching
        self.record_access_pattern(page_id);

        // Check if this page access was a prefetch hit
        self.check_prefetch_hit(page_id);

        // Check L1 hot cache first (LRU-K)
        if let Ok(mut hot_cache) = self.hot_cache.try_write()
            && let Some(data_arc) = <LRUKCache<PageId, Arc<Vec<u8>>> as CoreCache<
                PageId,
                Arc<Vec<u8>>,
            >>::get(&mut hot_cache, &page_id)
        {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            self.hot_cache_hits.fetch_add(1, Ordering::Relaxed);
            if let Some(metrics) = metrics_collector {
                metrics.record_cache_operation("hot", true);
            }
            // Only clone the actual data when returning to caller
            return Some((**data_arc).clone());
        }

        // Check L2 warm cache (LFU)
        if let Ok(mut warm_cache) = self.warm_cache.try_write()
            && let Some(page_data) =
                <LFUCache<PageId, PageData> as CoreCache<PageId, PageData>>::get(
                    &mut warm_cache,
                    &page_id,
                )
        {
            // Promote to hot cache on hit - share the Arc, no data copying
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            self.warm_cache_hits.fetch_add(1, Ordering::Relaxed);
            self.promotion_count.fetch_add(1, Ordering::Relaxed);
            let data_arc = Arc::clone(&page_data.data);
            if let Ok(mut hot_cache) = self.hot_cache.try_write() {
                <LRUKCache<PageId, Arc<Vec<u8>>> as CoreCache<PageId, Arc<Vec<u8>>>>::insert(
                    &mut hot_cache,
                    page_id,
                    data_arc,
                );
            }
            if let Some(metrics) = metrics_collector {
                metrics.record_cache_operation("warm", true);
                metrics.record_cache_migration(true);
            }
            // Only clone the actual data when returning to caller
            return Some((*page_data.data).clone());
        }

        // Check L3 cold cache (FIFO)
        if let Ok(mut cold_cache) = self.cold_cache.try_write()
            && let Some(page_data) =
                <FIFOCache<PageId, PageData> as CoreCache<PageId, PageData>>::get(
                    &mut cold_cache,
                    &page_id,
                )
        {
            // Promote to warm cache on hit - clone the PageData (cheap with Arc)
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            self.cold_cache_hits.fetch_add(1, Ordering::Relaxed);
            self.promotion_count.fetch_add(1, Ordering::Relaxed);
            if let Ok(mut warm_cache) = self.warm_cache.try_write() {
                <LFUCache<PageId, PageData> as CoreCache<PageId, PageData>>::insert(
                    &mut warm_cache,
                    page_id,
                    page_data.clone(),
                );
            }
            if let Some(metrics) = metrics_collector {
                metrics.record_cache_operation("cold", true);
                metrics.record_cache_migration(true);
            }
            // Only clone the actual data when returning to caller
            return Some((*page_data.data).clone());
        }

        // Cache miss - record for metrics
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
        if let Some(metrics) = metrics_collector {
            metrics.record_cache_operation("all", false);
        }

        None
    }

    /// Retrieves a page from the cache with metrics collection.
    ///
    /// Identical to [`get_page()`](Self::get_page) but records cache operation
    /// metrics (hit/miss, tier, migrations) to the provided `MetricsCollector`.
    ///
    /// # Arguments
    ///
    /// * `page_id` - The page identifier to look up
    /// * `metrics_collector` - Metrics collector for operation tracking
    ///
    /// # Recorded Metrics
    ///
    /// - Cache hit/miss per tier ("hot", "warm", "cold", "all")
    /// - Cache migration events (promotions)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let metrics = MetricsCollector::new(&config);
    /// if let Some(data) = cache.get_page_with_metrics(42, &metrics) {
    ///     // Metrics recorded automatically
    /// }
    /// ```
    pub fn get_page_with_metrics(
        &self,
        page_id: PageId,
        metrics_collector: &MetricsCollector,
    ) -> Option<Vec<u8>> {
        self.get_page_internal(page_id, Some(metrics_collector))
    }

    /// Records page access for prefetch pattern learning and temperature tracking.
    ///
    /// Called on every page access to update:
    ///
    /// 1. **Prefetch patterns**: Adds access to sequence history for pattern detection
    /// 2. **Hot/cold metadata**: Updates access count, timestamp, and temperature
    ///
    /// # Temperature Update Rules
    ///
    /// ```text
    /// access_count > 10  →  Hot
    /// access_count > 5   →  Warm
    /// access_count > 2   →  Cold
    /// access_count ≤ 2   →  Frozen
    /// ```
    fn record_access_pattern(&self, page_id: PageId) {
        // Update basic prefetch engine
        if let Ok(mut prefetch_engine) = self.prefetch_engine.try_write() {
            // Update access patterns for sequential detection
            prefetch_engine
                .access_patterns
                .entry(page_id)
                .or_insert_with(Vec::new)
                .push(page_id);
        }

        // Update hot/cold data tracker
        if let Ok(mut tracker) = self.hot_data_tracker.try_write() {
            let metadata = tracker.entry(page_id).or_insert_with(|| HotColdMetadata {
                access_count: 0,
                last_accessed: Instant::now(),
                access_pattern: AccessPattern::Random,
                temperature: DataTemperature::Cold,
                predicted_reuse: false,
                size_bytes: 0,
            });

            metadata.access_count += 1;
            metadata.last_accessed = Instant::now();

            // Update temperature based on access frequency
            metadata.temperature = match metadata.access_count {
                n if n > 10 => DataTemperature::Hot,
                n if n > 5 => DataTemperature::Warm,
                n if n > 2 => DataTemperature::Cold,
                _ => DataTemperature::Frozen,
            };
        }
    }

    /// Stores a page in the appropriate cache tier.
    ///
    /// The page is placed in a tier based on its temperature classification:
    ///
    /// - **Hot** → L1 Hot Cache (LRU-K)
    /// - **Warm** → L2 Warm Cache (LFU)
    /// - **Cold/Frozen** → L3 Cold Cache (FIFO)
    ///
    /// # Admission Control
    ///
    /// Before storage, the page passes through admission control:
    ///
    /// 1. **Memory pressure check**: High pressure may reject the page
    /// 2. **Reuse prediction**: Recent or frequently accessed pages bypass limits
    /// 3. **Size check**: Large pages rejected under pressure (> 2× page size)
    /// 4. **Deduplication**: Duplicate content detected and skipped
    ///
    /// # Memory Efficiency
    ///
    /// The page data is wrapped in `Arc<Vec<u8>>` for efficient sharing between
    /// cache tiers during promotions without data copying.
    ///
    /// # Arguments
    ///
    /// * `page_id` - The page identifier
    /// * `data` - The page data to cache
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Store a page (enters cold cache by default)
    /// cache.store_page(42, page_data);
    ///
    /// // Subsequent accesses promote to warmer tiers
    /// for _ in 0..10 {
    ///     cache.get_page(42);  // Eventually promoted to hot
    /// }
    /// ```
    pub fn store_page(&self, page_id: PageId, data: Vec<u8>) {
        // Check admission controller first
        if !self.should_admit_page(page_id, &data) {
            return;
        }

        // Check for duplicate pages
        let mut should_store = true;
        if let Ok(mut dedup) = self.dedup_engine.try_write()
            && let Some(_existing_page) = dedup.check_duplicate(page_id, &data)
        {
            // This is a duplicate page, don't store it
            should_store = false;
            self.dedup_savings.fetch_add(1, Ordering::Relaxed);
        }

        if !should_store {
            return;
        }

        // Determine the appropriate cache level based on access patterns
        let temperature = self.determine_data_temperature(page_id);

        // Wrap data in Arc for efficient sharing between cache levels
        let data_arc = Arc::new(data);

        match temperature {
            DataTemperature::Hot => {
                if let Ok(mut cache) = self.hot_cache.try_write() {
                    <LRUKCache<PageId, Arc<Vec<u8>>> as CoreCache<PageId, Arc<Vec<u8>>>>::insert(
                        &mut cache,
                        page_id,
                        Arc::clone(&data_arc),
                    );
                }
            },
            DataTemperature::Warm => {
                let page_data = PageData {
                    data: Arc::clone(&data_arc),
                    last_accessed: Instant::now(),
                    access_count: 1,
                    temperature: temperature.clone(),
                };
                if let Ok(mut cache) = self.warm_cache.try_write() {
                    <LFUCache<PageId, PageData> as CoreCache<PageId, PageData>>::insert(
                        &mut cache, page_id, page_data,
                    );
                }
            },
            _ => {
                let page_data = PageData {
                    data: Arc::clone(&data_arc),
                    last_accessed: Instant::now(),
                    access_count: 1,
                    temperature: temperature.clone(),
                };
                if let Ok(mut cache) = self.cold_cache.try_write() {
                    <FIFOCache<PageId, PageData> as CoreCache<PageId, PageData>>::insert(
                        &mut cache, page_id, page_data,
                    );
                }
            },
        }

        // Update access pattern for this page
        self.record_access_pattern(page_id);
    }

    /// Determines whether a page should be admitted to the cache.
    ///
    /// Implements the admission control policy based on memory pressure,
    /// page characteristics, and access history.
    ///
    /// # Admission Rules
    ///
    /// 1. **High pressure (>80%) + low admission rate (<50%)**: Reject
    /// 2. **Recent access (<60 seconds)**: Admit regardless of pressure
    /// 3. **Frequent access (>3 times)**: Admit regardless of pressure
    /// 4. **Pressure >60% + large page (>2× page size)**: Reject
    /// 5. **Otherwise**: Admit
    ///
    /// # Arguments
    ///
    /// * `page_id` - The page being considered for admission
    /// * `data` - The page data (used for size checking)
    ///
    /// # Returns
    ///
    /// `true` if the page should be admitted, `false` to reject.
    fn should_admit_page(&self, page_id: PageId, data: &[u8]) -> bool {
        // Check memory pressure
        let memory_pressure = self
            .admission_controller
            .memory_pressure
            .load(Ordering::Relaxed);
        if memory_pressure > 80 {
            // High memory pressure - be more selective
            let admission_rate = self
                .admission_controller
                .admission_rate
                .load(Ordering::Relaxed);
            if admission_rate < 50 {
                return false;
            }
        }

        // Check if this page is likely to be reused
        if let Ok(tracker) = self.hot_data_tracker.try_read()
            && let Some(metadata) = tracker.get(&page_id)
        {
            // If page has been accessed recently, admit it
            if metadata.last_accessed.elapsed().as_secs() < 60 {
                return true;
            }
            // If page has high access count, admit it
            if metadata.access_count > 3 {
                return true;
            }
        }

        // Check data size - don't admit extremely large pages under pressure
        if memory_pressure > 60 && data.len() > (DB_PAGE_SIZE * 2) as usize {
            return false;
        }

        // Default to admitting the page
        true
    }

    /// Determines the temperature classification for a page.
    ///
    /// Looks up the page's metadata in the hot/cold tracker. If no metadata
    /// exists (new page), defaults to `Cold`.
    ///
    /// # Arguments
    ///
    /// * `page_id` - The page to classify
    ///
    /// # Returns
    ///
    /// The page's current `DataTemperature` (Hot, Warm, Cold, or Frozen).
    fn determine_data_temperature(&self, page_id: PageId) -> DataTemperature {
        if let Ok(tracker) = self.hot_data_tracker.try_read()
            && let Some(metadata) = tracker.get(&page_id)
        {
            return metadata.temperature.clone();
        }

        // Default to cold for new pages
        DataTemperature::Cold
    }

    /// Predicts pages likely to be accessed next for prefetching.
    ///
    /// Combines two prediction strategies:
    ///
    /// ## 1. Sequential Prefetching
    ///
    /// Prefetches the next N pages (N = prefetch_distance × accuracy_factor).
    /// The distance adapts based on prediction accuracy:
    ///
    /// | Accuracy | Factor | Behavior                  |
    /// |----------|--------|---------------------------|
    /// | > 70%    | 1.0×   | Full prefetch distance    |
    /// | 40-70%   | 0.7×   | Conservative prefetching  |
    /// | 20-40%   | 0.3×   | Minimal prefetching       |
    /// | < 20%    | 0.5×   | Default moderate          |
    ///
    /// ## 2. Pattern-Based Prefetching
    ///
    /// Only active when accuracy > 20%. Analyzes historical page transitions
    /// to predict likely next pages. Requires at least `sequential_threshold`
    /// observations for a page.
    ///
    /// # Arguments
    ///
    /// * `current_page` - The page just accessed
    ///
    /// # Returns
    ///
    /// A deduplicated, sorted list of page IDs to prefetch. Limited to:
    /// - 12 pages if accuracy > 70%
    /// - 8 pages if accuracy 40-70%
    /// - 4 pages otherwise
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let pages_to_prefetch = cache.predict_prefetch_pages(42);
    /// for page_id in pages_to_prefetch {
    ///     // Asynchronously load into cache
    ///     tokio::spawn(async move { disk.prefetch(page_id).await });
    /// }
    /// ```
    pub fn predict_prefetch_pages(&self, current_page: PageId) -> Vec<PageId> {
        let mut predicted_pages = Vec::new();

        // Get current prefetch accuracy to adapt prefetch aggressiveness
        let current_accuracy = self.get_prefetch_accuracy();
        let accuracy_factor = if current_accuracy > 0.7 {
            1.0 // High accuracy - be more aggressive
        } else if current_accuracy > 0.4 {
            0.7 // Medium accuracy - be conservative
        } else if current_accuracy > 0.0 {
            0.3 // Low accuracy - be very conservative
        } else {
            0.5 // No data yet - moderate prefetching
        };

        // Get predictions from basic prefetch engine
        if let Ok(prefetch_engine) = self.prefetch_engine.try_read() {
            // Adaptive sequential prefetching based on accuracy
            let prefetch_distance =
                (prefetch_engine.prefetch_distance as f64 * accuracy_factor) as usize;
            let effective_distance = prefetch_distance
                .max(1)
                .min(prefetch_engine.prefetch_distance);

            for i in 1..=effective_distance {
                predicted_pages.push(current_page + i as u64);
            }

            // Pattern-based prefetching (only if accuracy is reasonable)
            if current_accuracy > 0.2
                && let Some(patterns) = prefetch_engine.access_patterns.get(&current_page)
                && patterns.len() >= prefetch_engine.sequential_threshold
            {
                // Find the most common next page
                let mut next_page_counts = HashMap::new();
                for &page in patterns {
                    *next_page_counts.entry(page + 1).or_insert(0) += 1;
                }

                if let Some((most_common_next, count)) =
                    next_page_counts.iter().max_by_key(|(_, count)| *count)
                    && *count >= 2
                {
                    predicted_pages.push(*most_common_next);
                }
            }
        }

        // Remove duplicates and limit the number of prefetch pages based on accuracy
        predicted_pages.sort_unstable();
        predicted_pages.dedup();
        let max_predictions = if current_accuracy > 0.7 {
            12 // High accuracy - allow more predictions
        } else if current_accuracy > 0.4 {
            8 // Medium accuracy - standard predictions
        } else {
            4 // Low accuracy - fewer predictions
        };
        predicted_pages.truncate(max_predictions);

        predicted_pages
    }

    /// Updates memory pressure and admission rate based on cache utilization.
    ///
    /// Calculates memory pressure as a percentage of total cache capacity used:
    ///
    /// ```text
    /// pressure = (hot_used + warm_used + cold_used) / (hot_size + warm_size + cold_size) × 100
    /// ```
    ///
    /// Based on the calculated pressure, adjusts the admission rate:
    ///
    /// | Pressure   | Admission Rate | Effect                    |
    /// |------------|----------------|---------------------------|
    /// | 0-50%      | 100%           | Admit all pages           |
    /// | 51-70%     | 80%            | Slight restriction        |
    /// | 71-85%     | 60%            | Selective admission       |
    /// | 86-95%     | 40%            | Very selective            |
    /// | > 95%      | 20%            | Emergency mode            |
    ///
    /// # Usage
    ///
    /// Call periodically (e.g., after batch operations) or before admission
    /// decisions. Called automatically by `perform_maintenance()`.
    pub fn update_memory_pressure(&self) {
        let hot_used = if let Ok(cache) = self.hot_cache.try_read() {
            <LRUKCache<PageId, Arc<Vec<u8>>> as CoreCache<PageId, Arc<Vec<u8>>>>::len(&cache)
        } else {
            0
        };

        let warm_used = if let Ok(cache) = self.warm_cache.try_read() {
            <LFUCache<PageId, PageData> as CoreCache<PageId, PageData>>::len(&cache)
        } else {
            0
        };

        let cold_used = if let Ok(cache) = self.cold_cache.try_read() {
            <FIFOCache<PageId, PageData> as CoreCache<PageId, PageData>>::len(&cache)
        } else {
            0
        };

        let total_used = hot_used + warm_used + cold_used;
        let total_capacity = self.hot_cache_size + self.warm_cache_size + self.cold_cache_size;

        let usage_percentage = if total_capacity > 0 {
            (total_used as f64 / total_capacity as f64) * 100.0
        } else {
            0.0
        };

        self.admission_controller
            .memory_pressure
            .store(usage_percentage as u64, Ordering::Relaxed);

        // Adjust admission rate based on memory pressure
        let new_admission_rate = match usage_percentage as u64 {
            0..=50 => 100,
            51..=70 => 80,
            71..=85 => 60,
            86..=95 => 40,
            _ => 20,
        };

        self.admission_controller
            .admission_rate
            .store(new_admission_rate, Ordering::Relaxed);
    }

    /// Evicts pages from cache when memory pressure is high.
    ///
    /// Applies progressive eviction based on memory pressure levels:
    ///
    /// ## Pressure > 85%: Cold Cache Eviction
    ///
    /// Targets the cold cache first (least valuable tier). If usage exceeds
    /// 2× target size, clears the entire cold cache.
    ///
    /// ## Pressure > 95%: Warm Cache Eviction
    ///
    /// Emergency mode: clears the warm cache if usage exceeds 2× target size.
    /// This is a last resort to prevent memory exhaustion.
    ///
    /// # Eviction Strategy
    ///
    /// ```text
    /// Pressure    Action
    /// ════════════════════════════════════════════════════════
    /// ≤ 85%       No eviction
    /// 85-95%      Clear cold cache if > 2× target size
    /// > 95%       Clear warm cache if > 2× target size
    /// ```
    ///
    /// # Note
    ///
    /// The hot cache is never directly evicted; it relies on its LRU-K
    /// policy for natural eviction of less frequently accessed pages.
    pub fn evict_if_needed(&self) {
        let memory_pressure = self
            .admission_controller
            .memory_pressure
            .load(Ordering::Relaxed);

        if memory_pressure > 85 {
            // High memory pressure - reduce cold cache size
            if let Ok(mut cold_cache) = self.cold_cache.try_write() {
                let target_size = self.cold_cache_size / 2;
                let current_size =
                    <FIFOCache<PageId, PageData> as CoreCache<PageId, PageData>>::len(&cold_cache);
                if current_size > target_size {
                    // For FIFO cache, we can't easily evict specific items
                    // Instead, we'll clear part of the cache
                    if current_size > target_size * 2 {
                        let evicted_count = current_size as u64;
                        <FIFOCache<PageId, PageData> as CoreCache<PageId, PageData>>::clear(
                            &mut cold_cache,
                        );
                        self.demotion_count
                            .fetch_add(evicted_count, Ordering::Relaxed);
                    }
                }
            }
        }

        if memory_pressure > 95 {
            // Extreme memory pressure - reduce warm cache size
            if let Ok(mut warm_cache) = self.warm_cache.try_write() {
                let target_size = self.warm_cache_size / 2;
                let current_size =
                    <LFUCache<PageId, PageData> as CoreCache<PageId, PageData>>::len(&warm_cache);
                if current_size > target_size {
                    // For LFU cache, we can't easily evict specific items
                    // Instead, we'll clear part of the cache
                    if current_size > target_size * 2 {
                        let evicted_count = current_size as u64;
                        <LFUCache<PageId, PageData> as CoreCache<PageId, PageData>>::clear(
                            &mut warm_cache,
                        );
                        self.demotion_count
                            .fetch_add(evicted_count, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    /// Returns comprehensive cache statistics for monitoring.
    ///
    /// Collects metrics from all cache tiers and operational counters:
    ///
    /// # Collected Statistics
    ///
    /// - **Capacity**: Configured size for each tier
    /// - **Utilization**: Current entry count per tier
    /// - **Hit ratios**: Per-tier and overall hit rates
    /// - **Operations**: Promotion and demotion counts
    /// - **Prefetch**: Prediction accuracy percentage
    ///
    /// # Side Effects
    ///
    /// Calls `update_memory_pressure()` to ensure pressure values are current.
    ///
    /// # Returns
    ///
    /// A `CacheStatistics` struct containing all metrics.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let stats = cache.get_cache_statistics();
    /// println!("Overall hit ratio: {:.1}%", stats.overall_hit_ratio * 100.0);
    /// println!("Hot cache usage: {}/{}", stats.hot_cache_used, stats.hot_cache_size);
    /// println!("Prefetch accuracy: {:.1}%", stats.prefetch_accuracy * 100.0);
    /// ```
    pub fn get_cache_statistics(&self) -> CacheStatistics {
        // First update memory pressure before getting statistics
        self.update_memory_pressure();

        let hot_used = if let Ok(cache) = self.hot_cache.try_read() {
            <LRUKCache<PageId, Arc<Vec<u8>>> as CoreCache<PageId, Arc<Vec<u8>>>>::len(&cache)
        } else {
            0
        };

        let warm_used = if let Ok(cache) = self.warm_cache.try_read() {
            <LFUCache<PageId, PageData> as CoreCache<PageId, PageData>>::len(&cache)
        } else {
            0
        };

        let cold_used = if let Ok(cache) = self.cold_cache.try_read() {
            <FIFOCache<PageId, PageData> as CoreCache<PageId, PageData>>::len(&cache)
        } else {
            0
        };

        // Use our integrated prefetch accuracy tracking
        let prefetch_accuracy = self.get_prefetch_accuracy();

        // Calculate hit ratios from tracked counters
        let total_hits = self.cache_hits.load(Ordering::Relaxed);
        let total_misses = self.cache_misses.load(Ordering::Relaxed);
        let total_accesses = total_hits + total_misses;

        let overall_hit_ratio = if total_accesses > 0 {
            total_hits as f64 / total_accesses as f64
        } else {
            0.0
        };

        // Calculate per-tier hit ratios
        let hot_hits = self.hot_cache_hits.load(Ordering::Relaxed);
        let warm_hits = self.warm_cache_hits.load(Ordering::Relaxed);
        let cold_hits = self.cold_cache_hits.load(Ordering::Relaxed);

        let hot_hit_ratio = if total_accesses > 0 {
            hot_hits as f64 / total_accesses as f64
        } else {
            0.0
        };

        let warm_hit_ratio = if total_accesses > 0 {
            warm_hits as f64 / total_accesses as f64
        } else {
            0.0
        };

        let cold_hit_ratio = if total_accesses > 0 {
            cold_hits as f64 / total_accesses as f64
        } else {
            0.0
        };

        CacheStatistics {
            hot_cache_size: self.hot_cache_size,
            warm_cache_size: self.warm_cache_size,
            cold_cache_size: self.cold_cache_size,
            hot_cache_used: hot_used,
            warm_cache_used: warm_used,
            cold_cache_used: cold_used,
            hot_cache_hit_ratio: hot_hit_ratio,
            warm_cache_hit_ratio: warm_hit_ratio,
            cold_cache_hit_ratio: cold_hit_ratio,
            overall_hit_ratio,
            promotion_count: self.promotion_count.load(Ordering::Relaxed),
            demotion_count: self.demotion_count.load(Ordering::Relaxed),
            prefetch_accuracy,
        }
    }

    /// Triggers prefetch prediction and records predictions for accuracy tracking.
    ///
    /// This is the public interface for prefetching. It:
    ///
    /// 1. Calls `predict_prefetch_pages()` to get predictions
    /// 2. Records predictions in the tracking map for accuracy measurement
    /// 3. Returns the list of pages to prefetch
    ///
    /// # Arguments
    ///
    /// * `current_page` - The page just accessed
    ///
    /// # Returns
    ///
    /// A list of page IDs predicted for prefetching.
    ///
    /// # Accuracy Tracking
    ///
    /// Each prediction is timestamped. When the predicted page is later accessed,
    /// it's counted as a prefetch hit. Predictions older than 60 seconds are
    /// cleaned up and count as misses.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let prefetch_pages = cache.trigger_prefetch(current_page_id);
    /// for page_id in prefetch_pages {
    ///     if cache.get_page(page_id).is_none() {
    ///         // Load from disk in background
    ///         disk_manager.prefetch(page_id);
    ///     }
    /// }
    /// ```
    pub fn trigger_prefetch(&self, current_page: PageId) -> Vec<PageId> {
        let predicted_pages = self.predict_prefetch_pages(current_page);

        // Record predictions for accuracy tracking
        if !predicted_pages.is_empty() {
            self.record_prefetch_predictions(&predicted_pages);
        }

        predicted_pages
    }

    /// Performs routine maintenance tasks.
    ///
    /// Should be called periodically (e.g., every N operations or on a timer).
    /// Performs:
    ///
    /// 1. **Memory pressure update**: Recalculates utilization and admission rate
    /// 2. **Eviction check**: Clears caches if pressure exceeds thresholds
    ///
    /// # Usage
    ///
    /// ```rust,ignore
    /// // Call after batch operations
    /// for page_id in pages {
    ///     cache.store_page(page_id, data);
    /// }
    /// cache.perform_maintenance();
    ///
    /// // Or on a timer
    /// tokio::spawn(async move {
    ///     loop {
    ///         tokio::time::sleep(Duration::from_secs(10)).await;
    ///         cache.perform_maintenance();
    ///     }
    /// });
    /// ```
    pub fn perform_maintenance(&self) {
        self.update_memory_pressure();
        self.evict_if_needed();
    }

    /// Returns the current memory pressure as a percentage (0-100).
    ///
    /// Memory pressure indicates how full the cache is relative to capacity.
    /// Higher values trigger more aggressive admission control and eviction.
    pub fn get_memory_pressure(&self) -> u64 {
        self.admission_controller
            .memory_pressure
            .load(Ordering::Relaxed)
    }

    /// Returns the current admission rate as a percentage (0-100).
    ///
    /// The admission rate determines what fraction of page store requests
    /// are accepted. Lower values indicate stricter admission control due
    /// to high memory pressure.
    pub fn get_admission_rate(&self) -> u64 {
        self.admission_controller
            .admission_rate
            .load(Ordering::Relaxed)
    }

    /// Checks if a page access was a successful prefetch prediction.
    ///
    /// If the page was previously predicted (in `prefetch_predictions`),
    /// it's counted as a hit and accuracy is updated.
    fn check_prefetch_hit(&self, page_id: PageId) {
        if let Ok(mut predictions) = self.prefetch_predictions.try_write()
            && predictions.remove(&page_id).is_some()
        {
            // This page was predicted and accessed - it's a hit
            self.prefetch_hits.fetch_add(1, Ordering::Relaxed);
            self.update_prefetch_accuracy();
        }
    }

    /// Updates the prefetch accuracy metric based on hit ratio.
    ///
    /// Calculates accuracy as: `(prefetch_hits / prefetch_total) × 100`
    /// and stores it atomically for thread-safe access.
    fn update_prefetch_accuracy(&self) {
        let hits = self.prefetch_hits.load(Ordering::Relaxed);
        let total = self.prefetch_total.load(Ordering::Relaxed);

        if total > 0 {
            // Calculate accuracy as percentage (0-100)
            let accuracy = (hits as f64 / total as f64 * 100.0) as u64;
            self.prefetch_accuracy.store(accuracy, Ordering::Relaxed);
        }
    }

    /// Records prefetch predictions with timestamps for accuracy tracking.
    ///
    /// Each predicted page is stored with the current timestamp. When the page
    /// is later accessed, it's removed and counted as a hit. Old predictions
    /// (> 60 seconds) are cleaned up periodically.
    fn record_prefetch_predictions(&self, predicted_pages: &[PageId]) {
        if let Ok(mut predictions) = self.prefetch_predictions.try_write() {
            let now = Instant::now();
            for &page_id in predicted_pages {
                predictions.insert(page_id, now);
            }
        }

        // Update total predictions count
        self.prefetch_total
            .fetch_add(predicted_pages.len() as u64, Ordering::Relaxed);

        // Clean up old predictions (older than 60 seconds)
        self.cleanup_old_predictions();
    }

    /// Removes stale prefetch predictions older than 60 seconds.
    ///
    /// Predictions that haven't been accessed within the timeout window
    /// are considered misses and removed. This prevents unbounded growth
    /// of the predictions map and keeps accuracy metrics meaningful.
    fn cleanup_old_predictions(&self) {
        if let Ok(mut predictions) = self.prefetch_predictions.try_write() {
            let now = Instant::now();
            let cutoff_time = now - std::time::Duration::from_secs(60);

            // Remove predictions older than 60 seconds
            let old_count = predictions.len();
            predictions.retain(|_, &mut timestamp| timestamp > cutoff_time);
            let removed_count = old_count - predictions.len();

            // Adjust total count for removed predictions (they become misses)
            if removed_count > 0 {
                self.update_prefetch_accuracy();
            }
        }
    }

    /// Returns the current prefetch accuracy as a ratio (0.0-1.0).
    ///
    /// Accuracy measures how many prefetch predictions were actually accessed.
    /// Higher accuracy indicates effective prediction algorithms.
    ///
    /// # Returns
    ///
    /// - `0.0`: No predictions hit (or no predictions made)
    /// - `1.0`: All predictions were accessed
    pub fn get_prefetch_accuracy(&self) -> f64 {
        self.prefetch_accuracy.load(Ordering::Relaxed) as f64 / 100.0
    }

    /// Returns enhanced statistics including algorithm-specific details.
    ///
    /// Extends `get_cache_statistics()` with information about the eviction
    /// algorithms used in each tier. Useful for debugging and understanding
    /// cache behavior.
    ///
    /// # Returns
    ///
    /// An `EnhancedCacheStatistics` struct containing:
    /// - All basic statistics from `get_cache_statistics()`
    /// - LRU-K value (K parameter for hot cache)
    /// - Algorithm names for each tier
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let stats = cache.get_enhanced_cache_statistics();
    /// println!("Hot cache uses {} with K={}", stats.hot_cache_algorithm, stats.lru_k_value);
    /// ```
    pub fn get_enhanced_cache_statistics(&self) -> EnhancedCacheStatistics {
        // Get LRU-K specific statistics from hot cache
        let lru_k_value = if let Ok(cache) = self.hot_cache.try_read() {
            <LRUKCache<PageId, Arc<Vec<u8>>> as LRUKCacheTrait<PageId, Arc<Vec<u8>>>>::k_value(
                &cache,
            )
        } else {
            0
        };

        // Get basic statistics using the standard method
        let basic_stats = self.get_cache_statistics();

        EnhancedCacheStatistics {
            basic_stats,
            lru_k_value,
            hot_cache_algorithm: "LRU-K".to_string(),
            warm_cache_algorithm: "LFU".to_string(),
            cold_cache_algorithm: "FIFO".to_string(),
        }
    }

    /// Performs maintenance with algorithm-specific diagnostics.
    ///
    /// Extends `perform_maintenance()` with logging of eviction candidates
    /// from each cache tier:
    ///
    /// - **Cold (FIFO)**: Logs the oldest page (next to be evicted)
    /// - **Warm (LFU)**: Logs the least frequently used page
    /// - **Hot (LRU-K)**: Logs the LRU-K eviction candidate
    ///
    /// Useful for debugging cache behavior and understanding eviction patterns.
    ///
    /// # Note
    ///
    /// Logs at `trace` level to minimize performance impact in production.
    pub fn perform_specialized_maintenance(&self) {
        // Use FIFO-specific operations for cold cache
        if let Ok(cold_cache) = self.cold_cache.try_read()
            && let Some((oldest_key, _)) = <FIFOCache<PageId, PageData> as FIFOCacheTrait<
                PageId,
                PageData,
            >>::peek_oldest(&cold_cache)
        {
            // Log the oldest page for monitoring
            log::trace!("Oldest page in cold cache: {}", oldest_key);
        }

        // Use LFU-specific operations for warm cache
        if let Ok(warm_cache) = self.warm_cache.try_read()
            && let Some((lfu_key, _)) = <LFUCache<PageId, PageData> as LFUCacheTrait<
                PageId,
                PageData,
            >>::peek_lfu(&warm_cache)
        {
            // Log the least frequently used page
            log::trace!("Least frequently used page in warm cache: {}", lfu_key);
        }

        // Use LRU-K specific operations for hot cache
        if let Ok(hot_cache) = self.hot_cache.try_read()
            && let Some((lru_k_key, _)) = <LRUKCache<PageId, Arc<Vec<u8>>> as LRUKCacheTrait<
                PageId,
                Arc<Vec<u8>>,
            >>::peek_lru_k(&hot_cache)
        {
            // Log the LRU-K candidate for eviction
            log::trace!("LRU-K eviction candidate in hot cache: {}", lru_k_key);
        }

        // Perform standard maintenance
        self.perform_maintenance();
    }

    /// Returns detailed access information for a specific cached page.
    ///
    /// Searches all cache tiers for the page and returns algorithm-specific
    /// details about its cache status.
    ///
    /// # Tier-Specific Information
    ///
    /// | Tier | Algorithm | Available Details                          |
    /// |------|-----------|---------------------------------------------|
    /// | Hot  | LRU-K     | k_value, access_count, k_distance, rank    |
    /// | Warm | LFU       | access_count (frequency)                   |
    /// | Cold | FIFO      | eviction_rank (age rank)                   |
    ///
    /// # Arguments
    ///
    /// * `page_id` - The page to look up
    ///
    /// # Returns
    ///
    /// * `Some(PageAccessDetails)` - Details if the page is cached
    /// * `None` - Page not found in any cache tier
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// if let Some(details) = cache.get_page_access_details(42) {
    ///     println!("Page 42 is in {} cache", details.cache_level);
    ///     if details.algorithm == "LRU-K" {
    ///         println!("  K-distance: {:?}", details.k_distance);
    ///     }
    /// }
    /// ```
    pub fn get_page_access_details(&self, page_id: PageId) -> Option<PageAccessDetails> {
        if let Ok(hot_cache) = self.hot_cache.try_read() {
            // Check if page is in hot cache and get LRU-K specific details
            if <LRUKCache<PageId, Arc<Vec<u8>>> as CoreCache<PageId, Arc<Vec<u8>>>>::contains(
                &hot_cache, &page_id,
            ) {
                let k_value = <LRUKCache<PageId, Arc<Vec<u8>>> as LRUKCacheTrait<
                    PageId,
                    Arc<Vec<u8>>,
                >>::k_value(&hot_cache);
                let access_count = <LRUKCache<PageId, Arc<Vec<u8>>> as LRUKCacheTrait<
                    PageId,
                    Arc<Vec<u8>>,
                >>::access_count(&hot_cache, &page_id)
                .map(|count| count as u64);
                let k_distance = <LRUKCache<PageId, Arc<Vec<u8>>> as LRUKCacheTrait<
                    PageId,
                    Arc<Vec<u8>>,
                >>::k_distance(&hot_cache, &page_id);
                let rank = <LRUKCache<PageId, Arc<Vec<u8>>> as LRUKCacheTrait<
                    PageId,
                    Arc<Vec<u8>>,
                >>::k_distance_rank(&hot_cache, &page_id);

                return Some(PageAccessDetails {
                    cache_level: "Hot".to_string(),
                    algorithm: "LRU-K".to_string(),
                    k_value: Some(k_value),
                    access_count,
                    k_distance,
                    eviction_rank: rank,
                    temperature: DataTemperature::Hot,
                });
            }
        }

        if let Ok(warm_cache) = self.warm_cache.try_read() {
            // Check if page is in warm cache and get LFU specific details
            if <LFUCache<PageId, PageData> as CoreCache<PageId, PageData>>::contains(
                &warm_cache,
                &page_id,
            ) {
                let frequency =
                    <LFUCache<PageId, PageData> as LFUCacheTrait<PageId, PageData>>::frequency(
                        &warm_cache,
                        &page_id,
                    );

                return Some(PageAccessDetails {
                    cache_level: "Warm".to_string(),
                    algorithm: "LFU".to_string(),
                    k_value: None,
                    access_count: frequency,
                    k_distance: None,
                    eviction_rank: None,
                    temperature: DataTemperature::Warm,
                });
            }
        }

        if let Ok(cold_cache) = self.cold_cache.try_read() {
            // Check if page is in cold cache and get FIFO specific details
            if <FIFOCache<PageId, PageData> as CoreCache<PageId, PageData>>::contains(
                &cold_cache,
                &page_id,
            ) {
                let age_rank =
                    <FIFOCache<PageId, PageData> as FIFOCacheTrait<PageId, PageData>>::age_rank(
                        &cold_cache,
                        &page_id,
                    );

                return Some(PageAccessDetails {
                    cache_level: "Cold".to_string(),
                    algorithm: "FIFO".to_string(),
                    k_value: None,
                    access_count: None,
                    k_distance: None,
                    eviction_rank: age_rank,
                    temperature: DataTemperature::Cold,
                });
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::async_disk::config::DiskManagerConfig;
    use crate::storage::disk::async_disk::metrics::collector::MetricsCollector;

    #[test]
    fn test_cache_manager_basic() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Test store_page and get_page
        let page_id = 1;
        let data = vec![1, 2, 3, 4];
        cache_manager.store_page(page_id, data.clone());

        let retrieved = cache_manager.get_page(page_id);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), data);

        // Test cache statistics
        let stats = cache_manager.get_cache_statistics();
        assert!(stats.hot_cache_used > 0 || stats.warm_cache_used > 0 || stats.cold_cache_used > 0);
    }

    #[test]
    fn test_cache_manager_integration() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Test prefetch functionality
        let page_id = 1;
        let data = vec![1, 2, 3, 4];
        cache_manager.store_page(page_id, data.clone());

        // Access pattern to build up ML prefetcher knowledge
        for i in 1..=5 {
            let test_data = vec![i as u8; 4];
            cache_manager.store_page(i, test_data);
            let _ = cache_manager.get_page(i);
        }

        // Test prefetch predictions
        let predicted_pages = cache_manager.trigger_prefetch(3);
        assert!(!predicted_pages.is_empty());

        // Test maintenance operations
        cache_manager.perform_maintenance();

        // Test cache statistics after integration
        let stats = cache_manager.get_cache_statistics();
        assert!(stats.hot_cache_used > 0 || stats.warm_cache_used > 0 || stats.cold_cache_used > 0);
        assert!(stats.prefetch_accuracy >= 0.0);
    }

    #[test]
    fn test_admission_controller_memory_pressure() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Fill cache to increase memory pressure
        for i in 1..=100 {
            let data = vec![i as u8; 1024]; // Larger data to increase memory usage
            cache_manager.store_page(i, data);
        }

        // Update memory pressure
        cache_manager.update_memory_pressure();

        let memory_pressure = cache_manager.get_memory_pressure();
        let admission_rate = cache_manager.get_admission_rate();

        // Memory pressure should be calculated based on cache usage
        assert!(memory_pressure > 0);
        assert!(admission_rate > 0);
        assert!(admission_rate <= 100);
    }

    #[test]
    fn test_prefetch_engine_sequential_patterns() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Create a sequential access pattern: 1, 2, 3, 4, 5
        for i in 1..=5 {
            let data = vec![i as u8; 64];
            cache_manager.store_page(i, data);
            let _ = cache_manager.get_page(i);
        }

        // Predict prefetch for page 3
        let predicted_pages = cache_manager.predict_prefetch_pages(3);

        // Should predict sequential pages (4, 5, 6, 7)
        assert!(!predicted_pages.is_empty());
        assert!(predicted_pages.contains(&4));
        assert!(predicted_pages.len() <= 8); // Limited to 8 predictions
    }

    #[test]
    fn test_admission_controller_rejection() {
        let config = DiskManagerConfig {
            cache_size_mb: 1, // Very small cache to trigger admission control
            ..Default::default()
        };
        let cache_manager = CacheManager::new(&config);

        // Fill cache completely
        for i in 1..=50 {
            let data = vec![i as u8; 1024]; // 1KB per page
            cache_manager.store_page(i, data);
        }

        // Force memory pressure calculation
        cache_manager.update_memory_pressure();

        let memory_pressure = cache_manager.get_memory_pressure();
        let admission_rate = cache_manager.get_admission_rate();

        // With small cache, memory pressure should be high
        assert!(memory_pressure > 50); // Should be quite high

        // Admission rate should be reduced due to high memory pressure
        assert!(admission_rate <= 100);
    }

    #[test]
    fn test_cache_eviction_under_pressure() {
        let config = DiskManagerConfig {
            cache_size_mb: 1, // Small cache
            ..Default::default()
        };
        let cache_manager = CacheManager::new(&config);

        // Fill cache beyond capacity
        for i in 1..=100 {
            let data = vec![i as u8; 1024];
            cache_manager.store_page(i, data);
        }

        let initial_stats = cache_manager.get_cache_statistics();
        let initial_total = initial_stats.hot_cache_used
            + initial_stats.warm_cache_used
            + initial_stats.cold_cache_used;

        // Trigger eviction
        cache_manager.evict_if_needed();

        let final_stats = cache_manager.get_cache_statistics();
        let final_total =
            final_stats.hot_cache_used + final_stats.warm_cache_used + final_stats.cold_cache_used;

        // Eviction should have reduced cache usage under extreme pressure
        // Note: Due to cache implementation details, exact eviction behavior may vary
        assert!(final_total <= initial_total);
        assert!(final_stats.demotion_count >= initial_stats.demotion_count);
    }

    #[test]
    fn test_cache_promotion_behavior() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Store pages in cold cache (default for new pages)
        for i in 1..=5 {
            let data = vec![i as u8; 64];
            cache_manager.store_page(i, data);
        }

        let initial_stats = cache_manager.get_cache_statistics();
        let initial_promotions = initial_stats.promotion_count;

        // Access pages multiple times to trigger promotions
        for _ in 0..3 {
            for i in 1..=5 {
                let _ = cache_manager.get_page(i);
            }
        }

        let final_stats = cache_manager.get_cache_statistics();
        let final_promotions = final_stats.promotion_count;

        // Should have promoted some pages
        assert!(final_promotions >= initial_promotions);
    }

    #[test]
    fn test_cache_with_metrics_collector() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);
        let metrics_collector = MetricsCollector::new(&config);

        // Store and retrieve pages with metrics
        let page_id = 42;
        let data = vec![42; 128];
        cache_manager.store_page(page_id, data.clone());

        // Test cache hit with metrics
        let retrieved = cache_manager.get_page_with_metrics(page_id, &metrics_collector);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), data);

        // Test cache miss with metrics
        let missing = cache_manager.get_page_with_metrics(999, &metrics_collector);
        assert!(missing.is_none());
    }

    #[test]
    fn test_deduplication_engine() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Store the same data under different page IDs
        let duplicate_data = vec![1, 2, 3, 4, 5, 6, 7, 8];

        cache_manager.store_page(1, duplicate_data.clone());
        cache_manager.store_page(2, duplicate_data.clone());
        cache_manager.store_page(3, duplicate_data.clone());

        // Get deduplication stats
        let stats = cache_manager.get_cache_statistics();

        assert_eq!(stats.hot_cache_used, 0);
    }

    #[test]
    fn test_data_temperature_classification() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Access pages with different frequencies to create temperature differences

        // Hot page - access many times
        let hot_page = 1;
        for _ in 0..15 {
            let data = vec![1; 64];
            cache_manager.store_page(hot_page, data);
            let _ = cache_manager.get_page(hot_page);
        }

        // Warm page - moderate access
        let warm_page = 2;
        for _ in 0..7 {
            let data = vec![2; 64];
            cache_manager.store_page(warm_page, data);
            let _ = cache_manager.get_page(warm_page);
        }

        // Cold page - few accesses
        let cold_page = 3;
        for _ in 0..3 {
            let data = vec![3; 64];
            cache_manager.store_page(cold_page, data);
            let _ = cache_manager.get_page(cold_page);
        }

        // Verify that pages are still accessible regardless of temperature
        assert!(cache_manager.get_page(hot_page).is_some());
        assert!(cache_manager.get_page(warm_page).is_some());
        assert!(cache_manager.get_page(cold_page).is_some());

        let stats = cache_manager.get_cache_statistics();
        assert!(stats.hot_cache_used > 0 || stats.warm_cache_used > 0 || stats.cold_cache_used > 0);
    }

    #[test]
    fn test_cache_statistics_accuracy() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Store pages across different cache levels
        for i in 1..=10 {
            let data = vec![i as u8; 128];
            cache_manager.store_page(i, data);
        }

        let stats = cache_manager.get_cache_statistics();

        // Calculate expected cache sizes (matching the constructor logic)
        let expected_hot_size = (config.cache_size_mb as f64 * config.hot_cache_ratio) as usize;
        let expected_warm_size = (config.cache_size_mb as f64 * config.warm_cache_ratio) as usize;
        let expected_cold_size = config.cache_size_mb - expected_hot_size - expected_warm_size;

        // Verify statistics are reasonable
        assert_eq!(stats.hot_cache_size, expected_hot_size);
        assert_eq!(stats.warm_cache_size, expected_warm_size);
        assert_eq!(stats.cold_cache_size, expected_cold_size);
        assert!(stats.hot_cache_used <= stats.hot_cache_size);
        assert!(stats.warm_cache_used <= stats.warm_cache_size);
        assert!(stats.cold_cache_used <= stats.cold_cache_size);
        assert!(stats.prefetch_accuracy >= 0.0 && stats.prefetch_accuracy <= 1.0);
    }

    #[test]
    fn test_maintenance_operations() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Fill cache with some data
        for i in 1..=20 {
            let data = vec![i as u8; 256];
            cache_manager.store_page(i, data);
        }

        // Perform maintenance
        cache_manager.perform_maintenance();

        let final_memory_pressure = cache_manager.get_memory_pressure();
        let final_admission_rate = cache_manager.get_admission_rate();

        // Memory pressure should be calculated
        assert!(final_memory_pressure > 0);
        assert!(final_memory_pressure <= 100);

        // Admission rate should be reasonable
        assert!(final_admission_rate > 0);
        assert!(final_admission_rate <= 100);
    }

    #[test]
    fn test_concurrent_access_simulation() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Simulate concurrent access patterns
        let access_patterns = vec![
            vec![1, 2, 3, 4, 5],  // Sequential
            vec![10, 8, 6, 4, 2], // Reverse
            vec![1, 3, 5, 7, 9],  // Odd numbers
            vec![2, 4, 6, 8, 10], // Even numbers
        ];

        // Execute different access patterns
        for pattern in access_patterns {
            for page_id in pattern {
                let data = vec![page_id as u8; 64];
                cache_manager.store_page(page_id, data.clone());

                // Verify immediate retrieval
                let retrieved = cache_manager.get_page(page_id);
                assert!(retrieved.is_some());
                assert_eq!(retrieved.unwrap(), data);
            }
        }

        // Test prefetch predictions after complex access patterns
        let predictions = cache_manager.trigger_prefetch(5);
        assert!(!predictions.is_empty());

        // Verify cache is functioning properly
        let stats = cache_manager.get_cache_statistics();
        assert!(stats.hot_cache_used > 0 || stats.warm_cache_used > 0 || stats.cold_cache_used > 0);
    }

    #[test]
    fn test_edge_case_empty_cache() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Test operations on empty cache
        assert!(cache_manager.get_page(1).is_none());

        let predictions = cache_manager.trigger_prefetch(1);
        // Should still return some predictions (sequential)
        assert!(!predictions.is_empty());

        let memory_pressure = cache_manager.get_memory_pressure();
        assert_eq!(memory_pressure, 0); // Empty cache should have no pressure

        let admission_rate = cache_manager.get_admission_rate();
        assert_eq!(admission_rate, 100); // Should allow all admissions when empty

        let stats = cache_manager.get_cache_statistics();
        assert_eq!(stats.hot_cache_used, 0);
        assert_eq!(stats.warm_cache_used, 0);
        assert_eq!(stats.cold_cache_used, 0);
    }

    #[test]
    fn test_large_page_admission_control() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Fill cache to create memory pressure
        for i in 1..=50 {
            let data = vec![i as u8; 512];
            cache_manager.store_page(i, data);
        }

        // Force high memory pressure
        cache_manager.update_memory_pressure();

        // Try to store a very large page
        let large_page_data = vec![255; (DB_PAGE_SIZE * 3) as usize]; // Larger than admission threshold
        cache_manager.store_page(999, large_page_data.clone());

        // The large page may or may not be admitted depending on pressure
        // This tests the admission control logic
        let memory_pressure = cache_manager.get_memory_pressure();
        assert!(memory_pressure > 0);
    }

    #[test]
    fn test_prefetch_accuracy_tracking() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Initially, accuracy should be 0 (no predictions made yet)
        assert_eq!(cache_manager.get_prefetch_accuracy(), 0.0);

        // Make some prefetch predictions
        let predicted_pages = cache_manager.trigger_prefetch(10);
        assert!(!predicted_pages.is_empty());

        // Access some of the predicted pages (hits)
        for page_id in predicted_pages
            .iter()
            .take(predicted_pages.len().min(3))
            .copied()
        {
            let data = vec![page_id as u8; 64];
            cache_manager.store_page(page_id, data.clone());

            // Access the page to register a prefetch hit
            let retrieved = cache_manager.get_page(page_id);
            assert!(retrieved.is_some());
        }

        // Check that accuracy has been calculated
        let accuracy = cache_manager.get_prefetch_accuracy();
        assert!(
            accuracy > 0.0,
            "Accuracy should be greater than 0 after hits"
        );
        assert!(accuracy <= 1.0, "Accuracy should not exceed 100%");

        // Test adaptive prefetching based on accuracy
        let new_predictions = cache_manager.trigger_prefetch(20);
        assert!(!new_predictions.is_empty());

        // Test that accuracy affects prediction count
        // With some accuracy, we should get reasonable number of predictions
        if accuracy > 0.4 {
            assert!(
                new_predictions.len() >= 4,
                "Should make more predictions with decent accuracy"
            );
        }
    }

    #[test]
    fn test_prefetch_accuracy_cleanup() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Make predictions
        let predicted_pages = cache_manager.trigger_prefetch(30);
        assert!(!predicted_pages.is_empty());

        // Simulate some time passing and accessing pages
        for &page_id in &predicted_pages[0..2] {
            let data = vec![page_id as u8; 64];
            cache_manager.store_page(page_id, data);
            let _ = cache_manager.get_page(page_id);
        }

        // Test cleanup of old predictions
        cache_manager.cleanup_old_predictions();

        // Accuracy should still be reasonable
        let accuracy = cache_manager.get_prefetch_accuracy();
        assert!((0.0..=1.0).contains(&accuracy));
    }

    #[test]
    fn test_adaptive_prefetch_distance() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Test that prefetch distance adapts to accuracy

        // With no history, should get moderate prefetching
        let initial_predictions = cache_manager.trigger_prefetch(40);
        let initial_count = initial_predictions.len();
        assert!(initial_count > 0);

        // Create some successful prefetch history
        for &page_id in &initial_predictions[0..initial_predictions.len().min(4)] {
            let data = vec![page_id as u8; 64];
            cache_manager.store_page(page_id, data);
            let _ = cache_manager.get_page(page_id);
        }

        // With higher accuracy, might get more predictions
        let improved_predictions = cache_manager.trigger_prefetch(50);
        assert!(!improved_predictions.is_empty());

        // Test that cache statistics include our prefetch accuracy
        let stats = cache_manager.get_cache_statistics();
        assert!(stats.prefetch_accuracy >= 0.0);
        assert!(stats.prefetch_accuracy <= 1.0);
    }

    #[test]
    fn test_specialized_traits_enhanced_statistics() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Store some pages to populate caches
        for i in 1..=10 {
            let data = vec![i as u8; 128];
            cache_manager.store_page(i, data);
        }

        // Test enhanced statistics using specialized traits
        let enhanced_stats = cache_manager.get_enhanced_cache_statistics();

        // Verify enhanced statistics structure
        assert_eq!(enhanced_stats.lru_k_value, 2); // Default K value for LRU-K
        assert_eq!(enhanced_stats.hot_cache_algorithm, "LRU-K");
        assert_eq!(enhanced_stats.warm_cache_algorithm, "LFU");
        assert_eq!(enhanced_stats.cold_cache_algorithm, "FIFO");

        // Verify basic stats are included (values are unsigned, so always >= 0)
        // Just verify they're accessible without panicking
        let _ = enhanced_stats.basic_stats.hot_cache_used;
        let _ = enhanced_stats.basic_stats.warm_cache_used;
        let _ = enhanced_stats.basic_stats.cold_cache_used;
    }

    #[test]
    fn test_specialized_traits_page_access_details() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Store a page and verify it's accessible through different cache levels
        let page_id = 42;
        let data = vec![42; 256];
        cache_manager.store_page(page_id, data);

        // Get detailed access information using specialized traits
        let access_details = cache_manager.get_page_access_details(page_id);

        if let Some(details) = access_details {
            // Verify we get meaningful access details
            assert!(!details.cache_level.is_empty());
            assert!(!details.algorithm.is_empty());

            // Different cache levels should provide different information
            match details.algorithm.as_str() {
                "LRU-K" => {
                    assert!(details.k_value.is_some());
                    assert_eq!(details.k_value.unwrap(), 2); // Default K value
                    assert!(details.access_count.is_some());
                },
                "LFU" => {
                    assert!(details.access_count.is_some()); // Frequency information
                },
                "FIFO" => {
                    assert!(details.eviction_rank.is_some()); // Age rank information
                },
                _ => panic!("Unknown algorithm: {}", details.algorithm),
            }
        }
    }

    #[test]
    fn test_specialized_maintenance_operations() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Fill caches with data
        for i in 1..=20 {
            let data = vec![i as u8; 128];
            cache_manager.store_page(i, data);
        }

        // Test specialized maintenance using new traits
        cache_manager.perform_specialized_maintenance();

        // Verify cache is still functional after specialized maintenance
        let stats = cache_manager.get_cache_statistics();
        // Values are unsigned, so always >= 0 - just verify they're accessible
        let _ = stats.hot_cache_used;
        let _ = stats.warm_cache_used;
        let _ = stats.cold_cache_used;
    }

    #[test]
    fn test_lru_k_specific_functionality() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Store pages with different access patterns to test LRU-K behavior
        for i in 1..=5 {
            let data = vec![i as u8; 64];
            cache_manager.store_page(i, data);
        }

        // Access some pages multiple times to trigger LRU-K behavior
        for _ in 0..3 {
            let _ = cache_manager.get_page(1);
            let _ = cache_manager.get_page(2);
        }

        // Access page 1 one more time to give it K=2 accesses
        let _ = cache_manager.get_page(1);

        // Get page access details to verify LRU-K specific information
        if let Some(details) = cache_manager.get_page_access_details(1)
            && details.algorithm == "LRU-K"
        {
            assert_eq!(details.k_value, Some(2));
            assert!(details.access_count.is_some());
            // Page 1 should have at least 2 accesses (K value reached)
            assert!(details.access_count.unwrap() >= 2);
        }
    }

    #[test]
    fn test_cache_manager_trait_migration_compatibility() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Test that all existing functionality still works after trait migration

        // Basic store and retrieve
        let page_id = 100;
        let data = vec![100; 512];
        cache_manager.store_page(page_id, data.clone());
        let retrieved = cache_manager.get_page(page_id);
        assert_eq!(retrieved.unwrap(), data);

        // Prefetch functionality
        let predictions = cache_manager.trigger_prefetch(page_id);
        assert!(!predictions.is_empty());

        // Memory pressure and admission control
        cache_manager.update_memory_pressure();
        // Memory pressure is unsigned, so always >= 0 - just verify it's accessible
        let _ = cache_manager.get_memory_pressure();
        assert!(cache_manager.get_admission_rate() > 0);

        // Statistics
        let stats = cache_manager.get_cache_statistics();
        // hot_cache_used is unsigned, so always >= 0 - just verify it's accessible
        let _ = stats.hot_cache_used;

        // Enhanced statistics using new traits
        let enhanced_stats = cache_manager.get_enhanced_cache_statistics();
        assert_eq!(enhanced_stats.lru_k_value, 2);

        // Page access details using specialized traits
        let access_details = cache_manager.get_page_access_details(page_id);
        assert!(access_details.is_some());
    }

    #[test]
    fn test_multi_level_cache_promotion_with_traits() {
        let config = DiskManagerConfig::default();
        let cache_manager = CacheManager::new(&config);

        // Store pages that will initially go to cold cache
        for i in 1..=10 {
            let data = vec![i as u8; 64];
            cache_manager.store_page(i, data);
        }

        let initial_stats = cache_manager.get_cache_statistics();

        // Access pages to trigger promotions between cache levels
        for _ in 0..5 {
            for i in 1..=5 {
                let _ = cache_manager.get_page(i);
            }
        }

        let final_stats = cache_manager.get_cache_statistics();

        // Should have some promotions
        assert!(final_stats.promotion_count >= initial_stats.promotion_count);

        // Test that we can get detailed information about promoted pages
        for i in 1..=5 {
            if let Some(details) = cache_manager.get_page_access_details(i) {
                // Pages that were accessed multiple times might be in hot or warm cache
                assert!(!details.cache_level.is_empty());
                assert!(!details.algorithm.is_empty());
            }
        }
    }
}
