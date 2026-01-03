//! # Coalescing Engine
//!
//! This module provides `CoalescingEngine`, which identifies and merges related write requests
//! before they are buffered. It acts as a short-term holding area where adjacent page writes
//! can be fused, converting random I/O patterns into more sequential patterns.
//!
//! ## Architecture
//!
//! ```text
//!   WriteManager
//!   ═══════════════════════════════════════════════════════════════════════════
//!          │
//!          │ try_coalesce_write(page_id, data)
//!          ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                       CoalescingEngine                                  │
//!   │                                                                         │
//!   │   ┌─────────────────────────────────────────────────────────────────┐   │
//!   │   │  pending_writes: HashMap<PageId, Vec<u8>>                       │   │
//!   │   │                                                                 │   │
//!   │   │  ┌──────┬──────────────────┬───────────────┬──────────────────┐ │   │
//!   │   │  │ PID  │ Data             │ Timestamp     │ Access Count     │ │   │
//!   │   │  ├──────┼──────────────────┼───────────────┼──────────────────┤ │   │
//!   │   │  │  1   │ [page bytes...]  │ 10:32:01.123  │ 3                │ │   │
//!   │   │  │  2   │ [page bytes...]  │ 10:32:01.456  │ 1                │ │   │
//!   │   │  │  5   │ [page bytes...]  │ 10:32:00.789  │ 2                │ │   │
//!   │   │  └──────┴──────────────────┴───────────────┴──────────────────┘ │   │
//!   │   └─────────────────────────────────────────────────────────────────┘   │
//!   │                                                                         │
//!   │   ┌─────────────────────────────────────────────────────────────────┐   │
//!   │   │  Configuration                                                  │   │
//!   │   │                                                                 │   │
//!   │   │  coalesce_window:   Duration   ← Max time to hold pending write │   │
//!   │   │  max_coalesce_size: usize      ← Max pending writes before evict│   │
//!   │   └─────────────────────────────────────────────────────────────────┘   │
//!   │                                                                         │
//!   │   ┌────────────────────────┐                                            │
//!   │   │  SizeAnalyzer          │ ← Efficiency calculations                  │
//!   │   └────────────────────────┘                                            │
//!   └─────────────────────────────────────────────────────────────────────────┘
//!          │
//!          │ CoalesceResult { NoCoalesce | Coalesced | Merged }
//!          ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │              WriteStagingBuffer                                         │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Write Coalescing Flow
//!
//! ```text
//!   try_coalesce_write(page_id, data)
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │ Step 1: INPUT VALIDATION                                               │
//!   │                                                                        │
//!   │   data.is_empty()?                                                     │
//!   │        │                                                               │
//!   │        ├── Yes: Return Err(InvalidInput)                               │
//!   │        └── No:  Continue                                               │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │ Step 2: CHECK FOR EXISTING WRITE                                       │
//!   │                                                                        │
//!   │   pending_writes.contains_key(page_id)?                                │
//!   │        │                                                               │
//!   │        ├── Yes: MERGE (newer data replaces older)                      │
//!   │        │        • Update pending_writes[page_id] = data                │
//!   │        │        • Update timestamp                                     │
//!   │        │        • Increment access_frequency                           │
//!   │        │        • Return Merged(data)                                  │
//!   │        │                                                               │
//!   │        └── No:  NEW WRITE                                              │
//!   │                 • Insert into pending_writes                           │
//!   │                 • Record timestamp                                     │
//!   │                 • Set access_frequency = 1                             │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │ Step 3: CAPACITY CHECK & CLEANUP                                       │
//!   │                                                                        │
//!   │   pending_writes.len() > max_coalesce_size?                            │
//!   │        │                                                               │
//!   │        ├── Yes: simple_cleanup()                                       │
//!   │        │        • Remove oldest entries until len <= max/2             │
//!   │        │                                                               │
//!   │        └── No:  Skip cleanup                                           │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   Return NoCoalesce(data)  or  Coalesced(data)
//! ```
//!
//! ## Cleanup Policies
//!
//! ```text
//!   cleanup_expired_writes(now)
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   Phase 1: TIME-BASED EXPIRATION
//!   ┌──────────────────────────────────────────────────────────────────────┐
//!   │  For each pending write:                                             │
//!   │     if now - timestamp > coalesce_window:                            │
//!   │        mark for cleanup (TimeExpired)                                │
//!   └──────────────────────────────────────────────────────────────────────┘
//!                              │
//!                              ▼
//!   Phase 2: MEMORY PRESSURE
//!   ┌──────────────────────────────────────────────────────────────────────┐
//!   │  memory_pressure = current_usage / max_usage                         │
//!   │                                                                      │
//!   │  if memory_pressure > 80% OR len > max_pending:                      │
//!   │     • Extreme (>90%): Remove 50% of writes                           │
//!   │     • High (>80%):    Remove excess writes                           │
//!   │                                                                      │
//!   │  Priority: older + larger + lower access frequency                   │
//!   └──────────────────────────────────────────────────────────────────────┘
//!                              │
//!                              ▼
//!   Phase 3: ACCESS PATTERN
//!   ┌──────────────────────────────────────────────────────────────────────┐
//!   │  if not enough cleaned by time/memory:                               │
//!   │     Select lowest access_frequency pages (up to 12.5%)               │
//!   │     mark for cleanup (LowAccess)                                     │
//!   └──────────────────────────────────────────────────────────────────────┘
//!                              │
//!                              ▼
//!   Phase 4: EXECUTE CLEANUP
//!   ┌──────────────────────────────────────────────────────────────────────┐
//!   │  For each marked page:                                               │
//!   │     • Remove from pending_writes                                     │
//!   │     • Remove from write_timestamps                                   │
//!   │     • Remove from access_frequencies                                 │
//!   │     • Track bytes freed and reason                                   │
//!   └──────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## CoalesceResult Variants
//!
//! | Variant       | Meaning                                             |
//! |---------------|-----------------------------------------------------|
//! | `NoCoalesce`  | First write for this page, stored as-is             |
//! | `Coalesced`   | Combined with adjacent pending writes               |
//! | `Merged`      | Replaced existing pending write for same page       |
//!
//! ## CleanupReason Variants
//!
//! | Reason           | Trigger Condition                                |
//! |------------------|--------------------------------------------------|
//! | `TimeExpired`    | Write exceeded `coalesce_window` duration        |
//! | `MemoryPressure` | Total memory usage > 80% of max                  |
//! | `LowAccess`      | Page has low access frequency relative to others |
//!
//! ## Key Components
//!
//! | Component          | Description                                        |
//! |--------------------|----------------------------------------------------|
//! | `CoalescingEngine` | Main engine with pending writes and tracking       |
//! | `CoalesceResult`   | Outcome of `try_coalesce_write()` operation        |
//! | `CleanupReason`    | Why a pending write was evicted                    |
//! | `CleanupMetrics`   | Statistics from last cleanup operation             |
//! | `CoalescingStats`  | Current engine state for monitoring                |
//!
//! ## Core Operations
//!
//! | Method                       | Description                                |
//! |------------------------------|--------------------------------------------|
//! | `new()`                      | Create with coalesce window and max size   |
//! | `try_coalesce_write()`       | Add write, possibly merging with existing  |
//! | `find_adjacent_pages()`      | Find pages within ±4 of given page ID      |
//! | `cleanup_expired_writes()`   | Multi-phase cleanup of stale/excess writes |
//! | `get_coalescing_size_analysis()` | Detailed size analysis via `SizeAnalyzer` |
//! | `get_stats()`                | Get current engine statistics              |
//! | `update_config()`            | Change window and max size at runtime      |
//! | `clear_all()`                | Remove all pending writes (for shutdown)   |
//!
//! ## CoalescingStats Fields
//!
//! | Field                 | Description                                    |
//! |-----------------------|------------------------------------------------|
//! | `pending_writes`      | Number of pages currently pending              |
//! | `total_data_size`     | Sum of all pending write data sizes            |
//! | `oldest_write_age`    | Duration since oldest pending write            |
//! | `memory_pressure`     | Current pressure ratio (0.0 to 1.0)            |
//! | `last_cleanup_age`    | Duration since last cleanup operation          |
//! | `last_cleanup_metrics`| Details from most recent cleanup               |
//!
//! ## CleanupMetrics Fields
//!
//! | Field                  | Description                                   |
//! |------------------------|-----------------------------------------------|
//! | `cleaned_by_time`      | Pages removed due to time expiration          |
//! | `cleaned_by_memory`    | Pages removed due to memory pressure          |
//! | `cleaned_by_access`    | Pages removed due to low access frequency     |
//! | `total_data_size_freed`| Bytes freed by cleanup                        |
//! | `cleanup_timestamp`    | When cleanup was performed                    |
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::storage::disk::async_disk::memory::coalescing::engine::{
//!     CoalescingEngine, CoalesceResult,
//! };
//! use std::time::Duration;
//!
//! // Create engine: 10ms window, max 64 pending writes
//! let mut engine = CoalescingEngine::new(
//!     Duration::from_millis(10),
//!     64,
//! );
//!
//! // First write to page 1
//! let data1 = vec![1u8; 4096];
//! match engine.try_coalesce_write(1, data1)? {
//!     CoalesceResult::NoCoalesce(data) => {
//!         println!("First write for page 1, stored as pending");
//!     }
//!     _ => unreachable!(),
//! }
//!
//! // Second write to same page (merge)
//! let data2 = vec![2u8; 4096];
//! match engine.try_coalesce_write(1, data2)? {
//!     CoalesceResult::Merged(data) => {
//!         println!("Merged with existing write for page 1");
//!     }
//!     _ => unreachable!(),
//! }
//!
//! // Find adjacent pages
//! let adjacent = engine.find_adjacent_pages(3);
//! println!("Pages adjacent to 3: {:?}", adjacent);
//!
//! // Check statistics
//! let stats = engine.get_stats();
//! println!("Pending writes: {}", stats.pending_writes);
//! println!("Memory pressure: {:.1}%", stats.memory_pressure * 100.0);
//!
//! // Manual cleanup (usually done automatically)
//! engine.cleanup_expired_writes(std::time::Instant::now())?;
//!
//! // Shutdown
//! engine.clear_all();
//! ```
//!
//! ## Adjacent Page Detection
//!
//! ```text
//!   find_adjacent_pages(page_id = 5)
//!
//!   Search range: page_id ± 4
//!
//!   ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐
//!   │  1  │  2  │  3  │  4  │  5  │  6  │  7  │  8  │  9  │
//!   └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘
//!         ◄───────────────────┼───────────────────►
//!              -4 to -1       │      +1 to +4
//!
//!   Pending writes: [2, 4, 7, 10]
//!
//!   Result: [2, 4, 7]  (page 10 is outside ±4 range)
//! ```
//!
//! ## Memory Pressure Priority Scoring
//!
//! ```text
//!   priority_score = age × 0.4 + size × 0.4 + (1/access_freq) × 0.2
//!
//!   Higher score = higher priority for eviction
//!
//!   Example:
//!   ┌────────┬────────┬────────┬──────────┬─────────────────┐
//!   │ PageID │ Age(ms)│ Size   │ Accesses │ Priority Score  │
//!   ├────────┼────────┼────────┼──────────┼─────────────────┤
//!   │   1    │  100   │  4096  │    5     │ 40 + 1638 + 0.04│
//!   │   3    │  500   │  4096  │    1     │ 200 + 1638 + 0.2│  ← Evict first
//!   │   7    │   50   │  1024  │    3     │ 20 + 410 + 0.07 │
//!   └────────┴────────┴────────┴──────────┴─────────────────┘
//! ```
//!
//! ## Thread Safety
//!
//! - `CoalescingEngine` is designed to be wrapped in `Arc<RwLock<>>`
//! - Internal state uses `HashMap` for `O(1)` lookups
//! - Cleanup is rate-limited (100ms minimum interval) to prevent thrashing
//! - All state mutations are done through `&mut self` methods

use std::collections::{HashMap, HashSet};
use std::io::{Error as IoError, ErrorKind, Result as IoResult};
use std::time::{Duration, Instant};

use super::size_analyzer::{CoalescedSizeInfo, SizeAnalyzer};
use crate::common::config::PageId;

/// Result of coalescing operation
#[derive(Debug)]
pub enum CoalesceResult {
    /// No coalescing performed, return original data
    NoCoalesce(Vec<u8>),
    /// Coalesced with existing writes
    Coalesced(Vec<u8>),
    /// Write was merged with existing data for same page
    Merged(Vec<u8>),
}

/// Reason for cleanup during write coalescing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CleanupReason {
    /// Write exceeded the configured time window
    TimeExpired,
    /// Cleanup due to memory pressure
    MemoryPressure,
    /// Cleanup due to low access frequency
    LowAccess,
}

/// Engine for coalescing adjacent writes
#[derive(Debug)]
pub struct CoalescingEngine {
    pub pending_writes: HashMap<PageId, Vec<u8>>,
    pub coalesce_window: Duration,
    pub max_coalesce_size: usize,
    // Track timestamps for pending writes to enable time-based cleanup
    pub write_timestamps: HashMap<PageId, Instant>,
    // Track access patterns for smarter cleanup decisions
    pub access_frequencies: HashMap<PageId, u32>,
    // Last cleanup time to avoid over-cleaning
    pub last_cleanup: Instant,
    // Size analyzer for coalescing decisions
    size_analyzer: SizeAnalyzer,
    // Track cleanup metrics from last cleanup operation
    last_cleanup_metrics: CleanupMetrics,
}

/// Cleanup metrics from a cleanup operation
#[derive(Debug, Clone, Default)]
pub struct CleanupMetrics {
    pub cleaned_by_time: usize,
    pub cleaned_by_memory: usize,
    pub cleaned_by_access: usize,
    pub total_data_size_freed: usize,
    pub cleanup_timestamp: Option<Instant>,
}

impl CoalescingEngine {
    /// Creates a new coalescing engine with the specified configuration
    pub fn new(coalesce_window: Duration, max_coalesce_size: usize) -> Self {
        Self {
            pending_writes: HashMap::new(),
            coalesce_window,
            max_coalesce_size,
            write_timestamps: HashMap::new(),
            access_frequencies: HashMap::new(),
            last_cleanup: Instant::now(),
            size_analyzer: SizeAnalyzer::new(),
            last_cleanup_metrics: CleanupMetrics::default(),
        }
    }

    /// Attempts to coalesce a write with pending writes (simplified for reliability)
    pub fn try_coalesce_write(
        &mut self,
        page_id: PageId,
        data: Vec<u8>,
    ) -> IoResult<CoalesceResult> {
        // Input validation
        if data.is_empty() {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "Cannot coalesce empty data",
            ));
        }

        // Simplified approach - just check if we already have this page
        if let Some(_existing_data) = self.pending_writes.get(&page_id) {
            // Same page - merge the writes (newer data wins)
            self.pending_writes.insert(page_id, data.clone());
            self.write_timestamps.insert(page_id, Instant::now());
            *self.access_frequencies.entry(page_id).or_insert(0) += 1;
            return Ok(CoalesceResult::Merged(data));
        }

        // New page - just add it (skip complex coalescing logic for now)
        self.pending_writes.insert(page_id, data.clone());
        self.write_timestamps.insert(page_id, Instant::now());
        self.access_frequencies.insert(page_id, 1);

        // Simple cleanup if we have too many pending writes
        if self.pending_writes.len() > self.max_coalesce_size {
            self.simple_cleanup();
        }

        Ok(CoalesceResult::NoCoalesce(data))
    }

    /// Simple cleanup that removes oldest entries
    fn simple_cleanup(&mut self) {
        if self.pending_writes.len() <= self.max_coalesce_size {
            return;
        }

        // Find oldest entries and remove them
        let target_size = self.max_coalesce_size / 2; // Keep only half
        let mut oldest_pages: Vec<_> = self
            .write_timestamps
            .iter()
            .map(|(&page_id, &timestamp)| (page_id, timestamp))
            .collect();

        // Sort by timestamp (oldest first)
        oldest_pages.sort_by_key(|(_, timestamp)| *timestamp);

        // Remove oldest entries until we reach target size
        let to_remove = self.pending_writes.len().saturating_sub(target_size);
        for (page_id, _) in oldest_pages.into_iter().take(to_remove) {
            self.pending_writes.remove(&page_id);
            self.write_timestamps.remove(&page_id);
            self.access_frequencies.remove(&page_id);
        }

        self.last_cleanup = Instant::now();
    }

    /// Cleans up expired writes that have exceeded the coalesce window
    pub fn cleanup_expired_writes(&mut self, now: Instant) -> IoResult<()> {
        // Rate limiting: avoid too frequent cleanups
        const MIN_CLEANUP_INTERVAL: Duration = Duration::from_millis(100);
        if now.duration_since(self.last_cleanup) < MIN_CLEANUP_INTERVAL {
            return Ok(());
        }

        let mut expired_pages = Vec::new();
        let mut cleaned_by_time = 0usize;
        let mut cleaned_by_memory = 0usize;
        let mut cleaned_by_access = 0usize;

        // Phase 1: Time-based expiration - Remove writes older than coalesce window
        for (&page_id, &timestamp) in &self.write_timestamps {
            if now.duration_since(timestamp) > self.coalesce_window {
                expired_pages.push((page_id, CleanupReason::TimeExpired));
                cleaned_by_time += 1;
            }
        }

        // Phase 2: Memory pressure detection and handling
        let max_pending_writes = self.calculate_max_pending_writes();
        let current_memory_usage = self.calculate_memory_usage();
        let memory_pressure = self.detect_memory_pressure(current_memory_usage);

        if memory_pressure > 0.8 || self.pending_writes.len() > max_pending_writes {
            // Under high memory pressure, be more aggressive
            let target_reduction = if memory_pressure > 0.9 {
                self.pending_writes.len() / 2 // Remove 50% under extreme pressure
            } else {
                self.pending_writes.len().saturating_sub(max_pending_writes)
            };

            let memory_candidates =
                self.select_memory_pressure_candidates(target_reduction, &expired_pages);

            cleaned_by_memory = memory_candidates.len();
            expired_pages.extend(
                memory_candidates
                    .into_iter()
                    .map(|p| (p, CleanupReason::MemoryPressure)),
            );
        }

        // Phase 3: Access pattern-based cleanup for rarely accessed pages
        if expired_pages.len() < max_pending_writes / 4 {
            let access_candidates = self.select_low_access_candidates(
                max_pending_writes / 8, // Clean up to 12.5% based on access patterns
                &expired_pages,
            );

            cleaned_by_access = access_candidates.len();
            expired_pages.extend(
                access_candidates
                    .into_iter()
                    .map(|p| (p, CleanupReason::LowAccess)),
            );
        }

        // Phase 4: Execute cleanup with proper bookkeeping
        let mut total_data_size_freed = 0usize;
        let mut pages_by_reason = HashMap::new();

        for (page_id, reason) in expired_pages {
            if let Some(data) = self.pending_writes.remove(&page_id) {
                total_data_size_freed += data.len();
                self.write_timestamps.remove(&page_id);
                self.access_frequencies.remove(&page_id);

                *pages_by_reason.entry(reason).or_insert(0usize) += 1;
            }
        }

        // Update cleanup timestamp
        self.last_cleanup = now;

        // Phase 5: Store cleanup metrics for monitoring and adaptive behavior
        self.last_cleanup_metrics = CleanupMetrics {
            cleaned_by_time,
            cleaned_by_memory,
            cleaned_by_access,
            total_data_size_freed,
            cleanup_timestamp: Some(now),
        };

        // Phase 6: Maintain data structure integrity
        self.validate_coalescing_state()?;

        // Phase 7: Log cleanup metrics (in production, this would go to metrics system)
        if total_data_size_freed > 0 {
            #[cfg(debug_assertions)]
            {
                println!(
                    "Cleanup completed: {} bytes freed, {} by time, {} by memory, {} by access",
                    total_data_size_freed, cleaned_by_time, cleaned_by_memory, cleaned_by_access
                );
            }
        }

        Ok(())
    }

    /// Finds adjacent pages that can be coalesced with the given page
    pub fn find_adjacent_pages(&self, page_id: PageId) -> Vec<PageId> {
        let mut adjacent = Vec::new();

        // Look for immediately adjacent pages (page_id ± 1, ± 2, etc.)
        for offset in 1..=4 {
            // Check previous pages (with underflow protection)
            if page_id >= offset {
                let prev_page = page_id - offset;
                if self.pending_writes.contains_key(&prev_page) {
                    adjacent.push(prev_page);
                }
            }

            // Check next pages (with overflow protection)
            if let Some(next_page) = page_id.checked_add(offset)
                && self.pending_writes.contains_key(&next_page)
            {
                adjacent.push(next_page);
            }
        }

        // Sort by page_id for consistent ordering
        adjacent.sort_unstable();
        adjacent
    }

    /// Detects memory pressure as a ratio (0.0 to 1.0)
    fn detect_memory_pressure(&self, current_usage: usize) -> f64 {
        let max_usage = self.max_coalesce_size * 4096; // Assuming 4KB pages
        if max_usage == 0 {
            return 0.0;
        }
        (current_usage as f64) / (max_usage as f64)
    }

    /// Calculates current memory usage of pending writes
    fn calculate_memory_usage(&self) -> usize {
        self.pending_writes.values().map(|data| data.len()).sum()
    }

    /// Selects candidates for cleanup under memory pressure
    fn select_memory_pressure_candidates(
        &self,
        target_count: usize,
        already_selected: &[(PageId, CleanupReason)],
    ) -> Vec<PageId> {
        let already_selected_set: HashSet<_> = already_selected.iter().map(|(id, _)| *id).collect();

        // Create a priority list: older writes + larger data + lower access frequency
        let mut candidates: Vec<_> = self
            .pending_writes
            .iter()
            .filter(|&(&page_id, _)| !already_selected_set.contains(&page_id))
            .map(|(&page_id, data)| {
                let age = self
                    .write_timestamps
                    .get(&page_id)
                    .map(|&ts| ts.elapsed().as_millis() as f64)
                    .unwrap_or(0.0);
                let size = data.len() as f64;
                let access_freq = *self.access_frequencies.get(&page_id).unwrap_or(&1) + 1;
                let access_penalty = 1.0 / (access_freq as f64);

                // Higher score = higher priority for cleanup
                let priority_score = age * 0.4 + size * 0.4 + access_penalty * 0.2;
                (page_id, priority_score)
            })
            .collect();

        // Sort by priority score (descending)
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        candidates
            .into_iter()
            .take(target_count)
            .map(|(page_id, _)| page_id)
            .collect()
    }

    /// Selects candidates with low access patterns for cleanup
    fn select_low_access_candidates(
        &self,
        target_count: usize,
        already_selected: &[(PageId, CleanupReason)],
    ) -> Vec<PageId> {
        let already_selected_set: HashSet<_> = already_selected.iter().map(|(id, _)| *id).collect();

        let mut candidates: Vec<_> = self
            .access_frequencies
            .iter()
            .filter(|&(&page_id, _)| !already_selected_set.contains(&page_id))
            .filter(|&(&page_id, _)| self.pending_writes.contains_key(&page_id))
            .map(|(&page_id, &freq)| (page_id, freq))
            .collect();

        // Sort by access frequency (ascending - least accessed first)
        candidates.sort_by_key(|(_, freq)| *freq);

        candidates
            .into_iter()
            .take(target_count)
            .map(|(page_id, _)| page_id)
            .collect()
    }

    /// Validates the internal consistency of coalescing data structures
    fn validate_coalescing_state(&self) -> IoResult<()> {
        // Ensure all data structures are in sync
        for &page_id in self.pending_writes.keys() {
            if !self.write_timestamps.contains_key(&page_id) {
                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    format!("Missing timestamp for page {}", page_id),
                ));
            }
            if !self.access_frequencies.contains_key(&page_id) {
                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    format!("Missing access frequency for page {}", page_id),
                ));
            }
        }

        // Check for orphaned timestamps or frequencies
        for &page_id in self.write_timestamps.keys() {
            if !self.pending_writes.contains_key(&page_id) {
                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    format!("Orphaned timestamp for page {}", page_id),
                ));
            }
        }

        for &page_id in self.access_frequencies.keys() {
            if !self.pending_writes.contains_key(&page_id) {
                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    format!("Orphaned access frequency for page {}", page_id),
                ));
            }
        }

        Ok(())
    }

    /// Calculates the maximum number of pending writes to maintain
    fn calculate_max_pending_writes(&self) -> usize {
        // Dynamic calculation based on coalesce_window and max_coalesce_size
        // This prevents unbounded memory growth
        std::cmp::max(self.max_coalesce_size, 128)
    }

    /// Gets detailed size analysis for coalescing decisions
    pub fn get_coalescing_size_analysis(
        &self,
        adjacent_pages: &[PageId],
        new_data: &[u8],
    ) -> CoalescedSizeInfo {
        self.size_analyzer.calculate_detailed_coalesced_size(
            adjacent_pages,
            new_data,
            &self.pending_writes,
        )
    }

    /// Gets statistics about the coalescing engine
    pub fn get_stats(&self) -> CoalescingStats {
        CoalescingStats {
            pending_writes: self.pending_writes.len(),
            total_data_size: self.calculate_memory_usage(),
            oldest_write_age: self.get_oldest_write_age(),
            memory_pressure: self.detect_memory_pressure(self.calculate_memory_usage()),
            last_cleanup_age: self.last_cleanup.elapsed(),
            last_cleanup_metrics: self.last_cleanup_metrics.clone(),
        }
    }

    /// Gets the age of the oldest pending write
    fn get_oldest_write_age(&self) -> Duration {
        self.write_timestamps
            .values()
            .map(|&timestamp| timestamp.elapsed())
            .max()
            .unwrap_or(Duration::ZERO)
    }

    /// Updates configuration
    pub fn update_config(&mut self, coalesce_window: Duration, max_coalesce_size: usize) {
        self.coalesce_window = coalesce_window;
        self.max_coalesce_size = max_coalesce_size;
    }

    /// Clears all pending writes (for shutdown or reset)
    pub fn clear_all(&mut self) {
        self.pending_writes.clear();
        self.write_timestamps.clear();
        self.access_frequencies.clear();
        self.last_cleanup = Instant::now();
        self.last_cleanup_metrics = CleanupMetrics::default();
    }
}

/// Statistics about the coalescing engine
#[derive(Debug, Clone)]
pub struct CoalescingStats {
    pub pending_writes: usize,
    pub total_data_size: usize,
    pub oldest_write_age: Duration,
    pub memory_pressure: f64,
    pub last_cleanup_age: Duration,
    pub last_cleanup_metrics: CleanupMetrics,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_coalescing_engine_creation() {
        let engine = CoalescingEngine::new(Duration::from_millis(100), 64);
        assert_eq!(engine.coalesce_window, Duration::from_millis(100));
        assert_eq!(engine.max_coalesce_size, 64);
        assert!(engine.pending_writes.is_empty());
    }

    #[test]
    fn test_try_coalesce_write_empty_data() {
        let mut engine = CoalescingEngine::new(Duration::from_millis(100), 64);
        let result = engine.try_coalesce_write(1, vec![]);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn test_try_coalesce_write_first_write() {
        let mut engine = CoalescingEngine::new(Duration::from_millis(100), 64);
        let data = vec![1, 2, 3, 4];

        let result = engine.try_coalesce_write(1, data.clone()).unwrap();
        match result {
            CoalesceResult::NoCoalesce(returned_data) => {
                assert_eq!(returned_data, data);
            },
            _ => panic!("Expected NoCoalesce result"),
        }

        assert_eq!(engine.pending_writes.len(), 1);
        assert!(engine.pending_writes.contains_key(&1));
    }

    #[test]
    fn test_try_coalesce_write_same_page_merge() {
        let mut engine = CoalescingEngine::new(Duration::from_millis(100), 64);
        let first_data = vec![1, 2, 3, 4];
        let second_data = vec![5, 6, 7, 8];

        // First write
        engine.try_coalesce_write(1, first_data).unwrap();

        // Second write to same page should merge
        let result = engine.try_coalesce_write(1, second_data.clone()).unwrap();
        match result {
            CoalesceResult::Merged(returned_data) => {
                assert_eq!(returned_data, second_data);
            },
            _ => panic!("Expected Merged result"),
        }

        assert_eq!(engine.pending_writes.len(), 1);
        assert_eq!(engine.pending_writes.get(&1), Some(&second_data));
    }

    #[test]
    fn test_find_adjacent_pages() {
        let mut engine = CoalescingEngine::new(Duration::from_millis(100), 64);

        // Set up some pending writes
        engine.pending_writes.insert(1, vec![1]);
        engine.pending_writes.insert(3, vec![3]);
        engine.pending_writes.insert(4, vec![4]);
        engine.pending_writes.insert(5, vec![5]);
        engine.pending_writes.insert(10, vec![10]);

        // Test finding adjacent pages for page 2
        let adjacent = engine.find_adjacent_pages(2);
        assert!(adjacent.contains(&1));
        assert!(adjacent.contains(&3));
        assert!(!adjacent.contains(&10)); // Too far

        // Test finding adjacent pages for page 4
        let adjacent = engine.find_adjacent_pages(4);
        assert!(adjacent.contains(&3));
        assert!(adjacent.contains(&5));
    }

    #[test]
    fn test_cleanup_expired_writes_rate_limiting() {
        let mut engine = CoalescingEngine::new(Duration::from_millis(100), 64);

        // Add some writes
        engine.try_coalesce_write(1, vec![1, 2, 3, 4]).unwrap();

        let now = Instant::now();

        // First cleanup should work
        let result = engine.cleanup_expired_writes(now);
        assert!(result.is_ok());

        // Immediate second cleanup should be rate limited (no error, just early return)
        let result = engine.cleanup_expired_writes(now);
        assert!(result.is_ok());

        // Data should still be there (wasn't cleaned due to rate limiting)
        assert_eq!(engine.pending_writes.len(), 1);
    }

    #[test]
    fn test_detect_memory_pressure() {
        let engine = CoalescingEngine::new(Duration::from_millis(100), 64);

        // Test with no usage
        let pressure = engine.detect_memory_pressure(0);
        assert_eq!(pressure, 0.0);

        // Test with some usage
        let half_usage = engine.max_coalesce_size * 4096 / 2;
        let pressure = engine.detect_memory_pressure(half_usage);
        assert!((pressure - 0.5).abs() < 0.01); // Should be ~50%
    }

    #[test]
    fn test_calculate_memory_usage() {
        let mut engine = CoalescingEngine::new(Duration::from_millis(100), 64);

        // Add some writes with known sizes
        engine.pending_writes.insert(1, vec![0u8; 100]);
        engine.pending_writes.insert(2, vec![0u8; 200]);

        let usage = engine.calculate_memory_usage();
        assert_eq!(usage, 300); // 100 + 200 bytes
    }

    #[test]
    fn test_validate_coalescing_state_valid() {
        let mut engine = CoalescingEngine::new(Duration::from_millis(100), 64);

        // Add a write normally (should be valid)
        engine.try_coalesce_write(1, vec![1, 2, 3, 4]).unwrap();

        let result = engine.validate_coalescing_state();
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_stats() {
        let mut engine = CoalescingEngine::new(Duration::from_millis(100), 64);

        // Add some writes with non-adjacent pages to avoid coalescing
        engine.try_coalesce_write(1, vec![1, 2, 3, 4]).unwrap();
        engine.try_coalesce_write(10, vec![5, 6, 7, 8]).unwrap();

        let stats = engine.get_stats();
        assert_eq!(stats.pending_writes, 2);
        assert!(stats.total_data_size > 0);
        assert!(stats.memory_pressure >= 0.0 && stats.memory_pressure <= 1.0);
    }

    #[test]
    fn test_update_config() {
        let mut engine = CoalescingEngine::new(Duration::from_millis(100), 64);

        engine.update_config(Duration::from_millis(200), 128);
        assert_eq!(engine.coalesce_window, Duration::from_millis(200));
        assert_eq!(engine.max_coalesce_size, 128);
    }

    #[test]
    fn test_clear_all() {
        let mut engine = CoalescingEngine::new(Duration::from_millis(100), 64);

        // Add some writes with non-adjacent pages to avoid coalescing
        engine.try_coalesce_write(1, vec![1, 2, 3, 4]).unwrap();
        engine.try_coalesce_write(10, vec![5, 6, 7, 8]).unwrap();

        assert_eq!(engine.pending_writes.len(), 2);

        engine.clear_all();

        assert!(engine.pending_writes.is_empty());
        assert!(engine.write_timestamps.is_empty());
        assert!(engine.access_frequencies.is_empty());
    }
}
