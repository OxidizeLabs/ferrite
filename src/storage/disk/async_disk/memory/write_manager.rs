//! # Orchestrated Write Manager
//!
//! This module provides `WriteManager`, the central coordinator for the memory-side write path.
//! It follows the Single Responsibility Principle by delegating specific tasks to specialized
//! sub-components while orchestrating the overall flow of data from memory to disk.
//!
//! ## Architecture
//!
//! ```text
//!   AsyncDiskManager
//!   ═══════════════════════════════════════════════════════════════════════════
//!          │
//!          │ buffer_write(page_id, data)
//!          ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                          WriteManager                                   │
//!   │                                                                         │
//!   │   ┌─────────────────────────────────────────────────────────────────┐   │
//!   │   │  Sub-Components (Single Responsibility)                         │   │
//!   │   │                                                                 │   │
//!   │   │  ┌──────────────────┐  ┌──────────────────┐                     │   │
//!   │   │  │ CoalescingEngine │  │ WriteStagingBuffer│                    │   │
//!   │   │  │ (Arc<RwLock<>>)  │  │ (Arc<Mutex<>>)   │                     │   │
//!   │   │  │                  │  │                  │                     │   │
//!   │   │  │ • Merge writes   │  │ • Buffer pages   │                     │   │
//!   │   │  │ • Batch adjacent │  │ • Compression    │                     │   │
//!   │   │  │ • Time window    │  │ • Memory limits  │                     │   │
//!   │   │  └──────────────────┘  └──────────────────┘                     │   │
//!   │   │                                                                 │   │
//!   │   │  ┌──────────────────┐  ┌──────────────────┐                     │   │
//!   │   │  │ FlushCoordinator │  │ DurabilityManager│                     │   │
//!   │   │  │ (Arc<>)          │  │ (Arc<>)          │                     │   │
//!   │   │  │                  │  │                  │                     │   │
//!   │   │  │ • When to flush  │  │ • fsync policy   │                     │   │
//!   │   │  │ • Prevent storms │  │ • WAL ordering   │                     │   │
//!   │   │  │ • Timing control │  │ • Durability lvl │                     │   │
//!   │   │  └──────────────────┘  └──────────────────┘                     │   │
//!   │   └─────────────────────────────────────────────────────────────────┘   │
//!   └─────────────────────────────────────────────────────────────────────────┘
//!          │
//!          │ flushed pages (Vec<(PageId, Vec<u8>)>)
//!          ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │              Disk I/O (performed by caller)                             │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Write Pipeline Flow
//!
//! ```text
//!   buffer_write(page_id, data)
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │ Step 1: COALESCING                                                     │
//!   │                                                                        │
//!   │   try_coalesce_write(page_id, data)                                    │
//!   │        │                                                               │
//!   │        ├── NoCoalesce: First write for this page                       │
//!   │        ├── Coalesced:  Merged with pending write (same page)           │
//!   │        └── Merged:     Combined with adjacent pages                    │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │ Step 2: BUFFERING                                                      │
//!   │                                                                        │
//!   │   buffer_manager.buffer_write(page_id, coalesced_data)                 │
//!   │        │                                                               │
//!   │        ├── Accepted: Page buffered (possibly compressed)               │
//!   │        └── Rejected: Buffer full → force flush                         │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │ Step 3: FLUSH DECISION                                                 │
//!   │                                                                        │
//!   │   flush_coordinator.should_flush(dirty_pages, time_since_flush)        │
//!   │        │                                                               │
//!   │        ├── NoFlush:        Continue buffering                          │
//!   │        ├── ThresholdFlush: Too many dirty pages                        │
//!   │        └── IntervalFlush:  Data too stale                              │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        │ if flush needed
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │ Step 4: FLUSH with DURABILITY                                          │
//!   │                                                                        │
//!   │   flush_coordinator.try_start_flush()  ← Prevent concurrent flushes    │
//!   │        │                                                               │
//!   │   buffer_manager.drain_buffer()        ← Get all buffered pages        │
//!   │        │                                                               │
//!   │   durability_manager.apply_durability() ← WAL first, then fsync        │
//!   │        │                                                               │
//!   │   flush_coordinator.complete_flush()   ← Release lock, update time     │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   Return: Option<Vec<(PageId, Vec<u8>)>>
//!           Some(pages) if flushed, None if buffered
//! ```
//!
//! ## Key Components
//!
//! | Component           | Responsibility                                      |
//! |---------------------|-----------------------------------------------------|
//! | `CoalescingEngine`  | Merge writes to same page, batch adjacent pages     |
//! | `WriteStagingBuffer`| Hold dirty pages in memory, optional compression    |
//! | `FlushCoordinator`  | Decide when to flush, prevent concurrent flushes    |
//! | `DurabilityManager` | Ensure WAL/fsync ordering for durability            |
//!
//! ## Core Operations
//!
//! | Method                     | Description                                    |
//! |----------------------------|------------------------------------------------|
//! | `new()`                    | Create from `DiskManagerConfig`                |
//! | `buffer_write()`           | Main entry: coalesce → buffer → maybe flush    |
//! | `flush()`                  | Drain buffer with durability guarantees        |
//! | `force_flush()`            | Flush regardless of thresholds (with retry)   |
//! | `should_flush()`           | Check if flush conditions are met              |
//! | `get_buffer_stats()`       | Get buffer manager statistics                  |
//! | `get_comprehensive_stats()`| Aggregate stats from all sub-components        |
//! | `apply_durability()`       | Apply durability to external page set          |
//! | `clear_all_pending()`      | Clear coalescing engine (for shutdown)         |
//!
//! ## Configuration (from `DiskManagerConfig`)
//!
//! | Field                  | Used For                                        |
//! |------------------------|-------------------------------------------------|
//! | `write_buffer_size_mb` | Max memory for `WriteStagingBuffer`             |
//! | `compression_enabled`  | Enable page compression in buffer               |
//! | `flush_threshold_pages`| Dirty page count trigger for `FlushCoordinator` |
//! | `flush_interval_ms`    | Time-based flush trigger                        |
//! | `fsync_policy`         | When to call fsync (`DurabilityManager`)        |
//! | `durability_level`     | None/Buffer/Sync/Durable                        |
//! | `wal_enabled`          | Use Write-Ahead Logging                         |
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::storage::disk::async_disk::memory::write_manager::WriteManager;
//! use crate::storage::disk::async_disk::config::DiskManagerConfig;
//!
//! // Create write manager from configuration
//! let config = DiskManagerConfig::default();
//! let write_manager = WriteManager::new(&config);
//!
//! // Buffer a write (automatically coalesces, compresses, decides on flush)
//! let page_id = 42;
//! let data = vec![0u8; 4096];
//! let flushed = write_manager.buffer_write(page_id, data, &disk_manager).await?;
//!
//! if let Some(pages) = flushed {
//!     // Pages were flushed - caller should write them to disk
//!     for (page_id, page_data) in pages {
//!         disk_manager.write_page(page_id, &page_data).await?;
//!     }
//! }
//!
//! // Force flush all buffered pages (e.g., on checkpoint)
//! let all_pages = write_manager.force_flush(&disk_manager).await?;
//!
//! // Get comprehensive statistics
//! let stats = write_manager.get_comprehensive_stats().await;
//! println!("Dirty pages: {}", stats.buffer_stats.dirty_pages);
//! println!("Flush in progress: {}", stats.flush_in_progress);
//! println!("WAL enabled: {}", stats.wal_enabled);
//! ```
//!
//! ## Concurrent Flush Handling
//!
//! ```text
//!   force_flush() with retry logic
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   ┌────────────────┐
//!   │ try_start_flush│
//!   └───────┬────────┘
//!           │
//!           ├── Success: Acquired lock → proceed with flush
//!           │
//!           └── Failure: Lock held by another task
//!                   │
//!                   ▼
//!              ┌────────────────────────────┐
//!              │ Retry loop (up to 100x)    │
//!              │ Sleep 10ms between retries │
//!              │ Total timeout: ~1 second   │
//!              └────────────────────────────┘
//!                   │
//!                   ├── Eventually succeeds → proceed with flush
//!                   │
//!                   └── Max retries exceeded → Error(WouldBlock)
//!
//!   Note: Regular flush() returns empty Vec if lock unavailable (no retry)
//! ```
//!
//! ## ComprehensiveStats Fields
//!
//! | Field                 | Source                   | Description                 |
//! |-----------------------|--------------------------|-----------------------------|
//! | `buffer_stats`        | `WriteStagingBuffer`     | Dirty pages, buffer size    |
//! | `coalescing_stats`    | `CoalescingEngine`       | Pending writes, merges      |
//! | `flush_in_progress`   | `FlushCoordinator`       | Is flush currently running  |
//! | `time_since_last_flush`| `FlushCoordinator`      | Staleness of buffered data  |
//! | `durability_level`    | `DurabilityManager`      | Current durability setting  |
//! | `wal_enabled`         | `DurabilityManager`      | Whether WAL is active       |
//!
//! ## Thread Safety
//!
//! - `buffer_manager`: `Arc<Mutex<>>` - exclusive access for buffer operations
//! - `coalescing_engine`: `Arc<RwLock<>>` - read-heavy access pattern
//! - `flush_coordinator`: `Arc<>` - internal atomics for lock-free flush gating
//! - `durability_manager`: `Arc<>` - mostly immutable config with atomic counters
//! - Safe for concurrent `buffer_write()` calls from multiple async tasks
//! - Only one flush can proceed at a time (coordinated by `FlushCoordinator`)

use super::{
    CoalesceResult, CoalescedSizeInfo, CoalescingEngine, DurabilityManager, DurabilityProvider,
    DurabilityResult, FlushCoordinator, FlushDecision, WriteBufferStats, WriteStagingBuffer,
};
use crate::common::config::PageId;
use crate::storage::disk::async_disk::config::DiskManagerConfig;
use std::io::{Error as IoError, ErrorKind, Result as IoResult};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// Orchestrated write management system following Single Responsibility Principle
///
/// The WriteManager acts as a coordinator between specialized components:
/// - WriteStagingBuffer: Handles write buffering and compression
/// - FlushCoordinator: Manages flush timing and coordination
/// - CoalescingEngine: Handles write coalescing and optimization
/// - DurabilityManager: Ensures durability guarantees and sync policies
#[derive(Debug)]
pub struct WriteManager {
    // Write buffering component
    buffer_manager: Arc<Mutex<WriteStagingBuffer>>,

    // Flush coordination component
    pub(crate) flush_coordinator: Arc<FlushCoordinator>,

    // Write coalescing component
    coalescing_engine: Arc<RwLock<CoalescingEngine>>,

    // Durability management component
    durability_manager: Arc<DurabilityManager>,
}

impl WriteManager {
    /// Creates a new orchestrated write manager
    pub fn new(config: &DiskManagerConfig) -> Self {
        let max_buffer_size = config.write_buffer_size_mb * 1024 * 1024; // Convert to bytes

        let buffer_manager = Arc::new(Mutex::new(WriteStagingBuffer::new(
            max_buffer_size,
            config.compression_enabled,
        )));

        let flush_coordinator = Arc::new(FlushCoordinator::new(
            config.flush_threshold_pages,
            std::time::Duration::from_millis(config.flush_interval_ms),
        ));

        let coalescing_engine = Arc::new(RwLock::new(CoalescingEngine::new(
            std::time::Duration::from_millis(10), // 10ms coalesce window
            64,                                   // max 64 pages to coalesce
        )));

        let durability_manager = Arc::new(DurabilityManager::new(
            config.fsync_policy.clone(),
            config.durability_level.clone(),
            config.wal_enabled,
        ));

        Self {
            buffer_manager,
            flush_coordinator,
            coalescing_engine,
            durability_manager,
        }
    }

    /// Adds a page to the write buffer with coalescing, compression, and buffering
    ///
    /// This orchestrates the entire write path:
    /// 1. Attempt write coalescing for efficiency
    /// 2. Add to buffer with compression if enabled
    /// 3. Check flush conditions and coordinate flush if needed
    ///
    /// Returns: Optional list of pages that were flushed and need to be written to disk
    pub async fn buffer_write<P>(
        &self,
        page_id: PageId,
        data: Vec<u8>,
        provider: &P,
    ) -> IoResult<Option<Vec<(PageId, Vec<u8>)>>>
    where
        P: DurabilityProvider,
    {
        log::debug!(
            "WriteManager::buffer_write called for page {} with {} bytes",
            page_id,
            data.len()
        );

        // Step 1: Try to coalesce the write for efficiency
        let coalesced_data = self.try_coalesce_write(page_id, data).await?;
        log::debug!(
            "WriteManager::buffer_write coalesced data for page {} now {} bytes",
            page_id,
            coalesced_data.len()
        );

        // Step 2: Add to buffer manager with compression
        let needs_flush = {
            let mut buffer = self.buffer_manager.lock().await;
            let stats_before = buffer.get_stats();
            log::debug!(
                "WriteManager::buffer_write buffer stats before: dirty_pages={}, buffer_size={}",
                stats_before.dirty_pages,
                stats_before.buffer_size_bytes
            );

            let buffer_accepted = buffer.buffer_write(page_id, coalesced_data)?;

            let stats_after = buffer.get_stats();
            log::debug!(
                "WriteManager::buffer_write buffer stats after: dirty_pages={}, buffer_size={}, accepted={}",
                stats_after.dirty_pages,
                stats_after.buffer_size_bytes,
                buffer_accepted
            );

            if !buffer_accepted {
                log::debug!("WriteManager::buffer_write buffer full, needs flush");
                true // Buffer is full, needs flush
            } else {
                // Check if we should flush based on coordinator's decisions
                let stats = buffer.get_stats();
                let flush_decision = self
                    .flush_coordinator
                    .should_flush(stats.dirty_pages, stats.time_since_last_flush)
                    .await;

                let should_flush = matches!(
                    flush_decision,
                    FlushDecision::ThresholdFlush | FlushDecision::IntervalFlush
                );
                log::debug!(
                    "WriteManager::buffer_write flush decision: {:?}, should_flush={}",
                    flush_decision,
                    should_flush
                );
                should_flush
            }
        };

        // Step 3: Coordinate flush if needed
        let flushed_pages = if needs_flush {
            log::debug!("WriteManager::buffer_write triggering flush");
            let pages = self.flush(provider).await?;
            log::debug!(
                "WriteManager::buffer_write flush returned {} pages",
                pages.len()
            );
            if pages.is_empty() { None } else { Some(pages) }
        } else {
            None
        };

        log::debug!("WriteManager::buffer_write completed for page {}", page_id);
        Ok(flushed_pages)
    }

    /// Attempts to coalesce a write using the coalescing engine
    async fn try_coalesce_write(&self, page_id: PageId, data: Vec<u8>) -> IoResult<Vec<u8>> {
        let mut coalescing = self.coalescing_engine.write().await;

        match coalescing.try_coalesce_write(page_id, data)? {
            CoalesceResult::NoCoalesce(data) => Ok(data),
            CoalesceResult::Coalesced(data) => Ok(data),
            CoalesceResult::Merged(data) => Ok(data),
        }
    }

    /// Flushes the write buffer with durability guarantees
    ///
    /// This orchestrates the flush process:
    /// 1. Coordinate flush timing to prevent concurrent flushes
    /// 2. Drain buffered writes from buffer manager
    /// 3. Apply durability guarantees through durability manager
    pub async fn flush<P>(&self, provider: &P) -> IoResult<Vec<(PageId, Vec<u8>)>>
    where
        P: DurabilityProvider,
    {
        // Step 1: Try to start flush coordination
        if !self.flush_coordinator.try_start_flush() {
            // Another flush is already in progress
            return Ok(Vec::new());
        }

        // Step 2: Perform the actual flush
        let result = self.flush_internal(provider).await;

        // Step 3: Complete flush coordination
        self.flush_coordinator.complete_flush().await;

        result
    }

    /// Forces a flush regardless of thresholds
    pub async fn force_flush<P>(&self, provider: &P) -> IoResult<Vec<(PageId, Vec<u8>)>>
    where
        P: DurabilityProvider,
    {
        log::debug!("WriteManager::force_flush called");

        // Step 1: Force start flush coordination
        // Retry logic to handle concurrent flush attempts
        let mut retry_count = 0;
        const MAX_RETRIES: u32 = 100; // 100 * 10ms = 1s timeout

        loop {
            if self.flush_coordinator.try_start_flush() {
                break;
            }

            if retry_count >= MAX_RETRIES {
                log::error!(
                    "WriteManager::force_flush could not acquire flush lock after {} retries",
                    MAX_RETRIES
                );
                return Err(IoError::new(
                    ErrorKind::WouldBlock,
                    "Could not acquire flush lock for force flush",
                ));
            }

            log::debug!(
                "WriteManager::force_flush waiting for flush lock (retry {})",
                retry_count + 1
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            retry_count += 1;
        }

        log::debug!("WriteManager::force_flush acquired flush lock");

        // Step 2: Perform the actual flush
        let result = self.flush_internal(provider).await;

        // Step 3: Complete flush coordination
        self.flush_coordinator.complete_flush().await;

        match &result {
            Ok(pages) => log::debug!("WriteManager::force_flush returning {} pages", pages.len()),
            Err(e) => log::error!("WriteManager::force_flush failed: {}", e),
        }

        result
    }

    /// Internal flush implementation that coordinates between components
    async fn flush_internal<P>(&self, provider: &P) -> IoResult<Vec<(PageId, Vec<u8>)>>
    where
        P: DurabilityProvider,
    {
        log::debug!("WriteManager::flush_internal called");

        // Step 1: Drain pages from buffer manager
        let pages_to_flush = {
            let mut buffer = self.buffer_manager.lock().await;
            let stats = buffer.get_stats();
            log::debug!(
                "WriteManager::flush_internal buffer stats before drain: dirty_pages={}, buffer_size={}",
                stats.dirty_pages,
                stats.buffer_size_bytes
            );

            let pages = buffer.drain_buffer();
            log::debug!(
                "WriteManager::flush_internal drained {} pages from buffer",
                pages.len()
            );
            pages
        };

        if pages_to_flush.is_empty() {
            log::debug!("WriteManager::flush_internal no pages to flush");
            return Ok(pages_to_flush);
        }

        log::debug!(
            "WriteManager::flush_internal applying durability to {} pages",
            pages_to_flush.len()
        );

        // Step 2: Apply durability guarantees
        let _durability_result = self
            .durability_manager
            .apply_durability(&pages_to_flush, provider)
            .await?;

        log::debug!(
            "WriteManager::flush_internal completed successfully with {} pages",
            pages_to_flush.len()
        );
        Ok(pages_to_flush)
    }

    /// Checks if a flush should be performed based on current state
    pub async fn should_flush(&self) -> bool {
        let buffer = self.buffer_manager.lock().await;
        let stats = buffer.get_stats();

        let flush_decision = self
            .flush_coordinator
            .should_flush(stats.dirty_pages, stats.time_since_last_flush)
            .await;

        !matches!(flush_decision, FlushDecision::NoFlush)
    }

    /// Gets comprehensive statistics from all components
    pub async fn get_buffer_stats(&self) -> WriteBufferStats {
        let buffer = self.buffer_manager.lock().await;
        buffer.get_stats()
    }

    /// Gets coalescing size analysis for monitoring
    pub async fn get_coalescing_size_analysis(
        &self,
        adjacent_pages: &[PageId],
        new_data: &[u8],
    ) -> CoalescedSizeInfo {
        let coalescing = self.coalescing_engine.read().await;
        coalescing.get_coalescing_size_analysis(adjacent_pages, new_data)
    }

    /// Gets durability manager for configuration access
    pub fn get_durability_manager(&self) -> &Arc<DurabilityManager> {
        &self.durability_manager
    }

    /// Gets flush coordinator for configuration access
    pub fn get_flush_coordinator(&self) -> &Arc<FlushCoordinator> {
        &self.flush_coordinator
    }

    /// Applies durability guarantees to a set of pages (for external callers)
    pub async fn apply_durability<P>(
        &self,
        pages: &[(PageId, Vec<u8>)],
        provider: &P,
    ) -> IoResult<DurabilityResult>
    where
        P: DurabilityProvider,
    {
        self.durability_manager
            .apply_durability(pages, provider)
            .await
    }

    /// Updates coalescing configuration
    pub async fn update_coalescing_config(
        &self,
        coalesce_window: std::time::Duration,
        max_coalesce_size: usize,
    ) {
        let mut coalescing = self.coalescing_engine.write().await;
        coalescing.update_config(coalesce_window, max_coalesce_size);
    }

    /// Clears all pending writes (for shutdown scenarios)
    pub async fn clear_all_pending(&self) {
        let mut coalescing = self.coalescing_engine.write().await;
        coalescing.clear_all();
    }

    /// Gets statistics from all components for monitoring
    pub async fn get_comprehensive_stats(&self) -> ComprehensiveStats {
        let buffer_stats = self.get_buffer_stats().await;
        let coalescing_stats = {
            let coalescing = self.coalescing_engine.read().await;
            coalescing.get_stats()
        };
        let flush_in_progress = self.flush_coordinator.is_flush_in_progress();
        let time_since_last_flush = self.flush_coordinator.time_since_last_flush().await;

        ComprehensiveStats {
            buffer_stats,
            coalescing_stats,
            flush_in_progress,
            time_since_last_flush,
            durability_level: self.durability_manager.durability_level().clone(),
            wal_enabled: self.durability_manager.is_wal_enabled(),
        }
    }
}

/// Comprehensive statistics from all write manager components
#[derive(Debug)]
pub struct ComprehensiveStats {
    pub buffer_stats: WriteBufferStats,
    pub coalescing_stats: super::coalescing::engine::CoalescingStats,
    pub flush_in_progress: bool,
    pub time_since_last_flush: std::time::Duration,
    pub durability_level: crate::storage::disk::async_disk::config::DurabilityLevel,
    pub wal_enabled: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::async_disk::config::DiskManagerConfig;

    struct MockDurabilityProvider;
    impl DurabilityProvider for MockDurabilityProvider {
        async fn sync_data(&self) -> IoResult<()> {
            Ok(())
        }
        async fn sync_log(&self) -> IoResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_write_manager_creation() {
        let config = DiskManagerConfig::default();
        let write_manager = WriteManager::new(&config);

        // Verify components are properly initialized
        let stats = write_manager.get_buffer_stats().await;
        assert_eq!(stats.dirty_pages, 0);
        assert!(!write_manager.should_flush().await);
    }

    #[tokio::test]
    async fn test_buffer_write_basic() {
        let config = DiskManagerConfig::default();
        let write_manager = WriteManager::new(&config);
        let provider = MockDurabilityProvider;

        let page_id = 1;
        let data = vec![1, 2, 3, 4];

        // Test buffer_write
        write_manager
            .buffer_write(page_id, data.clone(), &provider)
            .await
            .unwrap();

        // Test get_buffer_stats
        let stats = write_manager.get_buffer_stats().await;
        assert_eq!(stats.dirty_pages, 1);
    }

    #[tokio::test]
    async fn test_flush_coordination() {
        let config = DiskManagerConfig::default();
        let write_manager = WriteManager::new(&config);
        let provider = MockDurabilityProvider;

        // Add some data
        write_manager
            .buffer_write(1, vec![1, 2, 3, 4], &provider)
            .await
            .unwrap();
        write_manager
            .buffer_write(2, vec![5, 6, 7, 8], &provider)
            .await
            .unwrap();

        // Test flush
        let flushed_pages = write_manager.force_flush(&provider).await.unwrap();
        assert_eq!(flushed_pages.len(), 2);

        // Test buffer is empty after flush
        let stats_after_flush = write_manager.get_buffer_stats().await;
        assert_eq!(stats_after_flush.dirty_pages, 0);
    }

    #[tokio::test]
    async fn test_coalescing_integration() {
        let config = DiskManagerConfig::default();
        let write_manager = WriteManager::new(&config);
        let provider = MockDurabilityProvider;

        let page_id = 1;
        let first_data = vec![1, 2, 3, 4];
        let second_data = vec![5, 6, 7, 8];

        // First write
        write_manager
            .buffer_write(page_id, first_data, &provider)
            .await
            .unwrap();

        // Second write to same page (should be coalesced/merged)
        write_manager
            .buffer_write(page_id, second_data.clone(), &provider)
            .await
            .unwrap();

        // Should still only have one dirty page
        let stats = write_manager.get_buffer_stats().await;
        assert_eq!(stats.dirty_pages, 1);
    }

    #[tokio::test]
    async fn test_comprehensive_stats() {
        let config = DiskManagerConfig::default();
        let write_manager = WriteManager::new(&config);
        let provider = MockDurabilityProvider;

        // Add some data
        write_manager
            .buffer_write(1, vec![1, 2, 3, 4], &provider)
            .await
            .unwrap();

        let stats = write_manager.get_comprehensive_stats().await;
        assert_eq!(stats.buffer_stats.dirty_pages, 1);
        assert!(!stats.flush_in_progress);
        assert!(stats.wal_enabled); // Default config enables WAL
    }

    #[tokio::test]
    async fn test_durability_integration() {
        let config = DiskManagerConfig::default();
        let write_manager = WriteManager::new(&config);
        let provider = MockDurabilityProvider;

        let pages = vec![(1, vec![1, 2, 3, 4]), (2, vec![5, 6, 7, 8])];
        let result = write_manager
            .apply_durability(&pages, &provider)
            .await
            .unwrap();

        assert!(result.pages_synced > 0);
        assert_eq!(result.durability_level, config.durability_level);
    }

    #[tokio::test]
    async fn test_coalescing_config_update() {
        let config = DiskManagerConfig::default();
        let write_manager = WriteManager::new(&config);
        let provider = MockDurabilityProvider;

        // Update coalescing configuration
        write_manager
            .update_coalescing_config(std::time::Duration::from_millis(50), 128)
            .await;

        // Verify configuration was updated (indirectly through behavior)
        // Add some writes to test the new configuration
        write_manager
            .buffer_write(1, vec![1, 2, 3, 4], &provider)
            .await
            .unwrap();
        write_manager
            .buffer_write(2, vec![5, 6, 7, 8], &provider)
            .await
            .unwrap();

        let stats = write_manager.get_comprehensive_stats().await;
        assert!(stats.coalescing_stats.pending_writes <= 128); // Should respect new limit
    }

    #[tokio::test]
    async fn test_clear_all_pending() {
        let config = DiskManagerConfig::default();
        let write_manager = WriteManager::new(&config);
        let provider = MockDurabilityProvider;

        // Add some data that would be in coalescing engine
        write_manager
            .buffer_write(1, vec![1, 2, 3, 4], &provider)
            .await
            .unwrap();

        // Clear all pending
        write_manager.clear_all_pending().await;

        let stats = write_manager.get_comprehensive_stats().await;
        assert_eq!(stats.coalescing_stats.pending_writes, 0);
    }

    #[tokio::test]
    async fn test_concurrent_flushes() {
        let config = DiskManagerConfig::default();
        let write_manager = Arc::new(WriteManager::new(&config));
        let provider = Arc::new(MockDurabilityProvider);

        // Add some data
        write_manager
            .buffer_write(1, vec![1, 2, 3, 4], &*provider)
            .await
            .unwrap();

        // Start concurrent flushes
        let wm1 = Arc::clone(&write_manager);
        let wm2 = Arc::clone(&write_manager);
        let p1 = Arc::clone(&provider);
        let p2 = Arc::clone(&provider);

        let flush1 = tokio::spawn(async move { wm1.flush(&*p1).await });

        let flush2 = tokio::spawn(async move { wm2.flush(&*p2).await });

        let results = tokio::try_join!(flush1, flush2).unwrap();

        // One should succeed with pages, the other should return empty (already flushed)
        let total_pages = results.0.unwrap().len() + results.1.unwrap().len();
        assert_eq!(total_pages, 1); // Only one page should be flushed total
    }
}
