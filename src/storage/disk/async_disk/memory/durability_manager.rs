//! # Durability Manager
//!
//! This module provides `DurabilityManager`, which enforces data persistence guarantees by
//! coordinating `fsync` operations and Write-Ahead Log (WAL) synchronization. It translates
//! high-level durability policies into concrete I/O actions.
//!
//! ## Architecture
//!
//! ```text
//!   WriteManager / FlushCoordinator
//!   ═══════════════════════════════════════════════════════════════════════════
//!                          │
//!                          │ apply_durability(pages, provider)
//!                          ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                      DurabilityManager                                  │
//!   │                                                                         │
//!   │   ┌─────────────────────────────────────────────────────────────────┐   │
//!   │   │  Configuration                                                  │   │
//!   │   │                                                                 │   │
//!   │   │  fsync_policy: FsyncPolicy     ← When to sync                   │   │
//!   │   │  durability_level: DurabilityLevel  ← What guarantees           │   │
//!   │   │  wal_enabled: bool             ← Use Write-Ahead Logging?       │   │
//!   │   └─────────────────────────────────────────────────────────────────┘   │
//!   │                                                                         │
//!   │   ┌─────────────────────────────────────────────────────────────────┐   │
//!   │   │  State Tracking                                                 │   │
//!   │   │                                                                 │   │
//!   │   │  pending_syncs: AtomicUsize    ← Pages awaiting confirmation    │   │
//!   │   │  last_sync_time: Mutex<Instant> ← For periodic policy           │   │
//!   │   └─────────────────────────────────────────────────────────────────┘   │
//!   └─────────────────────────────────────────────────────────────────────────┘
//!                          │
//!                          │ calls sync_data() / sync_log()
//!                          ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │              DurabilityProvider (AsyncDiskManager)                      │
//!   │                                                                         │
//!   │   sync_data() ──► fsync(db_file)                                        │
//!   │   sync_log()  ──► fsync(wal_file)                                       │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Durability Levels
//!
//! ```text
//!   Level        Survives         Sync Behavior              Performance
//!   ═══════════════════════════════════════════════════════════════════════════
//!   None         Nothing          No sync                    Fastest
//!   Buffer       Process crash    OS buffers only            Fast
//!   Sync         Power loss       fsync data file            Moderate
//!   Durable      Power loss       fsync WAL first, then data Slowest (safest)
//! ```
//!
//! ## Write-Ahead Logging Protocol
//!
//! ```text
//!   DurabilityLevel::Durable with wal_enabled=true
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   Step 1: Write log records to WAL file
//!           (already done by LogManager before flush)
//!
//!   Step 2: sync_log() ──► fsync(wal_file)
//!           ↓
//!           WAL is now durable on disk
//!
//!   Step 3: sync_data() ──► fsync(db_file)
//!           ↓
//!           Data pages are now durable on disk
//!
//!   Why this order?
//!   • If crash after Step 2: WAL has redo info, recovery replays it
//!   • If crash after Step 3: Both WAL and data are consistent
//!   • Never sync data before WAL (would violate Write-Ahead property)
//! ```
//!
//! ## Fsync Policies
//!
//! | Policy           | Behavior                                            |
//! |------------------|-----------------------------------------------------|
//! | `Never`          | Never call fsync (rely on OS crash recovery only)   |
//! | `OnFlush`        | Sync when flush() is explicitly called              |
//! | `PerWrite`       | Sync after every write (very slow, very safe)       |
//! | `Periodic(dur)`  | Sync if `dur` has elapsed since last sync           |
//!
//! ## Key Components
//!
//! | Component             | Description                                       |
//! |-----------------------|---------------------------------------------------|
//! | `DurabilityManager`   | Coordinates sync operations based on policy       |
//! | `DurabilityProvider`  | Trait for performing actual sync I/O              |
//! | `DurabilityResult`    | Outcome of `apply_durability()` call              |
//! | `DurabilityLevel`     | Enum: None, Buffer, Sync, Durable                 |
//! | `FsyncPolicy`         | Enum: Never, OnFlush, PerWrite, Periodic          |
//!
//! ## Core Operations
//!
//! | Method                    | Description                                    |
//! |---------------------------|------------------------------------------------|
//! | `new()`                   | Create with policy, level, and WAL flag        |
//! | `apply_durability()`      | Apply guarantees to flushed pages              |
//! | `estimate_sync_overhead()`| Estimate latency cost of syncing pages         |
//! | `fsync_policy()`          | Get current fsync policy                       |
//! | `durability_level()`      | Get current durability level                   |
//! | `pending_syncs()`         | Get count of pages synced since creation       |
//! | `set_*()` methods         | Runtime configuration updates                  |
//!
//! ## DurabilityResult
//!
//! | Field              | Description                                        |
//! |--------------------|----------------------------------------------------|
//! | `sync_performed`   | Whether fsync was actually called                  |
//! | `wal_written`      | Whether WAL was involved                           |
//! | `pages_synced`     | Number of pages in the operation                   |
//! | `durability_level` | The level that was applied                         |
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::storage::disk::async_disk::memory::durability_manager::{
//!     DurabilityManager, DurabilityProvider,
//! };
//! use crate::storage::disk::async_disk::config::{DurabilityLevel, FsyncPolicy};
//!
//! // Create manager with full durability
//! let manager = DurabilityManager::new(
//!     FsyncPolicy::OnFlush,
//!     DurabilityLevel::Durable,
//!     true, // WAL enabled
//! );
//!
//! // Apply durability after writing pages
//! let pages = vec![(1, page_data1), (2, page_data2)];
//! let result = manager.apply_durability(&pages, &disk_manager).await?;
//!
//! if result.sync_performed {
//!     println!("Pages synced to disk");
//! }
//! if result.wal_written {
//!     println!("WAL was synced first");
//! }
//!
//! // Estimate overhead before deciding to sync
//! let overhead = manager.estimate_sync_overhead(&pages)?;
//! println!("Estimated sync time: {:?}", overhead);
//! ```
//!
//! ## Overhead Estimation
//!
//! ```text
//!   DurabilityLevel    Base Overhead    Size Factor         WAL Overhead
//!   ═══════════════════════════════════════════════════════════════════════════
//!   None               0                0                   0
//!   Buffer             10 µs            0                   0
//!   Sync               1 ms             1 ns per KB         0
//!   Durable            5 ms             2 ns per KB         +2 ms if WAL enabled
//! ```
//!
//! ## Thread Safety
//!
//! - `pending_syncs`: `AtomicUsize` for lock-free counting
//! - `last_sync_time`: `Mutex<Instant>` for accurate timing (minimal contention)
//! - Configuration fields are immutable after creation (use `set_*` for updates)
//! - Safe for concurrent `apply_durability()` calls from multiple tasks

use crate::common::config::PageId;
use crate::storage::disk::async_disk::config::{DurabilityLevel, FsyncPolicy};
use std::future::Future;
use std::io::{Result as IoResult};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Interface for performing durability operations
pub trait DurabilityProvider {
    /// Syncs data pages to disk
    fn sync_data(&self) -> impl Future<Output = IoResult<()>> + Send;
    
    /// Syncs the WAL/log file to disk
    fn sync_log(&self) -> impl Future<Output = IoResult<()>> + Send;
}

/// Manages durability guarantees and sync policies
#[derive(Debug)]
pub struct DurabilityManager {
    fsync_policy: FsyncPolicy,
    durability_level: DurabilityLevel,
    wal_enabled: bool,
    pending_syncs: AtomicUsize,
    last_sync_time: Mutex<Instant>,
}

/// Result of applying durability guarantees
#[derive(Debug, Clone, PartialEq)]
pub struct DurabilityResult {
    /// Whether fsync was performed
    pub sync_performed: bool,
    /// Whether WAL was written
    pub wal_written: bool,
    /// Number of pages that needed sync
    pub pages_synced: usize,
    /// Durability level applied
    pub durability_level: DurabilityLevel,
}

impl DurabilityManager {
    /// Creates a new durability manager with the specified configuration
    pub fn new(
        fsync_policy: FsyncPolicy,
        durability_level: DurabilityLevel,
        wal_enabled: bool,
    ) -> Self {
        Self {
            fsync_policy,
            durability_level,
            wal_enabled,
            pending_syncs: AtomicUsize::new(0),
            last_sync_time: Mutex::new(Instant::now()),
        }
    }

    /// Applies durability guarantees to flushed pages
    pub async fn apply_durability<P>(
        &self,
        pages: &[(PageId, Vec<u8>)],
        provider: &P,
    ) -> IoResult<DurabilityResult>
    where
        P: DurabilityProvider,
    {
        if pages.is_empty() {
            return Ok(DurabilityResult {
                sync_performed: false,
                wal_written: false,
                pages_synced: 0,
                durability_level: self.durability_level.clone(),
            });
        }

        let mut result = DurabilityResult {
            sync_performed: false,
            wal_written: false,
            pages_synced: pages.len(),
            durability_level: self.durability_level.clone(),
        };

        match self.durability_level {
            DurabilityLevel::None => {
                // No durability guarantees - just return
                Ok(result)
            }
            DurabilityLevel::Buffer => {
                // Buffer durability - data is in OS buffers but not necessarily on disk
                // We assume writing to WAL (OS buffer) is sufficient for this level if enabled
                result.wal_written = self.wal_enabled;
                Ok(result)
            }
            DurabilityLevel::Sync => {
                // Sync durability - ensure data reaches disk
                result.sync_performed = self.should_sync_pages(pages)?;
                result.wal_written = self.wal_enabled;

                if result.sync_performed {
                    // Sync data
                    provider.sync_data().await?;
                    self.pending_syncs.fetch_add(pages.len(), Ordering::Relaxed);
                    *self.last_sync_time.lock() = Instant::now();
                }

                Ok(result)
            }
            DurabilityLevel::Durable => {
                // Full durability - sync to disk and WAL
                result.sync_performed = true;
                result.wal_written = self.wal_enabled;

                // Ensure WAL is synced first (Write-Ahead Logging protocol)
                if self.wal_enabled {
                    provider.sync_log().await?;
                }

                // Then sync data
                provider.sync_data().await?;

                self.pending_syncs.fetch_add(pages.len(), Ordering::Relaxed);
                *self.last_sync_time.lock() = Instant::now();
                Ok(result)
            }
        }
    }

    /// Checks if pages should be synced based on fsync policy
    fn should_sync_pages(&self, _pages: &[(PageId, Vec<u8>)]) -> IoResult<bool> {
        match self.fsync_policy {
            FsyncPolicy::Never => Ok(false),
            FsyncPolicy::OnFlush => Ok(true),
            FsyncPolicy::PerWrite => Ok(true),
            FsyncPolicy::Periodic(duration) => {
                let last_sync = *self.last_sync_time.lock();
                Ok(last_sync.elapsed() >= duration)
            }
        }
    }

    /// Gets the current fsync policy
    pub fn fsync_policy(&self) -> &FsyncPolicy {
        &self.fsync_policy
    }

    /// Gets the current durability level
    pub fn durability_level(&self) -> &DurabilityLevel {
        &self.durability_level
    }

    /// Checks if WAL is enabled
    pub fn is_wal_enabled(&self) -> bool {
        self.wal_enabled
    }

    /// Gets the number of pending sync operations
    pub fn pending_syncs(&self) -> usize {
        self.pending_syncs.load(Ordering::Relaxed)
    }

    /// Updates the fsync policy
    pub fn set_fsync_policy(&mut self, policy: FsyncPolicy) {
        self.fsync_policy = policy;
    }

    /// Updates the durability level
    pub fn set_durability_level(&mut self, level: DurabilityLevel) {
        self.durability_level = level;
    }

    /// Enables or disables WAL
    pub fn set_wal_enabled(&mut self, enabled: bool) {
        self.wal_enabled = enabled;
    }

    /// Resets pending sync counter (for testing or maintenance)
    pub fn reset_pending_syncs(&self) {
        self.pending_syncs.store(0, Ordering::Relaxed);
    }

    /// Estimates the sync overhead for a given set of pages
    pub fn estimate_sync_overhead(&self, pages: &[(PageId, Vec<u8>)]) -> IoResult<Duration> {
        if pages.is_empty() {
            return Ok(Duration::ZERO);
        }

        match self.durability_level {
            DurabilityLevel::None => Ok(Duration::ZERO),
            DurabilityLevel::Buffer => Ok(Duration::from_micros(10)), // Minimal overhead
            DurabilityLevel::Sync => {
                // Estimate based on page count and data size
                let total_size: usize = pages.iter().map(|(_, data)| data.len()).sum();
                let base_overhead = Duration::from_millis(1);
                let size_overhead = Duration::from_nanos((total_size / 1024) as u64);
                Ok(base_overhead + size_overhead)
            }
            DurabilityLevel::Durable => {
                // Higher overhead for full durability
                let total_size: usize = pages.iter().map(|(_, data)| data.len()).sum();
                let base_overhead = Duration::from_millis(5);
                let size_overhead = Duration::from_nanos((total_size / 512) as u64);
                let wal_overhead = if self.wal_enabled {
                    Duration::from_millis(2)
                } else {
                    Duration::ZERO
                };
                Ok(base_overhead + size_overhead + wal_overhead)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::async_disk::config::{DurabilityLevel, FsyncPolicy};

    struct MockDurabilityProvider;
    impl DurabilityProvider for MockDurabilityProvider {
        async fn sync_data(&self) -> IoResult<()> { Ok(()) }
        async fn sync_log(&self) -> IoResult<()> { Ok(()) }
    }

    #[test]
    fn test_durability_manager_creation() {
        let manager = DurabilityManager::new(FsyncPolicy::OnFlush, DurabilityLevel::Sync, true);

        assert_eq!(*manager.fsync_policy(), FsyncPolicy::OnFlush);
        assert_eq!(*manager.durability_level(), DurabilityLevel::Sync);
        assert!(manager.is_wal_enabled());
        assert_eq!(manager.pending_syncs(), 0);
    }

    #[tokio::test]
    async fn test_apply_durability_none() {
        let manager = DurabilityManager::new(FsyncPolicy::Never, DurabilityLevel::None, false);
        let provider = MockDurabilityProvider;

        let pages = vec![(1, vec![1, 2, 3, 4])];
        let result = manager.apply_durability(&pages, &provider).await.unwrap();

        assert!(!result.sync_performed);
        assert!(!result.wal_written);
        assert_eq!(result.pages_synced, 1);
        assert_eq!(result.durability_level, DurabilityLevel::None);
    }

    #[tokio::test]
    async fn test_apply_durability_buffer() {
        let manager = DurabilityManager::new(FsyncPolicy::Never, DurabilityLevel::Buffer, true);
        let provider = MockDurabilityProvider;

        let pages = vec![(1, vec![1, 2, 3, 4]), (2, vec![5, 6, 7, 8])];
        let result = manager.apply_durability(&pages, &provider).await.unwrap();

        assert!(!result.sync_performed);
        assert!(result.wal_written);
        assert_eq!(result.pages_synced, 2);
        assert_eq!(result.durability_level, DurabilityLevel::Buffer);
    }

    #[tokio::test]
    async fn test_apply_durability_sync() {
        let manager = DurabilityManager::new(FsyncPolicy::OnFlush, DurabilityLevel::Sync, true);
        let provider = MockDurabilityProvider;

        let pages = vec![(1, vec![1, 2, 3, 4])];
        let result = manager.apply_durability(&pages, &provider).await.unwrap();

        assert!(result.sync_performed);
        assert!(result.wal_written);
        assert_eq!(result.pages_synced, 1);
        assert_eq!(result.durability_level, DurabilityLevel::Sync);
        assert!(manager.pending_syncs() > 0);
    }

    #[tokio::test]
    async fn test_apply_durability_durable() {
        let manager = DurabilityManager::new(FsyncPolicy::OnFlush, DurabilityLevel::Durable, true);
        let provider = MockDurabilityProvider;

        let pages = vec![(1, vec![1, 2, 3, 4])];
        let result = manager.apply_durability(&pages, &provider).await.unwrap();

        assert!(result.sync_performed);
        assert!(result.wal_written);
        assert_eq!(result.pages_synced, 1);
        assert_eq!(result.durability_level, DurabilityLevel::Durable);
    }

    #[tokio::test]
    async fn test_apply_durability_empty_pages() {
        let manager = DurabilityManager::new(FsyncPolicy::OnFlush, DurabilityLevel::Durable, true);
        let provider = MockDurabilityProvider;

        let pages = vec![];
        let result = manager.apply_durability(&pages, &provider).await.unwrap();

        assert!(!result.sync_performed);
        assert!(!result.wal_written);
        assert_eq!(result.pages_synced, 0);
    }

    #[tokio::test]
    async fn test_apply_durability_periodic() {
        let manager = DurabilityManager::new(
            FsyncPolicy::Periodic(Duration::from_millis(50)),
            DurabilityLevel::Sync,
            false,
        );
        let provider = MockDurabilityProvider;
        let pages = vec![(1, vec![1, 2, 3])];

        // First call should not sync (initialized with now)
        let result = manager.apply_durability(&pages, &provider).await.unwrap();
        assert!(!result.sync_performed);

        // Sleep to exceed duration
        tokio::time::sleep(Duration::from_millis(60)).await;

        // Second call should sync
        let result = manager.apply_durability(&pages, &provider).await.unwrap();
        assert!(result.sync_performed);

        // Immediate subsequent call should not sync
        let result = manager.apply_durability(&pages, &provider).await.unwrap();
        assert!(!result.sync_performed);
    }

    #[test]
    fn test_configuration_updates() {
        let mut manager = DurabilityManager::new(FsyncPolicy::Never, DurabilityLevel::None, false);

        manager.set_fsync_policy(FsyncPolicy::OnFlush);
        assert_eq!(*manager.fsync_policy(), FsyncPolicy::OnFlush);

        manager.set_durability_level(DurabilityLevel::Durable);
        assert_eq!(*manager.durability_level(), DurabilityLevel::Durable);

        manager.set_wal_enabled(true);
        assert!(manager.is_wal_enabled());
    }

    #[tokio::test]
    async fn test_pending_syncs_tracking() {
        let manager = DurabilityManager::new(FsyncPolicy::OnFlush, DurabilityLevel::Sync, false);
        let provider = MockDurabilityProvider;

        assert_eq!(manager.pending_syncs(), 0);

        // Apply durability to some pages
        let pages = vec![(1, vec![1, 2, 3, 4]), (2, vec![5, 6, 7, 8])];
        manager.apply_durability(&pages, &provider).await.unwrap();

        assert!(manager.pending_syncs() > 0);

        // Reset counter
        manager.reset_pending_syncs();
        assert_eq!(manager.pending_syncs(), 0);
    }

    #[test]
    fn test_estimate_sync_overhead() {
        let manager = DurabilityManager::new(FsyncPolicy::OnFlush, DurabilityLevel::Durable, true);

        // Empty pages
        let empty_pages = vec![];
        let overhead = manager.estimate_sync_overhead(&empty_pages).unwrap();
        assert_eq!(overhead, std::time::Duration::ZERO);

        // Non-empty pages
        let pages = vec![(1, vec![0u8; 1024])];
        let overhead = manager.estimate_sync_overhead(&pages).unwrap();
        assert!(overhead > std::time::Duration::ZERO);
    }

    #[test]
    fn test_should_sync_pages_policies() {
        let never_manager =
            DurabilityManager::new(FsyncPolicy::Never, DurabilityLevel::Sync, false);

        let on_flush_manager =
            DurabilityManager::new(FsyncPolicy::OnFlush, DurabilityLevel::Sync, false);

        let pages = vec![(1, vec![1, 2, 3, 4])];

        assert!(!never_manager.should_sync_pages(&pages).unwrap());
        assert!(on_flush_manager.should_sync_pages(&pages).unwrap());
    }
}
