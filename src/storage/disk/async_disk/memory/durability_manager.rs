//! Durability management and sync policy enforcement
//!
//! This module handles durability guarantees, WAL management,
//! and fsync policy enforcement for write operations.

use crate::common::config::PageId;
use crate::storage::disk::async_disk::config::{DurabilityLevel, FsyncPolicy};
use std::io::{Error as IoError, ErrorKind, Result as IoResult};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Manages durability guarantees and sync policies
#[derive(Debug)]
pub struct DurabilityManager {
    fsync_policy: FsyncPolicy,
    durability_level: DurabilityLevel,
    wal_enabled: bool,
    pending_syncs: AtomicUsize,
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
        }
    }

    /// Applies durability guarantees to flushed pages
    pub fn apply_durability(&self, pages: &[(PageId, Vec<u8>)]) -> IoResult<DurabilityResult> {
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
                result.wal_written = self.wal_enabled;
                Ok(result)
            }
            DurabilityLevel::Sync => {
                // Sync durability - ensure data reaches disk
                result.sync_performed = self.should_sync_pages(pages)?;
                result.wal_written = self.wal_enabled;

                if result.sync_performed {
                    self.pending_syncs.fetch_add(pages.len(), Ordering::Relaxed);
                }

                Ok(result)
            }
            DurabilityLevel::Durable => {
                // Full durability - sync to disk and WAL
                result.sync_performed = true;
                result.wal_written = self.wal_enabled;

                // In a real implementation, this would perform actual fsync
                self.perform_sync(pages)?;

                if self.wal_enabled {
                    self.write_wal_entries(pages)?;
                }

                self.pending_syncs.fetch_add(pages.len(), Ordering::Relaxed);
                Ok(result)
            }
        }
    }

    /// Checks if pages should be synced based on fsync policy
    fn should_sync_pages(&self, pages: &[(PageId, Vec<u8>)]) -> IoResult<bool> {
        match self.fsync_policy {
            FsyncPolicy::Never => Ok(false),
            FsyncPolicy::OnFlush => Ok(true),
            FsyncPolicy::PerWrite => Ok(true),
            FsyncPolicy::Periodic(_duration) => {
                // In a real implementation, this would check timing against the duration
                Ok(pages.len() > 10) // Simple heuristic for example
            }
        }
    }

    /// Performs actual sync operation
    fn perform_sync(&self, pages: &[(PageId, Vec<u8>)]) -> IoResult<()> {
        // In a real implementation, this would:
        // 1. Group pages by file
        // 2. Call fsync() on each file descriptor
        // 3. Handle any sync errors appropriately

        // For this example, we'll just simulate the operation
        if pages.is_empty() {
            return Err(IoError::new(ErrorKind::InvalidInput, "No pages to sync"));
        }

        Ok(())
    }

    /// Writes WAL entries for the pages
    fn write_wal_entries(&self, _pages: &[(PageId, Vec<u8>)]) -> IoResult<()> {
        if !self.wal_enabled {
            return Ok(());
        }

        // In a real implementation, this would:
        // 1. Create WAL entries for each page
        // 2. Write them to the WAL file
        // 3. Ensure WAL is synced before data pages

        // For this example, we'll just simulate
        Ok(())
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

    #[test]
    fn test_durability_manager_creation() {
        let manager = DurabilityManager::new(FsyncPolicy::OnFlush, DurabilityLevel::Sync, true);

        assert_eq!(*manager.fsync_policy(), FsyncPolicy::OnFlush);
        assert_eq!(*manager.durability_level(), DurabilityLevel::Sync);
        assert!(manager.is_wal_enabled());
        assert_eq!(manager.pending_syncs(), 0);
    }

    #[test]
    fn test_apply_durability_none() {
        let manager = DurabilityManager::new(FsyncPolicy::Never, DurabilityLevel::None, false);

        let pages = vec![(1, vec![1, 2, 3, 4])];
        let result = manager.apply_durability(&pages).unwrap();

        assert!(!result.sync_performed);
        assert!(!result.wal_written);
        assert_eq!(result.pages_synced, 1);
        assert_eq!(result.durability_level, DurabilityLevel::None);
    }

    #[test]
    fn test_apply_durability_buffer() {
        let manager = DurabilityManager::new(FsyncPolicy::Never, DurabilityLevel::Buffer, true);

        let pages = vec![(1, vec![1, 2, 3, 4]), (2, vec![5, 6, 7, 8])];
        let result = manager.apply_durability(&pages).unwrap();

        assert!(!result.sync_performed);
        assert!(result.wal_written);
        assert_eq!(result.pages_synced, 2);
        assert_eq!(result.durability_level, DurabilityLevel::Buffer);
    }

    #[test]
    fn test_apply_durability_sync() {
        let manager = DurabilityManager::new(FsyncPolicy::OnFlush, DurabilityLevel::Sync, true);

        let pages = vec![(1, vec![1, 2, 3, 4])];
        let result = manager.apply_durability(&pages).unwrap();

        assert!(result.sync_performed);
        assert!(result.wal_written);
        assert_eq!(result.pages_synced, 1);
        assert_eq!(result.durability_level, DurabilityLevel::Sync);
        assert!(manager.pending_syncs() > 0);
    }

    #[test]
    fn test_apply_durability_durable() {
        let manager = DurabilityManager::new(FsyncPolicy::OnFlush, DurabilityLevel::Durable, true);

        let pages = vec![(1, vec![1, 2, 3, 4])];
        let result = manager.apply_durability(&pages).unwrap();

        assert!(result.sync_performed);
        assert!(result.wal_written);
        assert_eq!(result.pages_synced, 1);
        assert_eq!(result.durability_level, DurabilityLevel::Durable);
    }

    #[test]
    fn test_apply_durability_empty_pages() {
        let manager = DurabilityManager::new(FsyncPolicy::OnFlush, DurabilityLevel::Durable, true);

        let pages = vec![];
        let result = manager.apply_durability(&pages).unwrap();

        assert!(!result.sync_performed);
        assert!(!result.wal_written);
        assert_eq!(result.pages_synced, 0);
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

    #[test]
    fn test_pending_syncs_tracking() {
        let manager = DurabilityManager::new(FsyncPolicy::OnFlush, DurabilityLevel::Sync, false);

        assert_eq!(manager.pending_syncs(), 0);

        // Apply durability to some pages
        let pages = vec![(1, vec![1, 2, 3, 4]), (2, vec![5, 6, 7, 8])];
        manager.apply_durability(&pages).unwrap();

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
