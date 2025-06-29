//! Write management system for the Async Disk Manager
//! 
//! This module contains the WriteManager and related components for efficient write buffering,
//! compression, and durability management.

use crate::common::config::PageId;
use crate::storage::disk::async_disk::compression::CompressionAlgorithm;
use crate::storage::disk::async_disk::config::{DiskManagerConfig, FsyncPolicy, DurabilityLevel};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Mutex};
use std::sync::atomic::{AtomicU64, AtomicUsize, AtomicBool, Ordering};
use std::io::Result as IoResult;

/// Statistics about the write buffer
#[derive(Debug)]
pub struct WriteBufferStats {
    pub dirty_pages: usize,
    pub buffer_size_bytes: usize,
    pub max_buffer_size: usize,
    pub utilization_percent: f64,
    pub compression_ratio: f64,
    pub compression_enabled: bool,
    pub time_since_last_flush: Duration,
}

/// Write buffer with compression
#[derive(Debug)]
pub struct CompressedWriteBuffer {
    pub buffer: HashMap<PageId, Vec<u8>>,
    pub dirty_pages: AtomicUsize,
    pub last_flush: Instant,
    pub compression_enabled: bool,
    pub buffer_size_bytes: AtomicUsize,
    pub max_buffer_size: usize,
    pub compression_ratio: AtomicU64,
}

/// Coordinates flush operations
#[derive(Debug)]
pub struct FlushCoordinator {
    pub flush_in_progress: AtomicBool,
    pub flush_threshold: usize,
    pub flush_interval: Duration,
    pub last_flush: Mutex<Instant>,
}

/// Engine for coalescing adjacent writes
#[derive(Debug)]
pub struct CoalescingEngine {
    pub pending_writes: HashMap<PageId, Vec<u8>>,
    pub coalesce_window: Duration,
    pub max_coalesce_size: usize,
}

/// Manages durability guarantees
#[derive(Debug)]
pub struct DurabilityManager {
    pub fsync_policy: FsyncPolicy,
    pub durability_level: DurabilityLevel,
    pub wal_enabled: bool,
    pub pending_syncs: AtomicUsize,
}



/// Advanced write management system
#[derive(Debug)]
pub struct WriteManager {
    // Write-ahead buffer with compression
    write_buffer: Arc<RwLock<CompressedWriteBuffer>>,

    // Async flush coordinator
    flush_coordinator: Arc<FlushCoordinator>,

    // Write coalescing engine
    coalescing_engine: Arc<RwLock<CoalescingEngine>>,

    // Durability guarantees
    durability_manager: Arc<DurabilityManager>,
}

impl WriteManager {
    /// Creates a new write manager
    pub fn new(config: &DiskManagerConfig) -> Self {
        let max_buffer_size = config.write_buffer_size_mb * 1024 * 1024; // Convert to bytes

        let write_buffer = Arc::new(RwLock::new(CompressedWriteBuffer {
            buffer: HashMap::new(),
            dirty_pages: AtomicUsize::new(0),
            last_flush: Instant::now(),
            compression_enabled: config.compression_enabled,
            buffer_size_bytes: AtomicUsize::new(0),
            max_buffer_size,
            compression_ratio: AtomicU64::new(10000), // 100% (no compression initially)
        }));

        let flush_coordinator = Arc::new(FlushCoordinator {
            flush_in_progress: AtomicBool::new(false),
            flush_threshold: config.flush_threshold_pages,
            flush_interval: Duration::from_millis(config.flush_interval_ms),
            last_flush: Mutex::new(Instant::now()),
        });

        let coalescing_engine = Arc::new(RwLock::new(CoalescingEngine {
            pending_writes: HashMap::new(),
            coalesce_window: Duration::from_millis(10),
            max_coalesce_size: 64,
        }));

        let durability_manager = Arc::new(DurabilityManager {
            fsync_policy: config.fsync_policy.clone(),
            durability_level: config.durability_level.clone(),
            wal_enabled: config.wal_enabled,
            pending_syncs: AtomicUsize::new(0),
        });

        Self {
            write_buffer,
            flush_coordinator,
            coalescing_engine,
            durability_manager,
        }
    }

    /// Adds a page to the write buffer with compression and coalescing
    pub async fn buffer_write(&self, page_id: PageId, data: Vec<u8>) -> IoResult<()> {
        // First, try to coalesce the write
        let coalesced_data = self.try_coalesce_write(page_id, data.clone()).await?;

        let mut buffer = self.write_buffer.write().await;

        // Check if we need to apply compression
        let final_data = if buffer.compression_enabled {
            // Use default compression algorithm and level for now
            self.compress_data(&coalesced_data, CompressionAlgorithm::Custom, 6)
        } else {
            coalesced_data
        };

        // Calculate size change for buffer size tracking
        let old_size = buffer.buffer.get(&page_id).map(|d| d.len()).unwrap_or(0);
        let new_size = final_data.len();
        let size_delta = if new_size > old_size { 
            new_size - old_size 
        } else { 
            0 
        };

        // Check if adding this write would exceed buffer capacity
        let current_size = buffer.buffer_size_bytes.load(Ordering::Relaxed);
        if current_size + size_delta > buffer.max_buffer_size {
            // Buffer is full, need to flush first
            drop(buffer); // Release lock before flush
            self.force_flush().await?;
            // Reacquire lock
            buffer = self.write_buffer.write().await;
        }

        // Add the write to buffer
        let was_new = !buffer.buffer.contains_key(&page_id);
        buffer.buffer.insert(page_id, final_data);
        buffer.buffer_size_bytes.fetch_add(size_delta, Ordering::Relaxed);

        if was_new {
            buffer.dirty_pages.fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Attempts to coalesce a write with pending writes
    async fn try_coalesce_write(&self, page_id: PageId, data: Vec<u8>) -> IoResult<Vec<u8>> {
        let mut coalescing = self.coalescing_engine.write().await;

        // Check for adjacent page writes that can be coalesced
        let mut coalesced_data = data.clone();
        
        // Simple coalescing logic - in a real system this would be more sophisticated
        if let Some(existing_data) = coalescing.pending_writes.get(&page_id) {
            // For demonstration, just use the newer data
            coalesced_data = data;
        }
        
        // Update pending writes
        coalescing.pending_writes.insert(page_id, coalesced_data.clone());
        
        Ok(coalesced_data)
    }

    /// Compresses data using the specified algorithm and level
    pub fn compress_data(&self, data: &[u8], algorithm: CompressionAlgorithm, level: u32) -> Vec<u8> {
        match algorithm {
            CompressionAlgorithm::None => data.to_vec(),
            CompressionAlgorithm::LZ4 => self.compress_lz4(data),
            CompressionAlgorithm::Zstd => self.compress_zstd(data, level),
            CompressionAlgorithm::Custom => self.compress_custom_simd(data),
        }
    }

    /// Compresses data using LZ4
    fn compress_lz4(&self, data: &[u8]) -> Vec<u8> {
        // In a real implementation, this would use the LZ4 compression library
        // For this example, we'll just return the original data
        data.to_vec()
    }

    /// Compresses data using Zstd
    fn compress_zstd(&self, data: &[u8], level: u32) -> Vec<u8> {
        // In a real implementation, this would use the Zstd compression library
        // For this example, we'll just return the original data
        data.to_vec()
    }

    /// Compresses data using custom SIMD-accelerated algorithm
    fn compress_custom_simd(&self, data: &[u8]) -> Vec<u8> {
        // In a real implementation, this would use SIMD instructions for compression
        // For this example, we'll just return the original data
        data.to_vec()
    }

    /// Decompresses data
    pub fn decompress_data(&self, compressed: &[u8]) -> Vec<u8> {
        // In a real implementation, this would detect the compression algorithm and decompress
        // For this example, we'll just return the original data
        compressed.to_vec()
    }

    /// Checks if a flush should be performed based on thresholds
    pub fn should_flush(&self) -> bool {
        let buffer = self.write_buffer.try_read().unwrap();
        let dirty_pages = buffer.dirty_pages.load(Ordering::Relaxed);
        let last_flush = buffer.last_flush;
        let elapsed = last_flush.elapsed();
        
        // Check if we've exceeded the flush threshold
        if dirty_pages >= self.flush_coordinator.flush_threshold {
            return true;
        }
        
        // Check if we've exceeded the flush interval
        if elapsed >= self.flush_coordinator.flush_interval {
            return true;
        }
        
        false
    }

    /// Forces a flush of the write buffer
    pub async fn force_flush(&self) -> IoResult<Vec<(PageId, Vec<u8>)>> {
        // Set flush in progress flag
        if self.flush_coordinator.flush_in_progress.compare_exchange(
            false, true, Ordering::SeqCst, Ordering::SeqCst
        ).is_err() {
            // Another flush is already in progress
            return Ok(Vec::new());
        }
        
        let result = self.flush_internal().await;
        
        // Reset flush in progress flag
        self.flush_coordinator.flush_in_progress.store(false, Ordering::SeqCst);
        
        result
    }

    /// Internal flush implementation
    async fn flush_internal(&self) -> IoResult<Vec<(PageId, Vec<u8>)>> {
        let mut buffer = self.write_buffer.write().await;
        
        // Get all dirty pages
        let mut pages_to_flush = Vec::new();
        for (page_id, data) in buffer.buffer.drain() {
            pages_to_flush.push((page_id, data));
        }
        
        // Reset buffer state
        buffer.dirty_pages.store(0, Ordering::Relaxed);
        buffer.buffer_size_bytes.store(0, Ordering::Relaxed);
        buffer.last_flush = Instant::now();
        
        // Update last flush time
        *self.flush_coordinator.last_flush.lock().await = Instant::now();
        
        Ok(pages_to_flush)
    }

    /// Flushes the write buffer
    pub async fn flush(&self) -> IoResult<Vec<(PageId, Vec<u8>)>> {
        if self.should_flush() {
            self.force_flush().await
        } else {
            Ok(Vec::new())
        }
    }

    /// Gets statistics about the write buffer
    pub fn get_buffer_stats(&self) -> WriteBufferStats {
        let buffer = self.write_buffer.try_read().unwrap();
        let dirty_pages = buffer.dirty_pages.load(Ordering::Relaxed);
        let buffer_size = buffer.buffer_size_bytes.load(Ordering::Relaxed);
        let max_size = buffer.max_buffer_size;
        let utilization = if max_size > 0 {
            (buffer_size as f64 / max_size as f64) * 100.0
        } else {
            0.0
        };
        let compression_ratio = buffer.compression_ratio.load(Ordering::Relaxed) as f64 / 10000.0;
        let time_since_last_flush = buffer.last_flush.elapsed();
        
        WriteBufferStats {
            dirty_pages,
            buffer_size_bytes: buffer_size,
            max_buffer_size: max_size,
            utilization_percent: utilization,
            compression_ratio,
            compression_enabled: buffer.compression_enabled,
            time_since_last_flush,
        }
    }

    /// Applies durability guarantees to flushed pages
    pub fn apply_durability(&self, _pages: &[(PageId, Vec<u8>)]) -> IoResult<bool> {
        // In a real implementation, this would apply the appropriate durability guarantees
        // based on the configured durability level and fsync policy
        
        match self.durability_manager.durability_level {
            DurabilityLevel::None => {
                // No durability guarantees
                Ok(false)
            },
            DurabilityLevel::Buffer => {
                // Buffer durability - data is in OS buffers
                Ok(false)
            },
            DurabilityLevel::Sync => {
                // Sync durability - data is synced to disk
                Ok(true)
            },
            DurabilityLevel::Durable => {
                // Durable - data is synced to disk and WAL is used
                Ok(true)
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::async_disk::config::DiskManagerConfig;

    #[tokio::test]
    async fn test_write_manager_basic() {
        let config = DiskManagerConfig::default();
        let write_manager = WriteManager::new(&config);
        
        // Test buffer_write
        let page_id = 1;
        let data = vec![1, 2, 3, 4];
        write_manager.buffer_write(page_id, data.clone()).await.unwrap();
        
        // Test get_buffer_stats
        let stats = write_manager.get_buffer_stats();
        assert_eq!(stats.dirty_pages, 1);
        
        // Test flush
        let flushed_pages = write_manager.force_flush().await.unwrap();
        assert_eq!(flushed_pages.len(), 1);
        assert_eq!(flushed_pages[0].0, page_id);
        
        // Test buffer is empty after flush
        let stats_after_flush = write_manager.get_buffer_stats();
        assert_eq!(stats_after_flush.dirty_pages, 0);
    }
}