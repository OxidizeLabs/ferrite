//! Write buffer management with compression support
//!
//! This module handles the core write buffering functionality including
//! compression, size tracking, and buffer capacity management.

use crate::common::config::PageId;
use crate::storage::disk::async_disk::compression::CompressionAlgorithm;
use std::collections::HashMap;
use std::io::Result as IoResult;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

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

/// Write buffer with compression capabilities
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

/// Manages write buffering with compression and size tracking
#[derive(Debug)]
pub struct BufferManager {
    buffer: CompressedWriteBuffer,
}

impl BufferManager {
    /// Creates a new buffer manager with the specified configuration
    pub fn new(max_buffer_size: usize, compression_enabled: bool) -> Self {
        Self {
            buffer: CompressedWriteBuffer {
                buffer: HashMap::new(),
                dirty_pages: AtomicUsize::new(0),
                last_flush: Instant::now(),
                compression_enabled,
                buffer_size_bytes: AtomicUsize::new(0),
                max_buffer_size,
                compression_ratio: AtomicU64::new(10000), // 100% (no compression initially)
            },
        }
    }

    /// Adds data to the write buffer, applying compression if enabled
    pub fn buffer_write(&mut self, page_id: PageId, data: Vec<u8>) -> IoResult<bool> {
        // Apply compression if enabled
        let final_data = if self.buffer.compression_enabled {
            self.compress_data(&data, CompressionAlgorithm::Custom, 6)
        } else {
            data
        };

        // Calculate size change for buffer size tracking
        let old_size = self
            .buffer
            .buffer
            .get(&page_id)
            .map(|d| d.len())
            .unwrap_or(0);
        let new_size = final_data.len();
        let size_delta = new_size.saturating_sub(old_size);

        // Check if adding this write would exceed buffer capacity
        let current_size = self.buffer.buffer_size_bytes.load(Ordering::Relaxed);
        if current_size + size_delta > self.buffer.max_buffer_size {
            return Ok(false); // Buffer full, needs flush
        }

        // Add the write to buffer
        let was_new = !self.buffer.buffer.contains_key(&page_id);
        self.buffer.buffer.insert(page_id, final_data);
        self.buffer
            .buffer_size_bytes
            .fetch_add(size_delta, Ordering::Relaxed);

        if was_new {
            self.buffer.dirty_pages.fetch_add(1, Ordering::Relaxed);
        }

        Ok(true)
    }

    /// Drains all buffered writes and resets buffer state
    pub fn drain_buffer(&mut self) -> Vec<(PageId, Vec<u8>)> {
        let pages = self.buffer.buffer.drain().collect();

        // Reset buffer state
        self.buffer.dirty_pages.store(0, Ordering::Relaxed);
        self.buffer.buffer_size_bytes.store(0, Ordering::Relaxed);
        self.buffer.last_flush = Instant::now();

        pages
    }

    /// Checks if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.buffer.buffer.is_empty()
    }

    /// Gets the number of dirty pages in the buffer
    pub fn dirty_page_count(&self) -> usize {
        self.buffer.dirty_pages.load(Ordering::Relaxed)
    }

    /// Gets the current buffer size in bytes
    pub fn buffer_size_bytes(&self) -> usize {
        self.buffer.buffer_size_bytes.load(Ordering::Relaxed)
    }

    /// Gets the maximum buffer size
    pub fn max_buffer_size(&self) -> usize {
        self.buffer.max_buffer_size
    }

    /// Gets the time since last flush
    pub fn time_since_last_flush(&self) -> Duration {
        self.buffer.last_flush.elapsed()
    }

    /// Gets comprehensive buffer statistics
    pub fn get_stats(&self) -> WriteBufferStats {
        let dirty_pages = self.buffer.dirty_pages.load(Ordering::Relaxed);
        let buffer_size = self.buffer.buffer_size_bytes.load(Ordering::Relaxed);
        let max_size = self.buffer.max_buffer_size;
        let utilization = if max_size > 0 {
            (buffer_size as f64 / max_size as f64) * 100.0
        } else {
            0.0
        };
        let compression_ratio =
            self.buffer.compression_ratio.load(Ordering::Relaxed) as f64 / 10000.0;
        let time_since_last_flush = self.buffer.last_flush.elapsed();

        WriteBufferStats {
            dirty_pages,
            buffer_size_bytes: buffer_size,
            max_buffer_size: max_size,
            utilization_percent: utilization,
            compression_ratio,
            compression_enabled: self.buffer.compression_enabled,
            time_since_last_flush,
        }
    }

    /// Compresses data using the specified algorithm and level
    fn compress_data(&self, data: &[u8], algorithm: CompressionAlgorithm, level: u32) -> Vec<u8> {
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
    fn compress_zstd(&self, data: &[u8], _level: u32) -> Vec<u8> {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_manager_creation() {
        let buffer_manager = BufferManager::new(1024 * 1024, true);
        assert_eq!(buffer_manager.max_buffer_size(), 1024 * 1024);
        assert!(buffer_manager.is_empty());
        assert_eq!(buffer_manager.dirty_page_count(), 0);
    }

    #[test]
    fn test_buffer_write_basic() {
        let mut buffer_manager = BufferManager::new(1024 * 1024, false);
        let data = vec![1, 2, 3, 4];

        let result = buffer_manager.buffer_write(1, data.clone());
        assert!(result.is_ok());
        assert!(result.unwrap());

        assert!(!buffer_manager.is_empty());
        assert_eq!(buffer_manager.dirty_page_count(), 1);
        assert_eq!(buffer_manager.buffer_size_bytes(), data.len());
    }

    #[test]
    fn test_buffer_full_rejection() {
        let mut buffer_manager = BufferManager::new(10, false); // Very small buffer
        let large_data = vec![0u8; 20]; // Larger than buffer

        let result = buffer_manager.buffer_write(1, large_data);
        assert!(result.is_ok());
        assert!(!result.unwrap()); // Should return false (buffer full)
    }

    #[test]
    fn test_buffer_drain() {
        let mut buffer_manager = BufferManager::new(1024, false);

        buffer_manager.buffer_write(1, vec![1, 2, 3, 4]).unwrap();
        buffer_manager.buffer_write(2, vec![5, 6, 7, 8]).unwrap();

        assert_eq!(buffer_manager.dirty_page_count(), 2);

        let drained = buffer_manager.drain_buffer();
        assert_eq!(drained.len(), 2);
        assert!(buffer_manager.is_empty());
        assert_eq!(buffer_manager.dirty_page_count(), 0);
        assert_eq!(buffer_manager.buffer_size_bytes(), 0);
    }

    #[test]
    fn test_buffer_stats() {
        let mut buffer_manager = BufferManager::new(1024, true);
        buffer_manager.buffer_write(1, vec![1, 2, 3, 4]).unwrap();

        let stats = buffer_manager.get_stats();
        assert_eq!(stats.dirty_pages, 1);
        assert!(stats.buffer_size_bytes > 0);
        assert!(stats.utilization_percent > 0.0);
        assert!(stats.compression_enabled);
    }

    #[test]
    fn test_overwrite_same_page() {
        let mut buffer_manager = BufferManager::new(1024, false);

        buffer_manager.buffer_write(1, vec![1, 2, 3, 4]).unwrap();
        assert_eq!(buffer_manager.dirty_page_count(), 1);

        // Overwrite same page
        buffer_manager.buffer_write(1, vec![5, 6, 7, 8]).unwrap();
        assert_eq!(buffer_manager.dirty_page_count(), 1); // Should still be 1
    }
}
