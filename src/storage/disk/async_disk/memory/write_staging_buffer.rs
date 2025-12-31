//! # Write Staging Buffer
//!
//! This module provides `WriteStagingBuffer`, which handles in-memory storage of dirty pages
//! waiting to be flushed to disk. It optimizes memory usage through optional compression and
//! provides precise tracking of buffer occupancy.
//!
//! ## Architecture
//!
//! ```text
//!   WriteManager
//!   ═══════════════════════════════════════════════════════════════════════════
//!          │
//!          │ buffer_write(page_id, data)
//!          ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                      WriteStagingBuffer                                 │
//!   │                                                                         │
//!   │   ┌─────────────────────────────────────────────────────────────────┐   │
//!   │   │  CompressedWriteBuffer                                          │   │
//!   │   │                                                                 │   │
//!   │   │  ┌───────────────────────────────────────────────────────────┐  │   │
//!   │   │  │  buffer: HashMap<PageId, Vec<u8>>                         │  │   │
//!   │   │  │                                                           │  │   │
//!   │   │  │  ┌──────┬──────────────────────────────────────────┐      │  │   │
//!   │   │  │  │ PID  │ Compressed Page Data                     │      │  │   │
//!   │   │  │  ├──────┼──────────────────────────────────────────┤      │  │   │
//!   │   │  │  │  1   │ [LZ4 compressed bytes...]                │      │  │   │
//!   │   │  │  │  5   │ [LZ4 compressed bytes...]                │      │  │   │
//!   │   │  │  │  12  │ [LZ4 compressed bytes...]                │      │  │   │
//!   │   │  │  └──────┴──────────────────────────────────────────┘      │  │   │
//!   │   │  └───────────────────────────────────────────────────────────┘  │   │
//!   │   │                                                                 │   │
//!   │   │  Tracking:                                                      │   │
//!   │   │    dirty_pages:       AtomicUsize  ← Count of unique pages      │   │
//!   │   │    buffer_size_bytes: AtomicUsize  ← Total compressed size      │   │
//!   │   │    max_buffer_size:   usize        ← Capacity limit             │   │
//!   │   │    last_flush:        Instant      ← For staleness tracking     │   │
//!   │   │    compression_ratio: AtomicU64    ← Running ratio (×10000)     │   │
//!   │   └─────────────────────────────────────────────────────────────────┘   │
//!   └─────────────────────────────────────────────────────────────────────────┘
//!          │
//!          │ drain_buffer() → Vec<(PageId, Vec<u8>)>
//!          ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │              WriteManager → Disk I/O                                    │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Two-Level Caching Architecture
//!
//! ```text
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │  L1: BufferPoolManager                                                  │
//!   │  • Optimizes for CPU access speed                                       │
//!   │  • Uncompressed Page objects                                            │
//!   │  • Application threads work here                                        │
//!   └───────────────────────────────┬─────────────────────────────────────────┘
//!                                   │ evict / flush
//!                                   ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │  L2: WriteStagingBuffer (this component)                                │
//!   │  • Optimizes for I/O throughput                                         │
//!   │  • Compressed Vec<u8> data                                              │
//!   │  • Batches writes for disk                                              │
//!   │  • Application threads don't wait here                                  │
//!   └───────────────────────────────┬─────────────────────────────────────────┘
//!                                   │ flush to disk
//!                                   ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │  Disk                                                                   │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Write Flow
//!
//! ```text
//!   buffer_write(page_id, data)
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │ Step 1: COMPRESSION (if enabled)                                       │
//!   │                                                                        │
//!   │   compress_data(data, LZ4, level=6)                                    │
//!   │        │                                                               │
//!   │        └── Returns compressed bytes (or original if disabled)          │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │ Step 2: CAPACITY CHECK                                                 │
//!   │                                                                        │
//!   │   current_size + size_delta > max_buffer_size?                         │
//!   │        │                                                               │
//!   │        ├── Yes: Return Ok(false) ← Backpressure signal                 │
//!   │        │                                                               │
//!   │        └── No:  Continue to insert                                     │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │ Step 3: INSERT INTO BUFFER                                             │
//!   │                                                                        │
//!   │   buffer.insert(page_id, compressed_data)                              │
//!   │        │                                                               │
//!   │        ├── New page:      dirty_pages += 1                             │
//!   │        │                                                               │
//!   │        └── Existing page: dirty_pages unchanged (overwrite)            │
//!   │                                                                        │
//!   │   buffer_size_bytes += size_delta                                      │
//!   │   Return Ok(true) ← Successfully buffered                              │
//!   └────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Components
//!
//! | Component               | Description                                      |
//! |-------------------------|--------------------------------------------------|
//! | `WriteStagingBuffer`    | Public API wrapper around `CompressedWriteBuffer`|
//! | `CompressedWriteBuffer` | Internal storage with compression and tracking   |
//! | `WriteBufferStats`      | Statistics snapshot for monitoring               |
//!
//! ## Core Operations
//!
//! | Method                   | Description                                     |
//! |--------------------------|-------------------------------------------------|
//! | `new()`                  | Create with max size and compression flag       |
//! | `buffer_write()`         | Add page to buffer, returns false if full       |
//! | `drain_buffer()`         | Remove all pages, reset counters                |
//! | `is_empty()`             | Check if buffer has no pages                    |
//! | `dirty_page_count()`     | Get count of unique pages in buffer             |
//! | `buffer_size_bytes()`    | Get current compressed size                     |
//! | `max_buffer_size()`      | Get capacity limit                              |
//! | `time_since_last_flush()`| Get staleness for flush decisions               |
//! | `get_stats()`            | Get comprehensive statistics snapshot           |
//! | `decompress_data()`      | Decompress previously compressed data           |
//!
//! ## WriteBufferStats Fields
//!
//! | Field                  | Description                                      |
//! |------------------------|--------------------------------------------------|
//! | `dirty_pages`          | Number of unique pages in buffer                 |
//! | `buffer_size_bytes`    | Current memory usage (compressed)                |
//! | `max_buffer_size`      | Maximum allowed buffer size                      |
//! | `utilization_percent`  | `buffer_size / max_size * 100`                   |
//! | `compression_ratio`    | Ratio of compressed to original size             |
//! | `compression_enabled`  | Whether compression is active                    |
//! | `time_since_last_flush`| Duration since last `drain_buffer()`             |
//!
//! ## Compression Algorithms
//!
//! | Algorithm | Characteristics                                        |
//! |-----------|--------------------------------------------------------|
//! | `None`    | No compression, fastest, no size reduction             |
//! | `LZ4`     | Fast compression/decompression, moderate ratio         |
//! | `Zstd`    | Better ratio, configurable level, slower than LZ4      |
//!
//! Note: Current implementation stubs return uncompressed data. Real
//! implementations would use `lz4` and `zstd` crates.
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::storage::disk::async_disk::memory::write_staging_buffer::WriteStagingBuffer;
//!
//! // Create a 1MB buffer with compression enabled
//! let mut buffer = WriteStagingBuffer::new(1024 * 1024, true);
//!
//! // Buffer a page write
//! let page_id = 42;
//! let page_data = vec![0u8; 4096];
//!
//! match buffer.buffer_write(page_id, page_data)? {
//!     true => println!("Page buffered successfully"),
//!     false => println!("Buffer full - need to flush!"),
//! }
//!
//! // Check buffer status
//! let stats = buffer.get_stats();
//! println!("Dirty pages: {}", stats.dirty_pages);
//! println!("Utilization: {:.1}%", stats.utilization_percent);
//! println!("Compression ratio: {:.2}", stats.compression_ratio);
//!
//! // Overwriting same page doesn't increase dirty count
//! let new_data = vec![1u8; 4096];
//! buffer.buffer_write(page_id, new_data)?;
//! assert_eq!(buffer.dirty_page_count(), 1); // Still 1
//!
//! // Drain all pages for flushing
//! let pages_to_flush = buffer.drain_buffer();
//! for (pid, data) in pages_to_flush {
//!     disk_manager.write_page(pid, &data).await?;
//! }
//!
//! // Buffer is now empty
//! assert!(buffer.is_empty());
//! ```
//!
//! ## Backpressure Mechanism
//!
//! ```text
//!   Caller (WriteManager)
//!        │
//!        │ buffer_write(page_id, data)
//!        ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │ WriteStagingBuffer                                                     │
//!   │                                                                        │
//!   │   current_size + new_size > max_buffer_size?                           │
//!   │        │                                                               │
//!   │        │ Yes                           │ No                            │
//!   │        ▼                               ▼                               │
//!   │   ┌──────────────────┐           ┌──────────────────┐                  │
//!   │   │ Return Ok(false) │           │ Insert page      │                  │
//!   │   │ (buffer full)    │           │ Return Ok(true)  │                  │
//!   │   └──────────────────┘           └──────────────────┘                  │
//!   └────────────────────────────────────────────────────────────────────────┘
//!        │
//!        ▼
//!   WriteManager sees Ok(false):
//!        │
//!        └──► Triggers flush() before retrying
//!
//!   This mechanism prevents unbounded memory growth and ensures
//!   the system responds to memory pressure.
//! ```
//!
//! ## Thread Safety
//!
//! - `dirty_pages`: `AtomicUsize` for lock-free counting
//! - `buffer_size_bytes`: `AtomicUsize` for lock-free size tracking
//! - `compression_ratio`: `AtomicU64` for lock-free ratio updates
//! - The `HashMap` buffer requires external synchronization (provided by `WriteManager`'s `Mutex`)
//! - `WriteStagingBuffer` is designed to be wrapped in `Arc<Mutex<>>` by `WriteManager`

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
pub struct WriteStagingBuffer {
    buffer: CompressedWriteBuffer,
}

impl WriteStagingBuffer {
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
            self.compress_data(&data, CompressionAlgorithm::LZ4, 6)
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

    /// Compresses data using custom SIMD-accelerated algorithm.
    ///
    /// Note: Reserved for future SIMD-optimized compression implementation.
    #[allow(dead_code)]
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
        let buffer_manager = WriteStagingBuffer::new(1024 * 1024, true);
        assert_eq!(buffer_manager.max_buffer_size(), 1024 * 1024);
        assert!(buffer_manager.is_empty());
        assert_eq!(buffer_manager.dirty_page_count(), 0);
    }

    #[test]
    fn test_buffer_write_basic() {
        let mut buffer_manager = WriteStagingBuffer::new(1024 * 1024, false);
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
        let mut buffer_manager = WriteStagingBuffer::new(10, false); // Very small buffer
        let large_data = vec![0u8; 20]; // Larger than buffer

        let result = buffer_manager.buffer_write(1, large_data);
        assert!(result.is_ok());
        assert!(!result.unwrap()); // Should return false (buffer full)
    }

    #[test]
    fn test_buffer_drain() {
        let mut buffer_manager = WriteStagingBuffer::new(1024, false);

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
        let mut buffer_manager = WriteStagingBuffer::new(1024, true);
        buffer_manager.buffer_write(1, vec![1, 2, 3, 4]).unwrap();

        let stats = buffer_manager.get_stats();
        assert_eq!(stats.dirty_pages, 1);
        assert!(stats.buffer_size_bytes > 0);
        assert!(stats.utilization_percent > 0.0);
        assert!(stats.compression_enabled);
    }

    #[test]
    fn test_overwrite_same_page() {
        let mut buffer_manager = WriteStagingBuffer::new(1024, false);

        buffer_manager.buffer_write(1, vec![1, 2, 3, 4]).unwrap();
        assert_eq!(buffer_manager.dirty_page_count(), 1);

        // Overwrite same page
        buffer_manager.buffer_write(1, vec![5, 6, 7, 8]).unwrap();
        assert_eq!(buffer_manager.dirty_page_count(), 1); // Should still be 1
    }
}
