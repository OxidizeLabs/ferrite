//! Configuration for the Async Disk Manager
//! 
//! This module contains the configuration structures and enums for the async disk manager.

use std::time::Duration;
use crate::common::config::PageId;
use crate::storage::disk::async_disk::compression::CompressionAlgorithm;

/// Configuration for the disk manager with optimizations
#[derive(Debug, Clone)]
pub struct DiskManagerConfig {
    // I/O Configuration
    pub io_threads: usize,
    pub max_concurrent_ops: usize,
    pub batch_size: usize,
    pub direct_io: bool,

    // Cache Configuration
    pub cache_size_mb: usize,
    pub hot_cache_ratio: f64,
    pub warm_cache_ratio: f64,
    pub prefetch_enabled: bool,
    pub prefetch_distance: usize,

    // Write Configuration
    pub write_buffer_size_mb: usize,
    pub flush_threshold_pages: usize,
    pub flush_interval_ms: u64,
    pub compression_enabled: bool,

    // Performance Configuration
    pub metrics_enabled: bool,
    pub detailed_metrics: bool,
    pub numa_aware: bool,
    pub cpu_affinity: Option<Vec<usize>>,

    // Durability Configuration
    pub fsync_policy: FsyncPolicy,
    pub durability_level: DurabilityLevel,
    pub wal_enabled: bool,

    // Advanced Performance Options
    pub compression_algorithm: CompressionAlgorithm,
    pub simd_optimizations: bool,
    pub numa_node_id: Option<usize>,
    pub work_stealing_enabled: bool,
    pub ml_prefetching: bool,
    pub zero_copy_io: bool,
    pub adaptive_algorithms: bool,
    pub memory_pool_size_mb: usize,
    pub parallel_io_degree: usize,
    pub cpu_cache_optimization: bool,
    pub lock_free_structures: bool,
    pub vectorized_operations: bool,
    pub hot_cold_separation: bool,
    pub deduplication_enabled: bool,
    pub compression_level: u32,
}

/// File synchronization policies
#[derive(Debug, Clone, PartialEq)]
pub enum FsyncPolicy {
    Never,
    OnFlush,
    PerWrite,
    Periodic(Duration),
}

/// Durability levels for writes
#[derive(Debug, Clone, PartialEq)]
pub enum DurabilityLevel {
    None,        // No durability guarantees
    Buffer,      // Buffered writes
    Sync,        // Synchronous writes
    Durable,     // Guaranteed durability
}

/// Priority levels for I/O operations
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum IOPriority {
    Low = 1,
    Normal = 2,
    High = 3,
    Critical = 4,
}

/// Types of I/O operations
#[derive(Debug, Clone)]
pub enum IOOperationType {
    Read { page_id: PageId },
    Write { page_id: PageId, data: Vec<u8> },
    BatchRead { page_ids: Vec<PageId> },
    BatchWrite { pages: Vec<(PageId, Vec<u8>)> },
    Flush,
    Sync,
}

/// Cache eviction policies
#[derive(Debug, Clone, PartialEq)]
pub enum EvictionPolicy {
    LRU,    // Least Recently Used
    LFU,    // Least Frequently Used
    FIFO,   // First In, First Out
    ARC,    // Adaptive Replacement Cache
}

impl Default for DiskManagerConfig {
    fn default() -> Self {
        Self {
            io_threads: 4,
            max_concurrent_ops: 1000,
            batch_size: 64,
            direct_io: false,

            cache_size_mb: 512,
            hot_cache_ratio: 0.1,
            warm_cache_ratio: 0.3,
            prefetch_enabled: true,
            prefetch_distance: 8,

            write_buffer_size_mb: 64,
            flush_threshold_pages: 128,
            flush_interval_ms: 100,
            compression_enabled: false,

            metrics_enabled: true,
            detailed_metrics: false,
            numa_aware: false,
            cpu_affinity: None,

            fsync_policy: FsyncPolicy::OnFlush,
            durability_level: DurabilityLevel::Sync,
            wal_enabled: true,

            // Advanced Performance Options
            compression_algorithm: CompressionAlgorithm::None,
            simd_optimizations: true,
            numa_node_id: None,
            work_stealing_enabled: true,
            ml_prefetching: true,
            zero_copy_io: false, // Disabled by default for compatibility
            adaptive_algorithms: true,
            memory_pool_size_mb: 128,
            parallel_io_degree: num_cpus::get(),
            cpu_cache_optimization: true,
            lock_free_structures: true,
            vectorized_operations: true,
            hot_cold_separation: true,
            deduplication_enabled: false, // Expensive operation, disabled by default
            compression_level: 6, // Balanced compression level
        }
    }
}