//! # Live Metrics
//!
//! This module defines the `LiveMetrics` struct, which serves as the "hot storage" for all performance counters.
//!
//! ## Design
//!
//! - **Lock-Free**: Uses `std::sync::atomic` types exclusively. This allows multiple threads to update metrics
//!   concurrently without contention or mutex overhead.
//! - **Performance**: Designed for minimal impact on the critical I/O path.
//! - **Histogram Support**: Includes a fixed-size atomic array `latency_buckets` to approximate latency distributions
//!   without heavy data structures.
//!
//! This struct is typically wrapped in an `Arc` and shared across the system.

use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::time::Instant;

/// Live performance metrics with enhanced monitoring
#[derive(Debug)]
pub struct LiveMetrics {
    // I/O Performance
    pub io_latency_sum: AtomicU64,
    pub read_latency_sum: AtomicU64,
    pub write_latency_sum: AtomicU64,
    pub io_count: AtomicU64,
    pub io_throughput_bytes: AtomicU64,
    pub io_queue_depth: AtomicUsize,
    
    pub read_ops_count: AtomicU64,
    pub write_ops_count: AtomicU64,
    pub batch_ops_count: AtomicU64,
    
    // Latency Distribution (Histogram)
    // Buckets: <10us, <100us, <500us, <1ms, <5ms, <10ms, <50ms, <100ms, <500ms, <1s, >1s
    // 11 buckets
    pub latency_buckets: [AtomicU64; 11],
    pub latency_max: AtomicU64,
    pub latency_min: AtomicU64,

    // Cache Performance
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub cache_evictions: AtomicU64,
    pub cache_memory_usage: AtomicUsize,
    
    pub hot_cache_hits: AtomicU64,
    pub warm_cache_hits: AtomicU64,
    pub cold_cache_hits: AtomicU64,
    pub cache_promotions: AtomicU64,
    pub cache_demotions: AtomicU64,
    pub prefetch_hits: AtomicU64,

    // Write Performance
    pub write_buffer_utilization: AtomicU64,
    pub flush_count: AtomicU64,
    pub write_amplification: AtomicU64, // Scaled by 100
    pub compression_ratio: AtomicU64, // Scaled by 100
    pub total_bytes_written: AtomicU64,

    // System Resources
    pub memory_usage: AtomicUsize,
    pub cpu_usage: AtomicU64, // Scaled by 100
    pub disk_utilization: AtomicU64, // Scaled by 100
    
    // Error Tracking
    pub error_count: AtomicU64,
    pub retry_count: AtomicU64,
    
    // Performance Counters (calculated periodically)
    pub transactions_per_second: AtomicU64,
    pub pages_per_second: AtomicU64,
    pub bytes_per_second: AtomicU64,

    // Health Indicators
    pub health_score: AtomicU64, // 0-100 health score
    pub uptime_seconds: AtomicU64,
    
    // Timing
    pub start_time: Instant,
}

impl Default for LiveMetrics {
    fn default() -> Self {
        // Initialize array of atomics
        let latency_buckets = std::array::from_fn(|_| AtomicU64::new(0));

        Self {
            io_latency_sum: AtomicU64::new(0),
            read_latency_sum: AtomicU64::new(0),
            write_latency_sum: AtomicU64::new(0),
            io_count: AtomicU64::new(0),
            io_throughput_bytes: AtomicU64::new(0),
            io_queue_depth: AtomicUsize::new(0),
            read_ops_count: AtomicU64::new(0),
            write_ops_count: AtomicU64::new(0),
            batch_ops_count: AtomicU64::new(0),
            
            latency_buckets,
            latency_max: AtomicU64::new(0),
            latency_min: AtomicU64::new(u64::MAX),
            
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            cache_evictions: AtomicU64::new(0),
            cache_memory_usage: AtomicUsize::new(0),
            hot_cache_hits: AtomicU64::new(0),
            warm_cache_hits: AtomicU64::new(0),
            cold_cache_hits: AtomicU64::new(0),
            cache_promotions: AtomicU64::new(0),
            cache_demotions: AtomicU64::new(0),
            prefetch_hits: AtomicU64::new(0),
            
            write_buffer_utilization: AtomicU64::new(0),
            flush_count: AtomicU64::new(0),
            write_amplification: AtomicU64::new(0),
            compression_ratio: AtomicU64::new(0),
            total_bytes_written: AtomicU64::new(0),
            
            memory_usage: AtomicUsize::new(0),
            cpu_usage: AtomicU64::new(0),
            disk_utilization: AtomicU64::new(0),
            
            error_count: AtomicU64::new(0),
            retry_count: AtomicU64::new(0),
            
            transactions_per_second: AtomicU64::new(0),
            pages_per_second: AtomicU64::new(0),
            bytes_per_second: AtomicU64::new(0),
            
            health_score: AtomicU64::new(100),
            uptime_seconds: AtomicU64::new(0),
            
            start_time: Instant::now(),
        }
    }
}
