use std::sync::atomic::{AtomicU64, AtomicUsize};

/// Live performance metrics with enhanced monitoring
#[derive(Debug, Default)]
pub struct LiveMetrics {
    // I/O Performance (Enhanced)
    pub io_latency_sum: AtomicU64,
    pub io_count: AtomicU64,
    pub io_throughput_bytes: AtomicU64,
    pub io_queue_depth: AtomicUsize,
    pub io_utilization: AtomicU64,
    pub read_ops_count: AtomicU64,
    pub write_ops_count: AtomicU64,
    pub batch_ops_count: AtomicU64,
    pub concurrent_ops_count: AtomicUsize,

    // Latency Distribution
    pub latency_p50: AtomicU64,
    pub latency_p95: AtomicU64,
    pub latency_p99: AtomicU64,
    pub latency_max: AtomicU64,

    // Cache Performance (Enhanced)
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub cache_evictions: AtomicU64,
    pub prefetch_hits: AtomicU64,
    pub cache_memory_usage: AtomicUsize,
    pub hot_cache_hits: AtomicU64,
    pub warm_cache_hits: AtomicU64,
    pub cold_cache_hits: AtomicU64,
    pub cache_promotions: AtomicU64,
    pub cache_demotions: AtomicU64,

    // Write Performance (Enhanced)
    pub write_buffer_utilization: AtomicU64,
    pub flush_count: AtomicU64,
    pub write_amplification: AtomicU64,
    pub compression_ratio: AtomicU64,
    pub coalesced_writes: AtomicU64,
    pub total_bytes_written: AtomicU64,
    pub total_bytes_compressed: AtomicU64,

    // System Resources (Enhanced)
    pub memory_usage: AtomicUsize,
    pub cpu_usage: AtomicU64,
    pub disk_utilization: AtomicU64,
    pub network_usage: AtomicU64,
    pub file_descriptors_used: AtomicUsize,

    // Error Tracking (Enhanced)
    pub error_count: AtomicU64,
    pub retry_count: AtomicU64,
    pub timeout_count: AtomicU64,
    pub corruption_count: AtomicU64,
    pub recovery_count: AtomicU64,

    // Performance Counters
    pub transactions_per_second: AtomicU64,
    pub pages_per_second: AtomicU64,
    pub bytes_per_second: AtomicU64,

    // Health Indicators
    pub health_score: AtomicU64, // 0-100 health score
    pub uptime_seconds: AtomicU64,
    pub last_checkpoint: AtomicU64,
}