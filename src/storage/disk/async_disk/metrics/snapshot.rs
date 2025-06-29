/// Snapshot of performance metrics
#[derive(Debug, Default, Clone)]
pub struct MetricsSnapshot {
    // I/O Performance
    pub read_latency_avg_ns: u64,
    pub write_latency_avg_ns: u64,
    pub io_throughput_mb_per_sec: f64,
    pub io_queue_depth: usize,

    // Cache Performance
    pub cache_hit_ratio: f64,
    pub prefetch_accuracy: f64,
    pub cache_memory_usage_mb: usize,

    // Write Performance
    pub write_buffer_utilization: f64,
    pub compression_ratio: f64,
    pub flush_frequency_per_sec: f64,

    // Error Tracking
    pub error_rate_per_sec: f64,
    pub retry_count: u64,
}