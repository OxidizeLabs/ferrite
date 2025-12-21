//! # Metrics Collector
//!
//! The `MetricsCollector` is the primary interface for recording performance data in the `AsyncDiskManager`.
//! It manages:
//!
//! 1.  **Real-time Recording**: Provides high-performance, thread-safe methods (like `record_read`, `record_write`)
//!     that update atomic counters in `LiveMetrics` without locking.
//! 2.  **Snapshot Generation**: Periodically captures the state of atomic counters into stable `MetricsSnapshot` objects.
//! 3.  **Historical Storage**: Maintains a history of snapshots in `MetricsStore` for trend analysis.
//! 4.  **Percentile Calculation**: Estimates latency percentiles (p50, p90, p99) from atomic histogram buckets.
//!
//! ## Usage
//!
//! ```rust,ignore
//! let collector = MetricsCollector::new(&config);
//!
//! // Hot path: record an event (wait-free)
//! collector.record_read(latency_ns, bytes, true);
//!
//! // Cold path: generate a report
//! let snapshot = collector.create_metrics_snapshot();
//! println!("Average Latency: {} ns", snapshot.read_latency_avg_ns);
//! ```

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::storage::disk::async_disk::config::DiskManagerConfig;
use crate::storage::disk::async_disk::metrics::live_metrics::LiveMetrics;
use crate::storage::disk::async_disk::metrics::snapshot::MetricsSnapshot;

/// Enhanced historical metrics storage with aggregation
#[derive(Debug)]
pub struct MetricsStore {
    pub snapshots: VecDeque<MetricsSnapshot>,
    pub max_snapshots: usize,
    pub snapshot_interval: Duration,
    pub aggregated_hourly: VecDeque<AggregatedMetrics>,
    pub aggregated_daily: VecDeque<AggregatedMetrics>,
    pub last_snapshot_time: Instant,
    pub start_time: Instant,
}

/// Aggregated metrics for trend analysis
#[derive(Debug, Clone)]
pub struct AggregatedMetrics {
    pub timestamp: Instant,
    pub duration: Duration,

    // Aggregated I/O metrics
    pub avg_latency_ns: u64,
    pub max_latency_ns: u64,
    pub total_operations: u64,
    pub total_throughput_mb: f64,

    // Aggregated cache metrics
    pub avg_hit_ratio: f64,
    pub total_cache_operations: u64,
    pub cache_efficiency_score: f64,

    // Aggregated write metrics
    pub avg_compression_ratio: f64,
    pub total_flushes: u64,
    pub avg_write_amplification: f64,

    // Health metrics
    pub avg_health_score: f64,
    pub error_rate: f64,
    pub availability_percentage: f64,
}

/// Comprehensive metrics collection system
#[derive(Debug)]
pub struct MetricsCollector {
    // Real-time metrics
    pub live_metrics: Arc<LiveMetrics>,

    // Historical data
    pub metrics_store: Arc<RwLock<MetricsStore>>,
}

// Latency thresholds for health score calculation
const LATENCY_THRESHOLD_HIGH_NS: u64 = 50_000_000; // 50ms
const LATENCY_THRESHOLD_MED_NS: u64 = 10_000_000; // 10ms

impl MetricsCollector {
    /// Creates a new enhanced metrics collector
    pub fn new(_config: &DiskManagerConfig) -> Self {
        let live_metrics = Arc::new(LiveMetrics::default());

        let now = Instant::now();
        let metrics_store = Arc::new(RwLock::new(MetricsStore {
            snapshots: VecDeque::new(),
            max_snapshots: 1000,
            snapshot_interval: Duration::from_secs(60),
            aggregated_hourly: VecDeque::new(),
            aggregated_daily: VecDeque::new(),
            last_snapshot_time: now,
            start_time: now,
        }));

        Self {
            live_metrics,
            metrics_store,
        }
    }

    /// Records read operation metrics
    pub fn record_read(&self, latency_ns: u64, bytes: u64, success: bool) {
        self.live_metrics
            .read_ops_count
            .fetch_add(1, Ordering::Relaxed);
        self.live_metrics
            .read_latency_sum
            .fetch_add(latency_ns, Ordering::Relaxed);
        self.live_metrics
            .io_latency_sum
            .fetch_add(latency_ns, Ordering::Relaxed);
        self.live_metrics.io_count.fetch_add(1, Ordering::Relaxed);
        self.live_metrics
            .io_throughput_bytes
            .fetch_add(bytes, Ordering::Relaxed);

        // Update latency distribution
        self.update_latency_distribution(latency_ns);

        if !success {
            self.live_metrics
                .error_count
                .fetch_add(1, Ordering::Relaxed);
        }

        // Update performance counters periodically
        self.update_performance_counters();
    }

    /// Records write operation metrics
    pub fn record_write(&self, latency_ns: u64, bytes: u64) {
        self.live_metrics
            .write_ops_count
            .fetch_add(1, Ordering::Relaxed);
        self.live_metrics
            .write_latency_sum
            .fetch_add(latency_ns, Ordering::Relaxed);
        self.live_metrics
            .io_latency_sum
            .fetch_add(latency_ns, Ordering::Relaxed);
        self.live_metrics.io_count.fetch_add(1, Ordering::Relaxed);
        self.live_metrics
            .io_throughput_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        self.live_metrics
            .total_bytes_written
            .fetch_add(bytes, Ordering::Relaxed);

        // Update latency distribution
        self.update_latency_distribution(latency_ns);

        // Update performance counters periodically
        self.update_performance_counters();
    }

    /// Records batch operation metrics
    pub fn record_batch_operation(
        &self,
        operation_count: usize,
        total_latency_ns: u64,
        total_bytes: u64,
    ) {
        self.live_metrics
            .batch_ops_count
            .fetch_add(1, Ordering::Relaxed);
        self.live_metrics
            .io_count
            .fetch_add(operation_count as u64, Ordering::Relaxed);
        self.live_metrics
            .io_latency_sum
            .fetch_add(total_latency_ns, Ordering::Relaxed);
        self.live_metrics
            .io_throughput_bytes
            .fetch_add(total_bytes, Ordering::Relaxed);
    }

    /// Records batch read operation metrics
    pub fn record_batch_read(
        &self,
        operation_count: usize,
        total_latency_ns: u64,
        total_bytes: u64,
    ) {
        self.live_metrics
            .batch_ops_count
            .fetch_add(1, Ordering::Relaxed);
        self.live_metrics
            .read_ops_count
            .fetch_add(operation_count as u64, Ordering::Relaxed);
        self.live_metrics
            .read_latency_sum
            .fetch_add(total_latency_ns, Ordering::Relaxed);
        self.live_metrics
            .io_latency_sum
            .fetch_add(total_latency_ns, Ordering::Relaxed);
        self.live_metrics
            .io_count
            .fetch_add(operation_count as u64, Ordering::Relaxed);
        self.live_metrics
            .io_throughput_bytes
            .fetch_add(total_bytes, Ordering::Relaxed);
    }

    /// Records batch write operation metrics
    pub fn record_batch_write(
        &self,
        operation_count: usize,
        total_latency_ns: u64,
        total_bytes: u64,
    ) {
        self.live_metrics
            .batch_ops_count
            .fetch_add(1, Ordering::Relaxed);
        self.live_metrics
            .write_ops_count
            .fetch_add(operation_count as u64, Ordering::Relaxed);
        self.live_metrics
            .write_latency_sum
            .fetch_add(total_latency_ns, Ordering::Relaxed);
        self.live_metrics
            .io_latency_sum
            .fetch_add(total_latency_ns, Ordering::Relaxed);
        self.live_metrics
            .io_count
            .fetch_add(operation_count as u64, Ordering::Relaxed);
        self.live_metrics
            .io_throughput_bytes
            .fetch_add(total_bytes, Ordering::Relaxed);
        self.live_metrics
            .total_bytes_written
            .fetch_add(total_bytes, Ordering::Relaxed);
    }

    /// Records flush operation metrics
    pub fn record_flush(&self, latency_ns: u64, pages_flushed: usize) {
        self.live_metrics
            .flush_count
            .fetch_add(1, Ordering::Relaxed);
        self.live_metrics
            .io_latency_sum
            .fetch_add(latency_ns, Ordering::Relaxed);
        self.live_metrics
            .io_count
            .fetch_add(pages_flushed as u64, Ordering::Relaxed);

        // Update performance counters
        self.update_performance_counters();
    }

    /// Records cache operation metrics
    pub fn record_cache_operation(&self, cache_level: &str, hit: bool) {
        if hit {
            match cache_level {
                "hot" => {
                    self.live_metrics
                        .hot_cache_hits
                        .fetch_add(1, Ordering::Relaxed);
                    self.live_metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                }
                "warm" => {
                    self.live_metrics
                        .warm_cache_hits
                        .fetch_add(1, Ordering::Relaxed);
                    self.live_metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                }
                "cold" => {
                    self.live_metrics
                        .cold_cache_hits
                        .fetch_add(1, Ordering::Relaxed);
                    self.live_metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
        } else {
            // Record cache miss
            self.live_metrics
                .cache_misses
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records cache migration metrics
    pub fn record_cache_migration(&self, promotion: bool) {
        if promotion {
            self.live_metrics
                .cache_promotions
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.live_metrics
                .cache_demotions
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Updates latency distribution metrics
    pub fn update_latency_distribution(&self, latency_ns: u64) {
        // Update max latency
        let current_max = self.live_metrics.latency_max.load(Ordering::Relaxed);
        if latency_ns > current_max {
            self.live_metrics
                .latency_max
                .store(latency_ns, Ordering::Relaxed);
        }

        // Update min latency
        let current_min = self.live_metrics.latency_min.load(Ordering::Relaxed);
        if latency_ns < current_min {
            self.live_metrics
                .latency_min
                .store(latency_ns, Ordering::Relaxed);
        }

        // Update buckets
        let bucket_idx = match latency_ns {
            0..=10_000 => 0,              // < 10us
            10_001..=100_000 => 1,        // < 100us
            100_001..=500_000 => 2,       // < 500us
            500_001..=1_000_000 => 3,     // < 1ms
            1_000_001..=5_000_000 => 4,   // < 5ms
            5_000_001..=10_000_000 => 5,  // < 10ms
            10_000_001..=50_000_000 => 6, // < 50ms
            50_000_001..=100_000_000 => 7,// < 100ms
            100_000_001..=500_000_000 => 8,// < 500ms
            500_000_001..=1_000_000_000 => 9,// < 1s
            _ => 10,                      // > 1s
        };

        if bucket_idx < self.live_metrics.latency_buckets.len() {
            self.live_metrics.latency_buckets[bucket_idx].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Updates performance counters
    pub fn update_performance_counters(&self) {
        // Calculate time-based metrics
        let elapsed_secs = self.live_metrics.start_time.elapsed().as_secs();
        if elapsed_secs > 0 {
            // Calculate throughput per second
            let total_bytes = self
                .live_metrics
                .io_throughput_bytes
                .load(Ordering::Relaxed);
            let bytes_per_sec = total_bytes / elapsed_secs;
            self.live_metrics
                .bytes_per_second
                .store(bytes_per_sec, Ordering::Relaxed);

            // Calculate operations per second
            let total_ops = self.live_metrics.io_count.load(Ordering::Relaxed);
            let ops_per_sec = total_ops / elapsed_secs;
            self.live_metrics
                .pages_per_second
                .store(ops_per_sec, Ordering::Relaxed);

            // Calculate transaction rate (using write ops as proxy for transactions)
            let write_ops = self.live_metrics.write_ops_count.load(Ordering::Relaxed);
            let transactions_per_sec = write_ops / elapsed_secs;
            self.live_metrics
                .transactions_per_second
                .store(transactions_per_sec, Ordering::Relaxed);

            // Update uptime
            self.live_metrics
                .uptime_seconds
                .store(elapsed_secs, Ordering::Relaxed);
        }
    }

    /// Resets metrics for a new measurement period
    pub fn reset_metrics(&self) {
        // Reset cumulative counters but keep configuration and structural metrics
        self.live_metrics.io_latency_sum.store(0, Ordering::Relaxed);
        self.live_metrics
            .read_latency_sum
            .store(0, Ordering::Relaxed);
        self.live_metrics
            .write_latency_sum
            .store(0, Ordering::Relaxed);
        self.live_metrics.io_count.store(0, Ordering::Relaxed);
        self.live_metrics
            .io_throughput_bytes
            .store(0, Ordering::Relaxed);
        self.live_metrics.read_ops_count.store(0, Ordering::Relaxed);
        self.live_metrics
            .write_ops_count
            .store(0, Ordering::Relaxed);
        self.live_metrics
            .batch_ops_count
            .store(0, Ordering::Relaxed);
        self.live_metrics.flush_count.store(0, Ordering::Relaxed);
        self.live_metrics
            .total_bytes_written
            .store(0, Ordering::Relaxed);
        self.live_metrics.error_count.store(0, Ordering::Relaxed);
        self.live_metrics.retry_count.store(0, Ordering::Relaxed);
        self.live_metrics.latency_max.store(0, Ordering::Relaxed);
        self.live_metrics.latency_min.store(u64::MAX, Ordering::Relaxed);
        
        for bucket in &self.live_metrics.latency_buckets {
            bucket.store(0, Ordering::Relaxed);
        }

        // Reset the start time for the new measurement period
        // Note: This is not atomic, but it's acceptable for metrics collection
    }

    /// Calculates the overall health score
    pub fn calculate_health_score(&self) -> u64 {
        let mut score = 100;

        // Penalize for high latency
        let avg_latency = if self.live_metrics.io_count.load(Ordering::Relaxed) > 0 {
            self.live_metrics.io_latency_sum.load(Ordering::Relaxed)
                / self.live_metrics.io_count.load(Ordering::Relaxed)
        } else {
            0
        };

        if avg_latency > LATENCY_THRESHOLD_HIGH_NS {
            score -= 20;
        } else if avg_latency > LATENCY_THRESHOLD_MED_NS {
            score -= 10;
        }

        // Penalize for low cache hit ratio
        let cache_hits = self.live_metrics.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.live_metrics.cache_misses.load(Ordering::Relaxed);
        let total_cache_ops = cache_hits + cache_misses;

        if total_cache_ops > 0 {
            let hit_ratio = cache_hits as f64 / total_cache_ops as f64;
            if hit_ratio < 0.5 {
                score -= 20;
            } else if hit_ratio < 0.8 {
                score -= 10;
            }
        }

        // Penalize for errors
        let error_count = self.live_metrics.error_count.load(Ordering::Relaxed);
        if error_count > 100 {
            score -= 30;
        } else if error_count > 10 {
            score -= 15;
        } else if error_count > 0 {
            score -= 5;
        }

        // Ensure score is between 0 and 100
        score = score.min(100);

        // Update the health score
        self.live_metrics
            .health_score
            .store(score, Ordering::Relaxed);

        score
    }

    /// Gets the live metrics
    pub fn get_live_metrics(&self) -> &LiveMetrics {
        &self.live_metrics
    }

    /// Helper to get approximate percentile latency
    fn get_percentile_latency(live_metrics: &LiveMetrics, percentile: f64) -> u64 {
        let total_samples = live_metrics.io_count.load(Ordering::Relaxed);
        if total_samples == 0 {
            return 0;
        }

        let target_count = (total_samples as f64 * percentile) as u64;
        let mut current_count = 0;
        
        let bucket_upper_bounds = [
            10_000, 100_000, 500_000, 1_000_000, 5_000_000, 10_000_000,
            50_000_000, 100_000_000, 500_000_000, 1_000_000_000, u64::MAX
        ];

        for (i, bucket) in live_metrics.latency_buckets.iter().enumerate() {
            current_count += bucket.load(Ordering::Relaxed);
            if current_count >= target_count {
                return bucket_upper_bounds[i];
            }
        }
        
        // If we fall through, return max recorded
        live_metrics.latency_max.load(Ordering::Relaxed)
    }

    /// Creates a metrics snapshot from live metrics
    pub fn create_metrics_snapshot(&self) -> MetricsSnapshot {
        // First, update performance counters to ensure time-based metrics are current
        self.update_performance_counters();
        Self::create_snapshot_from_live(&self.live_metrics)
    }

    /// Starts the metrics monitoring background task
    pub fn start_monitoring(&self) -> JoinHandle<()> {
        let metrics_store = self.metrics_store.clone();
        let live_metrics = self.live_metrics.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));

            loop {
                interval.tick().await;

                // Create a snapshot of current metrics
                let snapshot = Self::create_snapshot_from_live(&live_metrics);

                // Store the snapshot
                let mut store = metrics_store.write().await;
                store.snapshots.push_back(snapshot.clone());
                if store.snapshots.len() > store.max_snapshots {
                    store.snapshots.pop_front();
                }

                // Update aggregated metrics
                Self::update_aggregated_metrics(&mut store, &snapshot);
            }
        })
    }

    /// Creates a metrics snapshot from live metrics
    pub fn create_snapshot_from_live(live_metrics: &LiveMetrics) -> MetricsSnapshot {
        let read_ops = live_metrics.read_ops_count.load(Ordering::Relaxed);
        let write_ops = live_metrics.write_ops_count.load(Ordering::Relaxed);

        let read_latency_avg = if read_ops > 0 {
            live_metrics.read_latency_sum.load(Ordering::Relaxed) / read_ops
        } else {
            0
        };

        let write_latency_avg = if write_ops > 0 {
            live_metrics.write_latency_sum.load(Ordering::Relaxed) / write_ops
        } else {
            0
        };

        let cache_hits = live_metrics.cache_hits.load(Ordering::Relaxed);
        let cache_misses = live_metrics.cache_misses.load(Ordering::Relaxed);
        let total_cache_ops = cache_hits + cache_misses;
        let cache_hit_ratio = if total_cache_ops > 0 {
            cache_hits as f64 / total_cache_ops as f64
        } else {
            0.0
        };

        let prefetch_hits = live_metrics.prefetch_hits.load(Ordering::Relaxed);
        let prefetch_accuracy = if prefetch_hits > 0 {
            prefetch_hits as f64 / (prefetch_hits + cache_misses) as f64
        } else {
            0.0
        };

        // Use time-based calculations for throughput and frequency
        let elapsed_secs = live_metrics.start_time.elapsed().as_secs();
        let elapsed_secs_f64 = elapsed_secs.max(1) as f64; // Avoid division by zero

        let io_throughput_mb_per_sec = if elapsed_secs > 0 {
            live_metrics
                .io_throughput_bytes
                .load(Ordering::Relaxed) as f64
                / elapsed_secs_f64
                / 1_000_000.0
        } else {
            0.0
        };

        let flush_frequency_per_sec = if elapsed_secs > 0 {
            live_metrics.flush_count.load(Ordering::Relaxed) as f64 / elapsed_secs_f64
        } else {
            0.0
        };

        let error_rate_per_sec = if elapsed_secs > 0 {
            live_metrics.error_count.load(Ordering::Relaxed) as f64 / elapsed_secs_f64
        } else {
            0.0
        };
        
        // Calculate percentiles
        let p50 = Self::get_percentile_latency(live_metrics, 0.50);
        let p90 = Self::get_percentile_latency(live_metrics, 0.90);
        let p99 = Self::get_percentile_latency(live_metrics, 0.99);

        MetricsSnapshot {
            read_latency_avg_ns: read_latency_avg,
            write_latency_avg_ns: write_latency_avg,
            p50_latency_ns: p50,
            p90_latency_ns: p90,
            p99_latency_ns: p99,
            io_throughput_mb_per_sec,
            io_queue_depth: live_metrics.io_queue_depth.load(Ordering::Relaxed),
            cache_hit_ratio,
            prefetch_accuracy,
            cache_memory_usage_mb: live_metrics.cache_memory_usage.load(Ordering::Relaxed)
                / 1_000_000,
            write_buffer_utilization: live_metrics
                .write_buffer_utilization
                .load(Ordering::Relaxed) as f64
                / 100.0,
            compression_ratio: live_metrics.compression_ratio.load(Ordering::Relaxed) as f64
                / 100.0,
            flush_frequency_per_sec,
            error_rate_per_sec,
            retry_count: live_metrics.retry_count.load(Ordering::Relaxed),
        }
    }

    /// Updates aggregated metrics
    pub fn update_aggregated_metrics(_store: &mut MetricsStore, _snapshot: &MetricsSnapshot) {
        // This is a simplified implementation
        // In a real system, we would aggregate metrics over different time periods
        // and store them for trend analysis
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::async_disk::config::DiskManagerConfig;
    use std::sync::atomic::Ordering;

    fn create_test_collector() -> MetricsCollector {
        let config = DiskManagerConfig::default();
        MetricsCollector::new(&config)
    }

    #[test]
    fn test_separate_read_write_latency_tracking() {
        let collector = create_test_collector();

        // Record some read operations
        collector.record_read(1000, 4096, true); // 1μs, 4KB, success
        collector.record_read(2000, 8192, true); // 2μs, 8KB, success
        collector.record_read(3000, 4096, false); // 3μs, 4KB, failure

        // Record some write operations
        collector.record_write(5000, 4096); // 5μs, 4KB
        collector.record_write(7000, 8192); // 7μs, 8KB

        // Verify operation counts
        let read_ops = collector
            .live_metrics
            .read_ops_count
            .load(Ordering::Relaxed);
        let write_ops = collector
            .live_metrics
            .write_ops_count
            .load(Ordering::Relaxed);
        assert_eq!(read_ops, 3, "Should have recorded 3 read operations");
        assert_eq!(write_ops, 2, "Should have recorded 2 write operations");

        // Verify separate latency tracking
        let read_latency_sum = collector
            .live_metrics
            .read_latency_sum
            .load(Ordering::Relaxed);
        let write_latency_sum = collector
            .live_metrics
            .write_latency_sum
            .load(Ordering::Relaxed);
        assert_eq!(
            read_latency_sum, 6000,
            "Read latency sum should be 1000+2000+3000 = 6000"
        );
        assert_eq!(
            write_latency_sum, 12000,
            "Write latency sum should be 5000+7000 = 12000"
        );
    }
    
    #[test]
    fn test_latency_distribution() {
        let collector = create_test_collector();
        
        // Record latencies in different buckets
        // Bucket 0: < 10us
        collector.record_read(5_000, 4096, true); 
        // Bucket 1: < 100us
        collector.record_read(50_000, 4096, true);
        // Bucket 2: < 500us
        collector.record_read(200_000, 4096, true);
        // Bucket 3: < 1ms
        collector.record_read(600_000, 4096, true);
        
        // Verify buckets populated
        assert_eq!(collector.live_metrics.latency_buckets[0].load(Ordering::Relaxed), 1);
        assert_eq!(collector.live_metrics.latency_buckets[1].load(Ordering::Relaxed), 1);
        assert_eq!(collector.live_metrics.latency_buckets[2].load(Ordering::Relaxed), 1);
        assert_eq!(collector.live_metrics.latency_buckets[3].load(Ordering::Relaxed), 1);
        
        // Create snapshot to check percentiles
        let snapshot = collector.create_metrics_snapshot();
        
        // p50 should be around bucket 1 or 2 (4 items, index 2)
        // p50 latency < 500us
        assert!(snapshot.p50_latency_ns <= 500_000);
        
        // p99 should cover all items, so it should be bucket 3
        // p99 latency <= 1ms
        assert!(snapshot.p99_latency_ns <= 1_000_000);
        assert!(snapshot.p99_latency_ns >= 500_001);
    }
}
