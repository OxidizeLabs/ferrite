use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use log::{info, debug};

/// Performance metrics for database operations
#[derive(Debug, Default)]
pub struct PerformanceMetrics {
    // Insert operation metrics
    pub total_inserts: AtomicUsize,
    pub bulk_inserts: AtomicUsize,
    pub insert_latency_ns: AtomicU64,
    pub insert_throughput_rows_per_sec: AtomicU64,
    
    // Buffer pool metrics
    pub buffer_hits: AtomicUsize,
    pub buffer_misses: AtomicUsize,
    pub page_evictions: AtomicUsize,
    pub dirty_page_flushes: AtomicUsize,
    
    // Disk I/O metrics
    pub disk_reads: AtomicUsize,
    pub disk_writes: AtomicUsize,
    pub disk_read_latency_ns: AtomicU64,
    pub disk_write_latency_ns: AtomicU64,
    pub batch_operations: AtomicUsize,
    
    // Transaction metrics
    pub transactions_started: AtomicUsize,
    pub transactions_committed: AtomicUsize,
    pub transactions_aborted: AtomicUsize,
    
    // Lock metrics
    pub lock_acquisitions: AtomicUsize,
    pub lock_waits: AtomicUsize,
    pub lock_wait_time_ns: AtomicU64,
}

impl PerformanceMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an insert operation
    pub fn record_insert(&self, row_count: usize, duration: Duration, is_bulk: bool) {
        self.total_inserts.fetch_add(row_count, Ordering::Relaxed);
        if is_bulk {
            self.bulk_inserts.fetch_add(row_count, Ordering::Relaxed);
        }
        self.insert_latency_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        
        let throughput = if duration.as_secs_f64() > 0.0 {
            (row_count as f64 / duration.as_secs_f64()) as u64
        } else {
            0
        };
        self.insert_throughput_rows_per_sec.store(throughput, Ordering::Relaxed);
    }

    /// Record buffer pool hit/miss
    pub fn record_buffer_access(&self, is_hit: bool) {
        if is_hit {
            self.buffer_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.buffer_misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record page eviction
    pub fn record_page_eviction(&self) {
        self.page_evictions.fetch_add(1, Ordering::Relaxed);
    }

    /// Record dirty page flush
    pub fn record_dirty_page_flush(&self, count: usize) {
        self.dirty_page_flushes.fetch_add(count, Ordering::Relaxed);
    }

    /// Record disk I/O operation
    pub fn record_disk_io(&self, is_write: bool, duration: Duration, is_batch: bool) {
        if is_write {
            self.disk_writes.fetch_add(1, Ordering::Relaxed);
            self.disk_write_latency_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        } else {
            self.disk_reads.fetch_add(1, Ordering::Relaxed);
            self.disk_read_latency_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        }
        
        if is_batch {
            self.batch_operations.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record transaction event
    pub fn record_transaction(&self, event: TransactionEvent) {
        match event {
            TransactionEvent::Started => {
                self.transactions_started.fetch_add(1, Ordering::Relaxed);
            }
            TransactionEvent::Committed => {
                self.transactions_committed.fetch_add(1, Ordering::Relaxed);
            }
            TransactionEvent::Aborted => {
                self.transactions_aborted.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Record lock operation
    pub fn record_lock_operation(&self, wait_time: Option<Duration>) {
        self.lock_acquisitions.fetch_add(1, Ordering::Relaxed);
        if let Some(duration) = wait_time {
            self.lock_waits.fetch_add(1, Ordering::Relaxed);
            self.lock_wait_time_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        }
    }

    /// Get buffer pool hit ratio
    pub fn get_buffer_hit_ratio(&self) -> f64 {
        let hits = self.buffer_hits.load(Ordering::Relaxed);
        let misses = self.buffer_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        
        if total > 0 {
            hits as f64 / total as f64
        } else {
            0.0
        }
    }

    /// Get average insert latency in milliseconds
    pub fn get_avg_insert_latency_ms(&self) -> f64 {
        let total_ns = self.insert_latency_ns.load(Ordering::Relaxed);
        let total_inserts = self.total_inserts.load(Ordering::Relaxed);
        
        if total_inserts > 0 {
            (total_ns as f64 / total_inserts as f64) / 1_000_000.0
        } else {
            0.0
        }
    }

    /// Get current insert throughput
    pub fn get_insert_throughput(&self) -> u64 {
        self.insert_throughput_rows_per_sec.load(Ordering::Relaxed)
    }

    /// Get percentage of bulk inserts
    pub fn get_bulk_insert_percentage(&self) -> f64 {
        let total = self.total_inserts.load(Ordering::Relaxed);
        let bulk = self.bulk_inserts.load(Ordering::Relaxed);
        
        if total > 0 {
            (bulk as f64 / total as f64) * 100.0
        } else {
            0.0
        }
    }

    /// Log performance summary
    pub fn log_performance_summary(&self) {
        let total_inserts = self.total_inserts.load(Ordering::Relaxed);
        let bulk_inserts = self.bulk_inserts.load(Ordering::Relaxed);
        let hit_ratio = self.get_buffer_hit_ratio();
        let avg_latency = self.get_avg_insert_latency_ms();
        let throughput = self.get_insert_throughput();
        let bulk_percentage = self.get_bulk_insert_percentage();
        
        let disk_reads = self.disk_reads.load(Ordering::Relaxed);
        let disk_writes = self.disk_writes.load(Ordering::Relaxed);
        let batch_ops = self.batch_operations.load(Ordering::Relaxed);
        
        info!("=== TKDB Performance Summary ===");
        info!("Insert Performance:");
        info!("  Total Inserts: {}", total_inserts);
        info!("  Bulk Inserts: {} ({:.1}%)", bulk_inserts, bulk_percentage);
        info!("  Average Latency: {:.2}ms", avg_latency);
        info!("  Current Throughput: {} rows/sec", throughput);
        
        info!("Buffer Pool Performance:");
        info!("  Hit Ratio: {:.2}%", hit_ratio * 100.0);
        info!("  Page Evictions: {}", self.page_evictions.load(Ordering::Relaxed));
        info!("  Dirty Flushes: {}", self.dirty_page_flushes.load(Ordering::Relaxed));
        
        info!("Disk I/O Performance:");
        info!("  Reads: {}, Writes: {}", disk_reads, disk_writes);
        info!("  Batch Operations: {}", batch_ops);
        
        info!("Transaction Performance:");
        info!("  Started: {}, Committed: {}, Aborted: {}", 
              self.transactions_started.load(Ordering::Relaxed),
              self.transactions_committed.load(Ordering::Relaxed),
              self.transactions_aborted.load(Ordering::Relaxed));
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.total_inserts.store(0, Ordering::Relaxed);
        self.bulk_inserts.store(0, Ordering::Relaxed);
        self.insert_latency_ns.store(0, Ordering::Relaxed);
        self.insert_throughput_rows_per_sec.store(0, Ordering::Relaxed);
        self.buffer_hits.store(0, Ordering::Relaxed);
        self.buffer_misses.store(0, Ordering::Relaxed);
        self.page_evictions.store(0, Ordering::Relaxed);
        self.dirty_page_flushes.store(0, Ordering::Relaxed);
        self.disk_reads.store(0, Ordering::Relaxed);
        self.disk_writes.store(0, Ordering::Relaxed);
        self.disk_read_latency_ns.store(0, Ordering::Relaxed);
        self.disk_write_latency_ns.store(0, Ordering::Relaxed);
        self.batch_operations.store(0, Ordering::Relaxed);
        self.transactions_started.store(0, Ordering::Relaxed);
        self.transactions_committed.store(0, Ordering::Relaxed);
        self.transactions_aborted.store(0, Ordering::Relaxed);
        self.lock_acquisitions.store(0, Ordering::Relaxed);
        self.lock_waits.store(0, Ordering::Relaxed);
        self.lock_wait_time_ns.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TransactionEvent {
    Started,
    Committed,
    Aborted,
}

/// Global performance monitor instance
pub static PERFORMANCE_MONITOR: std::sync::LazyLock<PerformanceMetrics> = 
    std::sync::LazyLock::new(|| PerformanceMetrics::new());

/// Convenience function to record insert performance
pub fn record_insert_performance(row_count: usize, duration: Duration, is_bulk: bool) {
    PERFORMANCE_MONITOR.record_insert(row_count, duration, is_bulk);
}

/// Convenience function to record buffer access
pub fn record_buffer_access(is_hit: bool) {
    PERFORMANCE_MONITOR.record_buffer_access(is_hit);
}

/// Convenience function to log performance summary
pub fn log_performance_summary() {
    PERFORMANCE_MONITOR.log_performance_summary();
}

/// Convenience function to reset metrics
pub fn reset_performance_metrics() {
    PERFORMANCE_MONITOR.reset();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_performance_metrics() {
        let metrics = PerformanceMetrics::new();
        
        // Test insert recording
        metrics.record_insert(100, Duration::from_millis(500), true);
        assert_eq!(metrics.total_inserts.load(Ordering::Relaxed), 100);
        assert_eq!(metrics.bulk_inserts.load(Ordering::Relaxed), 100);
        
        // Test buffer access recording
        metrics.record_buffer_access(true);
        metrics.record_buffer_access(false);
        assert_eq!(metrics.get_buffer_hit_ratio(), 0.5);
        
        // Test transaction recording
        metrics.record_transaction(TransactionEvent::Started);
        metrics.record_transaction(TransactionEvent::Committed);
        assert_eq!(metrics.transactions_started.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.transactions_committed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_performance_calculations() {
        let metrics = PerformanceMetrics::new();
        
        // Test bulk insert percentage
        metrics.record_insert(50, Duration::from_millis(100), true);
        metrics.record_insert(50, Duration::from_millis(200), false);
        assert_eq!(metrics.get_bulk_insert_percentage(), 50.0);
        
        // Test average latency calculation
        let avg_latency = metrics.get_avg_insert_latency_ms();
        assert!(avg_latency > 0.0);
    }
} 