//! # I/O Metrics Collection
//!
//! The `IOMetrics` struct provides a comprehensive observability layer for the I/O subsystem.
//! It tracks performance counters, throughput, and latency statistics in real-time.
//!
//! ## Key Features
//!
//! - **Atomic Counters**: Lock-free tracking of total, completed, failed, and cancelled operations.
//! - **Throughput Tracking**: Monitors bytes read/written to calculate I/O bandwidth usage.
//! - **Latency Analysis**:
//!     - Tracks total, minimum, and maximum operation durations.
//!     - Maintains a rolling window of recent durations to calculate short-term averages.
//! - **State Monitoring**: Tracks current pending operations to detect backpressure or stalls.
//!
//! ## Usage
//!
//! Metrics are collected automatically by the `CompletionTracker` and can be queried by monitoring
//! systems or the dashboard to visualize database health.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

/// I/O operation metrics for monitoring and observability
#[derive(Debug)]
pub struct IOMetrics {
    // Operation counters
    total_operations: AtomicU64,
    completed_operations: AtomicU64,
    failed_operations: AtomicU64,
    cancelled_operations: AtomicU64,
    timed_out_operations: AtomicU64,

    // Performance metrics
    total_bytes_read: AtomicU64,
    total_bytes_written: AtomicU64,

    // Timing metrics (in microseconds for better precision)
    total_duration_micros: AtomicU64,
    min_duration_micros: AtomicU64,
    max_duration_micros: AtomicU64,

    // Current state
    pending_operations: AtomicUsize,

    // Recent operation times for calculating rolling averages
    recent_durations: RwLock<VecDeque<Duration>>,
    max_recent_samples: usize,

    // Start time for calculating rates
    start_time: Instant,
}

impl Default for IOMetrics {
    fn default() -> Self {
        Self::new(1000) // Keep last 1000 operation durations
    }
}

impl IOMetrics {
    /// Creates a new metrics collector
    pub fn new(max_recent_samples: usize) -> Self {
        Self {
            total_operations: AtomicU64::new(0),
            completed_operations: AtomicU64::new(0),
            failed_operations: AtomicU64::new(0),
            cancelled_operations: AtomicU64::new(0),
            timed_out_operations: AtomicU64::new(0),
            total_bytes_read: AtomicU64::new(0),
            total_bytes_written: AtomicU64::new(0),
            total_duration_micros: AtomicU64::new(0),
            min_duration_micros: AtomicU64::new(u64::MAX),
            max_duration_micros: AtomicU64::new(0),
            pending_operations: AtomicUsize::new(0),
            recent_durations: RwLock::new(VecDeque::new()),
            max_recent_samples,
            start_time: Instant::now(),
        }
    }

    /// Records the start of an I/O operation
    pub fn record_operation_start(&self) {
        self.total_operations.fetch_add(1, Ordering::Relaxed);
        self.pending_operations.fetch_add(1, Ordering::Relaxed);
    }

    /// Records the completion of an I/O operation
    pub async fn record_operation_complete(
        &self,
        duration: Duration,
        bytes_transferred: u64,
        is_read: bool,
    ) {
        self.completed_operations.fetch_add(1, Ordering::Relaxed);
        self.pending_operations.fetch_sub(1, Ordering::Relaxed);

        // Update byte counters
        if is_read {
            self.total_bytes_read
                .fetch_add(bytes_transferred, Ordering::Relaxed);
        } else {
            self.total_bytes_written
                .fetch_add(bytes_transferred, Ordering::Relaxed);
        }

        // Update timing metrics
        let duration_micros = duration.as_micros() as u64;
        self.total_duration_micros
            .fetch_add(duration_micros, Ordering::Relaxed);

        // Update min/max (using compare_exchange loops for atomicity)
        let mut current_min = self.min_duration_micros.load(Ordering::Relaxed);
        while current_min > duration_micros {
            match self.min_duration_micros.compare_exchange_weak(
                current_min,
                duration_micros,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(new_min) => current_min = new_min,
            }
        }

        let mut current_max = self.max_duration_micros.load(Ordering::Relaxed);
        while current_max < duration_micros {
            match self.max_duration_micros.compare_exchange_weak(
                current_max,
                duration_micros,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(new_max) => current_max = new_max,
            }
        }

        // Update recent durations for rolling average
        let mut recent = self.recent_durations.write().await;
        recent.push_back(duration);
        while recent.len() > self.max_recent_samples {
            recent.pop_front();
        }
    }

    /// Records a failed I/O operation
    pub fn record_operation_failed(&self) {
        self.failed_operations.fetch_add(1, Ordering::Relaxed);
        self.pending_operations.fetch_sub(1, Ordering::Relaxed);
    }

    /// Records a cancelled I/O operation
    pub fn record_operation_cancelled(&self) {
        self.cancelled_operations.fetch_add(1, Ordering::Relaxed);
        self.pending_operations.fetch_sub(1, Ordering::Relaxed);
    }

    /// Records a timed out I/O operation
    pub fn record_operation_timed_out(&self) {
        self.timed_out_operations.fetch_add(1, Ordering::Relaxed);
        self.pending_operations.fetch_sub(1, Ordering::Relaxed);
    }

    /// Gets the total number of operations started
    pub fn total_operations(&self) -> u64 {
        self.total_operations.load(Ordering::Relaxed)
    }

    /// Gets the number of completed operations
    pub fn completed_operations(&self) -> u64 {
        self.completed_operations.load(Ordering::Relaxed)
    }

    /// Gets the number of failed operations
    pub fn failed_operations(&self) -> u64 {
        self.failed_operations.load(Ordering::Relaxed)
    }

    /// Gets the number of cancelled operations
    pub fn cancelled_operations(&self) -> u64 {
        self.cancelled_operations.load(Ordering::Relaxed)
    }

    /// Gets the number of timed out operations
    pub fn timed_out_operations(&self) -> u64 {
        self.timed_out_operations.load(Ordering::Relaxed)
    }

    /// Gets the current number of pending operations
    pub fn pending_operations(&self) -> usize {
        self.pending_operations.load(Ordering::Relaxed)
    }

    /// Gets the total bytes read
    pub fn total_bytes_read(&self) -> u64 {
        self.total_bytes_read.load(Ordering::Relaxed)
    }

    /// Gets the total bytes written
    pub fn total_bytes_written(&self) -> u64 {
        self.total_bytes_written.load(Ordering::Relaxed)
    }

    /// Gets the average operation duration
    pub fn average_duration(&self) -> Duration {
        let total_ops = self.completed_operations.load(Ordering::Relaxed);
        if total_ops == 0 {
            return Duration::from_micros(0);
        }

        let total_micros = self.total_duration_micros.load(Ordering::Relaxed);
        Duration::from_micros(total_micros / total_ops)
    }

    /// Gets the minimum operation duration
    pub fn min_duration(&self) -> Duration {
        let min_micros = self.min_duration_micros.load(Ordering::Relaxed);
        if min_micros == u64::MAX {
            Duration::from_micros(0)
        } else {
            Duration::from_micros(min_micros)
        }
    }

    /// Gets the maximum operation duration
    pub fn max_duration(&self) -> Duration {
        Duration::from_micros(self.max_duration_micros.load(Ordering::Relaxed))
    }

    /// Gets the rolling average duration based on recent operations
    pub async fn rolling_average_duration(&self) -> Duration {
        let recent = self.recent_durations.read().await;
        if recent.is_empty() {
            return Duration::from_micros(0);
        }

        let total: Duration = recent.iter().sum();
        total / recent.len() as u32
    }

    /// Gets the operations per second rate
    pub fn operations_per_second(&self) -> f64 {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed == 0.0 {
            return 0.0;
        }

        self.completed_operations.load(Ordering::Relaxed) as f64 / elapsed
    }

    /// Gets the throughput in bytes per second
    pub fn throughput_bytes_per_second(&self) -> f64 {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed == 0.0 {
            return 0.0;
        }

        let total_bytes = self.total_bytes_read.load(Ordering::Relaxed)
            + self.total_bytes_written.load(Ordering::Relaxed);
        total_bytes as f64 / elapsed
    }

    /// Gets the success rate as a percentage
    pub fn success_rate(&self) -> f64 {
        let total_ops = self.total_operations.load(Ordering::Relaxed);
        if total_ops == 0 {
            return 100.0;
        }

        let successful_ops = self.completed_operations.load(Ordering::Relaxed);
        (successful_ops as f64 / total_ops as f64) * 100.0
    }

    /// Resets all metrics
    pub async fn reset(&self) {
        self.total_operations.store(0, Ordering::Relaxed);
        self.completed_operations.store(0, Ordering::Relaxed);
        self.failed_operations.store(0, Ordering::Relaxed);
        self.cancelled_operations.store(0, Ordering::Relaxed);
        self.timed_out_operations.store(0, Ordering::Relaxed);
        self.total_bytes_read.store(0, Ordering::Relaxed);
        self.total_bytes_written.store(0, Ordering::Relaxed);
        self.total_duration_micros.store(0, Ordering::Relaxed);
        self.min_duration_micros.store(u64::MAX, Ordering::Relaxed);
        self.max_duration_micros.store(0, Ordering::Relaxed);
        self.pending_operations.store(0, Ordering::Relaxed);

        let mut recent = self.recent_durations.write().await;
        recent.clear();
    }
}
