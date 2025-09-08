use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::storage::disk::async_disk::config::DiskManagerConfig;
use crate::storage::disk::async_disk::metrics::alerts::{
    AlertSeverity, AlertType, AlertingSystem, EscalationAction, EscalationRule,
};
use crate::storage::disk::async_disk::metrics::anomaly::{
    AnomalyDetector, BottleneckAction, BottleneckRule, MetricCondition, PerformanceAnalyzer,
    PerformanceThreshold, ThresholdComparison,
};
use crate::storage::disk::async_disk::metrics::live_metrics::LiveMetrics;
use crate::storage::disk::async_disk::metrics::prediction::{PredictionEngine, TrendAnalyzer};
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

    // Performance analysis
    pub analyzer: Arc<PerformanceAnalyzer>,

    // Alerting system
    pub alerting: Arc<RwLock<AlertingSystem>>,
}

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

        let analyzer = Arc::new(PerformanceAnalyzer {
            thresholds: Self::create_default_thresholds(),
            anomaly_detector: AnomalyDetector {
                baseline_metrics: HashMap::new(),
                sensitivity: 2.0,
                detection_window: Duration::from_secs(300),
                learning_rate: 0.1,
                anomaly_history: VecDeque::new(),
            },
            trend_analyzer: TrendAnalyzer {
                trend_data: HashMap::new(),
                trend_window: Duration::from_secs(3600),
                prediction_horizon: Duration::from_secs(1800),
            },
            bottleneck_detector:
                crate::storage::disk::async_disk::metrics::anomaly::BottleneckDetector {
                    active_bottlenecks: HashMap::new(),
                    detection_rules: Self::create_bottleneck_rules(),
                    correlation_matrix: HashMap::new(),
                },
            prediction_engine: PredictionEngine {
                models: HashMap::new(),
                forecast_horizon: Duration::from_secs(3600),
                confidence_threshold: 0.8,
            },
        });

        let alerting = Arc::new(RwLock::new(AlertingSystem::new()));

        Self {
            live_metrics,
            metrics_store,
            analyzer,
            alerting,
        }
    }

    /// Creates default performance thresholds
    pub fn create_default_thresholds() -> HashMap<String, PerformanceThreshold> {
        let mut thresholds = HashMap::new();

        // I/O Performance thresholds
        thresholds.insert(
            "read_latency".to_string(),
            PerformanceThreshold {
                metric_name: "read_latency".to_string(),
                warning_threshold: 10_000_000.0,  // 10ms in ns
                critical_threshold: 50_000_000.0, // 50ms in ns
                comparison: ThresholdComparison::GreaterThan,
                evaluation_window: Duration::from_secs(60),
                consecutive_violations: 3,
            },
        );

        thresholds.insert(
            "write_latency".to_string(),
            PerformanceThreshold {
                metric_name: "write_latency".to_string(),
                warning_threshold: 20_000_000.0,   // 20ms in ns
                critical_threshold: 100_000_000.0, // 100ms in ns
                comparison: ThresholdComparison::GreaterThan,
                evaluation_window: Duration::from_secs(60),
                consecutive_violations: 3,
            },
        );

        // Cache Performance thresholds
        thresholds.insert(
            "cache_hit_ratio".to_string(),
            PerformanceThreshold {
                metric_name: "cache_hit_ratio".to_string(),
                warning_threshold: 0.7,  // 70%
                critical_threshold: 0.5, // 50%
                comparison: ThresholdComparison::LessThan,
                evaluation_window: Duration::from_secs(300),
                consecutive_violations: 5,
            },
        );

        // Add more thresholds as needed...

        thresholds
    }

    /// Creates bottleneck detection rules
    pub fn create_bottleneck_rules() -> Vec<BottleneckRule> {
        let rules = vec![
            // I/O Bottleneck
            BottleneckRule {
                name: "IO Bottleneck".to_string(),
                conditions: vec![
                    MetricCondition {
                        metric: "io_queue_depth".to_string(),
                        operator: ThresholdComparison::GreaterThan,
                        value: 10.0,
                        duration: Duration::from_secs(60),
                    },
                    MetricCondition {
                        metric: "read_latency_avg_ns".to_string(),
                        operator: ThresholdComparison::GreaterThan,
                        value: 50_000_000.0, // 50ms
                        duration: Duration::from_secs(60),
                    },
                ],
                action: BottleneckAction::Alert("I/O subsystem is a bottleneck".to_string()),
            },
            // Cache Bottleneck
            BottleneckRule {
                name: "Cache Bottleneck".to_string(),
                conditions: vec![
                    MetricCondition {
                        metric: "cache_hit_ratio".to_string(),
                        operator: ThresholdComparison::LessThan,
                        value: 0.5, // 50%
                        duration: Duration::from_secs(300),
                    },
                    MetricCondition {
                        metric: "cache_evictions".to_string(),
                        operator: ThresholdComparison::GreaterThan,
                        value: 1000.0,
                        duration: Duration::from_secs(60),
                    },
                ],
                action: BottleneckAction::CacheEvict,
            },
        ];

        // Add more rules as needed...

        rules
    }

    /// Creates escalation rules
    pub fn create_escalation_rules() -> Vec<EscalationRule> {
        let rules = vec![
            // Critical performance issues
            EscalationRule {
                alert_type: AlertType::Performance,
                severity: AlertSeverity::Critical,
                escalation_delay: Duration::from_secs(300), // 5 minutes
                max_escalations: 3,
                escalation_actions: vec![
                    EscalationAction::NotifyAdmins,
                    EscalationAction::SendEmail("admin@example.com".to_string()),
                ],
            },
            // Resource exhaustion
            EscalationRule {
                alert_type: AlertType::Resource,
                severity: AlertSeverity::Warning,
                escalation_delay: Duration::from_secs(1800), // 30 minutes
                max_escalations: 2,
                escalation_actions: vec![EscalationAction::SendEmail(
                    "admin@example.com".to_string(),
                )],
            },
        ];

        // Add more rules as needed...

        rules
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

        // Update latency distribution (simplified)
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
        // This is a simplified implementation
        // In a real system, we would use a more sophisticated algorithm
        // to calculate percentiles accurately

        let current_max = self.live_metrics.latency_max.load(Ordering::Relaxed);
        if latency_ns > current_max {
            self.live_metrics
                .latency_max
                .store(latency_ns, Ordering::Relaxed);
        }

        // In a real implementation, we would update p50, p95, p99 here
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

        // Reset the start time for the new measurement period
        // Note: This is not atomic, but it's acceptable for metrics collection
        // In a production system, you might want to use a more sophisticated approach
    }

    /// Calculates the overall health score
    pub fn calculate_health_score(&self) -> u64 {
        // This is a simplified implementation
        // In a real system, we would use a more sophisticated algorithm
        // that takes into account multiple factors

        let mut score = 100;

        // Penalize for high latency
        let avg_latency = if self.live_metrics.io_count.load(Ordering::Relaxed) > 0 {
            self.live_metrics.io_latency_sum.load(Ordering::Relaxed)
                / self.live_metrics.io_count.load(Ordering::Relaxed)
        } else {
            0
        };

        if avg_latency > 50_000_000 {
            // 50ms
            score -= 20;
        } else if avg_latency > 10_000_000 {
            // 10ms
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

    /// Checks for anomalies in metrics
    pub fn check_for_anomalies(&self, metric_name: &str, value: f64) {
        // This is a simplified implementation
        // In a real system, we would use a more sophisticated algorithm
        // for anomaly detection

        // Check against thresholds
        if let Some(threshold) = self.analyzer.thresholds.get(metric_name) {
            let is_violation = match threshold.comparison {
                ThresholdComparison::GreaterThan => value > threshold.warning_threshold,
                ThresholdComparison::LessThan => value < threshold.warning_threshold,
                ThresholdComparison::GreaterThanOrEqual => value >= threshold.warning_threshold,
                ThresholdComparison::LessThanOrEqual => value <= threshold.warning_threshold,
            };

            if is_violation {
                // In a real implementation, we would create an alert here
                // and possibly trigger an action
            }
        }
    }

    /// Gets the live metrics
    pub fn get_live_metrics(&self) -> &LiveMetrics {
        &self.live_metrics
    }

    /// Creates a snapshot of the current metrics
    pub fn create_metrics_snapshot(&self) -> MetricsSnapshot {
        // First, update performance counters to ensure time-based metrics are current
        self.update_performance_counters();

        let read_ops = self.live_metrics.read_ops_count.load(Ordering::Relaxed);
        let write_ops = self.live_metrics.write_ops_count.load(Ordering::Relaxed);

        let read_latency_avg = if read_ops > 0 {
            self.live_metrics.read_latency_sum.load(Ordering::Relaxed) / read_ops
        } else {
            0
        };

        let write_latency_avg = if write_ops > 0 {
            self.live_metrics.write_latency_sum.load(Ordering::Relaxed) / write_ops
        } else {
            0
        };

        let cache_hits = self.live_metrics.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.live_metrics.cache_misses.load(Ordering::Relaxed);
        let total_cache_ops = cache_hits + cache_misses;
        let cache_hit_ratio = if total_cache_ops > 0 {
            cache_hits as f64 / total_cache_ops as f64
        } else {
            0.0
        };

        let prefetch_hits = self.live_metrics.prefetch_hits.load(Ordering::Relaxed);
        let prefetch_accuracy = if prefetch_hits > 0 {
            prefetch_hits as f64 / (prefetch_hits + cache_misses) as f64
        } else {
            0.0
        };

        // Use time-based calculations for throughput and frequency
        let elapsed_secs = self.live_metrics.start_time.elapsed().as_secs();
        let elapsed_secs_f64 = elapsed_secs.max(1) as f64; // Avoid division by zero

        let io_throughput_mb_per_sec = if elapsed_secs > 0 {
            self.live_metrics
                .io_throughput_bytes
                .load(Ordering::Relaxed) as f64
                / elapsed_secs_f64
                / 1_000_000.0
        } else {
            0.0
        };

        let flush_frequency_per_sec = if elapsed_secs > 0 {
            self.live_metrics.flush_count.load(Ordering::Relaxed) as f64 / elapsed_secs_f64
        } else {
            0.0
        };

        let error_rate_per_sec = if elapsed_secs > 0 {
            self.live_metrics.error_count.load(Ordering::Relaxed) as f64 / elapsed_secs_f64
        } else {
            0.0
        };

        MetricsSnapshot {
            read_latency_avg_ns: read_latency_avg,
            write_latency_avg_ns: write_latency_avg,
            io_throughput_mb_per_sec,
            io_queue_depth: self.live_metrics.io_queue_depth.load(Ordering::Relaxed),
            cache_hit_ratio,
            prefetch_accuracy,
            cache_memory_usage_mb: self.live_metrics.cache_memory_usage.load(Ordering::Relaxed)
                / 1_000_000,
            write_buffer_utilization: self
                .live_metrics
                .write_buffer_utilization
                .load(Ordering::Relaxed) as f64
                / 100.0,
            compression_ratio: self.live_metrics.compression_ratio.load(Ordering::Relaxed) as f64
                / 100.0,
            flush_frequency_per_sec,
            error_rate_per_sec,
            retry_count: self.live_metrics.retry_count.load(Ordering::Relaxed),
        }
    }

    /// Starts the metrics monitoring background task
    pub fn start_monitoring(&self) -> JoinHandle<()> {
        let metrics_store = self.metrics_store.clone();
        let live_metrics = self.live_metrics.clone();
        let analyzer = self.analyzer.clone();
        let alerting = self.alerting.clone();

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

                // Perform anomaly detection
                Self::perform_anomaly_detection(&analyzer, &snapshot);

                // Check alert conditions
                Self::check_alert_conditions(&alerting, &snapshot);
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

        // Use time-based calculations for throughput and frequency
        let elapsed_secs = live_metrics.start_time.elapsed().as_secs();
        let elapsed_secs_f64 = elapsed_secs.max(1) as f64; // Avoid division by zero

        let io_throughput_mb_per_sec = if elapsed_secs > 0 {
            live_metrics.io_throughput_bytes.load(Ordering::Relaxed) as f64
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

        MetricsSnapshot {
            read_latency_avg_ns: read_latency_avg,
            write_latency_avg_ns: write_latency_avg,
            io_throughput_mb_per_sec,
            io_queue_depth: live_metrics.io_queue_depth.load(Ordering::Relaxed),
            cache_hit_ratio,
            prefetch_accuracy: 0.0, // Simplified calculation
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

    /// Performs anomaly detection
    pub fn perform_anomaly_detection(_analyzer: &PerformanceAnalyzer, _snapshot: &MetricsSnapshot) {
        // This is a simplified implementation
        // In a real system, we would use machine learning algorithms
        // to detect anomalies in the metrics
    }

    /// Checks alert conditions
    pub fn check_alert_conditions(_alerting: &RwLock<AlertingSystem>, _snapshot: &MetricsSnapshot) {
        // This is a simplified implementation
        // In a real system, we would check various alert conditions
        // and trigger alerts when conditions are met
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::async_disk::config::DiskManagerConfig;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

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

        // Verify error counting
        let error_count = collector.live_metrics.error_count.load(Ordering::Relaxed);
        assert_eq!(
            error_count, 1,
            "Should have recorded 1 error from failed read"
        );

        // Create snapshot and verify average latencies
        let snapshot = collector.create_metrics_snapshot();
        assert_eq!(
            snapshot.read_latency_avg_ns, 2000,
            "Average read latency should be 6000/3 = 2000ns"
        );
        assert_eq!(
            snapshot.write_latency_avg_ns, 6000,
            "Average write latency should be 12000/2 = 6000ns"
        );
    }

    #[test]
    fn test_batch_operation_metrics() {
        let collector = create_test_collector();

        // Record batch read operation
        collector.record_batch_read(10, 50000, 40960); // 10 operations, 50μs total, 40KB total

        // Record batch write operation
        collector.record_batch_write(5, 25000, 20480); // 5 operations, 25μs total, 20KB total

        // Verify operation counts
        let read_ops = collector
            .live_metrics
            .read_ops_count
            .load(Ordering::Relaxed);
        let write_ops = collector
            .live_metrics
            .write_ops_count
            .load(Ordering::Relaxed);
        let batch_ops = collector
            .live_metrics
            .batch_ops_count
            .load(Ordering::Relaxed);

        assert_eq!(
            read_ops, 10,
            "Should have recorded 10 read operations from batch"
        );
        assert_eq!(
            write_ops, 5,
            "Should have recorded 5 write operations from batch"
        );
        assert_eq!(batch_ops, 2, "Should have recorded 2 batch operations");

        // Verify latency tracking
        let read_latency_sum = collector
            .live_metrics
            .read_latency_sum
            .load(Ordering::Relaxed);
        let write_latency_sum = collector
            .live_metrics
            .write_latency_sum
            .load(Ordering::Relaxed);

        assert_eq!(
            read_latency_sum, 50000,
            "Read latency sum should include batch latency"
        );
        assert_eq!(
            write_latency_sum, 25000,
            "Write latency sum should include batch latency"
        );

        // Verify bytes tracking
        let total_bytes_written = collector
            .live_metrics
            .total_bytes_written
            .load(Ordering::Relaxed);
        assert_eq!(
            total_bytes_written, 20480,
            "Should track bytes written from batch write"
        );
    }

    #[test]
    fn test_flush_operation_metrics() {
        let collector = create_test_collector();

        // Record flush operation
        collector.record_flush(10000, 5); // 10μs, 5 pages flushed

        // Verify flush count
        let flush_count = collector.live_metrics.flush_count.load(Ordering::Relaxed);
        assert_eq!(flush_count, 1, "Should have recorded 1 flush operation");

        // Verify I/O count includes flushed pages
        let io_count = collector.live_metrics.io_count.load(Ordering::Relaxed);
        assert_eq!(io_count, 5, "I/O count should include 5 pages from flush");

        // Create snapshot and verify flush frequency calculation
        std::thread::sleep(Duration::from_millis(100)); // Allow some time to pass
        let snapshot = collector.create_metrics_snapshot();
        assert!(
            snapshot.flush_frequency_per_sec >= 0.0,
            "Flush frequency should be non-negative"
        );
    }

    #[test]
    fn test_cache_operation_metrics() {
        let collector = create_test_collector();

        // Record cache operations
        collector.record_cache_operation("hot", true); // Hot cache hit
        collector.record_cache_operation("warm", true); // Warm cache hit
        collector.record_cache_operation("cold", false); // Cold cache miss
        collector.record_cache_operation("hot", true); // Another hot cache hit

        // Record cache migrations
        collector.record_cache_migration(true); // Promotion
        collector.record_cache_migration(false); // Demotion

        // Verify cache metrics
        let cache_hits = collector.live_metrics.cache_hits.load(Ordering::Relaxed);
        let cache_misses = collector.live_metrics.cache_misses.load(Ordering::Relaxed);
        let hot_cache_hits = collector
            .live_metrics
            .hot_cache_hits
            .load(Ordering::Relaxed);
        let warm_cache_hits = collector
            .live_metrics
            .warm_cache_hits
            .load(Ordering::Relaxed);
        let promotions = collector
            .live_metrics
            .cache_promotions
            .load(Ordering::Relaxed);
        let demotions = collector
            .live_metrics
            .cache_demotions
            .load(Ordering::Relaxed);

        assert_eq!(cache_hits, 3, "Should have recorded 3 cache hits");
        assert_eq!(cache_misses, 1, "Should have recorded 1 cache miss");
        assert_eq!(hot_cache_hits, 2, "Should have recorded 2 hot cache hits");
        assert_eq!(warm_cache_hits, 1, "Should have recorded 1 warm cache hit");
        assert_eq!(promotions, 1, "Should have recorded 1 promotion");
        assert_eq!(demotions, 1, "Should have recorded 1 demotion");

        // Create snapshot and verify cache hit ratio
        let snapshot = collector.create_metrics_snapshot();
        assert_eq!(
            snapshot.cache_hit_ratio, 0.75,
            "Cache hit ratio should be 3/4 = 0.75"
        );
    }

    #[test]
    fn test_time_based_metrics_calculation() {
        let collector = create_test_collector();

        // Record some operations
        collector.record_write(1000, 4096); // 1μs, 4KB
        collector.record_read(2000, 8192, true); // 2μs, 8KB

        // Allow some time to pass for time-based calculations
        std::thread::sleep(Duration::from_millis(100));

        // Update performance counters
        collector.update_performance_counters();

        // Verify time-based metrics are calculated
        let bytes_per_sec = collector
            .live_metrics
            .bytes_per_second
            .load(Ordering::Relaxed);
        let ops_per_sec = collector
            .live_metrics
            .pages_per_second
            .load(Ordering::Relaxed);
        let uptime = collector
            .live_metrics
            .uptime_seconds
            .load(Ordering::Relaxed);

        assert!(bytes_per_sec > 0, "Bytes per second should be calculated");
        assert!(
            ops_per_sec > 0,
            "Operations per second should be calculated"
        );
        assert!(uptime > 0, "Uptime should be tracked");

        // Create snapshot and verify throughput calculation
        let snapshot = collector.create_metrics_snapshot();
        assert!(
            snapshot.io_throughput_mb_per_sec >= 0.0,
            "Throughput should be non-negative"
        );
    }

    #[test]
    fn test_health_score_calculation() {
        let collector = create_test_collector();

        // Record operations with reasonable latencies (should maintain high health score)
        collector.record_read(1000000, 4096, true); // 1ms - reasonable
        collector.record_write(2000000, 4096); // 2ms - reasonable

        let health_score = collector.calculate_health_score();
        assert!(
            health_score >= 90,
            "Health score should be high with reasonable latencies: {}",
            health_score
        );

        // Record operations with high latencies (should reduce health score)
        for _ in 0..10 {
            collector.record_read(60000000, 4096, true); // 60ms - high latency
        }

        let health_score_after = collector.calculate_health_score();
        assert!(
            health_score_after < health_score,
            "Health score should decrease with high latencies"
        );

        // Record errors (should further reduce health score)
        for _ in 0..5 {
            collector.record_read(1000000, 4096, false); // Failed operations
        }

        let health_score_final = collector.calculate_health_score();
        assert!(
            health_score_final < health_score_after,
            "Health score should decrease with errors"
        );
    }

    #[test]
    fn test_metrics_reset() {
        let collector = create_test_collector();

        // Record some operations
        collector.record_read(1000, 4096, true);
        collector.record_write(2000, 8192);
        collector.record_flush(5000, 3);

        // Verify metrics are recorded
        assert!(
            collector
                .live_metrics
                .read_ops_count
                .load(Ordering::Relaxed)
                > 0
        );
        assert!(
            collector
                .live_metrics
                .write_ops_count
                .load(Ordering::Relaxed)
                > 0
        );
        assert!(collector.live_metrics.flush_count.load(Ordering::Relaxed) > 0);

        // Reset metrics
        collector.reset_metrics();

        // Verify metrics are reset
        assert_eq!(
            collector
                .live_metrics
                .read_ops_count
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            collector
                .live_metrics
                .write_ops_count
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            collector.live_metrics.flush_count.load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            collector
                .live_metrics
                .read_latency_sum
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            collector
                .live_metrics
                .write_latency_sum
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            collector
                .live_metrics
                .io_latency_sum
                .load(Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn test_metrics_snapshot_consistency() {
        let collector = create_test_collector();

        // Record a mix of operations
        collector.record_read(1000, 4096, true);
        collector.record_read(2000, 8192, false); // Failed read
        collector.record_write(3000, 4096);
        collector.record_batch_write(5, 15000, 20480);
        collector.record_flush(8000, 2);
        collector.record_cache_operation("hot", true);
        collector.record_cache_operation("warm", false);

        // Allow time to pass for proper time-based calculations
        std::thread::sleep(Duration::from_millis(50));

        // Create snapshot
        let snapshot = collector.create_metrics_snapshot();

        // Verify snapshot contains expected data
        assert!(
            snapshot.read_latency_avg_ns > 0,
            "Should have read latency data"
        );
        assert!(
            snapshot.write_latency_avg_ns > 0,
            "Should have write latency data"
        );
        assert!(
            snapshot.io_throughput_mb_per_sec >= 0.0,
            "Should have throughput data"
        );
        assert!(
            snapshot.cache_hit_ratio >= 0.0 && snapshot.cache_hit_ratio <= 1.0,
            "Cache hit ratio should be between 0 and 1"
        );
        assert!(
            snapshot.flush_frequency_per_sec >= 0.0,
            "Flush frequency should be non-negative"
        );
        assert!(
            snapshot.error_rate_per_sec >= 0.0,
            "Error rate should be non-negative"
        );

        // Verify consistency between different snapshot creation methods
        let snapshot2 = MetricsCollector::create_snapshot_from_live(&collector.live_metrics);

        // Key metrics should be identical
        assert_eq!(snapshot.read_latency_avg_ns, snapshot2.read_latency_avg_ns);
        assert_eq!(
            snapshot.write_latency_avg_ns,
            snapshot2.write_latency_avg_ns
        );
        assert_eq!(snapshot.cache_hit_ratio, snapshot2.cache_hit_ratio);
    }

    #[test]
    fn test_zero_division_handling() {
        let collector = create_test_collector();

        // Create snapshot with no operations (should handle zero division gracefully)
        let snapshot = collector.create_metrics_snapshot();

        assert_eq!(
            snapshot.read_latency_avg_ns, 0,
            "Read latency should be 0 with no operations"
        );
        assert_eq!(
            snapshot.write_latency_avg_ns, 0,
            "Write latency should be 0 with no operations"
        );
        assert_eq!(
            snapshot.cache_hit_ratio, 0.0,
            "Cache hit ratio should be 0 with no cache operations"
        );
        assert_eq!(
            snapshot.io_throughput_mb_per_sec, 0.0,
            "Throughput should be 0 with no operations"
        );

        // Health score should still be calculable
        let health_score = collector.calculate_health_score();
        assert!(
            health_score <= 100,
            "Health score should be valid even with no operations"
        );
    }

    #[tokio::test]
    async fn test_monitoring_background_task() {
        let collector = create_test_collector();

        // Start monitoring
        let handle = collector.start_monitoring();

        // Record some operations
        collector.record_write(1000, 4096);
        collector.record_read(2000, 8192, true);

        // Allow background task to run briefly
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Abort the background task
        handle.abort();

        // Verify the task was created and can be aborted without panic
        assert!(
            handle.is_finished(),
            "Background task should be finished after abort"
        );
    }
}
