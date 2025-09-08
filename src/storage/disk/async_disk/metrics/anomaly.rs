use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

/// Enhanced performance analyzer with AI-driven insights
#[derive(Debug)]
pub struct PerformanceAnalyzer {
    pub thresholds: HashMap<String, PerformanceThreshold>,
    pub anomaly_detector: AnomalyDetector,
    pub trend_analyzer: crate::storage::disk::async_disk::metrics::prediction::TrendAnalyzer,
    pub bottleneck_detector: BottleneckDetector,
    pub prediction_engine: crate::storage::disk::async_disk::metrics::prediction::PredictionEngine,
}

/// Performance threshold with configurable alerting
#[derive(Debug, Clone)]
pub struct PerformanceThreshold {
    pub metric_name: String,
    pub warning_threshold: f64,
    pub critical_threshold: f64,
    pub comparison: ThresholdComparison,
    pub evaluation_window: Duration,
    pub consecutive_violations: usize,
}

/// Threshold comparison types
#[derive(Debug, Clone, PartialEq)]
pub enum ThresholdComparison {
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
}

/// Advanced anomaly detection with machine learning
#[derive(Debug)]
pub struct AnomalyDetector {
    pub baseline_metrics: HashMap<String, MetricBaseline>,
    pub sensitivity: f64,
    pub detection_window: Duration,
    pub learning_rate: f64,
    pub anomaly_history: VecDeque<AnomalyEvent>,
}

/// Baseline metrics for anomaly detection
#[derive(Debug, Clone)]
pub struct MetricBaseline {
    pub mean: f64,
    pub std_dev: f64,
    pub min_value: f64,
    pub max_value: f64,
    pub sample_count: usize,
    pub last_updated: Instant,
}

/// Anomaly event tracking
#[derive(Debug, Clone)]
pub struct AnomalyEvent {
    pub timestamp: Instant,
    pub metric_name: String,
    pub actual_value: f64,
    pub expected_range: (f64, f64),
    pub severity: AnomalySeverity,
    pub resolved: bool,
}

/// Anomaly severity levels
#[derive(Debug, Clone, PartialEq)]
pub enum AnomalySeverity {
    Low,
    Medium,
    High,
    Critical,
}

/// Bottleneck detection and analysis
#[derive(Debug)]
pub struct BottleneckDetector {
    pub active_bottlenecks: HashMap<String, BottleneckInfo>,
    pub detection_rules: Vec<BottleneckRule>,
    pub correlation_matrix: HashMap<(String, String), f64>,
}

/// Bottleneck information
#[derive(Debug, Clone)]
pub struct BottleneckInfo {
    pub component: String,
    pub metric: String,
    pub severity: f64,
    pub first_detected: Instant,
    pub contributing_factors: Vec<String>,
    pub suggested_actions: Vec<String>,
}

/// Bottleneck detection rules
#[derive(Debug, Clone)]
pub struct BottleneckRule {
    pub name: String,
    pub conditions: Vec<MetricCondition>,
    pub action: BottleneckAction,
}

/// Metric condition for bottleneck detection
#[derive(Debug, Clone)]
pub struct MetricCondition {
    pub metric: String,
    pub operator: ThresholdComparison,
    pub value: f64,
    pub duration: Duration,
}

/// Actions to take when bottleneck is detected
#[derive(Debug, Clone)]
pub enum BottleneckAction {
    Alert(String),
    AutoScale(String),
    CacheEvict,
    ForceFlush,
    ReduceConcurrency,
}
