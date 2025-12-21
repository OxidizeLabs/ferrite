//! # Prediction & Trends
//!
//! Structures for storing historical trend data and predictive analysis results.
//!
//! This module supports features that look at metrics over time to identify trends
//! (e.g., "Latency is increasing over the last hour") or store forecast data.

use std::time::{Duration, Instant};

/// Trend prediction data point
#[derive(Debug, Clone)]
pub struct TrendPrediction {
    pub timestamp: Instant,
    pub metric: String,
    pub predicted_value: f64,
    pub confidence: f64,
}

/// Historical trend data
#[derive(Debug, Clone)]
pub struct TrendData {
    pub time_range: Duration,
    pub latency_trend: Vec<(Instant, f64)>,
    pub throughput_trend: Vec<(Instant, f64)>,
    pub cache_hit_ratio_trend: Vec<(Instant, f64)>,
    pub error_rate_trend: Vec<(Instant, f64)>,
    pub predictions: Vec<TrendPrediction>,
}
