use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

/// Trend analysis for predictive monitoring
#[derive(Debug)]
pub struct TrendAnalyzer {
    pub trend_data: HashMap<String, VecDeque<(Instant, f64)>>,
    pub trend_window: Duration,
    pub prediction_horizon: Duration,
}

/// Predictive performance engine
#[derive(Debug)]
pub struct PredictionEngine {
    pub models: HashMap<String, PredictionModel>,
    pub forecast_horizon: Duration,
    pub confidence_threshold: f64,
}

/// Prediction model for performance forecasting
#[derive(Debug)]
pub struct PredictionModel {
    pub metric_name: String,
    pub model_type: ModelType,
    pub accuracy: f64,
    pub last_trained: Instant,
    pub predictions: VecDeque<Prediction>,
}

/// Types of prediction models
#[derive(Debug)]
pub enum ModelType {
    LinearRegression,
    MovingAverage,
    ExponentialSmoothing,
    Seasonal,
}

/// Performance prediction
#[derive(Debug, Clone)]
pub struct Prediction {
    pub timestamp: Instant,
    pub predicted_value: f64,
    pub confidence_interval: (f64, f64),
    pub confidence_score: f64,
}

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
