use crate::storage::disk::async_disk::metrics::alerts::AlertSummary;
use tokio::time::Instant;

/// Comprehensive dashboard data for real-time monitoring
#[derive(Debug, Clone)]
pub struct DashboardData {
    pub timestamp: Instant,
    pub health_score: f64,
    pub performance: PerformanceDashboard,
    pub cache: CacheDashboard,
    pub storage: StorageDashboard,
    pub alerts: Vec<AlertSummary>,
}

/// Performance metrics for dashboard
#[derive(Debug, Clone)]
pub struct PerformanceDashboard {
    pub avg_read_latency_ms: f64,
    pub avg_write_latency_ms: f64,
    pub throughput_mb_sec: f64,
    pub iops: f64,
    pub queue_depth: usize,
}

/// Cache metrics for dashboard
#[derive(Debug, Clone)]
pub struct CacheDashboard {
    pub overall_hit_ratio: f64,
    pub hot_cache_hit_ratio: f64,
    pub warm_cache_hit_ratio: f64,
    pub cold_cache_hit_ratio: f64,
    pub memory_usage_mb: usize,
    pub promotions_per_sec: f64,
    pub evictions_per_sec: f64,
}

/// Storage metrics for dashboard
#[derive(Debug, Clone)]
pub struct StorageDashboard {
    pub buffer_utilization: f64,
    pub compression_ratio: f64,
    pub flush_frequency: f64,
    pub bytes_written_mb: f64,
    pub write_amplification: f64,
}

/// Comprehensive health report
#[derive(Debug, Clone)]
pub struct HealthReport {
    pub overall_health: f64,
    pub component_health: ComponentHealth,
    pub bottlenecks: Vec<String>,
    pub recommendations: Vec<String>,
    pub uptime_seconds: u64,
    pub last_error: Option<String>,
}

/// Component-specific health scores
#[derive(Debug, Clone)]
pub struct ComponentHealth {
    pub io_engine: f64,
    pub cache_manager: f64,
    pub write_manager: f64,
    pub storage_engine: f64,
}
