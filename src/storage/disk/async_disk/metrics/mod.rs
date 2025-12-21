//! # Async Disk Metrics
//!
//! This module provides a comprehensive metrics collection system for the `AsyncDiskManager`.
//! It is designed for high-concurrency environments using atomic operations for low-overhead
//! recording of performance counters and latencies.
//!
//! ## Core Components
//!
//! - **Collector (`collector.rs`)**: The central coordinator that records events (reads, writes, cache hits)
//!   and maintains historical snapshots.
//! - **Live Metrics (`live_metrics.rs`)**: A lock-free structure using atomic types (`AtomicU64`, `AtomicUsize`)
//!   to store real-time counters. This is the "hot path" storage.
//! - **Snapshot (`snapshot.rs`)**: An immutable point-in-time view of the metrics, used for reporting
//!   and dashboard display.
//! - **Dashboard (`dashboard.rs`)**: Structures for presenting aggregated views of system health,
//!   performance, and resource usage.
//!
//! ## Features
//!
//! - **Latency Histograms**: Tracks latency distributions (p50, p90, p99) using bucketed atomic counters.
//! - **Health Monitoring**: Calculates a composite health score based on latency, error rates, and cache efficiency.
//! - **Trend Analysis**: Supports tracking historical data points for predictive analysis (via `TrendData`).
//! - **Alerting Support**: Data structures for an alerting system (`AlertSummary`).

pub mod alerts;
pub mod collector;
pub mod dashboard;
pub mod live_metrics;
pub mod prediction;
pub mod snapshot;
