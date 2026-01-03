//! # Alerts
//!
//! Defines the data structures for system alerts and notifications.
//!
//! While the `MetricsCollector` records data, the alert types here are used to represent
//! significant events or threshold violations (e.g., "High Latency Warning") that need to be
//! surfaced to administrators or the dashboard.

use std::time::Instant;

/// Alert structure for dashboard display
#[derive(Debug, Clone)]
pub struct AlertSummary {
    pub alert_type: AlertType,
    pub severity: AlertSeverity,
    pub title: String,
    pub timestamp: Instant,
    pub acknowledged: bool,
}

/// Types of alerts
#[derive(Debug, Clone, PartialEq)]
pub enum AlertType {
    Performance,
    Resource,
    Error,
    Security,
    Health,
    Prediction,
}

/// Alert severity levels
#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq)]
pub enum AlertSeverity {
    Info      = 1,
    Warning   = 2,
    Critical  = 3,
    Emergency = 4,
}
