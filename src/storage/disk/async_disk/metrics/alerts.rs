use std::collections::HashMap;
use std::time::Instant;

/// Enhanced alerting system with multiple channels
pub struct AlertingSystem {
    alert_handlers: Vec<Box<dyn Fn(&Alert) + Send + Sync>>,
    alert_cooldown: HashMap<String, Instant>,
    active_alerts: HashMap<String, Alert>,
    alert_history: Vec<Alert>,
    escalation_rules: Vec<EscalationRule>,
    notification_channels: Vec<NotificationChannel>,
}

/// Alert structure with detailed information
#[derive(Debug, Clone)]
pub struct Alert {
    pub id: String,
    pub alert_type: AlertType,
    pub severity: AlertSeverity,
    pub title: String,
    pub description: String,
    pub metric_name: String,
    pub current_value: f64,
    pub threshold_value: f64,
    pub timestamp: Instant,
    pub resolved: bool,
    pub acknowledged: bool,
    pub escalated: bool,
    pub tags: HashMap<String, String>,
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
    Info = 1,
    Warning = 2,
    Critical = 3,
    Emergency = 4,
}

/// Escalation rules for alert management
#[derive(Debug, Clone)]
pub struct EscalationRule {
    pub alert_type: AlertType,
    pub severity: AlertSeverity,
    pub escalation_delay: std::time::Duration,
    pub max_escalations: usize,
    pub escalation_actions: Vec<EscalationAction>,
}

/// Actions to take during escalation
#[derive(Debug, Clone)]
pub enum EscalationAction {
    NotifyAdmins,
    SendEmail(String),
    SendSMS(String),
    CreateTicket(String),
    AutoRemediate(String),
}

/// Notification channels
#[derive(Debug, Clone)]
pub enum NotificationChannel {
    Email {
        smtp_server: String,
        recipients: Vec<String>,
    },
    Slack {
        webhook_url: String,
        channel: String,
    },
    PagerDuty {
        service_key: String,
    },
    Webhook {
        url: String,
        headers: HashMap<String, String>,
    },
    Console,
}

/// Alert summary for dashboard
#[derive(Debug, Clone)]
pub struct AlertSummary {
    pub alert_type: AlertType,
    pub severity: AlertSeverity,
    pub title: String,
    pub timestamp: Instant,
    pub acknowledged: bool,
}

impl AlertingSystem {
    /// Creates a new AlertingSystem with default values
    pub fn new() -> Self {
        Self {
            alert_handlers: Vec::new(),
            alert_cooldown: HashMap::new(),
            active_alerts: HashMap::new(),
            alert_history: Vec::new(),
            escalation_rules: Vec::new(),
            notification_channels: Vec::new(),
        }
    }
}

impl std::fmt::Debug for AlertingSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlertingSystem")
            .field("handler_count", &self.alert_handlers.len())
            .field("alert_cooldown", &self.alert_cooldown)
            .finish()
    }
}