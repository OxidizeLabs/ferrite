//! B+Tree lock observability utilities.
//!
//! Goals:
//! - Detect and surface potential deadlocks / hangs by periodically logging
//!   slow latch acquisition attempts.
//! - Provide simple in-process metrics (atomics) and a Prometheus-text export
//!   without introducing new external dependencies.

use crate::common::config::PageId;
use log::warn;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct LockObservabilityConfig {
    /// After this duration, start emitting "slow lock" warnings.
    pub warn_after: Duration,
    /// Emit a warning at most this often while waiting.
    pub warn_every: Duration,
    /// If set, fail the acquisition after this duration (prevents tests hanging forever).
    pub timeout_after: Option<Duration>,
    /// Sleep/backoff interval used by spin/try loops.
    pub poll_interval: Duration,
}

impl Default for LockObservabilityConfig {
    fn default() -> Self {
        let warn_after_ms = env_u64("FERRITE_BTREE_LOCK_WARN_AFTER_MS", 200);
        let warn_every_ms = env_u64("FERRITE_BTREE_LOCK_WARN_EVERY_MS", 1_000);
        // In tests we want to fail fast rather than hang forever.
        #[cfg(test)]
        let default_timeout_ms = 5_000;
        #[cfg(not(test))]
        let default_timeout_ms = 0;
        let timeout_ms = env_u64("FERRITE_BTREE_LOCK_TIMEOUT_MS", default_timeout_ms);
        let poll_ms = env_u64("FERRITE_BTREE_LOCK_POLL_MS", 25);

        Self {
            warn_after: Duration::from_millis(warn_after_ms),
            warn_every: Duration::from_millis(warn_every_ms.max(1)),
            timeout_after: if timeout_ms == 0 {
                None
            } else {
                Some(Duration::from_millis(timeout_ms))
            },
            poll_interval: Duration::from_millis(poll_ms.max(1)),
        }
    }
}

#[derive(Default)]
pub struct BTreeLockMetrics {
    pub internal_write_wait_ns_total: AtomicU64,
    pub internal_write_wait_count: AtomicU64,
    pub internal_write_wait_warns: AtomicU64,
    pub internal_write_timeouts: AtomicU64,
    pub internal_write_hold_ns_total: AtomicU64,
    pub internal_write_hold_count: AtomicU64,
    pub internal_write_hold_max_ns: AtomicU64,

    pub leaf_write_wait_ns_total: AtomicU64,
    pub leaf_write_wait_count: AtomicU64,
    pub leaf_write_wait_warns: AtomicU64,
    pub leaf_write_timeouts: AtomicU64,
}

static CONFIG: OnceLock<LockObservabilityConfig> = OnceLock::new();
static METRICS: OnceLock<BTreeLockMetrics> = OnceLock::new();

#[inline]
pub fn config() -> &'static LockObservabilityConfig {
    CONFIG.get_or_init(LockObservabilityConfig::default)
}

#[inline]
pub fn metrics() -> &'static BTreeLockMetrics {
    METRICS.get_or_init(BTreeLockMetrics::default)
}

#[inline]
pub fn record_internal_write_wait(wait: Duration, warned: bool, timed_out: bool) {
    let m = metrics();
    m.internal_write_wait_ns_total
        .fetch_add(wait.as_nanos() as u64, Ordering::Relaxed);
    m.internal_write_wait_count.fetch_add(1, Ordering::Relaxed);
    if warned {
        m.internal_write_wait_warns.fetch_add(1, Ordering::Relaxed);
    }
    if timed_out {
        m.internal_write_timeouts.fetch_add(1, Ordering::Relaxed);
    }
}

#[inline]
pub fn record_internal_write_hold(hold: Duration) {
    let m = metrics();
    let ns = hold.as_nanos() as u64;
    m.internal_write_hold_ns_total
        .fetch_add(ns, Ordering::Relaxed);
    m.internal_write_hold_count.fetch_add(1, Ordering::Relaxed);
    atomic_max(&m.internal_write_hold_max_ns, ns);
}

#[inline]
pub fn record_leaf_write_wait(wait: Duration, warned: bool, timed_out: bool) {
    let m = metrics();
    m.leaf_write_wait_ns_total
        .fetch_add(wait.as_nanos() as u64, Ordering::Relaxed);
    m.leaf_write_wait_count.fetch_add(1, Ordering::Relaxed);
    if warned {
        m.leaf_write_wait_warns.fetch_add(1, Ordering::Relaxed);
    }
    if timed_out {
        m.leaf_write_timeouts.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn export_prometheus() -> String {
    let m = metrics();

    // Keep this format stable and grep-friendly.
    // Note: no labels for now (simple counters only).
    format!(
        concat!(
            "# TYPE ferrite_btree_internal_write_wait_ns_total counter\n",
            "ferrite_btree_internal_write_wait_ns_total {}\n",
            "# TYPE ferrite_btree_internal_write_wait_count counter\n",
            "ferrite_btree_internal_write_wait_count {}\n",
            "# TYPE ferrite_btree_internal_write_wait_warns counter\n",
            "ferrite_btree_internal_write_wait_warns {}\n",
            "# TYPE ferrite_btree_internal_write_timeouts counter\n",
            "ferrite_btree_internal_write_timeouts {}\n",
            "# TYPE ferrite_btree_internal_write_hold_ns_total counter\n",
            "ferrite_btree_internal_write_hold_ns_total {}\n",
            "# TYPE ferrite_btree_internal_write_hold_count counter\n",
            "ferrite_btree_internal_write_hold_count {}\n",
            "# TYPE ferrite_btree_internal_write_hold_max_ns gauge\n",
            "ferrite_btree_internal_write_hold_max_ns {}\n",
            "# TYPE ferrite_btree_leaf_write_wait_ns_total counter\n",
            "ferrite_btree_leaf_write_wait_ns_total {}\n",
            "# TYPE ferrite_btree_leaf_write_wait_count counter\n",
            "ferrite_btree_leaf_write_wait_count {}\n",
            "# TYPE ferrite_btree_leaf_write_wait_warns counter\n",
            "ferrite_btree_leaf_write_wait_warns {}\n",
            "# TYPE ferrite_btree_leaf_write_timeouts counter\n",
            "ferrite_btree_leaf_write_timeouts {}\n",
        ),
        m.internal_write_wait_ns_total.load(Ordering::Relaxed),
        m.internal_write_wait_count.load(Ordering::Relaxed),
        m.internal_write_wait_warns.load(Ordering::Relaxed),
        m.internal_write_timeouts.load(Ordering::Relaxed),
        m.internal_write_hold_ns_total.load(Ordering::Relaxed),
        m.internal_write_hold_count.load(Ordering::Relaxed),
        m.internal_write_hold_max_ns.load(Ordering::Relaxed),
        m.leaf_write_wait_ns_total.load(Ordering::Relaxed),
        m.leaf_write_wait_count.load(Ordering::Relaxed),
        m.leaf_write_wait_warns.load(Ordering::Relaxed),
        m.leaf_write_timeouts.load(Ordering::Relaxed),
    )
}

pub fn warn_slow_lock(
    lock_kind: &'static str,
    lock_mode: &'static str,
    page_id: PageId,
    waited: Duration,
    op: &'static str,
    path: &[PageId],
    held_write_locks: usize,
) {
    warn!(
        target: "ferrite::storage::index::btree_lock",
        "Slow B+tree lock acquire: kind={}, mode={}, page_id={}, waited_ms={}, op={}, path={:?}, held_write_locks={}",
        lock_kind,
        lock_mode,
        page_id,
        waited.as_millis(),
        op,
        path,
        held_write_locks
    );
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn atomic_max(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}
