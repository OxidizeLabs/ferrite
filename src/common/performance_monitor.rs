use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use log::{debug, warn};

/// Comprehensive performance metrics for database operations
#[derive(Debug, Default)]
pub struct PerformanceMetrics {
    // === INSERT OPERATION METRICS ===
    pub total_inserts: AtomicUsize,
    pub bulk_inserts: AtomicUsize,
    pub insert_latency_ns: AtomicU64,
    pub insert_throughput_rows_per_sec: AtomicU64,
    pub failed_inserts: AtomicUsize,

    // === BUFFER POOL METRICS ===
    pub buffer_hits: AtomicUsize,
    pub buffer_misses: AtomicUsize,
    pub page_evictions: AtomicUsize,
    pub dirty_page_flushes: AtomicUsize,
    pub buffer_pool_utilization: AtomicUsize, // % of buffer pool used
    pub page_pin_duration_ns: AtomicU64,
    pub eviction_wait_time_ns: AtomicU64,

    // === DISK I/O METRICS ===
    pub disk_reads: AtomicUsize,
    pub disk_writes: AtomicUsize,
    pub disk_read_latency_ns: AtomicU64,
    pub disk_write_latency_ns: AtomicU64,
    pub batch_operations: AtomicUsize,
    pub disk_queue_depth: AtomicUsize,
    pub sequential_reads: AtomicUsize,
    pub random_reads: AtomicUsize,

    // === TRANSACTION METRICS ===
    pub transactions_started: AtomicUsize,
    pub transactions_committed: AtomicUsize,
    pub transactions_aborted: AtomicUsize,
    pub transaction_duration_ns: AtomicU64,
    pub active_transactions: AtomicUsize,
    pub deadlocks_detected: AtomicUsize,

    // === LOCK MANAGER METRICS ===
    pub lock_requests: AtomicUsize,
    pub lock_grants_immediate: AtomicUsize,
    pub lock_waits: AtomicUsize,
    pub lock_wait_time_ns: AtomicU64,
    pub lock_conflicts: AtomicUsize,
    pub deadlock_aborts: AtomicUsize,
    pub lock_timeouts: AtomicUsize,

    // === MVCC METRICS ===
    pub version_chain_traversals: AtomicUsize,
    pub avg_version_chain_length: AtomicUsize,
    pub undo_log_entries: AtomicUsize,
    pub version_cleanup_operations: AtomicUsize,
    pub tuple_visibility_checks: AtomicUsize,
    pub snapshot_creation_time_ns: AtomicU64,

    // === QUERY EXECUTION METRICS ===
    pub select_operations: AtomicUsize,
    pub update_operations: AtomicUsize,
    pub delete_operations: AtomicUsize,
    pub join_operations: AtomicUsize,
    pub aggregation_operations: AtomicUsize,
    pub query_execution_time_ns: AtomicU64,
    pub rows_scanned: AtomicUsize,
    pub rows_returned: AtomicUsize,

    // === MEMORY METRICS ===
    pub heap_allocations: AtomicUsize,
    pub memory_pool_usage: AtomicUsize,
    pub cache_memory_usage: AtomicUsize,

    // === CONCURRENCY METRICS ===
    pub concurrent_readers: AtomicUsize,
    pub concurrent_writers: AtomicUsize,
    pub lock_table_size: AtomicUsize,
    pub wait_for_graph_size: AtomicUsize,
}

/// Detailed bottleneck analysis results
#[derive(Debug, Clone)]
pub struct BottleneckAnalysis {
    pub primary_bottleneck: String,
    pub bottleneck_severity: f64, // 0.0 to 1.0
    pub affected_operations: Vec<String>,
    pub recommendations: Vec<String>,
    pub metrics_snapshot: HashMap<String, u64>,
}

/// Performance profiler for different subsystems
#[derive(Debug)]
pub struct SubsystemProfiler {
    pub buffer_pool_score: f64,
    pub disk_io_score: f64,
    pub lock_manager_score: f64,
    pub mvcc_score: f64,
    pub transaction_score: f64,
    pub query_execution_score: f64,
}

/// Global performance monitor instance
static PERFORMANCE_METRICS: PerformanceMetrics = PerformanceMetrics {
    total_inserts: AtomicUsize::new(0),
    bulk_inserts: AtomicUsize::new(0),
    insert_latency_ns: AtomicU64::new(0),
    insert_throughput_rows_per_sec: AtomicU64::new(0),
    failed_inserts: AtomicUsize::new(0),

    buffer_hits: AtomicUsize::new(0),
    buffer_misses: AtomicUsize::new(0),
    page_evictions: AtomicUsize::new(0),
    dirty_page_flushes: AtomicUsize::new(0),
    buffer_pool_utilization: AtomicUsize::new(0),
    page_pin_duration_ns: AtomicU64::new(0),
    eviction_wait_time_ns: AtomicU64::new(0),

    disk_reads: AtomicUsize::new(0),
    disk_writes: AtomicUsize::new(0),
    disk_read_latency_ns: AtomicU64::new(0),
    disk_write_latency_ns: AtomicU64::new(0),
    batch_operations: AtomicUsize::new(0),
    disk_queue_depth: AtomicUsize::new(0),
    sequential_reads: AtomicUsize::new(0),
    random_reads: AtomicUsize::new(0),

    transactions_started: AtomicUsize::new(0),
    transactions_committed: AtomicUsize::new(0),
    transactions_aborted: AtomicUsize::new(0),
    transaction_duration_ns: AtomicU64::new(0),
    active_transactions: AtomicUsize::new(0),
    deadlocks_detected: AtomicUsize::new(0),

    lock_requests: AtomicUsize::new(0),
    lock_grants_immediate: AtomicUsize::new(0),
    lock_waits: AtomicUsize::new(0),
    lock_wait_time_ns: AtomicU64::new(0),
    lock_conflicts: AtomicUsize::new(0),
    deadlock_aborts: AtomicUsize::new(0),
    lock_timeouts: AtomicUsize::new(0),

    version_chain_traversals: AtomicUsize::new(0),
    avg_version_chain_length: AtomicUsize::new(0),
    undo_log_entries: AtomicUsize::new(0),
    version_cleanup_operations: AtomicUsize::new(0),
    tuple_visibility_checks: AtomicUsize::new(0),
    snapshot_creation_time_ns: AtomicU64::new(0),

    select_operations: AtomicUsize::new(0),
    update_operations: AtomicUsize::new(0),
    delete_operations: AtomicUsize::new(0),
    join_operations: AtomicUsize::new(0),
    aggregation_operations: AtomicUsize::new(0),
    query_execution_time_ns: AtomicU64::new(0),
    rows_scanned: AtomicUsize::new(0),
    rows_returned: AtomicUsize::new(0),

    heap_allocations: AtomicUsize::new(0),
    memory_pool_usage: AtomicUsize::new(0),
    cache_memory_usage: AtomicUsize::new(0),

    concurrent_readers: AtomicUsize::new(0),
    concurrent_writers: AtomicUsize::new(0),
    lock_table_size: AtomicUsize::new(0),
    wait_for_graph_size: AtomicUsize::new(0),
};

// === RECORDING FUNCTIONS ===

/// Records insert operation performance
pub fn record_insert_performance(row_count: usize, duration: Duration, is_bulk: bool) {
    PERFORMANCE_METRICS
        .total_inserts
        .fetch_add(row_count, Ordering::Relaxed);
    if is_bulk {
        PERFORMANCE_METRICS
            .bulk_inserts
            .fetch_add(row_count, Ordering::Relaxed);
    }

    let duration_ns = duration.as_nanos() as u64;
    PERFORMANCE_METRICS
        .insert_latency_ns
        .store(duration_ns, Ordering::Relaxed);

    if duration.as_secs() > 0 {
        let throughput = (row_count as u64) / duration.as_secs();
        PERFORMANCE_METRICS
            .insert_throughput_rows_per_sec
            .store(throughput, Ordering::Relaxed);
    }
}

/// Records buffer pool operation
pub fn record_buffer_pool_operation(hit: bool, pin_duration: Duration) {
    if hit {
        PERFORMANCE_METRICS
            .buffer_hits
            .fetch_add(1, Ordering::Relaxed);
    } else {
        PERFORMANCE_METRICS
            .buffer_misses
            .fetch_add(1, Ordering::Relaxed);
    }

    PERFORMANCE_METRICS
        .page_pin_duration_ns
        .store(pin_duration.as_nanos() as u64, Ordering::Relaxed);
}

/// Records disk I/O operation
pub fn record_disk_io(is_write: bool, latency: Duration, is_sequential: bool) {
    if is_write {
        PERFORMANCE_METRICS
            .disk_writes
            .fetch_add(1, Ordering::Relaxed);
        PERFORMANCE_METRICS
            .disk_write_latency_ns
            .store(latency.as_nanos() as u64, Ordering::Relaxed);
    } else {
        PERFORMANCE_METRICS
            .disk_reads
            .fetch_add(1, Ordering::Relaxed);
        PERFORMANCE_METRICS
            .disk_read_latency_ns
            .store(latency.as_nanos() as u64, Ordering::Relaxed);

        if is_sequential {
            PERFORMANCE_METRICS
                .sequential_reads
                .fetch_add(1, Ordering::Relaxed);
        } else {
            PERFORMANCE_METRICS
                .random_reads
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Records transaction operation
pub fn record_transaction_operation(operation: &str, duration: Duration) {
    match operation {
        "begin" => PERFORMANCE_METRICS
            .transactions_started
            .fetch_add(1, Ordering::Relaxed),
        "commit" => PERFORMANCE_METRICS
            .transactions_committed
            .fetch_add(1, Ordering::Relaxed),
        "abort" => PERFORMANCE_METRICS
            .transactions_aborted
            .fetch_add(1, Ordering::Relaxed),
        _ => 0,
    };

    PERFORMANCE_METRICS
        .transaction_duration_ns
        .store(duration.as_nanos() as u64, Ordering::Relaxed);
}

/// Records lock manager operation
pub fn record_lock_operation(granted_immediately: bool, wait_time: Duration, conflict: bool) {
    PERFORMANCE_METRICS
        .lock_requests
        .fetch_add(1, Ordering::Relaxed);

    if granted_immediately {
        PERFORMANCE_METRICS
            .lock_grants_immediate
            .fetch_add(1, Ordering::Relaxed);
    } else {
        PERFORMANCE_METRICS
            .lock_waits
            .fetch_add(1, Ordering::Relaxed);
        PERFORMANCE_METRICS
            .lock_wait_time_ns
            .store(wait_time.as_nanos() as u64, Ordering::Relaxed);
    }

    if conflict {
        PERFORMANCE_METRICS
            .lock_conflicts
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Records MVCC operation
pub fn record_mvcc_operation(operation: &str, chain_length: usize, duration: Duration) {
    match operation {
        "version_traversal" => {
            PERFORMANCE_METRICS
                .version_chain_traversals
                .fetch_add(1, Ordering::Relaxed);
            PERFORMANCE_METRICS
                .avg_version_chain_length
                .store(chain_length, Ordering::Relaxed);
        },
        "visibility_check" => {
            PERFORMANCE_METRICS
                .tuple_visibility_checks
                .fetch_add(1, Ordering::Relaxed);
        },
        "snapshot_creation" => {
            PERFORMANCE_METRICS
                .snapshot_creation_time_ns
                .store(duration.as_nanos() as u64, Ordering::Relaxed);
        },
        _ => {},
    }
}

/// Records query execution metrics
pub fn record_query_execution(
    operation: &str,
    rows_scanned: usize,
    rows_returned: usize,
    duration: Duration,
) {
    match operation {
        "select" => PERFORMANCE_METRICS
            .select_operations
            .fetch_add(1, Ordering::Relaxed),
        "update" => PERFORMANCE_METRICS
            .update_operations
            .fetch_add(1, Ordering::Relaxed),
        "delete" => PERFORMANCE_METRICS
            .delete_operations
            .fetch_add(1, Ordering::Relaxed),
        "join" => PERFORMANCE_METRICS
            .join_operations
            .fetch_add(1, Ordering::Relaxed),
        "aggregation" => PERFORMANCE_METRICS
            .aggregation_operations
            .fetch_add(1, Ordering::Relaxed),
        _ => 0,
    };

    PERFORMANCE_METRICS
        .rows_scanned
        .fetch_add(rows_scanned, Ordering::Relaxed);
    PERFORMANCE_METRICS
        .rows_returned
        .fetch_add(rows_returned, Ordering::Relaxed);
    PERFORMANCE_METRICS
        .query_execution_time_ns
        .store(duration.as_nanos() as u64, Ordering::Relaxed);
}

// === ANALYSIS FUNCTIONS ===

/// Analyzes buffer pool performance
pub fn analyze_buffer_pool_performance() -> f64 {
    let hits = PERFORMANCE_METRICS.buffer_hits.load(Ordering::Relaxed);
    let misses = PERFORMANCE_METRICS.buffer_misses.load(Ordering::Relaxed);
    let total = hits + misses;

    if total == 0 {
        return 1.0; // No data, assume perfect
    }

    let hit_ratio = hits as f64 / total as f64;
    let evictions = PERFORMANCE_METRICS.page_evictions.load(Ordering::Relaxed);
    let eviction_pressure = if total > 0 {
        evictions as f64 / total as f64
    } else {
        0.0
    };

    // Score based on hit ratio and eviction pressure
    let score = hit_ratio * (1.0 - eviction_pressure.min(0.5));
    score.clamp(0.0, 1.0)
}

/// Analyzes disk I/O performance
pub fn analyze_disk_io_performance() -> f64 {
    let read_latency = PERFORMANCE_METRICS
        .disk_read_latency_ns
        .load(Ordering::Relaxed);
    let write_latency = PERFORMANCE_METRICS
        .disk_write_latency_ns
        .load(Ordering::Relaxed);
    let sequential = PERFORMANCE_METRICS.sequential_reads.load(Ordering::Relaxed);
    let random = PERFORMANCE_METRICS.random_reads.load(Ordering::Relaxed);

    // Good thresholds: <1ms for sequential, <5ms for random
    let avg_latency = if read_latency > 0 && write_latency > 0 {
        (read_latency + write_latency) / 2
    } else if read_latency > 0 {
        read_latency
    } else if write_latency > 0 {
        write_latency
    } else {
        return 1.0; // No data
    };

    let latency_score = if avg_latency < 1_000_000 {
        // < 1ms
        1.0
    } else if avg_latency < 5_000_000 {
        // < 5ms
        0.8
    } else if avg_latency < 10_000_000 {
        // < 10ms
        0.6
    } else {
        0.3
    };

    // Bonus for sequential reads
    let sequentiality_bonus = if sequential + random > 0 {
        (sequential as f64 / (sequential + random) as f64) * 0.2
    } else {
        0.0
    };

    (latency_score + sequentiality_bonus).min(1.0)
}

/// Analyzes lock manager performance
pub fn analyze_lock_manager_performance() -> f64 {
    let requests = PERFORMANCE_METRICS.lock_requests.load(Ordering::Relaxed);
    let immediate_grants = PERFORMANCE_METRICS
        .lock_grants_immediate
        .load(Ordering::Relaxed);
    let conflicts = PERFORMANCE_METRICS.lock_conflicts.load(Ordering::Relaxed);
    let wait_time = PERFORMANCE_METRICS
        .lock_wait_time_ns
        .load(Ordering::Relaxed);

    if requests == 0 {
        return 1.0; // No requests
    }

    let immediate_grant_ratio = immediate_grants as f64 / requests as f64;
    let conflict_ratio = conflicts as f64 / requests as f64;
    let avg_wait_time_ms = wait_time as f64 / 1_000_000.0;

    // Score based on immediate grants, low conflicts, and low wait times
    let grant_score = immediate_grant_ratio;
    let conflict_penalty = conflict_ratio * 0.5;
    let wait_penalty = (avg_wait_time_ms / 100.0).min(0.5); // Penalize >100ms waits

    (grant_score - conflict_penalty - wait_penalty).clamp(0.0, 1.0)
}

/// Analyzes MVCC performance
pub fn analyze_mvcc_performance() -> f64 {
    let chain_length = PERFORMANCE_METRICS
        .avg_version_chain_length
        .load(Ordering::Relaxed);
    let traversals = PERFORMANCE_METRICS
        .version_chain_traversals
        .load(Ordering::Relaxed);
    let visibility_checks = PERFORMANCE_METRICS
        .tuple_visibility_checks
        .load(Ordering::Relaxed);

    // Optimal chain length is 1-3, performance degrades with longer chains
    let chain_score = if chain_length <= 3 {
        1.0
    } else if chain_length <= 10 {
        0.8
    } else if chain_length <= 20 {
        0.5
    } else {
        0.2
    };

    // High traversal rate indicates version chain pressure
    let traversal_pressure = if visibility_checks > 0 {
        (traversals as f64 / visibility_checks as f64).min(1.0)
    } else {
        0.0
    };

    (chain_score * (1.0 - traversal_pressure * 0.3)).max(0.0)
}

/// Performs comprehensive bottleneck analysis
pub fn analyze_bottlenecks() -> BottleneckAnalysis {
    let buffer_score = analyze_buffer_pool_performance();
    let disk_score = analyze_disk_io_performance();
    let lock_score = analyze_lock_manager_performance();
    let mvcc_score = analyze_mvcc_performance();

    let scores = [
        ("Buffer Pool", buffer_score),
        ("Disk I/O", disk_score),
        ("Lock Manager", lock_score),
        ("MVCC", mvcc_score),
    ];

    // Find the worst performing component
    let worst = scores
        .iter()
        .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
        .unwrap();

    let mut recommendations = Vec::new();
    let mut affected_operations = Vec::new();

    match worst.0 {
        "Buffer Pool" => {
            recommendations.push("Increase buffer pool size".to_string());
            recommendations.push("Optimize page replacement policy".to_string());
            affected_operations.extend(vec![
                "INSERT".to_string(),
                "SELECT".to_string(),
                "UPDATE".to_string(),
            ]);
        },
        "Disk I/O" => {
            recommendations.push("Use faster storage (SSD)".to_string());
            recommendations.push("Implement better I/O scheduling".to_string());
            recommendations.push("Increase batch sizes".to_string());
            affected_operations.extend(vec!["All operations".to_string()]);
        },
        "Lock Manager" => {
            recommendations.push("Reduce lock granularity".to_string());
            recommendations.push("Optimize lock scheduling".to_string());
            recommendations.push("Consider optimistic concurrency".to_string());
            affected_operations.extend(vec!["Concurrent transactions".to_string()]);
        },
        "MVCC" => {
            recommendations.push("Implement more aggressive garbage collection".to_string());
            recommendations.push("Optimize version chain traversal".to_string());
            recommendations.push("Cache frequently accessed versions".to_string());
            affected_operations.extend(vec![
                "READ operations".to_string(),
                "Long transactions".to_string(),
            ]);
        },
        _ => {},
    }

    let mut metrics_snapshot = HashMap::new();
    metrics_snapshot.insert(
        "buffer_hit_ratio".to_string(),
        ((PERFORMANCE_METRICS.buffer_hits.load(Ordering::Relaxed) as f64
            / (PERFORMANCE_METRICS.buffer_hits.load(Ordering::Relaxed)
                + PERFORMANCE_METRICS.buffer_misses.load(Ordering::Relaxed)) as f64)
            * 100.0) as u64,
    );
    metrics_snapshot.insert(
        "avg_disk_latency_ms".to_string(),
        PERFORMANCE_METRICS
            .disk_read_latency_ns
            .load(Ordering::Relaxed)
            / 1_000_000,
    );
    metrics_snapshot.insert(
        "lock_conflicts".to_string(),
        PERFORMANCE_METRICS.lock_conflicts.load(Ordering::Relaxed) as u64,
    );
    metrics_snapshot.insert(
        "avg_version_chain_length".to_string(),
        PERFORMANCE_METRICS
            .avg_version_chain_length
            .load(Ordering::Relaxed) as u64,
    );

    BottleneckAnalysis {
        primary_bottleneck: worst.0.to_string(),
        bottleneck_severity: 1.0 - worst.1,
        affected_operations,
        recommendations,
        metrics_snapshot,
    }
}

/// Prints comprehensive performance summary
pub fn print_performance_summary() {
    let bottleneck_analysis = analyze_bottlenecks();

    println!("=== COMPREHENSIVE FERRITE PERFORMANCE ANALYSIS ===");

    // === THROUGHPUT METRICS ===
    println!("📊 THROUGHPUT METRICS:");
    let total_inserts = PERFORMANCE_METRICS.total_inserts.load(Ordering::Relaxed);
    let bulk_ratio = if total_inserts > 0 {
        (PERFORMANCE_METRICS.bulk_inserts.load(Ordering::Relaxed) as f64 / total_inserts as f64)
            * 100.0
    } else {
        0.0
    };

    println!("  Insert Performance:");
    println!("    Total Inserts: {}", total_inserts);
    println!("    Bulk Insert Ratio: {:.1}%", bulk_ratio);
    println!(
        "    Current Throughput: {} rows/sec",
        PERFORMANCE_METRICS
            .insert_throughput_rows_per_sec
            .load(Ordering::Relaxed)
    );

    // === BUFFER POOL ANALYSIS ===
    let buffer_hits = PERFORMANCE_METRICS.buffer_hits.load(Ordering::Relaxed);
    let buffer_misses = PERFORMANCE_METRICS.buffer_misses.load(Ordering::Relaxed);
    let hit_ratio = if buffer_hits + buffer_misses > 0 {
        (buffer_hits as f64 / (buffer_hits + buffer_misses) as f64) * 100.0
    } else {
        0.0
    };

    println!("🎯 BUFFER POOL ANALYSIS:");
    println!("  Hit Ratio: {:.2}% (Target: >95%)", hit_ratio);
    println!(
        "  Page Evictions: {}",
        PERFORMANCE_METRICS.page_evictions.load(Ordering::Relaxed)
    );
    println!(
        "  Dirty Flushes: {}",
        PERFORMANCE_METRICS
            .dirty_page_flushes
            .load(Ordering::Relaxed)
    );
    println!(
        "  Performance Score: {:.2}/1.0",
        analyze_buffer_pool_performance()
    );

    if hit_ratio < 95.0 {
        warn!("  ⚠️  LOW BUFFER HIT RATIO - Consider increasing buffer pool size");
    }

    // === DISK I/O ANALYSIS ===
    let read_latency_ms = PERFORMANCE_METRICS
        .disk_read_latency_ns
        .load(Ordering::Relaxed) as f64
        / 1_000_000.0;
    let write_latency_ms = PERFORMANCE_METRICS
        .disk_write_latency_ns
        .load(Ordering::Relaxed) as f64
        / 1_000_000.0;

    println!("💾 DISK I/O ANALYSIS:");
    println!(
        "  Reads: {}, Writes: {}",
        PERFORMANCE_METRICS.disk_reads.load(Ordering::Relaxed),
        PERFORMANCE_METRICS.disk_writes.load(Ordering::Relaxed)
    );
    println!(
        "  Read Latency: {:.2}ms, Write Latency: {:.2}ms",
        read_latency_ms, write_latency_ms
    );
    println!(
        "  Sequential vs Random Reads: {} vs {}",
        PERFORMANCE_METRICS.sequential_reads.load(Ordering::Relaxed),
        PERFORMANCE_METRICS.random_reads.load(Ordering::Relaxed)
    );
    println!(
        "  Performance Score: {:.2}/1.0",
        analyze_disk_io_performance()
    );

    if read_latency_ms > 5.0 || write_latency_ms > 5.0 {
        warn!("  ⚠️  HIGH DISK LATENCY - Consider SSD upgrade or I/O optimization");
    }

    // === CONCURRENCY ANALYSIS ===
    let lock_requests = PERFORMANCE_METRICS.lock_requests.load(Ordering::Relaxed);
    let immediate_grants = PERFORMANCE_METRICS
        .lock_grants_immediate
        .load(Ordering::Relaxed);
    let conflicts = PERFORMANCE_METRICS.lock_conflicts.load(Ordering::Relaxed);

    println!("🔒 CONCURRENCY ANALYSIS:");
    println!("  Lock Requests: {}", lock_requests);
    println!(
        "  Immediate Grants: {} ({:.1}%)",
        immediate_grants,
        if lock_requests > 0 {
            (immediate_grants as f64 / lock_requests as f64) * 100.0
        } else {
            0.0
        }
    );
    println!(
        "  Lock Conflicts: {} ({:.1}%)",
        conflicts,
        if lock_requests > 0 {
            (conflicts as f64 / lock_requests as f64) * 100.0
        } else {
            0.0
        }
    );
    println!(
        "  Lock Manager Score: {:.2}/1.0",
        analyze_lock_manager_performance()
    );

    // === MVCC ANALYSIS ===
    println!("🔄 MVCC ANALYSIS:");
    println!(
        "  Version Chain Length: {}",
        PERFORMANCE_METRICS
            .avg_version_chain_length
            .load(Ordering::Relaxed)
    );
    println!(
        "  Version Traversals: {}",
        PERFORMANCE_METRICS
            .version_chain_traversals
            .load(Ordering::Relaxed)
    );
    println!(
        "  Visibility Checks: {}",
        PERFORMANCE_METRICS
            .tuple_visibility_checks
            .load(Ordering::Relaxed)
    );
    println!("  MVCC Score: {:.2}/1.0", analyze_mvcc_performance());

    // === TRANSACTION ANALYSIS ===
    let total_txns = PERFORMANCE_METRICS
        .transactions_started
        .load(Ordering::Relaxed);
    let committed = PERFORMANCE_METRICS
        .transactions_committed
        .load(Ordering::Relaxed);
    let aborted = PERFORMANCE_METRICS
        .transactions_aborted
        .load(Ordering::Relaxed);

    println!("📋 TRANSACTION ANALYSIS:");
    println!(
        "  Started: {}, Committed: {}, Aborted: {}",
        total_txns, committed, aborted
    );
    if total_txns > 0 {
        println!(
            "  Success Rate: {:.1}%",
            (committed as f64 / total_txns as f64) * 100.0
        );
    }
    println!(
        "  Active Transactions: {}",
        PERFORMANCE_METRICS
            .active_transactions
            .load(Ordering::Relaxed)
    );

    // === BOTTLENECK IDENTIFICATION ===
    println!("🚨 BOTTLENECK ANALYSIS:");
    println!(
        "  Primary Bottleneck: {}",
        bottleneck_analysis.primary_bottleneck
    );
    println!(
        "  Severity: {:.1}% impact",
        bottleneck_analysis.bottleneck_severity * 100.0
    );
    println!(
        "  Affected Operations: {}",
        bottleneck_analysis.affected_operations.join(", ")
    );

    println!("💡 OPTIMIZATION RECOMMENDATIONS:");
    for (i, rec) in bottleneck_analysis.recommendations.iter().enumerate() {
        println!("  {}. {}", i + 1, rec);
    }

    // === PERFORMANCE GRADES ===
    println!("📊 SUBSYSTEM PERFORMANCE GRADES:");
    println!(
        "  Buffer Pool: {}",
        get_performance_grade(analyze_buffer_pool_performance())
    );
    println!(
        "  Disk I/O: {}",
        get_performance_grade(analyze_disk_io_performance())
    );
    println!(
        "  Lock Manager: {}",
        get_performance_grade(analyze_lock_manager_performance())
    );
    println!(
        "  MVCC: {}",
        get_performance_grade(analyze_mvcc_performance())
    );

    // === QUERY EXECUTION ANALYSIS ===
    let total_ops = PERFORMANCE_METRICS
        .select_operations
        .load(Ordering::Relaxed)
        + PERFORMANCE_METRICS
            .update_operations
            .load(Ordering::Relaxed)
        + PERFORMANCE_METRICS
            .delete_operations
            .load(Ordering::Relaxed);

    if total_ops > 0 {
        println!("📈 QUERY EXECUTION ANALYSIS:");
        println!("  Total Operations: {}", total_ops);
        println!(
            "  Selects: {}, Updates: {}, Deletes: {}",
            PERFORMANCE_METRICS
                .select_operations
                .load(Ordering::Relaxed),
            PERFORMANCE_METRICS
                .update_operations
                .load(Ordering::Relaxed),
            PERFORMANCE_METRICS
                .delete_operations
                .load(Ordering::Relaxed)
        );

        let rows_scanned = PERFORMANCE_METRICS.rows_scanned.load(Ordering::Relaxed);
        let rows_returned = PERFORMANCE_METRICS.rows_returned.load(Ordering::Relaxed);
        if rows_scanned > 0 {
            let selectivity = (rows_returned as f64 / rows_scanned as f64) * 100.0;
            println!(
                "  Query Selectivity: {:.1}% ({} returned / {} scanned)",
                selectivity, rows_returned, rows_scanned
            );

            if selectivity < 10.0 && rows_scanned > 1000 {
                warn!("  ⚠️  LOW QUERY SELECTIVITY - Consider adding indexes");
            }
        }

        let avg_exec_time_ms = PERFORMANCE_METRICS
            .query_execution_time_ns
            .load(Ordering::Relaxed) as f64
            / 1_000_000.0;
        println!("  Average Execution Time: {:.2}ms", avg_exec_time_ms);

        if avg_exec_time_ms > 100.0 {
            warn!("  ⚠️  SLOW QUERIES DETECTED - Consider query optimization");
        }
    }

    // === OVERALL SYSTEM HEALTH ===
    let overall_score = (analyze_buffer_pool_performance()
        + analyze_disk_io_performance()
        + analyze_lock_manager_performance()
        + analyze_mvcc_performance())
        / 4.0;

    println!(
        "🏥 OVERALL SYSTEM HEALTH: {} ({:.2}/1.0)",
        get_performance_grade(overall_score),
        overall_score
    );

    if overall_score < 0.6 {
        warn!("💔 CRITICAL: SYSTEM PERFORMANCE IS SEVERELY DEGRADED");
        warn!("   🚨 IMMEDIATE ACTION REQUIRED");
    } else if overall_score < 0.7 {
        warn!("⚠️  WARNING: SYSTEM PERFORMANCE IS BELOW OPTIMAL");
        warn!("   📈 OPTIMIZATION RECOMMENDED");
    } else if overall_score < 0.8 {
        println!("📊 NOTICE: SYSTEM PERFORMANCE COULD BE IMPROVED");
    } else {
        println!("✅ EXCELLENT: SYSTEM PERFORMANCE IS OPTIMAL");
    }

    // === REAL-TIME ALERTS ===
    print_real_time_alerts();
}

/// Converts performance score to letter grade
fn get_performance_grade(score: f64) -> String {
    match score {
        s if s >= 0.9 => "A+ (Excellent)".to_string(),
        s if s >= 0.8 => "A (Good)".to_string(),
        s if s >= 0.7 => "B (Fair)".to_string(),
        s if s >= 0.6 => "C (Poor)".to_string(),
        _ => "F (Critical)".to_string(),
    }
}

/// Prints real-time performance alerts for critical conditions
pub fn print_real_time_alerts() {
    println!("🚨 REAL-TIME PERFORMANCE ALERTS:");

    let mut alerts = Vec::new();

    // Buffer pool alerts
    let buffer_hits = PERFORMANCE_METRICS.buffer_hits.load(Ordering::Relaxed);
    let buffer_misses = PERFORMANCE_METRICS.buffer_misses.load(Ordering::Relaxed);
    if buffer_hits + buffer_misses > 100 {
        let hit_ratio = (buffer_hits as f64 / (buffer_hits + buffer_misses) as f64) * 100.0;
        if hit_ratio < 80.0 {
            alerts.push(format!(
                "🔴 CRITICAL: Buffer hit ratio only {:.1}% (target: >95%)",
                hit_ratio
            ));
        } else if hit_ratio < 95.0 {
            alerts.push(format!(
                "🟡 WARNING: Buffer hit ratio {:.1}% (target: >95%)",
                hit_ratio
            ));
        }
    }

    // Disk I/O alerts
    let read_latency_ms = PERFORMANCE_METRICS
        .disk_read_latency_ns
        .load(Ordering::Relaxed) as f64
        / 1_000_000.0;
    let write_latency_ms = PERFORMANCE_METRICS
        .disk_write_latency_ns
        .load(Ordering::Relaxed) as f64
        / 1_000_000.0;

    if read_latency_ms > 20.0 {
        alerts.push(format!(
            "🔴 CRITICAL: Disk read latency {:.1}ms (target: <5ms)",
            read_latency_ms
        ));
    } else if read_latency_ms > 10.0 {
        alerts.push(format!(
            "🟡 WARNING: Disk read latency {:.1}ms (target: <5ms)",
            read_latency_ms
        ));
    }

    if write_latency_ms > 20.0 {
        alerts.push(format!(
            "🔴 CRITICAL: Disk write latency {:.1}ms (target: <5ms)",
            write_latency_ms
        ));
    } else if write_latency_ms > 10.0 {
        alerts.push(format!(
            "🟡 WARNING: Disk write latency {:.1}ms (target: <5ms)",
            write_latency_ms
        ));
    }

    // Lock contention alerts
    let lock_requests = PERFORMANCE_METRICS.lock_requests.load(Ordering::Relaxed);
    let conflicts = PERFORMANCE_METRICS.lock_conflicts.load(Ordering::Relaxed);
    if lock_requests > 0 {
        let conflict_rate = (conflicts as f64 / lock_requests as f64) * 100.0;
        if conflict_rate > 20.0 {
            alerts.push(format!(
                "🔴 CRITICAL: Lock conflict rate {:.1}% (target: <5%)",
                conflict_rate
            ));
        } else if conflict_rate > 10.0 {
            alerts.push(format!(
                "🟡 WARNING: Lock conflict rate {:.1}% (target: <5%)",
                conflict_rate
            ));
        }
    }

    // MVCC alerts
    let chain_length = PERFORMANCE_METRICS
        .avg_version_chain_length
        .load(Ordering::Relaxed);
    if chain_length > 50 {
        alerts.push(format!(
            "🔴 CRITICAL: Version chain length {} (target: <10)",
            chain_length
        ));
    } else if chain_length > 20 {
        alerts.push(format!(
            "🟡 WARNING: Version chain length {} (target: <10)",
            chain_length
        ));
    }

    // Transaction alerts
    let total_txns = PERFORMANCE_METRICS
        .transactions_started
        .load(Ordering::Relaxed);
    let aborted = PERFORMANCE_METRICS
        .transactions_aborted
        .load(Ordering::Relaxed);
    if total_txns > 0 {
        let abort_rate = (aborted as f64 / total_txns as f64) * 100.0;
        if abort_rate > 20.0 {
            alerts.push(format!(
                "🔴 CRITICAL: Transaction abort rate {:.1}% (target: <5%)",
                abort_rate
            ));
        } else if abort_rate > 10.0 {
            alerts.push(format!(
                "🟡 WARNING: Transaction abort rate {:.1}% (target: <5%)",
                abort_rate
            ));
        }
    }

    // Throughput alerts
    let throughput = PERFORMANCE_METRICS
        .insert_throughput_rows_per_sec
        .load(Ordering::Relaxed);
    if throughput > 0 && throughput < 1000 {
        alerts.push(format!(
            "🟡 WARNING: Low insert throughput {} rows/sec",
            throughput
        ));
    }

    if alerts.is_empty() {
        println!("  ✅ No performance alerts - all systems operating normally");
    } else {
        for alert in alerts {
            warn!("  {}", alert);
        }
    }
}

/// Generates a performance report summary for external monitoring
pub fn get_performance_summary_json() -> String {
    let bottleneck_analysis = analyze_bottlenecks();

    let buffer_hits = PERFORMANCE_METRICS.buffer_hits.load(Ordering::Relaxed);
    let buffer_misses = PERFORMANCE_METRICS.buffer_misses.load(Ordering::Relaxed);
    let hit_ratio = if buffer_hits + buffer_misses > 0 {
        (buffer_hits as f64 / (buffer_hits + buffer_misses) as f64) * 100.0
    } else {
        0.0
    };

    let overall_score = (analyze_buffer_pool_performance()
        + analyze_disk_io_performance()
        + analyze_lock_manager_performance()
        + analyze_mvcc_performance())
        / 4.0;

    format!(
        r#"{{
  "timestamp": "{}",
  "overall_health_score": {:.2},
  "primary_bottleneck": "{}",
  "bottleneck_severity": {:.2},
  "metrics": {{
    "buffer_hit_ratio": {:.2},
    "disk_read_latency_ms": {:.2},
    "disk_write_latency_ms": {:.2},
    "lock_conflict_rate": {:.2},
    "version_chain_length": {},
    "transaction_success_rate": {:.2},
    "insert_throughput": {}
  }},
  "subsystem_scores": {{
    "buffer_pool": {:.2},
    "disk_io": {:.2},
    "lock_manager": {:.2},
    "mvcc": {:.2}
  }},
  "recommendations": {:?}
}}"#,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        overall_score,
        bottleneck_analysis.primary_bottleneck,
        bottleneck_analysis.bottleneck_severity,
        hit_ratio,
        PERFORMANCE_METRICS
            .disk_read_latency_ns
            .load(Ordering::Relaxed) as f64
            / 1_000_000.0,
        PERFORMANCE_METRICS
            .disk_write_latency_ns
            .load(Ordering::Relaxed) as f64
            / 1_000_000.0,
        if PERFORMANCE_METRICS.lock_requests.load(Ordering::Relaxed) > 0 {
            (PERFORMANCE_METRICS.lock_conflicts.load(Ordering::Relaxed) as f64
                / PERFORMANCE_METRICS.lock_requests.load(Ordering::Relaxed) as f64)
                * 100.0
        } else {
            0.0
        },
        PERFORMANCE_METRICS
            .avg_version_chain_length
            .load(Ordering::Relaxed),
        if PERFORMANCE_METRICS
            .transactions_started
            .load(Ordering::Relaxed)
            > 0
        {
            (PERFORMANCE_METRICS
                .transactions_committed
                .load(Ordering::Relaxed) as f64
                / PERFORMANCE_METRICS
                    .transactions_started
                    .load(Ordering::Relaxed) as f64)
                * 100.0
        } else {
            0.0
        },
        PERFORMANCE_METRICS
            .insert_throughput_rows_per_sec
            .load(Ordering::Relaxed),
        analyze_buffer_pool_performance(),
        analyze_disk_io_performance(),
        analyze_lock_manager_performance(),
        analyze_mvcc_performance(),
        bottleneck_analysis.recommendations
    )
}

/// Record system resource usage
pub fn record_system_resources(cpu_usage: f64, memory_usage_mb: u64, disk_usage_percent: f64) {
    // For now, just log the values - in a real system you'd store these
    debug!(
        "System Resources: CPU: {:.1}%, Memory: {}MB, Disk: {:.1}%",
        cpu_usage, memory_usage_mb, disk_usage_percent
    );

    if cpu_usage > 90.0 {
        warn!("🔴 CRITICAL: CPU usage at {:.1}%", cpu_usage);
    } else if cpu_usage > 80.0 {
        warn!("🟡 WARNING: High CPU usage at {:.1}%", cpu_usage);
    }

    if memory_usage_mb > 8192 {
        // 8GB threshold
        warn!("🟡 WARNING: High memory usage at {}MB", memory_usage_mb);
    }

    if disk_usage_percent > 90.0 {
        warn!("🔴 CRITICAL: Disk usage at {:.1}%", disk_usage_percent);
    } else if disk_usage_percent > 80.0 {
        warn!("🟡 WARNING: High disk usage at {:.1}%", disk_usage_percent);
    }
}

/// Update active transaction count
pub fn update_active_transaction_count(delta: i32) {
    if delta > 0 {
        PERFORMANCE_METRICS
            .active_transactions
            .fetch_add(delta as usize, Ordering::Relaxed);
    } else if delta < 0 {
        PERFORMANCE_METRICS
            .active_transactions
            .fetch_sub((-delta) as usize, Ordering::Relaxed);
    }
}

/// Resets all performance metrics
pub fn reset_metrics() {
    // Reset all atomic values to 0
    PERFORMANCE_METRICS
        .total_inserts
        .store(0, Ordering::Relaxed);
    PERFORMANCE_METRICS.bulk_inserts.store(0, Ordering::Relaxed);
    PERFORMANCE_METRICS.buffer_hits.store(0, Ordering::Relaxed);
    PERFORMANCE_METRICS
        .buffer_misses
        .store(0, Ordering::Relaxed);
    PERFORMANCE_METRICS.disk_reads.store(0, Ordering::Relaxed);
    PERFORMANCE_METRICS.disk_writes.store(0, Ordering::Relaxed);
    PERFORMANCE_METRICS
        .lock_requests
        .store(0, Ordering::Relaxed);
    PERFORMANCE_METRICS
        .transactions_started
        .store(0, Ordering::Relaxed);
    PERFORMANCE_METRICS
        .transactions_committed
        .store(0, Ordering::Relaxed);
    PERFORMANCE_METRICS
        .transactions_aborted
        .store(0, Ordering::Relaxed);
    PERFORMANCE_METRICS
        .lock_conflicts
        .store(0, Ordering::Relaxed);
    PERFORMANCE_METRICS
        .version_chain_traversals
        .store(0, Ordering::Relaxed);
    PERFORMANCE_METRICS
        .select_operations
        .store(0, Ordering::Relaxed);
    PERFORMANCE_METRICS
        .update_operations
        .store(0, Ordering::Relaxed);
    PERFORMANCE_METRICS
        .delete_operations
        .store(0, Ordering::Relaxed);

    println!("Performance metrics reset");
}
