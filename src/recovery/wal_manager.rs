//! # WAL Manager
//!
//! This module provides the `WALManager`, a high-level facade over the `LogManager`
//! that offers a transaction-oriented API for Write-Ahead Logging (WAL) operations.
//! It simplifies creating and appending log records for common transaction lifecycle
//! events.
//!
//! ## Architecture
//!
//! ```text
//!   Transaction Layer                    WAL Layer
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   ┌─────────────────┐
//!   │   Transaction   │
//!   │                 │
//!   │  txn_id: 42     │
//!   │  prev_lsn: 10   │
//!   └────────┬────────┘
//!            │
//!            │  write_begin_record(txn)
//!            │  write_update_record(txn, rid, old, new)
//!            │  write_commit_record(txn)
//!            ▼
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                            WALManager                                   │
//!   │                                                                         │
//!   │   Provides high-level API:              Delegates to:                   │
//!   │   ┌─────────────────────────┐          ┌─────────────────────────┐      │
//!   │   │ write_begin_record()    │─────────▶│                         │      │
//!   │   │ write_commit_record()   │─────────▶│      LogManager         │      │
//!   │   │ write_abort_record()    │─────────▶│  append_log_record()    │      │
//!   │   │ write_update_record()   │─────────▶│                         │      │
//!   │   └─────────────────────────┘          └─────────────────────────┘      │
//!   └─────────────────────────────────────────────────────────────────────────┘
//!                                                       │
//!                                                       ▼
//!                                              ┌─────────────────┐
//!                                              │   Disk (WAL)    │
//!                                              └─────────────────┘
//! ```
//!
//! ## Transaction Logging Flow
//!
//! ```text
//!   Typical Transaction Lifecycle
//!   ─────────────────────────────────────────────────────────────────────────
//!
//!   ┌──────────────────┐
//!   │ BEGIN TRANSACTION│
//!   └────────┬─────────┘
//!            │ write_begin_record(txn)
//!            │ → LogRecord { type: Begin, txn_id, prev_lsn: INVALID }
//!            │ → returns LSN 0
//!            ▼
//!   ┌──────────────────┐
//!   │ UPDATE row       │
//!   └────────┬─────────┘
//!            │ write_update_record(txn, rid, old_tuple, new_tuple)
//!            │ → LogRecord { type: Update, txn_id, prev_lsn: 0, ... }
//!            │ → returns LSN 1
//!            ▼
//!   ┌──────────────────┐
//!   │ COMMIT           │
//!   └────────┬─────────┘
//!            │ write_commit_record(txn)
//!            │ → LogRecord { type: Commit, txn_id, prev_lsn: 1 }
//!            │ → returns LSN 2 (blocks until durable)
//!            ▼
//!       Transaction Complete
//! ```
//!
//! ## Key Operations
//!
//! | Method | Record Type | Blocks? | Description |
//! |--------|-------------|---------|-------------|
//! | `write_begin_record()` | `Begin` | No | Start transaction logging |
//! | `write_update_record()` | `Update` | No | Log tuple modification |
//! | `write_commit_record()` | `Commit` | Yes | Commit (waits for durability) |
//! | `write_abort_record()` | `Abort` | No | Abort transaction |
//! | `force_run_flush_thread()` | N/A | No | Start background flusher |
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::recovery::wal_manager::WALManager;
//!
//! let wal_manager = WALManager::new(log_manager.clone());
//!
//! // Start transaction
//! let begin_lsn = wal_manager.write_begin_record(&txn);
//! txn.set_prev_lsn(begin_lsn);
//!
//! // Perform update
//! let update_lsn = wal_manager.write_update_record(
//!     &txn, rid, old_tuple, new_tuple
//! );
//! txn.set_prev_lsn(update_lsn);
//!
//! // Commit (blocks until WAL is durable)
//! let commit_lsn = wal_manager.write_commit_record(&txn);
//! ```
//!
//! ## Thread Safety
//!
//! - `LogManager` is protected by `RwLock` for concurrent access
//! - Write operations acquire exclusive lock briefly
//! - Commit durability waiting happens outside the lock
//!
//! ## Relationship to Other Components
//!
//! ```text
//!   ┌─────────────────┐
//!   │ TransactionMgr  │ ◀─── manages transaction lifecycle
//!   │   Factory       │
//!   └────────┬────────┘
//!            │ uses
//!            ▼
//!   ┌─────────────────┐
//!   │   WALManager    │ ◀─── this module (high-level WAL API)
//!   └────────┬────────┘
//!            │ wraps
//!            ▼
//!   ┌─────────────────┐
//!   │   LogManager    │ ◀─── low-level log append & flush
//!   └────────┬────────┘
//!            │ uses
//!            ▼
//!   ┌─────────────────┐
//!   │ AsyncDiskManager│ ◀─── physical WAL I/O
//!   └─────────────────┘
//! ```

use crate::common::config::INVALID_LSN;
use crate::common::rid::RID;
use crate::concurrency::transaction::Transaction;
use crate::recovery::log_manager::LogManager;
use crate::recovery::log_record::{LogRecord, LogRecordType};
use crate::storage::table::tuple::Tuple;
use parking_lot::RwLock;
use std::sync::Arc;

/// High-level facade for Write-Ahead Logging operations.
///
/// Wraps `LogManager` to provide a transaction-oriented API for creating
/// and appending log records for BEGIN, COMMIT, ABORT, and UPDATE operations.
pub struct WALManager {
    log_manager: Arc<RwLock<LogManager>>,
}

impl WALManager {
    pub fn new(log_manager: Arc<RwLock<LogManager>>) -> Self {
        Self { log_manager }
    }

    pub fn write_commit_record(&self, txn: &Transaction) -> u64 {
        let commit_record = Arc::new(LogRecord::new_transaction_record(
            txn.get_transaction_id(),
            txn.get_prev_lsn(),
            LogRecordType::Commit,
        ));
        let mut log_manager = self.log_manager.write();
        log_manager.append_log_record(commit_record)
    }

    pub fn write_abort_record(&self, txn: &Transaction) -> u64 {
        let abort_record = Arc::new(LogRecord::new_transaction_record(
            txn.get_transaction_id(),
            txn.get_prev_lsn(),
            LogRecordType::Abort,
        ));
        let mut log_manager = self.log_manager.write();
        log_manager.append_log_record(abort_record)
    }

    pub fn write_begin_record(&self, txn: &Transaction) -> u64 {
        let begin_record = Arc::new(LogRecord::new_transaction_record(
            txn.get_transaction_id(),
            INVALID_LSN,
            LogRecordType::Begin,
        ));
        let mut log_manager = self.log_manager.write();
        log_manager.append_log_record(begin_record)
    }

    pub fn write_update_record(
        &self,
        txn: &Transaction,
        rid: RID,
        old_tuple: Tuple,
        new_tuple: Tuple,
    ) -> u64 {
        let update_record = Arc::new(LogRecord::new_update_record(
            txn.get_transaction_id(),
            txn.get_prev_lsn(),
            LogRecordType::Update,
            rid,
            Arc::new(old_tuple),
            Arc::new(new_tuple),
        ));
        let mut log_manager = self.log_manager.write();
        log_manager.append_log_record(update_record)
    }

    pub fn force_run_flush_thread(&self) {
        let mut log_manager_write_guard = self.log_manager.write();
        log_manager_write_guard.run_flush_thread()
    }
}
