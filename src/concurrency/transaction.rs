//! # Transaction Management
//!
//! This module provides the core `Transaction` struct representing an active
//! database transaction. Transactions track state, timestamps, undo logs, and
//! write sets to support ACID properties and multi-version concurrency control.
//!
//! ## Architecture
//!
//! ```text
//!                          ┌─────────────────────────────────────────────────┐
//!                          │                 Transaction                     │
//!                          │                                                 │
//!                          │  ┌─────────────────────────────────────────┐    │
//!                          │  │           Immutable Properties          │    │
//!                          │  │  • txn_id: TxnId                        │    │
//!                          │  │  • isolation_level: IsolationLevel      │    │
//!                          │  │  • thread_id: ThreadId                  │    │
//!                          │  └─────────────────────────────────────────┘    │
//!                          │                                                 │
//!                          │  ┌─────────────────────────────────────────┐    │
//!                          │  │      Mutable State (Interior Mut.)      │    │
//!                          │  │  • state: RwLock<TransactionState>      │    │
//!                          │  │  • read_ts: RwLock<Timestamp>           │    │
//!                          │  │  • commit_ts: RwLock<Timestamp>         │    │
//!                          │  │  • prev_lsn: RwLock<Lsn>                │    │
//!                          │  └─────────────────────────────────────────┘    │
//!                          │                                                 │
//!                          │  ┌─────────────────────────────────────────┐    │
//!                          │  │           MVCC Support                  │    │
//!                          │  │  • undo_logs: Mutex<Vec<Arc<UndoLog>>>  │    │
//!                          │  │  • write_set: Mutex<HashMap<...>>       │    │
//!                          │  │  • scan_predicates: Mutex<HashMap<...>> │    │
//!                          │  └─────────────────────────────────────────┘    │
//!                          └─────────────────────────────────────────────────┘
//! ```
//!
//! ## Transaction States
//!
//! ```text
//!                    ┌─────────┐
//!                    │ Running │◄────── begin()
//!                    └────┬────┘
//!                         │
//!           ┌─────────────┼─────────────┐
//!           │             │             │
//!           ▼             ▼             ▼
//!     ┌──────────┐  ┌──────────┐  ┌──────────┐
//!     │ Growing  │  │ Tainted  │  │ Shrinking│
//!     │ (2PL)    │  │ (error)  │  │ (2PL)    │
//!     └────┬─────┘  └────┬─────┘  └────┬─────┘
//!          │             │             │
//!          └─────────────┼─────────────┘
//!                        │
//!           ┌────────────┴────────────┐
//!           ▼                         ▼
//!     ┌───────────┐            ┌───────────┐
//!     │ Committed │            │  Aborted  │
//!     └───────────┘            └───────────┘
//! ```
//!
//! ## Isolation Levels
//!
//! | Level            | Dirty Read | Non-Repeatable | Phantom | Implementation       |
//! |------------------|------------|----------------|---------|----------------------|
//! | ReadUncommitted  | ✓          | ✓              | ✓       | See uncommitted      |
//! | ReadCommitted    | ✗          | ✓              | ✓       | See committed only   |
//! | RepeatableRead   | ✗          | ✗              | ✓       | Snapshot at read_ts  |
//! | Serializable     | ✗          | ✗              | ✗       | + predicate locks    |
//! | Snapshot         | ✗          | ✗              | ✗       | MVCC snapshot        |
//!
//! ## Undo Log Chain
//!
//! ```text
//!   Current Tuple                Undo Log Chain
//!   ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
//!   │ tuple v3 │────►│ UndoLog  │────►│ UndoLog  │────►│ UndoLog  │
//!   │ ts: 300  │     │ ts: 200  │     │ ts: 100  │     │ ts: 50   │
//!   └──────────┘     │ tuple v2 │     │ tuple v1 │     │ tuple v0 │
//!                    └──────────┘     └──────────┘     └──────────┘
//!                          │                │                │
//!                          ▼                ▼                ▼
//!                       UndoLink         UndoLink      INVALID_TXN_ID
//!                    (prev_txn,idx)   (prev_txn,idx)     (terminal)
//! ```
//!
//! ## Transaction Lifecycle
//!
//! ```text
//!   begin(&mut watermark)          commit(&mut watermark)
//!         │                              │
//!         ▼                              ▼
//!   ┌─────────────────┐          ┌─────────────────┐
//!   │ Get read_ts     │          │ Get commit_ts   │
//!   │ Register with   │          │ Update watermark│
//!   │ watermark       │          │ Unregister txn  │
//!   │ Set Running     │          │ Set Committed   │
//!   └─────────────────┘          └─────────────────┘
//!
//!   abort(&mut watermark)
//!         │
//!         ▼
//!   ┌─────────────────┐
//!   │ Unregister txn  │
//!   │ Set Aborted     │
//!   │ (undo logs used │
//!   │  for rollback)  │
//!   └─────────────────┘
//! ```
//!
//! ## Write Set Tracking
//!
//! ```text
//!   write_set: HashMap<TableOidT, HashSet<RID>>
//!
//!   Table 1 ────► { RID(1,0), RID(1,1), RID(1,5) }
//!   Table 2 ────► { RID(2,3), RID(2,7) }
//!   Table 5 ────► { RID(5,0) }
//! ```
//!
//! Used for conflict detection and rollback identification.
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::concurrency::transaction::{Transaction, IsolationLevel};
//! use crate::concurrency::watermark::Watermark;
//!
//! // Create and begin a transaction
//! let mut txn = Transaction::new(txn_id, IsolationLevel::RepeatableRead);
//! txn.begin(&mut watermark);
//!
//! // Perform operations
//! txn.append_write_set(table_oid, rid);
//! let undo_link = txn.append_undo_log(Arc::new(undo_log));
//!
//! // Check visibility
//! if txn.is_tuple_visible(&tuple_meta) {
//!     // Read the tuple
//! }
//!
//! // Commit or abort
//! let commit_ts = txn.commit(&mut watermark);
//! // or: txn.abort(&mut watermark);
//! ```
//!
//! ## Tuple Visibility Rules
//!
//! | Isolation Level  | Visibility Criteria                                    |
//! |------------------|--------------------------------------------------------|
//! | ReadUncommitted  | `!deleted`                                             |
//! | ReadCommitted    | `(committed || own_txn) && !deleted`                   |
//! | RepeatableRead   | `!deleted && (own_txn || (committed && ts ≤ read_ts))` |
//! | Serializable     | Same as RepeatableRead + predicate locking             |
//! | Snapshot         | `visible_to(txn_id, read_ts)`                          |
//!
//! ## Thread Safety
//!
//! - Immutable fields (`txn_id`, `isolation_level`) are safe to read concurrently
//! - Mutable fields use `RwLock` or `Mutex` for interior mutability
//! - `Transaction` is typically wrapped in `Arc` for shared ownership
//! - `parking_lot::RwLock` used for performance-critical timestamp access
//!
//! ## Key Invariants
//!
//! 1. **Timestamp ordering**: `read_ts < commit_ts` for committed transactions
//! 2. **State machine**: Transitions follow the state diagram above
//! 3. **Undo chain**: Each `UndoLog.prev_version` points to a valid prior log
//!    or terminates with `INVALID_TXN_ID`
//! 4. **Write set completeness**: All modified RIDs must be in the write set

use crate::common::config::{
    INVALID_LSN, INVALID_TXN_ID, Lsn, TXN_START_ID, TableOidT, TimeStampOidT, Timestamp, TxnId,
};
use crate::common::rid::RID;
use crate::concurrency::watermark::Watermark;
use crate::sql::execution::expressions::abstract_expression::Expression;
use crate::storage::table::tuple::Tuple;
use crate::storage::table::tuple::TupleMeta;
use bincode::{Decode, Encode};
use log;
use log::debug;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::{fmt, thread};

/// Transaction state representing the current phase of a transaction's lifecycle.
///
/// See the module-level documentation for the state transition diagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransactionState {
    /// Transaction is actively executing operations.
    Running,
    /// Transaction encountered an error but hasn't been aborted yet.
    Tainted,
    /// Transaction has successfully committed all changes.
    Committed,
    /// Transaction has been rolled back; all changes are undone.
    Aborted,
    /// Transaction is in the shrinking phase of 2PL (releasing locks).
    Shrinking,
    /// Transaction is in the growing phase of 2PL (acquiring locks).
    Growing,
}

/// Transaction isolation level controlling visibility and concurrency behavior.
///
/// Higher isolation levels provide stronger consistency guarantees but may
/// reduce concurrency. See module-level documentation for visibility rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode, Default)]
pub enum IsolationLevel {
    /// Allows dirty reads; sees uncommitted changes from other transactions.
    ReadUncommitted,
    /// Only sees committed data; default isolation level.
    #[default]
    ReadCommitted,
    /// Snapshot at transaction start; prevents non-repeatable reads.
    RepeatableRead,
    /// Full serializability; prevents phantom reads via predicate locking.
    Serializable,
    /// MVCC snapshot isolation; sees a consistent snapshot at read timestamp.
    Snapshot,
}

/// Link to a previous version in the MVCC undo chain.
///
/// Each `UndoLink` points to an `UndoLog` entry stored in a transaction's
/// undo buffer. The chain is traversed during visibility checks and rollback.
///
/// An invalid link (terminating the chain) has `prev_txn = INVALID_TXN_ID`.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct UndoLink {
    /// Transaction ID containing the previous version's undo log.
    pub prev_txn: TxnId,
    /// Index of the undo log within that transaction's undo buffer.
    pub prev_log_idx: usize,
}

/// Undo log entry storing a previous tuple version for MVCC.
///
/// Undo logs form a chain linked by `prev_version`, enabling traversal
/// to find versions visible to different transactions. They're also used
/// during rollback to restore previous tuple states.
#[derive(Debug)]
pub struct UndoLog {
    /// `true` if this log represents a deleted tuple (tombstone).
    pub is_deleted: bool,
    /// Bitmask indicating which columns were modified.
    /// `modified_fields[i] == true` means column `i` was changed.
    pub modified_fields: Vec<bool>,
    /// The tuple data for this version.
    pub tuple: Arc<Tuple>,
    /// Commit timestamp of the transaction that created this version.
    pub ts: TimeStampOidT,
    /// Link to the next older version in the undo chain.
    pub prev_version: UndoLink,
    /// Original RID for delete operations (needed for rollback restoration).
    /// Only set when `is_deleted == true`.
    pub original_rid: Option<RID>,
}

impl UndoLog {
    /// Validates core invariants for undo logs (debug-only assertions).
    ///
    /// # Invariants
    /// - `modified_fields` length must match the tuple column count.
    /// - Deleted logs must carry the original RID so rollbacks can restore rows.
    /// - Non-delete logs must not set `original_rid`.
    fn validate_invariants(
        is_deleted: bool,
        modified_fields: &[bool],
        tuple: &Tuple,
        original_rid: Option<RID>,
    ) {
        let column_count = tuple.get_column_count();
        debug_assert_eq!(
            modified_fields.len(),
            column_count,
            "UndoLog modified_fields length {} must match tuple column count {}",
            modified_fields.len(),
            column_count
        );

        if is_deleted {
            debug_assert!(
                original_rid.is_some(),
                "Deleted undo logs must include original_rid for rollback"
            );
        } else {
            debug_assert!(
                original_rid.is_none(),
                "Non-delete undo logs must not set original_rid"
            );
        }
    }

    /// Creates a new undo log entry for an update operation.
    ///
    /// # Parameters
    /// - `is_deleted`: Whether this version is a tombstone (should be `false`).
    /// - `modified_fields`: Bitmask of which columns were modified.
    /// - `tuple`: The previous tuple data before the update.
    /// - `ts`: The timestamp of the transaction creating this log.
    /// - `prev_version`: Link to the next older version in the chain.
    ///
    /// # Returns
    /// A new `UndoLog` instance.
    ///
    /// # Panics (debug only)
    /// Panics if `modified_fields.len() != tuple.get_column_count()`.
    pub fn new(
        is_deleted: bool,
        modified_fields: Vec<bool>,
        tuple: Arc<Tuple>,
        ts: TimeStampOidT,
        prev_version: UndoLink,
    ) -> Self {
        Self::validate_invariants(is_deleted, &modified_fields, tuple.as_ref(), None);
        Self {
            is_deleted,
            modified_fields,
            tuple,
            ts,
            prev_version,
            original_rid: None,
        }
    }

    /// Creates a new undo log entry for a delete operation.
    ///
    /// # Parameters
    /// - `is_deleted`: Must be `true` for delete logs.
    /// - `modified_fields`: Bitmask of columns (typically all `true` for deletes).
    /// - `tuple`: The tuple data before deletion.
    /// - `ts`: The timestamp of the deleting transaction.
    /// - `prev_version`: Link to the next older version in the chain.
    /// - `rid`: The original RID of the deleted tuple (required for rollback).
    ///
    /// # Returns
    /// A new `UndoLog` instance with `original_rid` set.
    ///
    /// # Panics (debug only)
    /// Panics if `is_deleted` is `false` or if invariants are violated.
    pub fn new_for_delete(
        is_deleted: bool,
        modified_fields: Vec<bool>,
        tuple: Arc<Tuple>,
        ts: TimeStampOidT,
        prev_version: UndoLink,
        rid: RID,
    ) -> Self {
        debug_assert!(
            is_deleted,
            "UndoLog::new_for_delete must be called with is_deleted=true"
        );
        Self::validate_invariants(is_deleted, &modified_fields, tuple.as_ref(), Some(rid));
        Self {
            is_deleted,
            modified_fields,
            tuple,
            ts,
            prev_version,
            original_rid: Some(rid),
        }
    }
}

impl UndoLink {
    /// Creates a new undo link pointing to a specific undo log.
    ///
    /// # Parameters
    /// - `txn_id`: The transaction ID containing the undo log.
    /// - `log_idx`: The index of the undo log within that transaction's buffer.
    ///
    /// # Returns
    /// A new `UndoLink` instance.
    pub fn new(txn_id: TxnId, log_idx: usize) -> Self {
        Self {
            prev_txn: txn_id,
            prev_log_idx: log_idx,
        }
    }

    /// Checks if this undo link points to a valid undo log.
    ///
    /// An invalid link (chain terminator) has `prev_txn == INVALID_TXN_ID`.
    ///
    /// # Returns
    /// `true` if the link points to a valid undo log, `false` otherwise.
    pub fn is_valid(&self) -> bool {
        self.prev_txn != INVALID_TXN_ID && self.prev_log_idx != usize::MAX
    }
}

impl IsolationLevel {
    /// Parses an isolation level from a string (case-insensitive).
    ///
    /// # Parameters
    /// - `s`: The string to parse.
    ///
    /// # Returns
    /// `Some(IsolationLevel)` if the string matches a known level, `None` otherwise.
    ///
    /// # Recognized Strings
    /// - `"read uncommitted"` → `ReadUncommitted`
    /// - `"read committed"` → `ReadCommitted`
    /// - `"repeatable read"` → `RepeatableRead`
    /// - `"serializable"` → `Serializable`
    /// - `"snapshot"` or `"snapshot isolation"` → `Snapshot`
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "read uncommitted" => Some(IsolationLevel::ReadUncommitted),
            "read committed" => Some(IsolationLevel::ReadCommitted),
            "repeatable read" => Some(IsolationLevel::RepeatableRead),
            "serializable" => Some(IsolationLevel::Serializable),
            "snapshot" | "snapshot isolation" => Some(IsolationLevel::Snapshot),
            _ => None,
        }
    }
}

/// Represents an active database transaction.
///
/// Transactions track state, timestamps, undo logs, and write sets to support
/// ACID properties and multi-version concurrency control. See module-level
/// documentation for lifecycle and visibility rules.
///
/// # Thread Safety
///
/// `Transaction` uses interior mutability (`RwLock`, `Mutex`) for thread-safe
/// access to mutable state. Typically wrapped in `Arc` for shared ownership.
#[derive(Debug)]
pub struct Transaction {
    // === Immutable fields (set at creation) ===
    /// Unique identifier for this transaction.
    txn_id: TxnId,
    /// Isolation level controlling visibility and concurrency.
    isolation_level: IsolationLevel,
    /// ID of the thread that created this transaction.
    thread_id: thread::ThreadId,

    // === Mutable fields with interior mutability ===
    /// Current state in the transaction lifecycle.
    state: RwLock<TransactionState>,
    /// Read timestamp (snapshot point for MVCC visibility).
    read_ts: RwLock<Timestamp>,
    /// Commit timestamp (set when transaction commits).
    commit_ts: RwLock<Timestamp>,
    /// Undo log buffer for rollback and version chain traversal.
    undo_logs: Mutex<Vec<Arc<UndoLog>>>,
    /// Write set: maps table OID → set of modified RIDs.
    write_set: Mutex<HashMap<TableOidT, HashSet<RID>>>,
    /// Scan predicates for serializable isolation (predicate locking).
    scan_predicates: Mutex<HashMap<u32, Vec<Arc<Expression>>>>,
    /// Previous LSN for WAL chaining.
    prev_lsn: RwLock<Lsn>,
}

impl Transaction {
    /// Creates a new transaction.
    ///
    /// # Parameters
    /// - `txn_id`: The transaction ID.
    /// - `isolation_level`: The isolation level of the transaction.
    ///
    /// # Returns
    /// A new `Transaction` instance.
    pub fn new(txn_id: TxnId, isolation_level: IsolationLevel) -> Self {
        Self {
            txn_id,
            isolation_level,
            thread_id: thread::current().id(),
            state: RwLock::new(TransactionState::Running),
            read_ts: RwLock::new(0), // Initialize with 0, will be set by transaction manager
            commit_ts: RwLock::new(0),
            undo_logs: Mutex::new(Vec::new()),
            write_set: Mutex::new(HashMap::with_capacity(8)),
            scan_predicates: Mutex::new(HashMap::new()),
            prev_lsn: RwLock::new(INVALID_LSN),
        }
    }

    /// Returns the ID of the thread running the transaction.
    pub fn thread_id(&self) -> thread::ThreadId {
        self.thread_id
    }

    /// Returns the ID of this transaction.
    pub fn get_transaction_id(&self) -> TxnId {
        self.txn_id
    }

    /// Returns the human-readable transaction ID, stripping the highest bit.
    /// This should only be used for debugging.
    pub fn txn_id_human_readable(&self) -> TxnId {
        self.txn_id ^ TXN_START_ID
    }

    /// Returns the temporary timestamp of this transaction.
    pub fn temp_ts(&self) -> TimeStampOidT {
        self.txn_id
    }

    /// Returns the isolation level of this transaction.
    pub fn get_isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }

    /// Returns the transaction state.
    pub fn get_state(&self) -> TransactionState {
        *self.state.read()
    }

    /// Sets the transaction state.
    ///
    /// # Parameters
    /// - `state`: The new state to set.
    pub fn set_state(&self, state: TransactionState) {
        *self.state.write() = state;
    }

    /// Returns the read timestamp (snapshot point for MVCC).
    pub fn read_ts(&self) -> Timestamp {
        *self.read_ts.read()
    }

    /// Sets the read timestamp.
    ///
    /// # Parameters
    /// - `ts`: The read timestamp to set.
    pub fn set_read_ts(&self, ts: Timestamp) {
        *self.read_ts.write() = ts;
    }

    /// Returns the commit timestamp.
    pub fn commit_ts(&self) -> Timestamp {
        *self.commit_ts.read()
    }

    /// Sets the commit timestamp.
    ///
    /// # Parameters
    /// - `ts`: The commit timestamp to set.
    pub fn set_commit_ts(&self, ts: Timestamp) {
        *self.commit_ts.write() = ts;
    }

    /// Modifies an existing undo log.
    ///
    /// # Parameters
    /// - `log_idx`: The index of the undo log to modify.
    /// - `new_log`: The new undo log entry.
    pub fn modify_undo_log(&self, log_idx: usize, new_log: Arc<UndoLog>) {
        let mut logs = self.undo_logs.lock().unwrap();
        logs[log_idx] = new_log;
    }

    /// Appends an undo log entry and returns its link.
    ///
    /// # Parameters
    /// - `log`: The undo log entry to append.
    ///
    /// # Returns
    /// The link to the appended undo log entry.
    pub fn append_undo_log(&self, log: Arc<UndoLog>) -> UndoLink {
        let mut logs = self.undo_logs.lock().unwrap();
        let idx = logs.len();
        logs.push(log);

        // Add debug logging
        debug!(
            "Appended undo log at index {} for txn {}. New length: {}",
            idx,
            self.txn_id,
            logs.len()
        );

        UndoLink {
            prev_txn: self.txn_id,
            prev_log_idx: idx,
        }
    }

    /// Returns the undo log entry at the specified index.
    ///
    /// # Parameters
    /// - `log_id`: The index of the undo log entry.
    ///
    /// # Returns
    /// The undo log entry at the specified index.
    pub fn get_undo_log(&self, log_id: usize) -> Arc<UndoLog> {
        debug!(
            "Getting undo log at index {} for txn {}. Current undo logs: {}",
            log_id,
            self.txn_id,
            self.undo_logs.lock().unwrap().len()
        );

        // Lock the mutex and clone the Arc before returning
        let undo_logs = self.undo_logs.lock().unwrap();
        undo_logs
            .get(log_id)
            .unwrap_or_else(|| panic!("Undo log at index {} not found", log_id))
            .clone()
    }

    /// Returns the number of undo log entries.
    pub fn get_undo_log_num(&self) -> usize {
        self.undo_logs.lock().unwrap().len()
    }

    /// Clears the undo logs.
    ///
    /// # Returns
    /// The number of cleared undo logs.
    pub fn clear_undo_log(&self) -> usize {
        let mut logs = self.undo_logs.lock().unwrap();
        let size = logs.len();
        logs.clear();
        size
    }

    /// Appends a write operation to the transaction's write set.
    ///
    /// The write set tracks all tuples modified by this transaction,
    /// used for conflict detection and rollback.
    ///
    /// # Parameters
    /// - `table_oid`: The OID of the table containing the modified tuple.
    /// - `rid`: The record ID of the modified tuple.
    pub fn append_write_set(&self, table_oid: TableOidT, rid: RID) {
        let mut write_set = self.write_set.lock().unwrap();
        write_set.entry(table_oid).or_default().insert(rid);
    }

    /// Gets all write operations performed in this transaction.
    ///
    /// # Returns
    /// A vector of `(table_oid, rid)` pairs representing all modified tuples.
    pub fn get_write_set(&self) -> Vec<(TableOidT, RID)> {
        let write_set_guard = self.write_set.lock().expect("write_set lock poisoned");
        let write_set = write_set_guard
            .iter()
            .flat_map(|(&table_oid, rids)| rids.iter().map(move |&rid| (table_oid, rid)))
            .collect();
        write_set
    }

    /// Appends a scan predicate.
    ///
    /// # Parameters
    /// - `t`: The table OID.
    /// - `predicate`: The scan predicate expression.
    pub fn append_scan_predicate(&self, t: u32, predicate: Arc<Expression>) {
        let mut scan_predicates = self.scan_predicates.lock().unwrap();
        scan_predicates.entry(t).or_default().push(predicate);
    }

    /// Sets the transaction state to tainted.
    ///
    /// A tainted transaction has encountered an error but hasn't been
    /// explicitly aborted yet. It will typically be aborted soon.
    pub fn set_tainted(&self) {
        let mut state = self.state.write();
        *state = TransactionState::Tainted;
    }

    /// Gets all scan predicates recorded by this transaction.
    ///
    /// Used for serializable isolation to detect phantom reads.
    ///
    /// # Returns
    /// A map from table OID to the list of predicates scanned on that table.
    pub fn get_scan_predicates(&self) -> HashMap<u32, Vec<Arc<Expression>>> {
        self.scan_predicates.lock().unwrap().clone()
    }

    /// Sets the previous LSN for WAL chaining.
    ///
    /// # Parameters
    /// - `lsn`: The LSN of the previous log record for this transaction.
    pub fn set_prev_lsn(&self, lsn: Lsn) {
        *self.prev_lsn.write() = lsn;
    }

    /// Gets the previous LSN for WAL chaining.
    ///
    /// # Returns
    /// The LSN of the previous log record, or `INVALID_LSN` if none.
    pub fn get_prev_lsn(&self) -> Lsn {
        *self.prev_lsn.read()
    }

    /// Commits the transaction and updates the watermark.
    ///
    /// This method:
    /// 1. Obtains a commit timestamp from the watermark.
    /// 2. Updates the watermark with the commit timestamp.
    /// 3. Removes this transaction from the active transaction set.
    /// 4. Sets the transaction state to `Committed`.
    ///
    /// # Parameters
    /// - `watermark`: The watermark tracker to update.
    ///
    /// # Returns
    /// The commit timestamp assigned to this transaction.
    ///
    /// # Note
    /// If called on an already committed or aborted transaction, logs a
    /// warning and returns the existing commit timestamp without changes.
    pub fn commit(&self, watermark: &mut Watermark) -> Timestamp {
        match self.get_state() {
            TransactionState::Committed => {
                log::warn!(
                    "commit() called on already committed txn {}",
                    self.txn_id_human_readable()
                );
                return self.commit_ts();
            },
            TransactionState::Aborted => {
                log::warn!(
                    "commit() called on aborted txn {}",
                    self.txn_id_human_readable()
                );
                return self.commit_ts();
            },
            _ => {},
        }

        let commit_ts = watermark.get_next_ts();
        self.set_commit_ts(commit_ts);

        // Update the watermark with our commit timestamp
        watermark.update_commit_ts(commit_ts);

        // Remove this transaction from active transactions
        watermark.remove_txn(self.read_ts());

        self.set_state(TransactionState::Committed);
        commit_ts
    }

    /// Begins the transaction and registers with the watermark.
    ///
    /// This method:
    /// 1. Obtains a read timestamp from the watermark.
    /// 2. Registers this transaction as active in the watermark.
    /// 3. Sets the transaction state to `Running`.
    ///
    /// # Parameters
    /// - `watermark`: The watermark tracker to register with.
    pub fn begin(&mut self, watermark: &mut Watermark) {
        // Get read timestamp and register with watermark
        let read_ts = watermark.get_next_ts_and_register();
        self.set_read_ts(read_ts);
        self.set_state(TransactionState::Running);
    }

    /// Aborts the transaction and updates the watermark.
    ///
    /// This method:
    /// 1. Removes this transaction from the active transaction set.
    /// 2. Sets the transaction state to `Aborted`.
    ///
    /// Note: The actual rollback of changes (applying undo logs) is handled
    /// by the `TransactionManager`, not this method.
    ///
    /// # Parameters
    /// - `watermark`: The watermark tracker to update.
    ///
    /// # Note
    /// If called on an already aborted or committed transaction, logs a
    /// warning and returns without changes.
    pub fn abort(&self, watermark: &mut Watermark) {
        match self.get_state() {
            TransactionState::Aborted => {
                log::warn!(
                    "abort() called on already aborted txn {}",
                    self.txn_id_human_readable()
                );
                return;
            },
            TransactionState::Committed => {
                log::warn!(
                    "abort() called after commit on txn {}",
                    self.txn_id_human_readable()
                );
                return;
            },
            _ => {},
        }

        // Remove this transaction from active transactions
        watermark.remove_txn(self.read_ts());
        self.set_state(TransactionState::Aborted);
    }

    /// Checks if a tuple is visible to this transaction.
    ///
    /// Visibility rules depend on the transaction's isolation level:
    ///
    /// | Level            | Rule                                           |
    /// |------------------|------------------------------------------------|
    /// | ReadUncommitted  | `!deleted`                                     |
    /// | ReadCommitted    | `(committed || own_txn) && !deleted`           |
    /// | RepeatableRead   | `!deleted && (own || committed && ts ≤ read_ts)` |
    /// | Serializable     | Same as RepeatableRead                         |
    /// | Snapshot         | `is_visible_to(txn_id, read_ts)`               |
    ///
    /// # Parameters
    /// - `meta`: The tuple metadata to check.
    ///
    /// # Returns
    /// `true` if the tuple is visible to this transaction, `false` otherwise.
    pub fn is_tuple_visible(&self, meta: &TupleMeta) -> bool {
        let commit_ts_dbg = meta.get_commit_timestamp();
        let read_ts = *self.read_ts.read();
        debug!(
            "Transaction.is_tuple_visible(): txn_id={}, isolation={:?}, read_ts={}, meta={{creator={}, commit_ts={}, deleted={}}}",
            self.txn_id,
            self.isolation_level,
            read_ts,
            meta.get_creator_txn_id(),
            commit_ts_dbg
                .map(|ts| ts.to_string())
                .unwrap_or_else(|| "None".to_string()),
            meta.is_deleted()
        );

        match self.isolation_level {
            IsolationLevel::ReadUncommitted => {
                let visible = !meta.is_deleted();
                debug!("READ_UNCOMMITTED visibility: {}", visible);
                visible
            },
            IsolationLevel::ReadCommitted => {
                let visible = (meta.is_committed() || meta.get_creator_txn_id() == self.txn_id)
                    && !meta.is_deleted();
                debug!("READ_COMMITTED visibility: {}", visible);
                visible
            },
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                let visible = !meta.is_deleted()
                    && (meta.get_creator_txn_id() == self.txn_id
                        || (meta.is_committed()
                            && meta.get_commit_timestamp().is_some_and(|ts| ts <= read_ts)));
                debug!("REPEATABLE_READ/SERIALIZABLE visibility: {}", visible);
                visible
            },
            IsolationLevel::Snapshot => {
                let visible = meta.is_visible_to(self.txn_id, read_ts);
                debug!("SNAPSHOT visibility: {}", visible);
                visible
            },
        }
    }
}

/// Formats `IsolationLevel` as an uppercase SQL-style string.
///
/// # Example
/// ```rust,ignore
/// assert_eq!(IsolationLevel::ReadCommitted.to_string(), "READ_COMMITTED");
/// ```
impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            IsolationLevel::ReadUncommitted => "READ_UNCOMMITTED",
            IsolationLevel::ReadCommitted => "READ_COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE_READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
            IsolationLevel::Snapshot => "SNAPSHOT",
        };
        write!(f, "{}", name)
    }
}

/// Formats `TransactionState` as an uppercase string.
///
/// # Example
/// ```rust,ignore
/// assert_eq!(TransactionState::Running.to_string(), "RUNNING");
/// ```
impl fmt::Display for TransactionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            TransactionState::Running => "RUNNING",
            TransactionState::Tainted => "TAINTED",
            TransactionState::Committed => "COMMITTED",
            TransactionState::Aborted => "ABORTED",
            TransactionState::Shrinking => "SHRINKING",
            TransactionState::Growing => "GROWING",
        };
        write!(f, "{}", name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::common::config::TXN_START_ID;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::abstract_expression::Expression::Mock;
    use crate::sql::execution::expressions::mock_expression::MockExpression;
    use crate::storage::table::tuple::Tuple;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    fn create_test_tuple() -> Tuple {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::Integer),
        ]);
        Tuple::new(&[Value::from(1), Value::from(100)], &schema, RID::new(0, 0))
    }

    mod basic_behaviour {
        use crate::common::config::{INVALID_LSN, INVALID_TXN_ID, TXN_START_ID, TimeStampOidT};
        use crate::common::rid::RID;
        use crate::concurrency::transaction::tests::create_test_tuple;
        use crate::concurrency::transaction::{
            IsolationLevel, Transaction, TransactionState, UndoLink, UndoLog,
        };
        use crate::sql::execution::expressions::abstract_expression::Expression::Mock;
        use crate::sql::execution::expressions::mock_expression::MockExpression;
        use crate::types_db::value::Value;
        use std::sync::Arc;
        use std::thread;

        #[test]
        fn test_transaction_basic_properties() {
            let txn_id = 1;
            let isolation_level = IsolationLevel::ReadCommitted;
            let txn = Transaction::new(txn_id, isolation_level);

            assert_eq!(txn.get_transaction_id(), txn_id);
            assert_eq!(txn.get_isolation_level(), isolation_level);
            assert_eq!(txn.get_state(), TransactionState::Running);
            assert_eq!(txn.thread_id(), thread::current().id());
        }

        #[test]
        fn test_transaction_timestamps() {
            let txn = Transaction::new(1, IsolationLevel::ReadCommitted);

            // Test read timestamp
            txn.set_read_ts(100);
            assert_eq!(txn.read_ts(), 100);

            // Test commit timestamp
            txn.set_commit_ts(200);
            assert_eq!(txn.commit_ts(), 200);

            // Test temp timestamp
            assert_eq!(txn.temp_ts(), txn.get_transaction_id() as TimeStampOidT);
        }

        #[test]
        fn test_transaction_undo_logs() {
            let txn = Transaction::new(1, IsolationLevel::ReadCommitted);

            // Create multiple undo logs with modifications
            let tuple1 = create_test_tuple();
            let undo_log1 = UndoLog::new(
                false,
                vec![true, false],
                Arc::new(tuple1),
                100,
                UndoLink::new(INVALID_TXN_ID, 0),
            );

            // Append first undo log
            let link1 = txn.append_undo_log(Arc::new(undo_log1));
            assert_eq!(link1.prev_txn, txn.get_transaction_id());
            assert_eq!(link1.prev_log_idx, 0);
            assert_eq!(txn.get_undo_log_num(), 1);

            // Create a second undo log that points to the first
            let mut tuple2 = create_test_tuple();
            tuple2.get_values_mut()[0] = Value::new(2); // Change the ID value

            let undo_log2 = UndoLog::new(
                false,
                vec![true, false],
                Arc::new(tuple2),
                200,
                link1.clone(), // Clone the link to avoid moving it
            );

            // Append second undo log
            let link2 = txn.append_undo_log(Arc::new(undo_log2));
            assert_eq!(link2.prev_txn, txn.get_transaction_id());
            assert_eq!(link2.prev_log_idx, 1);
            assert_eq!(txn.get_undo_log_num(), 2);

            // Verify undo log chain
            let retrieved_log2 = txn.get_undo_log(link2.prev_log_idx);
            assert_eq!(retrieved_log2.ts, 200);
            assert_eq!(retrieved_log2.prev_version.prev_txn, link1.prev_txn);
            assert_eq!(retrieved_log2.prev_version.prev_log_idx, link1.prev_log_idx);

            // Get first log through chain
            let retrieved_log1 = txn.get_undo_log(retrieved_log2.prev_version.prev_log_idx);
            assert_eq!(retrieved_log1.ts, 100);
            assert_eq!(retrieved_log1.prev_version.prev_txn, INVALID_TXN_ID);

            // Modify an existing log
            let tuple3 = create_test_tuple();
            let rid = tuple3.get_rid();
            let modified_log1 = UndoLog::new_for_delete(
                true, // deleted version
                vec![true, false],
                Arc::new(tuple3),
                150, // changed timestamp
                UndoLink::new(INVALID_TXN_ID, 0),
                rid,
            );

            txn.modify_undo_log(link1.prev_log_idx, Arc::new(modified_log1));

            // Verify modification
            let updated_log1 = txn.get_undo_log(link1.prev_log_idx);
            assert!(updated_log1.is_deleted);
            assert_eq!(updated_log1.ts, 150);

            // Test clear
            let cleared_count = txn.clear_undo_log();
            assert_eq!(cleared_count, 2);
            assert_eq!(txn.get_undo_log_num(), 0);
        }

        #[test]
        fn test_transaction_write_sets() {
            let txn = Transaction::new(1, IsolationLevel::ReadCommitted);
            let rid1 = RID::new(1, 1);
            let rid2 = RID::new(1, 2);

            // Test append write set
            txn.append_write_set(1, rid1);
            txn.append_write_set(1, rid2);
            txn.append_write_set(2, rid1);

            // Test get write sets
            let write_sets = txn.get_write_set();
            assert_eq!(write_sets.len(), 3); // Total number of write records

            // Check each write record
            let mut found_table1_rid1 = false;
            let mut found_table1_rid2 = false;
            let mut found_table2_rid1 = false;

            for (table_id, rid) in write_sets {
                match table_id {
                    1 => {
                        if rid == rid1 {
                            found_table1_rid1 = true;
                        } else if rid == rid2 {
                            found_table1_rid2 = true;
                        }
                    },
                    2 => {
                        if rid == rid1 {
                            found_table2_rid1 = true;
                        }
                    },
                    _ => panic!("Unexpected table ID"),
                }
            }

            assert!(found_table1_rid1, "Missing write record for table 1, rid1");
            assert!(found_table1_rid2, "Missing write record for table 1, rid2");
            assert!(found_table2_rid1, "Missing write record for table 2, rid1");
        }

        #[test]
        fn test_transaction_scan_predicates() {
            let txn = Transaction::new(1, IsolationLevel::ReadCommitted);
            let predicate1 = Arc::new(Mock(MockExpression::new(
                "predicate1".to_string(),
                Default::default(),
            )));
            let predicate2 = Arc::new(Mock(MockExpression::new(
                "predicate2".to_string(),
                Default::default(),
            )));

            // Test append scan predicates
            txn.append_scan_predicate(1, predicate1.clone());
            txn.append_scan_predicate(1, predicate2.clone());
            txn.append_scan_predicate(2, predicate1.clone());

            // Test get scan predicates
            let scan_predicates = txn.get_scan_predicates();
            assert_eq!(scan_predicates.len(), 2); // Two tables
            assert_eq!(scan_predicates.get(&1).unwrap().len(), 2); // Two predicates for table 1
            assert_eq!(scan_predicates.get(&2).unwrap().len(), 1); // One predicate for table 2
        }

        #[test]
        fn test_transaction_tainted_state() {
            let txn = Transaction::new(1, IsolationLevel::ReadCommitted);
            assert_eq!(txn.get_state(), TransactionState::Running);

            txn.set_tainted();
            assert_eq!(txn.get_state(), TransactionState::Tainted);
        }

        #[test]
        fn test_transaction_lsn_management() {
            let txn = Transaction::new(1, IsolationLevel::ReadCommitted);

            // Test initial LSN
            assert_eq!(txn.get_prev_lsn(), INVALID_LSN);

            // Test LSN update
            let new_lsn = 100;
            txn.set_prev_lsn(new_lsn);
            assert_eq!(txn.get_prev_lsn(), new_lsn);
        }

        #[test]
        fn test_undo_link_validity() {
            let valid_link = UndoLink {
                prev_txn: 1,
                prev_log_idx: 0,
            };
            assert!(valid_link.is_valid());

            let invalid_link = UndoLink {
                prev_txn: INVALID_TXN_ID,
                prev_log_idx: 0,
            };
            assert!(!invalid_link.is_valid());
        }

        #[test]
        fn test_transaction_id_human_readable() {
            let txn_id = TXN_START_ID + 1;
            let txn = Transaction::new(txn_id, IsolationLevel::ReadCommitted);
            assert_eq!(txn.txn_id_human_readable(), 1);
        }
    }

    mod watermark_integration_tests {
        use super::*;

        #[test]
        fn test_transaction_begin_with_watermark() {
            // Test that begin() correctly gets a timestamp and registers with watermark
            let mut txn = Transaction::new(1, IsolationLevel::ReadCommitted);
            let mut watermark = Watermark::new();

            // Initial watermark state
            let _initial_ts = watermark.get_watermark();

            // Begin transaction
            txn.begin(&mut watermark);

            // Verify transaction state and timestamps
            assert_eq!(txn.get_state(), TransactionState::Running);
            assert!(txn.read_ts() > 0);

            // Verify watermark updated - the active transactions should affect the watermark
            assert!(watermark.get_watermark() <= txn.read_ts());
        }

        #[test]
        fn test_transaction_commit_with_watermark() {
            // Test that commit() correctly updates the watermark
            let mut txn = Transaction::new(1, IsolationLevel::ReadCommitted);
            let mut watermark = Watermark::new();

            // Begin transaction
            txn.begin(&mut watermark);
            let read_ts = txn.read_ts();

            // Commit transaction
            let commit_ts = txn.commit(&mut watermark);

            // Verify transaction state and timestamps
            assert_eq!(txn.get_state(), TransactionState::Committed);
            assert!(commit_ts > read_ts);
            assert_eq!(txn.commit_ts(), commit_ts);

            // After commit, watermark should reflect that this transaction is no longer active
            assert_ne!(watermark.get_watermark(), read_ts);
        }

        #[test]
        fn test_transaction_abort_with_watermark() {
            // Test that abort() correctly updates the watermark
            let mut txn = Transaction::new(1, IsolationLevel::ReadCommitted);
            let mut watermark = Watermark::new();

            // Begin transaction
            txn.begin(&mut watermark);
            let read_ts = txn.read_ts();

            // Abort transaction
            txn.abort(&mut watermark);

            // Verify transaction state
            assert_eq!(txn.get_state(), TransactionState::Aborted);

            // After abort, watermark should reflect that this transaction is no longer active
            assert_ne!(watermark.get_watermark(), read_ts);
        }

        #[test]
        fn test_multiple_transactions_with_watermark() {
            // Test multiple transactions interacting with a single watermark
            let mut watermark = Watermark::new();

            // Create and begin multiple transactions
            let mut txn1 = Transaction::new(1, IsolationLevel::ReadCommitted);
            let mut txn2 = Transaction::new(2, IsolationLevel::RepeatableRead);
            let mut txn3 = Transaction::new(3, IsolationLevel::Serializable);

            txn1.begin(&mut watermark);
            txn2.begin(&mut watermark);
            txn3.begin(&mut watermark);

            let read_ts1 = txn1.read_ts();
            let read_ts2 = txn2.read_ts();
            let read_ts3 = txn3.read_ts();

            // With all transactions active, watermark should be the minimum timestamp
            assert_eq!(watermark.get_watermark(), read_ts1);

            // Commit txn1
            txn1.commit(&mut watermark);

            // After committing txn1, watermark should move to the next minimum
            assert_eq!(watermark.get_watermark(), read_ts2);

            // Abort txn2
            txn2.abort(&mut watermark);

            // After aborting txn2, watermark should move to txn3
            assert_eq!(watermark.get_watermark(), read_ts3);

            // Commit txn3
            txn3.commit(&mut watermark);

            // After committing all transactions, watermark should be higher than all read timestamps
            let final_watermark = watermark.get_watermark();
            assert!(final_watermark > read_ts3);
        }
    }

    mod isolation_level_tests {
        use super::*;

        #[test]
        fn test_isolation_level_from_str_valid_inputs() {
            // Test all valid isolation level strings with different casing
            assert_eq!(
                IsolationLevel::from_str("read uncommitted"),
                Some(IsolationLevel::ReadUncommitted)
            );
            assert_eq!(
                IsolationLevel::from_str("READ UNCOMMITTED"),
                Some(IsolationLevel::ReadUncommitted)
            );
            assert_eq!(
                IsolationLevel::from_str("Read Uncommitted"),
                Some(IsolationLevel::ReadUncommitted)
            );

            assert_eq!(
                IsolationLevel::from_str("read committed"),
                Some(IsolationLevel::ReadCommitted)
            );
            assert_eq!(
                IsolationLevel::from_str("READ COMMITTED"),
                Some(IsolationLevel::ReadCommitted)
            );

            assert_eq!(
                IsolationLevel::from_str("repeatable read"),
                Some(IsolationLevel::RepeatableRead)
            );
            assert_eq!(
                IsolationLevel::from_str("REPEATABLE READ"),
                Some(IsolationLevel::RepeatableRead)
            );

            assert_eq!(
                IsolationLevel::from_str("serializable"),
                Some(IsolationLevel::Serializable)
            );
            assert_eq!(
                IsolationLevel::from_str("SERIALIZABLE"),
                Some(IsolationLevel::Serializable)
            );
            assert_eq!(
                IsolationLevel::from_str("snapshot"),
                Some(IsolationLevel::Snapshot)
            );
            assert_eq!(
                IsolationLevel::from_str("SNAPSHOT ISOLATION"),
                Some(IsolationLevel::Snapshot)
            );
        }

        #[test]
        fn test_isolation_level_from_str_invalid_inputs() {
            // Test invalid isolation level strings
            assert_eq!(IsolationLevel::from_str(""), None);
            assert_eq!(IsolationLevel::from_str("unknown"), None);
            assert_eq!(IsolationLevel::from_str("read_uncommitted"), None); // with underscore
            assert_eq!(IsolationLevel::from_str("readuncommitted"), None); // without space
            assert_eq!(IsolationLevel::from_str("level1"), None);
        }

        #[test]
        fn test_isolation_level_default() {
            // Test that the default isolation level is ReadCommitted
            let default_level = IsolationLevel::default();
            assert_eq!(default_level, IsolationLevel::ReadCommitted);
        }

        #[test]
        fn test_isolation_level_display() {
            assert_eq!(
                IsolationLevel::ReadUncommitted.to_string(),
                "READ_UNCOMMITTED"
            );
            assert_eq!(IsolationLevel::Serializable.to_string(), "SERIALIZABLE");
            assert_eq!(IsolationLevel::ReadCommitted.to_string(), "READ_COMMITTED");
            assert_eq!(
                IsolationLevel::RepeatableRead.to_string(),
                "REPEATABLE_READ"
            );
            assert_eq!(IsolationLevel::Serializable.to_string(), "SERIALIZABLE");
            assert_eq!(IsolationLevel::Snapshot.to_string(), "SNAPSHOT");
        }
    }

    mod tuple_visibility_tests {
        use super::*;

        fn create_tuple_meta(
            creator_txn_id: TxnId,
            commit_timestamp: Timestamp,
            deleted: bool,
            committed: bool,
        ) -> TupleMeta {
            let mut meta = TupleMeta::new(creator_txn_id);
            // A tuple is considered committed if its commit_timestamp != Timestamp::MAX
            // So for committed=true, we set the specified commit_timestamp
            // For committed=false, we keep the default Timestamp::MAX from TupleMeta::new
            if committed {
                meta.set_commit_timestamp(commit_timestamp);
            }
            meta.set_deleted(deleted);
            meta
        }

        #[test]
        fn test_read_uncommitted_visibility() {
            // Read uncommitted should see all non-deleted tuples regardless of commit status
            let txn = Transaction::new(1, IsolationLevel::ReadUncommitted);

            // Non-deleted, uncommitted tuple
            let meta1 = create_tuple_meta(2, 0, false, false);
            assert!(txn.is_tuple_visible(&meta1));

            // Non-deleted, committed tuple
            let meta2 = create_tuple_meta(2, 100, false, true);
            assert!(txn.is_tuple_visible(&meta2));

            // Deleted, uncommitted tuple
            let meta3 = create_tuple_meta(2, 0, true, false);
            assert!(!txn.is_tuple_visible(&meta3));

            // Deleted, committed tuple
            let meta4 = create_tuple_meta(2, 100, true, true);
            assert!(!txn.is_tuple_visible(&meta4));
        }

        #[test]
        fn test_read_committed_visibility() {
            // Read committed should see committed non-deleted tuples and own uncommitted tuples
            let txn_id = 1;
            let txn = Transaction::new(txn_id, IsolationLevel::ReadCommitted);

            // Own non-deleted, uncommitted tuple
            let meta1 = create_tuple_meta(txn_id, 0, false, false);
            assert!(txn.is_tuple_visible(&meta1));

            // Other's non-deleted, uncommitted tuple
            let meta2 = create_tuple_meta(2, 0, false, false);
            assert!(!txn.is_tuple_visible(&meta2));

            // Other's non-deleted, committed tuple
            let meta3 = create_tuple_meta(2, 100, false, true);
            assert!(txn.is_tuple_visible(&meta3));

            // Own deleted tuple
            let meta4 = create_tuple_meta(txn_id, 0, true, false);
            assert!(!txn.is_tuple_visible(&meta4));
        }

        #[test]
        fn test_repeatable_read_visibility() {
            // Repeatable read should see committed non-deleted tuples with commit_ts <= read_ts
            let txn = Transaction::new(1, IsolationLevel::RepeatableRead);
            txn.set_read_ts(150);

            // Uncommitted tuple (shouldn't be visible)
            let meta1 = create_tuple_meta(2, 0, false, false);
            assert!(!txn.is_tuple_visible(&meta1));

            // Committed tuple with commit_ts < read_ts
            let meta2 = create_tuple_meta(2, 100, false, true);
            assert!(txn.is_tuple_visible(&meta2));

            // Committed tuple with commit_ts = read_ts
            let meta3 = create_tuple_meta(2, 150, false, true);
            assert!(txn.is_tuple_visible(&meta3));

            // Committed tuple with commit_ts > read_ts
            let meta4 = create_tuple_meta(2, 200, false, true);
            assert!(!txn.is_tuple_visible(&meta4));

            // Deleted committed tuple
            let meta5 = create_tuple_meta(2, 100, true, true);
            assert!(!txn.is_tuple_visible(&meta5));
        }

        #[test]
        fn test_serializable_visibility() {
            // Serializable should behave like repeatable read for visibility
            let txn = Transaction::new(1, IsolationLevel::Serializable);
            txn.set_read_ts(150);

            // Same tests as repeatable read
            let meta1 = create_tuple_meta(2, 0, false, false);
            assert!(!txn.is_tuple_visible(&meta1));

            let meta2 = create_tuple_meta(2, 100, false, true);
            assert!(txn.is_tuple_visible(&meta2));

            let meta3 = create_tuple_meta(2, 150, false, true);
            assert!(txn.is_tuple_visible(&meta3));

            let meta4 = create_tuple_meta(2, 200, false, true);
            assert!(!txn.is_tuple_visible(&meta4));
        }

        #[test]
        fn test_snapshot_visibility() {
            // Snapshot isolation should respect the reader's snapshot timestamp and hide tombstones
            let txn_id = 1;
            let txn = Transaction::new(txn_id, IsolationLevel::Snapshot);
            txn.set_read_ts(150);

            // Own uncommitted version is visible
            let own_uncommitted = create_tuple_meta(txn_id, 0, false, false);
            assert!(txn.is_tuple_visible(&own_uncommitted));

            // Other's uncommitted version is invisible
            let other_uncommitted = create_tuple_meta(2, 0, false, false);
            assert!(!txn.is_tuple_visible(&other_uncommitted));

            // Committed version with commit_ts <= snapshot is visible
            let committed_visible = create_tuple_meta(2, 100, false, true);
            assert!(txn.is_tuple_visible(&committed_visible));

            // Committed version after snapshot is invisible
            let committed_future = create_tuple_meta(2, 200, false, true);
            assert!(!txn.is_tuple_visible(&committed_future));

            // Deleted committed version should not be returned
            let committed_deleted = create_tuple_meta(2, 100, true, true);
            assert!(!txn.is_tuple_visible(&committed_deleted));
        }
    }

    mod concurrency_tests {
        use super::*;

        #[test]
        fn test_concurrent_write_set_operations() {
            // Test concurrent modifications to write sets
            let txn = Arc::new(Transaction::new(1, IsolationLevel::ReadCommitted));
            let thread_count = 5;
            let mut handles = vec![];

            for i in 0..thread_count {
                let txn_clone = Arc::clone(&txn);
                let handle = thread::spawn(move || {
                    // Each thread adds different RIDs to the same table
                    for j in 0..10 {
                        let rid = RID::new(i as u64, j as u32);
                        txn_clone.append_write_set(1, rid);
                    }
                });
                handles.push(handle);
            }

            // Wait for all threads to complete
            for handle in handles {
                handle.join().unwrap();
            }

            // Verify results
            let write_set = txn.get_write_set();

            // Should have thread_count * 10 entries
            assert_eq!(write_set.len(), thread_count * 10);

            // Verify each expected entry exists
            for i in 0..thread_count {
                for j in 0..10 {
                    let rid = RID::new(i as u64, j as u32);
                    assert!(
                        write_set.contains(&(1, rid)),
                        "Missing write record for table 1, rid({},{})",
                        i,
                        j
                    );
                }
            }
        }

        #[test]
        fn test_concurrent_scan_predicate_operations() {
            // Test concurrent modifications to scan predicates
            let txn = Arc::new(Transaction::new(1, IsolationLevel::ReadCommitted));
            let thread_count = 5;
            let mut handles = vec![];

            for i in 0..thread_count {
                let txn_clone = Arc::clone(&txn);
                let table_id = (i % 3) as u32; // Use 3 different tables

                let handle = thread::spawn(move || {
                    // Each thread adds different predicates to assigned tables
                    for j in 0..5 {
                        let predicate = Arc::new(Mock(MockExpression::new(
                            format!("predicate_t{}_{}_{}", table_id, i, j),
                            Default::default(),
                        )));
                        txn_clone.append_scan_predicate(table_id, predicate);
                    }
                });
                handles.push(handle);
            }

            // Wait for all threads to complete
            for handle in handles {
                handle.join().unwrap();
            }

            // Verify results
            let scan_predicates = txn.get_scan_predicates();

            // Should have entries for 3 tables
            assert_eq!(scan_predicates.len(), 3);

            // Count predicates per table
            let mut total_predicates = 0;
            for table_id in 0..3 {
                let predicates = scan_predicates.get(&(table_id as u32)).unwrap();
                let expected_count = (thread_count + 2) / 3 * 5; // Ceiling of (thread_count/3) * 5
                assert!(
                    predicates.len() <= expected_count,
                    "Table {} has {} predicates, expected at most {}",
                    table_id,
                    predicates.len(),
                    expected_count
                );
                total_predicates += predicates.len();
            }

            // Total number of predicates should be thread_count * 5
            assert_eq!(total_predicates, thread_count * 5);
        }
    }

    mod edge_case_tests {
        use super::*;

        #[test]
        fn test_empty_undo_logs() {
            // Test behavior with empty undo logs
            let txn = Transaction::new(1, IsolationLevel::ReadCommitted);

            // Initially empty
            assert_eq!(txn.get_undo_log_num(), 0);

            // Clearing an empty log
            let cleared = txn.clear_undo_log();
            assert_eq!(cleared, 0);

            // Try to append and then clear
            let tuple = create_test_tuple();
            let undo_log = UndoLog::new(
                false,
                vec![true, false],
                Arc::new(tuple),
                100,
                UndoLink::new(INVALID_TXN_ID, 0),
            );
            txn.append_undo_log(Arc::new(undo_log));

            assert_eq!(txn.get_undo_log_num(), 1);

            let cleared = txn.clear_undo_log();
            assert_eq!(cleared, 1);
            assert_eq!(txn.get_undo_log_num(), 0);
        }

        #[test]
        fn test_txn_id_edge_values() {
            // Test with minimum transaction ID
            let min_txn = Transaction::new(TXN_START_ID, IsolationLevel::ReadCommitted);
            assert_eq!(min_txn.txn_id_human_readable(), 0);

            // Test with maximum transaction ID
            let max_txn_id = u64::MAX;
            let max_txn = Transaction::new(max_txn_id, IsolationLevel::ReadCommitted);
            assert_eq!(max_txn.txn_id_human_readable(), max_txn_id ^ TXN_START_ID);

            // Test with invalid transaction ID
            let invalid_txn = Transaction::new(INVALID_TXN_ID, IsolationLevel::ReadCommitted);
            assert_eq!(invalid_txn.get_transaction_id(), INVALID_TXN_ID);
        }

        #[test]
        #[should_panic(expected = "modified_fields length")]
        fn test_undo_log_empty_fields() {
            // Invalid: modified_fields must match tuple column count
            let tuple = create_test_tuple();
            let _ = UndoLog::new(
                false,
                vec![], // Empty modified fields
                Arc::new(tuple),
                100,
                UndoLink::new(INVALID_TXN_ID, 0),
            );
        }
    }

    mod state_transition_tests {
        use super::*;

        #[test]
        fn test_all_state_transitions() {
            // Test all valid state transitions
            let txn = Transaction::new(1, IsolationLevel::ReadCommitted);

            // Initial state is Running
            assert_eq!(txn.get_state(), TransactionState::Running);

            // Running -> Tainted
            txn.set_state(TransactionState::Tainted);
            assert_eq!(txn.get_state(), TransactionState::Tainted);

            // Tainted -> Aborted
            txn.set_state(TransactionState::Aborted);
            assert_eq!(txn.get_state(), TransactionState::Aborted);

            // Create a new transaction
            let txn2 = Transaction::new(2, IsolationLevel::ReadCommitted);

            // Running -> Committed
            txn2.set_state(TransactionState::Committed);
            assert_eq!(txn2.get_state(), TransactionState::Committed);

            // Create a new transaction for growing/shrinking phase tests
            let txn3 = Transaction::new(3, IsolationLevel::Serializable);

            // Running -> Growing
            txn3.set_state(TransactionState::Growing);
            assert_eq!(txn3.get_state(), TransactionState::Growing);

            // Growing -> Shrinking
            txn3.set_state(TransactionState::Shrinking);
            assert_eq!(txn3.get_state(), TransactionState::Shrinking);

            // Shrinking -> Committed
            txn3.set_state(TransactionState::Committed);
            assert_eq!(txn3.get_state(), TransactionState::Committed);
        }

        #[test]
        fn test_tainted_state_helper() {
            // Test the set_tainted() helper method
            let txn = Transaction::new(1, IsolationLevel::ReadCommitted);
            assert_eq!(txn.get_state(), TransactionState::Running);

            // Use the helper method
            txn.set_tainted();
            assert_eq!(txn.get_state(), TransactionState::Tainted);
        }

        #[test]
        fn test_transaction_state_display() {
            assert_eq!(TransactionState::Running.to_string(), "RUNNING");
            assert_eq!(TransactionState::Tainted.to_string(), "TAINTED");
            assert_eq!(TransactionState::Committed.to_string(), "COMMITTED");
            assert_eq!(TransactionState::Aborted.to_string(), "ABORTED");
            assert_eq!(TransactionState::Shrinking.to_string(), "SHRINKING");
            assert_eq!(TransactionState::Growing.to_string(), "GROWING");
        }
    }

    mod performance_tests {
        use super::*;
        use std::time::Instant;

        #[test]
        #[ignore] // Ignore by default as it's a performance test
        fn test_undo_log_performance() {
            // Test performance with large numbers of undo logs
            let txn = Transaction::new(1, IsolationLevel::ReadCommitted);
            let schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("value", TypeId::Integer),
            ]);

            // Measure time to add 10,000 undo logs
            let start = Instant::now();
            let num_logs = 10_000;

            for i in 0..num_logs {
                // Create a new tuple for each iteration
                let tuple = Tuple::new(
                    &[
                        Value::new(i32::try_from(i).unwrap_or(0)),
                        Value::new(i32::try_from(i * 10).unwrap_or(0)),
                    ],
                    &schema,
                    RID::new(0, 0),
                );

                let undo_log = UndoLog::new(
                    false,
                    vec![true, false],
                    Arc::new(tuple),
                    i as TimeStampOidT,
                    UndoLink::new(INVALID_TXN_ID, 0),
                );
                txn.append_undo_log(Arc::new(undo_log));
            }

            let append_duration = start.elapsed();
            println!(
                "Time to append {} undo logs: {:?}",
                num_logs, append_duration
            );

            // Measure time to clear logs
            let start = Instant::now();
            let cleared = txn.clear_undo_log();
            let clear_duration = start.elapsed();

            assert_eq!(cleared, num_logs);
            println!("Time to clear {} undo logs: {:?}", num_logs, clear_duration);
        }

        #[test]
        #[ignore] // Ignore by default as it's a performance test
        fn test_write_set_performance() {
            // Test performance with large numbers of write operations
            let txn = Transaction::new(1, IsolationLevel::ReadCommitted);

            // Measure time to add 10,000 write operations across 10 tables
            let start = Instant::now();
            let num_operations = 10_000;
            let num_tables = 10;

            for i in 0..num_operations {
                let table_id = (i % num_tables) as TableOidT + 1;
                let rid = RID::new(table_id, i as u32);
                txn.append_write_set(table_id, rid);
            }

            let append_duration = start.elapsed();
            println!(
                "Time to append {} write operations: {:?}",
                num_operations, append_duration
            );

            // Measure time to retrieve write set
            let start = Instant::now();
            let write_set = txn.get_write_set();
            let retrieve_duration = start.elapsed();

            assert_eq!(write_set.len(), num_operations);
            println!(
                "Time to retrieve {} write operations: {:?}",
                num_operations, retrieve_duration
            );
        }

        #[test]
        #[ignore] // Ignore by default as it's a performance test
        fn test_isolation_level_performance_comparison() {
            // Compare performance of different isolation levels for tuple visibility checks
            let num_checks = 100_000;
            let tuple_meta = TupleMeta::new(0); // Use default meta

            // Test ReadUncommitted
            let txn1 = Transaction::new(1, IsolationLevel::ReadUncommitted);
            let start = Instant::now();
            for _ in 0..num_checks {
                let _ = txn1.is_tuple_visible(&tuple_meta);
            }
            let ru_duration = start.elapsed();

            // Test ReadCommitted
            let txn2 = Transaction::new(2, IsolationLevel::ReadCommitted);
            let start = Instant::now();
            for _ in 0..num_checks {
                let _ = txn2.is_tuple_visible(&tuple_meta);
            }
            let rc_duration = start.elapsed();

            // Test RepeatableRead
            let txn3 = Transaction::new(3, IsolationLevel::RepeatableRead);
            txn3.set_read_ts(100); // Set a read timestamp
            let start = Instant::now();
            for _ in 0..num_checks {
                let _ = txn3.is_tuple_visible(&tuple_meta);
            }
            let rr_duration = start.elapsed();

            // Test Serializable
            let txn4 = Transaction::new(4, IsolationLevel::Serializable);
            txn4.set_read_ts(100); // Set a read timestamp
            let start = Instant::now();
            for _ in 0..num_checks {
                let _ = txn4.is_tuple_visible(&tuple_meta);
            }
            let ser_duration = start.elapsed();

            println!("Visibility check performance for {} checks:", num_checks);
            println!("- READ_UNCOMMITTED: {:?}", ru_duration);
            println!("- READ_COMMITTED: {:?}", rc_duration);
            println!("- REPEATABLE_READ: {:?}", rr_duration);
            println!("- SERIALIZABLE: {:?}", ser_duration);
        }
    }
}
