use crate::common::config::{TimeStampOidT, Timestamp, TxnId, INVALID_TS, INVALID_TXN_ID};
use crate::common::rid::RID;
use crate::execution::expressions::abstract_expression::Expression;
use crate::storage::table::tuple::Tuple;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::{fmt, thread};

/// Represents a link to a previous version of this tuple.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UndoLink {
    /// Previous version can be found in which txn.
    pub prev_txn: TxnId,
    /// The log index of the previous version in `prev_txn`.
    pub prev_log_idx: usize,
}

impl UndoLink {
    /// Checks if the undo link points to something.
    pub fn is_valid(&self) -> bool {
        self.prev_txn != INVALID_TXN_ID
    }
}

/// Represents an undo log entry.
#[derive(Debug, Clone)]
pub struct UndoLog {
    /// Whether this log is a deletion marker.
    pub is_deleted: bool,
    /// The fields modified by this undo log.
    pub modified_fields: Vec<bool>,
    /// The modified fields.
    pub tuple: Tuple,
    /// Timestamp of this undo log.
    pub ts: TimeStampOidT,
    /// Undo log previous version.
    pub prev_version: UndoLink,
}

/// Transaction state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransactionState {
    Running,
    Tainted,
    Committed,
    Aborted,
    Shrinking,
    Growing,
}

/// Transaction isolation level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IsolationLevel {
    ReadUncommitted,
    SnapshotIsolation,
    Serializable,
    RepeatableRead,
    ReadCommitted,
}

/// Represents a transaction.
pub struct Transaction {
    // Immutable fields
    txn_id: TxnId,
    isolation_level: IsolationLevel,
    thread_id: thread::ThreadId,

    // Mutable fields with interior mutability
    state: RwLock<TransactionState>,
    read_ts: RwLock<Timestamp>,
    commit_ts: RwLock<Timestamp>,
    undo_logs: Mutex<Vec<UndoLog>>,
    write_set: Mutex<HashMap<u32, HashSet<RID>>>,
    scan_predicates: Mutex<HashMap<u32, Vec<Arc<Expression>>>>,
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
            read_ts: RwLock::new(0),
            commit_ts: RwLock::new(INVALID_TS),
            undo_logs: Mutex::new(Vec::new()),
            write_set: Mutex::new(HashMap::new()),
            scan_predicates: Mutex::new(HashMap::new()),
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
        self.txn_id ^ crate::common::config::TXN_START_ID
    }

    /// Returns the temporary timestamp of this transaction.
    pub fn temp_ts(&self) -> TimeStampOidT {
        self.txn_id.try_into().unwrap()
    }

    /// Returns the isolation level of this transaction.
    pub fn get_isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }

    /// Returns the transaction state.
    pub fn get_state(&self) -> TransactionState {
        *self.state.read()
    }

    pub fn set_state(&self, state: TransactionState) {
        *self.state.write() = state;
    }

    /// Returns the read timestamp.
    pub fn read_ts(&self) -> TimeStampOidT {
        *self.read_ts.read()
    }

    pub fn set_read_ts(&self, ts: TimeStampOidT) {
        *self.read_ts.write() = ts;
    }

    /// Returns the commit timestamp.
    pub fn commit_ts(&self) -> TimeStampOidT {
        *self.commit_ts.read()
    }

    pub fn set_commit_ts(&self, ts: TimeStampOidT) {
        *self.commit_ts.write() = ts;
    }

    /// Modifies an existing undo log.
    ///
    /// # Parameters
    /// - `log_idx`: The index of the undo log to modify.
    /// - `new_log`: The new undo log entry.
    pub fn modify_undo_log(&self, log_idx: usize, new_log: UndoLog) {
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
    pub fn append_undo_log(&self, log: UndoLog) -> UndoLink {
        let mut logs = self.undo_logs.lock().unwrap();
        logs.push(log);
        UndoLink {
            prev_txn: self.txn_id,
            prev_log_idx: logs.len() - 1,
        }
    }

    /// Returns the undo log entry at the specified index.
    ///
    /// # Parameters
    /// - `log_id`: The index of the undo log entry.
    ///
    /// # Returns
    /// The undo log entry at the specified index.
    pub fn get_undo_log(&self, log_id: usize) -> UndoLog {
        self.undo_logs.lock().unwrap()[log_id].clone()
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

    /// Appends a write set entry.
    ///
    /// # Parameters
    /// - `t`: The table OID.
    /// - `rid`: The row ID.
    pub fn append_write_set(&self, t: u32, rid: RID) {
        let mut write_set = self.write_set.lock().unwrap();
        write_set.entry(t).or_insert_with(HashSet::new).insert(rid);
    }

    /// Returns the write sets.
    pub fn write_sets(&self) -> HashMap<u32, HashSet<RID>> {
        self.write_set.lock().unwrap().clone()
    }

    /// Appends a scan predicate.
    ///
    /// # Parameters
    /// - `t`: The table OID.
    /// - `predicate`: The scan predicate expression.
    pub fn append_scan_predicate(&self, t: u32, predicate: Arc<Expression>) {
        let mut scan_predicates = self.scan_predicates.lock().unwrap();
        scan_predicates
            .entry(t)
            .or_insert_with(Vec::new)
            .push(predicate);
    }

    /// Returns the scan predicates.
    pub fn scan_predicates(&self) -> HashMap<u32, Vec<Arc<Expression>>> {
        self.scan_predicates.lock().unwrap().clone()
    }

    /// Sets the transaction state to tainted.
    pub fn set_tainted(&self) {
        let mut state = self.state.write();
        *state = TransactionState::Tainted;
    }

    pub fn get_scan_predicates(&self) -> HashMap<u32, Vec<Arc<Expression>>> {
        self.scan_predicates.lock().unwrap().clone()
    }
}

/// Formatter implementation for `IsolationLevel`.
impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            IsolationLevel::ReadUncommitted => "READ_UNCOMMITTED",
            IsolationLevel::SnapshotIsolation => "SNAPSHOT_ISOLATION",
            IsolationLevel::Serializable => "SERIALIZABLE",
            IsolationLevel::RepeatableRead => "REPEATABLE_READ",
            IsolationLevel::ReadCommitted => "READ_commit_isolation",
        };
        write!(f, "{}", name)
    }
}

/// Formatter implementation for `TransactionState`.
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
