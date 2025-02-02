use crate::common::config::{
    Lsn, TableOidT, TimeStampOidT, Timestamp, TxnId, INVALID_LSN, INVALID_TXN_ID,
};
use crate::common::rid::RID;
use crate::concurrency::watermark::Watermark;
use crate::sql::execution::expressions::abstract_expression::Expression;
use crate::storage::table::tuple::Tuple;
use crate::storage::table::tuple::TupleMeta;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::{fmt, thread};

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// Represents a link to a previous version of this tuple.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UndoLink {
    /// Previous version can be found in which txn.
    pub prev_txn: TxnId,
    /// The log index of the previous version in `prev_txn`.
    pub prev_log_idx: usize,
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

/// Represents a transaction.
#[derive(Debug)]
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
    write_set: Mutex<HashMap<TableOidT, HashSet<RID>>>,
    scan_predicates: Mutex<HashMap<u32, Vec<Arc<Expression>>>>,
    prev_lsn: RwLock<Lsn>,
}

impl UndoLink {
    /// Checks if the undo link points to something.
    pub fn is_valid(&self) -> bool {
        self.prev_txn != INVALID_TXN_ID
    }
}

impl IsolationLevel {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "read uncommitted" => Some(IsolationLevel::ReadUncommitted),
            "read committed" => Some(IsolationLevel::ReadCommitted),
            "repeatable read" => Some(IsolationLevel::RepeatableRead),
            "serializable" => Some(IsolationLevel::Serializable),
            _ => None,
        }
    }
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
    pub fn read_ts(&self) -> Timestamp {
        *self.read_ts.read()
    }

    pub fn set_read_ts(&self, ts: Timestamp) {
        *self.read_ts.write() = ts;
    }

    /// Returns the commit timestamp.
    pub fn commit_ts(&self) -> Timestamp {
        *self.commit_ts.read()
    }

    pub fn set_commit_ts(&self, ts: Timestamp) {
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

    /// Appends a write operation to the transaction's write set
    pub fn append_write_set(&self, table_oid: TableOidT, rid: RID) {
        let mut write_set = self.write_set.lock().unwrap();
        write_set
            .entry(table_oid)
            .or_insert_with(HashSet::new)
            .insert(rid);
    }

    /// Gets all write operations performed in this transaction
    pub fn get_write_set(&self) -> Vec<(TableOidT, RID)> {
        let write_set = self.write_set.lock().unwrap();
        write_set
            .iter()
            .flat_map(|(&table_oid, rids)| rids.iter().map(move |&rid| (table_oid, rid)))
            .collect()
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

    pub fn set_prev_lsn(&self, lsn: Lsn) {
        *self.prev_lsn.write() = lsn;
    }

    pub fn get_prev_lsn(&self) -> Lsn {
        *self.prev_lsn.read()
    }

    /// Commits the transaction and updates watermark
    pub fn commit(&self, watermark: &mut Watermark) -> Timestamp {
        let commit_ts = watermark.get_next_ts();
        self.set_commit_ts(commit_ts);

        // Update the watermark with our commit timestamp
        watermark.update_commit_ts(commit_ts);

        // Remove this transaction from active transactions
        watermark.remove_txn(self.read_ts());

        self.set_state(TransactionState::Committed);
        commit_ts
    }

    /// Begins the transaction and registers with watermark
    pub fn begin(&mut self, watermark: &mut Watermark) {
        // Get read timestamp and register with watermark
        let read_ts = watermark.get_next_ts_and_register();
        self.set_read_ts(read_ts);
        self.set_state(TransactionState::Running);
    }

    /// Aborts the transaction and updates watermark
    pub fn abort(&self, watermark: &mut Watermark) {
        // Remove this transaction from active transactions
        watermark.remove_txn(self.read_ts());
        self.set_state(TransactionState::Aborted);
    }

    /// Enhanced tuple visibility check that considers watermark
    pub fn is_tuple_visible(&self, tuple_meta: &TupleMeta, watermark: &Watermark) -> bool {
        match self.get_isolation_level() {
            IsolationLevel::ReadUncommitted => {
                // Can see all non-deleted tuples, even uncommitted ones
                !tuple_meta.is_deleted()
            }
            IsolationLevel::ReadCommitted => {
                // Can only see committed tuples up to current watermark
                tuple_meta.is_visible_to(self.get_transaction_id(), watermark)
            }
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                // Can only see tuples committed before this transaction's read timestamp
                tuple_meta.is_committed()
                    && tuple_meta.get_commit_timestamp() <= self.read_ts()
                    && !tuple_meta.is_deleted()
            }
        }
    }
}

impl Default for Transaction {
    fn default() -> Self {
        Transaction::new(0, IsolationLevel::ReadUncommitted)
    }
}

impl Default for IsolationLevel {
    fn default() -> Self {
        IsolationLevel::ReadCommitted
    }
}

// Manually implement Clone for Transaction
impl Clone for Transaction {
    fn clone(&self) -> Self {
        Self {
            txn_id: self.txn_id,
            isolation_level: self.isolation_level,
            thread_id: self.thread_id,
            state: RwLock::new(*self.state.read()),
            read_ts: RwLock::new(*self.read_ts.read()),
            commit_ts: RwLock::new(*self.commit_ts.read()),
            undo_logs: Mutex::new(self.undo_logs.lock().unwrap().clone()),
            write_set: Mutex::new(self.write_set.lock().unwrap().clone()),
            scan_predicates: Mutex::new(self.scan_predicates.lock().unwrap().clone()),
            prev_lsn: RwLock::new(*self.prev_lsn.read()),
        }
    }
}

/// Formatter implementation for `IsolationLevel`.
impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            IsolationLevel::ReadUncommitted => "READ_UNCOMMITTED",
            IsolationLevel::ReadCommitted => "READ_COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE_READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
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
        Tuple::new(&[Value::from(1), Value::from(100)], schema, RID::new(0, 0))
    }

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
    fn test_transaction_state_transitions() {
        let txn = Transaction::new(1, IsolationLevel::ReadCommitted);

        // Test initial state
        assert_eq!(txn.get_state(), TransactionState::Running);

        // Test state transitions
        txn.set_state(TransactionState::Tainted);
        assert_eq!(txn.get_state(), TransactionState::Tainted);

        txn.set_state(TransactionState::Committed);
        assert_eq!(txn.get_state(), TransactionState::Committed);

        txn.set_state(TransactionState::Aborted);
        assert_eq!(txn.get_state(), TransactionState::Aborted);
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
        let tuple = create_test_tuple();

        // Create test undo log
        let undo_log = UndoLog {
            is_deleted: false,
            modified_fields: vec![true, false],
            tuple: tuple.clone(),
            ts: 100,
            prev_version: UndoLink {
                prev_txn: INVALID_TXN_ID,
                prev_log_idx: 0,
            },
        };

        // Test append undo log
        let link = txn.append_undo_log(undo_log.clone());
        assert_eq!(link.prev_txn, txn.get_transaction_id());
        assert_eq!(link.prev_log_idx, 0);

        // Test get undo log
        let retrieved_log = txn.get_undo_log(0);
        assert_eq!(retrieved_log.is_deleted, undo_log.is_deleted);
        assert_eq!(retrieved_log.modified_fields, undo_log.modified_fields);
        assert_eq!(retrieved_log.ts, undo_log.ts);

        // Test modify undo log
        let mut modified_log = undo_log.clone();
        modified_log.is_deleted = true;
        txn.modify_undo_log(0, modified_log);
        let retrieved_modified = txn.get_undo_log(0);
        assert!(retrieved_modified.is_deleted);

        // Test undo log count and clear
        assert_eq!(txn.get_undo_log_num(), 1);
        let cleared_count = txn.clear_undo_log();
        assert_eq!(cleared_count, 1);
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
                }
                2 => {
                    if rid == rid1 {
                        found_table2_rid1 = true;
                    }
                }
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
        let scan_predicates = txn.scan_predicates();
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

    #[test]
    fn test_concurrent_undo_log_operations() {
        let txn = Arc::new(Transaction::new(1, IsolationLevel::ReadCommitted));
        let tuple = create_test_tuple();
        let thread_count = 5;
        let mut handles = vec![];

        for i in 0..thread_count {
            let txn_clone = Arc::clone(&txn);
            let tuple_clone = tuple.clone();
            let handle = thread::spawn(move || {
                let undo_log = UndoLog {
                    is_deleted: false,
                    modified_fields: vec![true, false],
                    tuple: tuple_clone,
                    ts: i as TimeStampOidT,
                    prev_version: UndoLink {
                        prev_txn: INVALID_TXN_ID,
                        prev_log_idx: 0,
                    },
                };
                txn_clone.append_undo_log(undo_log);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(txn.get_undo_log_num(), thread_count);
    }
}
