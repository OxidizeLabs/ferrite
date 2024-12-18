use crate::catalogue::catalogue::Catalog;
use crate::common::config::{TransactionId, TxnId};
use crate::common::rid::RID;
use crate::concurrency::transaction::{
    IsolationLevel, Transaction, TransactionState, UndoLink, UndoLog,
};
use crate::concurrency::watermark::Watermark;
use crate::storage::table::table_heap::TableHeap;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct PageVersionInfo {
    // Stores previous version info for all slots
    prev_link: HashMap<usize, UndoLink>,
}

pub struct TransactionManager {
    next_txn_id: AtomicU64,
    txn_map: RwLock<HashMap<TransactionId, Arc<Mutex<Transaction>>>>,
    running_txns: Watermark,
    last_commit_ts: AtomicU64,
    catalog: Arc<RwLock<Catalog>>,
    version_info: RwLock<HashMap<u64, Arc<PageVersionInfo>>>,
}

impl TransactionManager {
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        TransactionManager {
            next_txn_id: AtomicU64::new(0),
            txn_map: RwLock::new(HashMap::new()),
            running_txns: Watermark::default(),
            last_commit_ts: AtomicU64::new(0),
            catalog,
            version_info: RwLock::new(HashMap::new()),
        }
    }

    /// Begins a new transaction.
    ///
    /// # Parameters
    /// - `isolation_level`: The isolation level for the new transaction.
    ///
    /// # Returns
    /// A reference to the new transaction.
    pub fn begin(&mut self, isolation_level: IsolationLevel) -> Arc<Mutex<Transaction>> {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);

        let txn = Arc::new(Mutex::new(Transaction::new(txn_id, isolation_level)));
        self.txn_map.write().insert(txn_id, txn.clone());

        {
            let txn_guard = txn.lock();
            self.running_txns.add_txn(txn_guard.read_ts());
        }

        txn
    }

    /// Commits the specified transaction.
    ///
    /// # Parameters
    /// - `txn`: The transaction to commit.
    ///
    /// # Returns
    /// `true` if the transaction was successfully committed; otherwise, `false`.
    pub fn commit(&mut self, txn: Arc<Mutex<Transaction>>) -> bool {
        let binding = txn.clone();
        let txn_guard = binding.lock();
        if txn_guard.get_state() != TransactionState::Running {
            panic!("txn not in running state");
        }

        if txn_guard.isolation_level() == IsolationLevel::Serializable {
            if !self.verify_txn(&txn_guard) {
                self.abort(txn);
                return false;
            }
        }

        {
            let mut txn_map = self.txn_map.write();

            txn_map.insert(txn_guard.txn_id(), txn.clone());
            txn_guard.set_state(TransactionState::Committed);
            self.running_txns.update_commit_ts(txn_guard.read_ts());
            self.running_txns.remove_txn(txn_guard.read_ts());
        }

        true
    }

    /// Aborts the specified transaction.
    ///
    /// # Parameters
    /// - `txn`: The transaction to abort.
    pub fn abort(&mut self, txn: Arc<Mutex<Transaction>>) {
        let txn_guard = txn.lock();
        if txn_guard.get_state() != TransactionState::Running
            && txn_guard.get_state() != TransactionState::Tainted
        {
            panic!("txn not in running / tainted state");
        }

        {
            let mut txn_map = self.txn_map.write();
            txn_map.insert(txn_guard.txn_id(), txn.clone());
            txn_guard.set_state(TransactionState::Aborted);
            self.running_txns.remove_txn(txn_guard.read_ts());
        }
    }

    /// Performs garbage collection.
    pub fn garbage_collection(&self) {
        unimplemented!("not implemented");
    }

    /// Updates an undo link that links table heap tuple to the first undo log.
    ///
    /// # Parameters
    /// - `rid`: The record ID.
    /// - `prev_link`: The previous undo link.
    /// - `check`: A function to ensure validity.
    ///
    /// # Returns
    /// `true` if the update was successful; otherwise, `false`.
    pub fn update_undo_link(
        &self,
        rid: RID,
        prev_link: Option<UndoLink>,
        check: Option<Box<dyn Fn(Option<UndoLink>) -> bool>>,
    ) -> bool {
        unimplemented!()
    }

    /// Gets the first undo log of a table heap tuple.
    ///
    /// # Parameters
    /// - `rid`: The record ID.
    ///
    /// # Returns
    /// The undo link, if it exists.
    pub fn get_undo_link(&self, rid: RID) -> Option<UndoLink> {
        unimplemented!()
    }

    /// Accesses the transaction undo log buffer and gets the undo log.
    ///
    /// # Parameters
    /// - `link`: The undo link.
    ///
    /// # Returns
    /// The undo log, if it exists.
    pub fn get_undo_log_optional(&self, link: UndoLink) -> Option<UndoLog> {
        unimplemented!()
    }

    /// Accesses the transaction undo log buffer and gets the undo log.
    ///
    /// # Parameters
    /// - `link`: The undo link.
    ///
    /// # Returns
    /// The undo log.
    pub fn get_undo_log(&self, link: UndoLink) -> UndoLog {
        unimplemented!()
    }

    /// Gets the lowest read timestamp in the system.
    ///
    /// # Returns
    /// The watermark.
    pub fn get_watermark(&self) -> u64 {
        self.running_txns.get_watermark()
    }

    fn verify_txn(&self, txn: &Transaction) -> bool {
        txn.get_state() == TransactionState::Committed
    }
}

/// Updates the tuple and its undo link in the table heap atomically.
///
/// # Parameters
/// - `txn_mgr`: The transaction manager.
/// - `rid`: The record ID.
/// - `undo_link`: The undo link.
/// - `table_heap`: The table heap.
/// - `txn`: The transaction.
/// - `meta`: The tuple metadata.
/// - `tuple`: The tuple.
/// - `check`: A function to check the validity.
///
/// # Returns
/// `true` if the update was successful; otherwise, `false`.
pub fn update_tuple_and_undo_link(
    txn_mgr: &TransactionManager,
    rid: RID,
    undo_link: Option<UndoLink>,
    table_heap: &TableHeap,
    txn: &Transaction,
    meta: &TupleMeta,
    tuple: &Tuple,
    check: Option<Box<dyn Fn(&TupleMeta, &Tuple, RID, Option<UndoLink>) -> bool>>,
) -> bool {
    unimplemented!()
}

/// Gets the tuple and its undo link in the table heap atomically.
///
/// # Parameters
/// - `txn_mgr`: The transaction manager.
/// - `table_heap`: The table heap.
/// - `rid`: The record ID.
///
/// # Returns
/// A tuple containing the tuple metadata, the tuple, and the undo link.
pub fn get_tuple_and_undo_link(
    txn_mgr: &TransactionManager,
    table_heap: &TableHeap,
    rid: RID,
) -> (TupleMeta, Tuple, Option<UndoLink>) {
    unimplemented!()
}
