use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock};

use crate::catalogue::catalogue::Catalog;
use crate::common::rid::RID;
use crate::concurrency::transaction::{Transaction, UndoLink};
use crate::storage::table::table_heap::TableHeap;
use crate::storage::table::tuple::{Tuple, TupleMeta};

type TransactionId = u64;

#[derive(Default)]
pub struct RunningTransactions {
    // Placeholder for running transactions management
}

impl RunningTransactions {
    pub fn add_txn(&self, _read_ts: u64) {
        unimplemented!()
    }

    pub fn update_commit_ts(&self, _commit_ts: u64) {
        unimplemented!()
    }

    pub fn remove_txn(&self, _read_ts: u64) {
        unimplemented!()
    }
}

pub struct PageVersionInfo {
    // Protects the map
    mutex: RwLock<()>,
    // Stores previous version info for all slots
    prev_link: HashMap<usize, UndoLink>,
}

pub struct TransactionManager {
    next_txn_id: AtomicU64,
    txn_map: RwLock<HashMap<TransactionId, Arc<Mutex<Transaction>>>>,
    running_txns: RunningTransactions,
    commit_mutex: Mutex<()>,
    last_commit_ts: AtomicU64,
    catalog: Arc<Catalog>,
    version_info: RwLock<HashMap<u64, Arc<PageVersionInfo>>>,
}

// impl TransactionManager {
//     pub fn new(catalog: Arc<Catalog>) -> Self {
//         TransactionManager {
//             next_txn_id: AtomicU64::new(0),
//             txn_map: RwLock::new(HashMap::new()),
//             running_txns: RunningTransactions::default(),
//             commit_mutex: Mutex::new(()),
//             last_commit_ts: AtomicU64::new(0),
//             catalog,
//             version_info: RwLock::new(HashMap::new()),
//         }
//     }
//
//     /// Begins a new transaction.
//     ///
//     /// # Parameters
//     /// - `isolation_level`: The isolation level for the new transaction.
//     ///
//     /// # Returns
//     /// A reference to the new transaction.
//     pub fn begin(&self, isolation_level: IsolationLevel) -> Arc<Mutex<Transaction>> {
//         let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
//
//         let txn = Arc::new(Mutex::new(Transaction::new(txn_id as TxnId, isolation_level)));
//         self.txn_map.write().unwrap().insert(txn_id, txn.clone());
//
//         // TODO: set the timestamps here. Watermark updated below.
//         // running_txns_.AddTxn(txn_ref->read_ts_);
//
//         txn
//     }
//
//     fn verify_txn(&self, _txn: &Transaction) -> bool {
//         true
//     }
//
//     /// Commits the specified transaction.
//     ///
//     /// # Parameters
//     /// - `txn`: The transaction to commit.
//     ///
//     /// # Returns
//     /// `true` if the transaction was successfully committed; otherwise, `false`.
//     pub fn commit(&self, txn: Arc<Mutex<Transaction>>) -> bool {
//         let commit_lck = self.commit_mutex.lock().unwrap();
//
//         // TODO: acquire commit ts!
//
//         let mut txn = txn.lock().unwrap();
//         if txn.state() != TransactionState::Running {
//             drop(commit_lck);
//             panic!("txn not in running state");
//         }
//
//         if txn.isolation_level() == IsolationLevel::Serializable {
//             if !self.verify_txn(&txn) {
//                 drop(commit_lck);
//                 self.abort(txn);
//                 return false;
//             }
//         }
//
//         // TODO: Implement the commit logic!
//
//         {
//             let mut txn_map = self.txn_map.write().unwrap();
//
//             // TODO: set commit timestamp + update last committed timestamp here.
//
//             txn.state = TransactionState::Committed;
//             // running_txns_.UpdateCommitTs(txn->commit_ts_);
//             // running_txns_.RemoveTxn(txn->read_ts_);
//         }
//
//         true
//     }
//
//     /// Aborts the specified transaction.
//     ///
//     /// # Parameters
//     /// - `txn`: The transaction to abort.
//     pub fn abort(&self, txn: Arc<Mutex<Transaction>>) {
//         let mut txn = txn.lock().unwrap();
//         if txn.state() != TransactionState::Running && txn.state() != TransactionState::Tainted {
//             panic!("txn not in running / tainted state");
//         }
//
//         // TODO: Implement the abort logic!
//
//         {
//             let mut txn_map = self.txn_map.write().unwrap();
//             txn.state = TransactionState::Aborted;
//             // running_txns_.RemoveTxn(txn->read_ts_);
//         }
//     }
//
//     /// Performs garbage collection.
//     pub fn garbage_collection(&self) {
//         unimplemented!("not implemented");
//     }
//
//     /// Updates an undo link that links table heap tuple to the first undo log.
//     ///
//     /// # Parameters
//     /// - `rid`: The record ID.
//     /// - `prev_link`: The previous undo link.
//     /// - `check`: A function to ensure validity.
//     ///
//     /// # Returns
//     /// `true` if the update was successful; otherwise, `false`.
//     pub fn update_undo_link(
//         &self,
//         rid: RID,
//         prev_link: Option<UndoLink>,
//         check: Option<Box<dyn Fn(Option<UndoLink>) -> bool>>,
//     ) -> bool {
//         unimplemented!()
//     }
//
//     /// Gets the first undo log of a table heap tuple.
//     ///
//     /// # Parameters
//     /// - `rid`: The record ID.
//     ///
//     /// # Returns
//     /// The undo link, if it exists.
//     pub fn get_undo_link(&self, rid: RID) -> Option<UndoLink> {
//         unimplemented!()
//     }
//
//     /// Accesses the transaction undo log buffer and gets the undo log.
//     ///
//     /// # Parameters
//     /// - `link`: The undo link.
//     ///
//     /// # Returns
//     /// The undo log, if it exists.
//     pub fn get_undo_log_optional(&self, link: UndoLink) -> Option<UndoLog> {
//         unimplemented!()
//     }
//
//     /// Accesses the transaction undo log buffer and gets the undo log.
//     ///
//     /// # Parameters
//     /// - `link`: The undo link.
//     ///
//     /// # Returns
//     /// The undo log.
//     pub fn get_undo_log(&self, link: UndoLink) -> UndoLog {
//         unimplemented!()
//     }
//
//     /// Gets the lowest read timestamp in the system.
//     ///
//     /// # Returns
//     /// The watermark.
//     pub fn get_watermark(&self) -> u64 {
//         unimplemented!()
//     }
// }

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
