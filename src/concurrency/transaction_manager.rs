use crate::catalogue::catalogue::Catalog;
use crate::common::config::TxnId;
use crate::common::rid::RID;
use crate::concurrency::transaction::{
    IsolationLevel, Transaction, TransactionState, UndoLink, UndoLog,
};
use crate::concurrency::watermark::Watermark;
use crate::execution::expressions::abstract_expression::ExpressionOps;
use crate::storage::table::table_heap::TableHeap;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct PageVersionInfo {
    // Stores previous version info for all slots
    prev_link: RwLock<HashMap<RID, UndoLink>>,
}

pub struct TransactionManager {
    next_txn_id: AtomicU64,
    txn_map: RwLock<HashMap<TxnId, Arc<Transaction>>>,
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
    pub fn begin(&mut self, isolation_level: IsolationLevel) -> Arc<Transaction> {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let txn = Arc::new(Transaction::new(txn_id, isolation_level));

        // Update transaction map
        self.txn_map.write().insert(txn_id, Arc::clone(&txn));

        // Add to running transactions
        self.running_txns.add_txn(txn.read_ts());

        txn
    }

    /// Commits the specified transaction.
    ///
    /// # Parameters
    /// - `txn`: The transaction to commit.
    ///
    /// # Returns
    /// `true` if the transaction was successfully committed; otherwise, `false`.
    pub fn commit(&mut self, txn: Arc<Transaction>) -> bool {
        // Check transaction state
        if txn.get_state() != TransactionState::Running {
            panic!("txn not in running state");
        }

        // Handle serializable transactions
        if txn.get_isolation_level() == IsolationLevel::Serializable {
            if !self.verify_transaction(&txn) {
                self.abort(txn);
                return false;
            }
        }

        // Update transaction state and metadata
        {
            let mut txn_map = self.txn_map.write();
            txn.set_state(TransactionState::Committed);

            // Update running transactions
            let read_ts = txn.read_ts();
            self.running_txns.update_commit_ts(read_ts);
            self.running_txns.remove_txn(read_ts);

            txn_map.insert(txn.get_transaction_id(), txn.clone());
        }

        true
    }

    /// Aborts the specified transaction.
    ///
    /// # Parameters
    /// - `txn`: The transaction to abort.
    pub fn abort(&mut self, txn: Arc<Transaction>) {
        let current_state = txn.get_state();
        if current_state != TransactionState::Running && current_state != TransactionState::Tainted
        {
            panic!("txn not in running / tainted state");
        }

        {
            let mut txn_map = self.txn_map.write();
            txn.set_state(TransactionState::Aborted);
            self.running_txns.remove_txn(txn.read_ts());
            txn_map.insert(txn.get_transaction_id(), txn.clone());
        }
    }

    /// Performs garbage collection.
    pub fn garbage_collection(&self) {
        let watermark = self.get_watermark();
        let mut version_info = self.version_info.write();

        // Remove version info for pages that are no longer needed
        version_info.retain(|_, page_info| {
            // Retain only links that point to transactions newer than watermark
            page_info.prev_link.write().retain(|_, link| {
                if let Some(txn) = self.txn_map.read().get(&link.prev_txn) {
                    txn.read_ts() > watermark
                } else {
                    false
                }
            });
            !page_info.prev_link.read().is_empty()
        });
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
        let mut version_info = self.version_info.write();
        let page_info = version_info.entry(rid.get_page_id()).or_insert_with(|| {
            Arc::new(PageVersionInfo {
                prev_link: RwLock::new(HashMap::new()),
            })
        });

        // If check function is provided, verify the update is valid
        if let Some(check_fn) = check {
            let current_link = page_info.prev_link.read().get(&rid).cloned();
            if !check_fn(current_link) {
                return false;
            }
        }

        // Update the link
        match prev_link {
            Some(link) => {
                page_info.prev_link.write().insert(rid, link);
            }
            None => {
                page_info.prev_link.write().remove(&rid);
            }
        }

        true
    }

    /// Gets the first undo log of a table heap tuple.
    ///
    /// # Parameters
    /// - `rid`: The record ID.
    ///
    /// # Returns
    /// The undo link, if it exists.
    pub fn get_undo_link(&self, rid: RID) -> Option<UndoLink> {
        let version_info = self.version_info.read();
        version_info
            .get(&rid.get_page_id())
            .and_then(|page_info| page_info.prev_link.read().get(&rid).cloned())
    }

    /// Accesses the transaction undo log buffer and gets the undo log.
    ///
    /// # Parameters
    /// - `link`: The undo link.
    ///
    /// # Returns
    /// The undo log, if it exists.
    pub fn get_undo_log_optional(&self, link: UndoLink) -> Option<UndoLog> {
        if !link.is_valid() {
            return None;
        }

        self.txn_map
            .read()
            .get(&link.prev_txn)
            .map(|txn| txn.get_undo_log(link.prev_log_idx))
    }

    /// Accesses the transaction undo log buffer and gets the undo log.
    ///
    /// # Parameters
    /// - `link`: The undo link.
    ///
    /// # Returns
    /// The undo log.
    pub fn get_undo_log(&self, link: UndoLink) -> UndoLog {
        self.get_undo_log_optional(link)
            .expect("Failed to get undo log for valid link")
    }

    /// Gets the lowest read timestamp in the system.
    ///
    /// # Returns
    /// The watermark.
    pub fn get_watermark(&self) -> u64 {
        self.running_txns.get_watermark()
    }

    pub fn get_transaction(&self, txn_id: &TxnId) -> Option<Arc<Transaction>> {
        self.txn_map.read().get(txn_id).cloned()
    }

    pub fn get_transactions(&self) -> Vec<Arc<Transaction>> {
        self.txn_map.read().values().cloned().collect()
    }

    pub fn get_active_transaction_count(&self) -> usize {
        self.get_transactions().len()
    }

    pub fn get_next_transaction_id(&self) -> u64 {
        self.txn_map.read().keys().next().map_or(0, |k| *k)
    }

    fn verify_transaction(&self, txn: &Transaction) -> bool {
        if txn.get_isolation_level() != IsolationLevel::Serializable {
            return true;
        }

        let read_ts = txn.read_ts();
        let write_set = txn.write_sets();

        // Check write-write conflicts
        for (_table_id, rids) in write_set {
            for rid in rids {
                if let Some(link) = self.get_undo_link(rid) {
                    if let Some(undo_txn) = self.txn_map.read().get(&link.prev_txn) {
                        if undo_txn.commit_ts() > read_ts {
                            return false;
                        }
                    }
                }
            }
        }

        // Check predicate-based conflicts
        let scan_predicates = txn.get_scan_predicates();
        let catalog = self.catalog.read();

        for (table_id, predicates) in scan_predicates {
            if let Some(table_info) = catalog.get_table_by_oid(table_id.into()) {
                let table_heap = table_info.get_table_heap();
                let schema = table_info.get_table_schema();
                let mut iter = table_heap.make_iterator();

                while let Some((_, tuple)) = iter.next() {
                    if let Some(link) = self.get_undo_link(tuple.get_rid()) {
                        if let Some(undo_txn) = self.txn_map.read().get(&link.prev_txn) {
                            if undo_txn.commit_ts() > read_ts {
                                for predicate in &predicates {
                                    if let Ok(_) = predicate.evaluate(&tuple, &schema) {
                                        return false;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        true
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
    tuple: &mut Tuple,
    check: Option<Box<dyn Fn(&TupleMeta, &Tuple, RID, Option<UndoLink>) -> bool>>,
) -> bool {
    // First try to update the tuple
    let update_result = match table_heap.update_tuple(meta, tuple, rid) {
        Ok(new_rid) => {
            if new_rid != rid {
                // If RID changed (tuple was moved), we need to update the undo link
                // Create a closure to handle the undo link update
                if !txn_mgr.update_undo_link(
                    new_rid,
                    undo_link.clone(),
                    Some(Box::new(move |_| true)),
                ) {
                    // If updating undo link fails, we need to clean up the newly inserted tuple
                    let rollback_meta = TupleMeta::new(txn.get_transaction_id(), true);
                    let _ = table_heap.update_tuple_meta(&rollback_meta, new_rid);
                    return false;
                }
                new_rid
            } else {
                // RID didn't change, update undo link normally
                if !txn_mgr.update_undo_link(rid, undo_link.clone(), Some(Box::new(|_| true))) {
                    // If updating undo link fails, revert the tuple update
                    let rollback_meta = TupleMeta::new(txn.get_transaction_id(), true);
                    let _ = table_heap.update_tuple_meta(&rollback_meta, rid);
                    return false;
                }
                rid
            }
        }
        Err(_) => return false,
    };

    // If check function is provided, verify the update
    if let Some(check_fn) = check {
        if !check_fn(meta, tuple, update_result, undo_link.clone()) {
            // If check fails, revert both updates
            let rollback_meta = TupleMeta::new(txn.get_transaction_id(), true);
            let _ = table_heap.update_tuple_meta(&rollback_meta, update_result);
            txn_mgr.update_undo_link(update_result, None, Some(Box::new(|_| true)));
            return false;
        }
    }

    true
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
    // Get the tuple from table heap
    let (meta, tuple) = table_heap.get_tuple(rid).unwrap();

    // Get the undo link
    let undo_link = txn_mgr.get_undo_link(rid);

    (meta, tuple, undo_link)
}
