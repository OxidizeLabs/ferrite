use log::{debug, error, info, warn};
use crate::catalog::catalog::Catalog;
use crate::common::config::{TxnId, INVALID_LSN};
use crate::common::rid::RID;
use crate::concurrency::transaction::{
    IsolationLevel, Transaction, TransactionState, UndoLink, UndoLog,
};
use crate::concurrency::watermark::Watermark;
use crate::recovery::log_manager::LogManager;
use crate::recovery::log_record::{LogRecord, LogRecordType};
use crate::sql::execution::expressions::abstract_expression::ExpressionOps;
use crate::storage::table::table_heap::TableHeap;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::concurrency::lock_manager::LockManager;

#[derive(Debug)]
pub struct PageVersionInfo {
    // Stores previous version info for all slots
    prev_link: RwLock<HashMap<RID, UndoLink>>,
}

#[derive(Debug)]
pub struct TransactionManager {
    next_txn_id: AtomicU64,
    txn_map: RwLock<HashMap<TxnId, Arc<Transaction>>>,
    running_txns: Watermark,
    catalog: Arc<RwLock<Catalog>>,
    version_info: RwLock<HashMap<u64, Arc<PageVersionInfo>>>,
    log_manager: Arc<RwLock<LogManager>>,
}

// Manually implement Clone
impl TransactionManager {
    // Modify constructor
    pub fn new(catalog: Arc<RwLock<Catalog>>, log_manager: Arc<RwLock<LogManager>>) -> Self {
        TransactionManager {
            next_txn_id: AtomicU64::new(0),
            txn_map: RwLock::new(HashMap::new()),
            running_txns: Watermark::default(),
            catalog,
            version_info: RwLock::new(HashMap::new()),
            log_manager,
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

        // Write begin log record
        let begin_record =
            LogRecord::new_transaction_record(txn_id, INVALID_LSN, LogRecordType::Begin);
        let lsn = self.log_manager.write().append_log_record(&begin_record);
        txn.set_prev_lsn(lsn);

        // Update transaction map
        self.txn_map.write().insert(txn_id, Arc::clone(&txn));
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
        if txn.get_state() != TransactionState::Running {
            panic!("txn not in running state");
        }

        debug!("Starting commit for transaction {}", txn.get_transaction_id());

        if !self.verify_transaction(&txn) {
            warn!("Transaction verification failed, aborting");
            self.abort(txn);
            return false;
        }

        // 1. Write commit log record (WAL)
        let commit_record = LogRecord::new_transaction_record(
            txn.get_transaction_id(),
            txn.get_prev_lsn(),
            LogRecordType::Commit,
        );
        let lsn = self.log_manager.write().append_log_record(&commit_record);
        txn.set_prev_lsn(lsn);
        debug!("Commit log record written with LSN {}", lsn);

        // 2. Flush all dirty pages for this transaction
        let write_set = txn.get_write_set();
        debug!("Transaction {} write set size: {}", txn.get_transaction_id(), write_set.len());

        let mut all_pages_flushed = true;
        for (table_id, rid) in write_set {
            debug!("Processing write set entry: table {}, RID {:?}", table_id, rid);
            let page_id = rid.get_page_id();
            debug!("Attempting to flush page {} for table {}", page_id, table_id);

            let buffer_pool = self.catalog.read().get_buffer_pool();
            if let Some(success) = buffer_pool.flush_page(page_id) {
                if !success {
                    error!("Failed to flush page {}", page_id);
                    all_pages_flushed = false;
                }
            }
        }

        if !all_pages_flushed {
            error!("Some pages failed to flush during commit of transaction {}", txn.get_transaction_id());
            self.abort(txn);
            return false;
        }

        // 3. Update transaction state and metadata
        {
            let mut txn_map = self.txn_map.write();
            txn.set_state(TransactionState::Committed);

            let read_ts = txn.read_ts();
            self.running_txns.update_commit_ts(read_ts);
            self.running_txns.remove_txn(read_ts);

            txn_map.insert(txn.get_transaction_id(), txn.clone());
        }

        info!("Transaction {} committed successfully", txn.get_transaction_id());
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

        // Write abort log record
        let abort_record = LogRecord::new_transaction_record(
            txn.get_transaction_id(),
            txn.get_prev_lsn(),
            LogRecordType::Abort,
        );
        let lsn = self.log_manager.write().append_log_record(&abort_record);
        txn.set_prev_lsn(lsn);

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
        let write_set = txn.get_write_set();

        // Check write-write conflicts
        for (_table_id, rid) in write_set {
            if let Some(link) = self.get_undo_link(rid) {
                    if let Some(undo_txn) = self.txn_map.read().get(&link.prev_txn) {
                        if undo_txn.commit_ts() > read_ts {
                            return false;
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

    pub fn update_tuple_and_undo_link(
        &self,
        rid: RID,
        undo_link: Option<UndoLink>,
        table_heap: &TableHeap,
        txn: &Transaction,
        meta: &TupleMeta,
        tuple: &mut Tuple,
        check: Option<Box<dyn Fn(&TupleMeta, &Tuple, RID, Option<UndoLink>) -> bool>>,
    ) -> bool {
        // Write update log record before modifying data
        let old_tuple = table_heap.get_tuple(rid).unwrap().1;
        let update_record = LogRecord::new_update_record(
            txn.get_transaction_id(),
            txn.get_prev_lsn(),
            LogRecordType::Update,
            rid,
            old_tuple,
            tuple.clone(),
        );
        let lsn = self.log_manager.write().append_log_record(&update_record);
        txn.set_prev_lsn(lsn);

        // Proceed with update without transaction context
        let update_result = match table_heap.update_tuple(meta, tuple, rid, None) {
            Ok(new_rid) => {
                if new_rid != rid {
                    if !self.update_undo_link(
                        new_rid,
                        undo_link.clone(),
                        Some(Box::new(move |_| true)),
                    ) {
                        let rollback_meta = TupleMeta::new(txn.get_transaction_id());
                        let _ = table_heap.update_tuple_meta(&rollback_meta, new_rid);
                        return false;
                    }
                    new_rid
                } else {
                    if !self.update_undo_link(rid, undo_link.clone(), Some(Box::new(|_| true))) {
                        let rollback_meta = TupleMeta::new(txn.get_transaction_id());
                        let _ = table_heap.update_tuple_meta(&rollback_meta, rid);
                        return false;
                    }
                    rid
                }
            }
            Err(_) => return false,
        };

        // Verify update if check function provided
        if let Some(check_fn) = check {
            if !check_fn(meta, tuple, update_result, undo_link.clone()) {
                let rollback_meta = TupleMeta::new(txn.get_transaction_id());
                let _ = table_heap.update_tuple_meta(&rollback_meta, update_result);
                self.update_undo_link(update_result, None, Some(Box::new(|_| true)));
                return false;
            }
        }

        true
    }

    pub fn get_tuple_and_undo_link(
        &self,
        table_heap: &TableHeap,
        rid: RID,
    ) -> (TupleMeta, Tuple, Option<UndoLink>) {
        // Get the tuple from table heap
        let (meta, tuple) = table_heap.get_tuple(rid).unwrap();

        // Get the undo link using self reference
        let undo_link = self.get_undo_link(rid);

        (meta, tuple, undo_link)
    }
}
