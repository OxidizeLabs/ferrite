use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::catalog::catalog::Catalog;
use crate::common::config::TxnId;
use crate::common::rid::RID;
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction::{
    IsolationLevel, Transaction, TransactionState, UndoLink, UndoLog,
};
use crate::concurrency::watermark::Watermark;
use crate::recovery::log_manager::LogManager;
use crate::recovery::wal_manager::WALManager;
use crate::sql::execution::expressions::abstract_expression::ExpressionOps;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::storage::table::table_heap::TableHeap;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

#[derive(Debug)]
pub struct PageVersionInfo {
    // Stores previous version info for all slots
    prev_link: RwLock<HashMap<RID, UndoLink>>,
}

/// Represents the internal state of the transaction manager
#[derive(Debug)]
struct TransactionManagerState {
    txn_map: RwLock<HashMap<TxnId, Arc<Transaction>>>,
    running_txns: RwLock<Watermark>,
    version_info: RwLock<HashMap<u64, Arc<PageVersionInfo>>>,
    is_shutdown: AtomicBool,
}

#[derive(Debug, Clone)]
pub struct TransactionManager {
    next_txn_id: Arc<AtomicU64>,
    state: Arc<TransactionManagerState>,
    wal_manager: Arc<WALManager>,
}

impl TransactionManager {
    pub fn new(log_manager: Arc<RwLock<LogManager>>) -> Self {
        TransactionManager {
            next_txn_id: Arc::new(AtomicU64::new(0)),
            state: Arc::new(TransactionManagerState {
                txn_map: RwLock::new(HashMap::new()),
                running_txns: RwLock::new(Watermark::default()),
                version_info: RwLock::new(HashMap::new()),
                is_shutdown: AtomicBool::new(false),
            }),
            wal_manager: Arc::new(WALManager::new(log_manager)),
        }
    }

    /// Begins a new transaction with proper state checks
    pub fn begin(&self, isolation_level: IsolationLevel) -> Result<Arc<Transaction>, String> {
        // Check if transaction manager is shutdown
        if self.state.is_shutdown.load(Ordering::SeqCst) {
            return Err("Transaction manager is shutdown".to_string());
        }

        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let txn = Arc::new(Transaction::new(txn_id, isolation_level));

        // Write begin log record using WALManager
        let lsn = self.wal_manager.write_begin_record(&txn);
        txn.set_prev_lsn(lsn);

        // Update transaction map and running transactions atomically
        {
            let mut txn_map = self.state.txn_map.write();
            let mut running_txns = self.state.running_txns.write();

            txn_map.insert(txn_id, Arc::clone(&txn));
            running_txns.add_txn(txn.read_ts());
        }

        Ok(txn)
    }

    /// Shuts down the transaction manager
    pub fn shutdown(&self) -> Result<(), String> {
        // Set shutdown flag
        self.state.is_shutdown.store(true, Ordering::SeqCst);

        // Wait for active transactions to complete
        let active_txns = {
            let txn_map = self.state.txn_map.read();
            txn_map
                .values()
                .filter(|txn| txn.get_state() == TransactionState::Running)
                .cloned()
                .collect::<Vec<_>>()
        };

        for txn in active_txns {
            self.abort(txn);
        }

        Ok(())
    }

    /// Gets the current state of the transaction manager
    pub fn get_state(&self) -> Arc<TransactionManagerState> {
        self.state.clone()
    }

    /// Commits the specified transaction.
    ///
    /// # Parameters
    /// - `txn`: The transaction to commit.
    /// - `buffer_pool`: The buffer pool for flushing pages.
    ///
    /// # Returns
    /// `true` if the transaction was successfully committed; otherwise, `false`.
    pub fn commit(&self, txn: Arc<Transaction>, buffer_pool: Arc<BufferPoolManager>) -> bool {
        if txn.get_state() != TransactionState::Running {
            panic!("txn not in running state");
        }

        // 1. Get write set before any state changes
        let write_set = txn.get_write_set();

        // 2. Flush dirty pages
        let mut all_pages_flushed = true;
        for (_, rid) in write_set {
            let page_id = rid.get_page_id();
            if let Some(success) = buffer_pool.flush_page(page_id) {
                if !success {
                    all_pages_flushed = false;
                    break;
                }
            }
        }

        if !all_pages_flushed {
            self.abort(txn);
            return false;
        }

        // Write commit log record using WALManager
        let lsn = self.wal_manager.write_commit_record(&txn);
        txn.set_prev_lsn(lsn);

        // 3. Update transaction state and metadata in a single critical section
        {
            let mut txn_map = self.state.txn_map.write();
            let mut running_txns = self.state.running_txns.write();

            txn.set_state(TransactionState::Committed);
            let read_ts = txn.read_ts();
            running_txns.update_commit_ts(read_ts);
            running_txns.remove_txn(read_ts);
            txn_map.insert(txn.get_transaction_id(), txn.clone());
        }

        true
    }

    /// Aborts the specified transaction.
    ///
    /// # Parameters
    /// - `txn`: The transaction to abort.
    pub fn abort(&self, txn: Arc<Transaction>) {
        let current_state = txn.get_state();
        if current_state != TransactionState::Running && current_state != TransactionState::Tainted
        {
            panic!("txn not in running / tainted state");
        }

        // Write abort log record using WALManager
        let lsn = self.wal_manager.write_abort_record(&txn);
        txn.set_prev_lsn(lsn);

        {
            let mut txn_map = self.state.txn_map.write();
            txn.set_state(TransactionState::Aborted);
            self.state.running_txns.write().remove_txn(txn.read_ts());
            txn_map.insert(txn.get_transaction_id(), txn.clone());
        }
    }

    /// Performs garbage collection.
    pub fn garbage_collection(&self) {
        let watermark = self.get_watermark();
        let mut version_info = self.state.version_info.write();

        // Remove version info for pages that are no longer needed
        version_info.retain(|_, page_info| {
            // Retain only links that point to transactions newer than watermark
            page_info.prev_link.write().retain(|_, link| {
                if let Some(txn) = self.state.txn_map.read().get(&link.prev_txn) {
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
        let mut version_info = self.state.version_info.write();
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
        let version_info = self.state.version_info.read();
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

        self.state
            .txn_map
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
        self.state.running_txns.read().get_watermark()
    }

    pub fn get_transaction(&self, txn_id: &TxnId) -> Option<Arc<Transaction>> {
        self.state.txn_map.read().get(txn_id).cloned()
    }

    pub fn get_transactions(&self) -> Vec<Arc<Transaction>> {
        self.state.txn_map.read().values().cloned().collect()
    }

    pub fn get_active_transaction_count(&self) -> usize {
        self.get_transactions().len()
    }

    pub fn get_next_transaction_id(&self) -> u64 {
        self.state.txn_map.read().keys().next().map_or(0, |k| *k)
    }

    pub fn verify_transaction(&self, txn: &Transaction, catalog: Option<&Catalog>) -> bool {
        if txn.get_isolation_level() != IsolationLevel::Serializable {
            return true;
        }

        let read_ts = txn.read_ts();
        let write_set = txn.get_write_set();

        // Check write-write conflicts
        for (_table_id, rid) in write_set {
            if let Some(link) = self.get_undo_link(rid) {
                if let Some(undo_txn) = self.state.txn_map.read().get(&link.prev_txn) {
                    if undo_txn.commit_ts() > read_ts {
                        return false;
                    }
                }
            }
        }

        // Check predicate-based conflicts
        let scan_predicates = txn.get_scan_predicates();
        if let Some(catalog) = catalog {
            for (table_id, predicates) in scan_predicates {
                if let Some(table_info) = catalog.get_table_by_oid(table_id.into()) {
                    let table_heap = table_info.get_table_heap();
                    let schema = table_info.get_table_schema();
                    let mut iter = table_heap.make_iterator();

                    while let Some((_, tuple)) = iter.next() {
                        if let Some(link) = self.get_undo_link(tuple.get_rid()) {
                            if let Some(undo_txn) = self.state.txn_map.read().get(&link.prev_txn) {
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
        // Write update log record using WALManager
        let old_tuple = table_heap.get_tuple(rid).unwrap().1;
        let lsn = self
            .wal_manager
            .write_update_record(txn, rid, old_tuple, tuple.clone());
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::common::config::TableOidT;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use tempfile::TempDir;

    /// Test context that holds shared components
    struct TestContext {
        txn_manager: Arc<TransactionManager>,
        catalog: Arc<RwLock<Catalog>>,
        buffer_pool: Arc<BufferPoolManager>,
        lock_manager: Arc<LockManager>,
        _temp_dir: TempDir, // Keep temp dir alive
    }

    impl TestContext {
        fn new(name: &str) -> Self {
            // Create temporary directory
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir
                .path()
                .join(format!("{name}.db"))
                .to_str()
                .unwrap()
                .to_string();
            let log_path = temp_dir
                .path()
                .join(format!("{name}.log"))
                .to_str()
                .unwrap()
                .to_string();

            // Create disk components
            let disk_manager = Arc::new(FileDiskManager::new(db_path, log_path, 10));
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));

            // Create buffer pool
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(10, 2)));
            let buffer_pool = Arc::new(BufferPoolManager::new(
                10,
                disk_scheduler,
                disk_manager.clone(),
                replacer,
            ));

            // Create log manager
            let log_manager = Arc::new(RwLock::new(LogManager::new(disk_manager)));

            // Create catalog
            let catalog = Arc::new(RwLock::new(Catalog::new(
                buffer_pool.clone(),
                0,
                0,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )));

            // Create transaction manager without catalog
            let txn_manager = Arc::new(TransactionManager::new(log_manager));
            let lock_manager = Arc::new(LockManager::new(txn_manager.clone()));

            Self {
                txn_manager,
                catalog,
                buffer_pool,
                lock_manager,
                _temp_dir: temp_dir,
            }
        }

        fn create_test_table(&self) -> (TableOidT, Arc<TableHeap>) {
            let schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("value", TypeId::Integer),
            ]);

            let table_info = self
                .catalog
                .write()
                .create_table("test_table".to_string(), schema)
                .unwrap();

            (table_info.get_table_oidt(), table_info.get_table_heap())
        }

        fn create_test_tuple() -> Tuple {
            let schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("value", TypeId::Integer),
            ]);
            Tuple::new(&[Value::new(1), Value::new(100)], schema, RID::new(0, 0))
        }

        fn begin_transaction(
            &self,
            isolation_level: IsolationLevel,
        ) -> Result<Arc<Transaction>, String> {
            self.txn_manager.begin(isolation_level)
        }

        fn lock_manager(&self) -> Arc<LockManager> {
            self.lock_manager.clone()
        }

        fn buffer_pool_manager(&self) -> Arc<BufferPoolManager> {
            self.buffer_pool.clone()
        }

        fn transaction_manager(&self) -> Arc<TransactionManager> {
            self.txn_manager.clone()
        }
    }

    #[test]
    fn test_begin_transaction() {
        let ctx = TestContext::new("test_begin_transaction");

        // Begin transaction - the ID will be 0 since it's the first transaction
        let txn = ctx
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        let txn_id = txn.get_transaction_id();

        // Verify transaction state
        assert_eq!(txn.get_isolation_level(), IsolationLevel::ReadCommitted);
        assert_eq!(txn.get_state(), TransactionState::Running);
        assert_eq!(txn_id, 0); // First transaction should have ID 0

        // Verify transaction is tracked - use separate scope for locks
        {
            let txn_manager = ctx.txn_manager.clone();
            let contains_key = {
                let txn_map = txn_manager.state.txn_map.read();
                txn_map.contains_key(&txn_id)
            };
            assert!(
                contains_key,
                "Transaction map should contain txn_id {}",
                txn_id
            );
        }

        // Start another transaction - should have ID 1
        let txn2 = ctx
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        assert_eq!(txn2.get_transaction_id(), 1);
    }

    #[test]
    fn test_commit_transaction() {
        let ctx = TestContext::new("test_commit_transaction");

        // Begin transaction
        let txn = {
            let txn = ctx
                .begin_transaction(IsolationLevel::ReadCommitted)
                .unwrap();
            txn
        };

        // Create test table and insert tuple - do this in a separate scope
        let (table_oid, table_heap, rid) = {
            let (table_oid, table_heap) = ctx.create_test_table();
            let mut tuple = TestContext::create_test_tuple();

            // Create transaction context
            let txn_ctx = Arc::new(TransactionContext::new(
                txn.clone(),
                ctx.lock_manager.clone(),
                ctx.txn_manager.clone(),
            ));

            // Insert tuple
            let rid = table_heap
                .insert_tuple(
                    &TupleMeta::new(txn.get_transaction_id()),
                    &mut tuple,
                    Some(txn_ctx),
                )
                .unwrap();

            (table_oid, table_heap, rid)
        };

        // Append to write set
        txn.append_write_set(table_oid, rid);

        // Commit transaction
        let commit_success = ctx.txn_manager.commit(txn.clone(), ctx.buffer_pool.clone());
        assert!(commit_success, "Transaction commit failed");

        // Verify state after commit
        assert_eq!(txn.get_state(), TransactionState::Committed);

        // Verify tuple exists and is visible - do this in a separate scope
        let (meta, committed_tuple) = table_heap.get_tuple(rid).unwrap();
        assert_eq!(meta.get_creator_txn_id(), txn.get_transaction_id());

        let expected_tuple = TestContext::create_test_tuple();
        assert_eq!(committed_tuple.get_values(), expected_tuple.get_values());
    }

    #[test]
    fn test_abort_transaction() {
        let ctx = TestContext::new("test_abort_transaction");

        // Begin transaction
        let txn = ctx
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        let txn_id = txn.get_transaction_id();

        // Create test table and insert tuple
        let (table_oid, table_heap) = ctx.create_test_table();
        let mut tuple = TestContext::create_test_tuple();

        // Create transaction context
        let txn_ctx = Arc::new(TransactionContext::new(
            txn.clone(),
            ctx.lock_manager.clone(),
            ctx.txn_manager.clone(),
        ));

        // Insert tuple
        let rid = table_heap
            .insert_tuple(&TupleMeta::new(txn_id), &mut tuple, Some(txn_ctx))
            .unwrap();

        txn.append_write_set(table_oid, rid);

        // Abort transaction
        ctx.txn_manager.abort(txn.clone());

        // Verify transaction state
        assert_eq!(txn.get_state(), TransactionState::Aborted);

        // Verify transaction is removed from map
        {
            let txn_manager = ctx.txn_manager;
            let txn_map = txn_manager.state.txn_map.read();
            assert!(
                !txn_map.contains_key(&txn_id),
                "Aborted transaction should be removed from map"
            );
        }

        // Verify tuple is not visible
        match table_heap.get_tuple(rid) {
            Ok(_) => panic!("Tuple should not be visible after abort"),
            Err(_) => (), // Expected - tuple should not be found
        }
    }

    #[test]
    fn test_transaction_isolation() {
        // Setup test context
        let ctx = TestContext::new("test_transaction_isolation");
        let mut txn_manager = ctx.txn_manager.clone();
        let catalog = ctx.catalog.clone();

        let (table_oid, table_heap) = ctx.create_test_table();
        // Create two transactions with different isolation levels
        let txn1 = txn_manager.begin(IsolationLevel::ReadCommitted).unwrap();
        let txn2 = txn_manager.begin(IsolationLevel::ReadCommitted).unwrap();
        let txn_ctx1 = Arc::new(TransactionContext::new(
            txn1.clone(),
            ctx.lock_manager(),
            ctx.transaction_manager()
        ));
        let txn_ctx2 = Arc::new(TransactionContext::new(
            txn2.clone(),
            ctx.lock_manager(),
            ctx.transaction_manager()
        ));

        // Insert a tuple with txn1
        let mut tuple = TestContext::create_test_tuple();

         // Insert tuple
        let rid = table_heap.insert_tuple(
                    &TupleMeta::new(txn1.get_transaction_id()),
                    &mut tuple,
                    Some(txn_ctx1),
                ).unwrap();


        // Attempt to update the same tuple with txn2 before txn1 commits
        let mut modified_tuple = tuple.clone();
        modified_tuple.set_values(vec![Value::new(200)]);

        // This should fail because txn1 hasn't committed yet
        assert!(table_heap.update_tuple(
            &TupleMeta::new(txn2.get_transaction_id()),
            &mut modified_tuple,
            rid,
            Some(txn_ctx2.clone())
        ).is_err(), "Update should fail due to uncommitted write from txn1");

        // Commit txn1
        txn_manager.commit(txn1, ctx.buffer_pool.clone());

        // Now txn2 should be able to update the tuple
        assert!(table_heap.update_tuple(
            &TupleMeta::new(txn2.get_transaction_id()),
            &mut modified_tuple,
            rid,
            Some(txn_ctx2)
        ).is_ok(), "Update should succeed after txn1 commits");

        // Cleanup
        txn_manager.commit(txn2, ctx.buffer_pool);
    }

    #[test]
    fn test_transaction_manager_shutdown() {
        let ctx = TestContext::new("test_transaction_manager_shutdown");

        // Start a transaction
        let txn1 = ctx
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();

        // Shutdown transaction manager
        assert!(ctx.txn_manager.shutdown().is_ok());

        // Verify can't start new transactions
        let result = ctx.begin_transaction(IsolationLevel::ReadCommitted);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Transaction manager is shutdown".to_string()
        );

        // Verify original transaction was aborted
        assert_eq!(txn1.get_state(), TransactionState::Aborted);
    }

    #[test]
    fn test_concurrent_transactions() {
        let ctx = TestContext::new("test_concurrent_transactions");
        let thread_count = 10;
        let mut handles = vec![];

        for _ in 0..thread_count {
            let txn_manager = ctx.txn_manager.clone();
            let handle = thread::spawn(move || {
                let txn = txn_manager.begin(IsolationLevel::ReadCommitted).unwrap();
                assert_eq!(txn.get_state(), TransactionState::Running);
                txn
            });
            handles.push(handle);
        }

        let txns: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Verify all transactions got unique IDs
        let mut txn_ids: Vec<_> = txns.iter().map(|txn| txn.get_transaction_id()).collect();
        txn_ids.sort();
        txn_ids.dedup();
        assert_eq!(txn_ids.len(), thread_count);
    }
}
