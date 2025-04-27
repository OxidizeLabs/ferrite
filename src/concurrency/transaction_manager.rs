use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::common::config::TableOidT;
use crate::common::config::{PageId, Timestamp, TxnId, INVALID_TXN_ID};
use crate::common::rid::RID;
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction::{
    IsolationLevel, Transaction, TransactionState, UndoLink, UndoLog,
};
use crate::concurrency::watermark::Watermark;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::storage::page::page_types::table_page::TablePage;
use crate::storage::table::table_heap::TableHeap;
use crate::storage::table::transactional_table_heap::TransactionalTableHeap;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct PageVersionInfo {
    // Stores previous version info for all slots
    prev_link: RwLock<HashMap<RID, UndoLink>>,
}

impl PageVersionInfo {
    pub fn new() -> Self {
        Self {
            prev_link: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_link(&self, rid: &RID) -> Option<UndoLink> {
        self.prev_link.read().get(rid).cloned()
    }

    pub fn set_link(&self, rid: RID, link: UndoLink) {
        self.prev_link.write().insert(rid, link);
    }

    pub fn remove_link(&self, rid: &RID) -> Option<UndoLink> {
        self.prev_link.write().remove(rid)
    }

    pub fn is_empty(&self) -> bool {
        self.prev_link.read().is_empty()
    }
}

/// Represents the internal state of the transaction manager
#[derive(Debug)]
struct TransactionManagerState {
    txn_map: RwLock<HashMap<TxnId, Arc<Transaction>>>,
    running_txns: RwLock<Watermark>,
    version_info: RwLock<HashMap<PageId, Arc<PageVersionInfo>>>,
    table_heaps: RwLock<HashMap<TableOidT, Arc<TransactionalTableHeap>>>,
    is_shutdown: AtomicBool,
}

#[derive(Debug, Clone)]
pub struct TransactionManager {
    next_txn_id: Arc<AtomicU64>,
    state: Arc<TransactionManagerState>,
}

impl TransactionManager {
    pub fn new() -> Self {
        // First create the transaction manager without a lock manager
        let tm = Self {
            next_txn_id: Arc::new(AtomicU64::new(0)),
            state: Arc::new(TransactionManagerState {
                txn_map: RwLock::new(HashMap::new()),
                running_txns: RwLock::new(Watermark::default()),
                version_info: RwLock::new(HashMap::new()),
                table_heaps: RwLock::new(HashMap::new()),
                is_shutdown: AtomicBool::new(false),
            }),
        };

        // Create an Arc of the transaction manager
        let tm_arc = Arc::new(tm);

        // Create the final transaction manager with the initialized lock manager
        Self {
            next_txn_id: tm_arc.next_txn_id.clone(),
            state: tm_arc.state.clone(),
        }
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

    /// Begins a new transaction with proper state checks
    pub fn begin(&self, isolation_level: IsolationLevel) -> Result<Arc<Transaction>, String> {
        // Check if transaction manager is shutdown
        if self.state.is_shutdown.load(Ordering::SeqCst) {
            return Err("Transaction manager is shutdown".to_string());
        }

        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let txn = Arc::new(Transaction::new(txn_id, isolation_level));

        // Explicitly set state to Running
        txn.set_state(TransactionState::Running);

        // Update transaction map and running transactions atomically
        {
            let mut txn_map = self.state.txn_map.write();
            let mut running_txns = self.state.running_txns.write();

            // Get and set read timestamp using watermark
            let read_ts = running_txns.get_next_ts_and_register();
            txn.set_read_ts(read_ts);

            txn_map.insert(txn_id, Arc::clone(&txn));
        }

        Ok(txn)
    }

    /// Commits a transaction and updates the watermark
    pub fn commit(&self, txn: Arc<Transaction>, bpm: Arc<BufferPoolManager>) -> bool {
        // Check if transaction is in a valid state to commit
        if txn.get_state() != TransactionState::Running {
            log::debug!(
                "Transaction {} not in running state: {:?}",
                txn.get_transaction_id(),
                txn.get_state()
            );
            return false;
        }

        // Get next timestamp for commit
        let commit_ts = self.state.running_txns.read().get_next_ts();
        log::debug!(
            "Committing transaction {} with commit_ts {}",
            txn.get_transaction_id(),
            commit_ts
        );

        // Set transaction's commit timestamp and state
        txn.set_commit_ts(commit_ts);
        txn.set_state(TransactionState::Committed);

        // Update watermark and transaction map atomically
        {
            let mut watermark = self.state.running_txns.write();
            let mut txn_map = self.state.txn_map.write();

            watermark.remove_txn(txn.read_ts());
            watermark.update_commit_ts(commit_ts);

            // Update transaction map with committed transaction
            txn_map.insert(txn.get_transaction_id(), txn.clone());
            log::debug!(
                "Updated watermark and transaction map for txn {}",
                txn.get_transaction_id()
            );
        }

        // Update commit timestamps in undo logs
        let write_set = txn.get_write_set();
        for (_, rid) in write_set {
            if let Some(undo_link) = self.get_undo_link(rid) {
                let undo_log = self.get_undo_log(undo_link.clone());
                // Create a new UndoLog with updated timestamp
                let mut new_undo_log = UndoLog {
                    is_deleted: undo_log.is_deleted,
                    modified_fields: undo_log.modified_fields.clone(),
                    tuple: undo_log.tuple.clone(),
                    ts: commit_ts,
                    prev_version: undo_log.prev_version.clone(),
                };
                txn.modify_undo_log(undo_link.prev_log_idx, Arc::new(new_undo_log));
            }
        }

        // Flush any dirty pages from this transaction
        for (_table_oid, rid) in txn.get_write_set() {
            // Update tuple metadata with commit timestamp
            let page_id = rid.get_page_id();
            if let Some(page_guard) = bpm.fetch_page::<TablePage>(page_id) {
                let mut page = page_guard.write();
                if let Ok((mut meta, _)) = page.get_tuple(&rid) {
                    // Only update commit timestamp if this transaction created/modified the tuple
                    if meta.get_creator_txn_id() == txn.get_transaction_id() {
                        meta.set_commit_timestamp(commit_ts);
                        if let Err(e) = page.update_tuple_meta(&meta, &rid) {
                            log::error!("Failed to update tuple metadata: {}", e);
                            return false;
                        }
                        log::debug!(
                            "Updated tuple metadata for RID {:?} - creator_txn: {}, commit_ts: {}",
                            rid,
                            meta.get_creator_txn_id(),
                            commit_ts
                        );
                    }
                }
            }

            // Flush the page
            if let Err(e) = bpm.flush_page(page_id) {
                log::error!("Failed to flush page {}: {}", page_id, e);
                return false;
            }
            log::debug!(
                "Flushed page {} for txn {}",
                page_id,
                txn.get_transaction_id()
            );
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

        // Get all modified tuples from write set
        let write_set = txn.get_write_set();

        // Roll back changes using undo logs
        for (table_oid, rid) in write_set {
            if let Some(table_heap) = self.get_table_heap(table_oid) {
                if let Some(undo_link) = self.get_undo_link(rid) {
                    // Get the undo log
                    let undo_log = self.get_undo_log(undo_link.clone());

                    // Restore the previous version
                    let mut meta = TupleMeta::new(undo_link.prev_txn);
                    meta.set_commit_timestamp(undo_log.ts);
                    meta.set_deleted(undo_log.is_deleted);
                    meta.set_undo_log_idx(undo_link.prev_log_idx);

                    // Use a reference to the Arc<Tuple> without cloning
                    if let Err(e) = table_heap.rollback_tuple(&meta, &*undo_log.tuple, rid) {
                        log::error!("Failed to rollback tuple: {}", e);
                    }
                } else {
                    // If no undo link exists, this was a newly inserted tuple
                    // Mark it as deleted since it was part of an aborted transaction
                    let mut meta = TupleMeta::new(txn.get_transaction_id());
                    meta.set_deleted(true);
                    meta.set_commit_timestamp(0); // Set to 0 to ensure it's not visible

                    // Update the tuple metadata to mark it as deleted
                    if let Some(page_guard) = table_heap
                        .get_table_heap()
                        .get_bpm()
                        .fetch_page::<TablePage>(rid.get_page_id())
                    {
                        let mut page = page_guard.write();
                        if let Ok((_, _)) = page.get_tuple(&rid) {
                            if let Err(e) = page.update_tuple_meta(&meta, &rid) {
                                log::error!("Failed to mark tuple as deleted: {}", e);
                            }
                        }
                    }
                }
            }
        }

        // Update transaction state and remove from running transactions
        {
            let mut txn_map = self.state.txn_map.write();
            let mut running_txns = self.state.running_txns.write();

            running_txns.remove_txn(txn.read_ts());
            txn.set_state(TransactionState::Aborted);
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
            Arc::new(PageVersionInfo::new())
        });

        // If check function is provided, verify the update is valid
        if let Some(check_fn) = check {
            let current_link = page_info.get_link(&rid);
            if !check_fn(current_link) {
                return false;
            }
        }

        // Update the link
        match prev_link {
            Some(link) => {
                page_info.set_link(rid, link);
            }
            None => {
                page_info.remove_link(&rid);
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
            .and_then(|page_info| page_info.get_link(&rid))
    }

    pub fn update_tuple(
        &self,
        table_heap: &TableHeap,
        meta: &TupleMeta,
        tuple: &mut Tuple,
        rid: RID,
        txn_ctx: Option<Arc<TransactionContext>>,
    ) -> Result<(), String> {
        // Get the current transaction
        let txn = txn_ctx.as_ref().map(|ctx| ctx.get_transaction());

        // Check if transaction is in a valid state
        if let Some(txn) = &txn {
            if txn.get_state() != TransactionState::Running {
                return Err("Transaction not in running state".to_string());
            }
        }

        // If successful, update transaction's write set
        if let Some(txn) = txn {
            txn.append_write_set(table_heap.get_table_oid(), rid);
        }

        Ok(())
    }

    pub fn get_tuple_and_undo_link(
        &self,
        table_heap: &TableHeap,
        rid: RID,
        lock_manager: Arc<LockManager>,
    ) -> (TupleMeta, Arc<Tuple>, Option<UndoLink>) {
        // Create transaction context
        let txn_ctx = Arc::new(TransactionContext::new(
            Arc::new(Transaction::new(
                INVALID_TXN_ID,
                IsolationLevel::ReadUncommitted,
            )),
            lock_manager,
            Arc::new(self.clone()),
        ));

        // Get the tuple from table heap with transaction context
        let (meta, tuple) = table_heap.get_tuple_with_txn(rid, txn_ctx).unwrap();

        // Get the undo link using self reference
        let undo_link = self.get_undo_link(rid);

        (meta, tuple, undo_link)
    }

    /// Accesses the transaction undo log buffer and gets the undo log.
    ///
    /// # Parameters
    /// - `link`: The undo link.
    ///
    /// # Returns
    /// The undo log, if it exists.
    pub fn get_undo_log_optional(&self, link: UndoLink) -> Option<Arc<UndoLog>> {
        // Add debug logging
        log::debug!(
            "Looking up transaction {} for undo log index {}",
            link.prev_txn,
            link.prev_log_idx
        );

        self.state
            .txn_map
            .read()
            .get(&link.prev_txn)
            .and_then(|txn| Some(txn.get_undo_log(link.prev_log_idx)))
    }

    /// Accesses the transaction undo log buffer and gets the undo log.
    ///
    /// # Parameters
    /// - `link`: The undo link.
    ///
    /// # Returns
    /// The undo log.
    pub fn get_undo_log(&self, link: UndoLink) -> Arc<UndoLog> {
        // Add debug logging
        log::debug!(
            "Getting undo log for link: txn={}, idx={}",
            link.prev_txn,
            link.prev_log_idx
        );

        self.get_undo_log_optional(link)
            .expect("Failed to get undo log")
    }

    /// Gets the lowest read timestamp in the system.
    ///
    /// # Returns
    /// The watermark.
    pub fn get_watermark(&self) -> Timestamp {
        self.state.running_txns.read().get_watermark()
    }

    /// Gets the running transactions watermark
    pub fn get_running_transactions(&self) -> Watermark {
        self.state.running_txns.read().clone_watermark()
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

    /// Gets the undo link for a specific transaction
    pub fn get_undo_link_for_txn(&self, rid: RID, txn_id: TxnId) -> Option<UndoLink> {
        let version_info = self.state.version_info.read();
        version_info.get(&rid.get_page_id()).and_then(|page_info| {
            page_info
                .prev_link
                .read()
                .get(&rid)
                .filter(|link| link.prev_txn == txn_id)
                .cloned()
        })
    }

    // Add method to register table heaps
    pub fn register_table(&self, table_heap: Arc<TransactionalTableHeap>) {
        let mut table_heaps = self.state.table_heaps.write();
        table_heaps.insert(table_heap.get_table_oid(), table_heap);
    }

    fn get_table_heap(&self, table_oid: TableOidT) -> Option<Arc<TransactionalTableHeap>> {
        self.state.table_heaps.read().get(&table_oid).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::common::config::TableOidT;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use std::thread;
    use tempfile::TempDir;

    /// Test context that holds shared components
    struct TestContext {
        txn_manager: Arc<TransactionManager>,
        lock_manager: Arc<LockManager>,
        catalog: Arc<RwLock<Catalog>>,
        buffer_pool: Arc<BufferPoolManager>,
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

            // Create transaction manager with its own lock manager
            let txn_manager = Arc::new(TransactionManager::new());

            let lock_manager = Arc::new(LockManager::new());

            // Create catalog using the transaction manager
            let catalog = Arc::new(RwLock::new(Catalog::new(
                buffer_pool.clone(),
                0,
                0,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                txn_manager.clone(),
            )));

            Self {
                txn_manager,
                lock_manager,
                catalog,
                buffer_pool,
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

            let table_heap = Arc::new(TableHeap::new(
                self.buffer_pool.clone(),
                table_info.get_table_oidt(),
            ));

            // Create and register transactional table heap
            let txn_table_heap = Arc::new(TransactionalTableHeap::new(
                table_heap.clone(),
                table_info.get_table_oidt(),
            ));
            self.txn_manager.register_table(txn_table_heap);

            (table_info.get_table_oidt(), table_heap)
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

        fn buffer_pool_manager(&self) -> Arc<BufferPoolManager> {
            self.buffer_pool.clone()
        }

        fn txn_manager(&self) -> Arc<TransactionManager> {
            self.txn_manager.clone()
        }

        fn lock_manager(&self) -> Arc<LockManager> {
            self.lock_manager.clone()
        }
    }

    #[test]
    fn test_begin_transaction() {
        let ctx = TestContext::new("test_begin_transaction");

        // Begin transaction
        let txn = ctx
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        let txn_id = txn.get_transaction_id();

        // Verify transaction state
        assert_eq!(txn.get_isolation_level(), IsolationLevel::ReadCommitted);
        assert_eq!(txn.get_state(), TransactionState::Running);
        assert_eq!(txn_id, 0);

        // Verify transaction is tracked
        {
            let txn_manager = ctx.txn_manager();
            assert!(txn_manager.get_transaction(&txn_id).is_some());
            assert_eq!(txn_manager.get_active_transaction_count(), 1);
        }

        // Start another transaction
        let txn2 = ctx
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        assert_eq!(txn2.get_transaction_id(), 1);
        assert_eq!(ctx.txn_manager().get_active_transaction_count(), 2);
    }

    #[test]
    fn test_commit_transaction() {
        let ctx = TestContext::new("test_commit_transaction");

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
            ctx.lock_manager(),
            ctx.txn_manager(),
        ));

        // Insert tuple
        let rid = table_heap
            .insert_tuple(&TupleMeta::new(txn_id), &mut tuple)
            .unwrap();

        // Append to write set
        txn.append_write_set(table_oid, rid);

        // Commit transaction
        assert!(ctx
            .txn_manager()
            .commit(txn.clone(), ctx.buffer_pool_manager()));
        assert_eq!(txn.get_state(), TransactionState::Committed);

        // Verify tuple with a new transaction using ReadCommitted isolation
        let verify_txn = ctx
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();

        // Ensure verify_txn has a higher timestamp than the commit
        thread::sleep(std::time::Duration::from_millis(1));

        // Get tuple with verification transaction
        let result = table_heap.get_tuple_with_txn(rid, txn_ctx);
        assert!(
            result.is_ok(),
            "Failed to read committed tuple: {:?}",
            result.err()
        );

        let (meta, committed_tuple) = result.unwrap();
        assert_eq!(meta.get_creator_txn_id(), txn_id);
        assert_eq!(committed_tuple.get_values(), tuple.get_values());

        // Cleanup
        ctx.txn_manager()
            .commit(verify_txn, ctx.buffer_pool_manager());
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
            ctx.lock_manager(),
            ctx.txn_manager(),
        ));

        // Insert tuple
        let rid = table_heap
            .insert_tuple(&TupleMeta::new(txn_id), &mut tuple)
            .unwrap();

        txn.append_write_set(table_oid, rid);

        // Abort transaction
        ctx.txn_manager().abort(txn.clone());
        assert_eq!(txn.get_state(), TransactionState::Aborted);

        // Verify tuple is not visible with a new transaction
        let verify_txn = ctx
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        let verify_ctx = Arc::new(TransactionContext::new(
            verify_txn.clone(),
            ctx.lock_manager(),
            ctx.txn_manager(),
        ));

        // Get tuple should fail since transaction was aborted
        let result = table_heap.get_tuple_with_txn(rid, txn_ctx);
        assert!(
            result.is_err(),
            "Tuple from aborted transaction should not be visible"
        );

        // Cleanup
        ctx.txn_manager()
            .commit(verify_txn, ctx.buffer_pool_manager());
    }

    #[test]
    fn test_transaction_isolation() {
        let ctx = TestContext::new("test_transaction_isolation");

        let (table_oid, table_heap) = ctx.create_test_table();
        let txn_table_heap = Arc::new(TransactionalTableHeap::new(table_heap.clone(), table_oid));

        // Start two transactions
        let txn1 = ctx
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        let txn2 = ctx
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();

        let txn_ctx1 = Arc::new(TransactionContext::new(
            txn1.clone(),
            ctx.lock_manager(),
            ctx.txn_manager(),
        ));
        let txn_ctx2 = Arc::new(TransactionContext::new(
            txn2.clone(),
            ctx.lock_manager(),
            ctx.txn_manager(),
        ));

        // Insert with txn1
        let mut tuple = TestContext::create_test_tuple();
        let rid = txn_table_heap
            .insert_tuple(
                &TupleMeta::new(txn1.get_transaction_id()),
                &mut tuple,
                txn_ctx1.clone(),
            )
            .unwrap();

        // Try to update with txn2 before txn1 commits - should fail
        let mut modified_tuple = tuple;
        modified_tuple.get_values_mut()[1] = Value::new(200);

        let update_result = txn_table_heap.update_tuple(
            &TupleMeta::new(txn2.get_transaction_id()),
            &mut modified_tuple,
            rid,
            txn_ctx2.clone(),
        );
        assert!(update_result.is_err(), "Update should fail before commit");

        // Commit txn1
        assert!(ctx.txn_manager().commit(txn1, ctx.buffer_pool_manager()));

        // Now txn2 should succeed in updating
        let update_result = txn_table_heap.update_tuple(
            &TupleMeta::new(txn2.get_transaction_id()),
            &mut modified_tuple,
            rid,
            txn_ctx2.clone(),
        );
        assert!(update_result.is_ok(), "Update should succeed after commit");

        // Cleanup
        ctx.txn_manager().commit(txn2, ctx.buffer_pool_manager());
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
