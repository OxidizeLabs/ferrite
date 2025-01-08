use crate::catalog::catalog::Catalog;
use crate::common::config::{TxnId, INVALID_LSN};
use crate::common::rid::RID;
use crate::concurrency::transaction::{
    IsolationLevel, Transaction, TransactionState, UndoLink, UndoLog,
};
use crate::concurrency::watermark::Watermark;
use crate::execution::expressions::abstract_expression::ExpressionOps;
use crate::recovery::log_manager::LogManager;
use crate::recovery::log_record::{LogRecord, LogRecordType};
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
    catalog: Arc<RwLock<Catalog>>,
    version_info: RwLock<HashMap<u64, Arc<PageVersionInfo>>>,
    log_manager: Arc<RwLock<LogManager>>,
}

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

        if !self.verify_transaction(&txn) {
            self.abort(txn);
            return false;
        }

        // Write commit log record
        let commit_record = LogRecord::new_transaction_record(
            txn.get_transaction_id(),
            txn.get_prev_lsn(),
            LogRecordType::Commit,
        );
        let lsn = self.log_manager.write().append_log_record(&commit_record);
        txn.set_prev_lsn(lsn);

        // Update transaction state and metadata
        {
            let mut txn_map = self.txn_map.write();
            txn.set_state(TransactionState::Committed);

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
    let lsn = txn_mgr
        .log_manager
        .write()
        .append_log_record(&update_record);
    txn.set_prev_lsn(lsn);

    // Proceed with update
    let update_result = match table_heap.update_tuple(meta, tuple, rid) {
        Ok(new_rid) => {
            if new_rid != rid {
                if !txn_mgr.update_undo_link(
                    new_rid,
                    undo_link.clone(),
                    Some(Box::new(move |_| true)),
                ) {
                    let rollback_meta = TupleMeta::new(txn.get_transaction_id(), true);
                    let _ = table_heap.update_tuple_meta(&rollback_meta, new_rid);
                    return false;
                }
                new_rid
            } else {
                if !txn_mgr.update_undo_link(rid, undo_link.clone(), Some(Box::new(|_| true))) {
                    let rollback_meta = TupleMeta::new(txn.get_transaction_id(), true);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use chrono::Utc;
    use parking_lot::RwLock;
    use std::fs;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    fn init_test_logger() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();
    }

    pub struct TestContext {
        transaction_manager: Arc<RwLock<TransactionManager>>,
        db_file: String,
        db_log_file: String,
        table_heap: Arc<TableHeap>,
        schema: Schema,
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            init_test_logger();

            const BUFFER_POOL_SIZE: usize = 10;
            const K: usize = 2;
            let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
            let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
            let db_log_file = format!("tests/data/{}_{}.log", test_name, timestamp);

            let disk_manager = Arc::new(FileDiskManager::new(
                db_file.clone(),
                db_log_file.clone(),
                100,
            ));
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

            let log_manager = Arc::new(RwLock::new(LogManager::new(disk_manager)));
            let catalog = create_catalog(bpm.clone());
            let transaction_manager = Arc::new(RwLock::new(TransactionManager::new(
                catalog.clone(),
                log_manager.clone(),
            )));

            // Create test schema
            let schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("value", TypeId::Integer),
            ]);

            // Create a test table with schema
            let table_heap = Arc::new(TableHeap::new(bpm.clone()));

            Self {
                db_file,
                db_log_file,
                transaction_manager,
                table_heap,
                schema,
            }
        }

        fn create_test_tuple(&self, id: i32, value: i32) -> Tuple {
            Tuple::new(
                &[Value::from(id), Value::from(value)],
                self.schema.clone(),
                RID::new(0, 0),  // Default RID that will be updated on insert
            )
        }

        fn get_tuple_with_transaction(&self, rid: RID, txn: &Transaction) -> Option<Tuple> {
            match get_tuple_and_undo_link(&self.transaction_manager.read(), &self.table_heap, rid) {
                (meta, tuple, _) => {
                    if meta.get_timestamp() == txn.get_transaction_id() ||
                        (!meta.is_deleted() &&
                            self.transaction_manager.read().get_transaction(&meta.get_timestamp())
                                .map_or(false, |t| t.get_state() == TransactionState::Committed)) {
                        Some(tuple)
                    } else {
                        None
                    }
                }
            }
        }

        fn cleanup(&self) {
            let _ = fs::remove_file(&self.db_file);
            let _ = fs::remove_file(&self.db_log_file);
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            self.cleanup();
        }
    }

    fn create_catalog(bpm: Arc<BufferPoolManager>) -> Arc<RwLock<Catalog>> {
        Arc::new(RwLock::new(Catalog::new(
            bpm,
            0,
            0,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )))
    }

    #[test]
    fn test_basic_transaction_lifecycle() {
        let ctx = TestContext::new("test_basic_transaction_lifecycle");

        // Begin transaction
        let txn = ctx.transaction_manager.write().begin(IsolationLevel::ReadCommitted);
        assert_eq!(txn.get_state(), TransactionState::Running);

        // Insert a tuple
        let mut tuple = ctx.create_test_tuple(1, 100);
        let meta = TupleMeta::new(txn.get_transaction_id(), false);
        let rid = ctx.table_heap.insert_tuple(&meta, &mut tuple).unwrap();

        // Commit transaction
        assert!(ctx.transaction_manager.write().commit(txn.clone()));
        assert_eq!(txn.get_state(), TransactionState::Committed);

        // Verify tuple exists
        let (_, result_tuple) = ctx.table_heap.get_tuple(rid).unwrap();
        assert_eq!(result_tuple, tuple);
    }

    #[test]
    fn test_transaction_abort() {
        let ctx = TestContext::new("test_transaction_abort");

        // Start transaction
        let txn = ctx.transaction_manager.write().begin(IsolationLevel::ReadCommitted);

        // Insert a tuple that will be rolled back
        let mut tuple = ctx.create_test_tuple(1, 100);
        let meta = TupleMeta::new(txn.get_transaction_id(), false);
        let rid = ctx.table_heap.insert_tuple(&meta, &mut tuple).unwrap();

        // Verify tuple is visible to the transaction
        assert!(ctx.get_tuple_with_transaction(rid, &txn).is_some());

        // Abort transaction
        ctx.transaction_manager.write().abort(txn.clone());
        assert_eq!(txn.get_state(), TransactionState::Aborted);

        // Start a new transaction and verify tuple is not visible
        let new_txn = ctx.transaction_manager.write().begin(IsolationLevel::ReadCommitted);
        assert!(ctx.get_tuple_with_transaction(rid, &new_txn).is_none());
    }

    #[test]
    fn test_serializable_transaction_verification() {
        let ctx = TestContext::new("test_serializable_transaction_verification");

        // First transaction - Insert tuple
        let txn1 = ctx.transaction_manager.write().begin(IsolationLevel::Serializable);
        let mut tuple1 = ctx.create_test_tuple(1, 100);
        let meta1 = TupleMeta::new(txn1.get_transaction_id(), false);
        let rid = ctx.table_heap.insert_tuple(&meta1, &mut tuple1).unwrap();

        // Add transaction to write set
        txn1.append_write_set(0, rid);

        // Explicitly set timestamps to create conflict scenario
        let base_ts = txn1.read_ts();
        txn1.set_commit_ts(base_ts + 50);
        ctx.transaction_manager.write().commit(txn1.clone());

        // Sleep to ensure timestamp difference
        thread::sleep(Duration::from_millis(1));

        // Second transaction - Try to update with serializable isolation
        let txn2 = ctx.transaction_manager.write().begin(IsolationLevel::Serializable);

        // Set read timestamp to be less than txn1's commit timestamp
        // This should create a serialization failure
        txn2.set_read_ts(base_ts);  // Set read_ts to be less than txn1's commit_ts

        // Add to write set before attempting update
        txn2.append_write_set(0, rid);

        // Create undo link to first transaction
        let undo_link = Some(UndoLink {
            prev_txn: txn1.get_transaction_id(),
            prev_log_idx: 0,
        });

        // Update version chain
        assert!(ctx.transaction_manager.read().update_undo_link(rid, undo_link.clone(), None));

        // Now try to commit txn2 - should fail verification
        assert!(!ctx.transaction_manager.write().commit(txn2.clone()),
                "Serializable transaction should fail to commit due to write-write conflict");

        // Verify original tuple is still intact
        let verification_txn = ctx.transaction_manager.write().begin(IsolationLevel::ReadCommitted);
        let result_tuple = ctx.get_tuple_with_transaction(rid, &verification_txn).unwrap();
        assert_eq!(result_tuple.get_value(0), &Value::from(1));
        assert_eq!(result_tuple.get_value(1), &Value::from(100));
    }

    #[test]
    fn test_transaction_isolation_levels() {
        let ctx = TestContext::new("test_transaction_isolation_levels");

        // First transaction - Insert tuple with READ COMMITTED
        let txn1 = ctx.transaction_manager.write().begin(IsolationLevel::ReadCommitted);
        let mut tuple1 = ctx.create_test_tuple(1, 100);
        let meta1 = TupleMeta::new(txn1.get_transaction_id(), false);
        let rid = ctx.table_heap.insert_tuple(&meta1, &mut tuple1).unwrap();

        // Set timestamps for first transaction
        let base_ts = txn1.read_ts();
        txn1.set_commit_ts(base_ts + 50);
        ctx.transaction_manager.write().commit(txn1.clone());

        // Sleep briefly to ensure timestamp difference
        thread::sleep(Duration::from_millis(1));

        // Test SERIALIZABLE transaction
        let txn_serial = ctx.transaction_manager.write().begin(IsolationLevel::Serializable);
        txn_serial.set_read_ts(base_ts);  // Set read_ts to be less than txn1's commit_ts

        // Add to write set
        txn_serial.append_write_set(0, rid);

        // Create undo link
        let undo_link = Some(UndoLink {
            prev_txn: txn1.get_transaction_id(),
            prev_log_idx: 0,
        });

        // Update version chain
        assert!(ctx.transaction_manager.read().update_undo_link(rid, undo_link.clone(), None));

        // Try to commit - should fail verification
        assert!(!ctx.transaction_manager.write().commit(txn_serial.clone()),
                "SERIALIZABLE transaction should fail to commit due to isolation violation");
    }

    #[test]
    fn test_concurrent_transactions() {
        let ctx = Arc::new(TestContext::new("test_concurrent_transactions"));
        let num_threads = 5;
        let mut handles = vec![];

        for i in 0..num_threads {
            let ctx_clone = Arc::clone(&ctx);
            let handle = thread::spawn(move || {
                let txn = ctx_clone.transaction_manager.write()
                    .begin(IsolationLevel::ReadCommitted);

                // Insert a tuple
                let mut tuple = ctx_clone.create_test_tuple(i as i32, i as i32 * 100);
                let meta = TupleMeta::new(txn.get_transaction_id(), false);
                let _ = ctx_clone.table_heap.insert_tuple(&meta, &mut tuple);

                thread::sleep(Duration::from_millis(10));
                ctx_clone.transaction_manager.write().commit(txn)
            });
            handles.push(handle);
        }

        for handle in handles {
            assert!(handle.join().unwrap());
        }
    }

    #[test]
    fn test_garbage_collection() {
        let ctx = TestContext::new("test_garbage_collection");

        // Create initial tuple
        let txn1 = ctx.transaction_manager.write()
            .begin(IsolationLevel::ReadCommitted);
        let mut tuple = ctx.create_test_tuple(1, 100);
        let meta = TupleMeta::new(txn1.get_transaction_id(), false);
        let rid = ctx.table_heap.insert_tuple(&meta, &mut tuple).unwrap();
        ctx.transaction_manager.write().commit(txn1);

        // Create several versions
        for i in 1..=5 {
            let txn = ctx.transaction_manager.write()
                .begin(IsolationLevel::ReadCommitted);
            let mut new_tuple = ctx.create_test_tuple(1, 100 + i);
            let meta = TupleMeta::new(txn.get_transaction_id(), false);
            update_tuple_and_undo_link(
                &ctx.transaction_manager.read(),
                rid,
                None,
                &ctx.table_heap,
                &txn,
                &meta,
                &mut new_tuple,
                None,
            );
            ctx.transaction_manager.write().commit(txn);
        }

        // Perform garbage collection
        ctx.transaction_manager.read().garbage_collection();

        // Verify that old versions are cleaned up
        let version_count = ctx.transaction_manager.read()
            .version_info.read()
            .get(&rid.get_page_id())
            .map(|info| info.prev_link.read().len())
            .unwrap_or(0);

        assert!(version_count < 5); // Some versions should be cleaned up
    }

    #[test]
    fn test_undo_link_operations() {
        let ctx = TestContext::new("test_undo_link_operations");

        // Create initial tuple
        let txn = ctx.transaction_manager.write()
            .begin(IsolationLevel::ReadCommitted);
        let mut tuple = ctx.create_test_tuple(1, 100);
        let meta = TupleMeta::new(txn.get_transaction_id(), false);
        let rid = ctx.table_heap.insert_tuple(&meta, &mut tuple).unwrap();

        // Create an undo link
        let undo_link = UndoLink {
            prev_txn: txn.get_transaction_id(),
            prev_log_idx: 0,
        };

        // Update undo link
        assert!(ctx.transaction_manager.read().update_undo_link(
            rid,
            Some(undo_link),
            None
        ));

        // Verify undo link
        let retrieved_link = ctx.transaction_manager.read().get_undo_link(rid);
        assert!(retrieved_link.is_some());
        assert_eq!(retrieved_link.unwrap().prev_txn, txn.get_transaction_id());

        ctx.transaction_manager.write().commit(txn);
    }
}
