use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::common::config::{PageId, INVALID_PAGE_ID};
use crate::common::logger::initialize_logger;
use crate::common::rid::RID;
use crate::concurrency::lock_manager::LockMode;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::storage::table::table_heap::{TableHeap, TableInfo};
use crate::storage::table::tuple::{Tuple, TupleMeta};
use log::{debug, error};
use std::sync::Arc;
use crate::concurrency::watermark::Watermark;
use crate::storage::page::page::PageType;

/// An iterator over the tuples in a table.
#[derive(Debug)]
pub struct TableIterator {
    table_heap: Arc<TableHeap>,
    rid: RID,
    stop_at_rid: RID,
    txn_ctx: Option<Arc<TransactionContext>>,
}

pub struct TableScanIterator {
    /// The underlying table iterator
    inner: TableIterator,
    /// Reference to table info
    table_info: Arc<TableInfo>,
}

impl TableIterator {
    pub fn new(
        table_heap: Arc<TableHeap>,
        rid: RID,
        stop_at_rid: RID,
        txn_ctx: Option<Arc<TransactionContext>>,
    ) -> Self {
        initialize_logger();
        let mut iterator = Self {
            table_heap,
            rid,
            stop_at_rid,
            txn_ctx,
        };
        iterator.initialize();
        iterator
    }

    pub fn get_rid(&self) -> RID {
        self.rid
    }

    pub fn is_end(&self) -> bool {
        // Check if we've hit an invalid page
        if self.rid.get_page_id() == INVALID_PAGE_ID {
            return true;
        }

        // Check if we've hit the stop point
        if self.stop_at_rid.get_page_id() != INVALID_PAGE_ID {
            if self.rid.get_page_id() > self.stop_at_rid.get_page_id() {
                return true;
            }
            if self.rid.get_page_id() == self.stop_at_rid.get_page_id()
                && self.rid.get_slot_num() > self.stop_at_rid.get_slot_num()
            {
                return true;
            }
        }

        false
    }

    fn initialize(&mut self) {
        debug!("Initializing iterator with starting RID: {:?}, stop_at_rid: {:?}",
               self.rid, self.stop_at_rid);

        // Acquire table lock if needed
        if !self.acquire_table_lock() {
            debug!("Failed to acquire table lock");
            self.rid = RID::new(INVALID_PAGE_ID, 0);
            return;
        }

        // Get initial page info
        let _guard = self.table_heap.latch.read(); // Hold read latch during initialization

        let first_page_id = self.table_heap.get_first_page_id();
        debug!("First page ID from table heap: {}", first_page_id);

        if first_page_id == INVALID_PAGE_ID {
            debug!("No valid pages exist in table heap");
            self.rid = RID::new(INVALID_PAGE_ID, 0);
            return;
        }

        // If starting RID is invalid or points to non-existent page, start from first page
        if self.rid.get_page_id() == INVALID_PAGE_ID ||
            self.check_page_validity(&self.table_heap.get_bpm(), self.rid.get_page_id()).is_none() {
            debug!("Starting from first page");
            self.rid = RID::new(first_page_id, 0);
            return;
        }

        // Validate slot number for starting page
        if let Some(num_tuples) = self.check_page_validity(&self.table_heap.get_bpm(), self.rid.get_page_id()) {
            if self.rid.get_slot_num() >= num_tuples as u32 {
                debug!("Invalid slot number, resetting to 0");
                self.rid = RID::new(self.rid.get_page_id(), 0);
            }
        }
    }

    /// Advances the iterator to the next position.
    fn advance(&mut self) {
        if self.rid.get_page_id() == INVALID_PAGE_ID {
            return;
        }

        let _guard = self.table_heap.latch.read();
        let bpm = self.table_heap.get_bpm();

        if let Some(page_guard) = bpm.fetch_page_guarded(self.rid.get_page_id()) {
            if let Some(page_type) = page_guard.into_specific_type() {
                match page_type {
                    PageType::Table(table_page) => {
                        let num_tuples = table_page.get_num_tuples();
                        let next_page_id = table_page.get_next_page_id();
                        let next_slot = self.rid.get_slot_num() + 1;

                        if next_slot >= num_tuples as u32 {
                            if next_page_id != INVALID_PAGE_ID {
                                self.rid = RID::new(next_page_id, 0);
                            } else {
                                self.rid = RID::new(INVALID_PAGE_ID, 0);
                            }
                        } else {
                            self.rid = RID::new(self.rid.get_page_id(), next_slot);
                        }
                        return;
                    }
                    _ => {
                        debug!("Wrong page type encountered during iteration");
                        self.rid = RID::new(INVALID_PAGE_ID, 0);
                        return;
                    }
                }
            }
        }

        // If we get here, something went wrong
        self.rid = RID::new(INVALID_PAGE_ID, 0);
    }

    fn acquire_table_lock(&self) -> bool {
        if let Some(txn_ctx) = &self.txn_ctx {
            let txn = txn_ctx.get_transaction();
            let lock_manager = txn_ctx.get_lock_manager();

            // Use table_heap's latch to protect the table_oid access
            let _latch_guard = self.table_heap.latch.read();

            if let Err(e) = lock_manager.lock_table(
                txn.clone(),
                LockMode::IntentionShared,
                self.table_heap.get_table_oid(),
            ) {
                error!("Failed to acquire table lock: {}", e);
                return false;
            }
        }
        true
    }

    fn check_page_validity(&self, bpm: &Arc<BufferPoolManager>, page_id: PageId) -> Option<u16> {
        debug!("Checking validity of page {}", page_id);
        match bpm.fetch_page_guarded(page_id) {
            Some(page_guard) => {
                debug!("Successfully fetched page {}", page_id);
                if let Some(page_type) = page_guard.into_specific_type() {
                    match page_type {
                        PageType::Table(table_page) => {
                            debug!("Successfully converted to table page {}", page_id);
                            let num_tuples = table_page.get_num_tuples();
                            debug!("Page {} has {} tuples", page_id, num_tuples);
                            Some(num_tuples)
                        }
                        _ => {
                            debug!("Wrong page type for page {}", page_id);
                            None
                        }
                    }
                } else {
                    debug!("Failed to convert page {} to table page", page_id);
                    None
                }
            }
            None => {
                debug!("Failed to fetch page {}", page_id);
                None
            }
        }
    }
}

impl TableScanIterator {
    pub fn new(table_info: Arc<TableInfo>) -> Self {
        let table_heap = table_info.get_table_heap();
        let inner = TableIterator::new(
            table_heap,
            RID::new(0, 0),
            RID::new(INVALID_PAGE_ID, 0),
            None,
        );

        Self { inner, table_info }
    }

    /// Check if scan has reached the end
    pub fn is_end(&self) -> bool {
        self.inner.is_end()
    }

    /// Get current RID
    pub fn get_rid(&self) -> RID {
        self.inner.get_rid()
    }

    /// Reset the iterator to start of table
    pub fn reset(&mut self) {
        let table_heap = self.table_info.get_table_heap();
        self.inner = TableIterator::new(
            table_heap,
            RID::new(0, 0),
            RID::new(INVALID_PAGE_ID, 0),
            None,
        );
    }
}

impl Iterator for TableIterator {
    type Item = (TupleMeta, Tuple);

    fn next(&mut self) -> Option<Self::Item> {
        while !self.is_end() {
            debug!("Attempting to get tuple with RID: {:?}", self.rid);

            // Get the tuple with read latch
            let result = {
                let _guard = self.table_heap.latch.read();
                match self.table_heap.get_tuple(self.rid, self.txn_ctx.clone()) {
                    Ok(tuple) => {
                        // Check if tuple is visible to transaction
                        if let Some(txn_ctx) = &self.txn_ctx {
                            let txn = txn_ctx.get_transaction();
                            // Create a new watermark
                            let watermark = Watermark::new();
                            let is_visible = tuple.0.is_visible_to(txn.get_transaction_id(), &watermark);
                            debug!(
                                "Tuple visibility check - RID: {:?}, TxnID: {}, Visible: {}, Meta: {:?}",
                                self.rid,
                                txn.get_transaction_id(),
                                is_visible,
                                tuple.0
                            );
                            if is_visible {
                                Some(tuple)
                            } else {
                                None
                            }
                        } else {
                            Some(tuple)
                        }
                    }
                    Err(e) => {
                        debug!("Failed to retrieve tuple: {:?}", e);
                        None
                    }
                }
            };

            // Advance the iterator
            self.advance();

            // Return the result if we got one
            if result.is_some() {
                return result;
            }
        }
        None
    }
}

impl Iterator for TableScanIterator {
    type Item = (TupleMeta, Tuple);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction, TransactionState};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanType};
    use crate::sql::execution::plans::table_scan_plan::TableScanNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use parking_lot::RwLock;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::TempDir;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_context: Arc<TransactionContext>,
        transaction_manager: Arc<TransactionManager>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        fn new(name: &str) -> Self {
            // Initialize logging first
            initialize_logger();

            let buffer_pool_size: usize = 5;
            const K: usize = 2;
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

            debug!("Creating test context with db_path: {}, log_path: {}", db_path, log_path);

            // Create disk components
            let disk_manager = Arc::new(FileDiskManager::new(db_path, log_path, 10));
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));

            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(buffer_pool_size, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                buffer_pool_size,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());

            // Create transaction with READ_COMMITTED isolation level
            let txn = Arc::new(Transaction::new(0, IsolationLevel::ReadCommitted));
            debug!("Created new transaction with ID: {}", txn.get_transaction_id());

            let transaction_context = Arc::new(TransactionContext::new(
                txn,
                lock_manager,
                transaction_manager.clone(),
            ));

            Self {
                bpm,
                transaction_context,
                transaction_manager,
                _temp_dir: temp_dir,
            }
        }

        fn get_transaction_context(&self) -> &Arc<TransactionContext> {
            &self.transaction_context
        }
    }

    fn setup_test_table(test_name: &str) -> Arc<TableHeap> {
        debug!("Setting up test table: {}", test_name);
        let ctx = TestContext::new(test_name);
        let bpm = ctx.bpm.clone();
        let table_heap = Arc::new(TableHeap::new(
            bpm,
            0,
            ctx.transaction_manager.clone(),
        ));
        debug!("Created table heap for test: {}", test_name);
        table_heap
    }

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ])
    }

    fn create_test_table_info(
        name: &str,
        schema: Schema,
        table_heap: Arc<TableHeap>,
    ) -> TableInfo {
        TableInfo::new(
            schema,
            name.to_string(),
            table_heap,
            1, // table_oid
        )
    }

    #[test]
    fn test_table_iterator_create() {
        let table_heap = setup_test_table("test_table_iterator_create");
        let rid = RID::new(INVALID_PAGE_ID, 0);

        let iterator = TableIterator::new(table_heap, rid, rid, None);
        assert_eq!(iterator.get_rid(), rid);
    }

    #[test]
    fn test_table_iterator_empty() {
        let table_heap = setup_test_table("test_table_iterator_empty");
        let rid = RID::new(0, 0);

        let mut iterator = TableIterator::new(table_heap, rid, rid, None);
        assert!(iterator.is_end());
        assert_eq!(
            None,
            iterator.next(),
            "Testing TableIterator returns none on empty table"
        );
    }

    #[test]
    fn test_table_iterator_single_tuple() {
        let ctx = TestContext::new("test_table_iterator_single_tuple");

        // Create transaction with READ_COMMITTED isolation level instead of READ_UNCOMMITTED
        let txn = Arc::new(Transaction::new(0, IsolationLevel::ReadCommitted));
        txn.set_state(TransactionState::Growing);

        let lock_manager = Arc::new(LockManager::new());
        let transaction_context = Arc::new(TransactionContext::new(
            txn,
            lock_manager,
            ctx.transaction_manager.clone(),
        ));

        let table_heap = setup_test_table("test_table_iterator_single_tuple");
        let table_heap_guard = table_heap.latch.write();

        let schema = Schema::new(vec![
            Column::new("col_1", TypeId::Integer),
            Column::new("col_2", TypeId::Integer),
            Column::new("col_3", TypeId::Integer),
        ]);
        let rid = RID::new(0, 0);
        let mut tuple = Tuple::new(
            &*vec![Value::from(1), Value::from(2), Value::from(3)],
            schema.clone(),
            rid,
        );
        let meta = TupleMeta::new(0); // Use same txn_id as the transaction

        // Insert tuple with transaction context
        table_heap
            .insert_tuple(&meta, &mut tuple, Some(transaction_context.clone()))
            .expect("failed to insert tuple");

        drop(table_heap_guard); // Release write lock before creating iterator

        let mut iterator = TableIterator::new(
            table_heap,
            rid,
            RID::new(INVALID_PAGE_ID, 0),
            Some(transaction_context.clone()),
        );

        assert!(
            !iterator.is_end(),
            "Iterator should not be at end with valid tuple"
        );

        let result = iterator.next();
        assert!(
            result.is_some(),
            "Iterator should return the inserted tuple"
        );

        if let Some((result_meta, result_tuple)) = result {
            assert_eq!(result_meta, meta, "Tuple metadata should match");
            assert_eq!(result_tuple, tuple, "Tuple data should match");
        }

        assert!(
            iterator.is_end(),
            "Iterator should be at end after reading tuple"
        );
        assert_eq!(iterator.next(), None, "Iterator should return None at end");
        debug!("Completed test_table_iterator_single_tuple");
    }

    #[test]
    fn test_table_iterator_multiple_tuples() {
        let ctx = TestContext::new("test_table_iterator_multiple_tuples");

        // Create transaction with READ_COMMITTED isolation level
        let txn = Arc::new(Transaction::new(0, IsolationLevel::ReadCommitted));
        txn.set_state(TransactionState::Growing);

        let lock_manager = Arc::new(LockManager::new());
        let transaction_context = Arc::new(TransactionContext::new(
            txn.clone(),
            lock_manager,
            ctx.transaction_manager.clone(),
        ));

        let table_heap = setup_test_table("test_table_iterator_multiple_tuples");
        let _table_heap_guard = table_heap.latch.write();

        let schema = Schema::new(vec![
            Column::new("col_1", TypeId::Integer),
            Column::new("col_2", TypeId::VarChar),
            Column::new("col_3", TypeId::Integer),
        ]);

        // Insert multiple tuples
        let mut inserted_rids = Vec::new();
        let mut metas = Vec::new();
        for i in 0..5 {
            let tuple_values = vec![
                Value::new(i as i32),
                Value::new(format!("Name{}", i)),
                Value::new(20 + i as i32),
            ];
            let mut tuple = Tuple::new(&tuple_values, schema.clone(), RID::new(0, i as u32));
            let mut meta = TupleMeta::new(0); // Use transaction's ID

            // Set commit timestamp before inserting
            let commit_ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64;
            meta.set_commit_timestamp(commit_ts);

            let rid = table_heap
                .insert_tuple(&meta, &mut tuple, Some(transaction_context.clone()))
                .expect("Failed to insert tuple");

            // Verify tuple was inserted correctly
            let (stored_meta, stored_tuple) = table_heap
                .get_tuple(rid, Some(transaction_context.clone()))
                .expect("Failed to get inserted tuple");
            debug!(
                "Inserted tuple - RID: {:?}, Meta: {:?}, Tuple: {:?}",
                rid, stored_meta, stored_tuple
            );

            inserted_rids.push(rid);
            metas.push((rid, meta));
        }

        // Verify table state before iteration
        debug!("Table state before iteration:");
        for (rid, _meta) in &metas {
            if let Ok((stored_meta, stored_tuple)) =
                table_heap.get_tuple(*rid, Some(transaction_context.clone()))
            {
                debug!(
                    "Stored tuple - RID: {:?}, Meta: {:?}, Tuple: {:?}",
                    rid, stored_meta, stored_tuple
                );
            }
        }

        // Keep transaction in Growing state for reading
        let iterator = table_heap.make_iterator(Some(transaction_context.clone()));

        // Debug initial iterator state
        debug!(
            "Iterator initial state - RID: {:?}, Stop RID: {:?}, Is End: {}",
            iterator.get_rid(),
            iterator.stop_at_rid,
            iterator.is_end()
        );

        // Try getting first tuple directly
        if let Ok((meta, tuple)) =
            table_heap.get_tuple(RID::new(0, 0), Some(transaction_context.clone()))
        {
            debug!(
                "Direct tuple fetch - RID: {:?}, Meta: {:?}, Tuple: {:?}",
                RID::new(0, 0),
                meta,
                tuple
            );
        }

        let tuples = iterator.collect::<Vec<(TupleMeta, Tuple)>>();

        debug!("Collected {} tuples", tuples.len());
        for (i, (meta, tuple)) in tuples.iter().enumerate() {
            debug!(
                "Retrieved tuple {}: Meta: {:?}, Tuple: {:?}",
                i, meta, tuple
            );
        }

        assert_eq!(
            tuples.len(),
            5,
            "Expected 5 tuples, but got {}",
            tuples.len()
        );

        // Verify tuples and metadata
        for (i, ((_rid, meta), (retrieved_meta, retrieved_tuple))) in
            metas.iter().zip(tuples.iter()).enumerate()
        {
            assert_eq!(retrieved_meta, meta, "Tuple {} metadata should match", i);
            assert_eq!(retrieved_tuple.get_value(0), &Value::new(i as i32));
            assert_eq!(
                retrieved_tuple.get_value(1),
                &Value::new(format!("Name{}", i))
            );
            assert_eq!(retrieved_tuple.get_value(2), &Value::new(20 + i as i32));
        }

        // Now we can commit the transaction
        txn.set_state(TransactionState::Committed);
        debug!("Completed test_table_iterator_multiple_tuples");
    }

    #[test]
    fn test_table_scan_creation() {
        let schema = create_test_schema();
        let table_heap = setup_test_table("test_table_scan_creation");
        let table_info = create_test_table_info("users", schema.clone(), table_heap);

        let scan = TableScanNode::new(table_info, Arc::from(schema), Some("u".to_string()));

        assert_eq!(scan.get_type(), PlanType::TableScan);
        assert_eq!(scan.get_table_name(), "users");
        assert_eq!(scan.get_table_alias(), Some("u"));
    }

    #[test]
    fn test_table_scan_iterator() {
        let schema = create_test_schema();
        let table_heap = setup_test_table("test_table_scan_iterator");
        let table_info = create_test_table_info("users", schema.clone(), table_heap);

        let scan = TableScanNode::new(table_info, Arc::from(schema), None);
        let mut iterator = scan.scan();

        // Test empty table
        assert!(iterator.next().is_none());

        // Reset and test again
        iterator.reset();
        assert!(iterator.next().is_none());
    }

    #[test]
    fn test_table_iterator_with_updates() {
        let ctx = TestContext::new("test_table_iterator_updates");
        let txn_ctx = ctx.get_transaction_context().clone();
        let table_heap = setup_test_table("test_table_iterator_updates");

        // Use table heap's internal latch instead of RwLock
        // let _guard = table_heap.latch.write();

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::VarChar),
        ]);

        // Insert initial tuple
        let mut tuple = Tuple::new(
            &[Value::new(1), Value::new("original")],
            schema.clone(),
            RID::new(0, 0),
        );
        let meta = TupleMeta::new(0);
        let rid = table_heap
            .insert_tuple(&meta, &mut tuple, Some(txn_ctx.clone()))
            .unwrap();

        // Update the tuple
        let mut new_tuple =
            Tuple::new(&[Value::new(1), Value::new("updated")], schema.clone(), rid);
        table_heap
            .update_tuple(&meta, &mut new_tuple, rid, Some(txn_ctx.clone()))
            .unwrap();

        // Iterate and verify we see the updated value
        let iterator = table_heap.make_iterator(Some(txn_ctx.clone()));
        let tuples: Vec<_> = iterator.collect();

        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0].1.get_value(1), &Value::new("updated"));
    }

    #[test]
    fn test_table_iterator_concurrent_modifications() {
        let ctx = TestContext::new("test_table_iterator_concurrent_mods");
        let txn_ctx1 = ctx.get_transaction_context().clone();
        let table_heap = setup_test_table("test_table_iterator_concurrent_mods");

        // Use table heap's internal latch
        let _guard = table_heap.latch.write();

        // Create second transaction
        let txn2 = Arc::new(Transaction::new(1, IsolationLevel::ReadCommitted));
        txn2.set_state(TransactionState::Growing);
        let txn_ctx2 = Arc::new(TransactionContext::new(
            txn2,
            txn_ctx1.get_lock_manager().clone(),
            ctx.transaction_manager.clone(),
        ));

        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);

        // Insert initial tuples with first transaction
        let mut rids = Vec::new();
        for i in 0..3 {
            let mut tuple = Tuple::new(&[Value::new(i)], schema.clone(), RID::new(0, i as u32));
            let meta = TupleMeta::new(0);
            let rid = table_heap
                .insert_tuple(&meta, &mut tuple, Some(txn_ctx1.clone()))
                .unwrap();
            rids.push(rid);
        }

        // Start iterator with first transaction
        let iterator = table_heap.make_iterator(Some(txn_ctx1.clone()));

        // Insert new tuple with second transaction
        let mut new_tuple = Tuple::new(&[Value::new(99)], schema.clone(), RID::new(0, 3));
        let meta = TupleMeta::new(1); // Different transaction ID
        table_heap
            .insert_tuple(&meta, &mut new_tuple, Some(txn_ctx2.clone()))
            .unwrap();

        // Iterator should only see original tuples
        let tuples: Vec<_> = iterator.collect();
        assert_eq!(tuples.len(), 3);
        for (i, (_, tuple)) in tuples.iter().enumerate() {
            assert_eq!(tuple.get_value(0), &Value::new(i as i32));
        }
    }

    #[test]
    fn test_table_iterator_with_deleted_tuples() {
        let ctx = TestContext::new("test_table_iterator_deleted_tuples");
        let txn_ctx = ctx.get_transaction_context().clone();
        let table_heap = setup_test_table("test_table_iterator_deleted_tuples");

        // Use table heap's internal latch
        // let _guard = table_heap.latch.write();

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::VarChar),
        ]);

        // Insert tuples
        let mut rids = Vec::new();
        for i in 0..3 {
            let mut tuple = Tuple::new(
                &[Value::new(i), Value::new(format!("value{}", i))],
                schema.clone(),
                RID::new(0, i as u32),
            );
            let meta = TupleMeta::new(0);
            let rid = table_heap
                .insert_tuple(&meta, &mut tuple, Some(txn_ctx.clone()))
                .unwrap();
            rids.push(rid);
        }

        let mut new_meta = TupleMeta::new(0);
        new_meta.set_deleted(true);
        // Delete middle tuple
        table_heap
            .update_tuple_meta(&new_meta, rids[1])
            .unwrap();

        // Create iterator
        let iterator = table_heap.make_iterator(Some(txn_ctx.clone()));
        let tuples: Vec<_> = iterator.collect();

        // Should only see 2 tuples (not the deleted one)
        assert_eq!(tuples.len(), 2);
        assert_eq!(tuples[0].1.get_value(0), &Value::new(0));
        assert_eq!(tuples[1].1.get_value(0), &Value::new(2));
    }

    #[test]
    fn test_table_iterator_with_multiple_pages() {
        let ctx = TestContext::new("test_table_iterator_multiple_pages");
        let txn_ctx = ctx.get_transaction_context().clone();
        let table_heap = setup_test_table("test_table_iterator_multiple_pages");
        let _table_heap_guard = table_heap.latch.write();

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            // Add a large varchar to help fill pages faster
            Column::new("data", TypeId::VarChar),
        ]);

        // Insert many tuples with large data to force multiple pages
        let large_string = "x".repeat(1000); // 1KB string
        let mut count = 0;
        let mut last_page_id = 0;
        let mut rids = Vec::new();

        // Insert until we have at least 3 pages
        while last_page_id < 2 {
            let mut tuple = Tuple::new(
                &[Value::new(count), Value::new(large_string.clone())],
                schema.clone(),
                RID::new(0, 0),
            );
            let meta = TupleMeta::new(0);
            let rid = table_heap
                .insert_tuple(&meta, &mut tuple, Some(txn_ctx.clone()))
                .unwrap();

            last_page_id = rid.get_page_id();
            rids.push(rid);
            count += 1;
        }

        // Iterate and verify
        let iterator = table_heap.make_iterator(Some(txn_ctx.clone()));
        let tuples: Vec<_> = iterator.collect();

        assert_eq!(tuples.len(), count as usize);

        // Verify tuples are in order
        for (i, (_, tuple)) in tuples.iter().enumerate() {
            assert_eq!(tuple.get_value(0), &Value::new(i as i32));
        }
    }

    #[test]
    fn test_table_iterator_with_partial_scan() {
        let ctx = TestContext::new("test_table_iterator_partial_scan");
        let txn_ctx = ctx.get_transaction_context().clone();
        let table_heap = setup_test_table("test_table_iterator_partial_scan");

        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);

        // Insert 5 tuples
        let mut rids = Vec::new();
        for i in 0..5 {
            let mut tuple = Tuple::new(&[Value::new(i)], schema.clone(), RID::new(0, i as u32));
            let meta = TupleMeta::new(0);
            let rid = table_heap
                .insert_tuple(&meta, &mut tuple, Some(txn_ctx.clone()))
                .unwrap();
            rids.push(rid);
        }

        // Create iterator that stops after 3 tuples
        let iterator = TableIterator::new(
            table_heap.clone(),
            rids[0],
            rids[2], // Stop after third tuple
            Some(txn_ctx.clone()),
        );

        let tuples: Vec<_> = iterator.collect();
        assert_eq!(tuples.len(), 3);

        // Verify we got exactly the first 3 tuples
        for i in 0..3 {
            assert_eq!(tuples[i].1.get_value(0), &Value::new(i as i32));
        }
    }

    #[test]
    fn test_table_iterator_empty_pages() {
        let ctx = TestContext::new("test_table_iterator_empty_pages");
        let txn_ctx = ctx.get_transaction_context().clone();
        let table_heap = setup_test_table("test_table_iterator_empty_pages");

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("data", TypeId::VarChar),
        ]);

        // Create several pages with gaps
        let large_string = "x".repeat(1000);
        let mut rids = Vec::new();

        // Insert tuple in first page
        let mut tuple1 = Tuple::new(
            &[Value::new(1), Value::new(large_string.clone())],
            schema.clone(),
            RID::new(0, 0),
        );
        let meta = TupleMeta::new(0);
        let rid1 = table_heap
            .insert_tuple(&meta, &mut tuple1, Some(txn_ctx.clone()))
            .unwrap();
        rids.push(rid1);

        // Force creation of second page and leave it empty
        let mut tuple2 = Tuple::new(
            &[Value::new(2), Value::new(large_string.clone())],
            schema.clone(),
            RID::new(1, 0),
        );
        let rid2 = table_heap
            .insert_tuple(&meta, &mut tuple2, Some(txn_ctx.clone()))
            .unwrap();

        // Mark second tuple as deleted to create gap
        let mut delete_meta = TupleMeta::new(0);
        delete_meta.set_deleted(true);
        table_heap
            .update_tuple_meta(&delete_meta, rid2)
            .unwrap();

        // Insert tuple in third page
        let mut tuple3 = Tuple::new(
            &[Value::new(3), Value::new(large_string.clone())],
            schema.clone(),
            RID::new(2, 0),
        );
        let rid3 = table_heap
            .insert_tuple(&meta, &mut tuple3, Some(txn_ctx.clone()))
            .unwrap();
        rids.push(rid3);

        // Iterator should skip empty middle page
        let iterator = table_heap.make_iterator(Some(txn_ctx.clone()));
        let tuples: Vec<_> = iterator.collect();

        assert_eq!(tuples.len(), 2);
        assert_eq!(tuples[0].1.get_value(0), &Value::new(1));
        assert_eq!(tuples[1].1.get_value(0), &Value::new(3));
    }

    #[test]
    fn test_table_iterator_invalid_start_position() {
        let ctx = TestContext::new("test_table_iterator_invalid_start");
        let txn_ctx = ctx.get_transaction_context().clone();
        let table_heap = setup_test_table("test_table_iterator_invalid_start");

        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);

        // Insert some valid tuples
        for i in 0..3 {
            let mut tuple = Tuple::new(&[Value::new(i)], schema.clone(), RID::new(0, i as u32));
            let meta = TupleMeta::new(0);
            table_heap
                .insert_tuple(&meta, &mut tuple, Some(txn_ctx.clone()))
                .unwrap();
        }

        // Create iterator with invalid starting RID
        let iterator = TableIterator::new(
            table_heap.clone(),
            RID::new(999, 0), // Non-existent page
            RID::new(INVALID_PAGE_ID, 0),
            Some(txn_ctx.clone()),
        );

        // Iterator should handle invalid start gracefully
        let tuples: Vec<_> = iterator.collect();
        assert_eq!(tuples.len(), 0);
    }

    #[test]
    fn test_table_iterator_tuple_count() {
        let ctx = TestContext::new("test_table_iterator_tuple_count");
        let txn_ctx = ctx.get_transaction_context().clone();
        let table_heap = setup_test_table("test_table_iterator_tuple_count");
        let table_heap_guard = table_heap.latch.write();

        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::VarChar),
        ]);

        // Insert a single tuple
        let mut tuple = Tuple::new(
            &[Value::new(1), Value::new("test")],
            schema.clone(),
            RID::new(0, 0),
        );
        let meta = TupleMeta::new(0);

        // Insert and verify the tuple
        let rid = table_heap
            .insert_tuple(&meta, &mut tuple, Some(txn_ctx.clone()))
            .unwrap();

        // Debug print the table state
        debug!("Table state after insertion:");
        debug!("RID: {:?}", rid);
        debug!("Table heap: {:?}", table_heap_guard);

        // Verify tuple count through different methods
        let count_through_iterator = {
            let iterator = table_heap.make_iterator(Some(txn_ctx.clone()));
            iterator.collect::<Vec<_>>().len()
        };

        let count_through_get = {
            let result = table_heap.get_tuple(rid, Some(txn_ctx.clone()));
            if result.is_ok() {
                1
            } else {
                0
            }
        };

        let direct_count = table_heap.get_num_tuples();

        debug!("Count through iterator: {}", count_through_iterator);
        debug!("Count through get: {}", count_through_get);
        debug!("Direct count: {}", direct_count);

        // All counting methods should agree
        assert_eq!(count_through_iterator, 1, "Iterator count should be 1");
        assert_eq!(count_through_get, 1, "Get tuple count should be 1");
        assert_eq!(direct_count, 1, "Direct count should be 1");

        // Verify the tuple content
        let (retrieved_meta, retrieved_tuple) = table_heap
            .get_tuple(rid, Some(txn_ctx.clone()))
            .expect("Failed to get tuple");

        assert_eq!(retrieved_meta, meta, "Tuple metadata should match");
        assert_eq!(retrieved_tuple, tuple, "Tuple data should match");
    }

    #[test]
    fn test_table_iterator_transaction_visibility() {
        let ctx = TestContext::new("test_table_iterator_transaction_visibility");
        let txn_ctx = ctx.get_transaction_context().clone();
        let table_heap = setup_test_table("test_table_iterator_transaction_visibility");

        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);

        // Insert a tuple with transaction
        let mut tuple = Tuple::new(&[Value::new(1)], schema.clone(), RID::new(0, 0));
        let mut meta = TupleMeta::new(txn_ctx.get_transaction().get_transaction_id());

        // Set commit timestamp
        let commit_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        meta.set_commit_timestamp(commit_ts);

        let rid = table_heap
            .insert_tuple(&meta, &mut tuple, Some(txn_ctx.clone()))
            .unwrap();

        // Update the tuple metadata to mark it as committed
        table_heap
            .update_tuple_meta(&meta, rid)
            .expect("Failed to update tuple metadata");

        // Verify visibility through iterator
        let iterator = table_heap.make_iterator(Some(txn_ctx.clone()));
        let tuples: Vec<_> = iterator.collect();

        assert_eq!(tuples.len(), 1, "Should see exactly one tuple");

        let (retrieved_meta, retrieved_tuple) = &tuples[0];
        assert_eq!(
            retrieved_meta.get_commit_timestamp(),
            meta.get_commit_timestamp()
        );
        assert_eq!(retrieved_tuple.get_value(0), &Value::new(1));

        // Debug visibility information
        debug!(
            "Tuple visibility - TxnID: {}, Commit TS: {:?}, Meta: {:?}",
            txn_ctx.get_transaction().get_transaction_id(),
            meta.get_commit_timestamp(),
            meta
        );
    }
}
