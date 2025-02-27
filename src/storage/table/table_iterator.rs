use crate::common::config::{PageId, INVALID_PAGE_ID};
use crate::common::rid::RID;
use crate::concurrency::lock_manager::LockMode;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::storage::table::table_heap::TableInfo;
use crate::storage::table::transactional_table_heap::TransactionalTableHeap;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use log::{debug, error};
use std::sync::Arc;

/// An iterator over the tuples in a table.
#[derive(Debug)]
pub struct TableIterator {
    /// The transactional table heap being iterated over
    table_heap: Arc<TransactionalTableHeap>,
    /// Current position in the table
    current_rid: RID,
    /// Stop position (if any)
    stop_at_rid: RID,
    /// Optional transaction context for visibility checks
    txn_ctx: Option<Arc<TransactionContext>>,
}

/// Iterator for scanning a table sequentially.
/// Provides a higher-level abstraction over TableIterator
/// with additional table metadata access.
pub struct TableScanIterator {
    /// The underlying table iterator
    inner: TableIterator,
    /// Reference to table info for metadata access
    table_info: Arc<TableInfo>,
    /// Track if we've reached the end
    is_end: bool,
}

impl TableIterator {
    pub fn new(
        table_heap: Arc<TransactionalTableHeap>,
        start_rid: RID,
        stop_rid: RID,
        txn_ctx: Option<Arc<TransactionContext>>,
    ) -> Self {
        let mut iterator = Self {
            table_heap,
            current_rid: start_rid,
            stop_at_rid: stop_rid,
            txn_ctx,
        };
        iterator.initialize();
        iterator
    }

    /// Get current RID
    pub fn get_rid(&self) -> RID {
        self.current_rid
    }

    /// Check if iterator has reached the end
    pub fn is_end(&self) -> bool {
        if self.current_rid.get_page_id() == INVALID_PAGE_ID {
            return true;
        }

        if self.stop_at_rid.get_page_id() != INVALID_PAGE_ID {
            if self.current_rid.get_page_id() > self.stop_at_rid.get_page_id() {
                return true;
            }
            if self.current_rid.get_page_id() == self.stop_at_rid.get_page_id()
                && self.current_rid.get_slot_num() > self.stop_at_rid.get_slot_num()
            {
                return true;
            }
        }

        false
    }

    /// Initialize iterator position
    fn initialize(&mut self) {
        debug!("Initializing iterator with starting RID: {:?}, stop_at_rid: {:?}",
               self.current_rid, self.stop_at_rid);

        // // Acquire table lock if needed
        // if !self.acquire_table_lock() {
        //     debug!("Failed to acquire table lock");
        //     self.current_rid = RID::new(INVALID_PAGE_ID, 0);
        //     return;
        // }

        let table_heap = self.table_heap.get_table_heap();
        let _guard = table_heap.latch.read();

        // Get first valid page
        let first_page_id = table_heap.get_first_page_id();
        if first_page_id == INVALID_PAGE_ID {
            debug!("No valid pages exist in table heap");
            self.current_rid = RID::new(INVALID_PAGE_ID, 0);
            return;
        }

        // Handle invalid start position
        if self.current_rid.get_page_id() == INVALID_PAGE_ID 
            || !self.is_valid_page(self.current_rid.get_page_id()) {
            debug!("Starting from first page");
            self.current_rid = RID::new(first_page_id, 0);
            return;
        }

        // Validate slot number
        if let Some(page_guard) = table_heap.get_page(self.current_rid.get_page_id()).ok() {
            let page = page_guard.read();
            if self.current_rid.get_slot_num() >= page.get_num_tuples() as u32 {
                debug!("Invalid slot number, resetting to 0");
                self.current_rid = RID::new(self.current_rid.get_page_id(), 0);
            }
        }
    }

    /// Advance to next valid tuple
    fn advance(&mut self) {
        if self.is_end() {
            return;
        }

        let table_heap = self.table_heap.get_table_heap();
        let _guard = table_heap.latch.read();

        if let Ok(page_guard) = table_heap.get_page(self.current_rid.get_page_id()) {
            let page = page_guard.read();
            let next_slot = self.current_rid.get_slot_num() + 1;

            if next_slot >= page.get_num_tuples() as u32 {
                // Move to next page
                let next_page_id = page.get_next_page_id();
                if next_page_id != INVALID_PAGE_ID {
                    self.current_rid = RID::new(next_page_id, 0);
                } else {
                    self.current_rid = RID::new(INVALID_PAGE_ID, 0);
                }
            } else {
                // Move to next slot
                self.current_rid = RID::new(self.current_rid.get_page_id(), next_slot);
            }
        } else {
            self.current_rid = RID::new(INVALID_PAGE_ID, 0);
        }
    }

    /// Check if a page exists and is valid
    fn is_valid_page(&self, page_id: PageId) -> bool {
        self.table_heap.get_table_heap().get_page(page_id).is_ok()
    }

    /// Acquire necessary table lock
    fn acquire_table_lock(&self) -> bool {
        if let Some(txn_ctx) = &self.txn_ctx {
            let txn = txn_ctx.get_transaction();
            let lock_manager = txn_ctx.get_lock_manager();
            
            // Get the table heap first
            let table_heap = self.table_heap.get_table_heap();
            // Then acquire the latch
            let _latch_guard = table_heap.latch.read();

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
}

impl TableScanIterator {
    pub fn new(table_info: Arc<TableInfo>) -> Self {
        let table_heap = table_info.get_table_heap();
        
        // Create a TransactionalTableHeap wrapper around the TableHeap
        let txn_table_heap = Arc::new(TransactionalTableHeap::new(
            table_heap.clone(),
            table_info.get_table_oidt(),
        ));
        
        // Start from first valid page
        let first_page_id = table_heap.get_first_page_id();
        let start_rid = RID::new(first_page_id, 0);
        
        // Create inner iterator that will scan to end
        let inner = TableIterator::new(
            txn_table_heap,
            start_rid,
            RID::new(INVALID_PAGE_ID, 0),
            None,
        );

        Self { 
            inner,
            table_info,
            is_end: false,
        }
    }

    /// Check if scan has reached the end
    pub fn is_end(&self) -> bool {
        self.is_end || self.inner.is_end()
    }

    /// Get current RID
    pub fn get_rid(&self) -> RID {
        self.inner.get_rid()
    }

    /// Reset the iterator to start of table
    pub fn reset(&mut self) {
        let table_heap = self.table_info.get_table_heap();
        let first_page_id = table_heap.get_first_page_id();
        
        // Create new TransactionalTableHeap wrapper
        let txn_table_heap = Arc::new(TransactionalTableHeap::new(
            table_heap.clone(),
            self.table_info.get_table_oidt(),
        ));
        
        self.inner = TableIterator::new(
            txn_table_heap,
            RID::new(first_page_id, 0),
            RID::new(INVALID_PAGE_ID, 0),
            None,
        );
        self.is_end = false;
    }

    /// Get reference to the table info
    pub fn get_table_info(&self) -> &Arc<TableInfo> {
        &self.table_info
    }
}

impl Iterator for TableScanIterator {
    type Item = (TupleMeta, Tuple);

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_end {
            return None;
        }

        let result = self.inner.next();
        
        // Update end status
        if result.is_none() {
            self.is_end = true;
        }

        result
    }
}

impl Iterator for TableIterator {
    type Item = (TupleMeta, Tuple);

    fn next(&mut self) -> Option<Self::Item> {
        while !self.is_end() {
            debug!("Attempting to get tuple with RID: {:?}", self.current_rid);

            let result = if let Some(txn_ctx) = &self.txn_ctx {
                // Use transactional get_tuple for MVCC
                self.table_heap.get_tuple(self.current_rid, txn_ctx.clone()).ok()
            } else {
                // Use direct table heap access for non-transactional reads
                self.table_heap.get_table_heap().get_tuple(self.current_rid).ok()
            };

            self.advance();

            if let Some(tuple) = result {
                return Some(tuple);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction, TransactionState};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use parking_lot::RwLock;
    use tempfile::TempDir;
    use crate::common::logger::initialize_logger;
    use crate::storage::table::table_heap::TableHeap;

    /// Helper struct for test setup and common operations
    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_context: Arc<TransactionContext>,
        transaction_manager: Arc<TransactionManager>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        fn new(name: &str) -> Self {
            initialize_logger();

            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join(format!("{name}.db")).to_str().unwrap().to_string();
            let log_path = temp_dir.path().join(format!("{name}.log")).to_str().unwrap().to_string();

            debug!("Creating test context with db_path: {}, log_path: {}", db_path, log_path);

            let disk_manager = Arc::new(FileDiskManager::new(db_path, log_path, 10));
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(5, 2)));
            let bpm = Arc::new(BufferPoolManager::new(
                5,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());
            let txn = Arc::new(Transaction::new(0, IsolationLevel::ReadCommitted));
            
            // Set transaction state to Running
            txn.set_state(TransactionState::Running);
            
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

        /// Creates a new table heap with transaction support
        fn create_table(&self) -> Arc<TransactionalTableHeap> {
            let table_heap = Arc::new(TableHeap::new(self.bpm.clone(), 0));
            Arc::new(TransactionalTableHeap::new(table_heap, 0))
        }

        /// Creates a new transaction context with specified isolation level
        fn create_transaction(&self, isolation_level: IsolationLevel) -> Arc<TransactionContext> {
            // Use a simple counter for test transaction IDs
            static NEXT_TXN_ID: AtomicU64 = AtomicU64::new(1);
            let txn_id = NEXT_TXN_ID.fetch_add(1, Ordering::SeqCst);
            
            let txn = Arc::new(Transaction::new(txn_id, isolation_level));
            // Set transaction state to Running
            txn.set_state(TransactionState::Running);
            
            Arc::new(TransactionContext::new(
                txn,
                Arc::new(LockManager::new()),
                self.transaction_manager.clone(),
            ))
        }

        /// Helper to insert a tuple and return its RID
        fn insert_tuple(
            &self,
            table: &TransactionalTableHeap,
            values: Vec<Value>,
            schema: &Schema,
            txn_ctx: Option<Arc<TransactionContext>>,
        ) -> RID {
            let mut tuple = Tuple::new(&values, schema.clone(), RID::new(0, 0));
            let meta = TupleMeta::new(txn_ctx.as_ref().map_or(0, |ctx| ctx.get_transaction().get_transaction_id()));
            
            table.insert_tuple(&meta, &mut tuple, txn_ctx.unwrap_or_else(|| self.transaction_context.clone()))
                .expect("Failed to insert tuple")
        }
    }

    /// Common test setup functions
    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ])
    }

    #[test]
    fn test_table_iterator_basic() {
        let ctx = TestContext::new("test_table_iterator_basic");
        let table = ctx.create_table();
        let schema = create_test_schema();

        // Test empty table
        let mut iterator = TableIterator::new(
            table.clone(),
            RID::new(0, 0),
            RID::new(INVALID_PAGE_ID, 0),
            None,
        );
        assert!(iterator.is_end());
        assert_eq!(iterator.next(), None);

        // Insert and test single tuple
        let rid = ctx.insert_tuple(
            &table,
            vec![Value::new(1), Value::new("test"), Value::new(25)],
            &schema,
            None,
        );

        let mut iterator = TableIterator::new(
            table.clone(),
            rid,
            RID::new(INVALID_PAGE_ID, 0),
            None,
        );
        
        assert!(!iterator.is_end());
        let result = iterator.next().expect("Should return tuple");
        assert_eq!(result.1.get_value(0), &Value::new(1));
        assert!(iterator.next().is_none());
    }

    #[test]
    fn test_table_iterator_transactions() {
        let ctx = TestContext::new("test_table_iterator_transactions");
        let table = ctx.create_table();
        let schema = create_test_schema();

        // Create two transactions
        let txn1_ctx = ctx.create_transaction(IsolationLevel::ReadCommitted);
        let txn2_ctx = ctx.create_transaction(IsolationLevel::ReadCommitted);

        // Insert a tuple using txn1
        let rid = ctx.insert_tuple(
            &table,
            vec![Value::new(1), Value::new("test"), Value::new(25)],
            &schema,
            Some(txn1_ctx.clone()),
        );

        // Second transaction shouldn't see uncommitted tuple
        let mut iterator = TableIterator::new(
            table.clone(),
            RID::new(0, 0),
            RID::new(INVALID_PAGE_ID, 0),
            Some(txn2_ctx.clone()),
        );
        assert_eq!(iterator.next(), None);

        // First transaction should see its own tuple
        let mut iterator = TableIterator::new(
            table.clone(),
            RID::new(0, 0),
            RID::new(INVALID_PAGE_ID, 0),
            Some(txn1_ctx.clone()),
        );
        assert!(iterator.next().is_some());
    }
}
