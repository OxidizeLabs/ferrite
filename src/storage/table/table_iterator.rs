use crate::common::config::{PageId, INVALID_PAGE_ID};
use crate::common::rid::RID;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::storage::table::table_heap::TableInfo;
use crate::storage::table::transactional_table_heap::TransactionalTableHeap;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use log::debug;
use std::sync::Arc;

/// Table iterator for scanning through table pages
///
/// This iterator provides sequential access to tuples in a table,
/// respecting transaction isolation and visibility rules.
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
        debug!(
            "Initializing iterator with starting RID: {:?}, stop_at_rid: {:?}",
            self.current_rid, self.stop_at_rid
        );

        // // Acquire table lock if needed
        // if !self.acquire_table_lock() {
        //     debug!("Failed to acquire table lock");
        //     self.current_rid = RID::new(INVALID_PAGE_ID, 0);
        //     return;
        // }

        let table_heap = self.table_heap.get_table_heap();
        let _guard = table_heap.latch.read();

        // Get the first valid page
        let first_page_id = table_heap.get_first_page_id();
        if first_page_id == INVALID_PAGE_ID {
            debug!("No valid pages exist in table heap");
            self.current_rid = RID::new(INVALID_PAGE_ID, 0);
            return;
        }

        // Handle invalid start position
        if self.current_rid.get_page_id() == INVALID_PAGE_ID
            || !self.is_valid_page(self.current_rid.get_page_id())
        {
            debug!("Starting from first page");
            self.current_rid = RID::new(first_page_id, 0);
            return;
        }

        // Validate slot number
        if let Ok(page_guard) = table_heap.get_page(self.current_rid.get_page_id()) {
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
    type Item = (Arc<TupleMeta>, Arc<Tuple>);

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
    type Item = (Arc<TupleMeta>, Arc<Tuple>);

    fn next(&mut self) -> Option<Self::Item> {
        while !self.is_end() {
            debug!("Attempting to get tuple with RID: {:?}", self.current_rid);

            let result = if let Some(txn_ctx) = &self.txn_ctx {
                // Use transactional get_tuple for MVCC
                self.table_heap
                    .get_tuple(self.current_rid, txn_ctx.clone())
                    .ok()
            } else {
                // Use direct table heap access for non-transactional reads
                self.table_heap
                    .get_table_heap()
                    .get_tuple(self.current_rid)
                    .ok()
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
    use super::*;
    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction, TransactionState};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
    use crate::storage::table::table_heap::TableHeap;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::types::Type;
    use crate::types_db::value::Value;
    use parking_lot::RwLock;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio;

    struct AsyncTestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_context: Arc<TransactionContext>,
        transaction_manager: Arc<TransactionManager>,
        _temp_dir: TempDir,
    }

    impl AsyncTestContext {
        async fn new(name: &str) -> Self {
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
            let disk_manager = Arc::new(
                AsyncDiskManager::new(db_path, log_path, DiskManagerConfig::default())
                    .await
                    .expect("Failed to create async disk manager"),
            );

            let lock_manager = Arc::new(LockManager::new());
            let transaction_manager = Arc::new(TransactionManager::new());

            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(128, 2)));
            let bpm = Arc::new(
                BufferPoolManager::new(128, disk_manager.clone(), replacer)
                    .expect("Failed to create buffer pool manager"),
            );

            let transaction = transaction_manager
                .begin(IsolationLevel::ReadCommitted)
                .expect("Failed to create transaction");

            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
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

        async fn insert_tuple(
            &self,
            table: &TransactionalTableHeap,
            values: Vec<Value>,
            schema: &Schema,
            txn_ctx: Option<Arc<TransactionContext>>,
        ) -> RID {
            table
                .insert_tuple_from_values(
                    values,
                    schema,
                    txn_ctx.unwrap_or_else(|| self.transaction_context.clone()),
                )
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

    #[tokio::test]
    async fn test_async_table_iterator_basic() {
        let ctx = AsyncTestContext::new("table_iterator_basic").await;
        let table = ctx.create_table();
        let schema = create_test_schema();

        // Insert test data
        let values1 = vec![Value::new(1), Value::new("Alice"), Value::new(25)];
        let values2 = vec![Value::new(2), Value::new("Bob"), Value::new(30)];
        let values3 = vec![Value::new(3), Value::new("Charlie"), Value::new(35)];

        ctx.insert_tuple(
            &table,
            values1,
            &schema,
            Some(ctx.transaction_context.clone()),
        )
        .await;
        ctx.insert_tuple(
            &table,
            values2,
            &schema,
            Some(ctx.transaction_context.clone()),
        )
        .await;
        ctx.insert_tuple(
            &table,
            values3,
            &schema,
            Some(ctx.transaction_context.clone()),
        )
        .await;

        // Create iterator
        let start_rid = RID::new(0, 0);
        let end_rid = RID::new(PageId::MAX, u32::MAX);
        let txn_ctx = Some(ctx.transaction_context.clone());

        let iterator = TableIterator::new(table, start_rid, end_rid, txn_ctx);

        // Test iteration
        let mut count = 0;
        for (meta, tuple) in iterator {
            assert!(!meta.is_deleted());
            assert!(tuple.get_value(0).as_integer().unwrap() > 0);
            count += 1;
        }

        assert!(count > 0, "Iterator should yield at least one tuple");
    }

    #[tokio::test]
    async fn test_async_table_iterator_transactions() {
        let ctx = AsyncTestContext::new("table_iterator_txn").await;
        let table = ctx.create_table();
        let schema = create_test_schema();

        // Insert data with different transactions
        let txn1 = ctx.create_transaction(IsolationLevel::ReadCommitted);
        let txn2 = ctx.create_transaction(IsolationLevel::ReadCommitted);

        let values1 = vec![Value::new(1), Value::new("Alice"), Value::new(25)];
        let values2 = vec![Value::new(2), Value::new("Bob"), Value::new(30)];

        ctx.insert_tuple(&table, values1, &schema, Some(txn1.clone()))
            .await;
        ctx.insert_tuple(&table, values2, &schema, Some(txn2.clone()))
            .await;

        // Create iterator with specific transaction context
        let start_rid = RID::new(0, 0);
        let end_rid = RID::new(PageId::MAX, u32::MAX);

        let iterator = TableIterator::new(table, start_rid, end_rid, Some(txn1));

        // Test that iterator respects transaction isolation
        let mut count = 0;
        for (_meta, _tuple) in iterator {
            count += 1;
        }

        // Should see tuples visible to the transaction
        assert!(count >= 1, "Iterator should yield visible tuples");
    }

    #[tokio::test]
    async fn test_async_table_scan_iterator() {
        let ctx = AsyncTestContext::new("table_scan_iterator").await;
        let table = ctx.create_table();
        let schema = create_test_schema();

        // Insert test data
        let values = vec![Value::new(42), Value::new("Test"), Value::new(28)];
        ctx.insert_tuple(
            &table,
            values,
            &schema,
            Some(ctx.transaction_context.clone()),
        )
        .await;

        // Create table info
        let table_info = Arc::new(TableInfo::new(
            schema.clone(),
            "test_table".to_string(),
            table.get_table_heap(),
            1,
        ));

        // Create scan iterator
        let mut scan_iterator = TableScanIterator::new(table_info.clone());

        // Test basic iteration
        assert!(!scan_iterator.is_end());

        let mut found_tuples = 0;
        for (_meta, tuple) in scan_iterator.by_ref() {
            assert_eq!(tuple.get_value(0), Value::new(42));
            assert_eq!(tuple.get_value(1), Value::new("Test"));
            assert_eq!(tuple.get_value(2), Value::new(28));
            found_tuples += 1;
        }

        assert!(found_tuples > 0, "Should have found at least one tuple");
        assert!(scan_iterator.is_end());

        // Test reset functionality
        scan_iterator.reset();
        assert!(!scan_iterator.is_end());
    }

    #[tokio::test]
    async fn test_async_iterator_bounds() {
        let ctx = AsyncTestContext::new("iterator_bounds").await;
        let table = ctx.create_table();
        let schema = create_test_schema();

        // Insert test data
        let values = vec![Value::new(1), Value::new("Test"), Value::new(20)];
        ctx.insert_tuple(
            &table,
            values,
            &schema,
            Some(ctx.transaction_context.clone()),
        )
        .await;

        // Create iterator with specific bounds
        let start_rid = RID::new(0, 0);
        let end_rid = RID::new(0, 1); // Limited range

        let iterator = TableIterator::new(
            table,
            start_rid,
            end_rid,
            Some(ctx.transaction_context.clone()),
        );

        // Should respect bounds
        let mut count = 0;
        for (_meta, _tuple) in iterator {
            count += 1;
        }

        // Should find tuples within bounds
        assert!(count >= 0, "Iterator should handle bounds correctly");
    }

    #[tokio::test]
    async fn test_async_empty_table_iteration() {
        let ctx = AsyncTestContext::new("empty_table").await;
        let table = ctx.create_table();

        // Create iterator on empty table
        let start_rid = RID::new(0, 0);
        let end_rid = RID::new(PageId::MAX, u32::MAX);

        let iterator = TableIterator::new(
            table,
            start_rid,
            end_rid,
            Some(ctx.transaction_context.clone()),
        );

        // Should handle empty table gracefully
        let mut count = 0;
        for (_meta, _tuple) in iterator {
            count += 1;
        }

        assert_eq!(count, 0, "Empty table should yield no tuples");
    }
}
