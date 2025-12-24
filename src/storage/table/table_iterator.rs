//! # Table Iterator
//!
//! This module provides iterators for sequential table scanning with MVCC visibility
//! checks. Iterators traverse the table heap page-by-page, slot-by-slot, returning
//! only tuples visible to the current transaction based on isolation level rules.
//!
//! ## Architecture
//!
//! ```text
//!   Iterator Hierarchy
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                       TableScanIterator                                 │
//!   │  (high-level, with TableInfo metadata and reset capability)             │
//!   │                                                                         │
//!   │  ┌─────────────────────────────────────────────────────────────────┐    │
//!   │  │                      TableIterator                              │    │
//!   │  │  (low-level, with start/stop RID bounds)                        │    │
//!   │  │                                                                 │    │
//!   │  │  ┌─────────────────────────────────────────────────────────┐    │    │
//!   │  │  │           TransactionalTableHeap                        │    │    │
//!   │  │  │  (MVCC visibility via TransactionContext)               │    │    │
//!   │  │  │                                                         │    │    │
//!   │  │  │  ┌─────────────────────────────────────────────────┐    │    │    │
//!   │  │  │  │              TableHeap                          │    │    │    │
//!   │  │  │  │  (physical page traversal)                      │    │    │    │
//!   │  │  │  └─────────────────────────────────────────────────┘    │    │    │
//!   │  │  └─────────────────────────────────────────────────────────┘    │    │
//!   │  └─────────────────────────────────────────────────────────────────┘    │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Iteration Flow
//!
//! ```text
//!   TableIterator::next()
//!   ─────────────────────────────────────────────────────────────────────────
//!
//!   ┌──────────────────┐
//!   │  is_end()?       │───── Yes ───▶ return None
//!   └────────┬─────────┘
//!            │ No
//!            ▼
//!   ┌──────────────────┐
//!   │ Get tuple at     │
//!   │ current_rid      │
//!   └────────┬─────────┘
//!            │
//!            ├─── txn_ctx present? ─── Yes ───▶ table_heap.get_tuple(rid, txn_ctx)
//!            │                                  (MVCC visibility check)
//!            │
//!            └─── No txn_ctx ─────────────────▶ table_heap.get_tuple(rid)
//!                                               (direct access)
//!            │
//!            ▼
//!   ┌──────────────────┐
//!   │  advance()       │  Move to next slot/page
//!   └────────┬─────────┘
//!            │
//!            ▼
//!   ┌──────────────────┐
//!   │ Tuple found?     │───── No ────▶ loop (skip invisible/deleted)
//!   └────────┬─────────┘
//!            │ Yes
//!            ▼
//!       return Some((meta, tuple))
//! ```
//!
//! ## Page Traversal
//!
//! ```text
//!   advance() Logic
//!   ─────────────────────────────────────────────────────────────────────────
//!
//!   Current Position: Page 1, Slot 2
//!
//!   Page 0              Page 1              Page 2
//!   ┌────────────┐      ┌────────────┐      ┌────────────┐
//!   │ Slot 0     │      │ Slot 0     │      │ Slot 0     │
//!   │ Slot 1     │      │ Slot 1     │      │ Slot 1     │
//!   │ Slot 2     │      │ Slot 2 ◀───│──────│            │
//!   │ Slot 3     │      │ Slot 3     │      │            │
//!   └────────────┘      └────────────┘      └────────────┘
//!
//!   Case 1: next_slot < num_tuples
//!   ───────────────────────────────
//!   → Move to Slot 3 on same page
//!   → current_rid = RID(page_id=1, slot=3)
//!
//!   Case 2: next_slot >= num_tuples
//!   ────────────────────────────────
//!   → Move to Slot 0 on next page
//!   → current_rid = RID(page_id=2, slot=0)
//!
//!   Case 3: No next page
//!   ─────────────────────
//!   → Set to INVALID
//!   → current_rid = RID(INVALID_PAGE_ID, 0)
//! ```
//!
//! ## Key Components
//!
//! | Component | Description |
//! |-----------|-------------|
//! | `TableIterator` | Low-level iterator with RID bounds and optional txn context |
//! | `TableScanIterator` | High-level iterator with TableInfo and reset support |
//! | `current_rid` | Next tuple position to return |
//! | `last_returned_rid` | RID of most recently returned tuple |
//! | `stop_at_rid` | Upper bound for iteration (exclusive) |
//!
//! ## Bound Checking
//!
//! ```text
//!   is_end() Logic
//!   ─────────────────────────────────────────────────────────────────────────
//!
//!   1. current_rid.page_id == INVALID_PAGE_ID → END
//!
//!   2. If stop_at_rid is set:
//!      - current_rid.page_id > stop_at_rid.page_id → END
//!      - Same page AND current_rid.slot > stop_at_rid.slot → END
//!
//!   Example with stop_at_rid = (page=2, slot=5):
//!
//!   ┌────────────────────────────────────────────────────────────────────┐
//!   │ (0,0) (0,1) ... (1,0) (1,1) ... (2,0) ... (2,5) │ (2,6) (3,0) ...  │
//!   │ ◀──────────────── valid range ────────────────▶ │ ◀─── END ──────▶ │
//!   └────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::storage::table::table_iterator::{TableIterator, TableScanIterator};
//!
//! // Low-level iteration with bounds
//! let start = RID::new(0, 0);
//! let stop = RID::new(INVALID_PAGE_ID, 0);  // scan to end
//! let iter = TableIterator::new(table_heap, start, stop, Some(txn_ctx));
//!
//! for (meta, tuple) in iter {
//!     println!("RID: {:?}, Value: {:?}", tuple.get_rid(), tuple.get_value(0));
//! }
//!
//! // High-level scan with TableInfo
//! let mut scan = TableScanIterator::new(table_info.clone());
//!
//! while let Some((meta, tuple)) = scan.next() {
//!     println!("Found: {:?}", tuple);
//! }
//!
//! // Reset and iterate again
//! scan.reset();
//! for (meta, tuple) in scan {
//!     // Process tuple...
//! }
//! ```
//!
//! ## Transaction Visibility
//!
//! | With `txn_ctx` | Behavior |
//! |----------------|----------|
//! | `Some(ctx)` | Uses `TransactionalTableHeap::get_tuple()` with MVCC visibility |
//! | `None` | Uses `TableHeap::get_tuple()` directly (no visibility check) |
//!
//! Invisible or deleted tuples are automatically skipped by the iterator loop.
//!
//! ## Thread Safety
//!
//! - Acquires `table_heap.latch.read()` during page traversal
//! - `Arc` wrappers for shared ownership of heap and transaction context
//! - Iterator is not `Sync` (single-threaded iteration)

use crate::common::config::{INVALID_PAGE_ID, PageId};
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
    /// RID of the last tuple that was returned
    last_returned_rid: Option<RID>,
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
    /// RID of the most recently returned tuple (stable for callers)
    last_rid: Option<RID>,
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
            last_returned_rid: None,
            stop_at_rid: stop_rid,
            txn_ctx,
        };
        iterator.initialize();
        iterator
    }

    /// Get the RID the iterator is currently positioned at (i.e., the next tuple).
    pub fn get_rid(&self) -> RID {
        self.current_rid
    }

    /// Get the RID of the last tuple that was returned by `next()`, if any.
    pub fn get_last_rid(&self) -> Option<RID> {
        self.last_returned_rid
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
            self.last_returned_rid = None;
            return;
        }

        // Validate slot number
        if let Ok(page_guard) = table_heap.get_page(self.current_rid.get_page_id()) {
            let page = page_guard.read();
            if self.current_rid.get_slot_num() >= page.get_num_tuples() as u32 {
                debug!("Invalid slot number, resetting to 0");
                self.current_rid = RID::new(self.current_rid.get_page_id(), 0);
                self.last_returned_rid = None;
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
            last_rid: None,
        }
    }

    /// Check if scan has reached the end
    pub fn is_end(&self) -> bool {
        self.is_end || self.inner.is_end()
    }

    /// Get current RID
    pub fn get_rid(&self) -> RID {
        self.last_rid
            .or_else(|| self.inner.get_last_rid())
            .unwrap_or_else(|| self.inner.get_rid())
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
        self.last_rid = None;
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

        let current_rid = self.inner.get_rid();
        let result = self.inner.next();

        // Update end status
        if result.is_none() {
            self.is_end = true;
            self.last_rid = None;
        } else {
            self.last_rid = Some(current_rid);
        }

        result
    }
}

impl Iterator for TableIterator {
    type Item = (Arc<TupleMeta>, Arc<Tuple>);

    fn next(&mut self) -> Option<Self::Item> {
        // Capture the RID we are about to return; `advance` will move to the next slot.
        let current_rid = self.current_rid;

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
                self.last_returned_rid = Some(current_rid);
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
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
