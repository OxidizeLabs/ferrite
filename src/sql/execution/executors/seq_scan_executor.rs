//! # Sequential Scan Executor
//!
//! This module implements the executor for full table scans, which reads all
//! tuples from a table by sequentially iterating through every page and slot.
//! Sequential scans are the fundamental data access method and serve as the
//! baseline for query execution.
//!
//! ## SQL Syntax
//!
//! Sequential scans are generated for queries that cannot use an index:
//!
//! ```sql
//! -- Full table scan (no WHERE clause)
//! SELECT * FROM employees;
//!
//! -- Scan with filter (when no suitable index exists)
//! SELECT * FROM employees WHERE salary > 50000;
//!
//! -- Scan for aggregation
//! SELECT COUNT(*) FROM employees;
//!
//! -- Scan as part of a join (inner relation without index)
//! SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id;
//! ```
//!
//! ## Execution Model
//!
//! The sequential scan follows the Volcano iterator model, returning one tuple
//! at a time. The scan proceeds through the table in physical storage order:
//!
//! ```text
//! Table Heap Pages
//! ┌─────────────────────────────────────────────────────────────┐
//! │ Page 0                  Page 1                  Page 2      │
//! │ ┌─────┬─────┬─────┐    ┌─────┬─────┬─────┐    ┌─────┬─────┐│
//! │ │ T0  │ T1  │ T2  │───▶│ T3  │ T4  │ T5  │───▶│ T6  │ T7  ││
//! │ └─────┴─────┴─────┘    └─────┴─────┴─────┘    └─────┴─────┘│
//! └─────────────────────────────────────────────────────────────┘
//!      │     │     │          │     │     │          │     │
//!      ▼     ▼     ▼          ▼     ▼     ▼          ▼     ▼
//!   next() returns tuples in slot order across pages
//! ```
//!
//! ## MVCC and Visibility
//!
//! The executor uses `TransactionalTableHeap` for MVCC-aware tuple access:
//!
//! - **Deleted tuples**: Automatically skipped (soft-delete via `TupleMeta`)
//! - **Transaction visibility**: Tuples respect transaction isolation levels
//! - **Concurrent access**: Read locks allow multiple concurrent scans
//!
//! ## Performance Characteristics
//!
//! | Aspect | Complexity | Notes |
//! |--------|------------|-------|
//! | Time | O(n) | Reads every tuple in the table |
//! | I/O | O(pages) | Accesses every page sequentially |
//! | Memory | O(1) | Only holds current tuple |
//! | Buffer Pool | Sequential | Benefits from prefetching |
//!
//! ## When Sequential Scan is Used
//!
//! The query optimizer chooses sequential scan when:
//!
//! 1. **No index exists** on the filtered columns
//! 2. **High selectivity**: Most rows match the predicate (index overhead not worth it)
//! 3. **Small tables**: Full scan is cheaper than index lookup
//! 4. **Aggregations**: Need to read all rows anyway (COUNT(*), SUM, etc.)
//! 5. **DISTINCT/GROUP BY**: No suitable index for grouping
//!
//! ## Comparison with Index Scan
//!
//! | Aspect | Sequential Scan | Index Scan |
//! |--------|-----------------|------------|
//! | Access pattern | Linear through heap | Random via index |
//! | Best for | Full table access | Selective queries |
//! | I/O pattern | Sequential (prefetch-friendly) | Random |
//! | Setup cost | None | Index traversal |
//! | Works without index | Yes | No |
//!
//! ## Implementation Notes
//!
//! - The table is looked up by OID (not name) for correctness
//! - `TableIterator` handles page boundary crossing transparently
//! - Deleted tuples are filtered out automatically
//! - The iterator is created lazily during `init()`

use std::sync::Arc;

use log::{debug, error, trace};
use parking_lot::RwLock;

use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::seq_scan_plan::SeqScanPlanNode;
use crate::storage::table::table_iterator::TableIterator;
use crate::storage::table::transactional_table_heap::TransactionalTableHeap;
use crate::storage::table::tuple::Tuple;

/// Executor for full table sequential scans.
///
/// `SeqScanExecutor` reads all tuples from a table by iterating through every
/// page in the table heap. It is the simplest and most fundamental data access
/// method, used when no index is available or when a full table scan is more
/// efficient than index access.
///
/// # Execution Flow
///
/// ```text
/// ┌──────────────────────────────────────────────────────────────┐
/// │                    SeqScanExecutor                           │
/// ├──────────────────────────────────────────────────────────────┤
/// │  new()   ─────▶  Lookup table by OID in catalog              │
/// │                  Create TransactionalTableHeap               │
/// │                                                              │
/// │  init()  ─────▶  Create TableIterator from first page        │
/// │                  Set start RID to (first_page, 0)            │
/// │                  Set end RID to (MAX, MAX) for full scan     │
/// │                                                              │
/// │  next()  ─────▶  Iterate through table                       │
/// │                  Skip deleted tuples                         │
/// │                  Return (Tuple, RID) or None at end          │
/// └──────────────────────────────────────────────────────────────┘
/// ```
///
/// # MVCC Considerations
///
/// The executor uses `TransactionalTableHeap` which provides:
/// - Visibility checks based on transaction isolation level
/// - Automatic filtering of uncommitted changes from other transactions
/// - Soft-delete awareness (deleted tuples have `is_deleted` flag set)
///
/// # Example Usage
///
/// ```ignore
/// // Create a sequential scan plan for the 'employees' table
/// let plan = Arc::new(SeqScanPlanNode::new(
///     schema.clone(),
///     table_oid,
///     "employees".to_string(),
/// ));
///
/// // Create and initialize the executor
/// let mut executor = SeqScanExecutor::new(context.clone(), plan);
/// executor.init();
///
/// // Iterate through all tuples
/// while let Ok(Some((tuple, rid))) = executor.next() {
///     println!("Found tuple at {:?}: {:?}", rid, tuple);
/// }
/// ```
///
/// # Fields
///
/// - `context`: Shared execution context with catalog and transaction access
/// - `plan`: The scan plan containing table OID, name, and output schema
/// - `table_heap`: Transactional wrapper around the table's storage
/// - `initialized`: Flag to prevent double initialization
/// - `iterator`: The table iterator that tracks current position
pub struct SeqScanExecutor {
    /// Shared execution context providing access to catalog, buffer pool,
    /// and transaction state.
    context: Arc<RwLock<ExecutionContext>>,

    /// The sequential scan plan node containing table metadata.
    plan: Arc<SeqScanPlanNode>,

    /// Transactional table heap providing MVCC-aware tuple access.
    table_heap: Arc<TransactionalTableHeap>,

    /// Flag indicating whether `init()` has been called.
    initialized: bool,

    /// Iterator over the table's tuples, created during initialization.
    iterator: Option<TableIterator>,
}

impl SeqScanExecutor {
    /// Creates a new `SeqScanExecutor` for the specified table.
    ///
    /// This constructor looks up the table in the catalog by OID (not name) to
    /// ensure correctness even if the table is renamed. It creates a
    /// `TransactionalTableHeap` wrapper for MVCC-aware access.
    ///
    /// # Arguments
    ///
    /// * `context` - Shared execution context containing catalog and transaction state
    /// * `plan` - The scan plan node with table OID, name, and output schema
    ///
    /// # Returns
    ///
    /// A new executor instance ready for initialization.
    ///
    /// # Panics
    ///
    /// Panics if the table with the specified OID is not found in the catalog.
    /// This indicates a bug in query planning or catalog corruption.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let plan = Arc::new(SeqScanPlanNode::new(
    ///     schema.clone(),
    ///     table_info.get_table_oid(),
    ///     "employees".to_string(),
    /// ));
    ///
    /// let executor = SeqScanExecutor::new(context.clone(), plan);
    /// // executor is ready for init() and next() calls
    /// ```
    ///
    /// # Note
    ///
    /// The table heap is fetched during construction, but the iterator is not
    /// created until `init()` is called. This allows the executor to be
    /// created without immediately starting the scan.
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: Arc<SeqScanPlanNode>) -> Self {
        let table_name = plan.get_table_name();
        trace!("Creating SeqScanExecutor for table '{}'", table_name);

        // Get the table heap using table OID
        let table_heap = {
            let context_guard = context.read();
            let catalog = context_guard.get_catalog();
            let catalog_guard = catalog.read();

            // Use table OID to get the table info
            let table_oid = plan.get_table_oid();
            debug!("Looking up table with OID: {}", table_oid);

            match catalog_guard.get_table_by_oid(table_oid) {
                Some(table_info) => {
                    trace!("Found table with OID {} in catalog", table_oid);
                    // Create TransactionalTableHeap with table_oid
                    Arc::new(TransactionalTableHeap::new(
                        table_info.get_table_heap(),
                        table_oid, // Pass table_oid instead of txn_ctx
                    ))
                },
                None => {
                    error!("Table with OID {} not found in catalog", table_oid);
                    panic!("Table not found");
                },
            }
        };

        Self {
            context,
            plan,
            table_heap,
            iterator: None,
            initialized: false,
        }
    }
}

impl AbstractExecutor for SeqScanExecutor {
    /// Initializes the sequential scan by creating the table iterator.
    ///
    /// This method performs the following setup:
    ///
    /// 1. Verifies the table still exists in the catalog (by OID)
    /// 2. Creates a fresh `TransactionalTableHeap` wrapper
    /// 3. Obtains the transaction context for visibility checks
    /// 4. Creates a `TableIterator` spanning the entire table
    ///
    /// # Iterator Setup
    ///
    /// The iterator is configured to scan the entire table:
    /// - **Start RID**: `(first_page_id, 0)` - first slot of first page
    /// - **End RID**: `(MAX, MAX)` - sentinel indicating full scan
    ///
    /// # Idempotency
    ///
    /// This method is idempotent - calling it multiple times has no effect
    /// after the first successful initialization. The `initialized` flag
    /// prevents redundant setup.
    ///
    /// # Panics
    ///
    /// Panics if the table OID from the plan doesn't match the table found
    /// in the catalog (indicates catalog corruption or concurrent DDL).
    fn init(&mut self) {
        if self.initialized {
            trace!("SeqScanExecutor already initialized");
            return;
        }

        trace!(
            "Initializing SeqScanExecutor for table: {} (OID: {})",
            self.plan.get_table_name(),
            self.plan.get_table_oid()
        );

        // Get table info from catalog using OID to ensure we get the correct table
        let table_info = {
            let context = self.context.read();
            let catalog = context.get_catalog();
            let catalog_guard = catalog.read();
            catalog_guard
                .get_table_by_oid(self.plan.get_table_oid())
                .cloned()
        };

        if let Some(table_info) = table_info {
            // Verify we have the correct table
            assert_eq!(
                table_info.get_table_oidt(),
                self.plan.get_table_oid(),
                "Table OID mismatch in SeqScanExecutor"
            );

            // Create table heap and iterator (table_heap should already be correct from constructor,
            // but recreate it here to be safe and ensure consistency)
            let table_heap = Arc::new(TransactionalTableHeap::new(
                table_info.get_table_heap(),
                table_info.get_table_oidt(),
            ));

            self.table_heap = table_heap.clone();

            // Get transaction context for the iterator
            let txn_ctx = {
                let context = self.context.read();
                Some(context.get_transaction_context())
            };

            // Create iterator with start/end RIDs and transaction context
            let first_page_id = table_heap.get_table_heap().get_first_page_id();
            self.iterator = Some(TableIterator::new(
                table_heap,
                RID::new(first_page_id, 0), // start_rid from table's first page
                RID::new(u64::MAX, u32::MAX), // end_rid (scan entire table)
                txn_ctx,
            ));
            self.initialized = true;
            trace!(
                "SeqScanExecutor initialized successfully for table OID {}",
                self.plan.get_table_oid()
            );
        } else {
            error!("Table with OID {} not found", self.plan.get_table_oid());
        }
    }

    /// Returns the next visible tuple from the table scan.
    ///
    /// This method iterates through the table heap, automatically skipping
    /// deleted tuples, and returns the next visible tuple along with its RID.
    ///
    /// # Deleted Tuple Handling
    ///
    /// Tuples marked as deleted (via `TupleMeta::is_deleted()`) are
    /// automatically skipped. This implements the soft-delete pattern used
    /// by MVCC:
    ///
    /// ```text
    /// Slot 0: [Active]  ──▶ Returned
    /// Slot 1: [Deleted] ──▶ Skipped
    /// Slot 2: [Active]  ──▶ Returned
    /// Slot 3: [Deleted] ──▶ Skipped
    /// ...
    /// ```
    ///
    /// # Returns
    ///
    /// - `Ok(Some((tuple, rid)))` - Next visible tuple and its location
    /// - `Ok(None)` - End of table reached, no more tuples
    /// - `Err(DBError::Execution)` - Iterator not initialized (internal error)
    ///
    /// # Lazy Initialization
    ///
    /// If `next()` is called before `init()`, initialization is performed
    /// automatically. However, explicit initialization is recommended for
    /// predictable error handling.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut executor = SeqScanExecutor::new(context, plan);
    /// executor.init();
    ///
    /// // Process all tuples
    /// while let Ok(Some((tuple, rid))) = executor.next() {
    ///     // tuple is guaranteed to be non-deleted and visible
    ///     process_tuple(&tuple);
    /// }
    /// ```
    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if !self.initialized {
            trace!("SeqScanExecutor not initialized, initializing now");
            self.init();
        }

        // Get iterator reference
        let iter = match self.iterator.as_mut() {
            Some(iter) => iter,
            None => {
                return Err(DBError::Execution("Iterator not initialized".to_string()));
            },
        };

        // Keep trying until we find a valid tuple or reach the end
        for (meta, tuple) in iter.by_ref() {
            let rid = tuple.get_rid();
            trace!("Found tuple with RID {:?}", rid);

            // Skip deleted tuples
            if meta.is_deleted() {
                trace!("Skipping deleted tuple with RID {:?}", rid);
                continue;
            }

            // Return valid tuple
            return Ok(Some((tuple, rid)));
        }

        trace!("Reached end of table scan");
        Ok(None)
    }

    /// Returns the output schema for this scan.
    ///
    /// The output schema defines the columns and types of tuples returned
    /// by this executor. For sequential scans, this is typically the full
    /// table schema (all columns), though it may be a subset if the query
    /// plan includes column pruning optimization.
    ///
    /// # Returns
    ///
    /// A reference to the schema from the underlying plan node.
    fn get_output_schema(&self) -> &Schema {
        trace!("Getting output schema: {:?}", self.plan.get_output_schema());
        self.plan.get_output_schema()
    }

    /// Returns the shared execution context.
    ///
    /// Provides access to:
    /// - **Catalog**: Table and index metadata
    /// - **Buffer Pool Manager**: Page access for the scan
    /// - **Transaction Context**: Isolation level and visibility rules
    ///
    /// # Returns
    ///
    /// An `Arc`-wrapped, `RwLock`-protected reference to the execution context.
    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.context.clone()
    }
}

#[cfg(test)]
mod tests {
    use parking_lot::RwLock;
    use tempfile::TempDir;

    use super::*;
    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
    use crate::storage::table::tuple::TupleMeta;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::{Val, Value};

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<TransactionManager>,
        transaction_context: Arc<TransactionContext>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 100;
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

            // Create disk components
            let disk_manager = AsyncDiskManager::new(
                db_path.clone(),
                log_path.clone(),
                DiskManagerConfig::default(),
            )
            .await;
            let disk_manager_arc = Arc::new(disk_manager.unwrap());
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(
                BufferPoolManager::new(
                    BUFFER_POOL_SIZE,
                    disk_manager_arc.clone(),
                    replacer.clone(),
                )
                .unwrap(),
            );

            // Create transaction manager and lock manager first
            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());

            let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));
            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            Self {
                bpm,
                transaction_manager,
                transaction_context,
                _temp_dir: temp_dir,
            }
        }

        pub fn bpm(&self) -> Arc<BufferPoolManager> {
            Arc::clone(&self.bpm)
        }
    }

    fn create_catalog(ctx: &TestContext) -> Catalog {
        Catalog::new(
            ctx.bpm(),
            ctx.transaction_manager.clone(), // Add transaction manager
        )
    }

    fn create_test_values(id: i32, name: &str, age: i32) -> (TupleMeta, Vec<Value>) {
        let values = vec![
            Value::new(id),
            Value::new(name.to_string()),
            Value::new(age),
        ];
        let meta = TupleMeta::new(0);
        (meta, values)
    }

    fn create_execution_context(
        ctx: &TestContext,
        catalog: Arc<RwLock<Catalog>>,
    ) -> Arc<RwLock<ExecutionContext>> {
        Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm(),
            catalog,
            ctx.transaction_context.clone(),
        )))
    }

    #[tokio::test]
    async fn test_seq_scan_executor_with_data() {
        let ctx = TestContext::new("test_seq_scan_executor_with_data").await;

        // Create catalog and schema
        let mut catalog = create_catalog(&ctx);
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ]);

        // Create transaction and table
        let table_name = "test_table";
        let table_info = catalog
            .create_table(table_name.to_string(), schema.clone())
            .unwrap();
        let table_heap = table_info.get_table_heap();
        let table_heap_guard = table_heap;

        // Insert test data
        let test_data = vec![(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)];

        for (id, name, age) in test_data.iter() {
            let (meta, values) = create_test_values(*id, name, *age);
            table_heap_guard
                .insert_tuple_from_values(values, &schema, Arc::from(meta))
                .expect("Failed to insert tuple");
        }

        // Create scan plan and executor
        let plan = Arc::new(SeqScanPlanNode::new(
            schema.clone(),
            table_info.get_table_oidt(),
            table_name.to_string(),
        ));

        let context = create_execution_context(&ctx, Arc::new(RwLock::new(catalog)));
        let mut executor = SeqScanExecutor::new(context, plan);
        executor.init();

        // Test scanning
        let mut found_tuples = Vec::new();
        while let Ok(Some((tuple, _))) = executor.next() {
            // Get values from tuple using proper indexing
            let id = match tuple.get_value(0).get_val() {
                Val::Integer(i) => *i,
                _ => panic!("Expected Integer value for id"),
            };

            let name = match tuple.get_value(1).get_val() {
                Val::VarLen(s) => s.clone(),
                _ => panic!("Expected VarLen value for name"),
            };

            let age = match tuple.get_value(2).get_val() {
                Val::Integer(i) => *i,
                _ => panic!("Expected Integer value for age"),
            };

            found_tuples.push((id, name, age));
        }

        assert_eq!(found_tuples.len(), 3, "Should have scanned 3 tuples");

        // Verify each tuple was found
        for (id, name, age) in test_data {
            assert!(
                found_tuples.iter().any(|(t_id, t_name, t_age)| {
                    *t_id == id && t_name == name && *t_age == age
                }),
                "Did not find tuple: ({}, {}, {})",
                id,
                name,
                age
            );
        }
    }

    #[tokio::test]
    async fn test_seq_scan_executor_empty_table() {
        let ctx = TestContext::new("test_seq_scan_executor_empty").await;
        let _transaction_context = ctx.transaction_context.clone();

        // Create catalog and schema
        let mut catalog = create_catalog(&ctx);
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);

        // Create transaction and empty table
        let table_name = "empty_table";
        let table_info = catalog
            .create_table(table_name.to_string(), schema.clone())
            .unwrap();

        // Create scan plan and executor
        let plan = Arc::new(SeqScanPlanNode::new(
            schema.clone(),
            table_info.get_table_oidt(),
            table_name.to_string(),
        ));

        let exec_context = create_execution_context(&ctx, Arc::new(RwLock::new(catalog)));
        let mut executor = SeqScanExecutor::new(exec_context, plan);

        // Initialize and test
        executor.init();
        assert!(executor.next().unwrap().is_none());
    }

    #[tokio::test]
    async fn test_seq_scan_executor_single_tuple() {
        let ctx = TestContext::new("test_seq_scan_executor_single").await;
        let transaction_context = ctx.transaction_context.clone();

        // Create catalog and schema
        let mut catalog = create_catalog(&ctx);
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // Create table
        let table_name = "single_tuple_table";
        let table_info = catalog
            .create_table(table_name.to_string(), schema.clone())
            .unwrap();

        // Insert a single tuple
        let table_heap = table_info.get_table_heap();
        let transactional_table_heap =
            TransactionalTableHeap::new(table_heap.clone(), table_info.get_table_oidt());

        let values = vec![Value::new(1), Value::new("Alice")];
        transactional_table_heap
            .insert_tuple_from_values(values, &schema, transaction_context.clone())
            .unwrap();

        // Create scan plan and executor
        let plan = Arc::new(SeqScanPlanNode::new(
            schema.clone(),
            table_info.get_table_oidt(),
            table_name.to_string(),
        ));

        let exec_context = create_execution_context(&ctx, Arc::new(RwLock::new(catalog)));
        let mut executor = SeqScanExecutor::new(exec_context, plan);

        // Initialize and test
        executor.init();

        // Should get one tuple
        let result = executor.next().unwrap();
        assert!(result.is_some());

        let (tuple, _rid) = result.unwrap();
        assert_eq!(tuple.get_values().len(), 2);

        // No more tuples
        assert!(executor.next().unwrap().is_none());
    }

    #[tokio::test]
    async fn test_seq_scan_executor_multiple_tuples() {
        let ctx = TestContext::new("test_seq_scan_executor_multiple").await;
        let transaction_context = ctx.transaction_context.clone();

        // Create catalog and schema
        let mut catalog = create_catalog(&ctx);
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ]);

        // Create table
        let table_name = "multiple_tuples_table";
        let table_info = catalog
            .create_table(table_name.to_string(), schema.clone())
            .unwrap();

        // Insert multiple tuples
        let table_heap = table_info.get_table_heap();
        let transactional_table_heap =
            TransactionalTableHeap::new(table_heap.clone(), table_info.get_table_oidt());

        let test_data = vec![
            vec![Value::new(1), Value::new("Alice"), Value::new(25)],
            vec![Value::new(2), Value::new("Bob"), Value::new(30)],
            vec![Value::new(3), Value::new("Charlie"), Value::new(35)],
        ];

        for values in test_data {
            transactional_table_heap
                .insert_tuple_from_values(values, &schema, transaction_context.clone())
                .unwrap();
        }

        // Create scan plan and executor
        let plan = Arc::new(SeqScanPlanNode::new(
            schema.clone(),
            table_info.get_table_oidt(),
            table_name.to_string(),
        ));

        let exec_context = create_execution_context(&ctx, Arc::new(RwLock::new(catalog)));
        let mut executor = SeqScanExecutor::new(exec_context, plan);

        // Initialize and test
        executor.init();

        // Count tuples
        let mut tuple_count = 0;
        while let Some((tuple, _rid)) = executor.next().unwrap() {
            tuple_count += 1;
            assert_eq!(tuple.get_values().len(), 3);
        }

        assert_eq!(tuple_count, 3);
    }
}
