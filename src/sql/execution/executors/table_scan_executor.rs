//! # Table Scan Executor
//!
//! This module implements a straightforward sequential scan executor that reads
//! all tuples from a table using the Volcano-style iterator model. It provides
//! a simpler implementation compared to `SeqScanExecutor` by delegating iterator
//! creation to the plan node.
//!
//! ## SQL Syntax
//!
//! Table scans are the fundamental data access method for queries:
//!
//! ```sql
//! -- Full table scan
//! SELECT * FROM employees;
//!
//! -- Scan with projection (columns selected at higher level)
//! SELECT name, salary FROM employees;
//!
//! -- Scan feeding into filter
//! SELECT * FROM employees WHERE department = 'Engineering';
//! ```
//!
//! ## Execution Model
//!
//! The executor follows the Volcano iterator model with three phases:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                     TableScanExecutor Lifecycle                     │
//! ├─────────────────────────────────────────────────────────────────────┤
//! │                                                                     │
//! │  new()  ─────▶  Store plan reference                                │
//! │                 No iterator created yet                             │
//! │                                                                     │
//! │  init() ─────▶  Create TableScanIterator via plan.scan()            │
//! │                 Iterator positioned at first tuple                  │
//! │                                                                     │
//! │  next() ─────▶  Loop: get next tuple from iterator                  │
//! │                       if deleted → skip, continue                   │
//! │                       if valid → return (Tuple, RID)                │
//! │                       if exhausted → return None                    │
//! │                                                                     │
//! └─────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Deleted Tuple Handling
//!
//! The executor automatically filters out deleted tuples:
//!
//! ```text
//! Table Heap Slots:
//! ┌─────────┬─────────┬─────────┬─────────┬─────────┐
//! │ Tuple 0 │ Tuple 1 │ Tuple 2 │ Tuple 3 │ Tuple 4 │
//! │ Active  │ DELETED │ Active  │ DELETED │ Active  │
//! └────┬────┴────┬────┴────┬────┴────┬────┴────┬────┘
//!      │         │         │         │         │
//!      ▼         ✗         ▼         ✗         ▼
//!   Return     Skip     Return     Skip     Return
//! ```
//!
//! ## Comparison with SeqScanExecutor
//!
//! | Aspect | TableScanExecutor | SeqScanExecutor |
//! |--------|-------------------|-----------------|
//! | Iterator source | Plan node (`scan()`) | Created manually |
//! | Table lookup | Via plan's TableInfo | Via catalog by OID |
//! | MVCC support | Basic (deleted flag) | Full transactional |
//! | Complexity | Simpler | More features |
//!
//! ## Performance Characteristics
//!
//! | Aspect | Complexity | Notes |
//! |--------|------------|-------|
//! | Time | O(n) | Reads every tuple |
//! | I/O | O(pages) | Sequential page access |
//! | Memory | O(1) | Only current tuple in memory |
//!
//! ## Implementation Notes
//!
//! - The iterator is created lazily during `init()`, not in constructor
//! - Deleted tuples are skipped using a loop (not recursion) to avoid stack overflow
//! - The plan node owns the `TableInfo` and provides the `scan()` method

use std::sync::Arc;

use log::{debug, error};
use parking_lot::RwLock;

use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::table_scan_plan::TableScanNode;
use crate::storage::table::table_iterator::TableScanIterator;
use crate::storage::table::tuple::Tuple;

/// Executor for sequential table scans using the Volcano iterator model.
///
/// `TableScanExecutor` provides a simple implementation of full table scans,
/// reading tuples sequentially from the underlying table heap. It delegates
/// iterator creation to the plan node, which encapsulates the table information.
///
/// # Architecture
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────┐
/// │                    TableScanExecutor                        │
/// │  ┌─────────────────────────────────────────────────────┐    │
/// │  │  plan: TableScanNode                                │    │
/// │  │    └── table_info: TableInfo                        │    │
/// │  │          └── table_heap: Arc<TableHeap>             │    │
/// │  └─────────────────────────────────────────────────────┘    │
/// │                           │                                 │
/// │                           ▼ plan.scan()                     │
/// │  ┌─────────────────────────────────────────────────────┐    │
/// │  │  iterator: TableScanIterator                        │    │
/// │  │    └── Iterates over (TupleMeta, Tuple) pairs       │    │
/// │  └─────────────────────────────────────────────────────┘    │
/// └─────────────────────────────────────────────────────────────┘
/// ```
///
/// # Example Usage
///
/// ```ignore
/// // Create a table scan plan from table info
/// let plan = Arc::new(TableScanNode::new(
///     table_info,
///     schema.clone(),
///     None,  // No predicate pushdown
/// ));
///
/// // Create and initialize executor
/// let mut executor = TableScanExecutor::new(context.clone(), plan);
/// executor.init();
///
/// // Iterate through all non-deleted tuples
/// while let Ok(Some((tuple, rid))) = executor.next() {
///     println!("Tuple at {:?}: {:?}", rid, tuple);
/// }
/// ```
///
/// # Fields
///
/// - `context`: Shared execution context (catalog, buffer pool, transaction)
/// - `plan`: The table scan plan containing table info and schema
/// - `initialized`: Flag preventing double initialization
/// - `iterator`: The underlying tuple iterator, created during init()
pub struct TableScanExecutor {
    /// Shared execution context providing access to catalog and transaction.
    context: Arc<RwLock<ExecutionContext>>,

    /// The table scan plan node containing table metadata and iterator factory.
    plan: Arc<TableScanNode>,

    /// Flag indicating whether `init()` has been called.
    initialized: bool,

    /// Iterator over the table's tuples, created during initialization.
    /// Returns `(TupleMeta, Tuple)` pairs for each slot.
    iterator: Option<TableScanIterator>,
}

impl TableScanExecutor {
    /// Creates a new `TableScanExecutor` with the given context and plan.
    ///
    /// The iterator is not created at this point - it will be instantiated
    /// lazily during `init()` by calling `plan.scan()`.
    ///
    /// # Arguments
    ///
    /// * `context` - Shared execution context for catalog and transaction access
    /// * `plan` - Table scan plan containing:
    ///   - `TableInfo` with the table heap reference
    ///   - Output schema for the scan
    ///   - Optional filter predicate (for predicate pushdown)
    ///
    /// # Returns
    ///
    /// A new executor instance ready for initialization.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let plan = Arc::new(TableScanNode::new(table_info, schema, None));
    /// let executor = TableScanExecutor::new(context.clone(), plan);
    /// // Call init() before next()
    /// ```
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: Arc<TableScanNode>) -> Self {
        Self {
            context,
            plan,
            iterator: None,
            initialized: false,
        }
    }
}

impl AbstractExecutor for TableScanExecutor {
    /// Initializes the table scan by creating the underlying iterator.
    ///
    /// This method delegates to `plan.scan()` to create a `TableScanIterator`
    /// that will iterate over all tuples in the table heap.
    ///
    /// # Behavior
    ///
    /// - Creates a fresh iterator positioned at the first tuple
    /// - Sets the `initialized` flag to prevent redundant initialization
    /// - Logs the table name being scanned for debugging
    ///
    /// # Note
    ///
    /// If `next()` is called before `init()`, initialization happens
    /// automatically. However, explicit initialization is recommended
    /// for predictable behavior.
    fn init(&mut self) {
        debug!(
            "Initializing TableScanExecutor for table: {}",
            self.plan.get_table_name()
        );

        // Create a new table scan iterator from the plan
        self.iterator = Some(self.plan.scan());
        self.initialized = true;

        debug!("TableScanExecutor initialized successfully");
    }

    /// Returns the next non-deleted tuple from the table scan.
    ///
    /// This method iterates through the table heap, automatically skipping
    /// any tuples marked as deleted, and returns the next visible tuple.
    ///
    /// # Deleted Tuple Filtering
    ///
    /// Uses a loop (not recursion) to skip deleted tuples, preventing
    /// potential stack overflow on tables with many consecutive deletions:
    ///
    /// ```text
    /// Iterator: [D] [D] [D] [Active] [D] [Active] ...
    ///            ↓   ↓   ↓     ↓
    ///          skip skip skip RETURN
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
    /// If called before `init()`, this method automatically initializes
    /// the executor first.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut executor = TableScanExecutor::new(context, plan);
    /// executor.init();
    ///
    /// let mut count = 0;
    /// while let Ok(Some((tuple, rid))) = executor.next() {
    ///     count += 1;
    ///     // Process visible tuple
    /// }
    /// println!("Scanned {} tuples", count);
    /// ```
    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        // Initialize if not already done
        if !self.initialized {
            self.init();
        }

        // Get the next tuple from the iterator
        loop {
            return match &mut self.iterator {
                Some(iter) => {
                    match iter.next() {
                        Some((meta, tuple)) => {
                            // Skip deleted tuples
                            if meta.is_deleted() {
                                continue; // Use continue instead of recursive call
                            }
                            debug!("Found tuple with RID: {:?}", tuple.get_rid());
                            Ok(Some((tuple.clone(), tuple.get_rid())))
                        },
                        None => {
                            debug!("No more tuples to scan");
                            Ok(None)
                        },
                    }
                },
                None => {
                    error!("Iterator not initialized");
                    Err(DBError::Execution("Iterator not initialized".to_string()))
                },
            };
        }
    }

    /// Returns the output schema for this scan.
    ///
    /// The schema defines the columns and types of tuples returned by
    /// this executor. For table scans, this is typically the full table
    /// schema unless column pruning has been applied.
    ///
    /// # Returns
    ///
    /// A reference to the schema from the underlying plan node.
    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }

    /// Returns the shared execution context.
    ///
    /// Provides access to:
    /// - **Catalog**: Table and index metadata
    /// - **Buffer Pool Manager**: Page access for the scan
    /// - **Transaction Context**: Current transaction state
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
    use crate::types_db::value::Value;

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

    #[tokio::test]
    async fn test_table_scan_executor() {
        let ctx = TestContext::new("test_table_scan_executor").await;
        let bpm = ctx.bpm.clone();
        let transaction_manager = ctx.transaction_manager.clone();

        // Create schema
        let schema = Arc::new(Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ]));

        // Create catalog and table
        let mut catalog = Catalog::new(bpm.clone(), transaction_manager);

        let table_name = "test_table".to_string();
        let table_info = catalog.create_table(table_name, (*schema).clone()).unwrap();

        // Insert test data
        let table_heap = table_info.get_table_heap();
        let test_data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)];

        for (id, name, age) in test_data.iter() {
            let (meta, values) = create_test_values(*id, name, *age);
            table_heap
                .insert_tuple_from_values(values, &schema, Arc::from(meta))
                .expect("Failed to insert tuple");
        }

        // Create scan plan and executor
        let plan = Arc::new(TableScanNode::new(
            table_info, // Wrap in Arc
            schema.clone(),
            None,
        ));

        let context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm,
            Arc::new(RwLock::new(catalog)),
            ctx.transaction_context,
        )));

        let mut executor = TableScanExecutor::new(context, plan);

        // Test scanning
        executor.init();
        let mut count = 0;
        while let Ok(Some(_)) = executor.next() {
            count += 1;
        }

        assert_eq!(count, 3, "Should have scanned exactly 3 tuples");
    }

    #[tokio::test]
    async fn test_table_scan_executor_empty() {
        let ctx = TestContext::new("test_table_scan_executor_empty").await;
        let bpm = ctx.bpm.clone();

        // Create schema and empty table
        let schema = Arc::new(Schema::new(vec![Column::new("id", TypeId::Integer)]));
        let catalog_guard = Arc::new(RwLock::new(Catalog::new(
            bpm,
            ctx.transaction_manager.clone(),
        )));

        let plan;
        {
            let mut catalog = catalog_guard.write();

            let table_name = "empty_table".to_string();
            let table_info = catalog.create_table(table_name, (*schema).clone()).unwrap();

            // Create executor
            plan = Arc::new(TableScanNode::new(table_info, schema.clone(), None));
        }

        let context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm,
            catalog_guard,
            ctx.transaction_context,
        )));

        let mut executor = TableScanExecutor::new(context, plan);
        executor.init();

        assert!(
            executor.next().unwrap().is_none(),
            "Empty table should return no tuples"
        );
    }
}
