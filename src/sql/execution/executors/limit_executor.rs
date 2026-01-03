//! # Limit Executor
//!
//! Implements the `LIMIT` clause executor for restricting the number of output rows.
//!
//! ## Overview
//!
//! The `LimitExecutor` is a pass-through executor that counts tuples from its
//! child executor and stops producing output once the specified limit is reached.
//! This enables efficient result set size control without materializing all results.
//!
//! ## SQL Syntax
//!
//! ```sql
//! SELECT column1, column2, ...
//! FROM table_name
//! WHERE condition
//! LIMIT count;
//!
//! -- Example: Get first 10 employees
//! SELECT * FROM employees ORDER BY hire_date LIMIT 10;
//! ```
//!
//! ## Execution Model
//!
//! The limit executor operates as a counting filter in the Volcano iterator model:
//!
//! ```text
//!                    ┌──────────────────────┐
//!                    │    Parent Executor   │
//!                    └──────────┬───────────┘
//!                               │ next()
//!                               ▼
//!                    ┌──────────────────────┐
//!                    │   LimitExecutor      │
//!                    │  ┌────────────────┐  │
//!                    │  │ current_index  │  │
//!                    │  │ limit: N       │  │
//!                    │  └────────────────┘  │
//!                    │                      │
//!                    │  if index < limit:   │
//!                    │    forward tuple     │
//!                    │    index++           │
//!                    │  else:               │
//!                    │    return None       │
//!                    └──────────┬───────────┘
//!                               │ next()
//!                               ▼
//!                    ┌──────────────────────┐
//!                    │    Child Executor    │
//!                    │  (Scan, Filter, etc) │
//!                    └──────────────────────┘
//! ```
//!
//! ## Early Termination
//!
//! Once the limit is reached, the executor immediately returns `None` without
//! calling the child executor. This provides optimal performance by avoiding
//! unnecessary tuple processing.
//!
//! ## Performance Characteristics
//!
//! | Scenario | Behavior |
//! |----------|----------|
//! | `LIMIT 0` | Returns `None` immediately, no child calls |
//! | `LIMIT N` (N < total rows) | Early termination after N tuples |
//! | `LIMIT N` (N >= total rows) | Returns all tuples from child |
//!
//! ## Common Query Patterns
//!
//! ```sql
//! -- Top-N queries (with ORDER BY)
//! SELECT * FROM products ORDER BY price DESC LIMIT 5;
//!
//! -- Pagination (typically combined with OFFSET)
//! SELECT * FROM orders LIMIT 20;
//!
//! -- Sample data
//! SELECT * FROM large_table LIMIT 100;
//! ```

use std::sync::Arc;

use log::debug;
use parking_lot::RwLock;

use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::limit_plan::LimitNode;
use crate::storage::table::tuple::Tuple;

/// Executor for SQL `LIMIT` clauses.
///
/// Restricts the number of tuples returned from a child executor to a specified
/// maximum count. Acts as a counting pass-through filter that stops requesting
/// tuples once the limit is reached.
///
/// # Architecture
///
/// The executor maintains a simple counter that increments with each tuple
/// returned. When the counter reaches the limit, all subsequent `next()` calls
/// return `None` without querying the child executor.
///
/// # Fields
///
/// * `context` - Shared execution context (inherited from parent)
/// * `plan` - The limit plan node containing the limit value
/// * `current_index` - Counter tracking how many tuples have been returned
/// * `initialized` - Whether `init()` has been called
/// * `child_executor` - The child executor producing tuples to limit
///
/// # Example
///
/// ```ignore
/// // Create a limit executor that returns at most 10 rows
/// let limit_plan = Arc::new(LimitNode::new(10, schema, children));
/// let mut executor = LimitExecutor::new(child_executor, context, limit_plan);
/// executor.init();
///
/// // Will return at most 10 tuples
/// while let Some((tuple, rid)) = executor.next()? {
///     process(tuple);
/// }
/// ```
///
/// # Performance
///
/// - **Time Complexity**: O(1) per tuple (simple counter check)
/// - **Space Complexity**: O(1) (only stores a counter)
/// - **Early Termination**: Stops child execution once limit reached
pub struct LimitExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<LimitNode>,
    current_index: usize,
    initialized: bool,
    child_executor: Option<Box<dyn AbstractExecutor>>,
}

impl LimitExecutor {
    /// Creates a new `LimitExecutor` with the specified limit.
    ///
    /// # Arguments
    ///
    /// * `child_executor` - The child executor producing tuples to limit
    /// * `context` - Shared execution context for catalog and transaction access
    /// * `plan` - The limit plan node containing the maximum row count
    ///
    /// # Returns
    ///
    /// A new uninitialized `LimitExecutor`. Call `init()` before `next()`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let child = create_scan_executor();
    /// let limit_plan = Arc::new(LimitNode::new(100, schema, children));
    /// let executor = LimitExecutor::new(Box::new(child), context, limit_plan);
    /// ```
    ///
    /// # Note
    ///
    /// The limit value of 0 is valid and will cause `next()` to return `None`
    /// immediately without ever calling the child executor.
    pub fn new(
        child_executor: Box<dyn AbstractExecutor>,
        context: Arc<RwLock<ExecutionContext>>,
        plan: Arc<LimitNode>,
    ) -> Self {
        debug!("Creating LimitExecutor");

        Self {
            context,
            plan,
            current_index: 0,
            initialized: false,
            child_executor: Some(child_executor),
        }
    }
}

impl AbstractExecutor for LimitExecutor {
    /// Initializes the limit executor and its child.
    ///
    /// Propagates initialization to the child executor. This ensures the
    /// entire executor tree is properly initialized before tuple retrieval.
    ///
    /// # Idempotency
    ///
    /// Multiple calls to `init()` are safe; the child is only initialized
    /// once due to the `initialized` flag.
    fn init(&mut self) {
        if !self.initialized {
            // Initialize child executor
            if let Some(child) = &mut self.child_executor {
                child.init();
            }
            self.initialized = true;
        }
    }

    /// Returns the next tuple if the limit has not been reached.
    ///
    /// # Algorithm
    ///
    /// 1. Check if `current_index >= limit` → return `None` (early termination)
    /// 2. Call `child.next()` to get the next tuple
    /// 3. If tuple received, increment counter and return it
    /// 4. If child exhausted, return `None`
    ///
    /// # Returns
    ///
    /// * `Ok(Some((tuple, rid)))` - Next tuple within limit
    /// * `Ok(None)` - Limit reached or child exhausted
    /// * `Err(DBError)` - Error from child executor
    ///
    /// # Early Termination
    ///
    /// When the limit is reached, this method returns `None` immediately
    /// without calling the child executor. This is an important optimization
    /// that prevents unnecessary work in the child executor tree.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // With LIMIT 2 and 5 rows in child:
    /// executor.next()  // Returns row 1, index becomes 1
    /// executor.next()  // Returns row 2, index becomes 2
    /// executor.next()  // Returns None (limit reached, child not called)
    /// ```
    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        // Check if we've reached the limit
        if self.current_index >= self.plan.get_limit() {
            return Ok(None);
        }

        // Get next tuple from child
        if let Some(child) = &mut self.child_executor {
            // Handle the Result from child.next()
            match child.next()? {
                Some(result) => {
                    self.current_index += 1;
                    Ok(Some(result))
                },
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    /// Returns the output schema for this executor.
    ///
    /// The limit executor preserves the schema from its child executor,
    /// as it only affects the number of rows, not their structure.
    ///
    /// # Returns
    ///
    /// Reference to the output schema (same as child's schema).
    fn get_output_schema(&self) -> &Schema {
        debug!("Getting output schema: {:?}", self.plan.get_output_schema());
        self.plan.get_output_schema()
    }

    /// Returns the shared execution context.
    ///
    /// Provides access to:
    /// - Buffer pool manager for page access
    /// - Catalog for table and index metadata
    /// - Transaction context for MVCC operations
    ///
    /// # Returns
    ///
    /// Arc-wrapped RwLock-protected execution context for thread-safe access.
    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.context.clone()
    }
}

#[cfg(test)]
mod tests {
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
    use crate::sql::execution::executors::mock_executor::MockExecutor;
    use crate::sql::execution::plans::abstract_plan::PlanNode;
    use crate::sql::execution::plans::mock_scan_plan::MockScanNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
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

            let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadCommitted));
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

    #[tokio::test]
    async fn test_limit_executor() {
        let ctx = TestContext::new("test_limit_executor").await;
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // Create mock tuples with raw values as expected by MockScanNode
        let mock_tuples = vec![
            (
                vec![Value::new(1), Value::new("a".to_string())],
                RID::new(0, 0), // Changed RID to match what MockScanExecutor expects
            ),
            (
                vec![Value::new(2), Value::new("b".to_string())],
                RID::new(0, 1), // Sequential slot numbers starting from 0
            ),
            (
                vec![Value::new(3), Value::new("c".to_string())],
                RID::new(0, 2),
            ),
        ];

        // Create mock scan plan with mock data
        let mock_scan_plan = MockScanNode::new(
            schema.clone(),
            "test_table".to_string(),
            vec![], // empty children vector
        )
        .with_tuples(mock_tuples.clone());

        // Create limit plan with limit of 2
        let limit_plan = Arc::new(LimitNode::new(
            2,
            schema.clone(),
            vec![PlanNode::MockScan(mock_scan_plan.clone())],
        ));

        // Create execution context
        let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            Arc::new(RwLock::new(create_catalog(&ctx))),
            ctx.transaction_context.clone(),
        )));

        // Create mock scan executor
        let child_executor = Box::new(MockExecutor::new(
            exec_ctx.clone(),
            Arc::new(mock_scan_plan.clone()),
            0,                   // current_index
            mock_tuples.clone(), // Pass the tuples directly
            schema.clone(),
        ));

        // Create and test limit executor
        let mut limit_executor = LimitExecutor::new(child_executor, exec_ctx, limit_plan);
        limit_executor.init();

        // Should get first two tuples
        let result1 = limit_executor.next().unwrap();
        assert!(result1.is_some(), "Expected first result to be Some");
        let (tuple1, rid1) = result1.unwrap();
        assert_eq!(
            tuple1.get_value(0),
            Value::new(1),
            "First tuple id should be 1"
        );
        assert_eq!(
            tuple1.get_value(1),
            Value::new("a".to_string()),
            "First tuple name should be 'a'"
        );
        assert_eq!(rid1, RID::new(0, 0), "First tuple RID should match");

        let result2 = limit_executor.next().unwrap();
        assert!(result2.is_some(), "Expected second result to be Some");
        let (tuple2, rid2) = result2.unwrap();
        assert_eq!(
            tuple2.get_value(0),
            Value::new(2),
            "Second tuple id should be 2"
        );
        assert_eq!(
            tuple2.get_value(1),
            Value::new("b".to_string()),
            "Second tuple name should be 'b'"
        );
        assert_eq!(rid2, RID::new(0, 1), "Second tuple RID should match");

        // Third call should return None since limit is 2
        let result3 = limit_executor.next().unwrap();
        assert!(
            result3.is_none(),
            "Expected third result to be None due to limit"
        );
    }

    #[tokio::test]
    async fn test_limit_zero() {
        let ctx = TestContext::new("test_limit_zero").await;
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);

        // Create mock tuples with raw values
        let mock_tuples = vec![(
            vec![Value::new(1)],
            RID::new(0, 0), // Changed RID to match what MockScanExecutor expects
        )];

        // Create mock scan plan with mock data
        let mock_scan_plan = MockScanNode::new(
            schema.clone(),
            "test_table".to_string(),
            vec![], // empty children vector
        )
        .with_tuples(mock_tuples.clone());

        // Create limit plan with limit of 0
        let limit_plan = Arc::new(LimitNode::new(
            0,
            schema.clone(),
            vec![PlanNode::MockScan(mock_scan_plan.clone())],
        ));

        // Create execution context
        let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm.clone(),
            Arc::new(RwLock::new(create_catalog(&ctx))),
            ctx.transaction_context.clone(),
        )));

        // Create mock scan executor
        let child_executor = Box::new(MockExecutor::new(
            exec_ctx.clone(),
            Arc::new(mock_scan_plan.clone()),
            0,                   // current_index
            mock_tuples.clone(), // Pass the tuples directly
            schema.clone(),
        ));

        // Create and test limit executor
        let mut limit_executor = LimitExecutor::new(child_executor, exec_ctx, limit_plan);
        limit_executor.init();

        // Should get no tuples
        let result = limit_executor.next().unwrap();
        assert!(result.is_none(), "Expected no results with limit of 0");
    }
}
