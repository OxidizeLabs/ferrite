//! # Mock Scan Executor
//!
//! Provides a self-generating test executor that produces mock tuples based on schema.
//!
//! ## Overview
//!
//! The `MockScanExecutor` is a testing utility that automatically generates mock
//! tuples during initialization based on the provided schema. Unlike `MockExecutor`
//! which requires pre-defined tuples, this executor creates its own test data.
//!
//! ## Purpose
//!
//! This executor is **not intended for production use**. It serves to:
//!
//! - Provide automatic test data generation based on schema
//! - Test executor pipelines without manual tuple construction
//! - Verify schema-aware processing in downstream executors
//! - Enable quick prototyping and debugging of query execution
//!
//! ## Mock Data Generation
//!
//! The executor generates 3 tuples with type-appropriate values:
//!
//! | Type | Generated Values |
//! |------|------------------|
//! | `Integer` | 0, 1, 2 |
//! | `VarChar` | "mock_value_0", "mock_value_1", "mock_value_2" |
//! | `Boolean` | true, false, true |
//! | Other | 0 (default) |
//!
//! ## Comparison with MockExecutor
//!
//! | Feature | MockScanExecutor | MockExecutor |
//! |---------|------------------|--------------|
//! | Data source | Auto-generated | User-provided |
//! | Tuple count | Fixed (3) | Variable |
//! | RID assignment | Sequential pages | User-defined |
//! | Use case | Quick tests | Precise test data |
//!
//! ## Usage Example
//!
//! ```ignore
//! let schema = Schema::new(vec![
//!     Column::new("id", TypeId::Integer),
//!     Column::new("name", TypeId::VarChar),
//! ]);
//!
//! let plan = Arc::new(MockScanNode::new(schema, "test_table".into(), vec![]));
//! let mut executor = MockScanExecutor::new(context, plan);
//! executor.init();
//!
//! // Will return 3 tuples:
//! // (0, "mock_value_0"), (1, "mock_value_1"), (2, "mock_value_2")
//! while let Some((tuple, rid)) = executor.next()? {
//!     println!("{:?}", tuple);
//! }
//! ```
//!
//! ## Reusability
//!
//! The executor supports multiple scans by calling `init()` again, which
//! resets the cursor to the beginning of the generated tuples.

use crate::catalog::schema::Schema;
use crate::common::config::PageId;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::mock_scan_plan::MockScanNode;
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use log::{debug, info};
use parking_lot::RwLock;
use std::sync::Arc;

/// Test executor that auto-generates mock tuples based on schema.
///
/// Generates 3 tuples during initialization with type-appropriate values
/// derived from the schema. Useful for quick testing without manually
/// constructing test data.
///
/// # Fields
///
/// * `context` - Shared execution context (required by trait interface)
/// * `plan` - Mock scan plan containing schema and table name
/// * `mock_tuples` - Generated tuples (populated during `init()`)
/// * `current_index` - Current position in the mock tuples vector
/// * `initialized` - Whether `init()` has been called
///
/// # Generated Data
///
/// For a schema with columns `(id: Integer, name: VarChar, active: Boolean)`:
///
/// ```text
/// Tuple 0: (0, "mock_value_0", true)  RID(0, 0)
/// Tuple 1: (1, "mock_value_1", false) RID(1, 0)
/// Tuple 2: (2, "mock_value_2", true)  RID(2, 0)
/// ```
///
/// # Note
///
/// This is a **testing-only** component. Do not use in production code.
pub struct MockScanExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<MockScanNode>,
    mock_tuples: Vec<(Arc<Tuple>, RID)>,
    current_index: usize,
    initialized: bool,
}

impl MockScanExecutor {
    /// Creates a new `MockScanExecutor` for the given plan.
    ///
    /// # Arguments
    ///
    /// * `context` - Shared execution context (required by trait interface)
    /// * `plan` - Mock scan plan containing schema and table name
    ///
    /// # Returns
    ///
    /// A new uninitialized `MockScanExecutor`. Call `init()` to generate
    /// mock tuples before calling `next()`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let plan = Arc::new(MockScanNode::new(schema, "users".into(), vec![]));
    /// let mut executor = MockScanExecutor::new(context, plan);
    /// executor.init(); // Generates 3 mock tuples
    /// ```
    ///
    /// # Note
    ///
    /// Mock tuples are not generated until `init()` is called.
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: Arc<MockScanNode>) -> Self {
        debug!(
            "Creating MockScanExecutor for table '{}'",
            plan.get_table_name()
        );

        Self {
            context,
            plan,
            mock_tuples: Vec::new(),
            current_index: 0,
            initialized: false,
        }
    }

    /// Generates mock tuples based on the schema.
    ///
    /// Creates 3 tuples with values appropriate for each column type:
    ///
    /// - `Integer`: Sequential values (0, 1, 2)
    /// - `VarChar`: Format strings ("mock_value_0", etc.)
    /// - `Boolean`: Alternating (true, false, true)
    /// - Other types: Default to 0
    ///
    /// # Returns
    ///
    /// Vector of 3 (tuple, RID) pairs with sequential page IDs.
    ///
    /// # RID Assignment
    ///
    /// Each tuple gets a RID with:
    /// - Page ID: Matches tuple index (0, 1, 2)
    /// - Slot number: Always 0
    fn generate_mock_data(&self) -> Vec<(Arc<Tuple>, RID)> {
        let schema = self.plan.get_output_schema();
        let mut mock_data = Vec::new();

        // Generate 3 mock tuples for testing
        for i in 0..3 {
            let mut values = Vec::new();
            for column in schema.get_columns().iter() {
                // Generate appropriate mock value based on column type
                let value = match column.get_type() {
                    TypeId::Integer => Value::new(i),
                    TypeId::VarChar => Value::new(format!("mock_value_{}", i)),
                    TypeId::Boolean => Value::new(i % 2 == 0),
                    // Add more types as needed
                    _ => Value::new(0), // Default value for unsupported types
                };
                values.push(value);
            }

            let rid = RID::new(i as PageId, 0);
            let tuple = Arc::new(Tuple::new(&values, schema, rid));
            mock_data.push((tuple, rid));
        }

        mock_data
    }
}

impl AbstractExecutor for MockScanExecutor {
    /// Initializes the executor and generates mock tuples.
    ///
    /// # First Call Behavior
    ///
    /// On first call, generates 3 mock tuples based on the schema and
    /// sets the cursor to the beginning.
    ///
    /// # Subsequent Call Behavior
    ///
    /// On subsequent calls, resets the cursor to allow rescanning the
    /// same mock tuples without regenerating them. This enables
    /// executor reuse in tests.
    ///
    /// # Example
    ///
    /// ```ignore
    /// executor.init();  // Generates tuples, cursor at 0
    /// // ... scan all tuples ...
    /// executor.init();  // Resets cursor to 0, same tuples
    /// // ... scan again ...
    /// ```
    fn init(&mut self) {
        if self.initialized {
            debug!("MockScanExecutor already initialized, resetting for reuse");
            // Reset the index to allow scanning again
            self.current_index = 0;
            return;
        }

        info!(
            "Initializing MockScanExecutor for table '{}'",
            self.plan.get_table_name()
        );

        // Generate mock data during initialization
        self.mock_tuples = self.generate_mock_data();
        self.current_index = 0;
        self.initialized = true;
    }

    /// Returns the next mock tuple.
    ///
    /// Iterates through the generated mock tuples sequentially,
    /// returning each tuple exactly once per scan.
    ///
    /// # Returns
    ///
    /// * `Ok(Some((tuple, rid)))` - Next mock tuple
    /// * `Ok(None)` - All 3 mock tuples have been returned
    ///
    /// # Auto-Initialization
    ///
    /// If `init()` was not called, it will be called automatically
    /// on the first `next()` invocation.
    ///
    /// # Tuple Cloning
    ///
    /// Tuples are cloned (Arc clone) on each call, allowing the
    /// executor to be rescanned after calling `init()` again.
    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if !self.initialized {
            debug!("MockScanExecutor not initialized, initializing now");
            self.init();
        }

        if self.current_index < self.mock_tuples.len() {
            let result = self.mock_tuples[self.current_index].clone();
            self.current_index += 1;
            debug!("Returning mock tuple with RID {:?}", result.1);
            Ok(Some(result))
        } else {
            info!("Reached end of mock scan");
            Ok(None)
        }
    }

    /// Returns the output schema from the mock scan plan.
    ///
    /// The schema is used both for mock data generation and by
    /// parent executors for tuple interpretation.
    ///
    /// # Returns
    ///
    /// Reference to the schema defined in the mock scan plan.
    fn get_output_schema(&self) -> &Schema {
        debug!("Getting output schema: {:?}", self.plan.get_output_schema());
        self.plan.get_output_schema()
    }

    /// Returns the shared execution context.
    ///
    /// While the mock scan executor doesn't actively use the context,
    /// it's required by the `AbstractExecutor` trait interface.
    ///
    /// # Returns
    ///
    /// Arc-wrapped RwLock-protected execution context.
    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.context.clone()
    }
}

#[cfg(test)]
mod tests {
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
    use crate::types_db::type_id::TypeId;
    use tempfile::TempDir;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<TransactionManager>,
        transaction_context: Arc<TransactionContext>,
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
            }
        }

        pub fn bpm(&self) -> Arc<BufferPoolManager> {
            Arc::clone(&self.bpm)
        }

        pub fn transaction_context(&self) -> Arc<TransactionContext> {
            self.transaction_context.clone()
        }
    }

    fn create_catalog(ctx: &TestContext) -> Catalog {
        Catalog::new(
            ctx.bpm.clone(),
            ctx.transaction_manager.clone(), // Add transaction manager
        )
    }

    #[tokio::test]
    async fn test_mock_scan_executor() {
        let ctx = TestContext::new("test_mock_scan_executor").await;

        // Create schema
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // Create catalog
        let catalog = Arc::new(RwLock::new(create_catalog(&ctx)));

        // Create mock scan plan
        let plan = Arc::new(MockScanNode::new(
            schema.clone(),
            "mock_table".to_string(),
            vec![],
        ));

        // Create executor context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            ctx.bpm(),
            catalog,
            ctx.transaction_context(),
        )));

        // Create and initialize executor
        let mut executor = MockScanExecutor::new(execution_context, plan.clone());
        executor.init();

        // Test scanning
        let mut tuple_count = 0;
        while let Ok(Some((_tuple, rid))) = executor.next() {
            // Verify tuple structure
            assert_eq!(plan.get_output_schema().get_column_count(), 2);
            assert!(rid.get_page_id() < 3); // We generate 3 mock tuples
            tuple_count += 1;
        }

        assert_eq!(tuple_count, 3, "Should have scanned 3 mock tuples");
    }

    #[tokio::test]
    async fn test_mock_scan_executor_reuse() {
        let ctx = TestContext::new("test_mock_scan_executor_reuse").await;
        let bpm = ctx.bpm();
        let transaction_context = ctx.transaction_context();

        // Create schema
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);

        // Create catalog using helper function
        let catalog = Arc::new(RwLock::new(create_catalog(&ctx)));

        // Create mock scan plan
        let plan = Arc::new(MockScanNode::new(
            schema.clone(),
            "mock_table".to_string(),
            vec![],
        ));

        // Create executor context
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            bpm,
            catalog,
            transaction_context,
        )));

        // Create executor
        let mut executor = MockScanExecutor::new(execution_context, plan);

        // First scan
        executor.init();
        let mut first_count = 0;
        while let Ok(Some(_)) = executor.next() {
            first_count += 1;
        }
        assert_eq!(first_count, 3, "First scan should return 3 tuples");

        // Reset and scan again
        executor.init();
        let mut second_count = 0;
        while let Ok(Some(_)) = executor.next() {
            second_count += 1;
        }
        assert_eq!(second_count, 3, "Second scan should also return 3 tuples");
    }
}
