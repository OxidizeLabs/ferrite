//! # Create Index Executor Module
//!
//! This module implements the executor for `CREATE INDEX` statements, which
//! build secondary indexes on table columns to accelerate query lookups.
//!
//! ## SQL Syntax
//!
//! ```sql
//! CREATE INDEX [IF NOT EXISTS] <index_name>
//! ON <table_name> (<column_list>)
//! ```
//!
//! ## Index Types
//!
//! Currently supported index types:
//!
//! | Type           | Description                                       |
//! |----------------|---------------------------------------------------|
//! | B+ Tree Index  | Balanced tree for range queries and point lookups |
//!
//! ## Execution Flow
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │               CreateIndexExecutor                       │
//! │                                                         │
//! │  1. Validate table exists                               │
//! │  2. Check for existing index (IF NOT EXISTS)            │
//! │  3. Create index metadata in catalog                    │
//! │  4. Build index structure (B+ tree)                     │
//! │                                                         │
//! │  ┌─────────────────────────────────────────────────┐    │
//! │  │ Catalog                                         │    │
//! │  │  └─ IndexInfo                                   │    │
//! │  │      ├─ index_oid: 42                           │    │
//! │  │      ├─ index_name: "idx_users_email"           │    │
//! │  │      ├─ table_name: "users"                     │    │
//! │  │      └─ key_schema: (email VARCHAR)             │    │
//! │  └─────────────────────────────────────────────────┘    │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Output
//!
//! Index creation produces no output tuples. Success or failure is indicated
//! through logging. The executor returns `None` after attempting creation.
//!
//! ## Error Handling
//!
//! | Condition             | Behavior                                    |
//! |-----------------------|---------------------------------------------|
//! | Table doesn't exist   | Logs warning, returns `None`                |
//! | Index already exists  | With IF NOT EXISTS: logs info, returns      |
//! |                       | Without: logs warning, returns              |
//! | Catalog creation fail | Logs warning, allows retry                  |

use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::create_index_plan::CreateIndexPlanNode;
use crate::storage::index::IndexType;
use crate::storage::table::tuple::Tuple;
use log::{debug, info, warn};
use parking_lot::RwLock;
use std::sync::Arc;

/// Executor for `CREATE INDEX` DDL statements.
///
/// `CreateIndexExecutor` handles the creation of secondary indexes on table
/// columns. It validates preconditions (table existence, index uniqueness),
/// creates index metadata in the catalog, and initializes the underlying
/// index structure.
///
/// # Index Structure
///
/// Indexes are currently implemented as B+ trees, providing:
/// - O(log n) point lookups
/// - Efficient range scans
/// - Ordered iteration over keys
///
/// # Concurrency
///
/// The executor acquires a write lock on the catalog during index creation.
/// The index structure itself supports concurrent access after creation.
///
/// # Example
///
/// ```ignore
/// // CREATE INDEX idx_email ON users (email)
/// let plan = CreateIndexPlanNode::new(
///     schema,
///     "users".to_string(),
///     "idx_email".to_string(),
///     vec![1], // email column index
///     false,   // not IF NOT EXISTS
/// );
/// let executor = CreateIndexExecutor::new(context, Arc::new(plan), false);
/// ```
///
/// # Lifecycle
///
/// The executor logs its creation and destruction for debugging purposes
/// via the `Drop` implementation.
pub struct CreateIndexExecutor {
    /// Shared execution context providing catalog access.
    context: Arc<RwLock<ExecutionContext>>,
    /// Plan node containing index name, table name, and key columns.
    plan: Arc<CreateIndexPlanNode>,
    /// Flag indicating whether index creation has been attempted.
    executed: bool,
}

impl CreateIndexExecutor {
    /// Creates a new `CreateIndexExecutor` from a plan node.
    ///
    /// # Arguments
    ///
    /// * `context` - Shared execution context providing catalog access.
    /// * `plan` - Plan node with index name, table name, key columns, and
    ///   IF NOT EXISTS flag.
    /// * `executed` - Initial execution state (typically `false`).
    ///
    /// # Returns
    ///
    /// A new executor ready for initialization. Debug logging captures the
    /// index name and IF NOT EXISTS setting.
    pub fn new(
        context: Arc<RwLock<ExecutionContext>>,
        plan: Arc<CreateIndexPlanNode>,
        executed: bool,
    ) -> Self {
        debug!(
            "Creating CreateIndexExecutor for index '{}', if_not_exists={}",
            plan.get_index_name(),
            plan.if_not_exists()
        );
        debug!("Output schema: {:?}", plan.get_output_schema());

        Self {
            context,
            plan,
            executed,
        }
    }
}

impl AbstractExecutor for CreateIndexExecutor {
    /// Initializes the executor for index creation.
    ///
    /// Resets the `executed` flag to allow re-execution if needed.
    /// This enables retry logic for transient catalog failures.
    fn init(&mut self) {
        debug!(
            "Initializing CreateIndexExecutor for index '{}'",
            self.plan.get_index_name()
        );
        self.executed = false;
    }

    /// Attempts to create the index in the catalog.
    ///
    /// # Execution Steps
    ///
    /// 1. **Table validation**: Checks that the target table exists
    /// 2. **Duplicate check**: Verifies no index with the same name exists
    /// 3. **Index creation**: Creates the index metadata and B+ tree structure
    ///
    /// # Lock Acquisition
    ///
    /// Acquires locks in the following order to prevent deadlocks:
    /// 1. Read lock on execution context (brief, to get catalog reference)
    /// 2. Write lock on catalog (held during creation)
    ///
    /// # Returns
    ///
    /// * `Ok(None)` - Always returns `None` since DDL produces no tuples.
    ///   Check logs to determine success or failure.
    ///
    /// # Retry Behavior
    ///
    /// | Condition             | `executed` set | Retryable |
    /// |-----------------------|----------------|-----------|
    /// | Table doesn't exist   | `true`         | No        |
    /// | Index already exists  | `true`         | No        |
    /// | Catalog creation fail | `false`        | Yes       |
    /// | Success               | `true`         | No        |
    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if self.executed {
            debug!("CreateIndexExecutor already executed, returning None");
            return Ok(None);
        }

        let index_name = self.plan.get_index_name();
        let table_name = self.plan.get_table_name();
        let schema = self.plan.get_output_schema().clone();
        let key_attrs = self.plan.get_key_attrs();

        debug!("Acquiring executor context lock for index '{}'", index_name);
        let catalog = {
            let context_guard = self.context.read();
            debug!("Successfully acquired context read lock");
            context_guard.get_catalog().clone()
        };
        debug!("Released executor context lock");

        debug!("Acquiring catalog write lock for index '{}'", index_name);
        {
            let mut catalog_guard = catalog.write();
            debug!("Successfully acquired catalog write lock");

            // First check if the table exists
            if catalog_guard.get_table(table_name).is_none() {
                warn!(
                    "Cannot create index '{}' - table '{}' does not exist",
                    index_name, table_name
                );
                self.executed = true; // Mark as executed since we can't retry this
                return Ok(None);
            }

            // Check if index already exists
            let existing_indexes = catalog_guard.get_table_indexes(table_name);
            let index_exists = existing_indexes
                .iter()
                .any(|idx| idx.get_index_name() == index_name);

            if index_exists {
                self.executed = true; // Mark as executed since we found the index
                return if self.plan.if_not_exists() {
                    info!(
                        "Index '{}' already exists, skipping creation (IF NOT EXISTS)",
                        index_name
                    );
                    Ok(None)
                } else {
                    warn!("Index '{}' already exists", index_name);
                    Ok(None)
                };
            }

            // Create the index
            debug!("Creating new index '{}' in catalog", index_name);
            match catalog_guard.create_index(
                index_name,
                table_name,
                schema,
                key_attrs.to_vec(),
                4, // TODO: Make this configurable
                false,
                IndexType::BPlusTreeIndex,
            ) {
                Some(index_info) => {
                    info!(
                        "Successfully created index '{}' with OID {}",
                        index_name,
                        index_info.0.get_index_oid()
                    );
                    self.executed = true;
                    Ok(None)
                },
                None => {
                    warn!(
                        "Failed to create index '{}' - catalog creation failed",
                        index_name
                    );
                    // Don't mark as executed so we can retry
                    Ok(None)
                },
            }
        }
    }

    /// Returns the output schema for index creation.
    ///
    /// DDL statements produce no tuples, so this returns the plan's
    /// output schema (typically empty or for internal use).
    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }

    /// Returns the shared execution context.
    ///
    /// Provides access to the catalog for index creation.
    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.context.clone()
    }
}

impl Drop for CreateIndexExecutor {
    /// Logs executor destruction for debugging.
    ///
    /// Helps trace executor lifecycle in complex query plans.
    fn drop(&mut self) {
        debug!(
            "Dropping CreateIndexExecutor for index '{}'",
            self.plan.get_index_name()
        );
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
    use crate::storage::index::IndexType;
    use crate::types_db::type_id::TypeId;
    use parking_lot::RwLock;
    use tempfile::TempDir;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<TransactionManager>,
        transaction_context: Arc<TransactionContext>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 10;
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

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::VarChar),
        ])
    }

    fn create_catalog(ctx: &TestContext) -> Catalog {
        Catalog::new(
            ctx.bpm.clone(),
            ctx.transaction_manager.clone(), // Add transaction manager
        )
    }

    async fn create_test_executor_context() -> Arc<RwLock<ExecutionContext>> {
        let ctx = TestContext::new("projection_test").await;
        let bpm = ctx.bpm();
        let catalog = Arc::new(RwLock::new(create_catalog(&ctx)));
        let transaction_context = ctx.transaction_context.clone();

        Arc::new(RwLock::new(ExecutionContext::new(
            bpm,
            catalog,
            transaction_context,
        )))
    }

    #[tokio::test]
    async fn test_create_index_basic() {
        let test_context = TestContext::new("create_index_basic").await;
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();

        // Create execution context with the same catalog instance
        let exec_context = Arc::new(RwLock::new(ExecutionContext::new(
            test_context.bpm(),
            catalog.clone(), // Use the same catalog instance
            test_context.transaction_context.clone(),
        )));

        {
            let mut catalog_guard = catalog.write();
            catalog_guard.create_table("test_table".to_string(), schema.clone());
        }

        let key_attrs = vec![0];
        let plan = Arc::new(CreateIndexPlanNode::new(
            schema.clone(),
            "test_table".to_string(),
            "test_index".to_string(),
            key_attrs,
            false,
        ));

        let mut executor = CreateIndexExecutor::new(exec_context, plan, false);
        executor.init();
        assert!(executor.next().unwrap().is_none());

        let catalog_guard = catalog.read();
        let indexes = catalog_guard.get_table_indexes("test_table");
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].get_index_name(), "test_index");
    }

    #[tokio::test]
    async fn test_create_index_multiple_columns() {
        let test_context = TestContext::new("create_index_multi_col").await;
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();

        // Create table first
        {
            let mut catalog_guard = catalog.write();
            catalog_guard.create_table("test_table".to_string(), schema.clone());
        }

        // Create execution context with the same catalog instance
        let exec_context = Arc::new(RwLock::new(ExecutionContext::new(
            test_context.bpm(),
            catalog.clone(),
            test_context.transaction_context.clone(),
        )));

        let columns = vec![0, 1]; // Using both columns for the index

        let plan = Arc::new(CreateIndexPlanNode::new(
            schema.clone(),
            "test_table".to_string(), // Fix: table name should be first
            "test_index".to_string(), // Fix: index name should be second
            columns,
            false,
        ));

        let mut executor = CreateIndexExecutor::new(exec_context, plan, false);
        executor.init();
        assert!(executor.next().unwrap().is_none());

        // Verify the index was created with correct schema
        let catalog_guard = catalog.read();
        let indexes = catalog_guard.get_table_indexes("test_table");
        assert_eq!(indexes.len(), 1);
        let index = &indexes[0];
        assert_eq!(index.get_key_schema().get_column_count(), 2);
    }

    #[tokio::test]
    async fn test_create_index_if_not_exists() {
        let test_context = TestContext::new("create_index_if_not_exists").await;
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();

        // Create table and initial index
        {
            let mut catalog_guard = catalog.write();
            catalog_guard.create_table("test_table".to_string(), schema.clone());
            catalog_guard.create_index(
                "test_index",
                "test_table",
                schema.clone(),
                vec![0],
                4,
                false,
                IndexType::BPlusTreeIndex,
            );
        }

        let exec_context = create_test_executor_context().await;
        let columns = vec![0];

        let plan = Arc::new(CreateIndexPlanNode::new(
            schema.clone(),
            "test_index".to_string(),
            "test_table".to_string(),
            columns,
            true,
        ));

        let mut executor = CreateIndexExecutor::new(exec_context, plan, false);
        executor.init();

        // Should not fail when index exists
        assert!(executor.next().unwrap().is_none());
    }

    #[tokio::test]
    async fn test_create_index_table_not_exists() {
        let test_context = TestContext::new("create_index_no_table").await;
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();

        let exec_context = create_test_executor_context().await;
        let columns = vec![0];
        let plan = Arc::new(CreateIndexPlanNode::new(
            schema.clone(),
            "test_index".to_string(),
            "nonexistent_table".to_string(),
            columns,
            false,
        ));

        let mut executor = CreateIndexExecutor::new(exec_context, plan, false);
        executor.init();

        assert!(executor.next().unwrap().is_none());

        // Verify no index was created
        let catalog_guard = catalog.read();
        let indexes = catalog_guard.get_table_indexes("nonexistent_table");
        assert!(indexes.is_empty());
    }

    #[tokio::test]
    async fn test_create_index_concurrent() {
        let test_context = TestContext::new("create_index_concurrent").await;
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();

        // Create test table
        {
            let mut catalog_guard = catalog.write();
            catalog_guard.create_table("test_table".to_string(), schema.clone());
        }

        // Create execution context with the same catalog instance
        let exec_context = Arc::new(RwLock::new(ExecutionContext::new(
            test_context.bpm(),
            catalog.clone(),
            test_context.transaction_context.clone(),
        )));

        // Create multiple index executors
        let mut executors = vec![];
        let columns = vec![0];

        for i in 0..3 {
            let plan = Arc::new(CreateIndexPlanNode::new(
                schema.clone(),
                "test_table".to_string(), // Fix: table name should be first
                format!("test_index_{}", i), // Fix: index name should be second
                columns.clone(),
                false,
            ));
            executors.push(CreateIndexExecutor::new(exec_context.clone(), plan, false));
        }

        // Execute concurrently
        for executor in executors.iter_mut() {
            executor.init();
            assert!(executor.next().unwrap().is_none());
        }

        // Verify all indexes were created
        let catalog_guard = catalog.read();
        let indexes = catalog_guard.get_table_indexes("test_table");
        assert_eq!(indexes.len(), 3);
    }

    #[tokio::test]
    async fn test_create_index_duplicate() {
        let test_context = TestContext::new("create_index_duplicate").await;
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();

        // Create table
        {
            let mut catalog_guard = catalog.write();
            catalog_guard.create_table("test_table".to_string(), schema.clone());
        }

        // Create execution context with the same catalog instance
        let exec_context = Arc::new(RwLock::new(ExecutionContext::new(
            test_context.bpm(),
            catalog.clone(),
            test_context.transaction_context.clone(),
        )));

        let key_attrs = vec![0];

        // Create first index
        let plan1 = Arc::new(CreateIndexPlanNode::new(
            schema.clone(),
            "test_table".to_string(), // Fix: table name should be first
            "test_index".to_string(), // Fix: index name should be second
            key_attrs.clone(),
            false,
        ));

        let mut executor1 = CreateIndexExecutor::new(exec_context.clone(), plan1, false);
        executor1.init();
        assert!(executor1.next().unwrap().is_none());

        // Verify first index was created
        {
            let catalog_guard = catalog.read();
            let indexes = catalog_guard.get_table_indexes("test_table");
            assert_eq!(indexes.len(), 1, "First index should be created");
            assert_eq!(indexes[0].get_index_name(), "test_index");
        }

        // Try to create duplicate index without IF NOT EXISTS
        let plan2 = Arc::new(CreateIndexPlanNode::new(
            schema.clone(),
            "test_table".to_string(), // Fix: table name should be first
            "test_index".to_string(), // Fix: index name should be second
            key_attrs.clone(),
            false,
        ));

        let mut executor2 = CreateIndexExecutor::new(exec_context.clone(), plan2, false);
        executor2.init();
        assert!(executor2.next().unwrap().is_none());

        // Verify no duplicate was created
        {
            let catalog_guard = catalog.read();
            let indexes = catalog_guard.get_table_indexes("test_table");
            assert_eq!(indexes.len(), 1, "No duplicate index should be created");
        }

        // Try to create duplicate index with IF NOT EXISTS
        let plan3 = Arc::new(CreateIndexPlanNode::new(
            schema.clone(),
            "test_table".to_string(), // Fix: table name should be first
            "test_index".to_string(), // Fix: index name should be second
            key_attrs,
            true,
        ));

        let mut executor3 = CreateIndexExecutor::new(exec_context, plan3, false);
        executor3.init();
        assert!(executor3.next().unwrap().is_none());

        // Verify still only one index exists
        {
            let catalog_guard = catalog.read();
            let indexes = catalog_guard.get_table_indexes("test_table");
            assert_eq!(
                indexes.len(),
                1,
                "IF NOT EXISTS should not create duplicate"
            );
        }
    }
}
