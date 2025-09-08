use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::create_table_plan::CreateTablePlanNode;
use crate::storage::table::tuple::Tuple;
use log::{debug, info};
use parking_lot::RwLock;
use std::sync::Arc;

pub struct CreateTableExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<CreateTablePlanNode>,
    executed: bool,
}

impl CreateTableExecutor {
    pub fn new(
        context: Arc<RwLock<ExecutionContext>>,
        plan: Arc<CreateTablePlanNode>,
        executed: bool,
    ) -> Self {
        debug!(
            "Creating CreateTableExecutor for table '{}', if_not_exists={}",
            plan.get_table_name(),
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

impl AbstractExecutor for CreateTableExecutor {
    fn init(&mut self) {
        debug!(
            "Initializing CreateTableExecutor for table '{}'",
            self.plan.get_table_name()
        );
        self.executed = false;
    }

    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if self.executed {
            debug!("CreateTableExecutor already executed, returning None");
            return Ok(None);
        }

        let table_name = self.plan.get_table_name();
        let schema = self.plan.get_output_schema().clone();

        debug!("Acquiring executor context lock for table '{}'", table_name);
        let catalog = {
            let context_guard = match self.context.try_read() {
                Some(guard) => {
                    debug!("Successfully acquired context read lock");
                    guard
                }
                None => {
                    return Err(DBError::Execution(
                        "Failed to acquire context read lock - lock contention detected"
                            .to_string(),
                    ));
                }
            };
            context_guard.get_catalog().clone()
        };
        debug!("Released executor context lock");

        debug!("Acquiring catalog write lock for table '{}'", table_name);
        {
            let mut catalog_guard = match catalog.try_write() {
                Some(guard) => {
                    debug!("Successfully acquired catalog write lock");
                    guard
                }
                None => {
                    return Err(DBError::Execution(
                        "Failed to acquire catalog write lock - lock contention detected"
                            .to_string(),
                    ));
                }
            };

            // Check existence first
            if self.plan.if_not_exists() && catalog_guard.get_table(table_name).is_some() {
                info!(
                    "Table '{}' already exists, skipping creation (IF NOT EXISTS)",
                    table_name
                );
                self.executed = true;
                return Ok(None);
            }

            // Create the table
            debug!("Creating new table '{}' in catalog", table_name);
            let table_info = catalog_guard.create_table(table_name.to_string(), schema);
            match table_info {
                Some(table_info) => {
                    info!(
                        "Successfully created table '{}' with OID {}",
                        table_name,
                        table_info.get_table_oidt()
                    );
                }
                None => {
                    // Return a proper error instead of panicking
                    return Err(DBError::Validation(format!(
                        "Failed to create table '{}' - constraints validation failed",
                        table_name
                    )));
                }
            }
        }
        debug!("Released catalog write lock");

        self.executed = true;
        debug!("CreateTableExecutor execution completed");
        Ok(None)
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.context.clone()
    }
}

impl Drop for CreateTableExecutor {
    fn drop(&mut self) {
        debug!(
            "Dropping CreateTableExecutor for table '{}'",
            self.plan.get_table_name()
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
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::types_db::type_id::TypeId;
    use tempfile::TempDir;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};

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
            let disk_manager = AsyncDiskManager::new(db_path.clone(), log_path.clone(), DiskManagerConfig::default()).await;
            let disk_manager_arc = Arc::new(disk_manager.unwrap());
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_manager_arc.clone(),
                replacer.clone(),
            ).unwrap());

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

    fn create_complex_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
            Column::new("salary", TypeId::Decimal),
            Column::new("created_at", TypeId::Timestamp),
            Column::new("is_active", TypeId::Boolean),
        ])
    }

    fn create_single_column_schema() -> Schema {
        Schema::new(vec![Column::new("single_col", TypeId::Integer)])
    }

    fn create_catalog(ctx: &TestContext) -> Catalog {
        Catalog::new(ctx.bpm.clone(), ctx.transaction_manager.clone())
    }

    fn setup_execution_context(
        test_context: &TestContext,
        catalog: Arc<RwLock<Catalog>>,
    ) -> Arc<RwLock<ExecutionContext>> {
        Arc::new(RwLock::new(ExecutionContext::new(
            test_context.bpm(),
            catalog,
            test_context.transaction_context.clone(),
        )))
    }

    #[tokio::test]
    async fn test_create_table_basic() {
        let test_context = TestContext::new("test_create_table_basic").await;
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();
        let exec_context = setup_execution_context(&test_context, catalog.clone());

        let plan = Arc::new(CreateTablePlanNode::new(
            schema.clone(),
            "test_table".to_string(),
            false,
        ));

        let mut executor = CreateTableExecutor::new(exec_context, plan, false);
        executor.init();
        assert!(executor.next().unwrap().is_none());

        let catalog_guard = catalog.read();
        let table = catalog_guard.get_table("test_table").unwrap();
        assert_eq!(table.get_table_name(), "test_table");
        assert_eq!(table.get_table_schema().get_columns().len(), 2);
    }

    #[tokio::test]
    async fn test_create_table_complex_schema() {
        let test_context = TestContext::new("test_create_table_complex").await;
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_complex_schema();
        let exec_context = setup_execution_context(&test_context, catalog.clone());

        let plan = Arc::new(CreateTablePlanNode::new(
            schema.clone(),
            "employees".to_string(),
            false,
        ));

        let mut executor = CreateTableExecutor::new(exec_context, plan, false);
        executor.init();
        assert!(executor.next().unwrap().is_none());

        let catalog_guard = catalog.read();
        let table = catalog_guard.get_table("employees").unwrap();
        assert_eq!(table.get_table_name(), "employees");
        assert_eq!(table.get_table_schema().get_columns().len(), 6);

        // Verify column types
        let binding = table.get_table_schema();
        let columns = binding.get_columns();
        assert_eq!(columns[0].get_type(), TypeId::Integer);
        assert_eq!(columns[1].get_type(), TypeId::VarChar);
        assert_eq!(columns[2].get_type(), TypeId::Integer);
        assert_eq!(columns[3].get_type(), TypeId::Decimal);
        assert_eq!(columns[4].get_type(), TypeId::Timestamp);
        assert_eq!(columns[5].get_type(), TypeId::Boolean);
    }

    #[tokio::test]
    async fn test_create_table_single_column() {
        let test_context = TestContext::new("test_create_table_single").await;
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_single_column_schema();
        let exec_context = setup_execution_context(&test_context, catalog.clone());

        let plan = Arc::new(CreateTablePlanNode::new(
            schema.clone(),
            "simple_table".to_string(),
            false,
        ));

        let mut executor = CreateTableExecutor::new(exec_context, plan, false);
        executor.init();
        assert!(executor.next().unwrap().is_none());

        let catalog_guard = catalog.read();
        let table = catalog_guard.get_table("simple_table").unwrap();
        assert_eq!(table.get_table_name(), "simple_table");
        assert_eq!(table.get_table_schema().get_columns().len(), 1);
        assert_eq!(
            table.get_table_schema().get_columns()[0].get_name(),
            "single_col"
        );
    }

    #[tokio::test]
    async fn test_create_table_if_not_exists_new_table() {
        let test_context = TestContext::new("test_create_table_if_not_exists_new").await;
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();
        let exec_context = setup_execution_context(&test_context, catalog.clone());

        let plan = Arc::new(CreateTablePlanNode::new(
            schema.clone(),
            "new_table".to_string(),
            true, // if_not_exists = true
        ));

        let mut executor = CreateTableExecutor::new(exec_context, plan, false);
        executor.init();
        assert!(executor.next().unwrap().is_none());

        let catalog_guard = catalog.read();
        let table = catalog_guard.get_table("new_table").unwrap();
        assert_eq!(table.get_table_name(), "new_table");
    }

    #[tokio::test]
    async fn test_create_table_if_not_exists_existing_table() {
        let test_context = TestContext::new("test_create_table_if_not_exists_existing").await;
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();
        let exec_context = setup_execution_context(&test_context, catalog.clone());

        // Pre-create the table
        {
            let mut catalog_guard = catalog.write();
            catalog_guard.create_table("existing_table".to_string(), schema.clone());
        }

        let plan = Arc::new(CreateTablePlanNode::new(
            schema.clone(),
            "existing_table".to_string(),
            true, // if_not_exists = true
        ));

        let mut executor = CreateTableExecutor::new(exec_context, plan, false);
        executor.init();
        assert!(executor.next().unwrap().is_none());

        // Should still exist and not panic
        let catalog_guard = catalog.read();
        let table = catalog_guard.get_table("existing_table").unwrap();
        assert_eq!(table.get_table_name(), "existing_table");
    }

    #[tokio::test]
    async fn test_create_table_duplicate_without_if_not_exists() {
        let test_context = TestContext::new("test_create_table_duplicate").await;
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();
        let exec_context = setup_execution_context(&test_context, catalog.clone());

        // Pre-create the table
        {
            let mut catalog_guard = catalog.write();
            catalog_guard.create_table("duplicate_table".to_string(), schema.clone());
        }

        let plan = Arc::new(CreateTablePlanNode::new(
            schema.clone(),
            "duplicate_table".to_string(),
            false, // if_not_exists = false
        ));

        let mut executor = CreateTableExecutor::new(exec_context, plan, false);
        executor.init();

        // This should return an error now instead of None
        let result = executor.next();
        assert!(result.is_err());

        // The table should still exist (the original one)
        let catalog_guard = catalog.read();
        let table = catalog_guard.get_table("duplicate_table").unwrap();
        assert_eq!(table.get_table_name(), "duplicate_table");
    }

    #[tokio::test]
    async fn test_create_table_multiple_tables() {
        let test_context = TestContext::new("test_create_table_multiple").await;
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let exec_context = setup_execution_context(&test_context, catalog.clone());

        let table_names = vec!["table1", "table2", "table3"];
        let schemas = vec![
            create_test_schema(),
            create_complex_schema(),
            create_single_column_schema(),
        ];

        // Create multiple tables
        for (name, schema) in table_names.iter().zip(schemas.iter()) {
            let plan = Arc::new(CreateTablePlanNode::new(
                schema.clone(),
                name.to_string(),
                false,
            ));

            let mut executor = CreateTableExecutor::new(exec_context.clone(), plan, false);
            executor.init();
            assert!(executor.next().unwrap().is_none());
        }

        // Verify all tables exist
        let catalog_guard = catalog.read();
        for name in table_names {
            let table = catalog_guard.get_table(name).unwrap();
            assert_eq!(table.get_table_name(), name);
        }
    }

    #[tokio::test]
    async fn test_create_table_executor_reuse() {
        let test_context = TestContext::new("test_create_table_executor_reuse").await;
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();
        let exec_context = setup_execution_context(&test_context, catalog.clone());

        let plan = Arc::new(CreateTablePlanNode::new(
            schema.clone(),
            "reuse_test".to_string(),
            true,
        ));

        let mut executor = CreateTableExecutor::new(exec_context, plan, false);

        // First execution
        executor.init();
        assert!(executor.next().unwrap().is_none());

        // Second call to next() should return None (already executed)
        assert!(executor.next().unwrap().is_none());

        // Re-initialize and try again
        executor.init();
        assert!(executor.next().unwrap().is_none());

        let catalog_guard = catalog.read();
        let table = catalog_guard.get_table("reuse_test").unwrap();
        assert_eq!(table.get_table_name(), "reuse_test");
    }

    #[tokio::test]
    async fn test_create_table_empty_table_name() {
        let test_context = TestContext::new("test_create_table_empty_name").await;
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();
        let exec_context = setup_execution_context(&test_context, catalog.clone());

        let plan = Arc::new(CreateTablePlanNode::new(
            schema.clone(),
            "".to_string(), // Empty table name
            false,
        ));

        let mut executor = CreateTableExecutor::new(exec_context, plan, false);
        executor.init();
        assert!(executor.next().unwrap().is_none());

        // The behavior here depends on the catalog implementation
        // It might create a table with empty name or handle it gracefully
    }

    #[tokio::test]
    async fn test_create_table_special_characters_in_name() {
        let test_context = TestContext::new("test_create_table_special_chars").await;
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();
        let exec_context = setup_execution_context(&test_context, catalog.clone());

        let special_names = vec![
            "table_with_underscores",
            "table123",
            "TABLE_UPPERCASE",
            "table-with-dashes",
            "table.with.dots",
        ];

        for name in special_names {
            let plan = Arc::new(CreateTablePlanNode::new(
                schema.clone(),
                name.to_string(),
                false,
            ));

            let mut executor = CreateTableExecutor::new(exec_context.clone(), plan, false);
            executor.init();
            assert!(executor.next().unwrap().is_none());

            let catalog_guard = catalog.read();
            let table = catalog_guard.get_table(name);
            // Note: Whether this succeeds depends on catalog's name validation
            if table.is_some() {
                assert_eq!(table.unwrap().get_table_name(), name);
            }
            drop(catalog_guard);
        }
    }

    #[tokio::test]
    async fn test_create_table_output_schema() {
        let test_context = TestContext::new("test_create_table_output_schema").await;
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_complex_schema();
        let exec_context = setup_execution_context(&test_context, catalog.clone());

        let plan = Arc::new(CreateTablePlanNode::new(
            schema.clone(),
            "schema_test".to_string(),
            false,
        ));

        let executor = CreateTableExecutor::new(exec_context, plan, false);

        // Test that output schema matches the input schema
        let output_schema = executor.get_output_schema();
        assert_eq!(
            output_schema.get_columns().len(),
            schema.get_columns().len()
        );

        for (i, column) in output_schema.get_columns().iter().enumerate() {
            let expected_column = &schema.get_columns()[i];
            assert_eq!(column.get_name(), expected_column.get_name());
            assert_eq!(column.get_type(), expected_column.get_type());
        }
    }

    #[tokio::test]
    async fn test_create_table_all_data_types() {
        let test_context = TestContext::new("test_create_table_all_types").await;
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let exec_context = setup_execution_context(&test_context, catalog.clone());

        let schema = Schema::new(vec![
            Column::new("col_integer", TypeId::Integer),
            Column::new("col_varchar", TypeId::VarChar),
            Column::new("col_decimal", TypeId::Decimal),
            Column::new("col_boolean", TypeId::Boolean),
            Column::new("col_timestamp", TypeId::Timestamp),
            Column::new("col_date", TypeId::Date),
        ]);

        let plan = Arc::new(CreateTablePlanNode::new(
            schema.clone(),
            "all_types_table".to_string(),
            false,
        ));

        let mut executor = CreateTableExecutor::new(exec_context, plan, false);
        executor.init();
        assert!(executor.next().unwrap().is_none());

        let catalog_guard = catalog.read();
        let table = catalog_guard.get_table("all_types_table").unwrap();
        let table_schema = table.get_table_schema();

        assert_eq!(table_schema.get_columns().len(), 6);

        let expected_types = [
            TypeId::Integer,
            TypeId::VarChar,
            TypeId::Decimal,
            TypeId::Boolean,
            TypeId::Timestamp,
            TypeId::Date,
        ];

        for (i, expected_type) in expected_types.iter().enumerate() {
            assert_eq!(table_schema.get_columns()[i].get_type(), *expected_type);
        }
    }

    #[tokio::test]
    async fn test_create_table_execution_context_access() {
        let test_context = TestContext::new("test_create_table_context_access").await;
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();
        let exec_context = setup_execution_context(&test_context, catalog.clone());

        let plan = Arc::new(CreateTablePlanNode::new(
            schema.clone(),
            "context_test".to_string(),
            false,
        ));

        let executor = CreateTableExecutor::new(exec_context.clone(), plan, false);

        // Test that we can access the execution context
        let retrieved_context = executor.get_executor_context();
        assert!(Arc::ptr_eq(&exec_context, &retrieved_context));
    }

    #[tokio::test]
    async fn test_create_table_concurrent_access() {
        let test_context = TestContext::new("test_create_table_concurrent").await;
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let schema = create_test_schema();
        let exec_context = setup_execution_context(&test_context, catalog.clone());

        // Test sequential creation of tables to verify lock behavior
        // First table
        let plan1 = Arc::new(CreateTablePlanNode::new(
            schema.clone(),
            "concurrent_table".to_string(),
            false,
        ));

        let mut executor1 = CreateTableExecutor::new(exec_context.clone(), plan1, false);
        executor1.init();
        assert!(executor1.next().unwrap().is_none());

        // Second table
        let plan = Arc::new(CreateTablePlanNode::new(
            schema.clone(),
            "concurrent_table2".to_string(),
            false,
        ));

        let mut executor = CreateTableExecutor::new(exec_context, plan, false);
        executor.init();
        assert!(executor.next().unwrap().is_none());

        // Verify both tables were created
        let catalog_guard = catalog.read();
        assert!(
            catalog_guard.get_table("concurrent_table").is_some(),
            "concurrent_table should have been created"
        );
        assert!(
            catalog_guard.get_table("concurrent_table2").is_some(),
            "concurrent_table2 should have been created"
        );
    }
}
