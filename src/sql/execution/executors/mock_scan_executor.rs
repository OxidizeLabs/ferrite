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

pub struct MockScanExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<MockScanNode>,
    mock_tuples: Vec<(Arc<Tuple>, RID)>,
    current_index: usize,
    initialized: bool,
}

impl MockScanExecutor {
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

    // Helper method to generate mock data based on schema
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

    fn get_output_schema(&self) -> &Schema {
        debug!("Getting output schema: {:?}", self.plan.get_output_schema());
        self.plan.get_output_schema()
    }

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
