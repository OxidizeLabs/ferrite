use crate::catalog::schema::Schema;
use crate::common::config::PageId;
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
    mock_tuples: Vec<(Tuple, RID)>,
    current_index: usize,
    initialized: bool,
}

impl MockScanExecutor {
    pub fn new(context: Arc<RwLock<ExecutionContext>>, plan: Arc<MockScanNode>) -> Self {
        debug!("Creating MockScanExecutor for table '{}'", plan.get_table_name());

        Self {
            context,
            plan,
            mock_tuples: Vec::new(),
            current_index: 0,
            initialized: false,
        }
    }

    // Helper method to generate mock data based on schema
    fn generate_mock_data(&self) -> Vec<(Tuple, RID)> {
        let schema = self.plan.get_output_schema();
        let mut mock_data = Vec::new();

        // Generate 3 mock tuples for testing
        for i in 0..3 {
            let mut values = Vec::new();
            for (_col_idx, column) in schema.get_columns().iter().enumerate() {
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
            let tuple = Tuple::new(&values, schema.clone(), rid);
            mock_data.push((tuple, rid));
        }

        mock_data
    }
}

impl AbstractExecutor for MockScanExecutor {
    fn init(&mut self) {
        if self.initialized {
            debug!("MockScanExecutor already initialized");
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

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if !self.initialized {
            debug!("MockScanExecutor not initialized, initializing now");
            self.init();
        }

        if self.current_index < self.mock_tuples.len() {
            let result = self.mock_tuples[self.current_index].clone();
            self.current_index += 1;
            debug!("Returning mock tuple with RID {:?}", result.1);
            Some(result)
        } else {
            info!("Reached end of mock scan");
            None
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
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::catalog::Catalog;
    use crate::catalog::column::Column;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::recovery::log_manager::LogManager;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use chrono::Utc;
    use std::collections::HashMap;
    use std::fs;
    use crate::sql::execution::transaction_context::TransactionContext;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<RwLock<TransactionManager>>,
        transaction_context: Arc<TransactionContext>,
        lock_manager: Arc<LockManager>,
        db_file: String,
        db_log_file: String,
    }

    impl TestContext {
        pub fn new(test_name: &str) -> Self {
            const BUFFER_POOL_SIZE: usize = 5;
            const K: usize = 2;

            let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
            let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
            let db_log_file = format!("tests/data/{}_{}.log", test_name, timestamp);

            let disk_manager = Arc::new(FileDiskManager::new(
                db_file.clone(),
                db_log_file.clone(),
                100,
            ));
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

            let catalog = Arc::new(RwLock::new(Catalog::new(
                bpm.clone(),
                0,
                0,
                Default::default(),
                Default::default(),
                Default::default(),
                Default::default(),
            )));

            let log_manager = Arc::new(RwLock::new(LogManager::new(Arc::clone(&disk_manager))));
            let transaction_manager = Arc::new(RwLock::new(TransactionManager::new(catalog, log_manager)));
            let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));

            let lock_manager = Arc::new(LockManager::new(Arc::clone(&transaction_manager.clone())));

            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            Self {
                bpm,
                transaction_manager,
                transaction_context,
                lock_manager,
                db_file,
                db_log_file,
            }
        }

        pub fn bpm(&self) -> Arc<BufferPoolManager> {
            Arc::clone(&self.bpm)
        }

        pub fn lock_manager(&self) -> Arc<LockManager> {
            Arc::clone(&self.lock_manager)
        }

        pub fn transaction_context(&self) -> Arc<TransactionContext> {
            self.transaction_context.clone()
        }

        fn cleanup(&self) {
            let _ = fs::remove_file(&self.db_file);
            let _ = fs::remove_file(&self.db_log_file);
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            self.cleanup();
        }
    }

    #[test]
    fn test_mock_scan_executor() {
        let ctx = TestContext::new("test_mock_scan_executor");

        // Create schema
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // Create catalog
        let catalog = Arc::new(RwLock::new(Catalog::new(
            ctx.bpm.clone(),
            0,
            0,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )));

        // Create transaction
        let txn = Arc::new(Transaction::new(0, IsolationLevel::Serializable));

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
            ctx.transaction_context()
        )));

        // Create and initialize executor
        let mut executor = MockScanExecutor::new(execution_context, plan.clone());
        executor.init();

        // Test scanning
        let mut tuple_count = 0;
        while let Some((_tuple, rid)) = executor.next() {
            // Verify tuple structure
            assert_eq!(plan.get_output_schema().get_column_count(), 2);
            assert!(rid.get_page_id() < 3); // We generate 3 mock tuples
            tuple_count += 1;
        }

        assert_eq!(tuple_count, 3, "Should have scanned 3 mock tuples");
    }

    #[test]
    fn test_mock_scan_executor_reuse() {
        let ctx = TestContext::new("test_mock_scan_executor_reuse");
        let bpm = ctx.bpm();
        let transaction_context = ctx.transaction_context();

        // Create schema
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);

        // Create catalog
        let catalog = Arc::new(RwLock::new(Catalog::new(
            ctx.bpm.clone(),
            0,
            0,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )));

        // Create transaction
        let txn = Arc::new(Transaction::new(0, IsolationLevel::Serializable));

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
            transaction_context
        )));

        // Create executor
        let mut executor = MockScanExecutor::new(execution_context, plan);

        // First scan
        executor.init();
        let first_count = executor.next().into_iter().count();
        assert_eq!(first_count, 1, "First scan should return 3 tuples");

        // Reset and scan again
        executor.init();
        let second_count = executor.next().into_iter().count();
        assert_eq!(second_count, 1, "Second scan should also return 3 tuples");
    }
}