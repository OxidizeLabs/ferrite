use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::limit_plan::LimitNode;
use crate::storage::table::tuple::Tuple;
use log::debug;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct LimitExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<LimitNode>,
    current_index: usize,
    initialized: bool,
    child_executor: Option<Box<dyn AbstractExecutor>>,
}

impl LimitExecutor {
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
    fn init(&mut self) {
        if !self.initialized {
            // Initialize child executor
            if let Some(child) = &mut self.child_executor {
                child.init();
            }
            self.initialized = true;
        }
    }

    fn next(&mut self) -> Option<(Arc<Tuple>, RID)> {
        // Check if we've reached the limit
        if self.current_index >= self.plan.get_limit() {
            return None;
        }

        // Get next tuple from child
        if let Some(child) = &mut self.child_executor {
            // Use a non-recursive approach to get the next tuple
            match child.next() {
                Some(result) => {
                    self.current_index += 1;
                    Some(result)
                }
                None => None,
            }
        } else {
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
    use crate::sql::execution::executors::mock_executor::MockExecutor;
    use crate::sql::execution::plans::abstract_plan::PlanNode;
    use crate::sql::execution::plans::mock_scan_plan::MockScanNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use std::collections::HashMap;
    use tempfile::TempDir;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<TransactionManager>,
        transaction_context: Arc<TransactionContext>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub fn new(name: &str) -> Self {
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
            let disk_manager = Arc::new(FileDiskManager::new(db_path, log_path, 10));
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(7, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

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

    #[test]
    fn test_limit_executor() {
        let ctx = TestContext::new("test_limit_executor");
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
        let result1 = limit_executor.next();
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

        let result2 = limit_executor.next();
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
        let result3 = limit_executor.next();
        assert!(
            result3.is_none(),
            "Expected third result to be None due to limit"
        );
    }

    #[test]
    fn test_limit_zero() {
        let ctx = TestContext::new("test_limit_zero");
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
        let result = limit_executor.next();
        assert!(result.is_none(), "Expected no results with limit of 0");
    }
}
