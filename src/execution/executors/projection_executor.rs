use crate::catalogue::schema::Schema;
use crate::common::rid::RID;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::execution::expressions::abstract_expression::ExpressionOps;
use crate::execution::plans::abstract_plan::AbstractPlanNode;
use crate::execution::plans::projection_plan::ProjectionNode;
use crate::storage::table::tuple::Tuple;
use log::{debug, error};
use parking_lot::RwLock;
use std::sync::Arc;

pub struct ProjectionExecutor {
    child_executor: Box<dyn AbstractExecutor>,
    context: Arc<RwLock<ExecutorContext>>,
    plan: Arc<ProjectionNode>,
    initialized: bool,
}

impl ProjectionExecutor {
    pub fn new(
        child_executor: Box<dyn AbstractExecutor>,
        context: Arc<RwLock<ExecutorContext>>,
        plan: Arc<ProjectionNode>,
    ) -> Self {
        debug!(
            "Creating ProjectionExecutor with {} expressions",
            plan.get_expressions().len()
        );

        Self {
            child_executor,
            context,
            plan,
            initialized: false,
        }
    }

    /// Projects values from input tuple according to projection expressions
    fn project_tuple(&self, tuple: &Tuple) -> Option<Tuple> {
        let expressions = self.plan.get_expressions();
        let mut values = Vec::with_capacity(expressions.len());

        // Evaluate each expression
        for expr in expressions {
            match expr.evaluate(tuple, &self.child_executor.get_output_schema()) {
                Ok(value) => values.push(value),
                Err(e) => {
                    error!("Failed to evaluate expression: {}", e);
                    return None;
                }
            }
        }

        // Create new tuple with projected values
        Some(Tuple::new(
            &values,
            self.plan.get_output_schema().clone(),
            tuple.get_rid(),
        ))
    }
}

impl AbstractExecutor for ProjectionExecutor {
    fn init(&mut self) {
        if self.initialized {
            debug!("ProjectionExecutor already initialized");
            return;
        }

        debug!("Initializing ProjectionExecutor");

        // Initialize child executor first
        self.child_executor.init();

        self.initialized = true;
        debug!("ProjectionExecutor initialization complete");
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if !self.initialized {
            debug!("ProjectionExecutor not initialized, initializing now");
            self.init();
        }

        // Get next tuple from child
        match self.child_executor.next() {
            Some((tuple, rid)) => {
                debug!("Processing tuple from child executor");

                // Project tuple values according to expressions
                match self.project_tuple(&tuple) {
                    Some(projected_tuple) => {
                        debug!("Successfully projected tuple");
                        Some((projected_tuple, rid))
                    }
                    None => {
                        debug!("Failed to project tuple, skipping");
                        self.next() // Try next tuple
                    }
                }
            }
            None => {
                debug!("No more tuples from child executor");
                None
            }
        }
    }

    fn get_output_schema(&self) -> Schema {
        self.plan.get_output_schema().clone()
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutorContext>> {
        self.context.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalogue::catalogue::Catalog;
    use crate::catalogue::column::Column;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::execution::executors::mock_executor::MockExecutor;
    use crate::execution::expressions::abstract_expression::Expression;
    use crate::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::execution::plans::abstract_plan::PlanNode;
    use crate::execution::plans::mock_scan_plan::MockScanNode;
    use crate::recovery::log_manager::LogManager;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::{Val, Value};
    use chrono::Utc;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;

    struct TestContext {
        catalog: Arc<RwLock<Catalog>>,
        buffer_pool_manager: Arc<BufferPoolManager>,
        transaction_manager: Arc<RwLock<TransactionManager>>,
        lock_manager: Arc<LockManager>,
        db_file: String,
        log_file: String,
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            let timestamp = Utc::now().timestamp();
            let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
            let log_file = format!("tests/data/{}_{}.log", test_name, timestamp);

            let disk_manager = Arc::new(FileDiskManager::new(
                db_file.clone(),
                log_file.clone(),
                100,
            ));
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(10, 2)));
            let buffer_pool_manager = Arc::new(BufferPoolManager::new(
                10,
                disk_scheduler,
                disk_manager.clone(),
                replacer,
            ));

            let log_manager = Arc::new(RwLock::new(LogManager::new(disk_manager)));
            let catalog = Arc::new(RwLock::new(Catalog::new(
                buffer_pool_manager.clone(),
                0,
                0,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )));
            let transaction_manager = Arc::new(RwLock::new(TransactionManager::new(
                catalog.clone(),
                log_manager,
            )));

            let lock_manager = Arc::new(LockManager::new(transaction_manager.clone()));

            Self {
                catalog,
                buffer_pool_manager,
                transaction_manager,
                lock_manager,
                db_file,
                log_file,
            }
        }

        fn cleanup(&self) {
            let _ = fs::remove_file(&self.db_file);
            let _ = fs::remove_file(&self.log_file);
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            self.cleanup();
        }
    }

    fn create_test_executor_context() -> (TestContext, Arc<RwLock<ExecutorContext>>) {
        let test_context = TestContext::new("projection_test");

        let transaction = Arc::new(Transaction::new(1, IsolationLevel::ReadCommitted));
        let executor_context = Arc::new(RwLock::new(ExecutorContext::new(
            transaction,
            Arc::clone(&test_context.transaction_manager),
            Arc::clone(&test_context.catalog),
            Arc::clone(&test_context.buffer_pool_manager),
            Arc::clone(&test_context.lock_manager),
        )));

        (test_context, executor_context)
    }

    #[test]
    fn test_projection_with_mock_data() {
        // Create input schema
        let input_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ]);

        // Create test data in the format expected by MockExecutor
        let tuples: Vec<(Vec<Value>, RID)> = vec![
            (
                vec![
                    Value::new(1),
                    Value::new("Alice"),
                    Value::new(25),
                ],
                RID::new(0, 0)
            ),
            (
                vec![
                    Value::new(2),
                    Value::new("Bob"),
                    Value::new(30),
                ],
                RID::new(0, 1)
            ),
        ];

        // Create output schema (projecting only id and name)
        let output_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // Create column reference expressions
        let expressions = vec![
            Expression::ColumnRef(ColumnRefExpression::new(
                0,  // tuple index
                0,  // column index for id
                Column::new("id", TypeId::Integer),
                vec![]
            )),
            Expression::ColumnRef(ColumnRefExpression::new(
                0,  // tuple index
                1,  // column index for name
                Column::new("name", TypeId::VarChar),
                vec![]
            )),
        ];

        // Create executor context with test context
        let (_, context) = create_test_executor_context();

        let mock_scan_plan = MockScanNode::new(input_schema.clone(), "mock_table".to_string(), vec![]);

        // Create child executor with correct parameters
        let child_executor = Box::new(MockExecutor::new(
            context.clone(),
            Arc::from(mock_scan_plan),
            0,
            tuples,
            input_schema,
        ));

        // Create projection plan
        let plan = Arc::new(ProjectionNode::new(
            output_schema,
            expressions,
            *Box::new(PlanNode::Empty),
        ));

        // Create projection executor
        let mut executor = ProjectionExecutor::new(child_executor, context, plan);

        // Initialize and get results
        executor.init();

        // First tuple
        let (tuple1, _rid1) = executor.next().unwrap();
        assert_eq!(tuple1.get_value(0).get_value(), &Val::from(1));
        assert_eq!(tuple1.get_value(1).get_value(), &Val::from("Alice"));
        assert_eq!(tuple1.get_values().len(), 2);

        // Second tuple
        let (tuple2, _rid2) = executor.next().unwrap();
        assert_eq!(tuple2.get_value(0).get_value(), &Val::from(2));
        assert_eq!(tuple2.get_value(1).get_value(), &Val::from("Bob"));
        assert_eq!(tuple2.get_values().len(), 2);

        // No more tuples
        assert!(executor.next().is_none());
    }
}