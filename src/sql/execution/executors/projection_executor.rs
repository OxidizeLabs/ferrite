use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::projection_plan::ProjectionNode;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use log::{debug, error};
use parking_lot::RwLock;
use std::fmt::Display;
use std::sync::Arc;
use crate::sql::execution::expressions::aggregate_expression::{AggregateExpression, AggregationType};

pub struct ProjectionExecutor {
    child_executor: Box<dyn AbstractExecutor>,
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<ProjectionNode>,
    initialized: bool,
}

impl ProjectionExecutor {
    pub fn new(
        child_executor: Box<dyn AbstractExecutor>,
        context: Arc<RwLock<ExecutionContext>>,
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
        // Evaluate expressions and handle errors
        match self.evaluate_projection_expressions(tuple) {
            Ok(values) => {
                if values.is_empty() {
                    debug!("No values after projection, skipping tuple");
                    return None;
                }

                // Create new tuple with projected values
                Some(Tuple::new(
                    &values,
                    self.plan.get_output_schema().clone(),
                    tuple.get_rid(),
                ))
            }
            Err(e) => {
                error!("Failed to evaluate projection expressions: {}", e);
                None
            }
        }
    }

    fn evaluate_projection_expressions(&self, tuple: &Tuple) -> Result<Vec<Value>, ExpressionError> {
        let mut values = Vec::with_capacity(self.plan.get_expressions().len());
        let input_schema = self.child_executor.get_output_schema();
        
        for expr in self.plan.get_expressions() {
            match expr.as_ref() {
                Expression::Aggregate(_) => {
                    // For aggregate expressions, just pass through the value
                    // The AggregationExecutor has already computed the result
                    let idx = values.len();
                    values.push(tuple.get_value(idx).clone());
                }
                Expression::ColumnRef(col_ref) => {
                    // For column references in aggregation results, use the column index
                    // from the input schema's matching column name
                    let col_name = col_ref.get_return_type().get_name();
                    let idx = input_schema
                        .get_columns()
                        .iter()
                        .position(|c| c.get_name() == col_name)
                        .ok_or_else(|| {
                            ExpressionError::InvalidOperation(format!(
                                "Column {} not found in input schema",
                                col_name
                            ))
                        })?;
                    values.push(tuple.get_value(idx).clone());
                }
                _ => {
                    // For other expressions, evaluate normally
                    let value = expr.evaluate(tuple, input_schema)?;
                    values.push(value);
                }
            }
        }
        
        Ok(values)
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
        while let Some((tuple, rid)) = self.child_executor.next() {
            debug!("Processing tuple from child executor");

            // Project tuple values according to expressions
            if let Some(projected_tuple) = self.project_tuple(&tuple) {
                debug!("Successfully projected tuple");
                return Some((projected_tuple, rid));
            }
            debug!("Failed to project tuple, trying next one");
        }

        debug!("No more tuples from child executor");
        None
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.context.clone()
    }
}

impl Display for ProjectionExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProjectionExecutor")
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
    use crate::sql::execution::expressions::abstract_expression::Expression;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;

    use crate::recovery::log_manager::LogManager;
    use crate::sql::execution::plans::mock_scan_plan::MockScanNode;
    use crate::sql::execution::transaction_context::TransactionContext;
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
        transaction_manager: Arc<TransactionManager>,
        transaction_context: Arc<TransactionContext>,
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
            let transaction_manager = Arc::new(TransactionManager::new(
                log_manager,
            ));

            let lock_manager = Arc::new(LockManager::new(transaction_manager.clone()));

            let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));

            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            Self {
                catalog,
                buffer_pool_manager,
                transaction_manager,
                transaction_context,
                lock_manager,
                db_file,
                log_file,
            }
        }

        pub fn bpm(&self) -> Arc<BufferPoolManager> {
            Arc::clone(&self.buffer_pool_manager)
        }

        pub fn lock_manager(&self) -> Arc<LockManager> {
            Arc::clone(&self.lock_manager)
        }

        pub fn catalog(&self) -> Arc<RwLock<Catalog>> {
            self.catalog.clone()
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

    fn create_test_executor_context() -> (TestContext, Arc<RwLock<ExecutionContext>>) {
        let ctx = TestContext::new("projection_test");
        let bpm = ctx.bpm();
        let lock_manager = ctx.lock_manager();
        let catalog = ctx.catalog();
        let transaction_manager = ctx.transaction_manager.clone();
        let transaction_context = ctx.transaction_context.clone();


        let transaction = Arc::new(Transaction::new(1, IsolationLevel::ReadCommitted));
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            Arc::clone(&bpm),
            catalog,
            transaction_context,
        )));

        (ctx, execution_context)
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
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,  // tuple index
                0,  // column index for id
                Column::new("id", TypeId::Integer),
                vec![]
            ))),
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,  // tuple index
                1,  // column index for name
                Column::new("name", TypeId::VarChar),
                vec![]
            ))),
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
        let plan = Arc::new(ProjectionNode::new(output_schema, expressions, vec![]));

        // Create projection executor
        let mut executor = ProjectionExecutor::new(child_executor, context, plan);

        // Initialize and get results
        executor.init();

        // First tuple
        let (tuple1, _rid1) = executor.next().unwrap();
        assert_eq!(tuple1.get_value(0).get_val(), &Val::from(1));
        assert_eq!(tuple1.get_value(1).get_val(), &Val::from("Alice"));
        assert_eq!(tuple1.get_values().len(), 2);

        // Second tuple
        let (tuple2, _rid2) = executor.next().unwrap();
        assert_eq!(tuple2.get_value(0).get_val(), &Val::from(2));
        assert_eq!(tuple2.get_value(1).get_val(), &Val::from("Bob"));
        assert_eq!(tuple2.get_values().len(), 2);

        // No more tuples
        assert!(executor.next().is_none());
    }

    #[test]
    fn test_projection_over_aggregation() {
        // Create input schema (simulating aggregation output)
        let input_schema = Schema::new(vec![
            Column::new("name", TypeId::VarChar),
            Column::new("SUM(age)", TypeId::Integer),
        ]);

        // Create test data simulating aggregation results
        let tuples: Vec<(Vec<Value>, RID)> = vec![
            (
                vec![
                    Value::new("Alice"),
                    Value::new(75),  // Sum of ages for Alice
                ],
                RID::new(0, 0)
            ),
            (
                vec![
                    Value::new("Bob"),
                    Value::new(45),  // Sum of ages for Bob
                ],
                RID::new(0, 1)
            ),
        ];

        // Create output schema for projection
        let output_schema = Schema::new(vec![
            Column::new("name", TypeId::VarChar),
            Column::new("total_age", TypeId::Integer),
        ]);

        // Create expressions for projection
        let expressions = vec![
            // Project the name column - use exact same name as input
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                0,
                Column::new("name", TypeId::VarChar),
                vec![],
            ))),
            // Project the aggregated age column - use exact same name as input
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                1,
                Column::new("SUM(age)", TypeId::Integer), // Match input schema name
                vec![],
            ))),
        ];

        // Create executor context
        let (_, context) = create_test_executor_context();

        // Create mock scan plan to simulate aggregation output
        let mock_scan_plan = MockScanNode::new(input_schema.clone(), "mock_agg".to_string(), vec![]);

        // Create child executor (simulating aggregation executor)
        let child_executor = Box::new(MockExecutor::new(
            context.clone(),
            Arc::from(mock_scan_plan),
            0,
            tuples,
            input_schema,
        ));

        // Create projection plan with output schema that renames SUM(age) to total_age
        let plan = Arc::new(ProjectionNode::new(output_schema, expressions, vec![]));

        // Create projection executor
        let mut executor = ProjectionExecutor::new(child_executor, context, plan);

        // Initialize and get results
        executor.init();

        // First tuple (Alice's group)
        let (tuple1, _) = executor.next().unwrap();
        assert_eq!(tuple1.get_value(0).get_val(), &Val::from("Alice"));
        assert_eq!(tuple1.get_value(1).get_val(), &Val::from(75));
        assert_eq!(tuple1.get_values().len(), 2);

        // Second tuple (Bob's group)
        let (tuple2, _) = executor.next().unwrap();
        assert_eq!(tuple2.get_value(0).get_val(), &Val::from("Bob"));
        assert_eq!(tuple2.get_value(1).get_val(), &Val::from(45));
        assert_eq!(tuple2.get_values().len(), 2);

        // No more tuples
        assert!(executor.next().is_none());
    }
}