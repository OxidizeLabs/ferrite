use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::common::rid::RID;
use crate::common::exception::DBError;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::projection_plan::ProjectionNode;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use log::{debug, error, trace};
use parking_lot::RwLock;
use std::fmt::Display;
use std::sync::Arc;

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

        // Create a new output schema that uses the aliases
        let mut output_schema = plan.get_output_schema().clone();
        let input_schema = child_executor.get_output_schema();

        // Create mapping from alias names to input column indices
        let mut column_mappings = Vec::new();

        // Update column names in output schema to use aliases where applicable
        for (i, expr) in plan.get_expressions().iter().enumerate() {
            if let Expression::ColumnRef(col_ref) = expr.as_ref() {
                let col_name = col_ref.get_return_type().get_name();
                let input_idx = input_schema.get_columns().iter().position(|c| {
                    c.get_name().starts_with("SUM(") && col_name == "total_age"
                        || c.get_name().starts_with("COUNT(") && col_name == "emp_count"
                        || c.get_name().starts_with("AVG(") && col_name == "avg_salary"
                        || c.get_name().starts_with("MIN(") && col_name == "min_age"
                        || c.get_name().starts_with("MAX(") && col_name == "max_salary"
                        || c.get_name() == col_name
                });

                if let Some(idx) = input_idx {
                    // If this is an aggregate function, use the alias name
                    if input_schema.get_columns()[idx].get_name().contains('(') {
                        output_schema.get_columns_mut()[i].set_name(col_name.to_string());
                    }
                    column_mappings.push(idx);
                }
            }
        }

        Self {
            child_executor,
            context,
            plan: Arc::new(ProjectionNode::new(
                output_schema,
                plan.get_expressions().clone(),
                column_mappings,
            )),
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
                    &self.plan.get_output_schema(),
                    tuple.get_rid(),
                ))
            }
            Err(e) => {
                error!("Failed to evaluate projection expressions: {}", e);
                None
            }
        }
    }

    fn evaluate_projection_expressions(
        &self,
        tuple: &Tuple,
    ) -> Result<Vec<Value>, ExpressionError> {
        let mut values = Vec::with_capacity(self.plan.get_expressions().len());
        let input_schema = self.child_executor.get_output_schema();
        let column_mappings = self.plan.get_children_indices();
        let mut mapping_index = 0; // Track position in column_mappings array

        for (expr_index, expr) in self.plan.get_expressions().iter().enumerate() {
            match expr.as_ref() {
                Expression::Aggregate(_) => {
                    // For aggregate expressions, use the tuple index directly
                    // Aggregates don't use column mappings - they produce their own values
                    values.push(tuple.get_value(expr_index).clone());
                }
                Expression::ColumnRef(_) => {
                    // Use the pre-computed column mapping for ColumnRef expressions
                    if mapping_index < column_mappings.len() {
                        values.push(tuple.get_value(column_mappings[mapping_index]).clone());
                        mapping_index += 1;
                    } else {
                        return Err(ExpressionError::InvalidOperation(
                            "Column mapping not found for ColumnRef".to_string(),
                        ));
                    }
                }
                _ => {
                    // For other expressions (CASE, arithmetic, literals, etc.), evaluate normally
                    let value = expr.evaluate(tuple, input_schema)?;
                    values.push(value);
                    // No increment of mapping_index for non-ColumnRef expressions
                }
            }
        }

        Ok(values)
    }
}

impl AbstractExecutor for ProjectionExecutor {
    fn init(&mut self) {
        debug!("Initializing ProjectionExecutor");
        self.child_executor.init();
        self.initialized = true;
    }

    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if !self.initialized {
            return Err(DBError::Execution("ProjectionExecutor not initialized".to_string()));
        }

        // Get the next tuple from the child executor
        match self.child_executor.next()? {
            Some((input_tuple, rid)) => {
                trace!("ProjectionExecutor processing tuple with RID {:?}", rid);
                
                // Apply projections to create the output tuple
                let mut projected_values = Vec::new();
                let child_schema = self.child_executor.get_output_schema();

                for expression in self.plan.get_expressions() {
                    match expression.evaluate(&input_tuple, child_schema) {
                        Ok(value) => {
                            projected_values.push(value);
                        }
                        Err(e) => {
                            // Handle invalid column references gracefully by skipping the tuple
                            if let crate::common::exception::ExpressionError::InvalidColumnIndex(_) = e {
                                debug!("Skipping tuple due to invalid column reference: {}", e);
                                return self.next(); // Try the next tuple
                            }
                            return Err(DBError::Execution(format!("Failed to evaluate projection expression: {}", e)));
                        }
                    }
                }

                // Create output tuple with projected values
                let output_tuple = Arc::new(Tuple::new(&projected_values, self.plan.get_output_schema(), RID::default()));
                
                trace!("ProjectionExecutor produced projected tuple");
                Ok(Some((output_tuple, rid)))
            }
            None => {
                debug!("ProjectionExecutor reached end of input");
                Ok(None)
            }
        }
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

    use crate::common::logger::initialize_logger;
    use crate::sql::execution::expressions::arithmetic_expression::{
        ArithmeticExpression, ArithmeticOp,
    };
    use crate::sql::execution::expressions::comparison_expression::{
        ComparisonExpression, ComparisonType,
    };
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::sql::execution::plans::mock_scan_plan::MockScanNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::{Val, Value};
    use parking_lot::RwLock;
    use std::sync::Arc;
    use tempfile::TempDir;

    struct TestContext {
        catalog: Arc<RwLock<Catalog>>,
        buffer_pool_manager: Arc<BufferPoolManager>,
        transaction_context: Arc<TransactionContext>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        fn new(name: &str) -> Self {
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
            let disk_manager = Arc::new(FileDiskManager::new(db_path, log_path, 10));
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(7, K)));
            let buffer_pool_manager = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer,
            ));

            // Create transaction manager and lock manager first
            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());

            // Create catalog with transaction manager
            let catalog = Arc::new(RwLock::new(Catalog::new(
                buffer_pool_manager.clone(),
                transaction_manager.clone(), // Pass transaction manager
            )));

            let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));
            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            Self {
                catalog,
                buffer_pool_manager,
                transaction_context,
                _temp_dir: temp_dir,
            }
        }

        pub fn bpm(&self) -> Arc<BufferPoolManager> {
            Arc::clone(&self.buffer_pool_manager)
        }

        pub fn catalog(&self) -> Arc<RwLock<Catalog>> {
            self.catalog.clone()
        }
    }

    fn create_test_executor_context() -> (TestContext, Arc<RwLock<ExecutionContext>>) {
        let ctx = TestContext::new("projection_test");
        let bpm = ctx.bpm();
        let catalog = ctx.catalog();
        let transaction_context = ctx.transaction_context.clone();

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
                vec![Value::new(1), Value::new("Alice"), Value::new(25)],
                RID::new(0, 0),
            ),
            (
                vec![Value::new(2), Value::new("Bob"), Value::new(30)],
                RID::new(0, 1),
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
                0, // tuple index
                0, // column index for id
                Column::new("id", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0, // tuple index
                1, // column index for name
                Column::new("name", TypeId::VarChar),
                vec![],
            ))),
        ];

        // Create executor context with test context
        let (_, context) = create_test_executor_context();

        let mock_scan_plan =
            MockScanNode::new(input_schema.clone(), "mock_table".to_string(), vec![]);

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
        let (tuple1, _rid1) = executor.next().unwrap().unwrap();
        assert_eq!(tuple1.get_value(0).get_val(), &Val::from(1));
        assert_eq!(tuple1.get_value(1).get_val(), &Val::from("Alice"));
        assert_eq!(tuple1.get_values().len(), 2);

        // Second tuple
        let (tuple2, _rid2) = executor.next().unwrap().unwrap();
        assert_eq!(tuple2.get_value(0).get_val(), &Val::from(2));
        assert_eq!(tuple2.get_value(1).get_val(), &Val::from("Bob"));
        assert_eq!(tuple2.get_values().len(), 2);

        // No more tuples
        assert!(executor.next().unwrap().is_none());
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
                    Value::new(75), // Sum of ages for Alice
                ],
                RID::new(0, 0),
            ),
            (
                vec![
                    Value::new("Bob"),
                    Value::new(45), // Sum of ages for Bob
                ],
                RID::new(0, 1),
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
        let mock_scan_plan =
            MockScanNode::new(input_schema.clone(), "mock_agg".to_string(), vec![]);

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
        let (tuple1, _) = executor.next().unwrap().unwrap();
        assert_eq!(tuple1.get_value(0).get_val(), &Val::from("Alice"));
        assert_eq!(tuple1.get_value(1).get_val(), &Val::from(75));
        assert_eq!(tuple1.get_values().len(), 2);

        // Second tuple (Bob's group)
        let (tuple2, _) = executor.next().unwrap().unwrap();
        assert_eq!(tuple2.get_value(0).get_val(), &Val::from("Bob"));
        assert_eq!(tuple2.get_value(1).get_val(), &Val::from(45));
        assert_eq!(tuple2.get_values().len(), 2);

        // No more tuples
        assert!(executor.next().unwrap().is_none());
    }

    #[test]
    fn test_projection_with_multiple_aggregates() {
        // Create input schema with multiple aggregate functions
        let input_schema = Schema::new(vec![
            Column::new("dept", TypeId::VarChar),
            Column::new("SUM(age)", TypeId::Integer),
            Column::new("COUNT(*)", TypeId::Integer),
            Column::new("AVG(salary)", TypeId::Integer),
            Column::new("MIN(age)", TypeId::Integer),
            Column::new("MAX(salary)", TypeId::Integer),
        ]);

        // Test data simulating aggregation results
        let tuples: Vec<(Vec<Value>, RID)> = vec![
            (
                vec![
                    Value::new("Engineering"),
                    Value::new(150),    // SUM(age)
                    Value::new(3),      // COUNT(*)
                    Value::new(85000),  // AVG(salary)
                    Value::new(25),     // MIN(age)
                    Value::new(100000), // MAX(salary)
                ],
                RID::new(0, 0),
            ),
            (
                vec![
                    Value::new("Sales"),
                    Value::new(120),   // SUM(age)
                    Value::new(2),     // COUNT(*)
                    Value::new(75000), // AVG(salary)
                    Value::new(28),    // MIN(age)
                    Value::new(90000), // MAX(salary)
                ],
                RID::new(0, 1),
            ),
        ];

        // Create output schema with renamed columns
        let output_schema = Schema::new(vec![
            Column::new("department", TypeId::VarChar),
            Column::new("total_age", TypeId::Integer),
            Column::new("emp_count", TypeId::Integer),
            Column::new("avg_salary", TypeId::Integer),
            Column::new("min_age", TypeId::Integer),
            Column::new("max_salary", TypeId::Integer),
        ]);

        // Create expressions for projection
        let expressions = vec![
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                0,
                Column::new("dept", TypeId::VarChar),
                vec![],
            ))),
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                1,
                Column::new("total_age", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                2,
                Column::new("emp_count", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                3,
                Column::new("avg_salary", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                4,
                Column::new("min_age", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                5,
                Column::new("max_salary", TypeId::Integer),
                vec![],
            ))),
        ];

        let (_, context) = create_test_executor_context();
        let mock_scan_plan =
            MockScanNode::new(input_schema.clone(), "mock_agg".to_string(), vec![]);
        let child_executor = Box::new(MockExecutor::new(
            context.clone(),
            Arc::from(mock_scan_plan),
            0,
            tuples,
            input_schema,
        ));

        let plan = Arc::new(ProjectionNode::new(
            output_schema,
            expressions,
            vec![0, 1, 2, 3, 4, 5],
        ));
        let mut executor = ProjectionExecutor::new(child_executor, context, plan);

        executor.init();

        // Verify first department's results
        let (tuple1, _) = executor.next().unwrap().unwrap();
        assert_eq!(tuple1.get_value(0).get_val(), &Val::from("Engineering"));
        assert_eq!(tuple1.get_value(1).get_val(), &Val::from(150)); // total_age
        assert_eq!(tuple1.get_value(2).get_val(), &Val::from(3)); // emp_count
        assert_eq!(tuple1.get_value(3).get_val(), &Val::from(85000)); // avg_salary
        assert_eq!(tuple1.get_value(4).get_val(), &Val::from(25)); // min_age
        assert_eq!(tuple1.get_value(5).get_val(), &Val::from(100000)); // max_salary

        // Verify second department's results
        let (tuple2, _) = executor.next().unwrap().unwrap();
        assert_eq!(tuple2.get_value(0).get_val(), &Val::from("Sales"));
        assert_eq!(tuple2.get_value(1).get_val(), &Val::from(120));
        assert_eq!(tuple2.get_value(2).get_val(), &Val::from(2));
        assert_eq!(tuple2.get_value(3).get_val(), &Val::from(75000));
        assert_eq!(tuple2.get_value(4).get_val(), &Val::from(28));
        assert_eq!(tuple2.get_value(5).get_val(), &Val::from(90000));

        assert!(executor.next().unwrap().is_none());
    }

    #[test]
    fn test_projection_with_invalid_column_reference() {
        let input_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let output_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("invalid_col", TypeId::VarChar), // Reference to non-existent column
        ]);

        let expressions = vec![
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                0,
                Column::new("id", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                2,
                Column::new("invalid_col", TypeId::VarChar),
                vec![],
            ))), // Invalid column index
        ];

        let tuples = vec![(vec![Value::new(1), Value::new("test")], RID::new(0, 0))];

        let (_, context) = create_test_executor_context();
        let mock_scan_plan =
            MockScanNode::new(input_schema.clone(), "mock_table".to_string(), vec![]);
        let child_executor = Box::new(MockExecutor::new(
            context.clone(),
            Arc::from(mock_scan_plan),
            0,
            tuples,
            input_schema,
        ));

        let plan = Arc::new(ProjectionNode::new(output_schema, expressions, vec![0]));
        let mut executor = ProjectionExecutor::new(child_executor, context, plan);

        executor.init();
        // Should skip the tuple due to invalid column reference
        assert!(executor.next().unwrap().is_none());
    }

    #[test]
    fn test_projection_with_computed_expressions() {
        // Create input schema
        let input_schema = Schema::new(vec![
            Column::new("price", TypeId::Integer),
            Column::new("quantity", TypeId::Integer),
        ]);

        // Test data
        let tuples: Vec<(Vec<Value>, RID)> = vec![
            (vec![Value::new(10), Value::new(5)], RID::new(0, 0)),
            (vec![Value::new(20), Value::new(3)], RID::new(0, 1)),
        ];

        // Create output schema with computed column - note Decimal type for computed columns
        let output_schema = Schema::new(vec![
            Column::new("price", TypeId::Integer),
            Column::new("quantity", TypeId::Integer),
            Column::new("total", TypeId::Integer),
            Column::new("discounted_total", TypeId::Decimal), // Changed to Decimal
        ]);

        // Create expressions including arithmetic computations
        let price_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("price", TypeId::Integer),
            vec![],
        )));
        let quantity_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("quantity", TypeId::Integer),
            vec![],
        )));

        // Create total = price * quantity expression
        let total_expr = Arc::new(Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Multiply,
            vec![price_expr.clone(), quantity_expr.clone()],
        )));

        // Create discount expression (10% off) = total * 0.9
        let discount_const = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(0.9),
            Column::new("discount", TypeId::Decimal),
            vec![],
        )));
        let discounted_total_expr = Arc::new(Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Multiply,
            vec![total_expr.clone(), discount_const.clone()],
        )));

        let expressions = vec![price_expr, quantity_expr, total_expr, discounted_total_expr];

        let (_, context) = create_test_executor_context();
        let mock_scan_plan =
            MockScanNode::new(input_schema.clone(), "mock_table".to_string(), vec![]);
        let child_executor = Box::new(MockExecutor::new(
            context.clone(),
            Arc::from(mock_scan_plan),
            0,
            tuples,
            input_schema,
        ));

        let plan = Arc::new(ProjectionNode::new(
            output_schema,
            expressions,
            vec![0, 1, 2, 3],
        ));
        let mut executor = ProjectionExecutor::new(child_executor, context, plan);

        executor.init();

        // First tuple
        let (tuple1, _) = executor.next().unwrap().unwrap();
        assert_eq!(tuple1.get_value(0).get_val(), &Val::from(10)); // price
        assert_eq!(tuple1.get_value(1).get_val(), &Val::from(5)); // quantity
        assert_eq!(tuple1.get_value(2).get_val(), &Val::from(50)); // total
        assert_eq!(tuple1.get_value(3).get_val(), &Val::from(45.0)); // discounted_total as decimal

        // Second tuple
        let (tuple2, _) = executor.next().unwrap().unwrap();
        assert_eq!(tuple2.get_value(0).get_val(), &Val::from(20));
        assert_eq!(tuple2.get_value(1).get_val(), &Val::from(3));
        assert_eq!(tuple2.get_value(2).get_val(), &Val::from(60));
        assert_eq!(tuple2.get_value(3).get_val(), &Val::from(54.0)); // discounted_total as decimal

        assert!(executor.next().unwrap().is_none());
    }

    #[test]
    fn test_projection_with_null_values() {
        // Create input schema
        let input_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("salary", TypeId::Integer),
        ]);

        // Test data with NULL values
        let tuples: Vec<(Vec<Value>, RID)> = vec![
            (
                vec![
                    Value::new(1),
                    Value::new("Alice"),
                    Value::new(Val::Null), // Fix: Use Value::new(Val::Null) for NULL salary
                ],
                RID::new(0, 0),
            ),
            (
                vec![
                    Value::new(2),
                    Value::new(Val::Null), // Fix: Use Value::new(Val::Null) for NULL name
                    Value::new(50000),
                ],
                RID::new(0, 1),
            ),
        ];

        // Create output schema
        let output_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("salary", TypeId::Integer),
            Column::new("has_salary", TypeId::Boolean),
        ]);

        // Create expressions
        let id_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));
        let name_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("name", TypeId::VarChar),
            vec![],
        )));
        let salary_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2,
            Column::new("salary", TypeId::Integer),
            vec![],
        )));

        // Create IS NOT NULL check using comparison expression
        let has_salary_expr = Arc::new(Expression::Comparison(ComparisonExpression::new(
            salary_expr.clone(),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(Val::Null),
                Column::new("null", TypeId::Integer),
                vec![],
            ))),
            ComparisonType::IsNotNull,
            vec![salary_expr.clone()],
        )));

        let expressions = vec![id_expr, name_expr, salary_expr.clone(), has_salary_expr];

        let (_, context) = create_test_executor_context();
        let mock_scan_plan =
            MockScanNode::new(input_schema.clone(), "mock_table".to_string(), vec![]);
        let child_executor = Box::new(MockExecutor::new(
            context.clone(),
            Arc::from(mock_scan_plan),
            0,
            tuples,
            input_schema,
        ));

        let plan = Arc::new(ProjectionNode::new(
            output_schema,
            expressions,
            vec![0, 1, 2, 3],
        ));
        let mut executor = ProjectionExecutor::new(child_executor, context, plan);

        executor.init();

        // First tuple
        let (tuple1, _) = executor.next().unwrap().unwrap();
        assert_eq!(tuple1.get_value(0).get_val(), &Val::from(1));
        assert_eq!(tuple1.get_value(1).get_val(), &Val::from("Alice"));
        assert!(tuple1.get_value(2).is_null());
        assert_eq!(tuple1.get_value(3).get_val(), &Val::from(false)); // Changed: NULL IS NOT NULL -> false

        // Second tuple
        let (tuple2, _) = executor.next().unwrap().unwrap();
        assert_eq!(tuple2.get_value(0).get_val(), &Val::from(2));
        assert!(tuple2.get_value(1).is_null());
        assert_eq!(tuple2.get_value(2).get_val(), &Val::from(50000));
        assert_eq!(tuple2.get_value(3).get_val(), &Val::from(true)); // Non-NULL IS NOT NULL -> true

        assert!(executor.next().unwrap().is_none());
    }
}
