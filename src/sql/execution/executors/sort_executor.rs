use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::expressions::abstract_expression::ExpressionOps;
use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
use crate::sql::execution::plans::sort_plan::SortNode;
use crate::storage::table::tuple::Tuple;
use log::{debug, error};
use parking_lot::RwLock;
use std::sync::Arc;

pub struct SortExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    plan: Arc<SortNode>,
    child_executor: Option<Box<dyn AbstractExecutor>>,
    sorted_tuples: Vec<(Tuple, RID)>,
    current_index: usize,
    initialized: bool,
}

impl SortExecutor {
    pub fn new(
        child_executor: Box<dyn AbstractExecutor>,
        context: Arc<RwLock<ExecutionContext>>,
        plan: Arc<SortNode>,
    ) -> Self {
        debug!("Creating SortExecutor");

        Self {
            context,
            plan,
            child_executor: Some(child_executor),
            sorted_tuples: Vec::new(),
            current_index: 0,
            initialized: false,
        }
    }
}

impl AbstractExecutor for SortExecutor {
    fn init(&mut self) {
        if self.initialized {
            debug!("SortExecutor already initialized");
            return;
        }

        debug!("Initializing SortExecutor");

        // Initialize child executor
        if let Some(child) = &mut self.child_executor {
            child.init();

            // Collect all tuples
            let mut count = 0;
            while let Some((tuple, rid)) = child.next() {
                self.sorted_tuples.push((tuple, rid));
                count += 1;
            }
            debug!("Collected {} tuples for sorting", count);

            // Create comparison closure that captures only what it needs
            let order_bys = self.plan.get_order_bys().clone();
            let schema = self.plan.get_output_schema().clone();

            // Sort tuples based on order_by expressions
            self.sorted_tuples.sort_by(|(a, _), (b, _)| {
                for order_by in &order_bys {
                    match (order_by.evaluate(a, &schema), order_by.evaluate(b, &schema)) {
                        (Ok(va), Ok(vb)) => {
                            match va.partial_cmp(&vb) {
                                Some(ordering) if !ordering.is_eq() => return ordering,
                                None => {
                                    debug!("Cannot compare values: {:?} and {:?}", va, vb);
                                    return std::cmp::Ordering::Equal;
                                }
                                _ => continue,
                            }
                        }
                        (Err(e), _) | (_, Err(e)) => {
                            error!("Error evaluating expression: {}", e);
                            return std::cmp::Ordering::Equal;
                        }
                    }
                }
                std::cmp::Ordering::Equal
            });
            debug!("Finished sorting {} tuples", count);
        }

        self.current_index = 0;
        self.initialized = true;
        debug!("SortExecutor initialization complete");
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if !self.initialized {
            debug!("SortExecutor not initialized, initializing now");
            self.init();
        }

        if self.current_index >= self.sorted_tuples.len() {
            debug!("No more tuples to return");
            return None;
        }

        let result = self.sorted_tuples[self.current_index].clone();
        self.current_index += 1;
        debug!("Returning tuple {} of {}", self.current_index, self.sorted_tuples.len());
        Some(result)
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
    use crate::sql::execution::expressions::abstract_expression::Expression;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::plans::abstract_plan::PlanNode;
    use crate::sql::execution::plans::mock_scan_plan::MockScanNode;
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::types::Type;
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
            0,                               // next_index_oid
            0,                               // next_table_oid
            HashMap::new(),                  // tables
            HashMap::new(),                  // indexes
            HashMap::new(),                  // table_names
            HashMap::new(),                  // index_names
            ctx.transaction_manager.clone(), // Add transaction manager
        )
    }

    fn create_test_executor_context(
        test_context: &TestContext,
        catalog: Arc<RwLock<Catalog>>,
    ) -> Arc<RwLock<ExecutionContext>> {
        // Create a new transaction

        Arc::new(RwLock::new(ExecutionContext::new(
            test_context.bpm(),
            catalog,
            test_context.transaction_context.clone(),
        )))
    }

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ])
    }

    #[test]
    fn test_sort_executor() {
        let ctx = TestContext::new("test_sort_executor");
        let catalog = Arc::new(RwLock::new(create_catalog(&ctx)));
        let exec_ctx = create_test_executor_context(&ctx, Arc::clone(&catalog));

        let schema = create_test_schema();

        // Create mock data
        let mock_data = vec![
            (1, "Alice", 25),
            (3, "Charlie", 35),
            (2, "Bob", 30),
            (5, "Eve", 32),
            (4, "David", 28),
        ];

        // Create mock tuples with raw values as expected by MockExecutor
        let mock_tuples: Vec<(Vec<Value>, RID)> = mock_data
            .iter()
            .enumerate()
            .map(|(i, (id, name, age))| {
                (
                    vec![
                        Value::new(*id),
                        Value::new(name.to_string()),
                        Value::new(*age),
                    ],
                    RID::new(0, i as u32),
                )
            })
            .collect();

        // Create mock scan plan
        let mock_plan = MockScanNode::new(
            schema.clone(),
            "test_table".to_string(),
            vec![],
        ).with_tuples(mock_tuples.clone());

        // Create sort expression (sort by age)
        let age_col = schema.get_column(2).unwrap().clone();
        let age_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2,
            age_col,
            vec![],
        )));

        // Create sort plan
        let sort_plan = Arc::new(SortNode::new(
            schema.clone(),
            vec![age_expr],
            vec![PlanNode::MockScan(mock_plan.clone())],
        ));

        // Create mock executor
        let child_executor = Box::new(MockExecutor::new(
            exec_ctx.clone(),
            Arc::from(mock_plan),
            0,
            mock_tuples,
            schema.clone(),
        ));

        // Create and test sort executor
        let mut sort_executor = SortExecutor::new(child_executor, exec_ctx, sort_plan);
        sort_executor.init();

        // Collect results
        let mut results = Vec::new();
        while let Some((tuple, _)) = sort_executor.next() {
            let age: i32 = tuple.get_value(2).as_integer().unwrap();
            results.push(age);
        }

        // Verify results are sorted by age
        assert_eq!(results, vec![25, 28, 30, 32, 35]);
    }

    #[test]
    fn test_sort_executor_multiple_columns() {
        let ctx = TestContext::new("test_sort_executor_multiple");
        let catalog = Arc::new(RwLock::new(create_catalog(&ctx)));
        let exec_ctx = create_test_executor_context(&ctx, Arc::clone(&catalog));

        let schema = create_test_schema();

        // Create test data with duplicate ages
        let mock_data = vec![
            (1, "Alice", 30),   // Should be first due to name
            (2, "Bob", 30),     // Should be second due to name
            (3, "Charlie", 25),
            (4, "David", 35),
            (5, "Eve", 25),     // Should come before Charlie due to name
        ];

        let mock_tuples: Vec<(Vec<Value>, RID)> = mock_data
            .iter()
            .enumerate()
            .map(|(i, (id, name, age))| {
                (
                    vec![
                        Value::new(*id),
                        Value::new(name.to_string()),
                        Value::new(*age),
                    ],
                    RID::new(0, i as u32),
                )
            })
            .collect();

        let mock_plan = MockScanNode::new(
            schema.clone(),
            "test_table".to_string(),
            vec![],
        ).with_tuples(mock_tuples.clone());

        // Sort by age, then name
        let age_col = schema.get_column(2).unwrap().clone();
        let name_col = schema.get_column(1).unwrap().clone();

        let age_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2,
            age_col,
            vec![],
        )));

        let name_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            name_col,
            vec![],
        )));

        let sort_plan = Arc::new(SortNode::new(
            schema.clone(),
            vec![age_expr, name_expr],
            vec![PlanNode::MockScan(mock_plan.clone())],
        ));

        let child_executor = Box::new(MockExecutor::new(
            exec_ctx.clone(),
            Arc::from(mock_plan),
            0,
            mock_tuples,
            schema.clone(),
        ));

        // Create and test sort executor
        let mut sort_executor = SortExecutor::new(child_executor, exec_ctx, sort_plan);
        sort_executor.init();

        // Collect results
        let mut results = Vec::new();
        while let Some((tuple, _)) = sort_executor.next() {
            let age: i32 = tuple.get_value(2).as_integer().unwrap();
            let name: String = ToString::to_string(&tuple.get_value(1));
            results.push((age, name));
        }

        // Verify results are sorted by age, then name
        assert_eq!(
            results,
            vec![
                (25, "Charlie".to_string()),
                (25, "Eve".to_string()),
                (30, "Alice".to_string()),
                (30, "Bob".to_string()),
                (35, "David".to_string()),
            ]
        );
    }
}