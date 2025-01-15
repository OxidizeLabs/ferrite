use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::catalog::catalog::Catalog;
use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::result_writer::ResultWriter;
use crate::common::rid::RID;
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction_manager::TransactionManager;
use crate::execution::check_option::CheckOptions;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::execution::plans::abstract_plan::PlanNode::{CreateIndex, CreateTable, Insert};
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
use crate::execution::plans::mock_scan_plan::MockScanNode;
use crate::optimizer::optimizer::Optimizer;
use crate::planner::planner::{LogicalPlan, LogicalToPhysical, QueryPlanner};
use crate::types_db::value::Value;
use log::{debug, info};
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;

pub struct ExecutorEngine {
    planner: QueryPlanner,
    optimizer: Optimizer,
    catalog: Arc<RwLock<Catalog>>,
    buffer_pool_manager: Arc<BufferPoolManager>,
    transaction_manager: Arc<RwLock<TransactionManager>>,
    lock_manager: Arc<LockManager>,
}

impl ExecutorEngine {
    pub fn new(
        catalog: Arc<RwLock<Catalog>>,
        buffer_pool_manager: Arc<BufferPoolManager>,
        transaction_manager: Arc<RwLock<TransactionManager>>,
        lock_manager: Arc<LockManager>,
    ) -> Self {
        Self {
            planner: QueryPlanner::new(catalog.clone()),
            optimizer: Optimizer::new(catalog.clone()),
            catalog,
            buffer_pool_manager,
            transaction_manager,
            lock_manager,
        }
    }

    /// Execute a SQL statement with the given context and writer
    pub fn execute_sql(
        &mut self,
        sql: &str,
        context: Arc<RwLock<ExecutorContext>>,
        writer: &mut impl ResultWriter,
    ) -> Result<bool, DBError> {
        // Parse and plan the SQL statement
        let plan = self.prepare_sql(sql, context.clone())?;

        // Execute the plan
        self.execute_plan(&plan, context, writer)
    }

    /// Prepare a SQL statement for execution
    fn prepare_sql(
        &mut self,
        sql: &str,
        context: Arc<RwLock<ExecutorContext>>,
    ) -> Result<PlanNode, DBError> {
        info!("Preparing SQL statement: {}", sql);

        // Create logical plan
        let logical_plan = self.create_logical_plan(sql)?;
        debug!("Initial logical plan generated: \n{}", logical_plan.explain(0));

        // Get check options from context
        let check_options = {
            let ctx = context.read();
            ctx.get_check_options().clone()
        };

        // Optimize plan
        let physical_plan = self.optimize_plan(logical_plan)?;
        debug!("Physical plan generated: \n{}", physical_plan.explain());

        Ok(physical_plan)
    }

    /// Execute a physical plan
    fn execute_plan(
        &self,
        plan: &PlanNode,
        context: Arc<RwLock<ExecutorContext>>,
        writer: &mut impl ResultWriter,
    ) -> Result<bool, DBError> {
        // Create root executor
        let mut root_executor = self.create_executor(plan, context)?;

        info!("Initializing executor");
        root_executor.init();
        debug!("Executor initialization complete");

        match plan {
            Insert(_) | CreateTable(_) | CreateIndex(_) => {
                debug!("Executing modification statement");
                let mut has_results = false;

                // Process all tuples from the executor
                while let Some(_) = root_executor.next() {
                    has_results = true;
                }

                if has_results {
                    info!("Modification statement executed successfully");
                    Ok(true)
                } else {
                    info!("No rows affected");
                    Ok(false)
                }
            }
            // For SELECT and other queries that produce output (including filtered results)
            _ => {
                debug!("Plan: {}", plan);

                let mut has_results = false;
                let mut row_count = 0;

                // Get schema information and store it
                let schema = root_executor.get_output_schema().clone();
                let column_count = schema.get_column_count();
                let columns = schema.get_columns();

                debug!("Writing output schema with {} columns", column_count);
                writer.begin_table(true);
                writer.begin_header();

                // Format column headers
                for col in columns {
                    let header_name = if col.get_name().starts_with("sum_") {
                        format!("SUM({})", col.get_name().trim_start_matches("sum_"))
                    } else if col.get_name().starts_with("count_") {
                        format!("COUNT({})", col.get_name().trim_start_matches("count_"))
                    } else if col.get_name().starts_with("avg_") {
                        format!("AVG({})", col.get_name().trim_start_matches("avg_"))
                    } else if col.get_name().starts_with("min_") {
                        format!("MIN({})", col.get_name().trim_start_matches("min_"))
                    } else if col.get_name().starts_with("max_") {
                        format!("MAX({})", col.get_name().trim_start_matches("max_"))
                    } else {
                        col.get_name().to_string()
                    };
                    writer.write_header_cell(&header_name);
                }
                writer.end_header();

                // Process rows
                debug!("Starting result processing");
                while let Some((tuple, _rid)) = root_executor.next() {
                    has_results = true;
                    row_count += 1;

                    if row_count % 1000 == 0 {
                        debug!("Processed {} rows", row_count);
                    }

                    writer.begin_row();
                    for i in 0..column_count {
                        let value = tuple.get_value(i as usize);
                        writer.write_cell(&value.to_string());
                    }
                    writer.end_row();
                }

                debug!(
                    "Result processing complete. Found {} matching rows",
                    row_count
                );
                writer.end_table();

                info!("Query execution finished. Processed {} rows", row_count);
                Ok(has_results)
            }
        }
    }

    /// Create an executor for the given plan
    fn create_executor(
        &self,
        plan: &PlanNode,
        context: Arc<RwLock<ExecutorContext>>,
    ) -> Result<Box<dyn AbstractExecutor>, DBError> {
        debug!("Creating executor for plan: {}", plan);
        plan.create_executor(context).map_err(DBError::ExecutorError)
    }

    /// Create a logical plan from SQL
    fn create_logical_plan(&mut self, sql: &str) -> Result<LogicalPlan, DBError> {
        self.planner
            .create_logical_plan(sql)
            .map(|boxed_plan| *boxed_plan)  // Unbox the LogicalPlan
            .map_err(DBError::PlanError)
    }

    /// Optimize a logical plan into a physical plan
    fn optimize_plan(&self, plan: LogicalPlan) -> Result<PlanNode, DBError> {
        // Box the logical plan and get check options
        let boxed_plan = Box::new(plan);
        let check_options = Arc::new(CheckOptions::new());

        // Optimize the plan
        let optimized_plan = self.optimizer
            .optimize(boxed_plan, check_options)
            .map_err(|e| DBError::OptimizeError(e.to_string()))?;

        // Convert to physical plan and map any String errors to DBError
        optimized_plan
            .to_physical_plan()
            .map_err(|e| DBError::OptimizeError(e))
    }

    /// Clean up after execution
    fn cleanup_after_execution(&self) {
        debug!("Starting post-execution cleanup");
        // Add cleanup logic here if needed
        debug!("Post-execution cleanup complete");
    }

    fn create_mock_scan(&self, input_schema: Schema, mock_tuples: Vec<(Vec<Value>, RID)>) -> PlanNode {
        PlanNode::MockScan(
            MockScanNode::new(
                input_schema.clone(),
                "mock_table".to_string(),
                vec![],  // No children initially
            )
                .with_tuples(mock_tuples)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::common::rid::RID;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::recovery::log_manager::LogManager;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;

    use crate::execution::expressions::abstract_expression::Expression;
    use crate::execution::expressions::aggregate_expression::{AggregateExpression, AggregationType};
    use crate::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::execution::plans::aggregation_plan::AggregationPlanNode;
    use crate::execution::plans::mock_scan_plan::MockScanNode;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    fn init_test_logger() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();
    }

    // Mock ResultWriter for testing
    struct MockResultWriter {
        rows: Vec<Vec<String>>,
        headers: Vec<String>,
        in_header: bool,
        in_row: bool,
        current_row: Vec<String>,
    }

    impl MockResultWriter {
        fn new() -> Self {
            Self {
                rows: Vec::new(),
                headers: Vec::new(),
                in_header: false,
                in_row: false,
                current_row: Vec::new(),
            }
        }
    }

    impl ResultWriter for MockResultWriter {
        fn begin_table(&mut self, _has_header: bool) {}

        fn end_table(&mut self) {}

        fn begin_header(&mut self) {
            self.in_header = true;
        }

        fn end_header(&mut self) {
            self.in_header = false;
        }

        fn begin_row(&mut self) {
            self.in_row = true;
            self.current_row.clear();
        }

        fn end_row(&mut self) {
            self.in_row = false;
            self.rows.push(self.current_row.clone());
        }

        fn write_cell(&mut self, value: &str) {
            if self.in_row {
                self.current_row.push(value.to_string());
            }
        }

        fn write_header_cell(&mut self, value: &str) {
            if self.in_header {
                self.headers.push(value.to_string());
            }
        }

        fn one_cell(&mut self, content: &str) {
            println!("{}", content);
        }
    }

    struct TestContext {
        buffer_pool_manager: Arc<BufferPoolManager>,
        transaction_manager: Arc<RwLock<TransactionManager>>,
        lock_manager: Arc<LockManager>,
        db_file: String,
        log_file: String,
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            init_test_logger();
            let timestamp = chrono::Utc::now().timestamp();
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

    fn create_catalog(ctx: &TestContext) -> Catalog {
        Catalog::new(
            ctx.buffer_pool_manager.clone(),
            0,
            0,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
    }

    fn create_test_executor_context(ctx: &TestContext, catalog: Arc<RwLock<Catalog>>) -> Arc<RwLock<ExecutorContext>> {
        let txn = Arc::new(Transaction::new(
            0,
            IsolationLevel::ReadCommitted,
        ));

        Arc::new(RwLock::new(ExecutorContext::new(
            txn,
            ctx.transaction_manager.clone(),
            catalog,
            ctx.buffer_pool_manager.clone(),
            ctx.lock_manager.clone(),
        )))
    }

    #[test]
    fn test_create_table() {
        let test_context = TestContext::new("create_table");
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let exec_ctx = create_test_executor_context(&test_context, catalog.clone());

        let mut engine = ExecutorEngine::new(
            catalog.clone(),
            test_context.buffer_pool_manager.clone(),
            test_context.transaction_manager.clone(),
            test_context.lock_manager.clone(),
        );

        let sql = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name VARCHAR(255))";
        let mut writer = MockResultWriter::new();

        let result = engine.execute_sql(sql, exec_ctx.clone(), &mut writer);

        assert!(result.is_ok());
        assert!(catalog.read().get_table("test_table").is_some());
    }

    #[test]
    fn test_insert_and_scan() {
        let test_context = TestContext::new("insert_scan");
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let exec_ctx = create_test_executor_context(&test_context, catalog.clone());

        let mut engine = ExecutorEngine::new(
            catalog.clone(),
            test_context.buffer_pool_manager.clone(),
            test_context.transaction_manager.clone(),
            test_context.lock_manager.clone(),
        );

        // First create table
        let create_sql = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name VARCHAR(255))";
        let mut writer = MockResultWriter::new();
        engine.execute_sql(create_sql, exec_ctx.clone(), &mut writer).unwrap();

        // Then insert data
        let insert_sql = "INSERT INTO test_table VALUES (1, 'test')";
        engine.execute_sql(insert_sql, exec_ctx.clone(), &mut writer).unwrap();

        // Finally scan
        let scan_sql = "SELECT * FROM test_table";
        let mut writer = MockResultWriter::new();
        let result = engine.execute_sql(scan_sql, exec_ctx.clone(), &mut writer);

        assert!(result.is_ok());
        assert_eq!(writer.rows.len(), 1);
        assert_eq!(writer.rows[0][0], "1");
        assert_eq!(writer.rows[0][1], "test");
    }

    #[test]
    fn test_filter_execution() {
        let test_context = TestContext::new("filter");
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let exec_ctx = create_test_executor_context(&test_context, catalog.clone());

        let mut engine = ExecutorEngine::new(
            catalog.clone(),
            test_context.buffer_pool_manager.clone(),
            test_context.transaction_manager.clone(),
            test_context.lock_manager.clone(),
        );

        // Create and populate table
        let create_sql = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value INTEGER)";
        let mut writer = MockResultWriter::new();
        engine.execute_sql(create_sql, exec_ctx.clone(), &mut writer).unwrap();

        let insert_sql = "INSERT INTO test_table VALUES (1, 100), (2, 200), (3, 300)";
        engine.execute_sql(insert_sql, exec_ctx.clone(), &mut writer).unwrap();

        // Test filter
        let filter_sql = "SELECT * FROM test_table WHERE value > 150";
        let mut writer = MockResultWriter::new();
        let result = engine.execute_sql(filter_sql, exec_ctx.clone(), &mut writer);

        assert!(result.is_ok());
        assert_eq!(writer.rows.len(), 2); // Should only return rows with value > 150
    }

    #[test]
    fn test_error_handling() {
        let test_context = TestContext::new("error_handling");
        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let exec_ctx = create_test_executor_context(&test_context, catalog.clone());

        let mut engine = ExecutorEngine::new(
            catalog.clone(),
            test_context.buffer_pool_manager.clone(),
            test_context.transaction_manager.clone(),
            test_context.lock_manager.clone(),
        );

        // Test invalid SQL
        let invalid_sql = "SELECT * FROM nonexistent_table";
        let mut writer = MockResultWriter::new();
        let result = engine.execute_sql(invalid_sql, exec_ctx.clone(), &mut writer);
        assert!(result.is_err());
    }

    #[test]
    fn test_aggregation() {
        let test_context = TestContext::new("aggregation");

        // Create input schema
        let input_schema = Schema::new(vec![
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ]);

        // Create output schema
        let output_schema = Schema::new(vec![
            Column::new("name", TypeId::VarChar),
            Column::new("sum_age", TypeId::Integer),
            Column::new("count_age", TypeId::BigInt),
        ]);

        let catalog = Arc::new(RwLock::new(create_catalog(&test_context)));
        let exec_ctx = create_test_executor_context(&test_context, catalog.clone());

        // Create test data
        let mock_tuples = vec![
            (vec![Value::new("alice"), Value::new(20)], RID::new(0, 0)),
            (vec![Value::new("alice"), Value::new(10)], RID::new(0, 1)),
            (vec![Value::new("bob"), Value::new(30)], RID::new(0, 2)),
            (vec![Value::new("bob"), Value::new(25)], RID::new(0, 3)),
            (vec![Value::new("bob"), Value::new(25)], RID::new(0, 4)),
        ];

        // Create mock scan plan
        let mock_scan_plan = MockScanNode::new(
            input_schema.clone(),
            "mock_table".to_string(),
            vec![],  // empty children vector
        )
            .with_tuples(mock_tuples.clone());

        // Create group by expression
        let group_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, Column::new("name", TypeId::VarChar), vec![],
        )));

        // Create aggregate expressions
        let age_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 1, Column::new("age", TypeId::Integer), vec![],
        )));

        let sum_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            age_col.clone(),
            vec![age_col.clone()],
        )));

        let count_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Count,
            age_col.clone(),
            vec![age_col],
        )));

        // Create aggregation plan with child plan
        let agg_plan = AggregationPlanNode::new(
            output_schema,
            vec![PlanNode::MockScan(mock_scan_plan)],  // Pass the mock scan plan as child
            vec![group_expr],
            vec![sum_expr, count_expr],
            vec![AggregationType::Sum, AggregationType::Count],
        );

        // Create executor engine
        let engine = ExecutorEngine::new(
            catalog.clone(),
            test_context.buffer_pool_manager.clone(),
            test_context.transaction_manager.clone(),
            test_context.lock_manager.clone(),
        );

        // Execute plan
        let mut writer = MockResultWriter::new();
        let result = engine.execute_plan(&PlanNode::Aggregation(agg_plan), exec_ctx, &mut writer);

        assert!(result.is_ok(), "Failed to execute plan: {:?}", result);

        // Verify headers
        assert_eq!(writer.headers, vec!["name", "SUM(age)", "COUNT(age)"]);

        // Verify results
        assert_eq!(writer.rows.len(), 2);

        // Sort rows by name to ensure consistent order
        let mut rows = writer.rows;
        rows.sort_by(|a, b| a[0].cmp(&b[0]));

        // Check alice's group
        assert_eq!(rows[0][0], "alice");
        assert_eq!(rows[0][1], "30");     // SUM(age) = 20 + 10
        assert_eq!(rows[0][2], "2");      // COUNT(age) = 2

        // Check bob's group
        assert_eq!(rows[1][0], "bob");
        assert_eq!(rows[1][1], "80");     // SUM(age) = 30 + 25 + 25
        assert_eq!(rows[1][2], "3");      // COUNT(age) = 3
    }

    #[test]
    fn test_mock_scan_execution() {
        let input_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
        ]);

        let mock_tuples = vec![
            (vec![Value::new(1)], RID::new(1, 1)),
            (vec![Value::new(2)], RID::new(1, 2)),
        ];

        let mock_scan = MockScanNode::new(
            input_schema.clone(),
            "mock_table".to_string(),
            vec![],
        )
            .with_tuples(mock_tuples.clone());

        // ... rest of the test ...
    }
}
