use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::catalog::catalog::Catalog;
use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::result_writer::ResultWriter;
use crate::concurrency::transaction_manager_factory::TransactionManagerFactory;
use crate::sql::execution::check_option::CheckOptions;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::PlanNode;
use crate::sql::execution::plans::insert_plan::InsertNode;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::sql::optimizer::optimizer::Optimizer;
use crate::storage::table::tuple::TupleMeta;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use log::{debug, info, error};
use parking_lot::RwLock;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;
use crate::sql::planner::logical_plan::{LogicalPlan, LogicalToPhysical};
use crate::sql::planner::query_planner::QueryPlanner;
use crate::recovery::log_manager::LogManager;
use crate::sql::execution::executors::seq_scan_executor::SeqScanExecutor;
use crate::sql::execution::executors::sort_executor::SortExecutor;
use crate::sql::execution::executors::table_scan_executor::TableScanExecutor;
use crate::sql::execution::expressions::abstract_expression::Expression;
use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::sql::execution::plans::sort_plan::SortNode;
use crate::sql::execution::plans::table_scan_plan::TableScanNode;
use tempfile::TempDir;

pub struct ExecutionEngine {
    planner: QueryPlanner,
    optimizer: Optimizer,
    catalog: Arc<RwLock<Catalog>>,
    buffer_pool_manager: Arc<BufferPoolManager>,
    transaction_factory: Arc<TransactionManagerFactory>,
}

impl ExecutionEngine {
    pub fn new(
        catalog: Arc<RwLock<Catalog>>,
        buffer_pool_manager: Arc<BufferPoolManager>,
        transaction_factory: Arc<TransactionManagerFactory>,
    ) -> Self {
        Self {
            planner: QueryPlanner::new(catalog.clone()),
            optimizer: Optimizer::new(catalog.clone()),
            catalog,
            buffer_pool_manager,
            transaction_factory,
        }
    }

    /// Execute a SQL statement with the given context and writer
    pub fn execute_sql(
        &mut self,
        sql: &str,
        context: Arc<RwLock<ExecutionContext>>,
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
        context: Arc<RwLock<ExecutionContext>>,
    ) -> Result<PlanNode, DBError> {
        info!("Preparing SQL statement: {}", sql);

        // Create logical plan
        let logical_plan = self.create_logical_plan(sql)?;
        debug!(
            "Initial logical plan generated: \n{}",
            logical_plan.explain(0)
        );

        // Optimize plan
        let physical_plan = self.optimize_plan(logical_plan)?;
        // debug!("Physical plan generated: \n{}", physical_plan.explain());

        Ok(physical_plan)
    }

    /// Execute a physical plan
    fn execute_plan(
        &self,
        plan: &PlanNode,
        context: Arc<RwLock<ExecutionContext>>,
        writer: &mut dyn ResultWriter,
    ) -> Result<bool, DBError> {
        debug!("Executing physical plan: {}", plan);
        
        // Create executor in a stack-efficient way
        let mut root_executor = match self.create_executor(plan, context) {
            Ok(executor) => executor,
            Err(e) => {
                error!("Failed to create executor: {}", e);
                return Err(e);
            }
        };
        
        // Initialize executor
        debug!("Initializing root executor");
        root_executor.init();
        debug!("Root executor initialized");

        match plan {
            PlanNode::Insert(_) | PlanNode::CreateTable(_) | PlanNode::CreateIndex(_) => {
                debug!("Executing modification statement");
                let mut has_results = false;

                // Process all tuples from the executor
                while let Some(_) = root_executor.next() {
                    has_results = true;
                }

                // For Insert, CreateTable, and CreateIndex, return true if execution completed successfully
                match plan {
                    PlanNode::CreateTable(_) | PlanNode::CreateIndex(_) => {
                        info!("Create operation executed successfully");
                        Ok(true)
                    }
                    _ => {
                        if has_results {
                            info!("Modification statement executed successfully");
                            Ok(true)
                        } else {
                            info!("No rows affected");
                            Ok(false)
                        }
                    }
                }
            }
            _ => {
                debug!("Executing query statement");
                let schema = root_executor.get_output_schema();
                let columns = schema.get_columns();

                // Write schema header with proper aggregate function names
                writer.write_schema_header(
                    columns
                        .iter()
                        .map(|col| { col.get_name().to_string() })
                        .collect()
                );

                let mut has_results = false;
                let mut row_count = 0;
                
                // Use a non-recursive approach to process results
                debug!("Starting to process result tuples");
                loop {
                    match root_executor.next() {
                        Some((tuple, _)) => {
                            has_results = true;
                            row_count += 1;
                            debug!("Processing result tuple {}", row_count);
                            writer.write_row(tuple.get_values().to_vec());
                        }
                        None => {
                            debug!("No more result tuples");
                            break;
                        }
                    }
                }

                debug!("Processed {} rows", row_count);
                Ok(has_results)
            }
        }
    }

    /// Create an executor for the given plan
    fn create_executor(
        &self,
        plan: &PlanNode,
        context: Arc<RwLock<ExecutionContext>>,
    ) -> Result<Box<dyn AbstractExecutor>, DBError> {
        debug!("Creating executor for plan: {}", plan);
        
        // Create executor in a stack-efficient way
        match plan.create_executor(context) {
            Ok(executor) => {
                debug!("Successfully created executor for plan: {}", plan);
                Ok(executor)
            },
            Err(e) => {
                error!("Failed to create executor for plan: {}, error: {}", plan, e);
                Err(DBError::Execution(e))
            }
        }
    }

    /// Create a logical plan from SQL
    fn create_logical_plan(&mut self, sql: &str) -> Result<LogicalPlan, DBError> {
        self.planner
            .create_logical_plan(sql)
            .map(|boxed_plan| *boxed_plan) // Unbox the LogicalPlan
            .map_err(DBError::PlanError)
    }

    /// Optimize a logical plan into a physical plan
    fn optimize_plan(&self, plan: LogicalPlan) -> Result<PlanNode, DBError> {
        // Box the logical plan and get check options
        let boxed_plan = Box::new(plan);
        let check_options = Arc::new(CheckOptions::new());

        // Optimize the plan
        let optimized_plan = self
            .optimizer
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

    /// Prepare a SQL statement and validate syntax
    /// Returns empty parameter types for now since we don't support parameters yet
    pub fn prepare_statement(&mut self, sql: &str) -> Result<Vec<TypeId>, DBError> {
        // Parse SQL to validate syntax
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql)
            .map_err(|e| DBError::SqlError(format!("Parse error: {}", e)))?;

        if ast.len() != 1 {
            return Err(DBError::SqlError("Expected single statement".to_string()));
        }

        // Create logical plan to validate semantics
        let _logical_plan = self.create_logical_plan(sql)?;

        // For now, return empty parameter types since we don't support parameters yet
        Ok(Vec::new())
    }

    /// Execute a prepared statement (currently same as regular execute)
    pub fn execute_prepared_statement(
        &mut self,
        sql: &str,
        _params: Vec<Value>,
        context: Arc<RwLock<ExecutionContext>>,
        writer: &mut impl ResultWriter,
    ) -> Result<bool, DBError> {
        // For now, just execute as regular SQL since we don't support parameters
        self.execute_sql(sql, context, writer)
    }

    fn execute_insert(&self, plan: &InsertNode, txn_ctx: Arc<TransactionContext>) -> Result<(), DBError> {
        debug!("Executing insert plan");

        let binding = self.catalog.read();
        let table_info = binding
            .get_table(plan.get_table_name())
            .ok_or_else(|| DBError::TableNotFound(plan.get_table_name().to_string()))?;

        // Create tuple meta with just the transaction ID
        let meta = TupleMeta::new(txn_ctx.get_transaction_id());

        // Get the first tuple from input tuples
        let input_tuples = plan.get_input_tuples();
        if input_tuples.is_empty() {
            return Err(DBError::Execution("No tuples to insert".to_string()));
        }

        let mut tuple = input_tuples[0].clone();  // Clone the first tuple

        // Get mutable table heap and insert tuple with transaction context
        let table_heap = table_info.get_table_heap_mut();
        let _table_heap_guard = table_heap.latch.write();

        table_heap.insert_tuple(&meta, &mut tuple).expect("Insert failed");

        debug!("Insert executed successfully");
        Ok(())
    }

    fn commit_transaction(&self, txn_ctx: Arc<TransactionContext>) -> Result<bool, DBError> {
        debug!("Committing transaction {}", txn_ctx.get_transaction_id());

        // Get transaction manager
        let txn_manager = self.transaction_factory.get_transaction_manager();

        // Attempt to commit
        match txn_manager.commit(txn_ctx.get_transaction(), self.buffer_pool_manager.clone()) {
            true => {
                debug!("Transaction committed successfully");
                Ok(true)
            }
            false => {
                debug!("Transaction commit failed");
                Ok(false)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::common::logger::initialize_logger;
    use crate::common::rid::RID;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::recovery::log_manager::LogManager;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::table::table_heap::TableInfo;
    use crate::storage::table::tuple::Tuple;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;
    use crate::types_db::value::Val::Null;

    struct TestContext {
        engine: ExecutionEngine,
        catalog: Arc<RwLock<Catalog>>,
        exec_ctx: Arc<RwLock<ExecutionContext>>,
        planner: QueryPlanner,
        bpm: Arc<BufferPoolManager>,
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
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(7, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler.clone(),
                disk_manager.clone(),
                replacer,
            ));

            let log_manager = Arc::new(RwLock::new(LogManager::new(
                disk_manager.clone(),
            )));

            let txn_manager = Arc::new(TransactionManager::new());

            let catalog = Arc::new(RwLock::new(Catalog::new(
                Arc::clone(&bpm),
                0,
                0,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                txn_manager.clone(),
            )));

            let lock_manager = Arc::new(LockManager::new());

            let txn = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));
            let transaction_context = Arc::new(TransactionContext::new(
                txn,
                lock_manager,
                txn_manager,
            ));

            let transaction_factory = Arc::new(TransactionManagerFactory::new(bpm.clone()));

            let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
                bpm.clone(),
                catalog.clone(),
                transaction_context.clone(),
            )));

            let engine = ExecutionEngine::new(
                catalog.clone(),
                bpm.clone(),
                transaction_factory,
            );

            let planner = QueryPlanner::new(catalog.clone());

            Self {
                engine,
                catalog,
                exec_ctx,
                planner,
                bpm,
                transaction_context,
                _temp_dir: temp_dir,
            }
        }

        fn create_test_table(&self, table_name: &str, schema: Schema) -> Result<TableInfo, String> {
            let mut catalog = self.catalog.write();
            Ok(catalog.create_table(table_name.to_string(), schema.clone()).unwrap())
        }

        fn insert_tuples(&self, table_name: &str, tuples: Vec<Vec<Value>>, schema: Schema) -> Result<(), String> {
            let catalog = self.catalog.read();
            let table = catalog.get_table(table_name).unwrap();
            let table_heap = table.get_table_heap();

            let meta = TupleMeta::new(0);
            for values in tuples {
                let mut tuple = Tuple::new(&values, schema.clone(), RID::new(0, 0));

                table_heap.insert_tuple(&meta, &mut tuple).map_err(|e| e.to_string())?;
            }
            Ok(())
        }

        fn transaction_context(&self) -> Arc<TransactionContext> {
            self.transaction_context.clone()
        }
    }

    struct TestResultWriter {
        schema: Option<Schema>,
        rows: Vec<Vec<Value>>,
    }

    impl TestResultWriter {
        fn new() -> Self {
            Self {
                schema: None,
                rows: Vec::new(),
            }
        }

        fn get_schema(&self) -> &Schema {
            self.schema.as_ref().expect("Schema not set")
        }

        fn get_rows(&self) -> &Vec<Vec<Value>> {
            &self.rows
        }
    }

    impl ResultWriter for TestResultWriter {
        fn write_schema_header(&mut self, columns: Vec<String>) {
            let schema_columns = columns.into_iter()
                .map(|name| Column::new(&name, TypeId::VarChar))
                .collect();
            self.schema = Some(Schema::new(schema_columns));
        }

        fn write_row(&mut self, values: Vec<Value>) {
            self.rows.push(values);
        }

        fn write_message(&mut self, message: &str) {
            todo!()
        }
    }

    #[test]
    fn test_group_by_column_names() {
        let mut ctx = TestContext::new("test_group_by_column_names");

        // Create test table
        let table_schema = Schema::new(vec![
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
            Column::new("salary", TypeId::BigInt),
        ]);

        let table_name = "employees";
        ctx.create_test_table(table_name, table_schema.clone()).unwrap();

        // Insert test data
        let test_data = vec![
            vec![Value::new("Alice"), Value::new(25), Value::new(50000i64)],
            vec![Value::new("Alice"), Value::new(25), Value::new(52000i64)],
            vec![Value::new("Bob"), Value::new(30), Value::new(60000i64)],
            vec![Value::new("Bob"), Value::new(30), Value::new(65000i64)],
            vec![Value::new("Charlie"), Value::new(35), Value::new(70000i64)],
        ];
        ctx.insert_tuples(table_name, test_data, table_schema).unwrap();

        // Test different GROUP BY queries
        let test_cases = vec![
            (
                "SELECT name, SUM(age) as total_age, COUNT(*) as emp_count FROM employees GROUP BY name",
                vec!["name", "total_age", "emp_count"],
                3, // Expected number of groups
            ),
            (
                "SELECT name, AVG(salary) as avg_salary FROM employees GROUP BY name",
                vec!["name", "avg_salary"],
                3,
            ),
            (
                "SELECT name, MIN(age) as min_age, MAX(salary) as max_salary FROM employees GROUP BY name",
                vec!["name", "min_age", "max_salary"],
                3,
            ),
        ];

        for (sql, expected_columns, expected_groups) in test_cases {
            let mut writer = TestResultWriter::new();
            let success = ctx.engine.execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).unwrap();
            assert!(success, "Query execution failed");

            // Check column names
            let schema = writer.get_schema();
            for (i, expected_name) in expected_columns.iter().enumerate() {
                assert_eq!(
                    schema.get_columns()[i].get_name(),
                    *expected_name,
                    "Column name mismatch for query: {}",
                    sql
                );
            }

            // Check number of result groups
            let mut row_count = 0;
            while writer.get_rows().iter().any(|row| row.len() != 0) {
                row_count += 1;
            }
            assert_eq!(
                row_count,
                expected_groups,
                "Incorrect number of groups for query: {}",
                sql
            );
        }
    }

    #[test]
    fn test_group_by_aggregates() {
        let mut ctx = TestContext::new("test_group_by_aggregates");

        // Create test table
        let table_schema = Schema::new(vec![
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
            Column::new("salary", TypeId::BigInt),
            Column::new("department", TypeId::VarChar),
        ]);

        let table_name = "employees";
        ctx.create_test_table(table_name, table_schema.clone()).unwrap();

        // Insert test data
        let test_data = vec![
            vec![Value::new("Alice"), Value::new(25), Value::new(50000i64), Value::new("Engineering")],
            vec![Value::new("Alice"), Value::new(25), Value::new(52000i64), Value::new("Engineering")],
            vec![Value::new("Bob"), Value::new(30), Value::new(60000i64), Value::new("Sales")],
            vec![Value::new("Bob"), Value::new(30), Value::new(65000i64), Value::new("Sales")],
            vec![Value::new("Charlie"), Value::new(35), Value::new(70000i64), Value::new("Engineering")],
            vec![Value::new("David"), Value::new(40), Value::new(80000i64), Value::new("Sales")],
        ];
        ctx.insert_tuples(table_name, test_data, table_schema).unwrap();

        let test_cases = vec![
            // Basic GROUP BY with multiple aggregates
            (
                "SELECT name, COUNT(*) as count, SUM(salary) as total_salary FROM employees GROUP BY name",
                vec!["name", "count", "total_salary"],
                4, // Alice, Bob, Charlie, David
            ),
            // GROUP BY with AVG
            (
                "SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department",
                vec!["department", "avg_salary"],
                2, // Engineering, Sales
            ),
            // GROUP BY with MIN/MAX
            (
                "SELECT department, MIN(age) as min_age, MAX(salary) as max_salary FROM employees GROUP BY department",
                vec!["department", "min_age", "max_salary"],
                2,
            ),
            // Multiple GROUP BY columns
            (
                "SELECT department, age, COUNT(*) as count FROM employees GROUP BY department, age",
                vec!["department", "age", "count"],
                5, // Unique department-age combinations
            ),
            // GROUP BY with HAVING clause
            (
                "SELECT department, COUNT(*) as emp_count FROM employees GROUP BY department HAVING COUNT(*) > 2",
                vec!["department", "emp_count"],
                2,
            ),
        ];

        for (sql, expected_columns, expected_groups) in test_cases {
            let mut writer = TestResultWriter::new();
            let success = ctx.engine.execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).unwrap();

            assert!(success, "Query execution failed for: {}", sql);

            // Verify column names
            let schema = writer.get_schema();
            assert_eq!(
                schema.get_columns().len(),
                expected_columns.len(),
                "Incorrect number of columns for query: {}",
                sql
            );

            for (i, expected_name) in expected_columns.iter().enumerate() {
                assert_eq!(
                    schema.get_columns()[i].get_name(),
                    *expected_name,
                    "Column name mismatch for query: {}",
                    sql
                );
            }

            // Verify number of result rows
            assert_eq!(
                writer.get_rows().len(),
                expected_groups,
                "Incorrect number of groups for query: {}",
                sql
            );
        }
    }

    #[test]
    fn test_simple_queries() {
        // Create a simpler test to avoid stack overflow
        let mut ctx = TestContext::new("test_simple_queries");

        // Create test table
        let table_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("active", TypeId::Boolean),
        ]);

        let table_name = "users";
        ctx.create_test_table(table_name, table_schema.clone()).unwrap();

        // Insert test data - just one row to keep it simple
        let test_data = vec![
            vec![Value::new(1), Value::new("Alice"), Value::new(true)],
        ];
        ctx.insert_tuples(table_name, test_data, table_schema).unwrap();

        // Execute a simple query without LIMIT to see if that works
        let mut writer = TestResultWriter::new();
        let sql = "SELECT name FROM users";

        println!("Executing simple query: {}", sql);
        match ctx.engine.execute_sql(sql, ctx.exec_ctx.clone(), &mut writer) {
            Ok(success) => {
                assert!(success, "Query execution failed for: {}", sql);
                let rows = writer.get_rows();
                println!("Query returned {} rows", rows.len());
                assert_eq!(rows.len(), 1, "Incorrect number of rows");
            },
            Err(e) => {
                panic!("Error executing query '{}': {:?}", sql, e);
            }
        }
    }

    #[test]
    // #[ignore = "Causes stack overflow in the logical plan to physical plan conversion"]
    fn test_order_by() {
        let mut ctx = TestContext::new("test_order_by");

        println!("Creating test table and schema");
        // Create test table with minimal schema
        let table_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let table_name = "sorted_users";
        ctx.create_test_table(table_name, table_schema.clone()).unwrap();

        println!("Inserting test data");
        // Insert minimal test data - just two rows to minimize stack usage
        let test_data = vec![
            vec![Value::new(2), Value::new("Alice")],
            vec![Value::new(1), Value::new("Bob")],
        ];
        ctx.insert_tuples(table_name, test_data, table_schema).unwrap();

        // Test a single simple ORDER BY case
        let sql = "SELECT id, name FROM sorted_users ORDER BY id";  // Use ASC order to simplify
        let mut writer = TestResultWriter::new();
        
        println!("Executing query: {}", sql);
        let success = match ctx.engine.execute_sql(sql, ctx.exec_ctx.clone(), &mut writer) {
            Ok(s) => {
                println!("Query execution succeeded");
                s
            },
            Err(e) => {
                println!("Query execution failed: {:?}", e);
                panic!("Query execution failed: {:?}", e);
            }
        };
        
        assert!(success, "Query execution failed");
        
        println!("Processing results");
        let rows = writer.get_rows();
        println!("Got {} rows", rows.len());
        
        // Verify results in a stack-efficient way
        assert_eq!(rows.len(), 2, "Expected 2 rows");
        
        if rows.len() >= 2 {
            println!("First row: {:?}", rows[0]);
            println!("Second row: {:?}", rows[1]);
            
            // Check first row (should be id=1, name=Bob)
            let first_id = &rows[0][0];
            let first_name = &rows[0][1];
            assert_eq!(first_id.to_string(), "1", "First row should have id=1");
            assert_eq!(first_name.to_string(), "Bob", "First row should have name=Bob");
            
            // Check second row (should be id=2, name=Alice)
            let second_id = &rows[1][0];
            let second_name = &rows[1][1];
            assert_eq!(second_id.to_string(), "2", "Second row should have id=2");
            assert_eq!(second_name.to_string(), "Alice", "Second row should have name=Alice");
        }
        
        println!("Test completed successfully");
    }

    #[test]
    fn test_where_clause() {
        let mut ctx = TestContext::new("test_where_clause");

        // Create test table
        let table_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
            Column::new("active", TypeId::Boolean),
        ]);

        let table_name = "users";
        ctx.create_test_table(table_name, table_schema.clone()).unwrap();

        // Insert test data
        let test_data = vec![
            vec![Value::new(1), Value::new("Alice"), Value::new(25), Value::new(true)],
            vec![Value::new(2), Value::new("Bob"), Value::new(30), Value::new(true)],
            vec![Value::new(3), Value::new("Charlie"), Value::new(35), Value::new(false)],
            vec![Value::new(4), Value::new("David"), Value::new(40), Value::new(true)],
        ];
        ctx.insert_tuples(table_name, test_data, table_schema).unwrap();

        let test_cases = vec![
            // Simple equality condition
            (
                "SELECT name FROM users WHERE id = 2",
                vec!["Bob"],
            ),
            // Comparison operator
            (
                "SELECT name FROM users WHERE age > 30",
                vec!["Charlie", "David"],
            ),
            // Boolean condition
            (
                "SELECT name FROM users WHERE active = true",
                vec!["Alice", "Bob", "David"],
            ),
            // Multiple conditions with AND
            (
                "SELECT name FROM users WHERE age > 25 AND active = true",
                vec!["Bob", "David"],
            ),
            // Multiple conditions with OR
            (
                "SELECT name FROM users WHERE id = 1 OR id = 3",
                vec!["Alice", "Charlie"],
            ),
        ];

        for (sql, expected_names) in test_cases {
            let mut writer = TestResultWriter::new();
            let success = ctx.engine.execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).unwrap();

            assert!(success, "Query execution failed for: {}", sql);

            let actual_names: Vec<String> = writer.get_rows()
                .iter()
                .map(|row| row[0].to_string())
                .collect();

            assert_eq!(
                actual_names.len(),
                expected_names.len(),
                "Incorrect number of results for query: {}",
                sql
            );

            for name in expected_names {
                assert!(
                    actual_names.contains(&name.to_string()),
                    "Expected name '{}' not found in results for query: {}",
                    name,
                    sql
                );
            }
        }
    }

    #[test]
    fn test_join_operations() {
        let mut ctx = TestContext::new("test_join_operations");

        // Create users table
        let users_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("dept_id", TypeId::Integer),
        ]);
        ctx.create_test_table("users", users_schema.clone()).unwrap();

        // Create departments table
        let depts_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        ctx.create_test_table("departments", depts_schema.clone()).unwrap();

        // Insert test data for users
        let users_data = vec![
            vec![Value::new(1), Value::new("Alice"), Value::new(1)],
            vec![Value::new(2), Value::new("Bob"), Value::new(2)],
            vec![Value::new(3), Value::new("Charlie"), Value::new(1)],
            vec![Value::new(4), Value::new("David"), Value::new(3)],
            vec![Value::new(5), Value::new("Eve"), Value::new(Null)],
        ];
        ctx.insert_tuples("users", users_data, users_schema).unwrap();

        // Insert test data for departments
        let depts_data = vec![
            vec![Value::new(1), Value::new("Engineering")],
            vec![Value::new(2), Value::new("Sales")],
            vec![Value::new(3), Value::new("Marketing")],
        ];
        ctx.insert_tuples("departments", depts_data, depts_schema).unwrap();

        let test_cases = vec![
            // Inner join
            (
                "SELECT u.name, d.name FROM users u JOIN departments d ON u.dept_id = d.id",
                4, // Alice, Bob, Charlie, David
            ),
            // Left join
            (
                "SELECT u.name, d.name FROM users u LEFT JOIN departments d ON u.dept_id = d.id",
                5, // All users including Eve with NULL department
            ),
            // Join with additional conditions
            (
                "SELECT u.name, d.name FROM users u JOIN departments d ON u.dept_id = d.id WHERE d.name = 'Engineering'",
                2, // Alice, Charlie
            ),
        ];

        for (sql, expected_rows) in test_cases {
            let mut writer = TestResultWriter::new();
            let success = ctx.engine.execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).unwrap();

            assert!(success, "Query execution failed for: {}", sql);

            assert_eq!(
                writer.get_rows().len(),
                expected_rows,
                "Incorrect number of rows for query: {}",
                sql
            );
        }
    }

    #[test]
    fn test_insert_operations() {
        let mut ctx = TestContext::new("test_insert_operations");

        // Create test table
        let table_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ]);

        let table_name = "users";
        ctx.create_test_table(table_name, table_schema.clone()).unwrap();

        // Test INSERT with VALUES
        let insert_sql = "INSERT INTO users VALUES (1, 'Alice', 25)";
        let mut writer = TestResultWriter::new();
        let success = ctx.engine.execute_sql(insert_sql, ctx.exec_ctx.clone(), &mut writer).unwrap();
        assert!(success, "Insert operation failed");

        // Verify the insert worked
        let select_sql = "SELECT * FROM users";
        let mut writer = TestResultWriter::new();
        let success = ctx.engine.execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer).unwrap();
        assert!(success, "Select operation failed");
        assert_eq!(writer.get_rows().len(), 1, "Expected 1 row after insert");

        // Test INSERT with multiple rows
        let multi_insert_sql = "INSERT INTO users VALUES (2, 'Bob', 30), (3, 'Charlie', 35)";
        let mut writer = TestResultWriter::new();
        let success = ctx.engine.execute_sql(multi_insert_sql, ctx.exec_ctx.clone(), &mut writer).unwrap();
        assert!(success, "Multi-row insert operation failed");

        // Verify multiple inserts worked
        let select_sql = "SELECT * FROM users";
        let mut writer = TestResultWriter::new();
        let success = ctx.engine.execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer).unwrap();
        assert!(success, "Select operation failed");
        assert_eq!(writer.get_rows().len(), 3, "Expected 3 rows after multiple inserts");

        // Test INSERT with SELECT
        let create_temp_sql = "CREATE TABLE temp_users (id INTEGER, name VARCHAR, age INTEGER)";
        let mut writer = TestResultWriter::new();
        ctx.engine.execute_sql(create_temp_sql, ctx.exec_ctx.clone(), &mut writer).unwrap();

        let insert_temp_sql = "INSERT INTO temp_users VALUES (4, 'David', 40), (5, 'Eve', 45)";
        let mut writer = TestResultWriter::new();
        ctx.engine.execute_sql(insert_temp_sql, ctx.exec_ctx.clone(), &mut writer).unwrap();

        let insert_select_sql = "INSERT INTO users SELECT * FROM temp_users";
        let mut writer = TestResultWriter::new();
        let success = ctx.engine.execute_sql(insert_select_sql, ctx.exec_ctx.clone(), &mut writer).unwrap();
        assert!(success, "Insert with SELECT operation failed");

        // Verify INSERT with SELECT worked
        let select_sql = "SELECT * FROM users";
        let mut writer = TestResultWriter::new();
        let success = ctx.engine.execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer).unwrap();
        assert!(success, "Select operation failed");
        assert_eq!(writer.get_rows().len(), 5, "Expected 5 rows after INSERT with SELECT");
    }

    #[test]
    fn test_create_table_operations() {
        let mut ctx = TestContext::new("test_create_table_operations");

        // Test CREATE TABLE
        let create_sql = "CREATE TABLE test_table (id INTEGER, name VARCHAR(50), age INTEGER, active BOOLEAN)";
        let mut writer = TestResultWriter::new();
        let success = ctx.engine.execute_sql(create_sql, ctx.exec_ctx.clone(), &mut writer).unwrap();
        assert!(success, "Create table operation failed");

        // Verify table was created by inserting and selecting
        let insert_sql = "INSERT INTO test_table VALUES (1, 'Alice', 25, true)";
        let mut writer = TestResultWriter::new();
        let success = ctx.engine.execute_sql(insert_sql, ctx.exec_ctx.clone(), &mut writer).unwrap();
        assert!(success, "Insert operation failed");

        let select_sql = "SELECT * FROM test_table";
        let mut writer = TestResultWriter::new();
        let success = ctx.engine.execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer).unwrap();
        assert!(success, "Select operation failed");
        assert_eq!(writer.get_rows().len(), 1, "Expected 1 row in newly created table");
    }

    #[test]
    fn test_transaction_handling() {
        let mut ctx = TestContext::new("test_transaction_handling");

        // Create test table
        let table_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("balance", TypeId::Integer),
        ]);

        let table_name = "accounts";
        ctx.create_test_table(table_name, table_schema.clone()).unwrap();

        // Insert initial data
        let test_data = vec![
            vec![Value::new(1), Value::new("Alice"), Value::new(1000)],
            vec![Value::new(2), Value::new("Bob"), Value::new(500)],
        ];
        ctx.insert_tuples(table_name, test_data, table_schema).unwrap();

        // Start transaction
        let begin_sql = "BEGIN";
        let mut writer = TestResultWriter::new();
        let success = ctx.engine.execute_sql(begin_sql, ctx.exec_ctx.clone(), &mut writer).unwrap();
        assert!(success, "Begin transaction failed");

        // Update Alice's balance
        let update_sql = "UPDATE accounts SET balance = balance - 200 WHERE id = 1";
        let mut writer = TestResultWriter::new();
        let success = ctx.engine.execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer).unwrap();
        assert!(success, "Update operation failed");

        // Update Bob's balance
        let update_sql = "UPDATE accounts SET balance = balance + 200 WHERE id = 2";
        let mut writer = TestResultWriter::new();
        let success = ctx.engine.execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer).unwrap();
        assert!(success, "Update operation failed");

        // Commit transaction
        let commit_sql = "COMMIT";
        let mut writer = TestResultWriter::new();
        let success = ctx.engine.execute_sql(commit_sql, ctx.exec_ctx.clone(), &mut writer).unwrap();
        assert!(success, "Commit transaction failed");

        // Verify changes were committed
        let select_sql = "SELECT balance FROM accounts WHERE id = 1";
        let mut writer = TestResultWriter::new();
        let success = ctx.engine.execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer).unwrap();
        assert!(success, "Select operation failed");
        assert_eq!(writer.get_rows()[0][0].to_string(), "800", "Expected Alice's balance to be 800");

        let select_sql = "SELECT balance FROM accounts WHERE id = 2";
        let mut writer = TestResultWriter::new();
        let success = ctx.engine.execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer).unwrap();
        assert!(success, "Select operation failed");
        assert_eq!(writer.get_rows()[0][0].to_string(), "700", "Expected Bob's balance to be 700");

        // Test rollback
        let begin_sql = "BEGIN";
        let mut writer = TestResultWriter::new();
        let success = ctx.engine.execute_sql(begin_sql, ctx.exec_ctx.clone(), &mut writer).unwrap();
        assert!(success, "Begin transaction failed");

        // Update Alice's balance
        let update_sql = "UPDATE accounts SET balance = balance - 300 WHERE id = 1";
        let mut writer = TestResultWriter::new();
        let success = ctx.engine.execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer).unwrap();
        assert!(success, "Update operation failed");

        // Rollback transaction
        let rollback_sql = "ROLLBACK";
        let mut writer = TestResultWriter::new();
        let success = ctx.engine.execute_sql(rollback_sql, ctx.exec_ctx.clone(), &mut writer).unwrap();
        assert!(success, "Rollback transaction failed");

        // Verify changes were rolled back
        let select_sql = "SELECT balance FROM accounts WHERE id = 1";
        let mut writer = TestResultWriter::new();
        let success = ctx.engine.execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer).unwrap();
        assert!(success, "Select operation failed");
        assert_eq!(writer.get_rows()[0][0].to_string(), "800", "Expected Alice's balance to still be 800 after rollback");
    }
}
