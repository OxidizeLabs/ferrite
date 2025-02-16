use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::catalog::catalog::Catalog;
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
use crate::sql::planner::planner::{LogicalPlan, LogicalToPhysical, QueryPlanner};
use crate::storage::table::tuple::TupleMeta;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use log::{debug, info};
use parking_lot::RwLock;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;

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
        debug!("Physical plan generated: \n{}", physical_plan.explain());

        Ok(physical_plan)
    }

    /// Execute a physical plan
    fn execute_plan(
        &self,
        plan: &PlanNode,
        context: Arc<RwLock<ExecutionContext>>,
        writer: &mut dyn ResultWriter,
    ) -> Result<bool, DBError> {
        let mut root_executor = self.create_executor(plan, context)?;
        root_executor.init();

        match plan {
            PlanNode::Insert(_) | PlanNode::CreateTable(_) | PlanNode::CreateIndex(_) => {
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
            _ => {
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

                while let Some((tuple, _)) = root_executor.next() {
                    has_results = true;
                    row_count += 1;
                    writer.write_row(tuple.get_values().to_vec());
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
        plan.create_executor(context).map_err(DBError::Execution)
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

    pub fn commit_transaction(&self, txn_ctx: Arc<TransactionContext>) -> Result<bool, DBError> {
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
}
