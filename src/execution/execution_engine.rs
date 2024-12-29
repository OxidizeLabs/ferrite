use crate::catalogue::catalogue::Catalog;
use crate::common::db_instance::ResultWriter;
use crate::common::exception::DBError;
use crate::execution::check_option::CheckOptions;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::execution::executors::aggregation_executor::AggregationExecutor;
use crate::execution::executors::create_table_executor::CreateTableExecutor;
use crate::execution::executors::filter_executor::FilterExecutor;
use crate::execution::executors::insert_executor::InsertExecutor;
use crate::execution::executors::seq_scan_executor::SeqScanExecutor;
use crate::execution::executors::table_scan_executor::TableScanExecutor;
use crate::execution::executors::values_executor::ValuesExecutor;
use crate::execution::plans::abstract_plan::PlanNode::*;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
use crate::optimizer::optimizer::Optimizer;
use crate::planner::planner::QueryPlanner;
use log::{debug, info, warn};
use parking_lot::RwLock;
use std::env;
use std::sync::Arc;

pub struct ExecutorEngine {
    // buffer_pool_manager: Arc<BufferPoolManager>,
    // catalog: Arc<RwLock<Catalog>>,
    planner: QueryPlanner,
    optimizer: Optimizer,
    log_detailed: bool,
}

impl ExecutorEngine {
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        Self {
            planner: QueryPlanner::new(catalog.clone()),
            optimizer: Optimizer::new(catalog),
            log_detailed: env::var("RUST_TEST").is_ok(),
        }
    }

    /// Prepares an SQL statement for execution
    pub fn prepare_statement(
        &mut self,
        sql: &str,
        check_options: Arc<CheckOptions>,
    ) -> Result<PlanNode, DBError> {
        info!("Preparing statement: {}", sql);

        // Generate initial plan using our QueryPlanner
        let initial_plan = match self.planner.create_plan(sql) {
            Ok(plan) => {
                if self.log_detailed {
                    debug!("Initial plan generated: {:?}", plan);
                }
                plan
            }
            Err(e) => {
                warn!("Failed to create plan: {}", e);
                return Err(DBError::PlanError(e));
            }
        };

        // Optimize the plan
        if check_options.is_modify() {
            info!("Optimizing plan with modification checks");
            self.optimizer.optimize(initial_plan, check_options)
        } else {
            if self.log_detailed {
                debug!("Skipping optimization for read-only query");
            }
            Ok(initial_plan)
        }
    }

    /// Executes a prepared statement
    pub fn execute_statement(
        &self,
        plan: &PlanNode,
        context: Arc<RwLock<ExecutorContext>>,
        writer: &mut impl ResultWriter,
    ) -> Result<bool, DBError> {
        info!("Starting execution of plan: {:?}", plan.get_type());

        let result = self.execute_statement_internal(plan, context, writer);

        match &result {
            Ok(has_results) => {
                info!(
                    "Statement execution completed successfully. Has results: {}",
                    has_results
                );
                debug!("Releasing all resources and returning control to CLI");
            }
            Err(e) => {
                warn!("Statement execution failed: {:?}", e);
                debug!("Cleaning up after execution failure");
            }
        }

        // Ensure proper cleanup happens even on success
        self.cleanup_after_execution();

        result
    }

    /// Creates appropriate executor for a plan node
    fn create_executor(
        &self,
        plan: &PlanNode,
        context: Arc<RwLock<ExecutorContext>>,
    ) -> Result<Box<dyn AbstractExecutor>, DBError> {
        debug!("Creating executor for plan type: {:?}", plan.get_type());
        match plan {
            Insert(insert_plan) => {
                info!("Creating insert executor");
                let executor = InsertExecutor::new(context, Arc::new(insert_plan.clone()));
                Ok(Box::new(executor))
            }
            SeqScan(scan_plan) => {
                info!("Creating sequential scan executor");
                let executor = SeqScanExecutor::new(context, Arc::new(scan_plan.clone()));
                Ok(Box::new(executor))
            }
            CreateTable(create_plan) => {
                info!("Creating table creation executor");
                let executor =
                    CreateTableExecutor::new(context, Arc::from(create_plan.clone()), false);
                Ok(Box::new(executor))
            }
            Filter(filter_plan) => {
                info!("Creating filter executor for WHERE clause");
                debug!("Filter predicate: {:?}", filter_plan.get_filter_predicate());
                // Create child executor first
                let child_executor =
                    self.create_executor(filter_plan.get_child_plan(), context.clone())?;
                let executor =
                    FilterExecutor::new(child_executor, context, Arc::new(filter_plan.clone()));
                Ok(Box::new(executor))
            }
            Values(values_plan) => {
                info!("Creating values executor");
                let executor = ValuesExecutor::new(context, Arc::new(values_plan.clone()));
                Ok(Box::new(executor))
            }
            TableScan(table_scan_plan) => {
                info!("Creating table scanner");
                let executor = TableScanExecutor::new(context, Arc::new(table_scan_plan.clone()));
                Ok(Box::new(executor))
            }
            Aggregation(aggregation_plan) => {
                info!("Creating aggregation executor");
                // Create child executor first
                let child_executor =
                    self.create_executor(&aggregation_plan.get_child_plan(), context.clone())?;

                let executor = AggregationExecutor::new(
                    child_executor,
                    Arc::new(aggregation_plan.clone()),
                    context,
                );
                Ok(Box::new(executor))
            }
            _ => {
                warn!("Unsupported plan type: {:?}", plan.get_type());
                Err(DBError::NotImplemented(format!(
                    "Executor type {:?} not implemented",
                    plan.get_type()
                )))
            }
        }
    }

    fn execute_statement_internal(
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

        // Handle different types of statements
        match plan {
            Insert(_) | CreateTable(_) => {
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
                let mut has_results = false;
                let mut row_count = 0;

                // Get schema information
                let column_count = root_executor.get_output_schema().get_column_count();
                let column_names: Vec<String> = root_executor
                    .get_output_schema()
                    .get_columns()
                    .iter()
                    .map(|col| col.get_name().to_string())
                    .collect();

                debug!("Writing output schema with {} columns", column_count);
                writer.begin_table(true);
                writer.begin_header();
                for name in &column_names {
                    writer.write_header_cell(name);
                }
                writer.end_header();

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

    fn cleanup_after_execution(&self) {
        debug!("Starting post-execution cleanup");

        // Add any necessary cleanup logic here
        // For example:
        // - Release any held locks
        // - Clear any temporary resources
        // - Reset any execution state

        debug!("Post-execution cleanup complete");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::common::db_instance::ResultWriter;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::recovery::log_manager::LogManager;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;

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

        fn begin_header(&mut self) {
            self.in_header = true;
        }

        fn write_header_cell(&mut self, value: &str) {
            if self.in_header {
                self.headers.push(value.to_string());
            }
        }

        fn end_header(&mut self) {
            self.in_header = false;
        }

        fn begin_row(&mut self) {
            self.in_row = true;
            self.current_row.clear();
        }

        fn write_cell(&mut self, value: &str) {
            if self.in_row {
                self.current_row.push(value.to_string());
            }
        }

        fn end_row(&mut self) {
            self.in_row = false;
            self.rows.push(self.current_row.clone());
        }

        fn end_table(&mut self) {}

        fn one_cell(&mut self, content: &str) {
            println!("{}", content);
        }
    }

    struct TestContext {
        catalog: Arc<RwLock<Catalog>>,
        buffer_pool_manager: Arc<BufferPoolManager>,
        transaction_manager: Arc<RwLock<TransactionManager>>,
        lock_manager: Arc<LockManager>,
        executor_engine: ExecutorEngine,
        executor_context: Arc<RwLock<ExecutorContext>>,
        db_file: String,
        log_file: String,
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            init_test_logger();
            let timestamp = chrono::Utc::now().timestamp();
            let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
            let log_file = format!("tests/data/{}_{}.log", test_name, timestamp);

            let disk_manager =
                Arc::new(FileDiskManager::new(db_file.clone(), log_file.clone(), 100));
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
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
            let executor_engine = ExecutorEngine::new(catalog.clone());

            // Create test transaction and executor context
            let transaction = Arc::new(Transaction::new(1, IsolationLevel::ReadCommitted));
            let executor_context = Arc::new(RwLock::new(ExecutorContext::new(
                transaction,
                transaction_manager.clone(),
                catalog.clone(),
                buffer_pool_manager.clone(),
                lock_manager.clone(),
            )));

            Self {
                catalog,
                buffer_pool_manager,
                transaction_manager,
                lock_manager,
                executor_engine,
                executor_context,
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

    #[test]
    fn test_create_table() {
        let mut ctx = TestContext::new("create_table");
        let sql = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name VARCHAR(255))";
        let check_options = Arc::new(CheckOptions::new());

        let plan = ctx
            .executor_engine
            .prepare_statement(sql, check_options)
            .unwrap();
        let mut writer = MockResultWriter::new();

        let result =
            ctx.executor_engine
                .execute_statement(&plan, ctx.executor_context.clone(), &mut writer);

        assert!(result.is_ok());
        assert!(ctx.catalog.read().get_table("test_table").is_some());
    }

    #[test]
    fn test_insert_and_scan() {
        let mut ctx = TestContext::new("insert_scan");

        // First create table
        let create_sql = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name VARCHAR(255))";
        let check_options = Arc::new(CheckOptions::new());
        let create_plan = ctx
            .executor_engine
            .prepare_statement(create_sql, check_options.clone())
            .unwrap();
        let mut writer = MockResultWriter::new();
        ctx.executor_engine
            .execute_statement(&create_plan, ctx.executor_context.clone(), &mut writer)
            .unwrap();

        // Then insert data
        let insert_sql = "INSERT INTO test_table VALUES (1, 'test')";
        let insert_plan = ctx
            .executor_engine
            .prepare_statement(insert_sql, check_options.clone())
            .unwrap();
        ctx.executor_engine
            .execute_statement(&insert_plan, ctx.executor_context.clone(), &mut writer)
            .unwrap();

        // Finally scan
        let scan_sql = "SELECT * FROM test_table";
        let scan_plan = ctx
            .executor_engine
            .prepare_statement(scan_sql, check_options)
            .unwrap();
        let mut writer = MockResultWriter::new();
        let result = ctx.executor_engine.execute_statement(
            &scan_plan,
            ctx.executor_context.clone(),
            &mut writer,
        );

        assert!(result.is_ok());
        assert_eq!(writer.rows.len(), 1);
        assert_eq!(writer.rows[0][0], "1");
        assert_eq!(writer.rows[0][1], "test");
    }

    #[test]
    fn test_filter_execution() {
        let mut ctx = TestContext::new("filter");

        // Create and populate table
        let create_sql = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value INTEGER)";
        let check_options = Arc::new(CheckOptions::new());
        let create_plan = ctx
            .executor_engine
            .prepare_statement(create_sql, check_options.clone())
            .unwrap();
        let mut writer = MockResultWriter::new();
        ctx.executor_engine
            .execute_statement(&create_plan, ctx.executor_context.clone(), &mut writer)
            .unwrap();

        let insert_sql = "INSERT INTO test_table VALUES (1, 100), (2, 200), (3, 300)";
        let insert_plan = ctx
            .executor_engine
            .prepare_statement(insert_sql, check_options.clone())
            .unwrap();
        ctx.executor_engine
            .execute_statement(&insert_plan, ctx.executor_context.clone(), &mut writer)
            .unwrap();

        // Test filter
        let filter_sql = "SELECT * FROM test_table WHERE value > 150";
        let filter_plan = ctx
            .executor_engine
            .prepare_statement(filter_sql, check_options)
            .unwrap();
        let mut writer = MockResultWriter::new();
        let result = ctx.executor_engine.execute_statement(
            &filter_plan,
            ctx.executor_context.clone(),
            &mut writer,
        );

        assert!(result.is_ok());
        assert_eq!(writer.rows.len(), 2); // Should only return rows with value > 150
    }

    #[test]
    fn test_aggregation() {
        let mut ctx = TestContext::new("aggregation");

        // Create and populate table
        let create_sql = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value INTEGER)";
        let check_options = Arc::new(CheckOptions::new());
        let create_plan = ctx
            .executor_engine
            .prepare_statement(create_sql, check_options.clone())
            .unwrap();
        let mut writer = MockResultWriter::new();
        ctx.executor_engine
            .execute_statement(&create_plan, ctx.executor_context.clone(), &mut writer)
            .unwrap();

        let insert_sql = "INSERT INTO test_table VALUES (1, 100), (2, 200), (3, 300)";
        let insert_plan = ctx
            .executor_engine
            .prepare_statement(insert_sql, check_options.clone())
            .unwrap();
        ctx.executor_engine
            .execute_statement(&insert_plan, ctx.executor_context.clone(), &mut writer)
            .unwrap();

        // Test aggregation
        let agg_sql = "SELECT COUNT(*), SUM(value) FROM test_table";
        let agg_plan = ctx
            .executor_engine
            .prepare_statement(agg_sql, check_options)
            .unwrap();
        let mut writer = MockResultWriter::new();
        let result = ctx.executor_engine.execute_statement(
            &agg_plan,
            ctx.executor_context.clone(),
            &mut writer,
        );

        assert!(result.is_ok());
        assert_eq!(writer.rows.len(), 1);
        assert_eq!(writer.rows[0][0], "3"); // COUNT(*)
        assert_eq!(writer.rows[0][1], "600"); // SUM(value)
    }

    #[test]
    fn test_error_handling() {
        let mut ctx = TestContext::new("error_handling");
        let check_options = Arc::new(CheckOptions::new());

        // Test invalid SQL
        let invalid_sql = "SELECT * FROM nonexistent_table";
        let result = ctx
            .executor_engine
            .prepare_statement(invalid_sql, check_options);
        assert!(result.is_err());
    }
}
