use sqlparser::ast::Statement;
use crate::catalog::catalog::Catalog;
use crate::common::exception::DBError;
use crate::common::result_writer::ResultWriter;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
use crate::optimizer::optimizer::Optimizer;
use crate::planner::planner::{LogicalPlan, LogicalToPhysical, QueryPlanner};
use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction_manager::TransactionManager;
use crate::execution::check_option::CheckOptions;
use log::{debug, info, warn};
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
        debug!("Initial logical plan generated: \n{}", logical_plan);

        // Get check options from context
        let check_options = {
            let ctx = context.read();
            ctx.get_check_options().clone()
        };

        // Optimize plan
        let physical_plan = self.optimize_plan(logical_plan)?;
        debug!("Physical plan generated: \n{}", physical_plan.display_physical_plan());

        Ok(physical_plan)
    }

    /// Execute a physical plan
    fn execute_plan(
        &self,
        plan: &PlanNode,
        context: Arc<RwLock<ExecutorContext>>,
        writer: &mut impl ResultWriter,
    ) -> Result<bool, DBError> {
        info!("Executing plan: {}", plan.display_physical_plan());

        // Create and initialize executor
        let mut executor = self.create_executor(plan, context)?;
        executor.init();

        // Process all tuples
        let mut has_results = false;
        while let Some((tuple, _)) = executor.next() {
            has_results = true;
            writer.write_tuple(&tuple).unwrap();
        }

        self.cleanup_after_execution();
        Ok(has_results)
    }

    /// Create an executor for the given plan
    fn create_executor(
        &self,
        plan: &PlanNode,
        context: Arc<RwLock<ExecutorContext>>,
    ) -> Result<Box<dyn AbstractExecutor>, DBError> {
        debug!("Creating executor for plan: {}", plan.display_physical_plan());
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
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
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
