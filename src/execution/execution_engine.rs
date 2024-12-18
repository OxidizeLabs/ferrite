use crate::catalogue::catalogue::Catalog;
use crate::common::db_instance::ResultWriter;
use crate::common::exception::DBError;
use crate::execution::check_option::CheckOptions;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_executor::AbstractExecutor;
use crate::execution::executors::create_table_executor::CreateTableExecutor;
use crate::execution::executors::insert_executor::InsertExecutor;
use crate::execution::executors::seq_scan_executor::SeqScanExecutor;
use crate::execution::plans::abstract_plan::PlanNode::{CreateTable, Filter, Insert, SeqScan};
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
use crate::optimizer::optimizer::Optimizer;
use crate::planner::planner::QueryPlanner;
use log::{debug, info, warn};
use parking_lot::RwLock;
use std::env;
use std::sync::Arc;
use crate::execution::executors::filter_executor::FilterExecutor;

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
        context: ExecutorContext,
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
        context: ExecutorContext,
    ) -> Result<Box<dyn AbstractExecutor>, DBError> {
        if self.log_detailed {
            debug!("Creating executor for plan type: {:?}", plan.get_type());
        }

        match plan {
            SeqScan(scan_plan) => {
                info!("Creating sequential scan executor");
                Ok(Box::new(SeqScanExecutor::new(
                    Arc::from(context),
                    Arc::new(scan_plan.clone()),
                )))
            }
            CreateTable(create_table_plan) => {
                info!("Creating table creation executor");
                Ok(Box::new(CreateTableExecutor::new(
                    Arc::new(context),
                    create_table_plan.clone(),
                    false,
                )))
            }
            Insert(insert_plan) => {
                info!("Creating insert executor");
                Ok(Box::new(InsertExecutor::new(
                    Arc::new(context),
                    Arc::new(insert_plan.clone()),
                )))
            }
            Filter(filter_plan) => {
                info!("Creating filter executor for WHERE clause");
                debug!("Filter predicate: {:?}", filter_plan.get_filter_predicate());
                Ok(Box::new(FilterExecutor::new(
                    Arc::new(context),
                    Arc::new(filter_plan.clone()),
                )))
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
        context: ExecutorContext,
        writer: &mut impl ResultWriter,
    ) -> Result<bool, DBError> {
        // Create root executor
        let mut root_executor = self.create_executor(plan, context)?;

        info!("Initializing executor");
        root_executor.init();
        debug!("Executor initialization complete");

        // Handle different types of statements
        match plan {
            // For INSERT, CREATE TABLE, etc. - just execute and return success
            PlanNode::Insert(_) | PlanNode::CreateTable(_) => {
                debug!("Executing modification statement");
                if root_executor.next().is_some() {
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

                debug!("Result processing complete. Found {} matching rows", row_count);
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::catalogue::catalogue::TestContext;
//     use crate::common::db_instance::ResultWriter;
//     use std::collections::HashMap;
//     use parking_lot::Mutex;
//     use crate::concurrency::transaction::{IsolationLevel, Transaction};
//     use crate::concurrency::transaction_manager::TransactionManager;
//
//     #[test]
//     fn test_select_with_where_clause() {
//         // Set up test context
//         let test_context = TestContext::new("select_where_test");
//         let catalog = Arc::new(RwLock::new(Catalog::new(
//             test_context.bpm(),
//             0,
//             0,
//             HashMap::new(),
//             HashMap::new(),
//             HashMap::new(),
//             HashMap::new(),
//         )));
//
//         // Create table and insert test data
//         let mut engine = ExecutorEngine::new(catalog.clone());
//
//         // Create table
//         let create_sql = "CREATE TABLE test_table (id INTEGER, name VARCHAR(255), age INTEGER)";
//         let create_plan = engine.prepare_statement(create_sql, Arc::new(CheckOptions::new())).unwrap();
//         let mut writer = ResultWriter::new();
//         let context = ExecutorContext::new(
//             Arc::new(Mutex::new(Transaction::new(0, IsolationLevel::ReadUncommitted))),
//             Arc::new(Mutex::new(TransactionManager::new(catalog.clone()))),
//             catalog.clone(),
//             test_context.bpm(),
//             test_context.lock_manager(),
//         );
//         engine.execute_statement(&create_plan, context, &mut writer).unwrap();
//
//         // Insert test data
//         let insert_sql = "INSERT INTO test_table VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35)";
//         let insert_plan = engine.prepare_statement(insert_sql, Arc::new(CheckOptions::new())).unwrap();
//         let context = ExecutorContext::new(
//             Arc::new(Mutex::new(Transaction::new(1, IsolationLevel::ReadUncommitted))),
//             Arc::new(Mutex::new(TransactionManager::new(catalog.clone()))),
//             catalog.clone(),
//             test_context.bpm(),
//             test_context.lock_manager(),
//         );
//         engine.execute_statement(&insert_plan, context, &mut writer).unwrap();
//
//         // Test SELECT with WHERE clause
//         let select_sql = "SELECT * FROM test_table WHERE age > 30";
//         let select_plan = engine.prepare_statement(select_sql, Arc::new(CheckOptions::new())).unwrap();
//         let mut result_writer = ResultWriter::new();
//         let context = ExecutorContext::new(
//             Arc::new(Mutex::new(Transaction::new(2, IsolationLevel::ReadUncommitted))),
//             Arc::new(Mutex::new(TransactionManager::new(catalog.clone()))),
//             catalog.clone(),
//             test_context.bpm(),
//             test_context.lock_manager(),
//         );
//
//         let success = engine.execute_statement(&select_plan, context, &mut result_writer).unwrap();
//         assert!(success, "Query should return results");
//
//         let result = result_writer.get_output();
//         assert!(result.contains("Charlie"));
//         assert!(result.contains("35"));
//         assert!(!result.contains("Bob"));
//         assert!(!result.contains("Alice"));
//     }
// }
