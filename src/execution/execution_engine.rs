use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::catalogue::catalogue::Catalog;
use crate::common::db_instance::ResultWriter;
use crate::common::exception::DBError;
use crate::execution::check_option::CheckOptions;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_exector::AbstractExecutor;
use crate::execution::executors::create_table_executor::CreateTableExecutor;
use crate::execution::executors::seq_scan_executor::SeqScanExecutor;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
use crate::optimizer::optimizer::Optimizer;
use crate::planner::planner::QueryPlanner;
use log::{debug, info, warn};
use parking_lot::RwLock;
use std::sync::Arc;
use std::env;

pub struct ExecutorEngine {
    buffer_pool_manager: Arc<BufferPoolManager>,
    catalog: Arc<RwLock<Catalog>>,
    planner: QueryPlanner,
    optimizer: Optimizer,
    log_detailed: bool,
}

impl ExecutorEngine {
    pub fn new(buffer_pool_manager: Arc<BufferPoolManager>, catalog: Arc<RwLock<Catalog>>) -> Self {
        Self {
            buffer_pool_manager,
            catalog: catalog.clone(),
            planner: QueryPlanner::new(),
            optimizer: Optimizer::new(catalog),
            log_detailed: env::var("RUST_TEST").is_ok(),
        }
    }

    /// Prepares an SQL statement for execution
    pub fn prepare_statement(
        &self,
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
                info!("Statement execution completed successfully. Has results: {}", has_results);
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
            PlanNode::SeqScan(scan_plan) => {
                info!("Creating sequential scan executor");
                Ok(Box::new(SeqScanExecutor::new(context, Arc::new(scan_plan.clone()))))
            }
            PlanNode::CreateTable(create_table_plan) => {
                info!("Creating table creation executor");
                Ok(Box::new(CreateTableExecutor::new(
                    Arc::new(context),
                    create_table_plan.clone(),
                    false,
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
                writer.write_cell(&tuple.get_value(i as usize).to_string());
            }
            writer.end_row();
        }

        debug!("Result processing complete");
        writer.end_table();

        info!("Statement execution finished. Processed {} rows", row_count);
        Ok(has_results)
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
    use std::env;

    // Add test setup helper
    fn setup_test_engine() -> ExecutorEngine {
        env::set_var("RUST_TEST", "1");
        // Create necessary components for ExecutorEngine
        // This is a placeholder - you'll need to provide actual implementations
        unimplemented!("Need to implement test setup");
    }

    #[test]
    fn test_prepare_statement() {
        let engine = setup_test_engine();
        // Add test implementation
    }

    #[test]
    fn test_execute_statement() {
        let engine = setup_test_engine();
        // Add test implementation
    }
}