use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::catalog::catalog::Catalog;
use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::result_writer::ResultWriter;
use crate::common::rid::RID;
use crate::concurrency::transaction_manager_factory::TransactionManagerFactory;
use crate::sql::execution::check_option::CheckOptions;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::PlanNode;
use crate::sql::execution::plans::mock_scan_plan::MockScanNode;
use crate::sql::optimizer::optimizer::Optimizer;
use crate::sql::planner::planner::{LogicalPlan, LogicalToPhysical, QueryPlanner};
use crate::types_db::value::Value;
use log::{debug, info};
use parking_lot::{Mutex, RawRwLock, RwLock};
use sqlparser::ast::Statement::{CreateIndex, CreateTable, Insert};
use std::sync::Arc;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::types_db::type_id::TypeId;
use sqlparser::parser::Parser;
use sqlparser::dialect::GenericDialect;

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
        context: Arc<RwLock<ExecutionContext>>,
        writer: &mut dyn ResultWriter,
    ) -> Result<bool, DBError> {
        // Create root executor
        let mut root_executor = self.create_executor(plan, context)?;

        info!("Initializing executor");
        root_executor.init();
        debug!("Executor initialization complete");

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
                debug!("Plan: {}", plan);

                let mut has_results = false;
                let mut row_count = 0;

                // Get schema information
                let schema = root_executor.get_output_schema();
                let columns = schema.get_columns();

                // Write schema header
                writer.write_schema_header(
                    columns
                        .iter()
                        .map(|col| {
                            if col.get_name().starts_with("sum_") {
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
                            }
                        })
                        .collect()
                );

                // Process rows
                debug!("Starting result processing");
                while let Some((tuple, _rid)) = root_executor.next() {
                    has_results = true;
                    row_count += 1;

                    if row_count % 1000 == 0 {
                        debug!("Processed {} rows", row_count);
                    }

                    writer.write_row(tuple.get_values().to_vec());
                }

                debug!(
                    "Result processing complete. Found {} matching rows",
                    row_count
                );

                info!("Query execution finished. Processed {} rows", row_count);
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
        plan.create_executor(context)
            .map_err(DBError::Execution)
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

    fn create_mock_scan(
        &self,
        input_schema: Schema,
        mock_tuples: Vec<(Vec<Value>, RID)>,
    ) -> PlanNode {
        PlanNode::MockScan(
            MockScanNode::new(
                input_schema.clone(),
                "mock_table".to_string(),
                vec![], // No children initially
            )
            .with_tuples(mock_tuples),
        )
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
}
