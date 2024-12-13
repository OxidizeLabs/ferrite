use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::catalogue::catalogue::Catalog;
use crate::common::db_instance::ResultWriter;
use crate::common::exception::DBError;
use crate::execution::check_option::CheckOptions;
use crate::execution::executor_context::ExecutorContext;
use crate::execution::executors::abstract_exector::AbstractExecutor;
use crate::execution::executors::seq_scan_executor::SeqScanExecutor;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
// use crate::execution::executors::insert_executor::InsertExecutor;
// use crate::execution::executors::delete_executor::DeleteExecutor;
// use crate::execution::executors::nested_loop_join_executor::NestedLoopJoinExecutor;
// use crate::execution::executors::aggregation_executor::AggregationExecutor;
use crate::optimizer::optimizer::Optimizer;
use crate::planner::planner::QueryPlanner;
use std::sync::Arc;

pub struct ExecutorEngine {
    buffer_pool_manager: Arc<BufferPoolManager>,
    catalog: Arc<Catalog>,
    planner: QueryPlanner,
    optimizer: Optimizer,
}

impl ExecutorEngine {
    pub fn new(buffer_pool_manager: Arc<BufferPoolManager>, catalog: Arc<Catalog>) -> Self {
        Self {
            buffer_pool_manager,
            catalog: catalog.clone(),
            planner: QueryPlanner::new(),
            optimizer: Optimizer::new(catalog),
        }
    }

    /// Prepares an SQL statement for execution
    pub fn prepare_statement(
        &self,
        sql: &str,
        check_options: Arc<CheckOptions>,
    ) -> Result<PlanNode, DBError> {
        // Generate initial plan using our QueryPlanner
        let initial_plan = self
            .planner
            .create_plan(sql)
            .map_err(|e| DBError::PlanError(e))?;

        // Optimize the plan
        if check_options.is_modify() {
            self.optimizer.optimize(initial_plan, check_options)
        } else {
            Ok(initial_plan)
        }
    }

    /// Executes a prepared statement
    pub fn execute_statement(
        &self,
        plan: &PlanNode,
        mut context: ExecutorContext,
        writer: &mut impl ResultWriter,
    ) -> Result<bool, DBError> {
        // Create root executor
        let mut root_executor = self.create_executor(plan, context)?;

        // Initialize the executor
        root_executor.init();

        let mut has_results = false;

        // Get schema information before starting iteration
        let column_count = root_executor.get_output_schema().get_column_count();
        let column_names: Vec<String> = root_executor
            .get_output_schema()
            .get_columns()
            .iter()
            .map(|col| col.get_name().to_string())
            .collect();

        // Write column headers
        writer.begin_table(true);
        writer.begin_header();
        for name in &column_names {
            writer.write_header_cell(name);
        }
        writer.end_header();

        // Execute and write results
        while let Some((tuple, _rid)) = root_executor.next() {
            has_results = true;
            writer.begin_row();
            for i in 0..column_count {
                writer.write_cell(&tuple.get_value(i as usize).to_string());
            }
            writer.end_row();
        }

        writer.end_table();
        Ok(has_results)
    }

    /// Creates appropriate executor for a plan node
    fn create_executor(
        &self,
        plan: &PlanNode,
        context: ExecutorContext,
    ) -> Result<Box<dyn AbstractExecutor>, DBError> {
        match plan {
            PlanNode::SeqScan(scan_plan) => {
                Ok(Box::new(SeqScanExecutor::new(context, scan_plan.clone())))
            }
            _ => Err(DBError::NotImplemented(format!(
                "Executor type {:?} not implemented",
                plan.get_type()
            ))),

            // PlanType::Insert => {
            //     let insert_plan = plan.as_insert()?;
            //     let table = self.catalog.get_table(insert_plan.get_table_name())?;
            //     let child_executor = self.create_executor(insert_plan.get_child(), context)?;
            //
            //     Ok(Box::new(InsertExecutor::new(
            //         context,
            //         table,
            //         child_executor,
            //     )))
            // }
            //
            // PlanType::Delete => {
            //     let delete_plan = plan.as_delete()?;
            //     let table = self.catalog.get_table(delete_plan.get_table_name())?;
            //     let child_executor = self.create_executor(delete_plan.get_child(), context)?;
            //
            //     context.set_delete(true);
            //     Ok(Box::new(DeleteExecutor::new(
            //         context,
            //         table,
            //         child_executor,
            //     )))
            // }
            //
            // PlanType::NestedLoopJoin => {
            //     let join_plan = plan.as_nested_loop_join()?;
            //     let left_child = self.create_executor(join_plan.get_left_child(), context)?;
            //     let right_child = self.create_executor(join_plan.get_right_child(), context)?;
            //
            //     if context.get_check_options().has_check(&CheckOption::EnableNljCheck) {
            //         context.add_check_option(left_child.clone(), right_child.clone());
            //     }
            //
            //     Ok(Box::new(NestedLoopJoinExecutor::new(
            //         context,
            //         join_plan.get_predicate().clone(),
            //         left_child,
            //         right_child,
            //         join_plan.get_output_schema().clone(),
            //     )))
            // }
            //
            // PlanType::Aggregation => {
            //     let agg_plan = plan.as_aggregation()?;
            //     let child_executor = self.create_executor(agg_plan.get_child(), context)?;
            //
            //     Ok(Box::new(AggregationExecutor::new(
            //         context,
            //         agg_plan.get_group_bys().clone(),
            //         agg_plan.get_aggregates().clone(),
            //         child_executor,
            //         agg_plan.get_output_schema().clone(),
            //     )))
            // }

            // Add other executor types as needed
            _ => Err(DBError::NotImplemented(format!(
                "Executor type {:?} not implemented",
                plan.get_type()
            ))),
        }
    }
}
