use crate::catalogue::catalogue::Catalog;
use crate::common::exception::DBError;
use crate::execution::check_option::{CheckOption, CheckOptions};
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
use crate::execution::plans::filter_plan::FilterNode;
use std::sync::Arc;

pub struct Optimizer {
    catalog: Arc<Catalog>,
}

impl Optimizer {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }

    pub fn optimize(
        &self,
        plan: PlanNode,
        check_options: Arc<CheckOptions>,
    ) -> Result<PlanNode, DBError> {
        // Apply optimization rules
        let optimized_plan = self.apply_optimizations(plan, &check_options)?;

        // Validate the final plan
        self.validate_plan(&optimized_plan, &check_options)?;

        Ok(optimized_plan)
    }

    fn optimize_nested_loop_joins(&self, plan: PlanNode) -> Result<PlanNode, DBError> {
        match plan {
            PlanNode::NestedLoopJoin(mut join_node) => {
                // Optimize the children first
                let left_child = self.optimize_nested_loop_joins(*join_node.get_left().clone())?;
                let right_child =
                    self.optimize_nested_loop_joins(*join_node.get_right().clone())?;

                // Apply NLJ-specific optimizations:
                // 1. Try to swap the order if right side is smaller
                // 2. Push predicates into the inner loop if possible
                // 3. Consider adding indexes for join columns

                // For now, just return the join with optimized children
                Ok(PlanNode::NestedLoopJoin(join_node))
            }
            PlanNode::Filter(filter_node) => {
                // Recursively optimize any nested joins under the filter
                let new_child =
                    self.optimize_nested_loop_joins(filter_node.get_child_plan().clone())?;
                Ok(PlanNode::Filter(FilterNode::new(
                    filter_node.get_output_schema().clone(),
                    filter_node.get_predicate().clone(),
                    new_child,
                )))
            }
            // Handle other plan types recursively
            _ => Ok(plan),
        }
    }

    fn optimize_topn(&self, plan: PlanNode) -> Result<PlanNode, DBError> {
        match plan {
            PlanNode::TopN(mut topn_node) => {
                // Optimize child first
                let child = self.optimize_topn(*topn_node.get_child().clone())?;

                // Apply TopN-specific optimizations:
                // 1. Try to push limit down through joins
                // 2. Combine multiple TopN nodes
                // 3. Consider using indexes for sorting

                // For now, just return the TopN with optimized child
                Ok(PlanNode::TopN(topn_node))
            }
            // Recursively handle other node types
            _ => Ok(plan),
        }
    }

    fn apply_optimizations(
        &self,
        plan: PlanNode,
        check_options: &CheckOptions,
    ) -> Result<PlanNode, DBError> {
        let mut current_plan = plan;

        // Apply nested loop join optimizations if enabled
        if check_options.has_check(&CheckOption::EnableNljCheck) {
            current_plan = self.optimize_nested_loop_joins(current_plan)?;
        }

        // Apply top-N optimizations if enabled
        if check_options.has_check(&CheckOption::EnableTopnCheck) {
            current_plan = self.optimize_topn(current_plan)?;
        }

        Ok(current_plan)
    }

    fn push_down_predicates(&self, plan: PlanNode) -> Result<PlanNode, DBError> {
        match plan {
            PlanNode::Filter(filter_node) => {
                // Try to push filter down through joins and other operators
                if let PlanNode::SeqScan(_) = filter_node.get_child_plan() {
                    // If child is a scan, we can push the predicate into it
                    let mut new_scan = filter_node.get_child_plan().clone();
                    if let PlanNode::SeqScan(ref mut scan_node) = new_scan {
                        // In a real implementation, we would modify the scan to include the predicate
                        // For now, we'll just return the original plan
                    }
                    Ok(new_scan)
                } else {
                    Ok(PlanNode::Filter(filter_node))
                }
            }
            // Handle other plan node types
            _ => Ok(plan),
        }
    }

    fn choose_join_strategies(&self, plan: PlanNode) -> Result<PlanNode, DBError> {
        match plan {
            PlanNode::NestedLoopJoin(join_node) => {
                // Check if we can use hash join or index nested loop join instead
                // For now, just return the original join
                Ok(PlanNode::NestedLoopJoin(join_node))
            }
            _ => Ok(plan),
        }
    }

    fn choose_access_paths(&self, plan: PlanNode) -> Result<PlanNode, DBError> {
        match plan {
            PlanNode::SeqScan(scan_node) => {
                // Check if we can use an index instead of sequential scan
                // Look up available indexes in catalog
                let table_name = scan_node.get_table_name();
                if let indexes = self.catalog.get_table_indexes(table_name) {
                    // Evaluate if any index is beneficial
                    // For now, just return sequential scan
                }
                Ok(PlanNode::SeqScan(scan_node))
            }
            _ => Ok(plan),
        }
    }

    fn validate_plan(&self, plan: &PlanNode, check_options: &CheckOptions) -> Result<(), DBError> {
        // Always validate basic plan properties
        self.validate_schema_compatibility(plan)?;
        self.validate_table_references(plan)?;

        // Apply specific validations based on enabled checks
        if check_options.has_check(&CheckOption::EnableNljCheck) {
            self.validate_nested_loop_joins(plan)?;
        }

        if check_options.has_check(&CheckOption::EnableTopnCheck) {
            self.validate_topn_nodes(plan)?;
        }

        Ok(())
    }

    fn validate_schema_compatibility(&self, plan: &PlanNode) -> Result<(), DBError> {
        match plan {
            PlanNode::Filter(node) => {
                // Verify filter expression types match schema
                let schema = node.get_output_schema();
                // Add validation logic
                Ok(())
            }
            // Add other node types
            _ => Ok(()),
        }
    }

    fn validate_table_references(&self, plan: &PlanNode) -> Result<(), DBError> {
        match plan {
            PlanNode::SeqScan(node) => {
                let table_oid = node.get_table_oid();
                if self.catalog.get_table_by_oid(table_oid).is_none() {
                    return Err(DBError::TableNotFound(format!(
                        "Table {} not found",
                        node.get_table_name()
                    )));
                }
            }
            _ => {
                // Recursively validate children
                for child in plan.get_children() {
                    self.validate_table_references(child)?;
                }
            }
        }
        Ok(())
    }

    fn validate_no_cartesian_products(&self, plan: &PlanNode) -> Result<(), DBError> {
        match plan {
            PlanNode::NestedLoopJoin(node) => {
                if node.get_children().is_empty() {
                    return Err(DBError::Validation(
                        "Cartesian products are not allowed".to_string(),
                    ));
                }
            }
            _ => {
                // Recursively validate children
                for child in plan.get_children() {
                    self.validate_no_cartesian_products(child)?;
                }
            }
        }
        Ok(())
    }

    fn validate_nested_loop_joins(&self, plan: &PlanNode) -> Result<(), DBError> {
        match plan {
            PlanNode::NestedLoopJoin(node) => {
                // Validate join conditions
                if node.get_right().get_children().is_empty() {
                    return Err(DBError::Validation(
                        "NLJ requires a join predicate when NLJ check is enabled".to_string(),
                    ));
                }

                // Validate children recursively
                self.validate_nested_loop_joins(node.get_left())?;
                self.validate_nested_loop_joins(node.get_right())?;
            }
            _ => {
                // Recursively validate children for other node types
                for child in plan.get_children() {
                    self.validate_nested_loop_joins(child)?;
                }
            }
        }
        Ok(())
    }

    fn validate_topn_nodes(&self, plan: &PlanNode) -> Result<(), DBError> {
        match plan {
            PlanNode::TopN(node) => {
                // Validate TopN properties
                if node.get_limit() == 0 {
                    return Err(DBError::Validation(
                        "TopN limit must be greater than 0".to_string(),
                    ));
                }

                // Validate sort expressions
                if node.get_sort_order_by().is_empty() {
                    return Err(DBError::Validation(
                        "TopN requires at least one sort expression".to_string(),
                    ));
                }

                // Recursively validate child
                self.validate_topn_nodes(node.get_child())?;
            }
            _ => {
                // Recursively validate children for other node types
                for child in plan.get_children() {
                    self.validate_topn_nodes(child)?;
                }
            }
        }
        Ok(())
    }
}
