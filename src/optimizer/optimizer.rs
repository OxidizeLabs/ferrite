use crate::catalogue::catalogue::Catalog;
use crate::catalogue::schema::Schema;
use crate::common::exception::DBError;
use crate::execution::check_option::{CheckOption, CheckOptions};
use crate::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::execution::expressions::logic_expression::{LogicExpression, LogicType};
use crate::planner::planner::{LogicalPlan, LogicalPlanType};
use crate::types_db::value::Val;
use log::{debug, info, trace, warn};
use parking_lot::RwLock;
use std::sync::Arc;


pub struct Optimizer {
    catalog: Arc<RwLock<Catalog>>,
}

impl Optimizer {
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        Self { catalog }
    }

    /// Main entry point for optimization
    pub fn optimize(
        &self,
        logical_plan: Box<LogicalPlan>,
        check_options: Arc<CheckOptions>,
    ) -> Result<Box<LogicalPlan>, DBError> {
        info!("Starting query optimization");
        debug!("Initial plan:\n{:?}", logical_plan);

        let mut plan = logical_plan;

        if check_options.is_modify() {
            debug!("Modifications enabled, applying optimization phases");

            // Phase 1: Index scan conversion and early pruning
            trace!("Phase 1: Starting index scan optimization");
            plan = self.apply_index_scan_optimization(plan)?;
            trace!("Phase 1: Starting early pruning");
            plan = self.apply_early_pruning(plan)?;

            // Phase 2: Rewrite rules
            trace!("Phase 2: Starting rewrite rules application");
            plan = self.apply_rewrite_rules(plan)?;

            // Phase 3: Join optimization
            if check_options.has_check(&CheckOption::EnableNljCheck) {
                trace!("Phase 3: Starting join optimization");
                plan = self.optimize_joins(plan)?;
            } else {
                debug!("Skipping join optimization - NLJ check not enabled");
            }

            // Phase 4: Sort and limit optimization
            if check_options.has_check(&CheckOption::EnableTopnCheck) {
                trace!("Phase 4: Starting sort and limit optimization");
                plan = self.optimize_sort_and_limit(plan)?;
            } else {
                debug!("Skipping sort optimization - TopN check not enabled");
            }
        } else {
            info!("Skipping optimization - modifications not enabled");
        }

        // Final validation
        trace!("Performing final plan validation");
        self.validate_plan(&plan, &check_options)?;

        debug!("Final optimized plan:\n{:?}", plan);
        info!("Query optimization completed successfully");

        Ok(plan)
    }

    /// Phase 0: Index scan optimization
    fn apply_index_scan_optimization(&self, plan: Box<LogicalPlan>) -> Result<Box<LogicalPlan>, DBError> {
        trace!("Attempting index scan optimization for plan node: {:?}", plan.plan_type);

        match &plan.plan_type {
            LogicalPlanType::TableScan { table_name, schema, table_oid } => {
                debug!("Examining table scan on {} for potential index usage", table_name);

                let catalog = self.catalog.read();
                let indexes = catalog.get_table_indexes(table_name);

                if indexes.is_empty() {
                    debug!("No indexes found for table {}", table_name);
                    return Ok(plan);
                }

                if let Some(first_col) = schema.get_columns().first() {
                    trace!("Checking indexes for column {}", first_col.get_name());

                    for index_info in indexes {
                        let key_attrs = index_info.get_key_attrs();

                        if !key_attrs.is_empty() {
                            if let Some(table_info) = catalog.get_table(table_name) {
                                let table_schema = table_info.get_table_schema();
                                if let Some(col_idx) = table_schema.get_column_index(&first_col.get_name()) {
                                    if key_attrs[0] == col_idx {
                                        info!("Converting table scan to index scan using index {} on table {}",
                                              index_info.get_index_name(), table_name);

                                        return Ok(Box::new(LogicalPlan::new(
                                            LogicalPlanType::IndexScan {
                                                table_name: table_name.clone(),
                                                table_oid: *table_oid,
                                                index_name: index_info.get_index_name().to_string(),
                                                index_oid: index_info.get_index_oid(),
                                                schema: schema.clone(),
                                            },
                                            vec![],
                                        )));
                                    }
                                }
                            }
                        }
                    }
                    debug!("No suitable index found for the first column");
                }
                Ok(plan)
            }
            _ => {
                trace!("Non-table-scan node, checking children");
                self.apply_index_scan_to_children(plan)
            }
        }
    }

    fn apply_index_scan_to_children(&self, mut plan: Box<LogicalPlan>) -> Result<Box<LogicalPlan>, DBError> {
        // Recursively try to convert any table scans in child nodes
        let mut new_children = Vec::new();
        for child in plan.children.drain(..) {
            new_children.push(self.apply_index_scan_optimization(child)?);
        }
        plan.children = new_children;
        Ok(plan)
    }

    /// Phase 1: Early pruning and simplification
    fn apply_early_pruning(&self, mut plan: Box<LogicalPlan>) -> Result<Box<LogicalPlan>, DBError> {
        trace!("Applying early pruning to plan node: {:?}", plan.plan_type);

        match &plan.plan_type {
            LogicalPlanType::Project { expressions, schema } => {
                if expressions.is_empty() {
                    debug!("Found empty projection, removing unnecessary node");
                    if let Some(child) = plan.children.pop() {
                        return Ok(child);
                    }
                }
                self.apply_early_pruning_to_children(plan)
            }
            LogicalPlanType::Filter { predicate } => {
                trace!("Examining filter predicate for simplification");
                if let Expression::Constant(const_expr) = predicate.as_ref() {
                    if let Val::Boolean(b) = const_expr.get_value().value_ {
                        if b {
                            debug!("Removing true filter predicate");
                            if let Some(child) = plan.children.pop() {
                                return Ok(child);
                            }
                        }
                    }
                }
                self.apply_early_pruning_to_children(plan)
            }
            _ => self.apply_early_pruning_to_children(plan),
        }
    }

    /// Phase 2: Apply rewrite rules
    fn apply_rewrite_rules(&self, mut plan: Box<LogicalPlan>) -> Result<Box<LogicalPlan>, DBError> {
        match &plan.plan_type {
            LogicalPlanType::Filter { predicate } => {
                // Push down filter predicates
                if let Some(child) = plan.children.pop() {
                    self.push_down_predicate(predicate.clone(), child)
                } else {
                    Ok(plan)
                }
            }
            LogicalPlanType::Project {
                expressions,
                schema,
            } => {
                // Combine consecutive projections
                if let Some(child) = plan.children.pop() {
                    if let LogicalPlanType::Project { .. } = child.plan_type {
                        return self.merge_projections(expressions.clone(), child);
                    }
                    plan.children = vec![self.apply_rewrite_rules(child)?];
                    Ok(plan)
                } else {
                    Ok(plan)
                }
            }
            _ => self.apply_rewrite_rules_to_children(plan),
        }
    }

    /// Phase 3: Join optimization
    fn optimize_joins(&self, mut plan: Box<LogicalPlan>) -> Result<Box<LogicalPlan>, DBError> {
        match &plan.plan_type {
            LogicalPlanType::NestedLoopJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                // Take ownership of children
                let mut children = plan.children.drain(..).collect::<Vec<_>>();
                let right = children.pop().unwrap();
                let left = children.pop().unwrap();

                // Recursively optimize children first
                let optimized_left = self.optimize_joins(left)?;
                let optimized_right = self.optimize_joins(right)?;

                // Try to convert to hash join if beneficial
                if self.should_use_hash_join(predicate.as_ref()) {
                    Ok(Box::new(LogicalPlan::new(
                        LogicalPlanType::HashJoin {
                            left_schema: left_schema.clone(),
                            right_schema: right_schema.clone(),
                            predicate: predicate.clone(),
                            join_type: join_type.clone(),
                        },
                        vec![optimized_left, optimized_right],
                    )))
                } else {
                    plan.children = vec![optimized_left, optimized_right];
                    Ok(plan)
                }
            }
            _ => self.optimize_joins_children(plan),
        }
    }

    /// Phase 4: Sort and limit optimization
    fn optimize_sort_and_limit(
        &self,
        mut plan: Box<LogicalPlan>,
    ) -> Result<Box<LogicalPlan>, DBError> {
        match &plan.plan_type {
            LogicalPlanType::Sort {
                sort_expressions,
                schema,
            } => {
                if let Some(child) = plan.children.pop() {
                    // Look for opportunities to use TopN instead of Sort
                    if let LogicalPlanType::Limit { limit, .. } = child.plan_type {
                        return Ok(Box::new(LogicalPlan::new(
                            LogicalPlanType::TopN {
                                k: limit,
                                sort_expressions: sort_expressions.clone(),
                                schema: schema.clone(),
                            },
                            vec![self.optimize_sort_and_limit(child)?],
                        )));
                    }
                    plan.children = vec![self.optimize_sort_and_limit(child)?];
                }
                Ok(plan)
            }
            _ => self.optimize_sort_and_limit_children(plan),
        }
    }

    /// Helper method to extract column name from an expression
    fn extract_column_name(&self, expr: &Expression) -> Option<String> {
        match expr {
            Expression::ColumnRef(col_ref) => {
                Some(col_ref.get_return_type().get_name().to_string())
            }
            _ => None
        }
    }

    fn apply_early_pruning_to_children(
        &self,
        mut plan: Box<LogicalPlan>,
    ) -> Result<Box<LogicalPlan>, DBError> {
        let mut new_children = Vec::new();
        for child in plan.children.drain(..) {
            new_children.push(self.apply_early_pruning(child)?);
        }
        plan.children = new_children;
        Ok(plan)
    }

    fn apply_rewrite_rules_to_children(
        &self,
        mut plan: Box<LogicalPlan>,
    ) -> Result<Box<LogicalPlan>, DBError> {
        let mut new_children = Vec::new();
        for child in plan.children.drain(..) {
            new_children.push(self.apply_rewrite_rules(child)?);
        }
        plan.children = new_children;
        Ok(plan)
    }

    fn optimize_joins_children(
        &self,
        mut plan: Box<LogicalPlan>,
    ) -> Result<Box<LogicalPlan>, DBError> {
        let mut new_children = Vec::new();
        for child in plan.children.drain(..) {
            new_children.push(self.optimize_joins(child)?);
        }
        plan.children = new_children;
        Ok(plan)
    }

    fn optimize_sort_and_limit_children(
        &self,
        mut plan: Box<LogicalPlan>,
    ) -> Result<Box<LogicalPlan>, DBError> {
        let mut new_children = Vec::new();
        for child in plan.children.drain(..) {
            new_children.push(self.optimize_sort_and_limit(child)?);
        }
        plan.children = new_children;
        Ok(plan)
    }

    // Helper methods for specific optimizations
    fn push_down_predicate(
        &self,
        predicate: Arc<Expression>,
        child: Box<LogicalPlan>,
    ) -> Result<Box<LogicalPlan>, DBError> {
        match child.plan_type {
            LogicalPlanType::TableScan { .. } => {
                // Create a new filter node above the scan
                Ok(Box::new(LogicalPlan::new(
                    LogicalPlanType::Filter { predicate },
                    vec![child],
                )))
            }
            LogicalPlanType::Filter {
                predicate: existing_pred,
            } => {
                // Combine predicates using AND
                let combined_pred = Arc::new(Expression::Logic(LogicExpression::new(
                    predicate.clone(),
                    existing_pred.clone(),
                    LogicType::And,
                    vec![],
                )));
                if let Some(grandchild) = child.children.first() {
                    self.push_down_predicate(combined_pred, grandchild.clone())
                } else {
                    Ok(Box::new(LogicalPlan::new(
                        LogicalPlanType::Filter {
                            predicate: combined_pred,
                        },
                        vec![],
                    )))
                }
            }
            _ => Ok(Box::new(LogicalPlan::new(
                LogicalPlanType::Filter { predicate },
                vec![child],
            ))),
        }
    }

    fn merge_projections(
        &self,
        expressions: Vec<Arc<Expression>>,
        child: Box<LogicalPlan>,
    ) -> Result<Box<LogicalPlan>, DBError> {
        if let LogicalPlanType::Project {
            expressions: child_exprs,
            schema: child_schema,
        } = child.clone().plan_type
        {
            // TODO: Implement projection merging logic
            // This would involve rewriting the outer expressions in terms of the inner ones
            Ok(child)
        } else {
            Ok(Box::new(LogicalPlan::new(
                LogicalPlanType::Project {
                    expressions,
                    schema: Schema::new(vec![]), // TODO: Compute correct schema
                },
                vec![child],
            )))
        }
    }

    fn should_use_hash_join(&self, predicate: &Expression) -> bool {
        // TODO: Implement logic to determine if hash join would be beneficial
        // Consider factors like:
        // - Is the join predicate an equality comparison?
        // - Are the input relations large enough to justify hash join overhead?
        // - Are there available statistics about the input relations?
        false
    }

    /// Validation
    fn validate_plan(&self, plan: &LogicalPlan, check_options: &CheckOptions) -> Result<(), DBError> {
        trace!("Validating plan node: {:?}", plan.plan_type);

        match &plan.plan_type {
            LogicalPlanType::TableScan { table_name, table_oid, .. } => {
                let catalog = self.catalog.read();
                if catalog.get_table_by_oid(*table_oid).is_none() {
                    warn!("Table with OID {} not found in catalog", table_oid);
                    return Err(DBError::TableNotFound(table_name.clone()));
                }
            }
            LogicalPlanType::NestedLoopJoin { predicate, .. } => {
                if check_options.has_check(&CheckOption::EnableNljCheck) && predicate.get_children().is_empty() {
                    warn!("NLJ validation failed: missing join predicate");
                    return Err(DBError::Validation("NLJ requires join predicate".to_string()));
                }
            }
            LogicalPlanType::TopN { k, sort_expressions, .. } => {
                if *k == 0 || sort_expressions.is_empty() {
                    warn!("TopN validation failed: invalid specification");
                    return Err(DBError::Validation("Invalid TopN specification".to_string()));
                }
            }
            _ => {}
        }

        // Recursively validate children
        for child in &plan.children {
            self.validate_plan(child, check_options)?;
        }

        trace!("Plan validation successful for node: {:?}", plan.plan_type);
        Ok(())
    }
}
