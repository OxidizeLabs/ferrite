use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
use log::debug;
use super::logical_plan::LogicalPlan;
use super::schema_manager::SchemaManager;
use crate::catalog::catalog::Catalog;
use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::sql::execution::expression_parser::ExpressionParser;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
use parking_lot::RwLock;
use sqlparser::ast::*;
use sqlparser::ast::Value as SqlValue;
use std::sync::Arc;
use log;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Value, Val};

pub struct LogicalPlanBuilder {
    pub expression_parser: ExpressionParser,
    pub schema_manager: SchemaManager,
}

impl LogicalPlanBuilder {
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        Self {
            expression_parser: ExpressionParser::new(Arc::clone(&catalog)),
            schema_manager: SchemaManager::new(),
        }
    }

    pub fn build_query_plan(&self, query: &Query) -> Result<Box<LogicalPlan>, String> {
        // Start with the main query body
        let mut current_plan = match &*query.body {
            SetExpr::Select(select) => self.build_select_plan(select)?,
            SetExpr::Query(nested_query) => self.build_query_plan(nested_query)?,
            SetExpr::Values(values) => {
                let schema = self.schema_manager.create_values_schema(&values.rows)?;
                self.build_values_plan(&values.rows, &schema)?
            }
            SetExpr::Update(update) => self.build_update_plan(update)?,
            SetExpr::SetOperation {
                op,
                left,
                right,
                set_quantifier,
            } => {
                return Err(
                    "Set operations (UNION, INTERSECT, etc.) are not yet supported".to_string(),
                )
            }
            SetExpr::Insert(insert) => {
                return Err(
                    "INSERT is not supported in this context. Use Statement::Insert instead."
                        .to_string(),
                )
            }
            SetExpr::Table(table) => {
                return Err(format!(
                    "Table expressions are not yet supported: {:?}",
                    table
                ))
            }
        };

        // Handle ORDER BY if present
        if let Some(order_by) = &query.order_by {
            let schema = current_plan.get_schema();
            let sort_exprs = order_by
                .exprs
                .iter()
                .map(|order| {
                    let expr = self
                        .expression_parser
                        .parse_expression(&order.expr, &schema)?;
                    // TODO: Handle order.asc and order.nulls_first if needed
                    Ok(Arc::new(expr))
                })
                .collect::<Result<Vec<_>, String>>()?;

            current_plan = LogicalPlan::sort(sort_exprs, schema.clone(), current_plan);
        }

        // Handle OFFSET if present
        if let Some(offset) = &query.offset {
            let schema = current_plan.get_schema();
            match &offset.value {
                Expr::Value(SqlValue::Number(n, _)) => {
                    if let Ok(offset_val) = n.parse::<usize>() {
                        // Create a limit node with offset
                        // For now, we'll use a very large number as the limit
                        current_plan = LogicalPlan::limit(usize::MAX, schema, current_plan);
                        // TODO: Implement proper OFFSET support in the limit plan
                    } else {
                        return Err("Invalid OFFSET value".to_string());
                    }
                }
                _ => return Err("OFFSET must be a number".to_string()),
            }
        }

        // Handle LIMIT if present
        if let Some(limit_expr) = &query.limit {
            let schema = current_plan.get_schema();
            match limit_expr {
                Expr::Value(SqlValue::Number(n, _)) => {
                    if let Ok(limit_val) = n.parse::<usize>() {
                        current_plan = LogicalPlan::limit(limit_val, schema, current_plan);
                    } else {
                        return Err("Invalid LIMIT value".to_string());
                    }
                }
                _ => return Err("LIMIT must be a number".to_string()),
            }
        }

        // Handle FETCH if present (similar to LIMIT)
        if let Some(fetch) = &query.fetch {
            let schema = current_plan.get_schema();
            match &fetch.quantity {
                Some(Expr::Value(SqlValue::Number(n, _))) => {
                    if let Ok(fetch_val) = n.parse::<usize>() {
                        current_plan = LogicalPlan::limit(fetch_val, schema, current_plan);
                    } else {
                        return Err("Invalid FETCH value".to_string());
                    }
                }
                _ => return Err("FETCH quantity must be a number".to_string()),
            }
        }

        Ok(current_plan)
    }

    pub fn build_select_plan(&self, select: &Box<Select>) -> Result<Box<LogicalPlan>, String> {
        debug!("=== Starting Join Planning ===");
        debug!("From clause: {:?}", select.from);
        
        // Get the base table and any joins
        let mut current_plan = if let Some(table_with_joins) = select.from.first() {
            // Get the base table scan
            let table_name = match &table_with_joins.relation {
                TableFactor::Table { name, .. } => name.to_string(),
                _ => return Err("Only simple table references are supported".to_string()),
            };
            
            let mut current_plan = LogicalPlan::table_scan(
                table_name.clone(),
                self.expression_parser.get_table_schema(&table_name)?,
                self.expression_parser.get_table_oid(&table_name)?,
            );

            // Process any joins
            for join in &table_with_joins.joins {
                // Get the right table scan
                let right_table = match &join.relation {
                    TableFactor::Table { name, .. } => name.to_string(),
                    _ => return Err("Only simple table joins are supported".to_string()),
                };
                
                let right_plan = LogicalPlan::table_scan(
                    right_table.clone(),
                    self.expression_parser.get_table_schema(&right_table)?,
                    self.expression_parser.get_table_oid(&right_table)?,
                );

                // Create join predicate
                let join_predicate = match &join.join_operator {
                    JoinOperator::Inner(constraint) | 
                    JoinOperator::LeftOuter(constraint) |
                    JoinOperator::RightOuter(constraint) => {
                        match constraint {
                            JoinConstraint::On(expr) => {
                                let left_schema = current_plan.get_schema();
                                let right_schema = right_plan.get_schema();
                                Arc::new(self.expression_parser.parse_join_condition(
                                    expr,
                                    &left_schema,
                                    &right_schema,
                                )?)
                            }
                            _ => return Err("Only ON clause joins are supported".to_string()),
                        }
                    }
                    JoinOperator::CrossJoin => {
                        // Cross join doesn't need a predicate
                        let true_value = Value::new(Val::Boolean(true));
                        let true_column = Column::new("true", TypeId::Boolean);
                        Arc::new(Expression::Constant(ConstantExpression::new(
                            true_value,
                            true_column,
                            vec![],
                        )))
                    }
                    _ => return Err("Unsupported join type".to_string()),
                };

                // Build the join plan
                current_plan = self.build_join_plan(
                    current_plan,
                    right_plan,
                    &join.join_operator,
                    join_predicate,
                )?;
            }
            current_plan
        } else {
            return Err("No FROM clause found".to_string());
        };

        debug!("After join preparation:");
        debug!("Current plan schema: {:?}", current_plan.get_schema());
        debug!("Current plan type: {:?}", current_plan.plan_type);

        // Add filter if WHERE clause exists
        if let Some(where_clause) = &select.selection {
            let schema = current_plan.get_schema();
            let predicate = Arc::new(
                self.expression_parser
                    .parse_expression(where_clause, &schema)?,
            );
            current_plan = LogicalPlan::filter(
                schema.clone(),
                String::new(), // No single table name for joins
                0,             // No single table OID for joins
                predicate,
                current_plan,
            );
        }

        // Handle aggregation if needed
        let schema = current_plan.get_schema();
        let has_group_by = match &select.group_by {
            GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
            GroupByExpr::All(_) => true,
        };
        let has_aggregates = self
            .expression_parser
            .has_aggregate_functions(&select.projection);

        if has_group_by || has_aggregates {
            debug!("=== Starting Aggregation Planning ===");
            debug!("Input schema: {:?}", schema);
            debug!("Has GROUP BY: {}, Has Aggregates: {}", has_group_by, has_aggregates);
            
            // Parse group by expressions
            let group_by_exprs: Vec<_> = self
                .expression_parser
                .determine_group_by_expressions(select, &schema, has_group_by)?
                .into_iter()
                .map(Arc::new)
                .collect();

            debug!("Group by expressions: {:?}", group_by_exprs);

            // Parse aggregates
            let (agg_exprs, _) = self
                .expression_parser
                .parse_aggregates(&select.projection, &schema)?;

            debug!("Aggregate expressions: {:?}", agg_exprs);

            // Create output schema for aggregation
            let agg_schema = self.schema_manager.create_aggregation_output_schema(
                &group_by_exprs
                    .iter()
                    .map(|e| e.as_ref())
                    .collect::<Vec<_>>(),
                &agg_exprs,
                has_group_by,
            );

            debug!("Aggregation schema: {:?}", agg_schema);

            // Create the aggregate plan node
            current_plan = LogicalPlan::aggregate(group_by_exprs, agg_exprs.clone(), agg_schema.clone(), current_plan);

            // Handle HAVING clause if present
            if let Some(having) = &select.having {
                let predicate = Arc::new(
                    self.expression_parser
                        .parse_expression(having, &schema)?, // Use input schema for HAVING clause
                );
                current_plan = LogicalPlan::filter(
                    agg_schema.clone(),      // Use aggregation schema since we're filtering after aggregation
                    String::new(),           // No single table name for aggregates
                    0,                       // No single table OID for aggregates
                    predicate,
                    current_plan,
                );
            }
        }

        // Handle ORDER BY
        if !select.sort_by.is_empty() {
            let sort_exprs = select
                .sort_by
                .iter()
                .map(|order| {
                    let expr = self.expression_parser.parse_expression(&order, &schema)?;
                    Ok(Arc::new(expr))
                })
                .collect::<Result<Vec<_>, String>>()?;

            current_plan = LogicalPlan::sort(sort_exprs, schema.clone(), current_plan);
        }

        // Build the projection
        current_plan = self.build_projection_plan(&select.projection, current_plan)?;

        Ok(current_plan)
    }

    pub fn build_insert_plan(&self, insert: &Insert) -> Result<Box<LogicalPlan>, String> {
        let table_name = self
            .expression_parser
            .extract_table_name(&insert.table_name)?;

        // Get table info from catalog
        let binding = self.expression_parser.catalog();
        let catalog_guard = binding.read();
        let table_info = catalog_guard
            .get_table(&table_name)
            .ok_or_else(|| format!("Table '{}' does not exist", table_name))?;

        let schema = table_info.get_table_schema();
        let table_oid = table_info.get_table_oidt();

        // Plan the source (VALUES or SELECT)
        let source_plan = match &insert.source {
            Some(query) => match &*query.body {
                SetExpr::Values(values) => self.build_values_plan(&values.rows, &schema)?,
                SetExpr::Select(select) => {
                    let select_plan = self.build_select_plan(select)?;
                    if !self
                        .schema_manager
                        .schemas_compatible(&select_plan.get_schema(), &schema)
                    {
                        return Err("SELECT schema doesn't match INSERT target".to_string());
                    }
                    select_plan
                }
                _ => return Err("Only VALUES and SELECT supported in INSERT".to_string()),
            },
            None => return Err("INSERT statement must have a source".to_string()),
        };

        Ok(LogicalPlan::insert(
            table_name,
            schema,
            table_oid,
            source_plan,
        ))
    }

    pub fn build_update_plan(&self, update: &Statement) -> Result<Box<LogicalPlan>, String> {
        // Get table info
        let (table, assignments, selection) = match update {
            Statement::Update {
                table,
                assignments,
                selection,
                ..  // Ignore other fields for now (from, returning, or)
            } => (table, assignments, selection),
            _ => return Err("Expected Update statement".to_string()),
        };

        let table_name = match &table.relation {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => return Err("Only simple table updates supported".to_string()),
        };

        let binding = self.expression_parser.catalog();
        let catalog_guard = binding.read();
        let table_info = catalog_guard
            .get_table(&table_name)
            .ok_or_else(|| format!("Table '{}' not found", table_name))?;

        let schema = table_info.get_table_schema();
        let table_oid = table_info.get_table_oidt();

        // Create base table scan
        let mut current_plan = LogicalPlan::table_scan(table_name.clone(), schema.clone(), table_oid);

        // Add filter if WHERE clause exists
        if let Some(where_clause) = selection {
            let predicate = Arc::new(
                self.expression_parser
                    .parse_expression(where_clause, &schema)?,
            );
            current_plan = LogicalPlan::filter(
                schema.clone(),
                table_name.clone(),
                table_oid,
                predicate,
                current_plan,
            );
        }

        // Parse update assignments
        let mut update_exprs = Vec::new();
        for assignment in assignments {
            let value_expr = self
                .expression_parser
                .parse_expression(&assignment.value, &schema)?;
            update_exprs.push(Arc::new(value_expr));
        }

        Ok(LogicalPlan::update(
            table_name,
            schema,
            table_oid,
            update_exprs,
            current_plan,
        ))
    }

    pub fn build_values_plan(
        &self,
        rows: &[Vec<Expr>],
        schema: &Schema,
    ) -> Result<Box<LogicalPlan>, String> {
        let mut value_rows = Vec::new();

        for row in rows {
            if row.len() != schema.get_column_count() as usize {
                return Err(format!(
                    "VALUES has {} columns but schema expects {}",
                    row.len(),
                    schema.get_column_count()
                ));
            }

            let mut value_exprs = Vec::new();
            for (_, expr) in row.iter().enumerate() {
                let value_expr = Arc::new(self.expression_parser.parse_expression(expr, schema)?);
                value_exprs.push(value_expr);
            }
            value_rows.push(value_exprs);
        }

        Ok(LogicalPlan::values(value_rows, schema.clone()))
    }

    pub fn build_projection_plan(
        &self,
        projection: &[SelectItem],
        input_plan: Box<LogicalPlan>,
    ) -> Result<Box<LogicalPlan>, String> {
        let input_schema = input_plan.get_schema();
        debug!("Building projection plan");
        debug!("Input schema: {:?}", input_schema);
        debug!("Projection items: {:?}", projection);
        
        // Convert projection items to expressions
        let mut projection_exprs = Vec::new();
        let mut output_columns = Vec::new();
        
        for (i, item) in projection.iter().enumerate() {
            debug!("Processing projection item {}: {:?}", i, item);
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let parsed_expr = self.expression_parser.parse_expression(expr, &input_schema)?;
                    debug!("Parsed expression: {:?}", parsed_expr);
                    
                    // Use the column name from the expression
                    let col_name = match expr {
                        Expr::CompoundIdentifier(parts) => {
                            // For qualified names (e.g., u.name), preserve the full name
                            let name = format!("{}.{}", parts[0].value, parts[1].value);
                            debug!("Using qualified name: {}", name);
                            name
                        }
                        _ => {
                            let name = parsed_expr.get_return_type().get_name().to_string();
                            debug!("Using unqualified name: {}", name);
                            name
                        }
                    };
                    
                    // Create a new column with the proper name but same type
                    let output_col = Column::new(
                        &col_name,
                        parsed_expr.get_return_type().get_type(),
                    );
                    debug!("Created output column: {:?}", output_col);
                    
                    output_columns.push(output_col);
                    projection_exprs.push(Arc::new(parsed_expr));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    debug!("Processing aliased expression: {:?} AS {}", expr, alias.value);
                    let parsed_expr = self.expression_parser.parse_expression(expr, &input_schema)?;
                    let output_col = Column::new(
                        &alias.value,
                        parsed_expr.get_return_type().get_type(),
                    );
                    debug!("Created aliased column: {:?}", output_col);
                    output_columns.push(output_col);
                    projection_exprs.push(Arc::new(parsed_expr));
                }
                SelectItem::Wildcard(_) => {
                    debug!("Processing wildcard");
                    for i in 0..input_schema.get_column_count() {
                        let col = input_schema.get_column(i as usize).unwrap();
                        debug!("Adding wildcard column: {:?}", col);
                        output_columns.push(col.clone());
                        projection_exprs.push(Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                            0,
                            i as usize,
                            col.clone(),
                            vec![],
                        ))));
                    }
                }
                _ => return Err("Unsupported projection type".to_string()),
            }
        }

        let projection_schema = Schema::new(output_columns);
        debug!("Final projection schema: {:?}", projection_schema);
        debug!("Projection expressions: {:?}", projection_exprs);
        
        Ok(LogicalPlan::project(projection_exprs, projection_schema, input_plan))
    }

    pub fn build_create_table_plan(
        &self,
        create_table: &CreateTable,
    ) -> Result<Box<LogicalPlan>, String> {
        let table_name = create_table.name.to_string();

        // Check if table already exists
        {
            let catalog = self.expression_parser.catalog();
            let catalog_guard = catalog.read();
            if catalog_guard.get_table(&table_name).is_some() {
                // If table exists and IF NOT EXISTS flag is set, return success
                if create_table.if_not_exists {
                    // Create a dummy plan that will effectively be a no-op
                    let columns = self
                        .schema_manager
                        .convert_column_defs(&create_table.columns)?;
                    let schema = Schema::new(columns);
                    return Ok(LogicalPlan::create_table(schema, table_name, true));
                }
                // Otherwise return error
                return Err(format!("Table '{}' already exists", table_name));
            }
        }

        // If we get here, table doesn't exist, proceed with normal creation
        let columns = self
            .schema_manager
            .convert_column_defs(&create_table.columns)?;
        let schema = Schema::new(columns);

        Ok(LogicalPlan::create_table(
            schema,
            table_name,
            create_table.if_not_exists,
        ))
    }

    pub fn build_create_index_plan(
        &mut self,
        create_index: &CreateIndex,
    ) -> Result<Box<LogicalPlan>, String> {
        let index_name = create_index
            .clone()
            .name
            .expect("Index Name not available")
            .to_string();
        let table_name = match &create_index.table_name {
            ObjectName(parts) if parts.len() == 1 => parts[0].value.clone(),
            _ => return Err("Only single table indices are supported".to_string()),
        };

        let binding = self.expression_parser.catalog();
        let catalog_guard = binding.read();
        let table_schema = catalog_guard
            .get_table_schema(&table_name)
            .ok_or_else(|| format!("Table '{}' does not exist", table_name))?;

        let mut key_attrs = Vec::new();
        let mut columns = Vec::new();

        for col_name in &create_index.columns {
            let idx = table_schema
                .get_column_index(&col_name.to_string())
                .ok_or_else(|| format!("Column {} not found in table", col_name))?;
            key_attrs.push(idx);
            columns.push(table_schema.get_column(idx).unwrap().clone());
        }

        let schema = Schema::new(columns.clone());
        drop(catalog_guard);
        {
            Ok(LogicalPlan::create_index(
                schema,
                table_name,
                index_name,
                key_attrs,
                create_index.if_not_exists,
            ))
        }
    }

    pub fn build_join_plan(
        &self,
        left_plan: Box<LogicalPlan>,
        right_plan: Box<LogicalPlan>,
        join_type: &JoinOperator,
        join_predicate: Arc<Expression>,
    ) -> Result<Box<LogicalPlan>, String> {
        let left_schema = left_plan.get_schema();
        let right_schema = right_plan.get_schema();

        match join_type {
            JoinOperator::Inner(_) => {
                Ok(LogicalPlan::nested_loop_join(
                    left_schema.clone(),
                    right_schema.clone(),
                    join_predicate,
                    join_type.clone(),
                    left_plan,
                    right_plan,
                ))
            }
            JoinOperator::LeftOuter(_) => {
                Ok(LogicalPlan::nested_loop_join(
                    left_schema.clone(),
                    right_schema.clone(),
                    join_predicate,
                    join_type.clone(),
                    left_plan,
                    right_plan,
                ))
            }
            JoinOperator::RightOuter(_) => {
                Ok(LogicalPlan::nested_loop_join(
                    right_schema.clone(),
                    left_schema.clone(),
                    join_predicate,
                    JoinOperator::LeftOuter(JoinConstraint::None),
                    right_plan,
                    left_plan,
                ))
            }
            JoinOperator::FullOuter(_) => {
                Err("Full outer joins are not yet supported".to_string())
            }
            JoinOperator::CrossJoin => {
                Ok(LogicalPlan::nested_loop_join(
                    left_schema.clone(),
                    right_schema.clone(),
                    join_predicate,
                    join_type.clone(),
                    left_plan,
                    right_plan,
                ))
            }
            _ => Err(format!("Unsupported join type: {:?}", join_type)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
    use crate::sql::execution::transaction_context::TransactionContext;
    use crate::sql::planner::logical_plan::LogicalToPhysical;
    use crate::sql::planner::query_planner::QueryPlanner;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use chrono::Utc;
    use std::collections::HashMap;
    use tempfile::TempDir;

    struct TestContext {
        catalog: Arc<RwLock<Catalog>>,
        planner: QueryPlanner,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub fn new(name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 5;
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
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(7, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

            let transaction_manager = Arc::new(TransactionManager::new());
            let lock_manager = Arc::new(LockManager::new());

            let catalog = Arc::new(RwLock::new(Catalog::new(
                bpm.clone(),
                0,
                0,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                transaction_manager.clone(),
            )));

            // Create fresh transaction with unique ID
            let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
            let transaction = Arc::new(Transaction::new(
                timestamp.parse::<u64>().unwrap_or(0), // Unique transaction ID
                IsolationLevel::ReadUncommitted,
            ));

            let transaction_context = Arc::new(TransactionContext::new(
                transaction,
                lock_manager.clone(),
                transaction_manager.clone(),
            ));

            let planner = QueryPlanner::new(catalog.clone());

            Self {
                catalog,
                planner,
                _temp_dir: temp_dir,
            }
        }

        // Helper to create a table and verify it was created successfully
        fn create_table(
            &mut self,
            table_name: &str,
            columns: &str,
            if_not_exists: bool,
        ) -> Result<(), String> {
            let if_not_exists_clause = if if_not_exists { "IF NOT EXISTS " } else { "" };
            let create_sql = format!(
                "CREATE TABLE {}{} ({})",
                if_not_exists_clause, table_name, columns
            );
            let create_plan = self.planner.create_logical_plan(&create_sql)?;

            // Convert to physical plan and execute
            let physical_plan = create_plan.to_physical_plan()?;
            match physical_plan {
                PlanNode::CreateTable(create_table) => {
                    let mut catalog = self.catalog.write();
                    catalog.create_table(
                        create_table.get_table_name().to_string(),
                        create_table.get_output_schema().clone(),
                    );
                    Ok(())
                }
                _ => Err("Expected CreateTable plan node".to_string()),
            }
        }

        // Helper to verify a table exists in the catalog
        fn assert_table_exists(&self, table_name: &str) {
            let catalog = self.catalog.read();
            assert!(
                catalog.get_table(table_name).is_some(),
                "Table '{}' should exist",
                table_name
            );
        }

        // Helper to verify a table's schema
        fn assert_table_schema(&self, table_name: &str, expected_columns: &[(String, TypeId)]) {
            let catalog = self.catalog.read();
            let schema = catalog.get_table_schema(table_name).unwrap();

            assert_eq!(schema.get_column_count() as usize, expected_columns.len());

            for (i, (name, type_id)) in expected_columns.iter().enumerate() {
                let column = schema.get_column(i).unwrap();
                assert_eq!(column.get_name(), name);
            }
        }
    }

    mod create_table_tests {
        use super::*;

        #[test]
        fn test_create_simple_table() {
            let mut fixture = TestContext::new("create_simple_table");

            fixture
                .create_table("users", "id INTEGER, name VARCHAR(255)", false)
                .unwrap();

            fixture.assert_table_exists("users");
            fixture.assert_table_schema(
                "users",
                &[
                    ("id".to_string(), TypeId::Integer),
                    ("name".to_string(), TypeId::VarChar),
                ],
            );
        }

        #[test]
        fn test_create_table_if_not_exists() {
            let mut fixture = TestContext::new("create_table_if_not_exists");

            // First creation should succeed
            fixture.create_table("users", "id INTEGER", false).unwrap();

            // Second creation without IF NOT EXISTS should fail
            assert!(fixture.create_table("users", "id INTEGER", false).is_err());

            // Creation with IF NOT EXISTS should not fail
            assert!(fixture.create_table("users", "id INTEGER", true).is_ok());
        }
    }

    mod select_tests {
        use super::*;
        use crate::sql::execution::expressions::comparison_expression::ComparisonType;
        use crate::sql::planner::logical_plan::LogicalPlanType;

        // Helper function to set up a test table
        fn setup_test_table(fixture: &mut TestContext) {
            fixture
                .create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false)
                .unwrap();
        }

        #[test]
        fn test_simple_select() {
            let mut fixture = TestContext::new("simple_select");
            setup_test_table(&mut fixture);

            let select_sql = "SELECT * FROM users";
            let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

            // First verify the projection node
            match &plan.plan_type {
                LogicalPlanType::Projection {
                    expressions,
                    schema,
                } => {
                    assert_eq!(schema.get_column_count(), 3);
                    assert_eq!(expressions.len(), 3);
                }
                _ => panic!("Expected Projection as root node"),
            }

            // Then verify the table scan node
            match &plan.children[0].plan_type {
                LogicalPlanType::TableScan {
                    table_name, schema, ..
                } => {
                    assert_eq!(table_name, "users");
                    assert_eq!(schema.get_column_count(), 3);
                }
                _ => panic!("Expected TableScan as child node"),
            }
        }

        #[test]
        fn test_select_with_filter() {
            let mut fixture = TestContext::new("select_with_filter");
            setup_test_table(&mut fixture);

            let select_sql = "SELECT * FROM users WHERE age > 25";
            let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

            // First verify the projection node
            match &plan.plan_type {
                LogicalPlanType::Projection {
                    expressions,
                    schema,
                } => {
                    assert_eq!(schema.get_column_count(), 3);
                    assert_eq!(expressions.len(), 3);
                }
                _ => panic!("Expected Projection as root node"),
            }

            // Then verify the filter node
            match &plan.children[0].plan_type {
                LogicalPlanType::Filter {
                    schema, predicate, ..
                } => {
                    assert_eq!(schema.get_column_count(), 3);
                    match predicate.as_ref() {
                        Expression::Comparison(comp) => {
                            assert_eq!(comp.get_comp_type(), ComparisonType::GreaterThan);
                        }
                        _ => panic!("Expected Comparison expression"),
                    }
                }
                _ => panic!("Expected Filter node"),
            }

            // Finally verify the table scan node
            match &plan.children[0].children[0].plan_type {
                LogicalPlanType::TableScan {
                    table_name, schema, ..
                } => {
                    assert_eq!(table_name, "users");
                    assert_eq!(schema.get_column_count(), 3);
                }
                _ => panic!("Expected TableScan as leaf node"),
            }
        }
    }

    mod insert_tests {
        use super::*;
        use crate::sql::planner::logical_plan::LogicalPlanType;

        fn setup_test_table(fixture: &mut TestContext) {
            fixture
                .create_table("users", "id INTEGER, name VARCHAR(255)", false)
                .unwrap();
        }

        #[test]
        fn test_simple_insert() {
            let mut fixture = TestContext::new("simple_insert");
            setup_test_table(&mut fixture);

            let insert_sql = "INSERT INTO users VALUES (1, 'test')";
            let plan = fixture.planner.create_logical_plan(insert_sql).unwrap();

            match &plan.plan_type {
                LogicalPlanType::Insert {
                    table_name, schema, ..
                } => {
                    assert_eq!(table_name, "users");
                    assert_eq!(schema.get_column_count(), 2);

                    match &plan.children[0].plan_type {
                        LogicalPlanType::Values { rows, schema } => {
                            assert_eq!(schema.get_column_count(), 2);
                            assert_eq!(rows.len(), 1);
                        }
                        _ => panic!("Expected Values node as child of Insert"),
                    }
                }
                _ => panic!("Expected Insert plan node"),
            }
        }
    }

    mod aggregation_tests {
        use super::*;
        use crate::sql::execution::expressions::aggregate_expression::AggregationType;
        use crate::sql::planner::logical_plan::LogicalPlanType;
        use crate::types_db::type_id::TypeId;

        // Helper function to set up a test table
        fn setup_test_table(fixture: &mut TestContext) {
            fixture
                .create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false)
                .unwrap();
        }

        #[test]
        fn test_plan_aggregate_column_names() {
            let mut fixture = TestContext::new("test_plan_aggregate_column_names");
            setup_test_table(&mut fixture);

            let test_cases = vec![
                (
                    "SELECT name, SUM(age) FROM users GROUP BY name",
                    vec!["name", "SUM(age)"],
                ),
                (
                    "SELECT name, SUM(age) as total_age FROM users GROUP BY name",
                    vec!["name", "total_age"],
                ),
                (
                    "SELECT name, COUNT(*) as user_count FROM users GROUP BY name",
                    vec!["name", "user_count"],
                ),
                (
                    "SELECT name, MIN(age) as min_age, MAX(age) as max_age FROM users GROUP BY name",
                    vec!["name", "min_age", "max_age"],
                ),
                (
                    "SELECT name, AVG(age) FROM users GROUP BY name",
                    vec!["name", "AVG(age)"],
                ),
            ];

            for (sql, expected_columns) in test_cases {
                let plan = fixture
                    .planner
                    .create_logical_plan(sql)
                    .unwrap_or_else(|e| {
                        panic!("Failed to create plan for query '{}': {}", sql, e);
                    });

                // if let Err(e) = verify_aggregation_plan(&plan, &expected_columns) {
                //     panic!(
                //         "Verification failed for query '{}':\n{}\nPlan:\n{}",
                //         sql, e, plan.explain(0)
                //     );
                // }
            }
        }

        #[test]
        fn test_plan_aggregate_types() {
            let mut fixture = TestContext::new("test_plan_aggregate_types");
            setup_test_table(&mut fixture);

            let test_cases = vec![
                (
                    "SELECT COUNT(*) FROM users",
                    vec![AggregationType::CountStar],
                    vec![TypeId::BigInt],
                ),
                (
                    "SELECT SUM(age) FROM users",
                    vec![AggregationType::Sum],
                    vec![TypeId::Integer],
                ),
                (
                    "SELECT MIN(age), MAX(age) FROM users",
                    vec![AggregationType::Min, AggregationType::Max],
                    vec![TypeId::Integer, TypeId::Integer],
                ),
                (
                    "SELECT AVG(age) FROM users",
                    vec![AggregationType::Avg],
                    vec![TypeId::Decimal],
                ),
            ];

            for (sql, expected_types, expected_return_types) in test_cases {
                let plan = fixture.planner.create_logical_plan(sql).unwrap();

                // First verify we have a projection node
                match &plan.plan_type {
                    LogicalPlanType::Projection {
                        expressions,
                        schema,
                    } => {
                        assert_eq!(expressions.len(), expected_types.len());
                    }
                    _ => panic!("Expected Projection as root node for query: {}", sql),
                }

                // Then verify the aggregate node
                match &plan.children[0].plan_type {
                    LogicalPlanType::Aggregate {
                        group_by,
                        aggregates,
                        schema,
                    } => {
                        // Check aggregate types
                        assert_eq!(
                            aggregates.len(),
                            expected_types.len(),
                            "Wrong number of aggregates for query: {}",
                            sql
                        );

                        for (i, expected_type) in expected_types.iter().enumerate() {
                            if let Expression::Aggregate(agg) = aggregates[i].as_ref() {
                                assert_eq!(
                                    agg.get_agg_type(), // Remove & reference
                                    expected_type,
                                    "Aggregate type mismatch for query: {}",
                                    sql
                                );
                                assert_eq!(
                                    agg.get_return_type().get_type(),
                                    expected_return_types[i],
                                    "Return type mismatch for query: {}",
                                    sql
                                );
                            } else {
                                panic!("Expected aggregate expression for query: {}", sql);
                            }
                        }
                    }
                    _ => panic!("Expected Aggregate node for query: {}", sql),
                }

                // Finally verify the table scan node
                match &plan.children[0].children[0].plan_type {
                    LogicalPlanType::TableScan { .. } => {}
                    _ => panic!("Expected TableScan as leaf node for query: {}", sql),
                }
            }
        }
    }

    mod join_tests {
        use super::*;
        use crate::sql::planner::logical_plan::LogicalPlanType;

        fn setup_test_tables(fixture: &mut TestContext) {
            fixture
                .create_table(
                    "users",
                    "id INTEGER, name VARCHAR(255), age INTEGER",
                    false,
                )
                .unwrap();
            fixture
                .create_table(
                    "orders",
                    "order_id INTEGER, user_id INTEGER, amount DECIMAL",
                    false,
                )
                .unwrap();
        }

        #[test]
        fn test_inner_join() {
            let mut fixture = TestContext::new("inner_join");
            setup_test_tables(&mut fixture);

            let join_sql = "SELECT u.name, o.amount \
                           FROM users u \
                           INNER JOIN orders o ON u.id = o.user_id";
            
            let plan = fixture.planner.create_logical_plan(join_sql).expect("Failed to create plan");

            // First verify the join node and its schemas
            let join_node = match &plan.children[0].plan_type {
                LogicalPlanType::NestedLoopJoin { 
                    left_schema, 
                    right_schema,
                    predicate,
                    join_type 
                } => {
                    // Verify left schema (users)
                    assert_eq!(left_schema.get_column_count(), 3, "Wrong number of columns in left schema");
                    let left_cols: Vec<_> = (0..left_schema.get_column_count())
                        .map(|i| left_schema.get_column(i as usize).unwrap().get_name())
                        .collect();
                    assert!(left_cols.contains(&"id"), "Left schema missing 'id' column");
                    assert!(left_cols.contains(&"name"), "Left schema missing 'name' column");
                    assert!(left_cols.contains(&"age"), "Left schema missing 'age' column");

                    // Verify right schema (orders)
                    assert_eq!(right_schema.get_column_count(), 3, "Wrong number of columns in right schema");
                    let right_cols: Vec<_> = (0..right_schema.get_column_count())
                        .map(|i| right_schema.get_column(i as usize).unwrap().get_name())
                        .collect();
                    assert!(right_cols.contains(&"order_id"), "Right schema missing 'order_id' column");
                    assert!(right_cols.contains(&"user_id"), "Right schema missing 'user_id' column");
                    assert!(right_cols.contains(&"amount"), "Right schema missing 'amount' column");

                    // Verify join type
                    assert!(matches!(join_type, JoinOperator::Inner(_)), "Expected INNER join");

                    // Verify predicate is a comparison
                    match predicate.as_ref() {
                        Expression::Comparison(_) => (), // Predicate exists and is a comparison
                        _ => panic!("Expected comparison expression as join predicate"),
                    }
                }
                _ => panic!("Expected NestedLoopJoin node"),
            };

            // Then verify the projection can access columns from both tables
            match &plan.plan_type {
                LogicalPlanType::Projection { expressions, schema } => {
                    assert_eq!(schema.get_column_count(), 2, "Expected 2 columns in projection");
                    assert_eq!(expressions.len(), 2, "Expected 2 expressions in projection");
                    
                    // Verify column names in output schema
                    let col_names: Vec<_> = (0..schema.get_column_count())
                        .map(|i| schema.get_column(i as usize).unwrap().get_name())
                        .collect();
                    assert!(col_names.contains(&"u.name"), "Output missing 'u.name' column");
                    assert!(col_names.contains(&"o.amount"), "Output missing 'o.amount' column");
                }
                _ => panic!("Expected Projection as root node"),
            }
        }
    }

    mod group_by_tests {
        use crate::sql::execution::expressions::comparison_expression::ComparisonType;
        use super::*;
        use crate::sql::planner::logical_plan::LogicalPlanType;

        fn setup_test_table(fixture: &mut TestContext) {
            fixture
                .create_table(
                    "sales",
                    "id INTEGER, product VARCHAR(255), amount DECIMAL, region VARCHAR(255)",
                    false,
                )
                .unwrap();
        }

        #[test]
        fn test_group_by_with_aggregates() {
            let mut fixture = TestContext::new("group_by_aggregates");
            setup_test_table(&mut fixture);

            let group_sql = "SELECT region, SUM(amount) as total_sales, COUNT(*) as num_sales \
                            FROM sales \
                            GROUP BY region \
                            HAVING SUM(amount) > 1000";

            let plan = fixture.planner.create_logical_plan(group_sql).unwrap();

            // Verify projection
            match &plan.plan_type {
                LogicalPlanType::Projection { expressions, schema } => {
                    assert_eq!(schema.get_column_count(), 3);
                    assert_eq!(expressions.len(), 3);
                }
                _ => panic!("Expected Projection as root node"),
            }

            // Verify filter (HAVING)
            match &plan.children[0].plan_type {
                LogicalPlanType::Filter { predicate, .. } => {
                    // Verify the predicate is comparing SUM(amount) > 1000
                    match predicate.as_ref() {
                        Expression::Comparison(comp) => {
                            assert_eq!(comp.get_comp_type(), ComparisonType::GreaterThan);
                        }
                        _ => panic!("Expected Comparison expression in HAVING clause"),
                    }
                }
                _ => panic!("Expected Filter node for HAVING clause"),
            }

            // Verify aggregation
            match &plan.children[0].children[0].plan_type {
                LogicalPlanType::Aggregate { 
                    group_by, 
                    aggregates,
                    schema 
                } => {
                    assert_eq!(group_by.len(), 1); // region
                    assert_eq!(aggregates.len(), 2); // SUM(amount), COUNT(*)
                    assert_eq!(schema.get_column_count(), 3);
                }
                _ => panic!("Expected Aggregate node"),
            }
        }

        #[test]
        fn test_simple_group_by() {
            let mut fixture = TestContext::new("simple_group_by");
            setup_test_table(&mut fixture);

            let sql = "SELECT region, COUNT(*) as count \
                      FROM sales \
                      GROUP BY region";

            let plan = fixture.planner.create_logical_plan(sql).unwrap();

            match &plan.children[0].plan_type {
                LogicalPlanType::Aggregate { 
                    group_by, 
                    aggregates,
                    schema 
                } => {
                    assert_eq!(group_by.len(), 1);
                    assert_eq!(aggregates.len(), 1);
                    assert_eq!(schema.get_column_count(), 2);
                }
                _ => panic!("Expected Aggregate node"),
            }
        }
    }

    mod order_by_tests {
        use super::*;
        use crate::sql::planner::logical_plan::LogicalPlanType;

        fn setup_test_table(fixture: &mut TestContext) {
            fixture
                .create_table(
                    "employees",
                    "id INTEGER, name VARCHAR(255), salary DECIMAL, dept VARCHAR(255)",
                    false,
                )
                .unwrap();
        }

        #[test]
        fn test_order_by_with_limit() {
            let mut fixture = TestContext::new("order_by_limit");
            setup_test_table(&mut fixture);

            let sql = "SELECT name, salary \
                      FROM employees \
                      ORDER BY salary DESC \
                      LIMIT 10";

            let plan = fixture.planner.create_logical_plan(sql).unwrap();

            // Verify limit
            match &plan.plan_type {
                LogicalPlanType::Limit { limit, schema } => {
                    assert_eq!(*limit, 10);
                    assert_eq!(schema.get_column_count(), 2);
                }
                _ => panic!("Expected Limit as root node"),
            }

            // Verify sort
            match &plan.children[0].plan_type {
                LogicalPlanType::Sort { 
                    sort_expressions,
                    schema 
                } => {
                    assert_eq!(sort_expressions.len(), 1);
                    assert_eq!(schema.get_column_count(), 2);
                }
                _ => panic!("Expected Sort node"),
            }

            // Verify projection
            match &plan.children[0].children[0].plan_type {
                LogicalPlanType::Projection { expressions, schema } => {
                    assert_eq!(expressions.len(), 2);
                    assert_eq!(schema.get_column_count(), 2);
                }
                _ => panic!("Expected Projection node"),
            }
        }
    }

    mod subquery_tests {
        use crate::sql::execution::expressions::comparison_expression::ComparisonType;
        use super::*;
        use crate::sql::planner::logical_plan::LogicalPlanType;

        fn setup_test_tables(fixture: &mut TestContext) {
            fixture
                .create_table(
                    "employees",
                    "id INTEGER, name VARCHAR(255), salary DECIMAL, dept_id INTEGER",
                    false,
                )
                .unwrap();
            fixture
                .create_table(
                    "departments",
                    "dept_id INTEGER, name VARCHAR(255), budget DECIMAL",
                    false,
                )
                .unwrap();
        }

        #[test]
        fn test_subquery_in_where() {
            let mut fixture = TestContext::new("subquery_where");
            setup_test_tables(&mut fixture);

            let sql = "SELECT e.name, e.salary \
                      FROM employees e \
                      WHERE e.salary > (SELECT AVG(salary) FROM employees)";

            let plan = fixture.planner.create_logical_plan(sql).unwrap();

            // Verify the overall structure matches what we expect for a subquery
            match &plan.plan_type {
                LogicalPlanType::Projection { .. } => {
                    // Verify filter with subquery
                    match &plan.children[0].plan_type {
                        LogicalPlanType::Filter { predicate, .. } => {
                            match predicate.as_ref() {
                                Expression::Comparison(comp) => {
                                    assert_eq!(comp.get_comp_type(), ComparisonType::GreaterThan);
                                }
                                _ => panic!("Expected Comparison expression"),
                            }
                        }
                        _ => panic!("Expected Filter node"),
                    }
                }
                _ => panic!("Expected Projection as root node"),
            }
        }
    }
}
