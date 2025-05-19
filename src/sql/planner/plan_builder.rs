use super::logical_plan::{LogicalPlan, LogicalPlanType};
use super::schema_manager::SchemaManager;
use crate::catalog::catalog::Catalog;
use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::concurrency::transaction::IsolationLevel;
use crate::sql::execution::expression_parser::ExpressionParser;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use log::debug;
use parking_lot::RwLock;
use sqlparser::ast::Value as SqlValue;
use sqlparser::ast::*;
use std::sync::Arc;

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
            SetExpr::Update(update_stmt) => match &*update_stmt {
                Statement::Update {
                    table,
                    assignments,
                    from,
                    selection,
                    returning,
                    or,
                } => self.build_update_plan(table, assignments, from, selection, returning, or)?,
                _ => return Err("Expected Update statement".to_string()),
            },
            SetExpr::Delete(_) => return Err("DELETE is not supported in this context".to_string()),
            SetExpr::SetOperation {
                op,
                left,
                right,
                set_quantifier,
            } => {
                return Err(
                    "Set operations (UNION, INTERSECT, etc.) are not yet supported".to_string(),
                );
            }
            SetExpr::Insert(insert) => {
                return Err(
                    "INSERT is not supported in this context. Use Statement::Insert instead."
                        .to_string(),
                );
            }
            SetExpr::Table(table) => {
                return Err(format!(
                    "Table expressions are not yet supported: {:?}",
                    table
                ));
            }
        };

        // Handle ORDER BY if present
        if let Some(order_by) = &query.order_by {
            let Some(schema) = current_plan.get_schema() else {
                todo!()
            };

            // Build sort expressions manually
            let mut sort_exprs = Vec::new();

            // Access expressions based on the OrderByKind
            match &order_by.kind {
                OrderByKind::Expressions(order_by_exprs) => {
                    for order_item in order_by_exprs {
                        let expr = self
                            .expression_parser
                            .parse_expression(&order_item.expr, &schema)?;
                        sort_exprs.push(Arc::new(expr));
                    }
                }
                OrderByKind::All(_) => {
                    // Handle ALL case if needed
                    return Err("ORDER BY ALL is not supported yet".to_string());
                }
            }

            current_plan = LogicalPlan::sort(sort_exprs, schema.clone(), current_plan);
        }

        // Handle LIMIT if present
        if let Some(limit_clause) = &query.limit_clause {
            let Some(schema) = current_plan.get_schema() else {
                todo!()
            };

            // Extract the limit expression based on the LimitClause variant
            let limit_expr = match limit_clause {
                sqlparser::ast::LimitClause::LimitOffset { limit, .. } => {
                    if let Some(expr) = limit {
                        expr
                    } else {
                        return Err("LIMIT clause has no limit value".to_string());
                    }
                }
                sqlparser::ast::LimitClause::OffsetCommaLimit { limit, .. } => limit,
            };

            // Process the limit expression
            if let Expr::Value(value_with_span) = limit_expr {
                if let sqlparser::ast::Value::Number(n, _) = &value_with_span.value {
                    if let Ok(limit_val) = n.parse::<usize>() {
                        current_plan = LogicalPlan::limit(limit_val, schema, current_plan);
                    } else {
                        return Err("Invalid LIMIT value".to_string());
                    }
                } else {
                    return Err("LIMIT must be a number".to_string());
                }
            } else {
                return Err("LIMIT must be a number".to_string());
            }
        }

        // Handle FETCH if present (similar to LIMIT)
        if let Some(fetch) = &query.fetch {
            let Some(schema) = current_plan.get_schema() else {
                todo!()
            };
            // In sqlparser 0.56.0, fetch.quantity is an Option<Expr>
            if let Some(quantity_expr) = &fetch.quantity {
                if let Expr::Value(value_with_span) = quantity_expr {
                    if let sqlparser::ast::Value::Number(n, _) = &value_with_span.value {
                        if let Ok(fetch_val) = n.parse::<usize>() {
                            current_plan = LogicalPlan::limit(fetch_val, schema, current_plan);
                        } else {
                            return Err("Invalid FETCH value".to_string());
                        }
                    } else {
                        return Err("FETCH quantity must be a number".to_string());
                    }
                } else {
                    return Err("FETCH quantity must be a number".to_string());
                }
            } else {
                return Err("FETCH quantity is required".to_string());
            }
        }
        Ok(current_plan)
    }

    pub fn build_select_plan(&self, select: &Box<Select>) -> Result<Box<LogicalPlan>, String> {
        let mut current_plan = {
            if select.from.is_empty() {
                return Err("FROM clause is required".to_string());
            }

            // Check if we have a join (multiple tables in FROM clause)
            if select.from.len() > 1 || !select.from[0].joins.is_empty() {
                // Use the expression parser's prepare_join_scan method to handle joins
                self.prepare_join_scan(select)?
            } else {
                // Single table query
                self.build_table_scan(&select.from[0])?
            }
        };

        // Process WHERE clause if it exists
        if let Some(where_clause) = &select.selection {
            debug!("Processing WHERE clause");
            let Some(parsing_schema) = current_plan.get_schema().clone() else {
                todo!()
            };
            debug!("Schema has {} columns", parsing_schema.get_column_count());
            let filter_expr = self
                .expression_parser
                .parse_expression(&where_clause, &parsing_schema)?;
            current_plan = LogicalPlan::filter(
                parsing_schema,
                String::new(),
                0,
                Arc::new(filter_expr),
                current_plan,
            );
        }

        // Handle GROUP BY and aggregates
        let has_group_by = match &select.group_by {
            GroupByExpr::All(_) => true,
            GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
        };
        let Some(schema) = current_plan.get_schema() else {
            todo!()
        };

        // Parse all expressions in the projection to identify aggregates
        let mut has_aggregates = false;
        let mut agg_exprs: Vec<Arc<Expression>> = Vec::new();

        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    // Use the original schema for parsing expressions to ensure all columns are available
                    let parsed_expr = self.expression_parser.parse_expression(expr, &schema)?;
                    if let Expression::Aggregate(_) = parsed_expr {
                        has_aggregates = true;
                        agg_exprs.push(Arc::new(parsed_expr));
                    }
                }
                SelectItem::ExprWithAlias { expr, alias: _ } => {
                    // Use the original schema for parsing expressions to ensure all columns are available
                    let parsed_expr = self.expression_parser.parse_expression(expr, &schema)?;
                    if let Expression::Aggregate(_) = parsed_expr {
                        has_aggregates = true;
                        agg_exprs.push(Arc::new(parsed_expr));
                    }
                }
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                    // We'll check for wildcards after determining if there are aggregates
                    // No action needed here
                }
            }
        }

        if has_group_by || has_aggregates {
            // Check for wildcards now that we know we have aggregates
            for item in &select.projection {
                if matches!(
                    item,
                    SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
                ) {
                    return Err("Wildcard projections are not supported in aggregates".to_string());
                }
            }

            // Get group by expressions
            let group_by_exprs = self.expression_parser.determine_group_by_expressions(
                select,
                &schema,
                has_group_by,
            )?;

            // Create a new schema for the aggregate plan that includes both group by and aggregate columns
            let mut agg_columns = Vec::new();

            // Add group by columns
            for expr in &group_by_exprs {
                let col_name = match expr {
                    Expression::ColumnRef(col_ref) => {
                        col_ref.get_return_type().get_name().to_string()
                    }
                    _ => expr.get_return_type().get_name().to_string(),
                };
                agg_columns.push(Column::new(&col_name, expr.get_return_type().get_type()));
            }

            // Add aggregate columns
            for agg_expr in &agg_exprs {
                match agg_expr.as_ref() {
                    Expression::Aggregate(agg) => {
                        let col_name = agg.get_column_name();
                        agg_columns.push(Column::new(&col_name, agg.get_return_type().get_type()));
                    }
                    _ => unreachable!("Expected aggregate expression"),
                }
            }

            let agg_schema = Schema::new(agg_columns);
            let agg_schema_clone = agg_schema.clone();

            // Create aggregation plan node
            current_plan = LogicalPlan::aggregate(
                group_by_exprs.into_iter().map(|e| Arc::new(e)).collect(),
                agg_exprs,
                agg_schema,
                current_plan,
            );

            // Apply HAVING clause if it exists
            if let Some(having) = &select.having {
                // Use the original schema for parsing the HAVING clause instead of the aggregate schema
                // This ensures columns like 'age' can be found when used inside aggregate functions
                let having_expr = self.expression_parser.parse_expression(having, &schema)?;

                current_plan = LogicalPlan::filter(
                    current_plan.get_schema().clone().unwrap(),
                    String::new(), // table_name
                    0,             // table_oid
                    Arc::new(having_expr),
                    current_plan,
                );
            }

            // Add projection on top of aggregation
            // Use the original schema for parsing projection expressions
            current_plan =
                self.build_projection_plan_with_schema(&select.projection, current_plan, &schema)?;
        } else {
            // No aggregates, just add projection
            current_plan = self.build_projection_plan(&select.projection, current_plan)?;
        }

        // Apply HAVING clause if it exists and we don't have aggregates
        // This is a bit unusual but SQL allows it
        if !has_aggregates && !has_group_by {
            if let Some(having) = &select.having {
                let having_expr = self.expression_parser.parse_expression(having, &schema)?;
                current_plan = LogicalPlan::filter(
                    current_plan.get_schema().clone().unwrap(),
                    String::new(), // table_name
                    0,             // table_oid
                    Arc::new(having_expr),
                    current_plan,
                );
            }
        }

        // Apply SORT BY if it exists (Hive-specific)
        if !select.sort_by.is_empty() {
            let Some(schema) = current_plan.get_schema() else {
                todo!()
            };
            let sort_exprs = select
                .sort_by
                .iter()
                .map(|expr| {
                    let parsed_expr = self.expression_parser.parse_expression(expr, &schema)?;
                    Ok(Arc::new(parsed_expr))
                })
                .collect::<Result<Vec<_>, String>>()?;

            if !sort_exprs.is_empty() {
                current_plan = LogicalPlan::sort(sort_exprs, schema.clone(), current_plan);
            }
        }

        Ok(current_plan)
    }

    // New helper method to build projection plan with a specific schema for expression parsing
    pub fn build_projection_plan_with_schema(
        &self,
        projection: &[SelectItem],
        input_plan: Box<LogicalPlan>,
        parse_schema: &Schema,
    ) -> Result<Box<LogicalPlan>, String> {
        let input_schema = input_plan.get_schema().unwrap();
        debug!("Building projection plan");
        debug!("Input schema: {:?}", input_schema);
        debug!("Projection items: {:?}", projection);

        let mut projection_exprs = Vec::new();
        let mut output_columns = Vec::new();

        // Check if we're projecting from an aggregate plan
        let is_aggregate_input = matches!(input_plan.plan_type, LogicalPlanType::Aggregate { .. });

        for (i, item) in projection.iter().enumerate() {
            debug!("Processing projection item {}: {:?}", i, item);
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    debug!("Processing unnamed expression: {:?}", expr);
                    // Use the provided schema for parsing expressions
                    let parsed_expr = self
                        .expression_parser
                        .parse_expression(expr, parse_schema)?;

                    // For column references, use the column name from the expression
                    let col_name = match expr {
                        Expr::Identifier(ident) => ident.value.clone(),
                        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                            format!("{}.{}", parts[0].value, parts[1].value)
                        }
                        _ => {
                            // For aggregate expressions, use the get_column_name method
                            match &parsed_expr {
                                Expression::Aggregate(agg) => agg.get_column_name(),
                                _ => parsed_expr.to_string(),
                            }
                        }
                    };

                    let output_col =
                        Column::new(&col_name, parsed_expr.get_return_type().get_type());
                    debug!("Created column: {:?}", output_col);
                    output_columns.push(output_col);

                    // For aggregate input, use column reference for non-aggregate expressions
                    if is_aggregate_input && !matches!(parsed_expr, Expression::Aggregate(_)) {
                        // Find the column in the input schema
                        if let Some(col_idx) = input_schema.get_qualified_column_index(&col_name) {
                            projection_exprs.push(Arc::new(Expression::ColumnRef(
                                ColumnRefExpression::new(
                                    0,
                                    col_idx,
                                    input_schema.get_column(col_idx).unwrap().clone(),
                                    vec![],
                                ),
                            )));
                        } else {
                            projection_exprs.push(Arc::new(parsed_expr));
                        }
                    } else {
                        projection_exprs.push(Arc::new(parsed_expr));
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    debug!(
                        "Processing aliased expression: {:?} AS {}",
                        expr, alias.value
                    );
                    // Use the provided schema for parsing expressions
                    let parsed_expr = self
                        .expression_parser
                        .parse_expression(expr, parse_schema)?;
                    let output_col =
                        Column::new(&alias.value, parsed_expr.get_return_type().get_type());
                    debug!("Created aliased column: {:?}", output_col);
                    output_columns.push(output_col);

                    // For aggregate input, use column reference for non-aggregate expressions
                    if is_aggregate_input && !matches!(parsed_expr, Expression::Aggregate(_)) {
                        // Find the column in the input schema
                        if let Some(col_idx) = input_schema.get_qualified_column_index(&alias.value)
                        {
                            projection_exprs.push(Arc::new(Expression::ColumnRef(
                                ColumnRefExpression::new(
                                    0,
                                    col_idx,
                                    input_schema.get_column(col_idx).unwrap().clone(),
                                    vec![],
                                ),
                            )));
                        } else {
                            projection_exprs.push(Arc::new(parsed_expr));
                        }
                    } else {
                        projection_exprs.push(Arc::new(parsed_expr));
                    }
                }
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                    debug!("Processing wildcard");
                    for i in 0..input_schema.get_column_count() {
                        let col = input_schema.get_column(i as usize).unwrap();
                        debug!("Adding wildcard column: {:?}", col);
                        output_columns.push(col.clone());
                        projection_exprs.push(Arc::new(Expression::ColumnRef(
                            ColumnRefExpression::new(0, i as usize, col.clone(), vec![]),
                        )));
                    }
                }
            }
        }

        let output_schema = Schema::new(output_columns);
        debug!("Final projection schema: {:?}", output_schema);
        debug!("Projection expressions: {:?}", projection_exprs);

        Ok(LogicalPlan::project(
            projection_exprs,
            output_schema,
            input_plan,
        ))
    }

    pub fn build_insert_plan(&self, insert: &Insert) -> Result<Box<LogicalPlan>, String> {
        // Extract the table name from the table field
        let table_name = match &insert.table {
            TableObject::TableName(obj_name) => {
                self.expression_parser.extract_table_name(obj_name)?
            }
            TableObject::TableFunction(_) => {
                return Err("Table functions are not supported in INSERT statements".to_string());
            }
        };

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
                        .schemas_compatible(&select_plan.get_schema().unwrap(), &schema)
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

    pub fn build_update_plan(
        &self,
        table: &TableWithJoins,
        assignments: &Vec<Assignment>,
        from: &Option<UpdateTableFromKind>,
        selection: &Option<Expr>,
        returning: &Option<Vec<SelectItem>>,
        or: &Option<SqliteOnConflict>,
    ) -> Result<Box<LogicalPlan>, String> {
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
        let mut current_plan =
            LogicalPlan::table_scan(table_name.clone(), schema.clone(), table_oid);

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
        select_items: &[SelectItem],
        input_plan: Box<LogicalPlan>,
    ) -> Result<Box<LogicalPlan>, String> {
        let input_schema = input_plan.get_schema().unwrap();
        let parse_schema = input_schema.as_ref();

        let mut output_columns = Vec::new();
        let mut projection_exprs = Vec::new();

        debug!(
            "Building projection plan with input schema: {:?}",
            input_schema
        );
        debug!("Select items: {:?}", select_items);

        for item in select_items {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    debug!("Processing unnamed expression: {:?}", expr);

                    // Determine column name from the expression
                    let column_name = match expr {
                        Expr::Identifier(ident) => ident.value.clone(),
                        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                            format!("{}.{}", parts[0].value, parts[1].value)
                        }
                        _ => {
                            // Use the provided schema for parsing expressions
                            let parsed_expr = self
                                .expression_parser
                                .parse_expression(expr, parse_schema)?;

                            // For aggregate expressions, use the get_column_name method
                            match &parsed_expr {
                                Expression::Aggregate(agg) => agg.get_column_name(),
                                _ => parsed_expr.to_string(),
                            }
                        }
                    };

                    // For qualified column references (table.column format)
                    let parsed_expr = self
                        .expression_parser
                        .parse_expression(expr, parse_schema)?;

                    // Use the column name from the expression for the output column
                    let output_col =
                        Column::new(&column_name, parsed_expr.get_return_type().get_type());
                    debug!("Created column: {:?}", output_col);
                    output_columns.push(output_col);
                    projection_exprs.push(Arc::new(parsed_expr));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    debug!(
                        "Processing aliased expression: {:?} AS {}",
                        expr, alias.value
                    );
                    // Use the provided schema for parsing expressions
                    let parsed_expr = self
                        .expression_parser
                        .parse_expression(expr, parse_schema)?;
                    let output_col =
                        Column::new(&alias.value, parsed_expr.get_return_type().get_type());
                    debug!("Created aliased column: {:?}", output_col);

                    output_columns.push(output_col);
                    projection_exprs.push(Arc::new(parsed_expr));
                }
                SelectItem::Wildcard(_) => {
                    debug!("Processing wildcard");
                    // Add all columns from the input schema
                    for i in 0..input_schema.get_column_count() {
                        let col = input_schema.get_column(i as usize).unwrap();
                        output_columns.push(col.clone());

                        let col_expr = Expression::ColumnRef(ColumnRefExpression::new(
                            0,
                            i as usize,
                            col.clone(),
                            vec![],
                        ));
                        projection_exprs.push(Arc::new(col_expr));
                    }
                }
                SelectItem::QualifiedWildcard(obj_name, _) => {
                    debug!("Processing qualified wildcard: {:?}", obj_name);
                    let table_alias = obj_name.to_string();

                    // Add all columns from the input schema that match the table alias
                    let mut found_columns = false;
                    for i in 0..input_schema.get_column_count() {
                        let col = input_schema.get_column(i as usize).unwrap();
                        let col_name = col.get_name();

                        // Check if the column belongs to the specified table
                        if col_name.starts_with(&format!("{}.", table_alias)) {
                            found_columns = true;
                            output_columns.push(col.clone());

                            let col_expr = Expression::ColumnRef(ColumnRefExpression::new(
                                0,
                                i as usize,
                                col.clone(),
                                vec![],
                            ));
                            projection_exprs.push(Arc::new(col_expr));
                        }
                    }

                    if !found_columns {
                        return Err(format!("No columns found for table alias: {}", table_alias));
                    }
                }
            }
        }

        let output_schema = Schema::new(output_columns);
        Ok(LogicalPlan::project(
            projection_exprs,
            output_schema,
            input_plan,
        ))
    }

    pub fn build_create_table_plan(
        &self,
        create_table: &CreateTable,
    ) -> Result<Box<LogicalPlan>, String> {
        let table_name = create_table.name.to_string();

        // let a = create_table.columns.iter().collect();

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
            ObjectName(parts) if parts.len() == 1 => parts[0].to_string(),
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

    pub fn prepare_join_scan(&self, select: &Box<Select>) -> Result<Box<LogicalPlan>, String> {
        if select.from.is_empty() {
            return Err("FROM clause is required".to_string());
        }

        // Process each table in the FROM clause
        let mut current_plan: Option<Box<LogicalPlan>> = None;

        for table_with_joins in &select.from {
            let table_plan = self.process_table_with_joins(table_with_joins)?;

            if let Some(existing_plan) = current_plan {
                // If we already have a plan, create a cross join with the new table
                let left_schema = existing_plan.get_schema().clone().unwrap();
                let right_schema = table_plan.get_schema().clone().unwrap();

                current_plan = Some(Box::new(LogicalPlan::new(
                    LogicalPlanType::NestedLoopJoin {
                        left_schema,
                        right_schema: right_schema.clone(),
                        predicate: Arc::new(Expression::Constant(ConstantExpression::new(
                            Value::new(true),
                            Column::new("TRUE", TypeId::Boolean),
                            vec![],
                        ))),
                        join_type: JoinOperator::CrossJoin,
                    },
                    vec![existing_plan, table_plan],
                )));
            } else {
                current_plan = Some(table_plan);
            }
        }

        current_plan.ok_or_else(|| "Failed to create join plan".to_string())
    }

    fn process_table_with_joins(
        &self,
        table_with_joins: &TableWithJoins,
    ) -> Result<Box<LogicalPlan>, String> {
        // First process the base table
        let mut current_plan = self.process_table_factor(&table_with_joins.relation)?;

        // Process each join
        for join in &table_with_joins.joins {
            let right_plan = self.process_table_factor(&join.relation)?;

            let left_schema = current_plan.get_schema().clone().unwrap();
            let right_schema = right_plan.get_schema().clone().unwrap();
            let joined_schema = Schema::merge_with_aliases(&left_schema, &right_schema, None, None);

            // Get the join condition if it exists
            let predicate = match &join.join_operator {
                JoinOperator::Inner(constraint)
                | JoinOperator::LeftOuter(constraint)
                | JoinOperator::RightOuter(constraint)
                | JoinOperator::FullOuter(constraint) => match constraint {
                    JoinConstraint::On(expr) => self
                        .expression_parser
                        .parse_expression(expr, &joined_schema)?,
                    _ => return Err("Only ON constraints are supported for joins".to_string()),
                },
                JoinOperator::CrossJoin => Expression::Constant(ConstantExpression::new(
                    Value::new(true),
                    Column::new("TRUE", TypeId::Boolean),
                    vec![],
                )),
                JoinOperator::LeftSemi(constraint)
                | JoinOperator::RightSemi(constraint)
                | JoinOperator::LeftAnti(constraint)
                | JoinOperator::RightAnti(constraint) => match constraint {
                    JoinConstraint::On(expr) => self
                        .expression_parser
                        .parse_expression(expr, &joined_schema)?,
                    _ => {
                        return Err(
                            "Only ON constraints are supported for semi/anti joins".to_string()
                        );
                    }
                },
                // Simple synonym for Inner join
                JoinOperator::Join(constraint) => match constraint {
                    JoinConstraint::On(expr) => self
                        .expression_parser
                        .parse_expression(expr, &joined_schema)?,
                    _ => return Err("Only ON constraints are supported for joins".to_string()),
                },
                // Simple synonym for LeftOuter join
                JoinOperator::Left(constraint) => match constraint {
                    JoinConstraint::On(expr) => self
                        .expression_parser
                        .parse_expression(expr, &joined_schema)?,
                    _ => return Err("Only ON constraints are supported for LEFT joins".to_string()),
                },
                // Simple synonym for RightOuter join
                JoinOperator::Right(constraint) => match constraint {
                    JoinConstraint::On(expr) => self
                        .expression_parser
                        .parse_expression(expr, &joined_schema)?,
                    _ => {
                        return Err("Only ON constraints are supported for RIGHT joins".to_string());
                    }
                },
                // Simple synonym for LeftSemi join
                JoinOperator::Semi(constraint) => match constraint {
                    JoinConstraint::On(expr) => self
                        .expression_parser
                        .parse_expression(expr, &joined_schema)?,
                    _ => return Err("Only ON constraints are supported for SEMI joins".to_string()),
                },
                // Simple synonym for LeftAnti join
                JoinOperator::Anti(constraint) => match constraint {
                    JoinConstraint::On(expr) => self
                        .expression_parser
                        .parse_expression(expr, &joined_schema)?,
                    _ => return Err("Only ON constraints are supported for ANTI joins".to_string()),
                },
                // APPLY joins require a table function on the right
                JoinOperator::CrossApply => {
                    return Err("CROSS APPLY joins are not yet supported".to_string());
                }
                JoinOperator::OuterApply => {
                    return Err("OUTER APPLY joins are not yet supported".to_string());
                }
                // Time-series join variant
                JoinOperator::AsOf { .. } => {
                    return Err("AS OF joins are not yet supported".to_string());
                }
                // MySQL-specific join
                JoinOperator::StraightJoin(constraint) => match constraint {
                    JoinConstraint::On(expr) => self
                        .expression_parser
                        .parse_expression(expr, &joined_schema)?,
                    _ => {
                        return Err(
                            "Only ON constraints are supported for STRAIGHT_JOIN".to_string()
                        );
                    }
                },
            };

            current_plan = Box::new(LogicalPlan::new(
                LogicalPlanType::NestedLoopJoin {
                    left_schema,
                    right_schema,
                    predicate: Arc::new(predicate),
                    join_type: join.join_operator.clone(),
                },
                vec![current_plan, right_plan],
            ));
        }

        Ok(current_plan)
    }

    fn process_table_factor(&self, table_factor: &TableFactor) -> Result<Box<LogicalPlan>, String> {
        match table_factor {
            TableFactor::Table { name, alias, .. } => {
                let table_name = self.expression_parser.extract_table_name(name)?;
                let mut schema = self.expression_parser.get_table_schema(&table_name)?;
                let table_oid = self.expression_parser.get_table_oid(&table_name)?;

                // Apply alias to schema if provided
                if let Some(table_alias) = alias {
                    let alias_name = table_alias.name.value.clone();
                    let mut aliased_columns = Vec::new();
                    for col in schema.get_columns() {
                        let mut new_col = col.clone();
                        if !col.get_name().contains('.') {
                            new_col.set_name(format!("{}.{}", alias_name, col.get_name()));
                        }
                        aliased_columns.push(new_col);
                    }
                    schema = Schema::new(aliased_columns);
                }

                Ok(LogicalPlan::table_scan(table_name, schema, table_oid))
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let mut plan = self.build_query_plan(subquery)?;

                // Apply alias if provided
                if let Some(table_alias) = alias {
                    let alias_name = table_alias.name.value.clone();
                    let schema = plan.get_schema().unwrap();
                    let mut aliased_columns = Vec::new();
                    for col in schema.get_columns() {
                        let mut new_col = col.clone();
                        if !col.get_name().contains('.') {
                            new_col.set_name(format!("{}.{}", alias_name, col.get_name()));
                        }
                        aliased_columns.push(new_col);
                    }
                    plan = Box::new(LogicalPlan::new(
                        LogicalPlanType::Projection {
                            expressions: vec![], // Will be filled by projection
                            schema: Schema::new(aliased_columns),
                            column_mappings: vec![],
                        },
                        vec![plan],
                    ));
                }

                Ok(plan)
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => {
                let mut plan = self.process_table_with_joins(table_with_joins)?;

                // Apply alias if provided
                if let Some(table_alias) = alias {
                    let alias_name = table_alias.name.value.clone();
                    let schema = plan.get_schema().unwrap();
                    let mut aliased_columns = Vec::new();
                    for col in schema.get_columns() {
                        let mut new_col = col.clone();
                        if !col.get_name().contains('.') {
                            new_col.set_name(format!("{}.{}", alias_name, col.get_name()));
                        }
                        aliased_columns.push(new_col);
                    }
                    plan = Box::new(LogicalPlan::new(
                        LogicalPlanType::Projection {
                            expressions: vec![], // Will be filled by projection
                            schema: Schema::new(aliased_columns),
                            column_mappings: vec![],
                        },
                        vec![plan],
                    ));
                }

                Ok(plan)
            }
            _ => Err(format!("Unsupported table factor type: {:?}", table_factor)),
        }
    }

    fn build_table_scan(
        &self,
        table_with_joins: &TableWithJoins,
    ) -> Result<Box<LogicalPlan>, String> {
        match &table_with_joins.relation {
            TableFactor::Table { name, alias, .. } => {
                let table_name = self.expression_parser.extract_table_name(name)?;
                let schema = self.expression_parser.get_table_schema(&table_name)?;
                let table_oid = self.expression_parser.get_table_oid(&table_name)?;

                // If an alias is provided, apply it to the schema
                let final_schema = if let Some(table_alias) = alias {
                    let alias_name = table_alias.name.value.clone();
                    // Create a new schema with the alias applied to all columns
                    let mut aliased_columns = Vec::new();
                    for col in schema.get_columns() {
                        let mut new_col = col.clone();
                        // Only add alias if the column doesn't already have one
                        if !col.get_name().contains('.') {
                            new_col.set_name(format!("{}.{}", alias_name, col.get_name()));
                        }
                        aliased_columns.push(new_col);
                    }
                    Schema::new(aliased_columns)
                } else {
                    schema
                };

                Ok(LogicalPlan::table_scan(table_name, final_schema, table_oid))
            }
            _ => Err("Only simple table scans are supported".to_string()),
        }
    }

    // ---------- PRIORITY 1: TRANSACTION MANAGEMENT ----------

    pub fn build_start_transaction_plan(
        &self,
        modes: &Vec<TransactionMode>,
        begin: &bool,
        transaction: &Option<BeginTransactionKind>,
        modifier: &Option<TransactionModifier>,
        statements: &Vec<Statement>,
        exception_statements: &Option<Vec<Statement>>,
        has_end_keyword: &bool,
    ) -> Result<Box<LogicalPlan>, String> {
        // Initialize default transaction properties
        let mut access_mode = TransactionAccessMode::ReadWrite;
        let mut isolation_level = TransactionIsolationLevel::ReadCommitted;

        // Parse transaction modes
        for mode in modes {
            match mode {
                TransactionMode::AccessMode(am) => access_mode = *am,
                TransactionMode::IsolationLevel(il) => isolation_level = *il,
            }
        }

        // Map AST isolation level to internal isolation level
        let internal_isolation_level = match isolation_level {
            TransactionIsolationLevel::ReadUncommitted => IsolationLevel::ReadUncommitted,
            TransactionIsolationLevel::ReadCommitted => IsolationLevel::ReadCommitted,
            TransactionIsolationLevel::RepeatableRead => IsolationLevel::RepeatableRead,
            TransactionIsolationLevel::Serializable => IsolationLevel::Serializable,
            TransactionIsolationLevel::Snapshot => IsolationLevel::Snapshot,
        };

        // Log the transaction modes
        debug!(
            "Transaction Access Mode: {:?}, Internal Isolation Level: {:?}",
            access_mode, internal_isolation_level
        );

        // Handle transaction modifiers
        let transaction_modifier = modifier.clone();

        // Determine if the transaction is read-only based on the access mode
        let read_only = matches!(access_mode, TransactionAccessMode::ReadOnly);

        // Determine the transaction kind
        let transaction_kind = match transaction {
            Some(BeginTransactionKind::Transaction) => "TRANSACTION",
            Some(BeginTransactionKind::Work) => "WORK",
            None => "",
        };

        // Determine if the transaction should begin
        let begin_keyword = if *begin { "BEGIN" } else { "" };

        // Create the logical plan for starting a transaction
        let transaction_plan = LogicalPlan::start_transaction(
            Some(internal_isolation_level),
            read_only,
            transaction_modifier,
            statements.clone(),
            exception_statements.clone(),
            *has_end_keyword,
        );

        // Log the transaction initiation
        debug!(
            "Starting transaction: {} {}",
            begin_keyword, transaction_kind
        );

        Ok(transaction_plan)
    }

    pub fn build_commit_plan(
        &self,
        chain: &bool,
        end: &bool,
        modifier: &Option<TransactionModifier>,
    ) -> Result<Box<LogicalPlan>, String> {
        // Create the logical plan for committing a transaction
        debug!("Creating commit plan: chain={}, end={}", chain, end);

        // Create a commit transaction plan
        let commit_plan = LogicalPlan::commit_transaction(*chain, *end, modifier.clone());

        Ok(commit_plan)
    }

    pub fn build_rollback_plan(
        &self,
        chain: &bool,
        savepoint: &Option<Ident>,
    ) -> Result<Box<LogicalPlan>, String> {
        // Create the logical plan for rolling back a transaction
        debug!("Creating rollback plan: chain={}", chain);

        // Create a rollback transaction plan
        let rollback_plan = LogicalPlan::rollback_transaction(*chain, savepoint.clone());

        Ok(rollback_plan)
    }

    pub fn build_savepoint_plan(&self, stmt: &Ident) -> Result<Box<LogicalPlan>, String> {
        // Create the logical plan for creating a savepoint
        debug!("Creating savepoint plan for: {}", stmt.value);

        // Create a savepoint plan
        let savepoint_plan = LogicalPlan::savepoint(stmt.value.clone());

        Ok(savepoint_plan)
    }

    pub fn build_release_savepoint_plan(&self, stmt: &Ident) -> Result<Box<LogicalPlan>, String> {
        // Create the logical plan for releasing a savepoint
        debug!("Creating release savepoint plan for: {}", stmt.value);

        // Create a release savepoint plan
        let release_savepoint_plan = LogicalPlan::release_savepoint(stmt.value.clone());

        Ok(release_savepoint_plan)
    }

    // ---------- PRIORITY 2: DDL OPERATIONS ----------

    pub fn build_drop_plan(
        &self,
        object_type: String,
        if_exists: bool,
        names: Vec<String>,
        cascade: bool,
    ) -> Result<Box<LogicalPlan>, String> {
        // Create the logical plan for dropping objects
        debug!(
            "Creating drop plan for {} objects: {:?}",
            object_type, names
        );

        // Create a drop plan using the static constructor
        let drop_plan = LogicalPlan::drop(object_type, if_exists, names, cascade);

        Ok(drop_plan)
    }

    pub fn build_create_schema_plan(
        &self,
        schema_name: &SchemaName,
        if_not_exists: &bool,
    ) -> Result<Box<LogicalPlan>, String> {
        // Extract the schema name string from SchemaName
        let schema_name_str = match schema_name {
            SchemaName::Simple(obj_name) => {
                if obj_name.0.is_empty() {
                    return Err("Schema name cannot be empty".to_string());
                }
                obj_name.0[0].to_string()
            }
            SchemaName::UnnamedAuthorization(ident) => {
                // Using authorization identifier as schema name
                ident.to_string()
            }
            SchemaName::NamedAuthorization(obj_name, _) => {
                if obj_name.0.is_empty() {
                    return Err("Schema name cannot be empty".to_string());
                }
                obj_name.0[0].to_string()
            }
        };

        // Use the existing LogicalPlan::create_schema constructor
        let plan = LogicalPlan::create_schema(schema_name_str, *if_not_exists);

        Ok(plan)
    }

    pub fn build_create_database_plan(
        &self,
        db_name: &ObjectName,
        if_not_exists: &bool,
        location: &Option<String>,
        managed_location: &Option<String>,
    ) -> Result<Box<LogicalPlan>, String> {
        // Extract the database name from the ObjectName
        if db_name.0.is_empty() {
            return Err("Database name cannot be empty".to_string());
        }

        // Extract the string value from the object name structure
        let db_name_str = match &db_name.0[0] {
            ObjectNamePart::Identifier(ident) => ident.value.clone(),
        };

        // Currently, location and managed_location parameters are not used in our logical plan
        // In a real implementation, you might want to handle these parameters
        if location.is_some() || managed_location.is_some() {
            // Log that these parameters are ignored for now
            debug!("Location and managed_location parameters are currently ignored");
        }

        // Use the existing LogicalPlan::create_database constructor
        let plan = LogicalPlan::create_database(db_name_str, *if_not_exists);

        Ok(plan)
    }

    pub fn build_alter_table_plan(
        &self,
        name: &ObjectName,
        if_exists: &bool,
        only: &bool,
        operations: &Vec<AlterTableOperation>,
        location: &Option<HiveSetLocation>,
        on_cluster: &Option<Ident>,
    ) -> Result<Box<LogicalPlan>, String> {
        // Extract the table name
        if name.0.is_empty() {
            return Err("Table name cannot be empty".to_string());
        }

        let table_name = match &name.0[0] {
            ObjectNamePart::Identifier(ident) => ident.value.clone(),
        };

        // Handle if_exists condition - if table doesn't exist and if_exists is false, it's an error
        // In our case, we just pass this information to the logical plan

        // Handle the ONLY keyword (PostgreSQL-specific for inheritance)
        let only_str = if *only { " ONLY" } else { "" };

        // Handle ON CLUSTER (ClickHouse-specific)
        let on_cluster_str = if let Some(cluster) = on_cluster {
            format!(" ON CLUSTER {}", cluster.value)
        } else {
            String::new()
        };

        // Convert operations to a string representation
        let operation_str = if operations.is_empty() {
            return Err("No operations specified for ALTER TABLE".to_string());
        } else {
            // Create a string representation of the first operation
            // In a real implementation, you might want to handle multiple operations
            match &operations[0] {
                AlterTableOperation::AddColumn {
                    column_keyword,
                    if_not_exists,
                    column_def,
                    ..
                } => {
                    format!(
                        "ADD {}COLUMN {}{}",
                        if *column_keyword { "COLUMN " } else { "" },
                        if *if_not_exists { "IF NOT EXISTS " } else { "" },
                        column_def.name.value
                    )
                }
                AlterTableOperation::DropColumn {
                    column_name,
                    if_exists,
                    ..
                } => {
                    format!(
                        "DROP COLUMN {}{}",
                        if *if_exists { "IF EXISTS " } else { "" },
                        column_name.value
                    )
                }
                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    format!(
                        "RENAME COLUMN {} TO {}",
                        old_column_name.value, new_column_name.value
                    )
                }
                AlterTableOperation::RenameTable {
                    table_name: new_table,
                } => {
                    let new_name = match &new_table.0[0] {
                        ObjectNamePart::Identifier(ident) => ident.value.clone(),
                    };
                    format!("RENAME TO {}", new_name)
                }
                AlterTableOperation::AlterColumn { column_name, op } => {
                    format!("ALTER COLUMN {}", column_name.value)
                }
                _ => {
                    // Generic operation string for other types
                    "ALTER TABLE OPERATION".to_string()
                }
            }
        };

        // Handle location (Hive-specific)
        let location_str = if let Some(loc) = location {
            let location_value = loc.location.value.clone();
            if loc.has_set {
                format!(" SET LOCATION '{}'", location_value)
            } else {
                format!(" LOCATION '{}'", location_value)
            }
        } else {
            String::new()
        };

        // Build the complete operation string
        let full_operation = format!(
            "{}{}{}{}",
            operation_str, only_str, on_cluster_str, location_str
        );

        // Use the existing logical plan constructor
        let plan = LogicalPlan::alter_table(table_name, full_operation);

        Ok(plan)
    }

    pub fn build_create_view_plan(
        &self,
        or_alter: &bool,
        or_replace: &bool,
        materialized: &bool,
        name: &ObjectName,
        columns: &Vec<ViewColumnDef>,
        query: &Box<Query>,
        options: &CreateTableOptions,
        cluster_by: &Vec<Ident>,
        comment: &Option<String>,
        with_no_schema_binding: &bool,
        if_not_exists: &bool,
        temporary: &bool,
        to: &Option<ObjectName>,
        params: &Option<CreateViewParams>,
    ) -> Result<Box<LogicalPlan>, String> {
        // Extract view name from ObjectName
        if name.0.is_empty() {
            return Err("View name cannot be empty".to_string());
        }

        let view_name = match &name.0[0] {
            ObjectNamePart::Identifier(ident) => ident.value.clone(),
        };

        // Check if view already exists
        {
            let catalog = self.expression_parser.catalog();
            let catalog_guard = catalog.read();
            if catalog_guard.get_table(&view_name).is_some() {
                // If view exists and IF NOT EXISTS flag is set, return success with a dummy plan
                if *if_not_exists {
                    // Build a query plan to get the schema
                    let query_plan = self.build_query_plan(query)?;
                    let schema = query_plan
                        .get_schema()
                        .ok_or_else(|| "Failed to determine schema for view".to_string())?;
                    return Ok(LogicalPlan::create_view(view_name, schema, true));
                }
                // Otherwise return error
                return Err(format!("View '{}' already exists", view_name));
            }
        }

        // Build a query plan to derive the view's schema
        let query_plan = self.build_query_plan(query)?;

        // Get the schema from the query plan
        let schema = query_plan
            .get_schema()
            .ok_or_else(|| "Failed to determine schema for view".to_string())?;

        // Handle custom column names if provided
        let final_schema = if !columns.is_empty() {
            // If column names are provided, replace the column names in the schema
            if columns.len() != schema.get_column_count() as usize {
                return Err(format!(
                    "Number of column names ({}) does not match number of columns in result set ({})",
                    columns.len(),
                    schema.get_column_count()
                ));
            }

            let mut new_columns = Vec::new();
            for (i, col_def) in columns.iter().enumerate() {
                let original_col = schema.get_column(i).unwrap();
                let mut new_col = original_col.clone();
                new_col.set_name(col_def.name.value.clone());
                new_columns.push(new_col);
            }

            Schema::new(new_columns)
        } else {
            schema.clone()
        };

        // Handle materialized views - in a real implementation, this would involve additional logic
        if *materialized {
            debug!("Creating materialized view (implemented as regular view for now)");
        }

        // Create the logical plan
        Ok(LogicalPlan::create_view(
            view_name,
            final_schema,
            *if_not_exists,
        ))
    }

    pub fn build_alter_view_plan(
        &self,
        name: &ObjectName,
        columns: &Vec<Ident>,
        query: &Box<Query>,
        with_options: &Vec<SqlOption>,
    ) -> Result<Box<LogicalPlan>, String> {
        // Extract view name from ObjectName
        if name.0.is_empty() {
            return Err("View name cannot be empty".to_string());
        }

        let view_name = match &name.0[0] {
            ObjectNamePart::Identifier(ident) => ident.value.clone(),
        };

        // Check if view exists
        {
            let catalog = self.expression_parser.catalog();
            let catalog_guard = catalog.read();
            if catalog_guard.get_table(&view_name).is_none() {
                return Err(format!("View '{}' does not exist", view_name));
            }
        }

        // Build a query plan to derive the view's new schema
        let query_plan = self.build_query_plan(query)?;

        // Get the schema from the query plan
        let schema = query_plan
            .get_schema()
            .ok_or_else(|| "Failed to determine schema for altered view".to_string())?;

        // Handle custom column names if provided
        if !columns.is_empty() {
            if columns.len() != schema.get_column_count() as usize {
                return Err(format!(
                    "Number of column names ({}) does not match number of columns in result set ({})",
                    columns.len(),
                    schema.get_column_count()
                ));
            }
        }

        // Construct the operation string
        let mut operation = "AS ".to_string();

        // Add the column list if specified
        if !columns.is_empty() {
            operation.push_str("(");
            for (i, col) in columns.iter().enumerate() {
                if i > 0 {
                    operation.push_str(", ");
                }
                operation.push_str(&col.value);
            }
            operation.push_str(") ");
        }

        // Add the new query
        // Instead of a placeholder, let's include the actual query text
        // We'll use a simplified representation that indicates the query type and some key details
        match &*query.body {
            SetExpr::Select(select) => {
                operation.push_str("SELECT ");

                // Add projection info
                if !select.projection.is_empty() {
                    let mut proj_str = Vec::new();
                    for (i, item) in select.projection.iter().enumerate() {
                        if i < 3 {
                            // Limit to first few items for brevity
                            match item {
                                SelectItem::UnnamedExpr(expr) => proj_str.push(expr.to_string()),
                                SelectItem::ExprWithAlias { expr, alias } => {
                                    proj_str.push(format!("{} AS {}", expr, alias.value));
                                }
                                SelectItem::Wildcard(_) => proj_str.push("*".to_string()),
                                SelectItem::QualifiedWildcard(_, _) => {
                                    proj_str.push("table.*".to_string())
                                }
                            }
                        }
                    }

                    if select.projection.len() > 3 {
                        proj_str.push("...".to_string());
                    }

                    operation.push_str(&proj_str.join(", "));
                }

                // Add FROM info if present
                if !select.from.is_empty() {
                    operation.push_str(" FROM ");
                    let table_names: Vec<String> = select
                        .from
                        .iter()
                        .map(|t| match &t.relation {
                            TableFactor::Table { name, .. } => name.to_string(),
                            _ => "...".to_string(),
                        })
                        .collect();
                    operation.push_str(&table_names.join(", "));
                }

                // Indicate if WHERE clause is present
                if select.selection.is_some() {
                    operation.push_str(" WHERE ...");
                }

                // Indicate if GROUP BY clause is present
                match &select.group_by {
                    GroupByExpr::All(_) => operation.push_str(" GROUP BY ALL"),
                    GroupByExpr::Expressions(exprs, _) if !exprs.is_empty() => {
                        operation.push_str(" GROUP BY ...");
                    }
                    _ => {}
                }

                // Indicate if HAVING clause is present
                if select.having.is_some() {
                    operation.push_str(" HAVING ...");
                }
            }
            SetExpr::Values(_) => operation.push_str("VALUES (...)"),
            SetExpr::Query(_) => operation.push_str("(...)"),
            SetExpr::SetOperation { op, .. } => operation.push_str(&format!("{:?} ...", op)),
            SetExpr::Insert(_) => operation.push_str("INSERT ..."),
            SetExpr::Update(_) => operation.push_str("UPDATE ..."),
            SetExpr::Delete(_) => operation.push_str("DELETE ..."),
            SetExpr::Table(_) => operation.push_str("TABLE ..."),
        };

        // Add WITH options if specified
        if !with_options.is_empty() {
            operation.push_str(" WITH (");
            for (i, option) in with_options.iter().enumerate() {
                if i > 0 {
                    operation.push_str(", ");
                }

                // Handle each SqlOption variant appropriately
                match option {
                    SqlOption::Clustered(clustered) => match clustered {
                        TableOptionsClustered::ColumnstoreIndex => {
                            operation.push_str("COLUMNSTORE_INDEX");
                        }
                        TableOptionsClustered::ColumnstoreIndexOrder(columns) => {
                            operation.push_str("COLUMNSTORE_INDEX_ORDER (");
                            for (j, col) in columns.iter().enumerate() {
                                if j > 0 {
                                    operation.push_str(", ");
                                }
                                operation.push_str(&col.value);
                            }
                            operation.push_str(")");
                        }
                        TableOptionsClustered::Index(indexes) => {
                            operation.push_str("INDEX (");
                            for (j, index) in indexes.iter().enumerate() {
                                if j > 0 {
                                    operation.push_str(", ");
                                }
                                operation.push_str(&index.name.value);
                                if let Some(asc) = index.asc {
                                    operation.push_str(if asc { " ASC" } else { " DESC" });
                                }
                            }
                            operation.push_str(")");
                        }
                    },
                    SqlOption::Ident(ident) => {
                        operation.push_str(&ident.value);
                    }
                    SqlOption::KeyValue { key, value } => {
                        operation.push_str(&format!("{} = {}", key.value, value));
                    }
                    SqlOption::Partition {
                        column_name,
                        range_direction,
                        for_values,
                    } => {
                        operation.push_str(&format!("PARTITION ({} ", column_name.value));

                        if let Some(direction) = range_direction {
                            operation.push_str(&format!("RANGE {:?} ", direction));
                        }

                        operation.push_str("FOR VALUES (");
                        for (j, value) in for_values.iter().enumerate() {
                            if j > 0 {
                                operation.push_str(", ");
                            }
                            operation.push_str(&value.to_string());
                        }
                        operation.push_str("))");
                    }
                }
            }
            operation.push_str(")");
        }

        // Create the logical plan
        Ok(LogicalPlan::alter_view(view_name, operation))
    }

    pub fn build_delete_plan(&self, stmt: &Statement) -> Result<Box<LogicalPlan>, String> {
        if let Statement::Delete(delete) = stmt {
            // Get table info
            let table_name = match &delete.from {
                FromTable::WithFromKeyword(tables) => {
                    if tables.is_empty() {
                        return Err("No table specified in DELETE statement".to_string());
                    }
                    if tables.len() > 1 {
                        return Err("DELETE from multiple tables not supported".to_string());
                    }

                    let table = &tables[0];
                    if !table.joins.is_empty() {
                        return Err("DELETE with JOIN not supported".to_string());
                    }

                    match &table.relation {
                        TableFactor::Table { name, .. } => name.to_string(),
                        _ => return Err("Only simple table deletes supported".to_string()),
                    }
                }
                FromTable::WithoutKeyword(tables) => {
                    if tables.is_empty() {
                        return Err("No table specified in DELETE statement".to_string());
                    }
                    if tables.len() > 1 {
                        return Err("DELETE from multiple tables not supported".to_string());
                    }

                    let table = &tables[0];
                    if !table.joins.is_empty() {
                        return Err("DELETE with JOIN not supported".to_string());
                    }

                    match &table.relation {
                        TableFactor::Table { name, .. } => name.to_string(),
                        _ => return Err("Only simple table deletes supported".to_string()),
                    }
                }
            };

            // Get schema from catalog
            let binding = self.expression_parser.catalog();
            let catalog_guard = binding.read();
            let table_info = catalog_guard
                .get_table(&table_name)
                .ok_or_else(|| format!("Table '{}' not found", table_name))?;

            let schema = table_info.get_table_schema();
            let table_oid = table_info.get_table_oidt();

            // Create base table scan
            let mut current_plan =
                LogicalPlan::table_scan(table_name.clone(), schema.clone(), table_oid);

            // Add filter if WHERE clause exists
            if let Some(where_clause) = &delete.selection {
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

            // Create delete plan node
            Ok(LogicalPlan::delete(
                table_name,
                schema,
                table_oid,
                current_plan,
            ))
        } else {
            Err("Expected Delete statement".to_string())
        }
    }

    // ---------- DATABASE INFORMATION ----------

    pub fn build_show_tables_plan(
        &self,
        terse: &bool,
        history: &bool,
        extended: &bool,
        full: &bool,
        external: &bool,
        show_options: &ShowStatementOptions,
    ) -> Result<Box<LogicalPlan>, String> {
        // Try to extract schema name from the options if available
        let schema_name = match &show_options.show_in {
            Some(in_clause) => {
                // Extract from IN clause if specified
                Some(in_clause.to_string())
            }
            None => None,
        };

        // Log options that are currently not fully implemented
        if *history {
            debug!("SHOW TABLES HISTORY option is not fully implemented");
        }

        if *extended {
            debug!("SHOW TABLES EXTENDED option is not fully implemented");
        }

        if *full {
            debug!("SHOW TABLES FULL option is not fully implemented");
        }

        if *external {
            debug!("SHOW TABLES EXTERNAL option is not fully implemented");
        }

        if *terse {
            debug!("SHOW TABLES TERSE option is not fully implemented");
        }

        // Create the logical plan with all options
        Ok(LogicalPlan::show_tables_with_options(
            schema_name,
            *terse,
            *history,
            *extended,
            *full,
            *external,
        ))
    }

    pub fn build_show_databases_plan(
        &self,
        terse: &bool,
        history: &bool,
        show_options: &ShowStatementOptions,
    ) -> Result<Box<LogicalPlan>, String> {
        // Create the logical plan with the appropriate options
        let plan = LogicalPlan::show_databases_with_options(*terse, *history);

        // Log any additional options that we're not using yet
        if let Some(starts_with) = &show_options.starts_with {
            debug!("STARTS WITH clause not fully implemented for SHOW DATABASES");
        }

        // Handle filter position (LIKE patterns and WHERE clauses)
        if let Some(filter_position) = &show_options.filter_position {
            match filter_position {
                ShowStatementFilterPosition::Infix(filter)
                | ShowStatementFilterPosition::Suffix(filter) => match filter {
                    ShowStatementFilter::Like(pattern) => {
                        debug!(
                            "LIKE pattern '{}' not fully implemented for SHOW DATABASES",
                            pattern
                        );
                    }
                    ShowStatementFilter::ILike(pattern) => {
                        debug!(
                            "ILIKE pattern '{}' not fully implemented for SHOW DATABASES",
                            pattern
                        );
                    }
                    ShowStatementFilter::Where(expr) => {
                        debug!("WHERE clause not fully implemented for SHOW DATABASES");
                    }
                    ShowStatementFilter::NoKeyword(value) => {
                        debug!(
                            "NoKeyword filter '{}' not fully implemented for SHOW DATABASES",
                            value
                        );
                    }
                },
            }
        }

        // Handle limit clause
        if let Some(limit_expr) = &show_options.limit {
            debug!("LIMIT option not fully implemented for SHOW DATABASES");
        }

        // Handle limit from clause
        if let Some(limit_from) = &show_options.limit_from {
            debug!("LIMIT FROM option not fully implemented for SHOW DATABASES");
        }

        Ok(plan)
    }

    pub fn build_show_columns_plan(
        &self,
        extended: &bool,
        full: &bool,
        show_options: &ShowStatementOptions,
    ) -> Result<Box<LogicalPlan>, String> {
        // Extract table name and schema name from the options
        let (table_name, schema_name) = match &show_options.show_in {
            Some(in_clause) => {
                // Extract table name from the show_in clause
                let parent_name = match &in_clause.parent_name {
                    Some(obj_name) => {
                        if obj_name.0.is_empty() {
                            return Err("Table name cannot be empty".to_string());
                        }
                        // Get the table name from the object name
                        let table_name = match &obj_name.0[0] {
                            ObjectNamePart::Identifier(ident) => ident.value.clone(),
                        };
                        table_name
                    }
                    None => return Err("Table name is required for SHOW COLUMNS".to_string()),
                };

                // Extract schema name if present
                let schema_name = match &in_clause.parent_type {
                    Some(
                        ShowStatementInParentType::Database | ShowStatementInParentType::Schema,
                    ) => {
                        // In this case, the parent_name is the schema and we need to get the table from elsewhere
                        // This would typically come from the filter_position
                        if let Some(filter_pos) = &show_options.filter_position {
                            match filter_pos {
                                ShowStatementFilterPosition::Infix(filter)
                                | ShowStatementFilterPosition::Suffix(filter) => {
                                    match filter {
                                        ShowStatementFilter::NoKeyword(table) => {
                                            // Parent name is the schema, this is the table
                                            return Ok(LogicalPlan::show_columns_with_options(
                                                table.clone(),
                                                Some(parent_name),
                                                *extended,
                                                *full,
                                            ));
                                        }
                                        _ => {
                                            return Err("Table name is required for SHOW COLUMNS"
                                                .to_string());
                                        }
                                    }
                                }
                            }
                        }
                        // If we don't have a filter with the table name, we can't proceed
                        return Err("Table name is required for SHOW COLUMNS".to_string());
                    }
                    Some(ShowStatementInParentType::Table) => {
                        // In this case, parent_name is the table and we might have the schema elsewhere
                        None
                    }
                    _ => None,
                };

                (parent_name, schema_name)
            }
            None => {
                // If there's no show_in clause, we need to extract the table name
                // from the filter position if available
                if let Some(filter_pos) = &show_options.filter_position {
                    match filter_pos {
                        ShowStatementFilterPosition::Infix(filter)
                        | ShowStatementFilterPosition::Suffix(filter) => {
                            match filter {
                                ShowStatementFilter::NoKeyword(table) => {
                                    // No schema provided, just the table
                                    return Ok(LogicalPlan::show_columns_with_options(
                                        table.clone(),
                                        None,
                                        *extended,
                                        *full,
                                    ));
                                }
                                _ => {
                                    return Err(
                                        "Table name is required for SHOW COLUMNS".to_string()
                                    );
                                }
                            }
                        }
                    }
                }
                return Err("Table name is required for SHOW COLUMNS".to_string());
            }
        };

        // Create the logical plan with the appropriate options
        let plan =
            LogicalPlan::show_columns_with_options(table_name, schema_name, *extended, *full);

        // Handle filter position (LIKE patterns and WHERE clauses)
        if let Some(filter_position) = &show_options.filter_position {
            match filter_position {
                ShowStatementFilterPosition::Infix(filter)
                | ShowStatementFilterPosition::Suffix(filter) => {
                    match filter {
                        ShowStatementFilter::Like(pattern) => {
                            debug!(
                                "LIKE pattern '{}' not fully implemented for SHOW COLUMNS",
                                pattern
                            );
                        }
                        ShowStatementFilter::ILike(pattern) => {
                            debug!(
                                "ILIKE pattern '{}' not fully implemented for SHOW COLUMNS",
                                pattern
                            );
                        }
                        ShowStatementFilter::Where(expr) => {
                            debug!("WHERE clause not fully implemented for SHOW COLUMNS");
                        }
                        ShowStatementFilter::NoKeyword(_) => {
                            // Already handled above
                        }
                    }
                }
            }
        }

        // Handle other options
        if let Some(starts_with) = &show_options.starts_with {
            debug!("STARTS WITH clause not fully implemented for SHOW COLUMNS");
        }

        if let Some(limit_expr) = &show_options.limit {
            debug!("LIMIT option not fully implemented for SHOW COLUMNS");
        }

        if let Some(limit_from) = &show_options.limit_from {
            debug!("LIMIT FROM option not fully implemented for SHOW COLUMNS");
        }

        Ok(plan)
    }

    pub fn build_use_plan(&self, stmt: &Use) -> Result<Box<LogicalPlan>, String> {
        // Extract the database name from the Use statement based on its variant
        let db_name = match stmt {
            Use::Database(obj_name) | Use::Schema(obj_name) | Use::Object(obj_name) => {
                if obj_name.0.is_empty() {
                    return Err("Empty database name in USE statement".to_string());
                }

                match &obj_name.0[0] {
                    ObjectNamePart::Identifier(ident) => ident.value.clone(),
                }
            }
            Use::Catalog(obj_name) => {
                if obj_name.0.is_empty() {
                    return Err("Empty catalog name in USE statement".to_string());
                }

                match &obj_name.0[0] {
                    ObjectNamePart::Identifier(ident) => ident.value.clone(),
                }
            }
            Use::Warehouse(obj_name) => {
                return Err("USE WAREHOUSE statement is not supported".to_string());
            }
            Use::Role(obj_name) => {
                return Err("USE ROLE statement is not supported".to_string());
            }
            Use::SecondaryRoles(_) => {
                return Err("USE SECONDARY ROLES statement is not supported".to_string());
            }
            Use::Default => {
                return Err("USE DEFAULT statement is not supported".to_string());
            }
        };

        // Create and return the USE logical plan
        Ok(LogicalPlan::use_db(db_name))
    }

    // Add a helper method that directly creates the explain logical plan
    fn create_explain_plan(&self, inner_plan: &LogicalPlan) -> Box<LogicalPlan> {
        let schema = Schema::new(vec![]);
        // Create the Explain plan node using the static constructor
        Box::new(LogicalPlan::new(
            LogicalPlanType::Explain {
                plan: Box::new(inner_plan.clone()),
            },
            vec![], // No children
        ))
    }

    pub fn build_explain_plan(&self, explain: &Statement) -> Result<Box<LogicalPlan>, String> {
        if let Statement::Explain { statement, .. } = explain {
            let inner_plan = match statement.as_ref() {
                Statement::Query(query) => self.build_query_plan(query)?,
                Statement::Insert { .. } => {
                    return Err("EXPLAIN INSERT not yet supported".to_string());
                }
                _ => return Err(format!("EXPLAIN for this statement type not yet supported")),
            };

            // Use the helper method to create the explain plan
            Ok(self.create_explain_plan(&inner_plan))
        } else {
            Err("Expected EXPLAIN statement".to_string())
        }
    }

    pub fn build_explain_table_plan(&self, stmt: &Statement) -> Result<Box<LogicalPlan>, String> {
        // TODO: Implement explain table plan
        Err("ExplainTable not yet implemented".to_string())
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
                    column_mappings: _,
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
                    column_mappings: _,
                } => {
                    assert_eq!(schema.get_column_count(), 3);
                    assert_eq!(expressions.len(), 3);
                }
                _ => panic!("Expected Projection as root node"),
            }

            // Then verify the filter node
            match &plan.children[0].plan_type {
                LogicalPlanType::Filter {
                    schema,
                    output_schema,
                    predicate,
                    ..
                } => {
                    assert_eq!(output_schema.get_column_count(), 3);
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
        use crate::sql::execution::expressions::comparison_expression::ComparisonType;
        use crate::sql::planner::logical_plan::LogicalPlanType;
        use crate::types_db::type_id::TypeId;

        // Helper function to set up a test table
        fn setup_test_table(fixture: &mut TestContext) {
            // Create the table with the correct schema
            fixture
                .create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false)
                .unwrap();

            // Verify the table exists and has the correct schema
            fixture.assert_table_exists("users");
            fixture.assert_table_schema(
                "users",
                &[
                    ("id".to_string(), TypeId::Integer),
                    ("name".to_string(), TypeId::VarChar),
                    ("age".to_string(), TypeId::Integer),
                ],
            );
        }

        #[test]
        fn test_plan_aggregate_column_names() {
            let mut fixture = TestContext::new("test_plan_aggregate_column_names");
            setup_test_table(&mut fixture);

            let test_cases = vec![
                (
                    "SELECT name, SUM(age) FROM users GROUP BY name",
                    vec!["name", "SUM_age"],
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
                    vec!["name", "AVG_age"],
                ),
            ];

            for (sql, expected_columns) in test_cases {
                let plan = fixture
                    .planner
                    .create_logical_plan(sql)
                    .unwrap_or_else(|e| panic!("Failed to create plan for query '{}': {}", sql, e));

                // Verify the plan structure
                match &plan.plan_type {
                    LogicalPlanType::Projection { schema, .. } => {
                        // Verify column names match expected
                        let actual_columns: Vec<_> = (0..schema.get_column_count())
                            .map(|i| {
                                schema
                                    .get_column(i as usize)
                                    .unwrap()
                                    .get_name()
                                    .to_string()
                            })
                            .collect();
                        assert_eq!(
                            actual_columns, expected_columns,
                            "Column names don't match for query: {}",
                            sql
                        );
                    }
                    _ => panic!("Expected Projection as root node for query: {}", sql),
                }
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
                    vec![TypeId::BigInt],
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
                let plan = fixture
                    .planner
                    .create_logical_plan(sql)
                    .unwrap_or_else(|e| panic!("Failed to create plan for query '{}': {}", sql, e));

                // First verify we have a projection node
                match &plan.plan_type {
                    LogicalPlanType::Projection { expressions, .. } => {
                        assert_eq!(expressions.len(), expected_types.len());
                    }
                    _ => panic!("Expected Projection as root node for query: {}", sql),
                }

                // Then verify the aggregate node
                match &plan.children[0].plan_type {
                    LogicalPlanType::Aggregate { aggregates, .. } => {
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
                                    agg.get_agg_type(),
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
            }
        }

        #[test]
        fn test_group_by_with_aggregates() {
            let mut fixture = TestContext::new("group_by_aggregates");
            setup_test_table(&mut fixture);

            // Verify the table was created correctly
            fixture.assert_table_exists("users");
            fixture.assert_table_schema(
                "users",
                &[
                    ("id".to_string(), TypeId::Integer),
                    ("name".to_string(), TypeId::VarChar),
                    ("age".to_string(), TypeId::Integer),
                ],
            );

            let group_sql = "SELECT name, SUM(age) as total_age, COUNT(*) as num_users \
                            FROM users \
                            GROUP BY name \
                            HAVING SUM(age) > 25";

            let plan = fixture.planner.create_logical_plan(group_sql).unwrap();

            // Verify projection
            match &plan.plan_type {
                LogicalPlanType::Projection {
                    expressions,
                    schema,
                    column_mappings: _,
                } => {
                    assert_eq!(schema.get_column_count(), 3);
                    assert_eq!(expressions.len(), 3);
                }
                _ => panic!("Expected Projection as root node"),
            }

            // Verify filter (HAVING)
            match &plan.children[0].plan_type {
                LogicalPlanType::Filter { predicate, .. } => {
                    // Verify the predicate is comparing SUM(age) > 25
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
                    schema,
                } => {
                    assert_eq!(group_by.len(), 1); // name
                    assert_eq!(aggregates.len(), 2); // SUM(age), COUNT(*)
                    assert_eq!(schema.get_column_count(), 3);
                }
                _ => panic!("Expected Aggregate node"),
            }
        }
    }

    mod join_tests {
        use super::*;
        use crate::sql::planner::logical_plan::LogicalPlanType;

        fn setup_test_tables(fixture: &mut TestContext) {
            fixture
                .create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false)
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

            let plan = fixture
                .planner
                .create_logical_plan(join_sql)
                .expect("Failed to create plan");

            // First verify the join node and its schemas
            let join_node = match &plan.children[0].plan_type {
                LogicalPlanType::NestedLoopJoin {
                    left_schema,
                    right_schema,
                    predicate,
                    join_type,
                } => {
                    // Verify left schema (users)
                    assert_eq!(
                        left_schema.get_column_count(),
                        3,
                        "Wrong number of columns in left schema"
                    );
                    let left_cols: Vec<_> = (0..left_schema.get_column_count())
                        .map(|i| left_schema.get_column(i as usize).unwrap().get_name())
                        .collect();
                    assert!(
                        left_cols.contains(&"u.id"),
                        "Left schema missing 'u.id' column"
                    );
                    assert!(
                        left_cols.contains(&"u.name"),
                        "Left schema missing 'u.name' column"
                    );
                    assert!(
                        left_cols.contains(&"u.age"),
                        "Left schema missing 'u.age' column"
                    );

                    // Verify right schema (orders)
                    assert_eq!(
                        right_schema.get_column_count(),
                        3,
                        "Wrong number of columns in right schema"
                    );
                    let right_cols: Vec<_> = (0..right_schema.get_column_count())
                        .map(|i| right_schema.get_column(i as usize).unwrap().get_name())
                        .collect();
                    assert!(
                        right_cols.contains(&"o.order_id"),
                        "Right schema missing 'o.order_id' column"
                    );
                    assert!(
                        right_cols.contains(&"o.user_id"),
                        "Right schema missing 'o.user_id' column"
                    );
                    assert!(
                        right_cols.contains(&"o.amount"),
                        "Right schema missing 'o.amount' column"
                    );

                    // Verify join type
                    assert!(
                        matches!(join_type, JoinOperator::Inner(_)),
                        "Expected INNER join"
                    );

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
                LogicalPlanType::Projection {
                    expressions,
                    schema,
                    column_mappings: _,
                } => {
                    assert_eq!(
                        schema.get_column_count(),
                        2,
                        "Expected 2 columns in projection"
                    );
                    assert_eq!(expressions.len(), 2, "Expected 2 expressions in projection");

                    // Verify column names in output schema
                    let col_names: Vec<_> = (0..schema.get_column_count())
                        .map(|i| schema.get_column(i as usize).unwrap().get_name())
                        .collect();
                    assert!(
                        col_names.contains(&"u.name"),
                        "Output missing 'u.name' column"
                    );
                    assert!(
                        col_names.contains(&"o.amount"),
                        "Output missing 'o.amount' column"
                    );
                }
                _ => panic!("Expected Projection as root node"),
            }
        }
    }

    mod group_by_tests {
        use super::*;
        use crate::sql::execution::expressions::comparison_expression::ComparisonType;
        use crate::sql::planner::logical_plan::LogicalPlanType;
        use crate::types_db::type_id::TypeId;

        fn setup_test_table(fixture: &mut TestContext) {
            fixture
                .create_table("test_sales", "region VARCHAR(255), amount DECIMAL", false)
                .unwrap();
        }

        #[test]
        fn test_group_by_with_aggregates() {
            let mut fixture = TestContext::new("group_by_aggregates");
            setup_test_table(&mut fixture);

            // Verify the table was created correctly
            fixture.assert_table_exists("test_sales");
            fixture.assert_table_schema(
                "test_sales",
                &[
                    ("region".to_string(), TypeId::VarChar),
                    ("amount".to_string(), TypeId::Decimal),
                ],
            );

            let group_sql = "SELECT region, SUM(amount) as total_sales \
                            FROM test_sales \
                            GROUP BY region \
                            HAVING SUM(amount) > 1000";

            let plan = fixture.planner.create_logical_plan(group_sql).unwrap();

            // Verify projection
            match &plan.plan_type {
                LogicalPlanType::Projection {
                    expressions,
                    schema,
                    column_mappings: _,
                } => {
                    assert_eq!(schema.get_column_count(), 2);
                    assert_eq!(expressions.len(), 2);
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
                    schema,
                } => {
                    assert_eq!(group_by.len(), 1); // region
                    assert_eq!(aggregates.len(), 1); // SUM(amount)
                    assert_eq!(schema.get_column_count(), 2);
                }
                _ => panic!("Expected Aggregate node"),
            }
        }

        #[test]
        fn test_simple_group_by() {
            let mut fixture = TestContext::new("simple_group_by");
            setup_test_table(&mut fixture);

            let sql = "SELECT region, COUNT(*) as count \
                      FROM test_sales \
                      GROUP BY region";

            let plan = fixture.planner.create_logical_plan(sql).unwrap();

            match &plan.children[0].plan_type {
                LogicalPlanType::Aggregate {
                    group_by,
                    aggregates,
                    schema,
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
                    schema,
                } => {
                    assert_eq!(sort_expressions.len(), 1);
                    assert_eq!(schema.get_column_count(), 2);
                }
                _ => panic!("Expected Sort node"),
            }

            // Verify projection
            match &plan.children[0].children[0].plan_type {
                LogicalPlanType::Projection {
                    expressions,
                    schema,
                    column_mappings: _,
                } => {
                    assert_eq!(expressions.len(), 2);
                    assert_eq!(schema.get_column_count(), 2);
                }
                _ => panic!("Expected Projection node"),
            }
        }
    }

    mod subquery_tests {
        use super::*;
        use crate::sql::execution::expressions::comparison_expression::ComparisonType;
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
                        LogicalPlanType::Filter { predicate, .. } => match predicate.as_ref() {
                            Expression::Comparison(comp) => {
                                assert_eq!(comp.get_comp_type(), ComparisonType::GreaterThan);
                            }
                            _ => panic!("Expected Comparison expression"),
                        },
                        _ => panic!("Expected Filter node"),
                    }
                }
                _ => panic!("Expected Projection as root node"),
            }
        }
    }

    mod transaction_tests {
        use super::*;
        use sqlparser::ast::{TransactionAccessMode, TransactionIsolationLevel, TransactionMode};

        #[test]
        fn test_build_start_transaction_plan_with_access_mode() {
            let mut context = TestContext::new("test_access_mode");
            let builder = LogicalPlanBuilder::new(context.catalog.clone());
            let modes = vec![TransactionMode::AccessMode(TransactionAccessMode::ReadOnly)];
            let plan = builder.build_start_transaction_plan(
                &modes,
                &true,
                &None,
                &None,
                &vec![],
                &None,
                &false,
            );
            assert!(plan.is_ok());
            let plan = plan.unwrap();
            match plan.plan_type {
                LogicalPlanType::StartTransaction { read_only, .. } => {
                    assert!(read_only);
                }
                _ => panic!("Expected StartTransaction plan"),
            }
        }

        #[test]
        fn test_build_start_transaction_plan_with_isolation_level() {
            let mut context = TestContext::new("test_isolation_level");
            let builder = LogicalPlanBuilder::new(context.catalog.clone());
            let modes = vec![TransactionMode::IsolationLevel(
                TransactionIsolationLevel::Serializable,
            )];
            let plan = builder.build_start_transaction_plan(
                &modes,
                &true,
                &None,
                &None,
                &vec![],
                &None,
                &false,
            );
            assert!(plan.is_ok());
            let plan = plan.unwrap();
            match plan.plan_type {
                LogicalPlanType::StartTransaction {
                    isolation_level, ..
                } => {
                    assert_eq!(isolation_level, Some(IsolationLevel::Serializable));
                }
                _ => panic!("Expected StartTransaction plan"),
            }
        }
    }

    // Add this at the end of the tests module
    mod savepoint_tests {
        use super::*;
        use sqlparser::ast::Ident;

        #[test]
        fn test_build_savepoint_plan() {
            let mut fixture = TestContext::new("savepoint_test");

            // Create a logical plan builder directly
            let plan_builder = LogicalPlanBuilder::new(fixture.catalog.clone());

            let savepoint_name = Ident::new("my_savepoint");
            let plan = plan_builder.build_savepoint_plan(&savepoint_name).unwrap();

            match &plan.plan_type {
                LogicalPlanType::Savepoint { name } => {
                    assert_eq!(name, "my_savepoint");
                }
                _ => panic!("Expected Savepoint plan"),
            }
        }

        #[test]
        fn test_build_release_savepoint_plan() {
            let mut fixture = TestContext::new("release_savepoint_test");

            // Create a logical plan builder directly
            let plan_builder = LogicalPlanBuilder::new(fixture.catalog.clone());

            let savepoint_name = Ident::new("my_savepoint");
            let plan = plan_builder
                .build_release_savepoint_plan(&savepoint_name)
                .unwrap();

            match &plan.plan_type {
                LogicalPlanType::ReleaseSavepoint { name } => {
                    assert_eq!(name, "my_savepoint");
                }
                _ => panic!("Expected ReleaseSavepoint plan"),
            }
        }
    }

    // Tests for build_drop_plan
    mod drop_tests {
        use super::*;
        use crate::sql::planner::logical_plan::LogicalPlanType;

        #[test]
        fn test_build_drop_plan() {
            let mut fixture = TestContext::new("drop_plan_test");

            // Create a logical plan builder directly
            let plan_builder = LogicalPlanBuilder::new(fixture.catalog.clone());

            // Test parameters
            let object_type = "TABLE".to_string();
            let if_exists = true;
            let names = vec!["users".to_string(), "orders".to_string()];
            let cascade = true;

            // Call the method being tested
            let plan = plan_builder
                .build_drop_plan(object_type.clone(), if_exists, names.clone(), cascade)
                .unwrap();

            // Verify the plan has the correct type and fields
            match &plan.plan_type {
                LogicalPlanType::Drop {
                    object_type: ot,
                    if_exists: ie,
                    names: n,
                    cascade: c,
                } => {
                    assert_eq!(object_type, *ot, "object_type doesn't match");
                    assert_eq!(if_exists, *ie, "if_exists doesn't match");
                    assert_eq!(names, *n, "names don't match");
                    assert_eq!(cascade, *c, "cascade doesn't match");
                }
                _ => panic!("Expected Drop plan, got {:?}", plan.plan_type),
            }
        }
    }
}
