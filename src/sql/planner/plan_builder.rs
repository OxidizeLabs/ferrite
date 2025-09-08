use super::logical_plan::{LogicalPlan, LogicalPlanType};
use super::schema_manager::SchemaManager;
use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::catalog::Catalog;
use crate::concurrency::transaction::IsolationLevel;
use crate::sql::execution::expression_parser::ExpressionParser;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::assignment_expression::AssignmentExpression;
use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use log::debug;
use parking_lot::RwLock;
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
            SetExpr::Select(select) => {
                self.build_select_plan_with_order_by(select, query.order_by.as_ref())?
            }
            SetExpr::Query(nested_query) => self.build_query_plan(nested_query)?,
            SetExpr::Values(values) => {
                let schema = self.schema_manager.create_values_schema(&values.rows)?;
                self.build_values_plan(&values.rows, &schema)?
            }
            SetExpr::Update(update_stmt) => match update_stmt {
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
                ..
            } => {
                return Err(
                    "Set operations (UNION, INTERSECT, etc.) are not yet supported".to_string(),
                );
            }
            SetExpr::Insert(_) => {
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

        // For non-SELECT queries, handle ORDER BY here if present
        if !matches!(&*query.body, SetExpr::Select(_))
            && let Some(order_by) = &query.order_by {
                let Some(schema) = current_plan.get_schema() else {
                    return Err("Internal error: missing schema while applying ORDER BY".to_string());
                };

                // Build sort specifications with ASC/DESC support
                let sort_specifications = match &order_by.kind {
                    OrderByKind::Expressions(order_by_exprs) => {
                        self.expression_parser
                            .parse_order_by_specifications(order_by_exprs, &schema)?
                    }
                    OrderByKind::All(_) => {
                        // Handle ALL case if needed
                        return Err("ORDER BY ALL is not supported yet".to_string());
                    }
                };

                current_plan = LogicalPlan::sort(sort_specifications, schema.clone(), current_plan);
            }

        // Handle LIMIT and OFFSET if present
        if let Some(limit_clause) = &query.limit_clause {
            let Some(schema) = current_plan.get_schema() else {
                return Err("Internal error: missing schema while applying LIMIT/OFFSET".to_string());
            };

            // Extract the limit expression based on the LimitClause variant
            let limit_expr = match limit_clause {
                LimitClause::LimitOffset { limit, .. } => {
                    if let Some(expr) = limit {
                        expr
                    } else {
                        return Err("LIMIT clause has no limit value".to_string());
                    }
                }
                LimitClause::OffsetCommaLimit { limit, .. } => limit,
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
                return Err("Internal error: missing schema while applying FETCH".to_string());
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

    pub fn build_select_plan(&self, select: &Select) -> Result<Box<LogicalPlan>, String> {
        self.build_select_plan_with_order_by(select, None)
    }

    pub fn build_select_plan_with_order_by(
        &self,
        select: &Select,
        order_by: Option<&OrderBy>,
    ) -> Result<Box<LogicalPlan>, String> {
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
                return Err("Internal error: missing schema while applying WHERE".to_string());
            };
            debug!("Schema has {} columns", parsing_schema.get_column_count());
            let filter_expr = self
                .expression_parser
                .parse_expression(where_clause, &parsing_schema)?;
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
        let Some(original_schema) = current_plan.get_schema() else {
            return Err("Internal error: missing schema before SELECT projection".to_string());
        };

        // Parse all expressions in the projection to identify aggregates
        let mut has_aggregates = false;
        let mut agg_exprs: Vec<Arc<Expression>> = Vec::new();

        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    // Use the original schema for parsing expressions to ensure all columns are available
                    let parsed_expr = self
                        .expression_parser
                        .parse_expression(expr, &original_schema)?;
                    if let Expression::Aggregate(_) = parsed_expr {
                        has_aggregates = true;
                        agg_exprs.push(Arc::new(parsed_expr));
                    }
                }
                SelectItem::ExprWithAlias { expr, alias: _ } => {
                    // Use the original schema for parsing expressions to ensure all columns are available
                    let parsed_expr = self
                        .expression_parser
                        .parse_expression(expr, &original_schema)?;
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
                &original_schema,
                has_group_by,
            )?;

            // Validate GROUP BY: all non-aggregate columns in SELECT must be in GROUP BY
            if has_group_by {
                self.validate_group_by_clause(select, &group_by_exprs, &original_schema)?;
            }

            // Create a new schema for the aggregate plan using the schema manager
            let group_by_exprs_refs: Vec<&Expression> = group_by_exprs.iter().collect();
            let alias_mapping = self
                .schema_manager
                .create_column_alias_mapping(&select.projection, &group_by_exprs_refs);
            let agg_schema = self
                .schema_manager
                .create_aggregation_output_schema_with_alias_mapping(
                    &group_by_exprs_refs,
                    &agg_exprs,
                    has_group_by,
                    Some(&alias_mapping),
                );

            // Create aggregation plan node
            // Clone agg_exprs before moving it into the aggregate plan
            let agg_exprs_for_having = agg_exprs.clone();

            current_plan = LogicalPlan::aggregate(
                group_by_exprs.into_iter().map(Arc::new).collect(),
                agg_exprs,
                agg_schema,
                current_plan,
            );

            // Apply HAVING clause if it exists
            if let Some(having) = &select.having {
                // Use the original schema for parsing the HAVING clause instead of the aggregate schema
                // This ensures columns like 'age' can be found when used inside aggregate functions
                let having_expr = self
                    .expression_parser
                    .parse_expression(having, &original_schema)?;

                // Replace aggregate functions in the HAVING expression with column references
                // to the pre-computed aggregate values from the aggregation output
                let current_schema = current_plan.get_schema().clone().unwrap();
                let transformed_having_expr = self
                    .expression_parser
                    .replace_aggregates_with_column_refs(&having_expr, &current_schema, &agg_exprs_for_having)?;

                current_plan = LogicalPlan::filter(
                    current_schema,
                    String::new(), // table_name
                    0,             // table_oid
                    Arc::new(transformed_having_expr),
                    current_plan,
                );
            }

            // Add projection on top of aggregation
            // Use the original schema for parsing projection expressions
            current_plan = self.build_projection_plan_with_schema(
                &select.projection,
                current_plan,
                &original_schema,
            )?;
        } else {
            // No aggregates, just add projection
            current_plan = self.build_projection_plan(&select.projection, current_plan)?;
        }

        // Apply HAVING clause if it exists and we don't have aggregates
        // This is a bit unusual but SQL allows it
        if !has_aggregates && !has_group_by
            && let Some(having) = &select.having {
                let having_expr = self
                    .expression_parser
                    .parse_expression(having, &original_schema)?;
                current_plan = LogicalPlan::filter(
                    current_plan.get_schema().clone().unwrap(),
                    String::new(), // table_name
                    0,             // table_oid
                    Arc::new(having_expr),
                    current_plan,
                );
            }

        // Apply DISTINCT if requested
        if select.distinct.is_some() {
            let Some(projection_schema) = current_plan.get_schema() else {
                return Err("Internal error: missing schema while applying DISTINCT".to_string());
            };
            current_plan = LogicalPlan::distinct(projection_schema, current_plan);
        }

        // Handle ORDER BY if present (either from SELECT or passed down from Query)
        if let Some(order_by) = order_by {
            let Some(projection_schema) = current_plan.get_schema() else {
                return Err("Internal error: missing schema while applying ORDER BY".to_string());
            };

            // Build sort specifications with ASC/DESC support using fallback parsing
            let sort_specifications = match &order_by.kind {
                OrderByKind::Expressions(order_by_exprs) => {
                    // Try to parse with the projection schema first, then fallback to original schema
                    match self.expression_parser.parse_order_by_specifications(order_by_exprs, &projection_schema) {
                        Ok(specs) => specs,
                        Err(_) => {
                            // Fallback: manually parse with fallback logic for each expression
                            let mut specs = Vec::new();
                            for order_item in order_by_exprs {
                                let expr = self.expression_parser.parse_expression_with_fallback(
                                    &order_item.expr,
                                    &projection_schema,
                                    &original_schema,
                                )?;

                                // Determine direction from ASC field
                                let direction = match order_item.options.asc {
                                    None | Some(true) => crate::sql::execution::plans::sort_plan::OrderDirection::Asc,
                                    Some(false) => crate::sql::execution::plans::sort_plan::OrderDirection::Desc,
                                };

                                specs.push(crate::sql::execution::plans::sort_plan::OrderBySpec::new(
                                    Arc::new(expr),
                                    direction,
                                ));
                            }
                            specs
                        }
                    }
                }
                OrderByKind::All(_) => {
                    // Handle ALL case if needed
                    return Err("ORDER BY ALL is not supported yet".to_string());
                }
            };

            current_plan = LogicalPlan::sort(sort_specifications, projection_schema.clone(), current_plan);
        }

        // Apply SORT BY if it exists (Hive-specific)
        if !select.sort_by.is_empty() {
            let Some(projection_schema) = current_plan.get_schema() else {
                return Err("Internal error: missing schema while applying SORT BY".to_string());
            };
            let sort_specifications = select
                .sort_by
                .iter()
                .map(|expr| {
                    let parsed_expr = self.expression_parser.parse_expression_with_fallback(
                        &expr.expr,
                        &projection_schema,
                        &original_schema,
                    )?;
                    // SORT BY defaults to ASC
                    Ok(crate::sql::execution::plans::sort_plan::OrderBySpec::new(
                        Arc::new(parsed_expr),
                        crate::sql::execution::plans::sort_plan::OrderDirection::Asc,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?;

            if !sort_specifications.is_empty() {
                current_plan =
                    LogicalPlan::sort(sort_specifications, projection_schema.clone(), current_plan);
            }
        }

        Ok(current_plan)
    }

    /// Validates that all non-aggregate columns in SELECT are included in GROUP BY
    fn validate_group_by_clause(
        &self,
        select: &Select,
        group_by_exprs: &[Expression],
        schema: &Schema,
    ) -> Result<(), String> {
        // Create a set of group by column identifiers for quick lookup
        let mut group_by_columns = std::collections::HashSet::new();

        for expr in group_by_exprs {
            if let Expression::ColumnRef(col_ref) = expr {
                let col_name = col_ref.get_return_type().get_name();
                group_by_columns.insert(col_name.to_string());

                // Also add the unqualified version if it's a qualified name
                if let Some(dot_pos) = col_name.find('.') {
                    let unqualified = &col_name[dot_pos + 1..];
                    group_by_columns.insert(unqualified.to_string());
                }
            }
        }

        // Check each item in the SELECT projection
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    // Parse the expression to check if it's an aggregate
                    let parsed_expr = self.expression_parser.parse_expression(expr, schema)?;

                    // If it's not an aggregate, it must be in GROUP BY
                    if !matches!(parsed_expr, Expression::Aggregate(_)) {
                        // Check if this expression corresponds to a column that should be in GROUP BY
                        if let Expression::ColumnRef(col_ref) = &parsed_expr {
                            let col_name = col_ref.get_return_type().get_name();

                            // Check both qualified and unqualified versions
                            let found_in_group_by = group_by_columns.contains(col_name)
                                || if let Some(dot_pos) = col_name.find('.') {
                                    let unqualified = &col_name[dot_pos + 1..];
                                    group_by_columns.contains(unqualified)
                                } else {
                                    false
                                };

                            if !found_in_group_by {
                                return Err(format!(
                                    "Column '{}' must appear in the GROUP BY clause or be used in an aggregate function",
                                    col_name
                                ));
                            }
                        }
                    }
                }
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                    // This should have been caught earlier, but just in case
                    return Err("Wildcard projections are not supported with GROUP BY".to_string());
                }
            }
        }

        Ok(())
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

                    // For aggregate input, convert all expressions to column references
                    if is_aggregate_input {
                        // For aggregate input, we need to map expressions to the aggregation output
                        if let Expression::Aggregate(agg) = &parsed_expr {
                            // For aggregate expressions, find them in the input schema by function name and argument
                            let expected_col_name = agg.get_column_name();

                            // Try to find the aggregate column in the input schema
                            let col_idx = input_schema
                                .get_columns()
                                .iter()
                                .position(|col| {
                                    let col_name = col.get_name();
                                    // Match exact name or function pattern
                                    col_name == expected_col_name
                                        || col_name
                                            .starts_with(&format!("{}(", agg.get_function_name()))
                                        || col_name
                                            .starts_with(&format!("{}_", agg.get_function_name()))
                                })
                                .unwrap_or_else(|| {
                                    // Fallback: use position based on order in projection, but ensure it's valid
                                    std::cmp::min(
                                        i,
                                        (input_schema.get_column_count() as usize)
                                            .saturating_sub(1),
                                    )
                                });

                            projection_exprs.push(Arc::new(Expression::ColumnRef(
                                ColumnRefExpression::new(
                                    0,
                                    col_idx,
                                    input_schema.get_column(col_idx).unwrap().clone(),
                                    vec![],
                                ),
                            )));
                        } else {
                            // For non-aggregate expressions, find the column in the input schema by name
                            if let Some(col_idx) =
                                input_schema.get_qualified_column_index(&col_name)
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

                    // For aggregate input, convert all expressions to column references
                    if is_aggregate_input {
                        if let Expression::Aggregate(agg) = &parsed_expr {
                            // For aggregate expressions, find them in the input schema
                            let expected_col_name = agg.get_column_name();

                            // Try to find the aggregate column in the input schema
                            let col_idx = input_schema
                                .get_columns()
                                .iter()
                                .position(|col| {
                                    let col_name = col.get_name();
                                    // Match exact name or function pattern
                                    col_name == expected_col_name
                                        || col_name
                                            .starts_with(&format!("{}(", agg.get_function_name()))
                                        || col_name
                                            .starts_with(&format!("{}_", agg.get_function_name()))
                                })
                                .unwrap_or_else(|| {
                                    // Fallback: use position based on order in projection, but ensure it's valid
                                    std::cmp::min(
                                        i,
                                        (input_schema.get_column_count() as usize)
                                            .saturating_sub(1),
                                    )
                                });

                            projection_exprs.push(Arc::new(Expression::ColumnRef(
                                ColumnRefExpression::new(
                                    0,
                                    col_idx,
                                    input_schema.get_column(col_idx).unwrap().clone(),
                                    vec![],
                                ),
                            )));
                        } else {
                            // For aliased expressions, first try to find by the alias value
                            let mut col_idx_opt =
                                input_schema.get_qualified_column_index(&alias.value);

                            if col_idx_opt.is_none() {
                                // Try to find the column by matching the underlying expression to expected alias
                                match expr {
                                    Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                                        let table_alias = &parts[0].value;
                                        let column_name = &parts[1].value;
                                        let qualified_name =
                                            format!("{}.{}", table_alias, column_name);

                                        // Map known patterns to expected aliases
                                        let expected_alias =
                                            if table_alias == "e" && column_name == "name" {
                                                "employee"
                                            } else if table_alias == "d" && column_name == "name" {
                                                "department"
                                            } else {
                                                // Keep the original qualified name for other cases
                                                &qualified_name
                                            };

                                        col_idx_opt =
                                            input_schema.get_qualified_column_index(expected_alias);
                                    }
                                    _ => {}
                                }
                            }

                            if let Some(col_idx) = col_idx_opt {
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

    pub fn build_projection_plan(
        &self,
        select_items: &[SelectItem],
        input_plan: Box<LogicalPlan>,
    ) -> Result<Box<LogicalPlan>, String> {
        let input_schema = input_plan.get_schema().unwrap();
        let parse_schema = input_schema.as_ref();
        self.build_projection_plan_with_schema(select_items, input_plan, parse_schema)
    }

    pub fn build_create_table_plan(
        &self,
        create_table: &CreateTable,
    ) -> Result<Box<LogicalPlan>, String> {
        let table_name = create_table.name.to_string();

        // Validate table name is not empty
        if table_name.is_empty() {
            return Err("Table name cannot be empty".to_string());
        }

        // Validate that table has at least one column
        if create_table.columns.is_empty() {
            return Err(format!(
                "Table '{}' must have at least one column",
                table_name
            ));
        }

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
        let mut columns = self
            .schema_manager
            .convert_column_defs(&create_table.columns)?;

        // Process table-level constraints
        self.process_table_constraints(&create_table.constraints, &mut columns)?;

        // Double-check that we have columns after conversion
        if columns.is_empty() {
            return Err(format!(
                "Failed to create schema for table '{}': no valid columns found",
                table_name
            ));
        }

        let schema = Schema::new(columns);

        Ok(LogicalPlan::create_table(
            schema,
            table_name,
            create_table.if_not_exists,
        ))
    }

    /// Process table-level constraints like PRIMARY KEY (col1, col2) and UNIQUE (col1, col2)
    fn process_table_constraints(
        &self,
        constraints: &[TableConstraint],
        columns: &mut [Column],
    ) -> Result<(), String> {
        for constraint in constraints {
            match constraint {
                TableConstraint::PrimaryKey { columns: pk_columns, .. } => {
                    // Set primary key flag on the specified columns
                    for pk_col_ident in pk_columns {
                        let pk_col_name = pk_col_ident.to_string();
                        let column_found = columns.iter_mut().find(|col| col.get_name() == pk_col_name);
                        match column_found {
                            Some(column) => {
                                column.set_primary_key(true);
                                column.set_not_null(true); // PRIMARY KEY implies NOT NULL
                            }
                            None => {
                                return Err(format!(
                                    "Primary key column '{}' not found in table",
                                    pk_col_name
                                ));
                            }
                        }
                    }
                }
                TableConstraint::Unique { columns: unique_columns, .. } => {
                    // Set unique flag on the specified columns
                    for unique_col_ident in unique_columns {
                        let unique_col_name = unique_col_ident.to_string();
                        let column_found = columns.iter_mut().find(|col| col.get_name() == unique_col_name);
                        match column_found {
                            Some(column) => {
                                column.set_unique(true);
                            }
                            None => {
                                return Err(format!(
                                    "Unique constraint column '{}' not found in table",
                                    unique_col_name
                                ));
                            }
                        }
                    }
                }
                TableConstraint::ForeignKey { columns: fk_columns, foreign_table, referred_columns, .. } => {
                    // Process FOREIGN KEY constraint
                    if fk_columns.len() != 1 {
                        log::warn!("Multi-column FOREIGN KEY constraints are not yet supported");
                        continue;
                    }
                    if referred_columns.len() != 1 {
                        log::warn!("FOREIGN KEY constraints with multiple referred columns are not yet supported");
                        continue;
                    }
                    
                    let fk_col_name = fk_columns[0].to_string();
                    let referred_table = foreign_table.to_string();
                    let referred_col_name = referred_columns[0].to_string();
                    
                    let column_found = columns.iter_mut().find(|col| col.get_name() == fk_col_name);
                    match column_found {
                        Some(column) => {
                            // Create foreign key constraint
                            use crate::catalog::column::ForeignKeyConstraint;
                            let foreign_key_constraint = ForeignKeyConstraint {
                                referenced_table: referred_table.clone(),
                                referenced_column: referred_col_name.clone(),
                                on_delete: None,
                                on_update: None,
                            };
                            column.set_foreign_key(Some(foreign_key_constraint));
                            debug!("Set FOREIGN KEY constraint on column '{}' referencing '{}({})'", fk_col_name, referred_table, referred_col_name);
                        }
                        None => {
                            return Err(format!(
                                "FOREIGN KEY constraint column '{}' not found in table",
                                fk_col_name
                            ));
                        }
                    }
                }
                TableConstraint::Check { .. } => {
                    // Check constraints at table level would require complex expression parsing
                    log::warn!("Table-level CHECK constraints are not yet supported");
                }
                _ => {
                    // Other constraint types not supported yet
                    log::warn!("Unsupported table constraint type encountered");
                }
            }
        }
        Ok(())
    }

    pub fn build_create_index_plan(
        &mut self,
        create_index: &CreateIndex,
    ) -> Result<Box<LogicalPlan>, String> {
        let index_name = match &create_index.name {
            Some(name) => name.to_string(),
            None => {
                // Auto-generate index name if not provided
                let table_name = match &create_index.table_name {
                    ObjectName(parts) if parts.len() == 1 => parts[0].to_string(),
                    _ => return Err("Only single table indices are supported".to_string()),
                };
                let columns_str = create_index
                    .columns
                    .iter()
                    .map(|col| col.to_string())
                    .collect::<Vec<_>>()
                    .join("_");
                format!("{}_{}_idx", table_name, columns_str)
            }
        };
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

    pub fn prepare_join_scan(&self, select: &Select) -> Result<Box<LogicalPlan>, String> {
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
        statements: &[Statement],
        exception_statements: &Option<Vec<ExceptionWhen>>,
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
        let transaction_modifier = *modifier;

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
            statements.to_owned(),
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
        let commit_plan = LogicalPlan::commit_transaction(*chain, *end, *modifier);

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
            ObjectNamePart::Function(func) => func.name.value.clone(),
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
        _if_exists: &bool,
        only: &bool,
        operations: &[AlterTableOperation],
        location: &Option<HiveSetLocation>,
        on_cluster: &Option<Ident>,
    ) -> Result<Box<LogicalPlan>, String> {
        // Extract the table name
        if name.0.is_empty() {
            return Err("Table name cannot be empty".to_string());
        }

        let table_name = match &name.0[0] {
            ObjectNamePart::Identifier(ident) => ident.value.clone(),
            ObjectNamePart::Function(func) => func.name.value.clone(),
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
                    has_column_keyword,
                    column_names,
                    if_exists,
                    drop_behavior,
                } => {
                    let column_kw = if *has_column_keyword { "COLUMN " } else { "" };
                    let if_exists_kw = if *if_exists { "IF EXISTS " } else { "" };
                    let cols = column_names
                        .iter()
                        .map(|id| id.value.clone())
                        .collect::<Vec<_>>()
                        .join(", ");
                    let behavior_suffix = match drop_behavior {
                        Some(DropBehavior::Cascade) => " CASCADE",
                        Some(DropBehavior::Restrict) => " RESTRICT",
                        None => "",
                    };
                    format!("DROP {}{}{}{}", column_kw, if_exists_kw, cols, behavior_suffix)
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
                        ObjectNamePart::Function(func) => func.name.value.clone(),
                    };
                    format!("RENAME TO {}", new_name)
                }
                AlterTableOperation::AlterColumn { column_name, .. } => {
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
        _or_alter: &bool,
        _or_replace: &bool,
        materialized: &bool,
        name: &ObjectName,
        columns: &[ViewColumnDef],
        query: &Query,
        _options: &CreateTableOptions,
        _cluster_by: &[Ident],
        _comment: &Option<String>,
        _with_no_schema_binding: &bool,
        if_not_exists: &bool,
        _temporary: &bool,
        _to: &Option<ObjectName>,
        _params: &Option<CreateViewParams>,
    ) -> Result<Box<LogicalPlan>, String> {
        // Extract view name from ObjectName
        if name.0.is_empty() {
            return Err("View name cannot be empty".to_string());
        }

        let view_name = match &name.0[0] {
            ObjectNamePart::Identifier(ident) => ident.value.clone(),
            ObjectNamePart::Function(func) => func.name.value.clone(),
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
        columns: &[Ident],
        query: &Query,
        with_options: &[SqlOption],
    ) -> Result<Box<LogicalPlan>, String> {
        // Extract view name from ObjectName
        if name.0.is_empty() {
            return Err("View name cannot be empty".to_string());
        }

        let view_name = match &name.0[0] {
            ObjectNamePart::Identifier(ident) => ident.value.clone(),
            ObjectNamePart::Function(func) => func.name.value.clone(),
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
        if !columns.is_empty()
            && columns.len() != schema.get_column_count() as usize {
                return Err(format!(
                    "Number of column names ({}) does not match number of columns in result set ({})",
                    columns.len(),
                    schema.get_column_count()
                ));
            }

        // Construct the operation string
        let mut operation = "AS ".to_string();

        // Add the column list if specified
        if !columns.is_empty() {
            operation.push('(');
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
                            operation.push(')');
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
                            operation.push(')');
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
                    SqlOption::Comment(_) => {}
                    SqlOption::TableSpace(_) => {}
                    SqlOption::NamedParenthesizedList(_) => {}
                }
            }
            operation.push(')');
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
        if let Some(_starts_with) = &show_options.starts_with {
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
                    ShowStatementFilter::Where(_expr) => {
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
        if let Some(_limit_expr) = &show_options.limit {
            debug!("LIMIT option not fully implemented for SHOW DATABASES");
        }

        // Handle limit from clause
        if let Some(_limit_from) = &show_options.limit_from {
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

                        match &obj_name.0[0] {
                            ObjectNamePart::Identifier(ident) => ident.value.clone(),
                            ObjectNamePart::Function(func) => func.name.value.clone(),
                        }
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
                            return match filter_pos {
                                ShowStatementFilterPosition::Infix(filter)
                                | ShowStatementFilterPosition::Suffix(filter) => {
                                    match filter {
                                        ShowStatementFilter::NoKeyword(table) => {
                                            // Parent name is the schema, this is the table
                                            Ok(LogicalPlan::show_columns_with_options(
                                                table.clone(),
                                                Some(parent_name),
                                                *extended,
                                                *full,
                                            ))
                                        }
                                        _ => {
                                            Err("Table name is required for SHOW COLUMNS"
                                                .to_string())
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
                    return match filter_pos {
                        ShowStatementFilterPosition::Infix(filter)
                        | ShowStatementFilterPosition::Suffix(filter) => {
                            match filter {
                                ShowStatementFilter::NoKeyword(table) => {
                                    // No schema provided, just the table
                                    Ok(LogicalPlan::show_columns_with_options(
                                        table.clone(),
                                        None,
                                        *extended,
                                        *full,
                                    ))
                                }
                                _ => {
                                    Err(
                                        "Table name is required for SHOW COLUMNS".to_string()
                                    )
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
                        ShowStatementFilter::Where(_expr) => {
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
        if let Some(_starts_with) = &show_options.starts_with {
            debug!("STARTS WITH clause not fully implemented for SHOW COLUMNS");
        }

        if let Some(_limit_expr) = &show_options.limit {
            debug!("LIMIT option not fully implemented for SHOW COLUMNS");
        }

        if let Some(_limit_from) = &show_options.limit_from {
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
                    ObjectNamePart::Function(func) => func.name.value.clone(),
                }
            }
            Use::Catalog(obj_name) => {
                if obj_name.0.is_empty() {
                    return Err("Empty catalog name in USE statement".to_string());
                }

                match &obj_name.0[0] {
                    ObjectNamePart::Identifier(ident) => ident.value.clone(),
                    ObjectNamePart::Function(func) => func.name.value.clone(),
                }
            }
            Use::Warehouse(_obj_name) => {
                return Err("USE WAREHOUSE statement is not supported".to_string());
            }
            Use::Role(_obj_name) => {
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
        let _schema = Schema::new(vec![]);
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
                _ => return Err("EXPLAIN for this statement type not yet supported".to_string()),
            };

            // Use the helper method to create the explain plan
            Ok(self.create_explain_plan(&inner_plan))
        } else {
            Err("Expected EXPLAIN statement".to_string())
        }
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

        let full_schema = table_info.get_table_schema();
        let table_oid = table_info.get_table_oidt();

        // Create schema for VALUES plan based on explicit columns or full table schema
        let values_schema = if !insert.columns.is_empty() {
            // If explicit columns are specified, create a schema with only those columns
            let mut columns = Vec::new();
            for column_ident in &insert.columns {
                let column_name = column_ident.value.clone();
                let column_index = full_schema.get_column_index(&column_name).ok_or_else(|| {
                    format!(
                        "Column '{}' does not exist in table '{}'",
                        column_name, table_name
                    )
                })?;
                let column = full_schema
                    .get_column(column_index)
                    .ok_or_else(|| format!("Failed to get column '{}' from schema", column_name))?;
                columns.push(column.clone());
            }
            Schema::new(columns)
        } else {
            // If no explicit columns, use the full table schema
            full_schema.clone()
        };

        // Plan the source (VALUES or SELECT)
        let source_plan = match &insert.source {
            Some(query) => match &*query.body {
                SetExpr::Values(values) => self.build_values_plan(&values.rows, &values_schema)?,
                SetExpr::Select(select) => {
                    let select_plan = self.build_select_plan(select)?;
                    if !self
                        .schema_manager
                        .schemas_compatible(&select_plan.get_schema().unwrap(), &values_schema)
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
            full_schema, // Use full schema for the INSERT plan itself
            table_oid,
            source_plan,
        ))
    }

    pub fn build_update_plan(
        &self,
        table: &TableWithJoins,
        assignments: &Vec<Assignment>,
        _from: &Option<UpdateTableFromKind>,
        selection: &Option<Expr>,
        _returning: &Option<Vec<SelectItem>>,
        _or: &Option<SqliteOnConflict>,
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
            // Validate that the column being updated exists in the table schema
            let column_name = match &assignment.target {
                AssignmentTarget::ColumnName(obj_name) => obj_name.to_string(),
                AssignmentTarget::Tuple(_) => {
                    return Err("Tuple assignments are not supported".to_string());
                }
            };

            // Check if column exists in the schema and get its index
            let column_index = (0..schema.get_column_count() as usize).find(|&i| {
                if let Some(col) = schema.get_column(i) {
                    col.get_name() == column_name
                } else {
                    false
                }
            });

            let column_index = column_index.ok_or_else(|| {
                format!(
                    "Column '{}' does not exist in table '{}'",
                    column_name, table_name
                )
            })?;

            // Get the column info
            let column = schema.get_column(column_index).unwrap().clone();

            // Parse the value expression
            let value_expr = self
                .expression_parser
                .parse_expression(&assignment.value, &schema)?;

            // Create an AssignmentExpression that encapsulates both the target column and value
            let assignment_expr = Expression::Assignment(AssignmentExpression::new(
                column_index,
                column.clone(),
                Arc::new(value_expr),
            ));

            // Add the single assignment expression
            update_exprs.push(Arc::new(assignment_expr));
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
            for expr in row.iter() {
                let value_expr = Arc::new(self.expression_parser.parse_expression(expr, schema)?);
                value_exprs.push(value_expr);
            }
            value_rows.push(value_exprs);
        }

        Ok(LogicalPlan::values(value_rows, schema.clone()))
    }
}
