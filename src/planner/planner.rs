use crate::catalogue::catalogue::Catalog;
use crate::catalogue::column::Column;
use crate::catalogue::schema::Schema;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::expressions::abstract_expression::ExpressionOps;
use crate::execution::expressions::aggregate_expression::AggregationType;
use crate::execution::expressions::arithmetic_expression::{ArithmeticExpression, ArithmeticOp};
use crate::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::execution::expressions::comparison_expression::{ComparisonExpression, ComparisonType};
use crate::execution::expressions::constant_value_expression::ConstantExpression;
use crate::execution::expressions::logic_expression::{LogicExpression, LogicType};
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
use crate::execution::plans::aggregation_plan::AggregationPlanNode;
use crate::execution::plans::create_plan::CreateTablePlanNode;
use crate::execution::plans::filter_plan::FilterNode;
use crate::execution::plans::insert_plan::InsertNode;
use crate::execution::plans::projection_plan::ProjectionNode;
use crate::execution::plans::seq_scan_plan::SeqScanPlanNode;
use crate::execution::plans::values_plan::ValuesNode;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use chrono::NaiveDateTime;
use log::debug;
use parking_lot::RwLock;
use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    SelectItem, UnaryOperator,
};
use sqlparser::ast::{
    ColumnDef, CreateTable, DataType, Insert, ObjectName, Query, Select, SetExpr, Statement,
    TableFactor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::env;
use std::sync::Arc;

pub struct QueryPlanner {
    catalog: Arc<RwLock<Catalog>>,
    log_detailed: bool,
}

impl QueryPlanner {
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        Self {
            catalog,
            log_detailed: env::var("RUST_TEST").is_ok(),
        }
    }

    pub fn with_detailed_logging(catalog: Arc<RwLock<Catalog>>, detailed: bool) -> Self {
        Self {
            catalog,
            log_detailed: detailed || env::var("RUST_TEST").is_ok(),
        }
    }

    pub fn set_detailed_logging(&mut self, detailed: bool) {
        self.log_detailed = detailed || env::var("RUST_TEST").is_ok();
    }

    pub fn create_plan(&mut self, sql: &str) -> Result<PlanNode, String> {
        debug!("Planning query: {}", sql);

        let dialect = GenericDialect {};
        let ast = match Parser::parse_sql(&dialect, sql) {
            Ok(ast) => ast,
            Err(e) => {
                debug!("Failed to parse SQL: {}", e);
                return Err(format!("Failed to parse SQL: {}", e));
            }
        };

        if ast.len() != 1 {
            debug!("Error: Expected exactly one statement");
            return Err("Expected exactly one statement".to_string());
        }

        if self.log_detailed {
            debug!("Parsed AST: {:?}", ast[0]);
        }

        match &ast[0] {
            Statement::CreateTable(create_table) => {
                debug!("Planning CREATE TABLE statement");
                self.plan_create_table(create_table)
            }
            Statement::Query(query) => {
                debug!("Planning SELECT query");
                self.plan_query(query)
            }
            Statement::Insert(insert) => {
                debug!("Planning INSERT statement");
                self.plan_insert(insert)
            }
            _ => {
                debug!("Error: Unsupported statement type");
                Err("Only SELECT statements are supported".to_string())
            }
        }
    }

    pub fn explain(&mut self, sql: &str) -> Result<String, String> {
        let plan = self.create_plan(sql)?;
        Ok(format!("Query Plan:\n{}\n", plan.explain()))
    }

    fn plan_create_table(&mut self, create_table: &CreateTable) -> Result<PlanNode, String> {
        let columns = self.convert_column_defs(&create_table.columns)?;
        let schema = Schema::new(columns);
        let table_name = create_table.name.to_string();

        // Add the new table metadata to the catalog
        let mut catalog_guard = self.catalog.write();
        if let Some(_) = catalog_guard.create_table(&table_name, schema.clone()) {
            Ok(PlanNode::CreateTable(CreateTablePlanNode::new(
                schema,
                table_name,
                create_table.if_not_exists,
            )))
        } else {
            Err(format!("Table '{}' already exists", table_name))
        }
    }

    fn plan_query(&self, query: &Query) -> Result<PlanNode, String> {
        match &*query.body {
            SetExpr::Select(select) => self.plan_select(select),
            SetExpr::Query(query) => self.plan_query(query),
            _ => Err("Only simple SELECT statements are supported".to_string()),
        }
    }

    fn plan_select(&self, select: &Box<Select>) -> Result<PlanNode, String> {
        // Validate and extract table information
        let (table_name, schema, table_oid) = self.prepare_table_scan(select)?;

        // Start building the plan from bottom up
        let mut current_plan = self.create_base_scan_plan(&schema, table_oid, &table_name);

        // Apply filtering if WHERE clause is present
        current_plan = self.apply_where_filter(select, current_plan, &schema, table_oid, &table_name)?;

        // Handle aggregation and projection
        current_plan = self.process_aggregation_and_projection(select, current_plan, &schema)?;

        Ok(current_plan)
    }

    fn plan_insert(&self, insert: &Insert) -> Result<PlanNode, String> {
        // Extract table name
        let table_name = self.extract_table_name(&insert.table_name)?;

        // Check table existence in the catalog
        let catalog_guard = self.catalog.read();
        let table_info = catalog_guard
            .get_table(&table_name)
            .ok_or_else(|| format!("Table '{}' does not exist in the catalog", table_name))?;

        // Fetch schema and OID
        let table_schema = table_info.get_table_schema();
        let table_oid = table_info.get_table_oidt();

        // Plan values or select source
        let child_plan = match &insert.source {
            Some(query) => match &*query.body {
                SetExpr::Values(values) => self.plan_values_node(&values.rows, &table_schema)?,
                SetExpr::Select(select) => {
                    let select_plan = self.plan_select(select)?;
                    if !self.schemas_compatible(select_plan.get_output_schema(), &table_schema) {
                        return Err("SELECT schema does not match INSERT target schema".to_string());
                    }
                    select_plan
                }
                _ => {
                    return Err(
                        "Only VALUES and SELECT are supported in INSERT statements".to_string()
                    )
                }
            },
            None => {
                return Err("INSERT statement must have a source (VALUES or SELECT)".to_string())
            }
        };

        // Return constructed insert plan node
        Ok(PlanNode::Insert(InsertNode::new(
            table_schema,
            table_oid,
            table_name,
            vec![],
            child_plan,
        )))
    }

    fn plan_values_node(&self, rows: &[Vec<Expr>], schema: &Schema) -> Result<PlanNode, String> {
        let mut all_rows = Vec::new();

        for row in rows {
            if row.len() != schema.get_column_count() as usize {
                return Err(format!(
                    "VALUES has {} columns but schema expects {}",
                    row.len(),
                    schema.get_column_count()
                ));
            }

            let mut row_values = Vec::new();
            for (i, expr) in row.iter().enumerate() {
                let column = schema.get_column(i).unwrap();
                let value_expr = match expr {
                    Expr::Value(value) => Arc::new(Expression::Constant(
                        self.create_constant_expression(value, column)?,
                    )),
                    Expr::Cast { expr, .. } => {
                        // For cast expressions, use the target column type directly
                        match expr.as_ref() {
                            Expr::Value(value) => Arc::new(Expression::Constant(
                                self.create_constant_expression(value, column)?,
                            )),
                            _ => {
                                return Err(format!(
                                    "Unsupported inner expression in CAST: {:?}",
                                    expr
                                ))
                            }
                        }
                    }
                    _ => return Err(format!("Unsupported expression in VALUES: {:?}", expr)),
                };
                row_values.push(value_expr);
            }
            all_rows.push(row_values);
        }

        Ok(PlanNode::Values(
            ValuesNode::new(schema.clone(), all_rows, PlanNode::Empty).unwrap(),
        ))
    }

    fn parse_expression(&self, expr: &Expr, schema: &Schema) -> Result<Expression, String> {
        match expr {
            Expr::Identifier(ident) => {
                // Look up column in the provided schema
                let column_idx = schema
                    .get_column_index(&ident.value)
                    .ok_or_else(|| format!("Column {} not found in schema", ident.value))?;

                let column = schema
                    .get_column(column_idx)
                    .ok_or_else(|| format!("Failed to get column at index {}", column_idx))?;

                Ok(Expression::ColumnRef(ColumnRefExpression::new(
                    0, // table index (for single table, it's always 0)
                    column_idx,
                    column.clone(),
                    vec![],
                )))
            }

            Expr::CompoundIdentifier(parts) => {
                if parts.len() != 2 {
                    return Err("Only table.column compound identifiers are supported".to_string());
                }

                let table_name = &parts[0].value;
                let column_name = &parts[1].value;

                // Look up column in the provided schema
                let column_idx = schema
                    .get_column_index(&format!("{}.{}", table_name, column_name))
                    .ok_or_else(|| {
                        format!("Column {}.{} not found in schema", table_name, column_name)
                    })?;

                let column = schema
                    .get_column(column_idx)
                    .ok_or_else(|| format!("Failed to get column at index {}", column_idx))?;

                Ok(Expression::ColumnRef(ColumnRefExpression::new(
                    0,
                    column_idx,
                    column.clone(),
                    vec![],
                )))
            }

            Expr::Value(value) => {
                let (val, type_id) = match value {
                    sqlparser::ast::Value::Number(n, _) => (
                        Value::from(n.parse::<i32>().map_err(|e| e.to_string())?),
                        TypeId::Integer,
                    ),
                    sqlparser::ast::Value::SingleQuotedString(s)
                    | sqlparser::ast::Value::DoubleQuotedString(s) => {
                        (Value::from(s.as_str()), TypeId::VarChar)
                    }
                    sqlparser::ast::Value::Boolean(b) => (Value::from(*b), TypeId::Boolean),
                    sqlparser::ast::Value::Null => (Value::new(Val::Null), TypeId::Invalid),
                    _ => return Err(format!("Unsupported value type: {:?}", value)),
                };

                Ok(Expression::Constant(ConstantExpression::new(
                    val,
                    Column::new("const", type_id),
                    vec![],
                )))
            }

            Expr::BinaryOp { left, op, right } => {
                let left_expr = self.parse_expression(left, schema)?;
                let right_expr = self.parse_expression(right, schema)?;

                match op {
                    BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Gt
                    | BinaryOperator::Lt
                    | BinaryOperator::GtEq
                    | BinaryOperator::LtEq => {
                        let comparison_type = match op {
                            BinaryOperator::Eq => ComparisonType::Equal,
                            BinaryOperator::NotEq => ComparisonType::NotEqual,
                            BinaryOperator::Gt => ComparisonType::GreaterThan,
                            BinaryOperator::Lt => ComparisonType::LessThan,
                            BinaryOperator::GtEq => ComparisonType::GreaterThanOrEqual,
                            BinaryOperator::LtEq => ComparisonType::LessThanOrEqual,
                            _ => unreachable!(),
                        };

                        Ok(Expression::Comparison(ComparisonExpression::new(
                            Arc::new(left_expr),
                            Arc::new(right_expr),
                            comparison_type,
                            vec![],
                        )))
                    }

                    BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide => {
                        let op = match op {
                            BinaryOperator::Plus => ArithmeticOp::Add,
                            BinaryOperator::Minus => ArithmeticOp::Subtract,
                            BinaryOperator::Multiply => ArithmeticOp::Multiply,
                            BinaryOperator::Divide => ArithmeticOp::Divide,
                            _ => unreachable!(),
                        };

                        Ok(Expression::Arithmetic(ArithmeticExpression::new(
                            Arc::new(left_expr.clone()),
                            Arc::new(right_expr.clone()),
                            op,
                            vec![Arc::new(left_expr.clone()), Arc::new(right_expr.clone())],
                        )))
                    }

                    BinaryOperator::And | BinaryOperator::Or => {
                        let logic_type = match op {
                            BinaryOperator::And => LogicType::And,
                            BinaryOperator::Or => LogicType::Or,
                            _ => unreachable!(),
                        };

                        Ok(Expression::Logic(LogicExpression::new(
                            Arc::new(left_expr.clone()),
                            Arc::new(right_expr.clone()),
                            logic_type,
                            vec![Arc::new(left_expr.clone()), Arc::new(right_expr.clone())],
                        )))
                    }

                    _ => Err("Unsupported binary operator".to_string()),
                }
            }

            Expr::UnaryOp { op, expr } => {
                let inner_expr = self.parse_expression(expr, schema)?;
                match op {
                    UnaryOperator::Not => Ok(Expression::Logic(LogicExpression::new(
                        Arc::new(inner_expr),
                        Arc::new(Expression::Constant(ConstantExpression::new(
                            Value::new(true),
                            Column::new("const", TypeId::Boolean),
                            vec![],
                        ))),
                        LogicType::And,
                        vec![],
                    ))),
                    UnaryOperator::Plus => Ok(inner_expr),
                    UnaryOperator::Minus => {
                        let zero = Expression::Constant(ConstantExpression::new(
                            Value::new(0),
                            Column::new("const", TypeId::Integer),
                            vec![],
                        ));
                        Ok(Expression::Arithmetic(ArithmeticExpression::new(
                            Arc::new(zero),
                            Arc::new(inner_expr),
                            ArithmeticOp::Subtract,
                            vec![],
                        )))
                    }
                    _ => Err("Unsupported unary operator".to_string()),
                }
            }

            Expr::IsNull(expr) => {
                let inner_expr = self.parse_expression(expr, schema)?;
                Ok(Expression::Comparison(ComparisonExpression::new(
                    Arc::new(inner_expr),
                    Arc::new(Expression::Constant(ConstantExpression::new(
                        Value::new(Val::Null),
                        Column::new("const", TypeId::Invalid),
                        vec![],
                    ))),
                    ComparisonType::Equal,
                    vec![],
                )))
            }

            Expr::IsNotNull(expr) => {
                let inner_expr = self.parse_expression(expr, schema)?;
                Ok(Expression::Comparison(ComparisonExpression::new(
                    Arc::new(inner_expr),
                    Arc::new(Expression::Constant(ConstantExpression::new(
                        Value::new(Val::Null),
                        Column::new("const", TypeId::Invalid),
                        vec![],
                    ))),
                    ComparisonType::NotEqual,
                    vec![],
                )))
            }

            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr = self.parse_expression(expr, schema)?;
                let low = self.parse_expression(low, schema)?;
                let high = self.parse_expression(high, schema)?;

                let low_compare = Expression::Comparison(ComparisonExpression::new(
                    Arc::new(expr.clone()),
                    Arc::new(low),
                    ComparisonType::GreaterThanOrEqual,
                    vec![],
                ));

                let high_compare = Expression::Comparison(ComparisonExpression::new(
                    Arc::new(expr),
                    Arc::new(high),
                    ComparisonType::LessThanOrEqual,
                    vec![],
                ));

                let mut result = Expression::Logic(LogicExpression::new(
                    Arc::new(low_compare),
                    Arc::new(high_compare),
                    LogicType::And,
                    vec![],
                ));

                if *negated {
                    result = Expression::Logic(LogicExpression::new(
                        Arc::new(result),
                        Arc::new(Expression::Constant(ConstantExpression::new(
                            Value::new(true),
                            Column::new("const", TypeId::Boolean),
                            vec![],
                        ))),
                        LogicType::And, // Using AND with true is effectively NOT
                        vec![],
                    ));
                }

                Ok(result)
            }

            Expr::Function(func) => match self.parse_aggregate_function(&func, schema) {
                Ok(aggregate_function) => Ok(aggregate_function.0),
                _ => Err(format!("Unsupported function: {}", func)),
            },

            _ => Err(format!("Unsupported expression type: {:?}", expr)),
        }
    }

    fn parse_aggregate_function(
        &self,
        func: &Function,
        schema: &Schema,
    ) -> Result<(Expression, AggregationType), String> {
        let func_name = func.name.to_string().to_uppercase();

        let (agg_type, arg_expr) = match func_name.as_str() {
            "COUNT" => {
                match &func.args {
                    FunctionArguments::None => (
                        // COUNT without args treated as COUNT(*)
                        AggregationType::CountStar,
                        Expression::Constant(ConstantExpression::new(
                            Value::new(1),
                            Column::new("count", TypeId::Integer), // Return type for COUNT
                            vec![],
                        )),
                    ),
                    FunctionArguments::List(list) => {
                        if list.args.is_empty() {
                            // COUNT() treated as COUNT(*)
                            (
                                AggregationType::CountStar,
                                Expression::Constant(ConstantExpression::new(
                                    Value::new(1),
                                    Column::new("count", TypeId::Integer),
                                    vec![],
                                )),
                            )
                        } else {
                            match &list.args[0] {
                                FunctionArg::Named { .. } => {
                                    return Err("Named arguments not supported in aggregate functions".to_string());
                                }
                                FunctionArg::Unnamed(arg_expr) => match arg_expr {
                                    FunctionArgExpr::Expr(expr) => {
                                        let inner_expr = self.parse_expression(expr, schema)?;
                                        // For COUNT, we always return an Integer column regardless of input type
                                        let count_col = Column::new("count", TypeId::Integer);
                                        (AggregationType::Count, inner_expr)
                                    }
                                    FunctionArgExpr::Wildcard => (
                                        AggregationType::CountStar,
                                        Expression::Constant(ConstantExpression::new(
                                            Value::new(1),
                                            Column::new("count", TypeId::Integer),
                                            vec![],
                                        )),
                                    ),
                                    FunctionArgExpr::QualifiedWildcard(_) => {
                                        return Err("Qualified wildcards not supported".to_string());
                                    }
                                },
                            }
                        }
                    }
                    _ => return Err("Subqueries not supported in aggregate functions".to_string()),
                }
            }
            "SUM" | "MIN" | "MAX" => {
                let agg_type = match func_name.as_str() {
                    "SUM" => AggregationType::Sum,
                    "MIN" => AggregationType::Min,
                    "MAX" => AggregationType::Max,
                    _ => unreachable!(),
                };

                match &func.args {
                    FunctionArguments::None => {
                        return Err(format!("{}() requires an argument", func_name));
                    }
                    FunctionArguments::List(list) => {
                        if list.args.is_empty() {
                            return Err(format!("{}() requires an argument", func_name));
                        }

                        match &list.args[0] {
                            FunctionArg::Named { .. } => {
                                return Err("Named arguments not supported in aggregate functions"
                                    .to_string());
                            }
                            FunctionArg::Unnamed(arg_expr) => match arg_expr {
                                FunctionArgExpr::Expr(expr) => {
                                    (agg_type, self.parse_expression(expr, schema)?)
                                }
                                FunctionArgExpr::Wildcard
                                | FunctionArgExpr::QualifiedWildcard(_) => {
                                    return Err(format!("{}(*) is not valid", func_name));
                                }
                            },
                        }
                    }
                    FunctionArguments::Subquery(_) => {
                        return Err("Subqueries not supported in aggregate functions".to_string());
                    }
                }
            }
            _ => return Err(format!("Unsupported aggregate function: {}", func_name)),
        };

        Ok((arg_expr, agg_type))
    }

    fn parse_aggregates(
        &self,
        projection: &Vec<SelectItem>,
        schema: &Schema,
    ) -> Result<(Vec<Arc<Expression>>, Vec<AggregationType>), String> {
        let mut agg_exprs = Vec::new();
        let mut agg_types = Vec::new();

        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    if let Expr::Function(func) = expr {
                        match self.parse_aggregate_function(func, schema) {
                            Ok((agg_expr, agg_type)) => {
                                agg_exprs.push(Arc::new(agg_expr));
                                agg_types.push(agg_type);
                            }
                            Err(_) => continue, // Skip non-aggregate functions
                        }
                    }
                }
                SelectItem::Wildcard(_) => {} // Ignore wildcard for aggregates
                _ => {}
            }
        }

        Ok((agg_exprs, agg_types))
    }

    fn process_aggregation(
        &self,
        select: &Box<Select>,
        mut current_plan: PlanNode,
        base_schema: &Schema,
        has_group_by: bool,
        has_aggregates: bool,
    ) -> Result<PlanNode, String> {
        // Parse aggregate functions
        let (agg_exprs, agg_types) = self.parse_aggregates(&select.projection, base_schema)?;

        // Determine group by expressions and wrap them in Arc
        let group_by_exprs = self.determine_group_by_expressions(select, base_schema, has_group_by)?
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<Arc<Expression>>>();

        // Create output schema for aggregation
        let output_schema = self.create_aggregation_output_schema(
            &group_by_exprs.iter().map(|e| e.as_ref()).collect::<Vec<&Expression>>(),
            &agg_exprs,
            has_group_by,
        );

        // Create aggregation plan node
        current_plan = PlanNode::Aggregation(AggregationPlanNode::new(
            output_schema,
            vec![current_plan],
            group_by_exprs,  // Now a Vec<Arc<Expression>>
            agg_exprs,
            agg_types,
        ));

        // Add projection for aggregation results
        current_plan = self.project_aggregation_results(select, current_plan)?;

        Ok(current_plan)
    }

    fn parse_projection_expressions(
        &self,
        projection: &[SelectItem],
        schema: &Schema,
    ) -> Result<Vec<Expression>, String> {
        let mut expressions = Vec::new();

        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    match expr {
                        Expr::Function(func) => {
                            // Handle aggregate functions in projection
                            let (agg_expr, _) = self.parse_aggregate_function(func, schema)?;
                            expressions.push(Expression::ColumnRef(ColumnRefExpression::new(
                                0,  // tuple index
                                expressions.len(),  // Use current position as column index
                                Column::new("count", TypeId::Integer),  // Fixed column type for aggregates
                                vec![],
                            )));
                        }
                        _ => {
                            expressions.push(self.parse_expression(expr, schema)?);
                        }
                    }
                }
                SelectItem::Wildcard(_) => {
                    // Add all columns from schema
                    for i in 0..schema.get_column_count() {
                        let col = schema.get_column(i as usize).unwrap();
                        expressions.push(Expression::ColumnRef(
                            ColumnRefExpression::new(0, i as usize, col.clone(), vec![]),
                        ));
                    }
                }
                _ => return Err("Unsupported projection type".to_string()),
            }
        }
        Ok(expressions)
    }

    fn create_projection_schema_for_aggregate(
        &self,
        agg_expr: &Expression,
        agg_type: AggregationType,
    ) -> Column {
        match agg_type {
            AggregationType::Count | AggregationType::CountStar => {
                Column::new("count", TypeId::Integer)
            }
            AggregationType::Sum => agg_expr.get_return_type().clone(),
            AggregationType::Min | AggregationType::Max => agg_expr.get_return_type().clone(),
            // We can either make this exhaustive or have a default case
            #[allow(unreachable_patterns)]
            _ => agg_expr.get_return_type().clone()  // Default to input type
        }
    }

    fn create_constant_expression(
        &self,
        value: &sqlparser::ast::Value,
        column: &Column,
    ) -> Result<ConstantExpression, String> {
        match (value, column.get_type()) {
            // Handle numeric conversions
            (sqlparser::ast::Value::Number(n, _), type_id) => match type_id {
                TypeId::SmallInt => {
                    let num = n
                        .parse::<i16>()
                        .map_err(|_| format!("Failed to parse '{}' as SmallInt", n))?;
                    Ok(ConstantExpression::new(
                        Value::new(num),
                        column.clone(),
                        Vec::new(),
                    ))
                }
                TypeId::Integer => {
                    let num = n
                        .parse::<i32>()
                        .map_err(|_| format!("Failed to parse '{}' as Integer", n))?;
                    Ok(ConstantExpression::new(
                        Value::new(num),
                        column.clone(),
                        Vec::new(),
                    ))
                }
                TypeId::BigInt => {
                    let num = n
                        .parse::<i64>()
                        .map_err(|_| format!("Failed to parse '{}' as BigInt", n))?;
                    Ok(ConstantExpression::new(
                        Value::new(num),
                        column.clone(),
                        Vec::new(),
                    ))
                }
                TypeId::Decimal => {
                    let num = n
                        .parse::<f64>()
                        .map_err(|_| format!("Failed to parse '{}' as Decimal", n))?;
                    Ok(ConstantExpression::new(
                        Value::new(num),
                        column.clone(),
                        Vec::new(),
                    ))
                }
                _ => Err(format!("Cannot convert number to {:?}", type_id)),
            },

            // Handle string values
            (
                sqlparser::ast::Value::SingleQuotedString(s)
                | sqlparser::ast::Value::DoubleQuotedString(s),
                type_id,
            ) => {
                match type_id {
                    TypeId::VarChar => Ok(ConstantExpression::new(
                        Value::new(s.clone()),
                        column.clone(),
                        Vec::new(),
                    )),
                    TypeId::Timestamp => {
                        // Parse timestamp string to u64
                        // First try standard format "YYYY-MM-DD HH:MM:SS"
                        let timestamp = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                            .map(|dt| dt.and_utc().timestamp() as u64)
                            .or_else(|_| {
                                // Try format with milliseconds
                                NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.3f")
                                    .map(|dt| dt.and_utc().timestamp() as u64)
                            })
                            .map_err(|_| format!("Failed to parse timestamp from string: {}", s))?;

                        Ok(ConstantExpression::new(
                            Value::new(timestamp),
                            column.clone(),
                            Vec::new(),
                        ))
                    }
                    _ => Err(format!("Cannot convert string to {:?}", type_id)),
                }
            }

            // Handle boolean values
            (sqlparser::ast::Value::Boolean(b), TypeId::Boolean) => Ok(ConstantExpression::new(
                Value::new(*b),
                column.clone(),
                Vec::new(),
            )),

            // Handle NULL values
            (sqlparser::ast::Value::Null, _) => Ok(ConstantExpression::new(
                Value::new(Val::Null),
                column.clone(),
                Vec::new(),
            )),

            // Handle unsupported combinations
            (value, type_id) => Err(format!("Cannot convert {:?} to {:?}", value, type_id)),
        }
    }

    fn create_aggregation_output_schema(
        &self,
        group_by_exprs: &[&Expression],
        agg_exprs: &[Arc<Expression>],
        has_group_by: bool,
    ) -> Schema {
        if has_group_by {
            // Include both group by and aggregate columns
            let mut columns = group_by_exprs
                .iter()
                .map(|expr| expr.get_return_type().clone())
                .collect::<Vec<_>>();

            columns.extend(
                agg_exprs
                    .iter()
                    .map(|expr| expr.get_return_type().clone())
            );

            Schema::new(columns)
        } else {
            // For global aggregation, output schema has one column per aggregate
            let columns = agg_exprs
                .iter()
                .map(|expr| expr.get_return_type().clone())
                .collect();
            Schema::new(columns)
        }
    }

    fn create_projection_schema(&self, proj_exprs: &[Expression]) -> Result<Schema, String> {
        let columns = proj_exprs
            .iter()
            .enumerate()
            .map(|(idx, expr)| {
                match expr {
                    Expression::ColumnRef(col_ref) => col_ref.get_return_type().clone(),
                    Expression::Constant(const_expr) => const_expr.get_return_type().clone(),
                    Expression::Comparison(comp_expr) => comp_expr.get_return_type().clone(),
                    Expression::Arithmetic(arith_expr) => arith_expr.get_return_type().clone(),
                    Expression::Logic(logic_expr) => logic_expr.get_return_type().clone(),
                    _ => Column::new(&format!("expr_{}", idx), TypeId::Invalid)
                }
            })
            .collect::<Vec<Column>>();

        if columns.is_empty() {
            Err("Cannot create projection schema with no columns".to_string())
        } else {
            Ok(Schema::new(columns))
        }
    }

    /// Create base sequential scan plan node
    fn create_base_scan_plan(&self, schema: &Schema, table_oid: u64, table_name: &str) -> PlanNode {
        PlanNode::SeqScan(SeqScanPlanNode::new(
            schema.clone(),
            table_oid,
            table_name.to_string(),
            None,
        ))
    }

    fn has_aggregate_functions(&self, projection: &Vec<SelectItem>) -> bool {
        projection.iter().any(|item| match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                self.is_aggregate_function(expr)
            }
            _ => false,
        })
    }

    fn is_aggregate_function(&self, expr: &Expr) -> bool {
        if let Expr::Function(func) = expr {
            let name = func.name.to_string().to_uppercase();
            matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
        } else {
            false
        }
    }

    fn convert_column_defs(&self, column_defs: &[ColumnDef]) -> Result<Vec<Column>, String> {
        let mut columns = Vec::new();

        for col_def in column_defs {
            let column_name = col_def.name.to_string();
            let type_id = self.convert_sql_type(&col_def.data_type)?;

            // Handle VARCHAR/STRING types specifically with length
            let column = match &col_def.data_type {
                DataType::Varchar(_) | DataType::String(_) => {
                    // Default length for variable length types
                    Column::new_varlen(&column_name, type_id, 255)
                }
                _ => Column::new(&column_name, type_id),
            };

            columns.push(column);
        }

        Ok(columns)
    }

    fn convert_sql_type(&self, sql_type: &DataType) -> Result<TypeId, String> {
        match sql_type {
            DataType::Boolean | DataType::Bool => Ok(TypeId::Boolean),
            DataType::TinyInt(_) => Ok(TypeId::TinyInt),
            DataType::SmallInt(_) => Ok(TypeId::SmallInt),
            DataType::Int(_) | DataType::Integer(_) => Ok(TypeId::Integer),
            DataType::BigInt(_) => Ok(TypeId::BigInt),
            DataType::Decimal(_) | DataType::Float(_) => Ok(TypeId::Decimal),
            DataType::Varchar(_) | DataType::String(_) | DataType::Text => Ok(TypeId::VarChar),
            DataType::Array(_) => Ok(TypeId::Vector),
            DataType::Timestamp(_, _) => Ok(TypeId::Timestamp),
            _ => Err(format!("Unsupported SQL type: {:?}", sql_type)),
        }
    }

    fn schemas_compatible(&self, source: &Schema, target: &Schema) -> bool {
        if source.get_column_count() != target.get_column_count() {
            return false;
        }

        for i in 0..source.get_column_count() {
            let source_col = source.get_column(i as usize).unwrap();
            let target_col = target.get_column(i as usize).unwrap();

            if !self.types_compatible(source_col.get_type(), target_col.get_type()) {
                return false;
            }
        }

        true
    }

    fn types_compatible(&self, source_type: TypeId, target_type: TypeId) -> bool {
        // Add your type compatibility rules here
        // For example:
        match (source_type, target_type) {
            // Same types are always compatible
            (a, b) if a == b => true,
            _ => false,
        }
    }

    fn extract_table_name(&self, table_name: &ObjectName) -> Result<String, String> {
        match table_name {
            ObjectName(parts) if parts.len() == 1 => Ok(parts[0].value.clone()),
            _ => Err("Only single table INSERT statements are supported".to_string()),
        }
    }

    /// Validate and extract table information for the SELECT statement
    fn prepare_table_scan(&self, select: &Box<Select>) -> Result<(String, Schema, u64), String> {
        if select.from.len() != 1 {
            return Err("Only single table queries are supported".to_string());
        }

        // Extract table name
        let table_name = match &select.from[0].relation {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => return Err("Only simple table scans are supported".to_string()),
        };

        // Retrieve table information from catalog
        let catalog_guard = self.catalog.read();
        let table_info = catalog_guard
            .get_table(&table_name)
            .ok_or_else(|| format!("Table '{}' not found in catalog", table_name))?;

        let schema = table_info.get_table_schema();
        let table_oid = table_info.get_table_oidt();

        Ok((table_name, schema, table_oid))
    }

    /// Apply WHERE clause filter if present
    fn apply_where_filter(
        &self,
        select: &Box<Select>,
        current_plan: PlanNode,
        schema: &Schema,
        table_oid: u64,
        table_name: &str,
    ) -> Result<PlanNode, String> {
        if let Some(where_clause) = &select.selection {
            let filter_expr = self.parse_expression(where_clause, schema)?;
            Ok(PlanNode::Filter(FilterNode::new(
                schema.clone(),
                table_oid,
                table_name.to_string(),
                filter_expr,
                current_plan,
            )))
        } else {
            Ok(current_plan)
        }
    }

    /// Process aggregation and projection
    fn process_aggregation_and_projection(
        &self,
        select: &Box<Select>,
        mut current_plan: PlanNode,
        base_schema: &Schema,
    ) -> Result<PlanNode, String> {
        // Check if aggregation is needed
        let has_group_by = match &select.group_by {
            GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
            GroupByExpr::All(_) => true,
        };
        let has_aggregates = self.has_aggregate_functions(&select.projection);

        // If no aggregation is needed, handle simple projection
        if !has_group_by && !has_aggregates {
            return self.handle_simple_projection(select, current_plan, base_schema);
        }

        // Process aggregation
        current_plan = self.process_aggregation(
            select,
            current_plan,
            base_schema,
            has_group_by,
            has_aggregates,
        )?;

        Ok(current_plan)
    }

    /// Handle simple projection for non-aggregate queries
    fn handle_simple_projection(
        &self,
        select: &Box<Select>,
        mut current_plan: PlanNode,
        schema: &Schema,
    ) -> Result<PlanNode, String> {
        match &select.projection[..] {
            [SelectItem::Wildcard(_)] => Ok(current_plan),
            _ => {
                let proj_exprs = self.parse_projection_expressions(&select.projection, schema)?;
                let proj_schema = self.create_projection_schema(&proj_exprs)?;

                Ok(PlanNode::Projection(ProjectionNode::new(
                    Arc::new(proj_schema),
                    proj_exprs,
                    current_plan,
                )))
            }
        }
    }

    /// Determine group by expressions
    fn determine_group_by_expressions(
        &self,
        select: &Box<Select>,
        base_schema: &Schema,
        has_group_by: bool,
    ) -> Result<Vec<Expression>, String> {
        if !has_group_by {
            return Ok(Vec::new());
        }

        match &select.group_by {
            GroupByExpr::Expressions(exprs, _) => {
                exprs.iter()
                    .map(|expr| self.parse_expression(expr, base_schema))
                    .collect()
            }
            GroupByExpr::All(_) => {
                // If GROUP BY ALL, use all columns from the input schema
                (0..base_schema.get_column_count())
                    .map(|i| {
                        let col = base_schema.get_column(i as usize).unwrap();
                        Ok(Expression::ColumnRef(
                            ColumnRefExpression::new(0, i as usize, col.clone(), vec![])
                        ))
                    })
                    .collect()
            }
        }
    }

    /// Project aggregation results
    fn project_aggregation_results(
        &self,
        select: &Box<Select>,
        mut current_plan: PlanNode,
    ) -> Result<PlanNode, String> {
        let proj_exprs = match &select.projection[..] {
            [SelectItem::Wildcard(_)] => {
                // Add all columns from aggregation output
                let agg_schema = current_plan.get_output_schema();
                (0..agg_schema.get_column_count())
                    .map(|i| {
                        let col = agg_schema.get_column(i as usize).unwrap();
                        Expression::ColumnRef(
                            ColumnRefExpression::new(0, i as usize, col.clone(), vec![]),
                        )
                    })
                    .collect()
            }
            _ => self.parse_projection_expressions(&select.projection, current_plan.get_output_schema())?
        };

        let proj_schema = self.create_projection_schema(&proj_exprs)?;
        Ok(PlanNode::Projection(ProjectionNode::new(
            Arc::new(proj_schema),
            proj_exprs,
            current_plan,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use chrono::Utc;
    use sqlparser::ast::Ident;
    use std::collections::HashMap;

    struct TestContext {
        catalog: Arc<RwLock<Catalog>>,
        planner: QueryPlanner,
        _db_file: String,
        _log_file: String,
        _disk_manager: Arc<FileDiskManager>,
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 5;
            const K: usize = 2;

            let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
            let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
            let log_file = format!("tests/data/{}_{}.log", test_name, timestamp);

            let disk_manager =
                Arc::new(FileDiskManager::new(db_file.clone(), log_file.clone(), 100));
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

            let catalog = Arc::new(RwLock::new(Catalog::new(
                bpm,
                0,              // next_index_oid
                0,              // next_table_oid
                HashMap::new(), // tables
                HashMap::new(), // indexes
                HashMap::new(), // table_names
                HashMap::new(), // index_names
            )));

            let planner = QueryPlanner::new(Arc::clone(&catalog));

            Self {
                catalog,
                planner,
                _db_file: db_file,
                _log_file: log_file,
                _disk_manager: disk_manager,
            }
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self._db_file);
            let _ = std::fs::remove_file(&self._log_file);
        }
    }

    #[test]
    fn test_create_table_plan() {
        let mut ctx = TestContext::new("create_table_test");

        // Create a table
        let sql = "CREATE TABLE users (id INTEGER, name VARCHAR(255), age INTEGER)";
        let plan = ctx.planner.create_plan(sql).unwrap();

        match plan {
            PlanNode::CreateTable(create_table) => {
                assert_eq!(create_table.get_table_name(), "users");
                assert_eq!(create_table.if_not_exists(), false);

                let schema = create_table.get_output_schema();
                assert_eq!(schema.get_column_count(), 3);

                let columns = schema.get_columns();
                assert_eq!(columns[0].get_name(), "id");
                assert_eq!(columns[0].get_type(), TypeId::Integer);
                assert_eq!(columns[1].get_name(), "name");
                assert_eq!(columns[1].get_type(), TypeId::VarChar);
                assert_eq!(columns[2].get_name(), "age");
                assert_eq!(columns[2].get_type(), TypeId::Integer);

                // Verify table was added to catalog
                let catalog = ctx.catalog.read();
                let table = catalog.get_table("users");
                assert!(table.is_some());
                assert_eq!(table.unwrap().get_table_schema(), schema.clone());
            }
            _ => panic!("Expected CreateTable plan node"),
        }
    }

    #[test]
    fn test_select_queries() {
        let mut ctx = TestContext::new("select_queries_test");

        // First create a table
        let create_sql = "CREATE TABLE users (id INTEGER, name VARCHAR(255), age INTEGER)";
        ctx.planner.create_plan(create_sql).unwrap();

        // Test simple select
        let select_sql = "SELECT * FROM users";
        let plan = ctx.planner.create_plan(select_sql).unwrap();

        match plan {
            PlanNode::SeqScan(scan) => {
                assert_eq!(scan.get_table_name(), "users");
                assert_eq!(scan.get_output_schema().get_column_count(), 3);
            }
            _ => panic!("Expected SeqScan plan node"),
        }

        // Test select with filter
        let filter_sql = "SELECT * FROM users WHERE age > 25";
        let plan = ctx.planner.create_plan(filter_sql).unwrap();

        match plan {
            PlanNode::Filter(filter) => {
                assert_eq!(filter.get_table_name(), "users");
                match filter.get_child_plan() {
                    PlanNode::SeqScan(scan) => {
                        assert_eq!(scan.get_table_name(), "users");
                        assert_eq!(scan.get_output_schema().get_column_count(), 3);
                    }
                    _ => panic!("Expected SeqScan as child of Filter"),
                }
            }
            _ => panic!("Expected Filter plan node"),
        }
    }

    #[test]
    fn test_insert_plan() {
        let mut ctx = TestContext::new("insert_test");

        // Create table first
        let create_sql = "CREATE TABLE users (id INTEGER, name VARCHAR(255))";
        ctx.planner.create_plan(create_sql).unwrap();

        // Test insert
        let insert_sql = "INSERT INTO users VALUES (1, 'test')";
        let plan = ctx.planner.create_plan(insert_sql).unwrap();

        match plan {
            PlanNode::Insert(insert) => {
                assert_eq!(insert.get_table_name(), "users");

                // Check child plan (Values node)
                match insert.get_child() {
                    // Double dereference to get to the inner PlanNode
                    PlanNode::Values(values) => {
                        assert_eq!(values.get_output_schema().get_column_count(), 2);
                    }
                    _ => panic!("Expected Values node as child of Insert"),
                }

                // Verify schema matches table schema
                let catalog = ctx.catalog.read();
                let table_schema = catalog.get_table_schema("users").unwrap();
                assert_eq!(insert.get_output_schema(), &table_schema);
            }
            _ => panic!("Expected Insert plan node"),
        }
    }

    #[test]
    fn test_error_handling() {
        let mut ctx = TestContext::new("error_handling_test");

        // Test invalid SQL
        let invalid_sql = "INVALID SQL";
        assert!(ctx.planner.create_plan(invalid_sql).is_err());

        // Test selecting from non-existent table
        let select_sql = "SELECT * FROM nonexistent_table";
        assert!(ctx.planner.create_plan(select_sql).is_err());

        // Test creating duplicate table
        let create_sql = "CREATE TABLE users (id INTEGER)";
        ctx.planner.create_plan(create_sql).unwrap();
        assert!(ctx.planner.create_plan(create_sql).is_err());

        // Test insert into non-existent table
        let insert_sql = "INSERT INTO nonexistent_table VALUES (1)";
        assert!(ctx.planner.create_plan(insert_sql).is_err());
    }

    #[test]
    fn test_select_with_where() {
        let mut ctx = TestContext::new("select_where_test");

        // First create a table
        let create_sql = "CREATE TABLE users (id INTEGER, name VARCHAR(255), age INTEGER)";
        ctx.planner.create_plan(create_sql).unwrap();

        // Test select with different comparison operators
        let test_cases = vec![
            (
                "SELECT * FROM users WHERE age > 25",
                ComparisonType::GreaterThan,
            ),
            ("SELECT * FROM users WHERE age = 30", ComparisonType::Equal),
            (
                "SELECT * FROM users WHERE age < 40",
                ComparisonType::LessThan,
            ),
            (
                "SELECT * FROM users WHERE age >= 25",
                ComparisonType::GreaterThanOrEqual,
            ),
            (
                "SELECT * FROM users WHERE age <= 40",
                ComparisonType::LessThanOrEqual,
            ),
            (
                "SELECT * FROM users WHERE age != 35",
                ComparisonType::NotEqual,
            ),
        ];

        for (sql, expected_comp_type) in test_cases {
            let plan = ctx.planner.create_plan(sql).unwrap();

            match plan {
                PlanNode::Filter(filter) => {
                    assert_eq!(filter.get_table_name(), "users");

                    // Verify predicate
                    match filter.get_filter_predicate() {
                        Expression::Comparison(comp) => {
                            assert_eq!(comp.get_comp_type(), expected_comp_type);

                            // Verify left side is column reference
                            match comp.get_left().as_ref() {
                                Expression::ColumnRef(col_ref) => {
                                    assert_eq!(col_ref.get_return_type().to_string(true), "age");
                                }
                                _ => panic!("Expected ColumnRef on left side of comparison"),
                            }
                        }
                        _ => panic!("Expected Comparison expression"),
                    }

                    // Verify child plan
                    match filter.get_child_plan() {
                        PlanNode::SeqScan(scan) => {
                            assert_eq!(scan.get_table_name(), "users");
                            assert_eq!(scan.get_output_schema().get_column_count(), 3);
                        }
                        _ => panic!("Expected SeqScan as child of Filter"),
                    }
                }
                _ => panic!("Expected Filter plan node"),
            }
        }
    }

    #[test]
    fn test_parse_column_reference() {
        let mut ctx = TestContext::new("column_ref_test");

        // Create a table with a schema
        ctx.planner
            .create_plan("CREATE TABLE test_table (id INTEGER, age INTEGER, name VARCHAR(255))")
            .unwrap();

        // Get the schema from the catalog
        let catalog = ctx.catalog.read();
        let schema = catalog.get_table("test_table").unwrap().get_table_schema();

        // Create an identifier expression
        let ident = Expr::Identifier(Ident {
            value: "age".to_string(),
            quote_style: None,
        });

        // Parse the expression
        let result = ctx.planner.parse_expression(&ident, &schema);
        assert!(result.is_ok());

        match result.unwrap() {
            Expression::ColumnRef(col_ref) => {
                assert_eq!(col_ref.get_column_index(), 1); // age is the second column
                assert_eq!(col_ref.get_return_type().get_type(), TypeId::Integer);
            }
            _ => panic!("Expected ColumnRef expression"),
        }
    }

    #[test]
    fn test_parse_binary_op_with_columns() {
        let mut ctx = TestContext::new("binary_op_test");

        // Create a table with a schema
        ctx.planner
            .create_plan("CREATE TABLE test_table (id INTEGER, age INTEGER, name VARCHAR(255))")
            .unwrap();

        // Get the schema from the catalog
        let catalog = ctx.catalog.read();
        let schema = catalog.get_table("test_table").unwrap().get_table_schema();

        // Create a binary operation: age > 25
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident {
                value: "age".to_string(),
                quote_style: None,
            })),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Value(sqlparser::ast::Value::Number(
                "25".to_string(),
                false,
            ))),
        };

        // Parse the expression
        let result = ctx.planner.parse_expression(&expr, &schema);
        assert!(result.is_ok());

        match result.unwrap() {
            Expression::Comparison(comp) => {
                assert_eq!(comp.get_comp_type(), ComparisonType::GreaterThan);

                match comp.get_left().as_ref() {
                    Expression::ColumnRef(col_ref) => {
                        assert_eq!(col_ref.get_column_index(), 1);
                        assert_eq!(col_ref.get_return_type().get_type(), TypeId::Integer);
                    }
                    _ => panic!("Expected ColumnRef expression for left side"),
                }

                match comp.get_right().as_ref() {
                    Expression::Constant(const_expr) => {
                        assert_eq!(const_expr.get_value(), &Value::from(25));
                    }
                    _ => panic!("Expected Constant expression for right side"),
                }
            }
            _ => panic!("Expected Comparison expression"),
        }
    }
}
