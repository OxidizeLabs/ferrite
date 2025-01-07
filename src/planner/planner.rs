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
use crate::execution::plans::abstract_plan::PlanNode::IndexScan;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
use crate::execution::plans::aggregation_plan::AggregationPlanNode;
use crate::execution::plans::create_index_plan::CreateIndexPlanNode;
use crate::execution::plans::create_table_plan::CreateTablePlanNode;
use crate::execution::plans::delete_plan::DeleteNode;
use crate::execution::plans::filter_plan::FilterNode;
use crate::execution::plans::index_scan_plan::IndexScanNode;
use crate::execution::plans::insert_plan::InsertNode;
use crate::execution::plans::limit_plan::LimitNode;
use crate::execution::plans::mock_scan_plan::MockScanNode;
use crate::execution::plans::projection_plan::ProjectionNode;
use crate::execution::plans::seq_scan_plan::SeqScanPlanNode;
use crate::execution::plans::sort_plan::SortNode;
use crate::execution::plans::topn_plan::TopNNode;
use crate::execution::plans::update_plan::UpdateNode;
use crate::execution::plans::values_plan::ValuesNode;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use log::debug;
use parking_lot::RwLock;
use sqlparser::ast::{
    BinaryOperator, ColumnDef, CreateIndex, CreateTable, DataType, Expr, Function, FunctionArg,
    FunctionArgExpr, FunctionArguments, GroupByExpr, Insert, JoinOperator, ObjectName, Query,
    Select, SelectItem, SetExpr, Statement, TableFactor, UnaryOperator,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::env;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum LogicalPlanType {
    CreateTable {
        schema: Schema,
        table_name: String,
        if_not_exists: bool,
    },
    CreateIndex {
        schema: Schema,
        table_name: String,
        index_name: String,
        key_attrs: Vec<usize>,
        if_not_exists: bool,
    },
    MockScan {
        table_name: String,
        schema: Schema,
        table_oid: u64,
    },
    TableScan {
        table_name: String,
        schema: Schema,
        table_oid: u64,
    },
    IndexScan {
        table_name: String,
        table_oid: u64,
        index_name: String,
        index_oid: u64,
        schema: Schema,
    },
    Filter {
        predicate: Arc<Expression>,
    },
    Project {
        expressions: Vec<Arc<Expression>>,
        schema: Schema,
    },
    Insert {
        table_name: String,
        schema: Schema,
        table_oid: u64,
    },
    Delete {
        table_name: String,
        schema: Schema,
        table_oid: u64,
    },
    Update {
        table_name: String,
        schema: Schema,
        table_oid: u64,
        update_expressions: Vec<Arc<Expression>>,
    },
    Values {
        rows: Vec<Vec<Arc<Expression>>>,
        schema: Schema,
    },
    Aggregate {
        group_by: Vec<Arc<Expression>>,
        aggregates: Vec<Arc<Expression>>,
        schema: Schema,
    },
    NestedLoopJoin {
        left_schema: Schema,
        right_schema: Schema,
        predicate: Arc<Expression>,
        join_type: JoinOperator,
    },
    HashJoin {
        left_schema: Schema,
        right_schema: Schema,
        predicate: Arc<Expression>,
        join_type: JoinOperator,
    },
    Sort {
        sort_expressions: Vec<Arc<Expression>>,
        schema: Schema,
    },
    Limit {
        limit: usize,
        schema: Schema,
    },
    TopN {
        k: usize,
        sort_expressions: Vec<Arc<Expression>>,
        schema: Schema,
    },
}

#[derive(Debug, Clone)]
pub struct LogicalPlan {
    pub plan_type: LogicalPlanType,
    pub children: Vec<Box<LogicalPlan>>,
}

pub struct QueryPlanner {
    catalog: Arc<RwLock<Catalog>>,
    log_detailed: bool,
}

pub trait LogicalToPhysical {
    fn to_physical_plan(&self) -> Result<PlanNode, String>;
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

    pub fn explain(&mut self, sql: &str) -> Result<String, String> {
        let plan = self.create_logical_plan(sql)?;
        Ok(format!("Query Plan:\n{}\n", plan.explain(0)))
    }

    pub fn create_logical_plan(&mut self, sql: &str) -> Result<Box<LogicalPlan>, String> {
        debug!("Creating logical plan for query: {}", sql);

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

        match &ast[0] {
            Statement::CreateTable(create_table) => self.plan_create_table_logical(create_table),
            Statement::CreateIndex(create_index) => self.plan_create_index_logical(create_index),
            Statement::Query(query) => self.plan_query_logical(query),
            Statement::Insert(insert) => self.plan_insert_logical(insert),
            _ => Err("Unsupported statement type".to_string()),
        }
    }

    fn plan_create_table_logical(
        &mut self,
        create_table: &CreateTable,
    ) -> Result<Box<LogicalPlan>, String> {
        let table_name = create_table.name.to_string();

        // Check if table already exists
        {
            let catalog = self.catalog.read();
            if catalog.get_table(&table_name).is_some() {
                // If table exists and IF NOT EXISTS flag is set, return success
                if create_table.if_not_exists {
                    // Create a dummy plan that will effectively be a no-op
                    let columns = self.convert_column_defs(&create_table.columns)?;
                    let schema = Schema::new(columns);
                    return Ok(LogicalPlan::create_table(schema, table_name, true));
                }
                // Otherwise return error
                return Err(format!("Table '{}' already exists", table_name));
            }
        }

        // If we get here, table doesn't exist, proceed with normal creation
        let columns = self.convert_column_defs(&create_table.columns)?;
        let schema = Schema::new(columns);

        Ok(LogicalPlan::create_table(
            schema,
            table_name,
            create_table.if_not_exists,
        ))
    }

    fn plan_create_index_logical(
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

        let catalog_guard = self.catalog.read();
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

    fn plan_query_logical(&self, query: &Query) -> Result<Box<LogicalPlan>, String> {
        match &*query.body {
            SetExpr::Select(select) => self.plan_select_logical(select),
            SetExpr::Query(nested_query) => self.plan_query_logical(nested_query),
            _ => Err("Only simple SELECT statements are supported".to_string()),
        }
    }

    fn plan_select_logical(&self, select: &Box<Select>) -> Result<Box<LogicalPlan>, String> {
        // Get base table scan
        let (table_name, schema, table_oid) = self.prepare_table_scan(select)?;
        let mut current_plan =
            LogicalPlan::table_scan(table_name.clone(), schema.clone(), table_oid);

        // Add filter if WHERE clause exists
        if let Some(where_clause) = &select.selection {
            let predicate = Arc::new(self.parse_expression(where_clause, &schema)?);
            current_plan = LogicalPlan::filter(predicate, current_plan);
        }

        // Handle aggregation if needed
        let has_group_by = match &select.group_by {
            GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
            GroupByExpr::All(_) => true,
        };
        let has_aggregates = self.has_aggregate_functions(&select.projection);

        if has_group_by || has_aggregates {
            current_plan = self.add_aggregation_logical(select, current_plan, &schema)?;
        }

        // Add final projection
        self.add_projection_logical(select, current_plan, &schema)
    }

    fn plan_insert_logical(&self, insert: &Insert) -> Result<Box<LogicalPlan>, String> {
        let table_name = self.extract_table_name(&insert.table_name)?;

        // Get table info from catalog
        let catalog_guard = self.catalog.read();
        let table_info = catalog_guard
            .get_table(&table_name)
            .ok_or_else(|| format!("Table '{}' does not exist", table_name))?;

        let schema = table_info.get_table_schema();
        let table_oid = table_info.get_table_oidt();

        // Plan the source (VALUES or SELECT)
        let source_plan = match &insert.source {
            Some(query) => match &*query.body {
                SetExpr::Values(values) => self.plan_values_logical(&values.rows, &schema)?,
                SetExpr::Select(select) => {
                    let select_plan = self.plan_select_logical(select)?;
                    if !self.schemas_compatible(&select_plan.get_schema(), &schema) {
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

    fn plan_values_logical(
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
                let value_expr = Arc::new(self.parse_expression(expr, schema)?);
                value_exprs.push(value_expr);
            }
            value_rows.push(value_exprs);
        }

        Ok(LogicalPlan::values(value_rows, schema.clone()))
    }

    // fn plan_index_scan(&self, index: &Box<IndexType>) {}

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
                                    return Err(
                                        "Named arguments not supported in aggregate functions"
                                            .to_string(),
                                    );
                                }
                                FunctionArg::Unnamed(arg_expr) => match arg_expr {
                                    FunctionArgExpr::Expr(expr) => {
                                        let inner_expr = self.parse_expression(expr, schema)?;
                                        // For COUNT, we always return an Integer column regardless of input type
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
                                FunctionArg::ExprNamed { .. } => {
                                    return Err(
                                        "ExprNamed arguments not supported in aggregate functions"
                                            .to_string(),
                                    );
                                }
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
                            FunctionArg::ExprNamed { .. } => {
                                return Err("ExprNamed arguments not supported in aggregate functions"
                                    .to_string());
                            }
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
                        Expr::Function(_func) => {
                            // Handle aggregate functions in projection
                            expressions.push(Expression::ColumnRef(ColumnRefExpression::new(
                                0,                                     // tuple index
                                expressions.len(), // Use current position as column index
                                Column::new("count", TypeId::Integer), // Fixed column type for aggregates
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
                        expressions.push(Expression::ColumnRef(ColumnRefExpression::new(
                            0,
                            i as usize,
                            col.clone(),
                            vec![],
                        )));
                    }
                }
                _ => return Err("Unsupported projection type".to_string()),
            }
        }
        Ok(expressions)
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

            columns.extend(agg_exprs.iter().map(|expr| expr.get_return_type().clone()));

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
            GroupByExpr::Expressions(exprs, _) => exprs
                .iter()
                .map(|expr| self.parse_expression(expr, base_schema))
                .collect(),
            GroupByExpr::All(_) => {
                // If GROUP BY ALL, use all columns from the input schema
                (0..base_schema.get_column_count())
                    .map(|i| {
                        let col = base_schema.get_column(i as usize).unwrap();
                        Ok(Expression::ColumnRef(ColumnRefExpression::new(
                            0,
                            i as usize,
                            col.clone(),
                            vec![],
                        )))
                    })
                    .collect()
            }
        }
    }

    fn add_aggregation_logical(
        &self,
        select: &Box<Select>,
        input: Box<LogicalPlan>,
        schema: &Schema,
    ) -> Result<Box<LogicalPlan>, String> {
        let (agg_exprs, _) = self.parse_aggregates(&select.projection, schema)?;
        let group_by_exprs: Vec<_> = self
            .determine_group_by_expressions(select, schema, true)?
            .into_iter()
            .map(|e| Arc::new(e))
            .collect();

        let output_schema = self.create_aggregation_output_schema(
            &group_by_exprs
                .iter()
                .map(|e| e.as_ref())
                .collect::<Vec<_>>(),
            &agg_exprs,
            true,
        );

        Ok(LogicalPlan::aggregate(
            group_by_exprs,
            agg_exprs,
            output_schema,
            input,
        ))
    }

    fn add_projection_logical(
        &self,
        select: &Box<Select>,
        input: Box<LogicalPlan>,
        schema: &Schema,
    ) -> Result<Box<LogicalPlan>, String> {
        match &select.projection[..] {
            [SelectItem::Wildcard(_)] => Ok(input),
            _ => {
                // First parse the projection expressions
                let proj_exprs: Vec<Expression> =
                    self.parse_projection_expressions(&select.projection, schema)?;

                // Create the schema from the parsed expressions
                let proj_schema = self.create_projection_schema(&proj_exprs)?;

                // Convert expressions to Arc<Expression>
                let arc_exprs = proj_exprs.into_iter().map(Arc::new).collect();

                // Create the projection plan
                Ok(LogicalPlan::project(arc_exprs, proj_schema, input))
            }
        }
    }

    fn create_projection_schema(&self, proj_exprs: &[Expression]) -> Result<Schema, String> {
        let columns = proj_exprs
            .iter()
            .enumerate()
            .map(|(idx, expr)| match expr {
                Expression::ColumnRef(col_ref) => col_ref.get_return_type().clone(),
                Expression::Constant(const_expr) => const_expr.get_return_type().clone(),
                Expression::Comparison(comp_expr) => comp_expr.get_return_type().clone(),
                Expression::Arithmetic(arith_expr) => arith_expr.get_return_type().clone(),
                Expression::Logic(logic_expr) => logic_expr.get_return_type().clone(),
                _ => Column::new(&format!("expr_{}", idx), TypeId::Invalid),
            })
            .collect::<Vec<Column>>();

        if columns.is_empty() {
            Err("Cannot create projection schema with no columns".to_string())
        } else {
            Ok(Schema::new(columns))
        }
    }
}

impl Display for LogicalPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.explain(0))
    }
}

impl LogicalPlan {
    pub fn new(plan_type: LogicalPlanType, children: Vec<Box<LogicalPlan>>) -> Self {
        Self {
            plan_type,
            children,
        }
    }

    /// Returns a string representation of the logical plan tree
    pub fn explain(&self, indent: usize) -> String {
        let mut result = format!("{:indent$}", "", indent = indent);

        match &self.plan_type {
            LogicalPlanType::CreateTable {
                table_name,
                schema,
                if_not_exists,
            } => {
                result.push_str(&format!("→ CreateTable: {}\n", table_name));
                result.push_str(&format!(
                    "{:indent$}   Schema: {}\n",
                    "",
                    schema,
                    indent = indent
                ));
                if *if_not_exists {
                    result.push_str(&format!(
                        "{:indent$}   IF NOT EXISTS\n",
                        "",
                        indent = indent
                    ));
                }
            }
            LogicalPlanType::TableScan {
                table_name, schema, ..
            } => {
                result.push_str(&format!("→ TableScan: {}\n", table_name));
                result.push_str(&format!(
                    "{:indent$}   Schema: {}\n",
                    "",
                    schema,
                    indent = indent
                ));
            }
            LogicalPlanType::MockScan {
                table_name, schema, ..
            } => {
                result.push_str(&format!("→ MockScan: {}\n", table_name));
                result.push_str(&format!(
                    "{:indent$}   Schema: {}\n",
                    "",
                    schema,
                    indent = indent
                ));
            }
            LogicalPlanType::Filter { predicate } => {
                result.push_str(&format!("→ Filter: {}\n", predicate));
            }
            LogicalPlanType::Project {
                expressions,
                schema,
            } => {
                result.push_str("→ Project\n");
                for expr in expressions {
                    result.push_str(&format!("{:indent$}   {}\n", "", expr, indent = indent));
                }
                result.push_str(&format!(
                    "{:indent$}   Output Schema: {}\n",
                    "",
                    schema,
                    indent = indent
                ));
            }
            LogicalPlanType::Insert {
                table_name, schema, ..
            } => {
                result.push_str(&format!("→ Insert into {}\n", table_name));
                result.push_str(&format!(
                    "{:indent$}   Schema: {}\n",
                    "",
                    schema,
                    indent = indent
                ));
            }
            LogicalPlanType::Values { rows, schema } => {
                result.push_str("→ Values\n");
                result.push_str(&format!(
                    "{:indent$}   Rows: {}\n",
                    "",
                    rows.len(),
                    indent = indent
                ));
                result.push_str(&format!(
                    "{:indent$}   Schema: {}\n",
                    "",
                    schema,
                    indent = indent
                ));
            }
            LogicalPlanType::Aggregate {
                group_by,
                aggregates,
                schema,
            } => {
                result.push_str("→ Aggregate\n");
                if !group_by.is_empty() {
                    result.push_str(&format!(
                        "{:indent$}   Group By: {:?}\n",
                        "",
                        group_by,
                        indent = indent
                    ));
                }
                result.push_str(&format!(
                    "{:indent$}   Aggregates: {:?}\n",
                    "",
                    aggregates,
                    indent = indent
                ));
                result.push_str(&format!(
                    "{:indent$}   Schema: {}\n",
                    "",
                    schema,
                    indent = indent
                ));
            }
            LogicalPlanType::CreateIndex { .. } => {}
            LogicalPlanType::IndexScan { .. } => {}
            LogicalPlanType::Delete { .. } => {}
            LogicalPlanType::Update { .. } => {}
            LogicalPlanType::NestedLoopJoin { .. } => {}
            LogicalPlanType::HashJoin { .. } => {}
            LogicalPlanType::Sort { .. } => {}
            LogicalPlanType::Limit { .. } => {}
            LogicalPlanType::TopN { .. } => {}
        }

        // Recursively explain children
        for child in &self.children {
            result.push_str(&child.explain(indent + 2));
        }

        result
    }

    pub fn create_table(schema: Schema, table_name: String, if_not_exists: bool) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::CreateTable {
                schema,
                table_name,
                if_not_exists,
            },
            vec![],
        ))
    }

    pub fn create_index(
        schema: Schema,
        table_name: String,
        index_name: String,
        key_attrs: Vec<usize>,
        if_not_exists: bool,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::CreateIndex {
                schema,
                table_name,
                index_name,
                key_attrs,
                if_not_exists,
            },
            vec![],
        ))
    }

    pub fn table_scan(table_name: String, schema: Schema, table_oid: u64) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::TableScan {
                table_name,
                schema,
                table_oid,
            },
            vec![],
        ))
    }

    pub fn filter(predicate: Arc<Expression>, input: Box<LogicalPlan>) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Filter { predicate },
            vec![input],
        ))
    }

    pub fn project(
        expressions: Vec<Arc<Expression>>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Project {
                expressions,
                schema: schema,
            },
            vec![input],
        ))
    }

    pub fn insert(
        table_name: String,
        schema: Schema,
        table_oid: u64,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Insert {
                table_name,
                schema,
                table_oid,
            },
            vec![input],
        ))
    }

    pub fn values(rows: Vec<Vec<Arc<Expression>>>, schema: Schema) -> Box<Self> {
        Box::new(Self::new(LogicalPlanType::Values { rows, schema }, vec![]))
    }

    pub fn aggregate(
        group_by: Vec<Arc<Expression>>,
        aggregates: Vec<Arc<Expression>>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Aggregate {
                group_by,
                aggregates,
                schema,
            },
            vec![input],
        ))
    }

    pub fn index_scan(
        table_name: String,
        table_oid: u64,
        index_name: String,
        index_oid: u64,
        schema: Schema,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::IndexScan {
                table_name,
                table_oid,
                index_name,
                index_oid,
                schema,
            },
            vec![],
        ))
    }

    pub fn mock_scan(table_name: String, schema: Schema, table_oid: u64) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::MockScan {
                table_name,
                schema,
                table_oid,
            },
            vec![],
        ))
    }

    pub fn get_schema(&self) -> Schema {
        match &self.plan_type {
            // Plans with explicit schemas
            LogicalPlanType::CreateTable { schema, .. } => schema.clone(),
            LogicalPlanType::TableScan { schema, .. } => schema.clone(),
            LogicalPlanType::Project { schema, .. } => (*schema).clone(),
            LogicalPlanType::Insert { schema, .. } => schema.clone(),
            LogicalPlanType::Values { schema, .. } => schema.clone(),
            LogicalPlanType::Aggregate { schema, .. } => schema.clone(),
            LogicalPlanType::CreateIndex { .. } => self.children[0].get_schema(),
            LogicalPlanType::IndexScan { schema, .. } => schema.clone(),
            LogicalPlanType::Delete { schema, .. } => schema.clone(),
            LogicalPlanType::Update { schema, .. } => schema.clone(),

            // For joins, combine schemas from both inputs
            LogicalPlanType::NestedLoopJoin {
                left_schema,
                right_schema,
                ..
            } => {
                let mut left_columns = left_schema.clone();
                let combined_columns: &mut Vec<Column> = left_columns.get_columns_mut();
                combined_columns.extend(right_schema.get_columns().iter().cloned());
                Schema::new(combined_columns.to_vec())
            }
            LogicalPlanType::HashJoin {
                left_schema,
                right_schema,
                ..
            } => {
                let mut left_columns = left_schema.clone();
                let combined_columns: &mut Vec<Column> = left_columns.get_columns_mut();
                combined_columns.extend(right_schema.get_columns().iter().cloned());
                Schema::new(combined_columns.to_vec())
            }

            // Plans that propagate schema from child
            LogicalPlanType::Filter { .. } => self.children[0].get_schema(),

            // Plans that modify schema structure
            LogicalPlanType::Sort { schema, .. } => schema.clone(),
            LogicalPlanType::Limit { schema, .. } => schema.clone(),
            LogicalPlanType::TopN { schema, .. } => schema.clone(),
            LogicalPlanType::MockScan { .. } => self.get_schema(),
        }
    }
}

impl LogicalToPhysical for LogicalPlan {
    fn to_physical_plan(&self) -> Result<PlanNode, String> {
        match &self.plan_type {
            LogicalPlanType::CreateTable {
                schema,
                table_name,
                if_not_exists,
            } => Ok(PlanNode::CreateTable(CreateTablePlanNode::new(
                schema.clone(),
                table_name.clone(),
                *if_not_exists,
            ))),
            LogicalPlanType::CreateIndex {
                schema,
                table_name,
                index_name,
                key_attrs,
                if_not_exists,
            } => Ok(PlanNode::CreateIndex(CreateIndexPlanNode::new(
                schema.clone(),
                table_name.clone(),
                index_name.clone(),
                key_attrs.clone(),
                *if_not_exists,
            ))),
            LogicalPlanType::MockScan {
                table_name,
                schema,
                table_oid,
            } => Ok(PlanNode::MockScan(MockScanNode::new(
                schema.clone(),
                table_name.clone(),
                vec![],
            ))),
            LogicalPlanType::TableScan {
                table_name,
                schema,
                table_oid,
            } => Ok(PlanNode::SeqScan(SeqScanPlanNode::new(
                schema.clone(),
                *table_oid,
                table_name.clone(),
            ))),

            LogicalPlanType::IndexScan {
                table_name,
                table_oid,
                index_name,
                index_oid,
                schema,
            } => Ok(IndexScan(IndexScanNode::new(
                schema.clone(),
                table_name.clone(),
                *table_oid,
                index_name.clone(),
                *index_oid,
                vec![],
                self.children
                    .iter()
                    .map(|_| self.to_physical_plan())
                    .collect::<Result<Vec<PlanNode>, String>>()?,
            ))),

            LogicalPlanType::Filter { predicate } => {
                let child_plan = self.children[0].to_physical_plan()?;
                let schema = child_plan.get_output_schema().clone();
                Ok(PlanNode::Filter(FilterNode::new(
                    schema,
                    0, // table_oid will be set by executor
                    String::new(),
                    predicate.as_ref().clone(),
                    child_plan,
                )))
            }

            LogicalPlanType::Project {
                expressions,
                schema,
            } => {
                let child_plan = self.children[0].to_physical_plan()?;
                Ok(PlanNode::Projection(ProjectionNode::new(
                    schema.as_ref().clone(),
                    expressions.iter().map(|e| e.as_ref().clone()).collect(),
                    child_plan,
                )))
            }

            LogicalPlanType::Insert {
                table_name,
                schema,
                table_oid,
            } => {
                let child_plan = self.children[0].to_physical_plan()?;
                Ok(PlanNode::Insert(InsertNode::new(
                    schema.clone(),
                    *table_oid,
                    table_name.clone(),
                    vec![],
                    child_plan,
                )))
            }

            LogicalPlanType::Delete {
                table_name,
                schema,
                table_oid,
            } => Ok(PlanNode::Delete(DeleteNode::new(
                schema.clone(),
                table_name.clone(),
                table_oid.clone(),
                self.children
                    .iter()
                    .map(|child| child.to_physical_plan())
                    .collect::<Result<Vec<PlanNode>, String>>()?,
            ))),

            LogicalPlanType::Update {
                table_name,
                schema,
                table_oid,
                update_expressions,
            } => Ok(PlanNode::Update(UpdateNode::new(
                schema.clone(),
                table_name.clone(),
                *table_oid,
                update_expressions.clone(),
                self.children
                    .iter()
                    .map(|_| self.to_physical_plan())
                    .collect::<Result<Vec<PlanNode>, String>>()?,
            ))),

            LogicalPlanType::Values { rows, schema } => {
                let physical_rows: Vec<Vec<Arc<Expression>>> = rows
                    .iter()
                    .map(|row| row.iter().map(Arc::clone).collect())
                    .collect();

                Ok(PlanNode::Values(
                    ValuesNode::new(schema.clone(), physical_rows, PlanNode::Empty).unwrap(),
                ))
            }

            LogicalPlanType::Aggregate {
                group_by,
                aggregates,
                schema,
            } => {
                let child_plan = self.children[0].to_physical_plan()?;
                let agg_types = vec![AggregationType::CountStar; aggregates.len()];
                Ok(PlanNode::Aggregation(AggregationPlanNode::new(
                    schema.clone(),
                    vec![child_plan],
                    group_by.clone(),
                    aggregates.clone(),
                    agg_types,
                )))
            }

            LogicalPlanType::NestedLoopJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                let left_child = self.children[0].to_physical_plan()?;
                let right_child = self.children[1].to_physical_plan()?;

                // let output_schema = Schema::merge(left_schema, right_schema);
                // Ok(PlanNode::NestedLoopJoin(NestedLoopJoinNode::new(
                //     output_schema,
                //     predicate.clone(),
                //     join_type.clone(),
                //     vec![left_child, right_child],
                // )))
                Err("NestedLoopJoin not implemented".to_string())
            }

            LogicalPlanType::HashJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                // Extract join key expressions from the predicate
                // let (left_keys, right_keys) = extract_join_keys(predicate)?;
                //
                // let output_schema = Schema::merge(left_schema, right_schema);
                // Ok(PlanNode::HashJoin(HashJoinNode::new(
                //     output_schema,
                //     left_keys,
                //     right_keys,
                //     join(TableFactor::Table {
                //         name: ObjectName(vec![]),
                //         alias: None,
                //         args: None,
                //         with_hints: vec![],
                //         version: None,
                //         with_ordinality: false,
                //         partitions: vec![],
                //     }),
                //     self.children.iter().map(|child| child.to_physical_plan()).collect::<Result<Vec<PlanNode>, String>>()?,
                // )))
                Err("HashJoin not implemented".to_string())
            }

            LogicalPlanType::Sort {
                sort_expressions,
                schema,
            } => Ok(PlanNode::Sort(SortNode::new(
                schema.clone(),
                sort_expressions.clone(),
                self.children
                    .iter()
                    .map(|child| child.to_physical_plan())
                    .collect::<Result<Vec<PlanNode>, String>>()?,
            ))),

            LogicalPlanType::Limit { limit, schema } => Ok(PlanNode::Limit(LimitNode::new(
                limit.clone(),
                schema.clone(),
                self.children
                    .iter()
                    .map(|child| child.to_physical_plan())
                    .collect::<Result<Vec<PlanNode>, String>>()?,
            ))),

            LogicalPlanType::TopN {
                k,
                sort_expressions,
                schema,
            } => Ok(PlanNode::TopN(TopNNode::new(
                schema.clone(),
                sort_expressions.clone(),
                k.clone(),
                self.children
                    .iter()
                    .map(|child| child.to_physical_plan())
                    .collect::<Result<Vec<PlanNode>, String>>()?,
            ))),
        }
    }
}

/// Helper function to extract join keys from a join predicate
fn extract_join_keys(
    predicate: &Arc<Expression>,
) -> Result<(Vec<Arc<Expression>>, Vec<Arc<Expression>>), String> {
    // This is a simplified implementation - in practice, you'd need to:
    // 1. Parse the predicate to identify equijoin conditions
    // 2. Separate expressions that reference only left table columns
    // 3. Separate expressions that reference only right table columns
    // 4. Ensure they form valid join keys

    // For now, we'll return empty vectors
    Ok((vec![], vec![]))
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
    use std::collections::HashMap;

    // Test fixture that encapsulates common test setup
    struct TestFixture {
        catalog: Arc<RwLock<Catalog>>,
        planner: QueryPlanner,
        _db_file: String,
        _log_file: String,
        _disk_manager: Arc<FileDiskManager>,
    }

    impl TestFixture {
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
                        create_table.get_table_name(),
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
                assert_eq!(column.get_type(), *type_id);
            }
        }
    }

    impl Drop for TestFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self._db_file);
            let _ = std::fs::remove_file(&self._log_file);
        }
    }

    mod create_table_tests {
        use super::*;

        #[test]
        fn test_create_simple_table() {
            let mut fixture = TestFixture::new("create_simple_table");

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
            let mut fixture = TestFixture::new("create_table_if_not_exists");

            // First creation should succeed
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

        // Helper function to set up a test table
        fn setup_test_table(fixture: &mut TestFixture) {
            fixture
                .create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false)
                .unwrap();
        }

        #[test]
        fn test_simple_select() {
            let mut fixture = TestFixture::new("simple_select");
            setup_test_table(&mut fixture);

            let select_sql = "SELECT * FROM users";
            let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

            match &plan.plan_type {
                LogicalPlanType::TableScan {
                    table_name, schema, ..
                } => {
                    assert_eq!(table_name, "users");
                    assert_eq!(schema.get_column_count(), 3);
                }
                _ => panic!("Expected TableScan plan node"),
            }
        }

        #[test]
        fn test_select_with_filter() {
            let mut fixture = TestFixture::new("select_with_filter");
            setup_test_table(&mut fixture);

            let select_sql = "SELECT * FROM users WHERE age > 25";
            let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

            match &plan.plan_type {
                LogicalPlanType::Filter { predicate } => match predicate.as_ref() {
                    Expression::Comparison(comp) => {
                        assert_eq!(comp.get_comp_type(), ComparisonType::GreaterThan);
                    }
                    _ => panic!("Expected Comparison expression"),
                },
                _ => panic!("Expected Filter plan node"),
            }
        }
    }

    mod insert_tests {
        use super::*;

        fn setup_test_table(fixture: &mut TestFixture) {
            fixture
                .create_table("users", "id INTEGER, name VARCHAR(255)", false)
                .unwrap();
        }

        #[test]
        fn test_simple_insert() {
            let mut fixture = TestFixture::new("simple_insert");
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
}
