use crate::catalog::catalog::Catalog;
use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::config::{IndexOidT, TableOidT};
use crate::sql::execution::expressions::abstract_expression::Expression;
use crate::sql::execution::expressions::abstract_expression::ExpressionOps;
use crate::sql::execution::expressions::aggregate_expression::{AggregateExpression, AggregationType};
use crate::sql::execution::expressions::arithmetic_expression::{ArithmeticExpression, ArithmeticOp};
use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::sql::execution::expressions::comparison_expression::{ComparisonExpression, ComparisonType};
use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
use crate::sql::execution::expressions::logic_expression::{LogicExpression, LogicType};
use crate::sql::execution::plans::abstract_plan::PlanNode;
use crate::sql::execution::plans::aggregation_plan::AggregationPlanNode;
use crate::sql::execution::plans::create_index_plan::CreateIndexPlanNode;
use crate::sql::execution::plans::create_table_plan::CreateTablePlanNode;
use crate::sql::execution::plans::delete_plan::DeleteNode;
use crate::sql::execution::plans::filter_plan::FilterNode;
use crate::sql::execution::plans::hash_join_plan::HashJoinNode;
use crate::sql::execution::plans::index_scan_plan::IndexScanNode;
use crate::sql::execution::plans::insert_plan::InsertNode;
use crate::sql::execution::plans::limit_plan::LimitNode;
use crate::sql::execution::plans::mock_scan_plan::MockScanNode;
use crate::sql::execution::plans::nested_index_join_plan::NestedIndexJoinNode;
use crate::sql::execution::plans::nested_loop_join_plan::NestedLoopJoinNode;
use crate::sql::execution::plans::projection_plan::ProjectionNode;
use crate::sql::execution::plans::seq_scan_plan::SeqScanPlanNode;
use crate::sql::execution::plans::sort_plan::SortNode;
use crate::sql::execution::plans::topn_per_group_plan::TopNPerGroupNode;
use crate::sql::execution::plans::topn_plan::TopNNode;
use crate::sql::execution::plans::update_plan::UpdateNode;
use crate::sql::execution::plans::values_plan::ValuesNode;
use crate::sql::execution::plans::window_plan::WindowFunction;
use crate::sql::execution::plans::window_plan::WindowFunctionType;
use crate::sql::execution::plans::window_plan::WindowNode;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
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
        table_oid: TableOidT,
    },
    TableScan {
        table_name: String,
        schema: Schema,
        table_oid: TableOidT,
    },
    IndexScan {
        table_name: String,
        table_oid: TableOidT,
        index_name: String,
        index_oid: IndexOidT,
        schema: Schema,
        predicate_keys: Vec<Arc<Expression>>,
    },
    Filter {
        schema: Schema,
        table_oid: TableOidT,
        table_name: String,
        predicate: Arc<Expression>,
    },
    Projection {
        expressions: Vec<Arc<Expression>>,
        schema: Schema,
    },
    Insert {
        table_name: String,
        schema: Schema,
        table_oid: TableOidT,
    },
    Delete {
        table_name: String,
        schema: Schema,
        table_oid: TableOidT,
    },
    Update {
        table_name: String,
        schema: Schema,
        table_oid: TableOidT,
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
    NestedIndexJoin {
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
    TopNPerGroup {
        k: usize,
        sort_expressions: Vec<Arc<Expression>>,
        groups: Vec<Arc<Expression>>,
        schema: Schema,
    },
    Window {
        group_by: Vec<Arc<Expression>>,
        aggregates: Vec<Arc<Expression>>,
        partitions: Vec<Arc<Expression>>,
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

    pub fn create_logical_plan_from_statement(
        &mut self,
        stmt: &Statement,
    ) -> Result<Box<LogicalPlan>, String> {
        match stmt {
            Statement::Query(query) => self.plan_query_logical(query),
            Statement::Insert(stmt) => self.plan_insert_logical(stmt),
            Statement::CreateTable(stmt) => self.plan_create_table_logical(stmt),
            Statement::CreateIndex(stmt) => self.plan_create_index_logical(stmt),
            _ => Err(format!("Unsupported statement type: {:?}", stmt)),
        }
    }

    pub fn create_logical_plan(&mut self, sql: &str) -> Result<Box<LogicalPlan>, String> {
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;

        if ast.len() != 1 {
            return Err("Only single SQL statement is supported".to_string());
        }

        self.create_logical_plan_from_statement(&ast[0])
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
        let mut current_plan = LogicalPlan::table_scan(table_name.clone(), schema.clone(), table_oid);

        // Add filter if WHERE clause exists
        if let Some(where_clause) = &select.selection {
            let predicate = Arc::new(self.parse_expression(where_clause, &schema)?);
            current_plan = LogicalPlan::filter(
                schema.clone(),
                table_name,
                table_oid,
                predicate,
                current_plan,
            );
        }

        // Handle aggregation if needed
        let has_group_by = match &select.group_by {
            GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
            GroupByExpr::All(_) => true,
        };
        let has_aggregates = self.has_aggregate_functions(&select.projection);

        if has_group_by || has_aggregates {
            // Parse group by expressions
            let group_by_exprs: Vec<_> = self.determine_group_by_expressions(select, &schema, has_group_by)?
                .into_iter()
                .map(Arc::new)
                .collect();

            // Parse aggregates
            let (agg_exprs, _) = self.parse_aggregates(&select.projection, &schema)?;

            // Create output schema for aggregation
            let agg_schema = self.create_aggregation_output_schema(
                &group_by_exprs.iter().map(|e| e.as_ref()).collect::<Vec<_>>(),
                &agg_exprs,
                has_group_by,
            );

            current_plan = LogicalPlan::aggregate(
                group_by_exprs,
                agg_exprs,
                agg_schema,
                current_plan,
            );
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
                        AggregationType::CountStar,
                        Expression::Constant(ConstantExpression::new(
                            Value::new(1),
                            Column::new("count", TypeId::BigInt), // Changed to BigInt
                            vec![],
                        )),
                    ),
                    FunctionArguments::List(list) => {
                        if list.args.is_empty() {
                            (
                                AggregationType::CountStar,
                                Expression::Constant(ConstantExpression::new(
                                    Value::new(1),
                                    Column::new("count", TypeId::BigInt), // Changed to BigInt
                                    vec![],
                                )),
                            )
                        } else {
                            match &list.args[0] {
                                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => (
                                    AggregationType::CountStar,
                                    Expression::Constant(ConstantExpression::new(
                                        Value::new(1),
                                        Column::new("count", TypeId::BigInt), // Changed to BigInt
                                        vec![],
                                    )),
                                ),
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                                    let inner_expr = self.parse_expression(expr, schema)?;
                                    (AggregationType::Count, inner_expr)
                                }
                                _ => return Err("Unsupported COUNT argument".to_string()),
                            }
                        }
                    }
                    _ => return Err("Invalid COUNT function arguments".to_string()),
                }
            }
            "SUM" | "MIN" | "MAX" | "AVG" => {
                let agg_type = match func_name.as_str() {
                    "SUM" => AggregationType::Sum,
                    "MIN" => AggregationType::Min,
                    "MAX" => AggregationType::Max,
                    "AVG" => AggregationType::Avg,
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
                                return Err(
                                    "ExprNamed arguments not supported in aggregate functions"
                                        .to_string(),
                                );
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

        Ok((
            Expression::Aggregate(AggregateExpression::new(
                agg_type.clone(),
                Arc::new(arg_expr),
                vec![],
            )),
            agg_type,
        ))
    }

    fn parse_aggregates(
        &self,
        projection: &[SelectItem],
        schema: &Schema,
    ) -> Result<(Vec<Arc<Expression>>, Vec<AggregationType>), String> {
        let mut agg_exprs = Vec::new();
        let mut agg_types = Vec::new();

        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    if let Expr::Function(func) = expr {
                        let (agg_expr, agg_type) = self.parse_aggregate_function(&func, schema)?;
                        agg_exprs.push(Arc::new(agg_expr));
                        agg_types.push(agg_type);
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    if let Expr::Function(func) = expr {
                        let (agg_expr, agg_type) = self.parse_aggregate_function(&func, schema)?;
                        let mut agg = AggregateExpression::new(
                            agg_type.clone(),
                            Arc::from(agg_expr),
                            vec![],
                        );

                        // Create a new column with the alias name but keep the original type
                        let ret_type = Column::new(&alias.value, agg.get_return_type().get_type());
                        agg = agg.with_return_type(ret_type);

                        agg_exprs.push(Arc::new(Expression::Aggregate(agg)));
                        agg_types.push(agg_type);
                    }
                }
                SelectItem::QualifiedWildcard(_, _) => {}
                SelectItem::Wildcard(_) => {}
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
                        _ => expressions.push(self.parse_expression(expr, schema)?),
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => match expr {
                    _ => {
                        let expr = self.parse_expression(expr, schema)?;
                        if let Expression::ColumnRef(col_ref) = expr {
                            expressions.push(Expression::ColumnRef(ColumnRefExpression::new(
                                col_ref.get_column_index(),
                                col_ref.get_column_index(),
                                col_ref.get_return_type().with_name(&alias.value),
                                col_ref.get_children().clone(),
                            )));
                        } else {
                            expressions.push(expr);
                        }
                    }
                },
                SelectItem::Wildcard(_) => {
                    // Add all columns from the schema
                    for i in 0..schema.get_column_count() {
                        let column = schema.get_column(i as usize).unwrap();
                        expressions.push(Expression::ColumnRef(ColumnRefExpression::new(
                            0,
                            i as usize,
                            column.clone(),
                            vec![],
                        )));
                    }
                }
                _ => return Err("Unsupported projection type".to_string()),
            }
        }

        Ok(expressions)
    }

    fn validate_aggregation_plan(
        &self,
        plan: &Box<LogicalPlan>,
        group_by: &[Arc<Expression>],
        aggregates: &[Arc<Expression>],
    ) -> Result<(), String> {
        match &plan.plan_type {
            LogicalPlanType::Aggregate { schema, .. } => {
                // Count unique columns - don't double count columns that appear in both group by and aggregates
                let mut unique_cols = std::collections::HashSet::new();

                // Add group by columns
                for expr in group_by {
                    let _ = unique_cols.insert(expr.get_return_type().get_name().to_string());
                }

                // Add aggregate columns
                for expr in aggregates {
                    match expr.as_ref() {
                        Expression::Aggregate(agg) => {
                            let name = format!(
                                "{}({})",
                                agg.get_agg_type().to_string(),
                                agg.get_arg().get_return_type().get_name()
                            );
                            let _ = unique_cols.insert(name);
                        }
                        Expression::ColumnRef(col_ref) => {
                            // Only add if not already in group by
                            if !group_by.iter().any(|g| {
                                matches!(g.as_ref(), Expression::ColumnRef(g_ref) if g_ref.get_column_index() == col_ref.get_column_index())
                            }) {
                                let _ = unique_cols.insert(col_ref.get_return_type().get_name().to_string());
                            }
                        }
                        _ => {
                            let _ = unique_cols.insert(expr.get_return_type().get_name().to_string());
                        }
                    }
                }

                // Validate schema column count matches unique columns
                let expected_cols = unique_cols.len();
                if schema.get_column_count() as usize != expected_cols {
                    return Err(format!(
                        "Schema column count mismatch: expected {}, got {}\nExpected columns: {:?}\nActual columns: {:?}",
                        expected_cols,
                        schema.get_column_count(),
                        unique_cols,
                        schema.get_columns().iter().map(|c| c.get_name()).collect::<Vec<_>>()
                    ));
                }

                // Validate all expected columns are present in schema
                for col_name in unique_cols {
                    if schema.get_column_index(&col_name).is_none() {
                        return Err(format!("Column '{}' missing from schema", col_name));
                    }
                }

                Ok(())
            }
            _ => Err("Expected Aggregate plan type".to_string()),
        }
    }

    fn create_aggregation_output_schema(
        &self,
        group_bys: &[&Expression],
        aggregates: &[Arc<Expression>],
        has_group_by: bool,
    ) -> Schema {
        let mut columns = Vec::new();
        let mut seen_columns = std::collections::HashSet::new();

        // Add group by columns first if we have them
        if has_group_by {
            for expr in group_bys {
                let col_name = expr.get_return_type().get_name().to_string();
                if seen_columns.insert(col_name.clone()) {
                    columns.push(expr.get_return_type().clone());
                }
            }
        }

        // Add aggregate columns
        for agg_expr in aggregates {
            match agg_expr.as_ref() {
                Expression::Aggregate(agg) => {
                    let col_name = match agg.get_agg_type() {
                        AggregationType::CountStar => "COUNT(*)".to_string(),
                        _ => format!(
                            "{}({})",
                            agg.get_agg_type().to_string(),
                            agg.get_arg().get_return_type().get_name()
                        ),
                    };

                    if seen_columns.insert(col_name.clone()) {
                        let col_type = match agg.get_agg_type() {
                            AggregationType::Count | AggregationType::CountStar => TypeId::BigInt,
                            AggregationType::Sum => agg.get_arg().get_return_type().get_type(),
                            AggregationType::Avg => TypeId::Decimal,
                            AggregationType::Min | AggregationType::Max => {
                                agg.get_arg().get_return_type().get_type()
                            }
                        };
                        columns.push(Column::new(&col_name, col_type));
                    }
                }
                _ => {
                    let col_name = agg_expr.get_return_type().get_name().to_string();
                    if seen_columns.insert(col_name.clone()) {
                        columns.push(agg_expr.get_return_type().clone());
                    }
                }
            }
        }

        if self.log_detailed {
            println!(
                "Created aggregation schema with columns: {:?}",
                columns.iter().map(|c| c.get_name()).collect::<Vec<_>>()
            );
        }

        Schema::new(columns)
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

    fn add_projection_logical(
        &self,
        select: &Select,
        input: Box<LogicalPlan>,
        schema: &Schema,
    ) -> Result<Box<LogicalPlan>, String> {
        let mut projection_exprs = Vec::new();
        let mut output_columns = Vec::new();

        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    if let Expr::Function(func) = expr {
                        // For aggregate functions, use the formatted name
                        let (expr, agg_type) = self.parse_aggregate_function(&func, schema)?;
                        let col_name = format!(
                            "{}({})",
                            agg_type.to_string(),
                            expr.get_return_type().get_name()
                        );
                        let col_ref = Expression::ColumnRef(ColumnRefExpression::new(
                            0,
                            projection_exprs.len(),
                            Column::new(&col_name, expr.get_return_type().get_type()),
                            vec![],
                        ));
                        projection_exprs.push(Arc::new(col_ref));
                        output_columns.push(Column::new(&col_name, expr.get_return_type().get_type()));
                    } else {
                        let parsed_expr = self.parse_expression(expr, schema)?;
                        projection_exprs.push(Arc::new(parsed_expr.clone()));
                        output_columns.push(parsed_expr.get_return_type().clone());
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    if let Expr::Function(func) = expr {
                        // For aliased aggregate functions, use the alias name
                        let (expr, _) = self.parse_aggregate_function(&func, schema)?;
                        let col_ref = Expression::ColumnRef(ColumnRefExpression::new(
                            0,
                            projection_exprs.len(),
                            Column::new(&alias.value, expr.get_return_type().get_type()),
                            vec![],
                        ));
                        projection_exprs.push(Arc::new(col_ref));
                        output_columns.push(Column::new(&alias.value, expr.get_return_type().get_type()));
                    } else {
                        let parsed_expr = self.parse_expression(expr, schema)?;
                        let col_ref = Expression::ColumnRef(ColumnRefExpression::new(
                            0,
                            projection_exprs.len(),
                            Column::new(&alias.value, parsed_expr.get_return_type().get_type()),
                            vec![],
                        ));
                        projection_exprs.push(Arc::new(col_ref));
                        output_columns.push(Column::new(&alias.value, parsed_expr.get_return_type().get_type()));
                    }
                }
                SelectItem::Wildcard(_) => {
                    // For wildcards, include all columns from input
                    for i in 0..schema.get_column_count() {
                        let col = schema.get_column(i as usize).unwrap();
                        let col_ref = Expression::ColumnRef(ColumnRefExpression::new(
                            0,
                            i as usize,
                            col.clone(),
                            vec![],
                        ));
                        projection_exprs.push(Arc::new(col_ref));
                        output_columns.push(col.clone());
                    }
                }
                _ => return Err("Unsupported projection item".to_string()),
            }
        }

        Ok(LogicalPlan::project(
            projection_exprs,
            Schema::new(output_columns),
            input,
        ))
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
                Expression::Aggregate(agg_expr) => {
                    agg_expr.get_return_type().clone()
                    // let input_name = agg_expr.get_return_type().get_return_type().get_name().to_string();
                    // let input_type = agg_expr.get_return_type().get_type();
                    // Handle aggregate function return types
                    // match agg_expr.get_agg_type() {
                    //     AggregationType::CountStar => {
                    //         Column::new(&format!("count_all"), TypeId::Integer)
                    //     }
                    //     AggregationType::Count => {
                    //         Column::new(&format!("count_{}", input_name), TypeId::Integer)
                    //     }
                    //     AggregationType::Sum => {
                    //         // For SUM, use the same type as the input column
                    //         Column::new(&format!("sum_{}", input_name), input_type)
                    //     }
                    //     AggregationType::Min => {
                    //         // For SUM, use the same type as the input column
                    //         Column::new(&format!("min_{}", input_name), input_type)
                    //     }
                    //     AggregationType::Max => {
                    //         // For SUM, use the same type as the input column
                    //         Column::new(&format!("max_{}", input_name), input_type)
                    //     }
                    //     AggregationType::Avg => {
                    //         // For SUM, use the same type as the input column
                    //         Column::new(&format!("avg_{}", input_name), input_type)
                    //     }
                    // }
                }
                _ => Column::new(&format!("expr_{}", idx), TypeId::Integer),
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
        match self {
            _ => write!(f, "{:#?}", self),
        }
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
    pub fn explain(&self, depth: usize) -> String {
        let indent_str = "  ".repeat(depth);
        let mut result = String::new();

        match &self.plan_type {
            LogicalPlanType::CreateTable {
                schema,
                table_name,
                if_not_exists,
            } => {
                result.push_str(&format!("{}→ CreateTable: {}\n", indent_str, table_name));
                if *if_not_exists {
                    result.push_str(&format!("{}   IF NOT EXISTS\n", indent_str));
                }
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::CreateIndex {
                schema,
                table_name,
                index_name,
                key_attrs,
                if_not_exists,
            } => {
                result.push_str(&format!(
                    "{}→ CreateIndex: {} on {}\n",
                    indent_str, index_name, table_name
                ));
                result.push_str(&format!("{}   Key Columns: {:?}\n", indent_str, key_attrs));
                if *if_not_exists {
                    result.push_str(&format!("{}   IF NOT EXISTS\n", indent_str));
                }
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::MockScan {
                table_name, schema, ..
            } => {
                result.push_str(&format!("{}→ MockScan: {}\n", indent_str, table_name));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::TableScan {
                table_name, schema, ..
            } => {
                result.push_str(&format!("{}→ TableScan: {}\n", indent_str, table_name));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::IndexScan {
                table_name,
                table_oid,
                index_name,
                index_oid,
                schema,
                predicate_keys,
            } => {
                result.push_str(&format!(
                    "{}→ IndexScan: {} using {}\n",
                    indent_str, table_name, index_name
                ));
                result.push_str(&format!("{}   Predicate Keys: [", indent_str));
                for (i, key) in predicate_keys.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&key.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::Filter {
                schema,
                table_oid,
                table_name,
                predicate,
                ..
            } => {
                result.push_str(&format!("{}→ Filter\n", indent_str));
                result.push_str(&format!("{}   Predicate: {}\n", indent_str, predicate));
                result.push_str(&format!("{}   Table: {}\n", indent_str, table_name));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::Projection {
                expressions,
                schema,
            } => {
                result.push_str(&format!("{}→ Projection\n", indent_str));
                result.push_str(&format!("{}   Expressions: [", indent_str));
                for (i, expr) in expressions.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&expr.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::Limit { limit, schema } => {
                result.push_str(&format!("{}→ Limit: {}\n", indent_str, limit));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::Insert {
                table_name, schema, ..
            } => {
                result.push_str(&format!("{}→ Insert\n", indent_str));
                result.push_str(&format!("{}   Table: {}\n", indent_str, table_name));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::Delete {
                table_name, schema, ..
            } => {
                result.push_str(&format!("{}→ Delete\n", indent_str));
                result.push_str(&format!("{}   Table: {}\n", indent_str, table_name));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::Update {
                table_name,
                schema,
                update_expressions,
                ..
            } => {
                result.push_str(&format!("{}→ Update\n", indent_str));
                result.push_str(&format!("{}   Table: {}\n", indent_str, table_name));
                result.push_str(&format!("{}   Target Expressions: [", indent_str));
                for (i, expr) in update_expressions.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&expr.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::Values { rows, schema } => {
                result.push_str(&format!("{}→ Values: {} rows\n", indent_str, rows.len()));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::Aggregate {
                group_by,
                aggregates,
                schema,
            } => {
                result.push_str(&format!("{}→ Aggregate\n", indent_str));
                if !group_by.is_empty() {
                    result.push_str(&format!("{}   Group By: [", indent_str));
                    for (i, expr) in group_by.iter().enumerate() {
                        if i > 0 {
                            result.push_str(", ");
                        }
                        result.push_str(&expr.to_string());
                    }
                    result.push_str("]\n");
                }
                result.push_str(&format!("{}   Aggregates: [", indent_str));
                for (i, expr) in aggregates.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&expr.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::NestedLoopJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                result.push_str(&format!("{}→ NestedLoopJoin\n", indent_str));
                result.push_str(&format!("{}   Join Type: {:?}\n", indent_str, join_type));
                result.push_str(&format!("{}   Predicate: {}\n", indent_str, predicate));
                result.push_str(&format!("{}   Left Schema: {}\n", indent_str, left_schema));
                result.push_str(&format!(
                    "{}   Right Schema: {}\n",
                    indent_str, right_schema
                ));
            }
            LogicalPlanType::HashJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                result.push_str(&format!("{}→ HashJoin\n", indent_str));
                result.push_str(&format!("{}   Join Type: {:?}\n", indent_str, join_type));
                result.push_str(&format!("{}   Predicate: {}\n", indent_str, predicate));
                result.push_str(&format!("{}   Left Schema: {}\n", indent_str, left_schema));
                result.push_str(&format!(
                    "{}   Right Schema: {}\n",
                    indent_str, right_schema
                ));
            }
            LogicalPlanType::Sort {
                sort_expressions,
                schema,
            } => {
                result.push_str(&format!("{}→ Sort\n", indent_str));
                result.push_str(&format!("{}   Order By: [", indent_str));
                for (i, expr) in sort_expressions.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&expr.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::Limit { limit, schema } => {
                result.push_str(&format!("{}→ Limit: {}\n", indent_str, limit));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::TopN {
                k,
                sort_expressions,
                schema,
            } => {
                result.push_str(&format!("{}→ TopN: {}\n", indent_str, k));
                result.push_str(&format!("{}   Order By: [", indent_str));
                for (i, expr) in sort_expressions.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&expr.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::MockScan {
                table_name, schema, ..
            } => {
                result.push_str(&format!("{}→ MockScan: {}\n", indent_str, table_name));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::NestedIndexJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                result.push_str(&format!("{}→ NestedIndexJoin\n", indent_str));
                result.push_str(&format!("{}   Join Type: {:?}\n", indent_str, join_type));
                result.push_str(&format!("{}   Predicate: {}\n", indent_str, predicate));
                result.push_str(&format!("{}   Left Schema: {}\n", indent_str, left_schema));
                result.push_str(&format!(
                    "{}   Right Schema: {}\n",
                    indent_str, right_schema
                ));
            }
            LogicalPlanType::TopNPerGroup {
                k,
                sort_expressions,
                groups,
                schema,
            } => {
                result.push_str(&format!("{}→ TopNPerGroup: {}\n", indent_str, k));
                result.push_str(&format!("{}   Order By: [", indent_str));
                for (i, expr) in sort_expressions.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&expr.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Group By: [", indent_str));
                for (i, expr) in groups.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&expr.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
            LogicalPlanType::Window {
                group_by,
                aggregates,
                partitions,
                schema,
            } => {
                result.push_str(&format!("{}→ Window\n", indent_str));
                if !group_by.is_empty() {
                    result.push_str(&format!("{}   Group By: [", indent_str));
                    for (i, expr) in group_by.iter().enumerate() {
                        if i > 0 {
                            result.push_str(", ");
                        }
                        result.push_str(&expr.to_string());
                    }
                    result.push_str("]\n");
                }
                result.push_str(&format!("{}   Window Functions: [", indent_str));
                for (i, expr) in aggregates.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&expr.to_string());
                }
                result.push_str("]\n");
                if !partitions.is_empty() {
                    result.push_str(&format!("{}   Partition By: [", indent_str));
                    for (i, expr) in partitions.iter().enumerate() {
                        if i > 0 {
                            result.push_str(", ");
                        }
                        result.push_str(&expr.to_string());
                    }
                    result.push_str("]\n");
                }
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            }
        }

        // Add children recursively
        for child in &self.children {
            result.push_str(&child.explain(depth + 1));
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

    pub fn filter(
        schema: Schema,
        table_name: String,
        table_oid: TableOidT,
        predicate: Arc<Expression>,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Filter {
                schema,
                table_oid,
                table_name,
                predicate,
            },
            vec![input],
        ))
    }

    pub fn project(
        expressions: Vec<Arc<Expression>>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Projection {
                expressions,
                schema,
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
        predicate_keys: Vec<Arc<Expression>>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::IndexScan {
                table_name,
                table_oid,
                index_name,
                index_oid,
                schema,
                predicate_keys,
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

    pub fn delete(
        table_name: String,
        schema: Schema,
        table_oid: u64,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Delete {
                table_name,
                schema,
                table_oid,
            },
            vec![input],
        ))
    }

    pub fn update(
        table_name: String,
        schema: Schema,
        table_oid: u64,
        update_expressions: Vec<Arc<Expression>>,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Update {
                table_name,
                schema,
                table_oid,
                update_expressions,
            },
            vec![input],
        ))
    }

    pub fn nested_loop_join(
        left_schema: Schema,
        right_schema: Schema,
        predicate: Arc<Expression>,
        join_type: JoinOperator,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::NestedLoopJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            },
            vec![left, right],
        ))
    }

    pub fn hash_join(
        left_schema: Schema,
        right_schema: Schema,
        predicate: Arc<Expression>,
        join_type: JoinOperator,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::HashJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            },
            vec![left, right],
        ))
    }

    pub fn sort(
        sort_expressions: Vec<Arc<Expression>>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Sort {
                sort_expressions,
                schema,
            },
            vec![input],
        ))
    }

    pub fn limit(limit: usize, schema: Schema, input: Box<LogicalPlan>) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Limit { limit, schema },
            vec![input],
        ))
    }

    pub fn top_n(
        k: usize,
        sort_expressions: Vec<Arc<Expression>>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::TopN {
                k,
                sort_expressions,
                schema,
            },
            vec![input],
        ))
    }

    pub fn get_schema(&self) -> Schema {
        match &self.plan_type {
            // Plans with explicit schemas
            LogicalPlanType::CreateTable { schema, .. } => schema.clone(),
            LogicalPlanType::TableScan { schema, .. } => schema.clone(),
            LogicalPlanType::Projection { schema, .. } => (*schema).clone(),
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
            LogicalPlanType::MockScan { schema, .. } => schema.clone(),
            LogicalPlanType::NestedIndexJoin {
                left_schema,
                right_schema,
                ..
            } => {
                let mut left_columns = left_schema.clone();
                let combined_columns: &mut Vec<Column> = left_columns.get_columns_mut();
                combined_columns.extend(right_schema.get_columns().iter().cloned());
                Schema::new(combined_columns.to_vec())
            }
            LogicalPlanType::TopNPerGroup { schema, .. } => schema.clone(),
            LogicalPlanType::Window { schema, .. } => schema.clone(),
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
                predicate_keys,
            } => Ok(PlanNode::IndexScan(IndexScanNode::new(
                schema.clone(),
                table_name.to_string(),
                *table_oid,
                index_name.to_string(),
                *index_oid,
                predicate_keys.clone(),
            ))),

            LogicalPlanType::Filter {
                schema,
                table_oid,
                table_name,
                predicate,
            } => {
                let children = self
                    .children
                    .iter()
                    .map(|child| child.to_physical_plan().unwrap())
                    .collect::<Vec<PlanNode>>();
                Ok(PlanNode::Filter(FilterNode::new(
                    schema.clone(),
                    *table_oid,
                    table_name.to_string(),
                    predicate.clone(),
                    children,
                )))
            }

            LogicalPlanType::Projection {
                expressions,
                schema,
            } => {
                let children = self
                    .children
                    .iter()
                    .map(|child| child.to_physical_plan().unwrap())
                    .collect::<Vec<PlanNode>>();
                Ok(PlanNode::Projection(ProjectionNode::new(
                    schema.clone(),
                    expressions.clone(),
                    children,
                )))
            }

            LogicalPlanType::Insert {
                table_name,
                schema,
                table_oid,
            } => {
                let children = self
                    .children
                    .iter()
                    .map(|child| child.to_physical_plan().unwrap())
                    .collect::<Vec<PlanNode>>();
                Ok(PlanNode::Insert(InsertNode::new(
                    schema.clone(),
                    *table_oid,
                    table_name.to_string(),
                    vec![],
                    children,
                )))
            }

            LogicalPlanType::Delete {
                table_name,
                schema,
                table_oid,
            } => {
                let children = self
                    .children
                    .iter()
                    .map(|child| child.to_physical_plan().unwrap())
                    .collect::<Vec<PlanNode>>();
                Ok(PlanNode::Delete(DeleteNode::new(
                    schema.clone(),
                    table_name.clone(),
                    *table_oid,
                    children,
                )))
            }

            LogicalPlanType::Update {
                table_name,
                schema,
                table_oid,
                update_expressions,
            } => {
                let children = self
                    .children
                    .iter()
                    .map(|child| child.to_physical_plan().unwrap())
                    .collect::<Vec<PlanNode>>();
                Ok(PlanNode::Update(UpdateNode::new(
                    schema.clone(),
                    table_name.clone(),
                    *table_oid,
                    update_expressions.clone(),
                    children,
                )))
            }

            LogicalPlanType::Values { rows, schema } => {
                let children = self
                    .children
                    .iter()
                    .map(|child| child.to_physical_plan().unwrap())
                    .collect::<Vec<PlanNode>>();
                Ok(PlanNode::Values(ValuesNode::new(
                    schema.clone(),
                    rows.clone(),
                    children,
                )))
            }

            LogicalPlanType::Aggregate { group_by, aggregates, schema } => {
                let children = self.children.iter()
                    .map(|child| child.to_physical_plan())
                    .collect::<Result<Vec<PlanNode>, String>>()?;

                // Filter out duplicate expressions
                let agg_exprs = aggregates.iter()
                    .filter(|expr| {
                        match expr.as_ref() {
                            Expression::ColumnRef(col_ref) => {
                                !group_by.iter().any(|g| match g.as_ref() {
                                    Expression::ColumnRef(g_ref) => {
                                        g_ref.get_column_index() == col_ref.get_column_index()
                                    }
                                    _ => false
                                })
                            }
                            _ => true
                        }
                    })
                    .cloned()
                    .collect::<Vec<_>>();

                Ok(PlanNode::Aggregation(AggregationPlanNode::new(
                    children,
                    group_by.clone(),
                    agg_exprs,
                )))
            }

            LogicalPlanType::NestedLoopJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                let left = self.children[0].to_physical_plan()?;
                let right = self.children[1].to_physical_plan()?;

                // Extract join key expressions from the predicate
                let (left_keys, right_keys) = extract_join_keys(predicate)?;

                Ok(PlanNode::NestedLoopJoin(NestedLoopJoinNode::new(
                    left_schema.clone(),
                    right_schema.clone(),
                    predicate.clone(),
                    join_type.clone(),
                    left_keys,
                    right_keys,
                    vec![left, right],
                )))
            }

            LogicalPlanType::HashJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                let left = self.children[0].to_physical_plan()?;
                let right = self.children[1].to_physical_plan()?;

                // Extract join key expressions from the predicate
                let (left_keys, right_keys) = extract_join_keys(predicate)?;

                Ok(PlanNode::HashJoin(HashJoinNode::new(
                    left_schema.clone(),
                    right_schema.clone(),
                    predicate.clone(),
                    join_type.clone(),
                    left_keys,
                    right_keys,
                    vec![left, right],
                )))
            }

            LogicalPlanType::Sort {
                sort_expressions,
                schema,
            } => {
                let child = self.children[0].to_physical_plan()?;
                Ok(PlanNode::Sort(SortNode::new(
                    schema.clone(),
                    sort_expressions.clone(),
                    vec![child],
                )))
            }

            LogicalPlanType::Limit { limit, schema } => {
                let child = self.children[0].to_physical_plan()?;
                Ok(PlanNode::Limit(LimitNode::new(
                    *limit,
                    schema.clone(),
                    vec![child],
                )))
            }

            LogicalPlanType::TopN {
                k,
                sort_expressions,
                schema,
            } => {
                let children = self
                    .children
                    .iter()
                    .map(|child| child.to_physical_plan().unwrap())
                    .collect::<Vec<PlanNode>>();
                Ok(PlanNode::TopN(TopNNode::new(
                    schema.clone(),
                    sort_expressions.clone(),
                    k.clone(),
                    children,
                )))
            }

            LogicalPlanType::MockScan {
                table_name,
                schema,
                table_oid: _,
            } => Ok(PlanNode::MockScan(MockScanNode::new(
                schema.clone(),
                table_name.clone(),
                vec![],
            ))),

            LogicalPlanType::NestedIndexJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                let left = self.children[0].to_physical_plan()?;
                let right = self.children[1].to_physical_plan()?;

                // Extract join key expressions from the predicate
                let (left_keys, right_keys) = extract_join_keys(predicate)?;

                Ok(PlanNode::NestedIndexJoin(NestedIndexJoinNode::new(
                    left_schema.clone(),
                    right_schema.clone(),
                    predicate.clone(),
                    join_type.clone(),
                    left_keys,
                    right_keys,
                    vec![left, right],
                )))
            }
            LogicalPlanType::TopNPerGroup {
                k,
                sort_expressions,
                groups,
                schema,
            } => {
                let children = self
                    .children
                    .iter()
                    .map(|child| child.to_physical_plan())
                    .collect::<Result<Vec<PlanNode>, String>>()?;

                Ok(PlanNode::TopNPerGroup(TopNPerGroupNode::new(
                    *k,
                    sort_expressions.clone(),
                    groups.clone(),
                    schema.clone(),
                    children,
                )))
            }
            LogicalPlanType::Window {
                group_by,
                aggregates,
                partitions,
                schema,
            } => {
                let children = self
                    .children
                    .iter()
                    .map(|child| child.to_physical_plan())
                    .collect::<Result<Vec<PlanNode>, String>>()?;

                // Convert the logical window expressions into WindowFunction structs
                let window_functions = aggregates
                    .iter()
                    .enumerate()
                    .map(|(i, agg_expr)| {
                        // Determine the window function type based on the aggregate expression
                        let function_type = match agg_expr.as_ref() {
                            Expression::Aggregate(agg) => match agg.get_agg_type() {
                                AggregationType::Count => WindowFunctionType::Count,
                                AggregationType::Sum => WindowFunctionType::Sum,
                                AggregationType::Min => WindowFunctionType::Min,
                                AggregationType::Max => WindowFunctionType::Max,
                                AggregationType::Avg => WindowFunctionType::Average,
                                // Add other mappings as needed
                                _ => return Err("Unsupported window function type".to_string()),
                            },
                            Expression::Window(window_func) => {
                                // If it's already a window function, use its type directly
                                window_func.get_window_type()
                            }
                            _ => return Err("Invalid window function expression".to_string()),
                        };

                        // Create a new WindowFunction with the appropriate partitioning and ordering
                        Ok(WindowFunction::new(
                            function_type,
                            Arc::clone(agg_expr),
                            if i < partitions.len() {
                                vec![Arc::clone(&partitions[i])]
                            } else {
                                vec![]
                            },
                            if i < group_by.len() {
                                vec![Arc::clone(&group_by[i])]
                            } else {
                                vec![]
                            },
                        ))
                    })
                    .collect::<Result<Vec<WindowFunction>, String>>()?;

                Ok(PlanNode::Window(WindowNode::new(
                    schema.clone(),
                    window_functions,
                    children,
                )))
            }
        }
    }
}

/// Helper function to extract join key expressions from a join predicate
fn extract_join_keys(
    predicate: &Arc<Expression>,
) -> Result<(Vec<Arc<Expression>>, Vec<Arc<Expression>>), String> {
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();

    match predicate.as_ref() {
        Expression::Comparison(comp_expr) => {
            // Handle simple equality comparison
            if let ComparisonType::Equal = comp_expr.get_comp_type() {
                let children = comp_expr.get_children();
                if children.len() == 2 {
                    left_keys.push(Arc::clone(&children[0]));
                    right_keys.push(Arc::clone(&children[1]));
                }
            }
        }
        Expression::Logic(logic_expr) => {
            // Handle AND conditions for multiple join keys
            if let LogicType::And = logic_expr.get_logic_type() {
                for child in logic_expr.get_children() {
                    if let Expression::Comparison(comp_expr) = child.as_ref() {
                        if let ComparisonType::Equal = comp_expr.get_comp_type() {
                            let comp_children = comp_expr.get_children();
                            if comp_children.len() == 2 {
                                left_keys.push(Arc::clone(&comp_children[0]));
                                right_keys.push(Arc::clone(&comp_children[1]));
                            }
                        }
                    }
                }
            }
        }
        _ => return Err("Unsupported join predicate type".to_string()),
    }

    if left_keys.is_empty() || right_keys.is_empty() {
        return Err("No valid join keys found in predicate".to_string());
    }

    Ok((left_keys, right_keys))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::plans::abstract_plan::AbstractPlanNode;
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

            let transaction_manager =
                Arc::new(TransactionManager::new());

            let catalog = Arc::new(RwLock::new(Catalog::new(
                bpm,
                0,              // next_index_oid
                0,              // next_table_oid
                HashMap::new(), // tables
                HashMap::new(), // indexes
                HashMap::new(), // table_names
                HashMap::new(), // index_names
                transaction_manager,
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

            // First verify the projection node
            match &plan.plan_type {
                LogicalPlanType::Projection { expressions, schema } => {
                    assert_eq!(schema.get_column_count(), 3);
                    assert_eq!(expressions.len(), 3);
                }
                _ => panic!("Expected Projection as root node"),
            }

            // Then verify the table scan node
            match &plan.children[0].plan_type {
                LogicalPlanType::TableScan { table_name, schema, .. } => {
                    assert_eq!(table_name, "users");
                    assert_eq!(schema.get_column_count(), 3);
                }
                _ => panic!("Expected TableScan as child node"),
            }
        }

        #[test]
        fn test_select_with_filter() {
            let mut fixture = TestFixture::new("select_with_filter");
            setup_test_table(&mut fixture);

            let select_sql = "SELECT * FROM users WHERE age > 25";
            let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

            // First verify the projection node
            match &plan.plan_type {
                LogicalPlanType::Projection { expressions, schema } => {
                    assert_eq!(schema.get_column_count(), 3);
                    assert_eq!(expressions.len(), 3);
                }
                _ => panic!("Expected Projection as root node"),
            }

            // Then verify the filter node
            match &plan.children[0].plan_type {
                LogicalPlanType::Filter { schema, predicate, .. } => {
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
                LogicalPlanType::TableScan { table_name, schema, .. } => {
                    assert_eq!(table_name, "users");
                    assert_eq!(schema.get_column_count(), 3);
                }
                _ => panic!("Expected TableScan as leaf node"),
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

    #[cfg(test)]
    mod aggregation_tests {
        use super::*;
        use crate::sql::execution::expressions::aggregate_expression::AggregationType;
        use crate::types_db::type_id::TypeId;

        // Helper function to set up a test table
        fn setup_test_table(fixture: &mut TestFixture) {
            fixture
                .create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false)
                .unwrap();
        }

        #[test]
        fn test_plan_aggregate_column_names() {
            let mut fixture = TestFixture::new("test_plan_aggregate_column_names");
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
                let plan = fixture.planner.create_logical_plan(sql).unwrap_or_else(|e| {
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
            let mut fixture = TestFixture::new("test_plan_aggregate_types");
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
                    LogicalPlanType::Projection { expressions, schema } => {
                        assert_eq!(expressions.len(), expected_types.len());
                    }
                    _ => panic!("Expected Projection as root node for query: {}", sql),
                }

                // Then verify the aggregate node
                match &plan.children[0].plan_type {
                    LogicalPlanType::Aggregate { group_by, aggregates, schema } => {
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
                                    *agg.get_agg_type(),
                                    *expected_type,
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
}
