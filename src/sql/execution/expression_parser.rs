use crate::catalog::catalog::Catalog;
use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::aggregate_expression::{
    AggregateExpression, AggregationType,
};
use crate::sql::execution::expressions::arithmetic_expression::{
    ArithmeticExpression, ArithmeticOp,
};
use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::sql::execution::expressions::comparison_expression::{
    ComparisonExpression, ComparisonType,
};
use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
use crate::sql::execution::expressions::logic_expression::{LogicExpression, LogicType};
use crate::sql::execution::expressions::case_expression::CaseExpression;
use crate::sql::execution::expressions::cast_expression::CastExpression;
use crate::sql::execution::expressions::string_expression::{StringExpression, StringExpressionType};
use crate::sql::planner::planner::LogicalPlan;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use parking_lot::RwLock;
use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    JoinConstraint, JoinOperator, ObjectName, Select, SelectItem, TableFactor, UnaryOperator,
};
use std::sync::Arc;

/// 1. Responsible for parsing SQL expressions into our internal expression types
pub struct ExpressionParser {
    catalog: Arc<RwLock<Catalog>>,
}

impl ExpressionParser {
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        Self { catalog }
    }

    pub fn catalog(&self) -> Arc<RwLock<Catalog>> {
        self.catalog.clone()
    }

    pub fn parse_expression(&self, expr: &Expr, schema: &Schema) -> Result<Expression, String> {
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
                    sqlparser::ast::Value::Number(n, _) => {
                        if n.contains('.') {
                            // Parse as decimal if it contains a decimal point
                            (Value::from(n.parse::<f64>().map_err(|e| e.to_string())?), TypeId::Decimal)
                        } else {
                            // Parse as integer otherwise
                            (Value::from(n.parse::<i32>().map_err(|e| e.to_string())?), TypeId::Integer)
                        }
                    },
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
                let left_expr = Arc::new(self.parse_expression(left, schema)?);
                let right_expr = Arc::new(self.parse_expression(right, schema)?);

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
                            left_expr.clone(),
                            right_expr.clone(),
                            comparison_type,
                            vec![left_expr.clone(), right_expr.clone()], // Add both expressions as children
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
                            left_expr.clone(),
                            right_expr.clone(),
                            op,
                            vec![left_expr.clone(), right_expr.clone()],
                        )))
                    }

                    BinaryOperator::And | BinaryOperator::Or => {
                        let logic_type = match op {
                            BinaryOperator::And => LogicType::And,
                            BinaryOperator::Or => LogicType::Or,
                            _ => unreachable!(),
                        };

                        Ok(Expression::Logic(LogicExpression::new(
                            left_expr.clone(),
                            right_expr.clone(),
                            logic_type,
                            vec![left_expr.clone(), right_expr.clone()],
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

            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                match name.as_str() {
                    "LOWER" | "UPPER" => {
                        // Get the argument
                        let arg = match &func.args {
                            FunctionArguments::List(list) if list.args.len() == 1 => {
                                match &list.args[0] {
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                                        Arc::new(self.parse_expression(expr, schema)?)
                                    }
                                    _ => return Err("Invalid string function argument".to_string()),
                                }
                            }
                            _ => return Err(format!("{}() requires exactly one argument", name)),
                        };

                        let expr_type = match name.as_str() {
                            "LOWER" => StringExpressionType::Lower,
                            "UPPER" => StringExpressionType::Upper,
                            _ => unreachable!(),
                        };

                        Ok(Expression::String(StringExpression::new(
                            arg.clone(),
                            expr_type,
                            vec![arg],
                        )))
                    }
                    // Handle other function types (aggregates etc.)
                    _ => match self.parse_aggregate_function(&func, schema) {
                        Ok(aggregate_function) => Ok(aggregate_function.0),
                        _ => Err(format!("Unsupported function: {}", func)),
                    },
                }
            }

            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                // Parse the base expression if present
                let base_expr = match operand {
                    Some(expr) => Some(Arc::new(self.parse_expression(expr, schema)?)),
                    None => None,
                };

                // Parse WHEN conditions
                let when_exprs = conditions
                    .iter()
                    .map(|expr| self.parse_expression(expr, schema))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .map(Arc::new)
                    .collect();

                // Parse THEN results
                let then_exprs = results
                    .iter()
                    .map(|expr| self.parse_expression(expr, schema))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .map(Arc::new)
                    .collect();

                // Parse ELSE result if present
                let else_expr = match else_result {
                    Some(expr) => Some(Arc::new(self.parse_expression(expr, schema)?)),
                    None => None,
                };

                // Create the CASE expression
                Ok(Expression::Case(
                    CaseExpression::new(base_expr, when_exprs, then_exprs, else_expr)
                        .map_err(|e| e.to_string())?,
                ))
            }

            Expr::Cast { expr, data_type, .. } => {
                let inner_expr = Arc::new(self.parse_expression(expr, schema)?);
                let target_type = match data_type {
                    sqlparser::ast::DataType::Int(_) => TypeId::Integer,
                    sqlparser::ast::DataType::BigInt(_) => TypeId::BigInt,
                    sqlparser::ast::DataType::Float(_) | 
                    sqlparser::ast::DataType::Double | 
                    sqlparser::ast::DataType::Decimal(_) => TypeId::Decimal,  // Add support for Decimal with precision
                    sqlparser::ast::DataType::Char(_) => TypeId::Char,
                    sqlparser::ast::DataType::Varchar(_) => TypeId::VarChar,
                    sqlparser::ast::DataType::Boolean => TypeId::Boolean,
                    _ => return Err(format!("Unsupported cast target type: {:?}", data_type)),
                };

                Ok(Expression::Cast(CastExpression::new(inner_expr, target_type)))
            }

            _ => Err(format!("Unsupported expression type: {:?}", expr)),
        }
    }

    pub fn parse_aggregate_function(
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

    pub fn parse_aggregates(
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
                        // Create new aggregate expression with aliased column
                        if let Expression::Aggregate(agg) = agg_expr {
                            let orig_type = agg.get_return_type().get_type();
                            let new_col = Column::new(&alias.value, orig_type);
                            let new_agg = AggregateExpression::new(
                                agg.get_agg_type().clone(),
                                agg.get_arg().clone(),
                                vec![],
                            )
                            .with_return_type(new_col);
                            agg_exprs.push(Arc::new(Expression::Aggregate(new_agg)));
                        } else {
                            agg_exprs.push(Arc::new(agg_expr));
                        }
                        agg_types.push(agg_type);
                    }
                }
                SelectItem::Wildcard(_) => {
                    // Handle COUNT(*) case
                    if projection.len() == 1 {
                        let count_star = Expression::Aggregate(AggregateExpression::new(
                            AggregationType::CountStar,
                            Arc::new(Expression::Constant(ConstantExpression::new(
                                Value::new(1),
                                Column::new("count", TypeId::BigInt),
                                vec![],
                            ))),
                            vec![],
                        ));
                        agg_exprs.push(Arc::new(count_star));
                        agg_types.push(AggregationType::CountStar);
                    }
                }
                _ => {}
            }
        }

        Ok((agg_exprs, agg_types))
    }

    pub fn has_aggregate_functions(&self, projection: &Vec<SelectItem>) -> bool {
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

    pub fn determine_group_by_expressions(
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
    /// Helper function to extract join key expressions from a join predicate

    pub fn prepare_table_scan(
        &self,
        select: &Box<Select>,
    ) -> Result<(String, Schema, u64), String> {
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

    pub fn prepare_join_scan(&self, select: &Box<Select>) -> Result<Box<LogicalPlan>, String> {
        if select.from.len() <= 1 {
            return self
                .prepare_table_scan(select)
                .map(|(name, schema, oid)| LogicalPlan::table_scan(name, schema, oid));
        }

        let mut current_plan = None;
        let mut current_schema = None;

        for (i, table_with_joins) in select.from.iter().enumerate() {
            let table_factor = &table_with_joins.relation;
            let table_name = match table_factor {
                TableFactor::Table { name, .. } => name.to_string(),
                _ => return Err("Only simple table joins are supported".to_string()),
            };

            let catalog_guard = self.catalog.read();
            let table_info = catalog_guard
                .get_table(&table_name)
                .ok_or_else(|| format!("Table '{}' not found", table_name))?;

            let schema = table_info.get_table_schema();
            let table_oid = table_info.get_table_oidt();
            let table_scan = LogicalPlan::table_scan(table_name, schema.clone(), table_oid);

            if i == 0 {
                current_plan = Some(table_scan);
                current_schema = Some(schema);
            } else {
                for join in &table_with_joins.joins {
                    let left_plan = current_plan.take().unwrap();
                    let left_schema = current_schema.take().unwrap();

                    let join_predicate = match &join.join_operator {
                        JoinOperator::Inner(constraint) |
                        JoinOperator::LeftOuter(constraint) |
                        JoinOperator::RightOuter(constraint) |
                        JoinOperator::FullOuter(constraint) => match constraint {
                            JoinConstraint::On(expr) => {
                                let combined_schema = Schema::merge(&left_schema, &schema);
                                self.parse_expression(expr, &combined_schema)?
                            }
                            _ => return Err("Only ON join constraints supported".to_string()),
                        },
                        _ => return Err("Only INNER, LEFT OUTER, RIGHT OUTER, and FULL OUTER joins with ON clause are supported".to_string()),
                    };

                    current_plan = Some(match &join.join_operator {
                        JoinOperator::Inner(_) => LogicalPlan::hash_join(
                            left_schema.clone(),
                            schema.clone(),
                            Arc::new(join_predicate),
                            join.join_operator.clone(),
                            left_plan,
                            table_scan.clone(),
                        ),
                        _ => return Err("Only INNER JOIN supported".to_string()),
                    });

                    current_schema = Some(Schema::merge(&left_schema, &schema));
                }
            }
        }

        current_plan.ok_or_else(|| "No tables in FROM clause".to_string())
    }

    pub fn extract_table_name(&self, table_name: &ObjectName) -> Result<String, String> {
        match table_name {
            ObjectName(parts) if parts.len() == 1 => Ok(parts[0].value.clone()),
            _ => Err("Only single table INSERT statements are supported".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::expressions::abstract_expression::ExpressionOps;
    use crate::sql::execution::expressions::arithmetic_expression::ArithmeticOp;
    use crate::sql::execution::expressions::comparison_expression::ComparisonType;
    use crate::sql::execution::expressions::logic_expression::LogicType;
    use crate::sql::planner::planner::QueryPlanner;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use sqlparser::ast::{SetExpr, Statement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
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

            let planner = QueryPlanner::new(catalog.clone());

            Self {
                catalog,
                planner,
                _temp_dir: temp_dir,
            }
        }

        pub fn catalog(&self) -> Arc<RwLock<Catalog>> {
            self.catalog.clone()
        }

        fn setup_test_schema(&self) -> Schema {
            Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("age", TypeId::Integer),
                Column::new("salary", TypeId::Decimal),
            ])
        }

        fn parse_expression(
            &self,
            expr_str: &str,
            schema: &Schema,
            catalog: Arc<RwLock<Catalog>>,
        ) -> Result<Expression, String> {
            let dialect = GenericDialect {};
            let ast = Parser::parse_sql(&dialect, &format!("SELECT * FROM t WHERE {}", expr_str))
                .map_err(|e| e.to_string())?;

            if let Statement::Query(query) = &ast[0] {
                if let SetExpr::Select(select) = &*query.body {
                    if let Some(expr) = &select.selection {
                        let parser = ExpressionParser::new(catalog);
                        return parser.parse_expression(expr, schema);
                    }
                }
            }

            Err("Failed to parse expression".to_string())
        }
    }

    #[test]
    fn test_parse_comparison_expressions() {
        let ctx = TestContext::new("test_parse_comparison_expressions");
        let schema = ctx.setup_test_schema();

        let test_cases = vec![
            ("age > 25", ComparisonType::GreaterThan),
            ("age >= 25", ComparisonType::GreaterThanOrEqual),
            ("age < 25", ComparisonType::LessThan),
            ("age <= 25", ComparisonType::LessThanOrEqual),
            ("age = 25", ComparisonType::Equal),
            ("age != 25", ComparisonType::NotEqual),
        ];

        for (expr_str, expected_type) in test_cases {
            let expr = ctx
                .parse_expression(expr_str, &schema, ctx.catalog())
                .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", expr_str, e));

            match expr {
                Expression::Comparison(comp) => {
                    assert_eq!(
                        comp.get_comp_type(),
                        expected_type,
                        "Expression '{}' should be {:?}",
                        expr_str,
                        expected_type
                    );
                }
                _ => panic!("Expected Comparison expression for '{}'", expr_str),
            }
        }
    }

    #[test]
    fn test_parse_arithmetic_expressions() {
        let ctx = TestContext::new("test_parse_arithmetic_expressions");
        let schema = ctx.setup_test_schema();

        let test_cases = vec![
            ("age + 5", ArithmeticOp::Add),
            ("age - 5", ArithmeticOp::Subtract),
            ("age * 5", ArithmeticOp::Multiply),
            ("age / 5", ArithmeticOp::Divide),
        ];

        for (expr_str, expected_op) in test_cases {
            let expr = ctx
                .parse_expression(expr_str, &schema, ctx.catalog())
                .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", expr_str, e));

            match expr {
                Expression::Arithmetic(arith) => {
                    assert_eq!(
                        arith.get_op(), // Changed from get_return_type() to get_op()
                        expected_op,
                        "Expression '{}' should be {:?}",
                        expr_str,
                        expected_op
                    );
                }
                _ => panic!("Expected Arithmetic expression for '{}'", expr_str),
            }
        }
    }

    #[test]
    fn test_parse_logical_expressions() {
        let ctx = TestContext::new("test_parse_logical_expressions");
        let schema = ctx.setup_test_schema();

        let test_cases = vec![
            ("age > 20 AND salary < 50000", LogicType::And),
            ("age < 25 OR salary > 60000", LogicType::Or),
        ];

        for (expr_str, expected_type) in test_cases {
            let expr = ctx
                .parse_expression(expr_str, &schema, ctx.catalog())
                .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", expr_str, e));

            match expr {
                Expression::Logic(logic) => {
                    assert_eq!(
                        logic.get_logic_type(),
                        expected_type,
                        "Expression '{}' should be {:?}",
                        expr_str,
                        expected_type
                    );
                }
                _ => panic!("Expected Logic expression for '{}'", expr_str),
            }
        }
    }

    #[test]
    fn test_parse_column_references() {
        let ctx = TestContext::new("test_parse_column_references");
        let schema = ctx.setup_test_schema();

        let test_cases = vec!["id = 1", "name = 'John'", "age > 25", "salary < 50000"];

        for expr_str in test_cases {
            let expr = ctx
                .parse_expression(expr_str, &schema, ctx.catalog())
                .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", expr_str, e));

            match expr {
                Expression::Comparison(comp) => {
                    let children = comp.get_children();

                    // Verify we have exactly 2 children
                    assert_eq!(
                        children.len(),
                        2,
                        "Comparison expression '{}' should have exactly 2 children (got {})",
                        expr_str,
                        children.len()
                    );

                    // Verify left child is a column reference
                    match children[0].as_ref() {
                        Expression::ColumnRef(col_ref) => {
                            assert!(
                                schema
                                    .get_column_index(col_ref.get_return_type().get_name())
                                    .is_some(),
                                "Column '{}' not found in schema",
                                col_ref.get_return_type().get_name()
                            );
                        }
                        other => panic!(
                            "Expected ColumnRef as left child for '{}', got {:?}",
                            expr_str, other
                        ),
                    }

                    // Verify right child is a constant
                    match children[1].as_ref() {
                        Expression::Constant(_) => {} // Success
                        other => panic!(
                            "Expected Constant as right child for '{}', got {:?}",
                            expr_str, other
                        ),
                    }
                }
                _ => panic!("Expected Comparison expression for '{}'", expr_str),
            }
        }
    }

    #[test]
    fn test_parse_constants() {
        let ctx = TestContext::new("test_parse_constants");
        let schema = ctx.setup_test_schema();

        let test_cases = vec![
            ("age = 25", TypeId::Integer),
            ("name = 'John'", TypeId::VarChar),
            ("salary > 50000.0", TypeId::Decimal),
        ];

        for (expr_str, expected_type) in test_cases {
            let expr = ctx
                .parse_expression(expr_str, &schema, ctx.catalog())
                .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", expr_str, e));

            match expr {
                Expression::Comparison(comp) => {
                    let children = comp.get_children();
                    match children[1].as_ref() {
                        Expression::Constant(c) => {
                            assert_eq!(
                                c.get_return_type().get_type(),
                                expected_type,
                                "Constant in '{}' should be {:?}",
                                expr_str,
                                expected_type
                            );
                        }
                        _ => panic!("Expected Constant as right child for '{}'", expr_str),
                    }
                }
                _ => panic!("Expected Comparison expression for '{}'", expr_str),
            }
        }
    }

    #[test]
    fn test_parse_case_expressions() {
        let ctx = TestContext::new("test_parse_case_expressions");
        let schema = ctx.setup_test_schema();

        let test_cases = vec![
            // Simple CASE
            ("CASE WHEN age > 18 THEN 'Adult' ELSE 'Minor' END", TypeId::VarChar),
            // CASE with multiple WHEN clauses
            ("CASE WHEN age < 13 THEN 'Child' WHEN age < 20 THEN 'Teen' ELSE 'Adult' END", TypeId::VarChar),
            // CASE with expression
            ("CASE age WHEN 18 THEN 'New Adult' WHEN 21 THEN 'Drinking Age' ELSE 'Other' END", TypeId::VarChar),
        ];

        for (expr_str, expected_type) in test_cases {
            let expr = ctx
                .parse_expression(expr_str, &schema, ctx.catalog())
                .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", expr_str, e));

            match expr {
                Expression::Case(case) => {
                    assert_eq!(
                        case.get_return_type().get_type(),
                        expected_type,
                        "CASE expression '{}' should return {:?}",
                        expr_str,
                        expected_type
                    );
                }
                _ => panic!("Expected Case expression for '{}'", expr_str),
            }
        }
    }

    #[test]
    fn test_parse_cast_expressions() {
        let ctx = TestContext::new("test_parse_cast_expressions");
        let schema = ctx.setup_test_schema();

        let test_cases = vec![
            ("CAST(age AS DECIMAL)", TypeId::Decimal),
            ("CAST('123' AS INTEGER)", TypeId::Integer),
            ("CAST(salary AS INTEGER)", TypeId::Integer),
        ];

        for (expr_str, expected_type) in test_cases {
            let expr = ctx
                .parse_expression(expr_str, &schema, ctx.catalog())
                .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", expr_str, e));

            match expr {
                Expression::Cast(cast) => {
                    assert_eq!(
                        cast.get_return_type().get_type(),
                        expected_type,
                        "CAST expression '{}' should return {:?}",
                        expr_str,
                        expected_type
                    );
                }
                _ => panic!("Expected Cast expression for '{}'", expr_str),
            }
        }
    }

    #[test]
    fn test_parse_string_functions() {
        let ctx = TestContext::new("test_parse_string_functions");
        let schema = ctx.setup_test_schema();

        let test_cases = vec![
            ("LOWER(name)", TypeId::VarChar),
            ("UPPER(name)", TypeId::VarChar),
            ("LOWER(UPPER(name))", TypeId::VarChar),  // Nested function calls
        ];

        for (expr_str, expected_type) in test_cases {
            let expr = ctx
                .parse_expression(expr_str, &schema, ctx.catalog())
                .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", expr_str, e));

            match expr {
                Expression::String(string) => {
                    assert_eq!(
                        string.get_return_type().get_type(),
                        expected_type,
                        "String function '{}' should return {:?}",
                        expr_str,
                        expected_type
                    );
                }
                _ => panic!("Expected String expression for '{}'", expr_str),
            }
        }
    }

    #[test]
    fn test_parse_complex_expressions() {
        let ctx = TestContext::new("test_parse_complex_expressions");
        let schema = ctx.setup_test_schema();

        let test_cases = vec![
            // Arithmetic with CAST
            ("CAST(age AS DECIMAL) + salary", TypeId::Decimal),
            // String function with CASE
            ("UPPER(CASE WHEN age > 21 THEN name ELSE 'Minor' END)", TypeId::VarChar),
            // Complex condition
            ("CASE WHEN LOWER(name) = 'john' AND salary > 50000.0 THEN 'High Paid' ELSE 'Standard' END", TypeId::VarChar),
        ];

        for (expr_str, expected_type) in test_cases {
            let expr = ctx
                .parse_expression(expr_str, &schema, ctx.catalog())
                .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", expr_str, e));

            assert_eq!(
                expr.get_return_type().get_type(),
                expected_type,
                "Complex expression '{}' should return {:?}",
                expr_str,
                expected_type
            );
        }
    }
}
