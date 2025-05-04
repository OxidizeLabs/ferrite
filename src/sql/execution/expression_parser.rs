use crate::catalog::catalog::Catalog;
use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::config::TableOidT;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::aggregate_expression::{
    AggregateExpression, AggregationType,
};
use crate::sql::execution::expressions::all_expression::AllExpression;
use crate::sql::execution::expressions::any_expression::AnyExpression;
use crate::sql::execution::expressions::arithmetic_expression::{
    ArithmeticExpression, ArithmeticOp,
};
use crate::sql::execution::expressions::array_expression::ArrayExpression;
use crate::sql::execution::expressions::binary_op_expression::BinaryOpExpression;
use crate::sql::execution::expressions::case_expression::CaseExpression;
use crate::sql::execution::expressions::cast_expression::CastExpression;
use crate::sql::execution::expressions::ceil_floor_expression::{
    CeilFloorExpression, CeilFloorOperation,
};
use crate::sql::execution::expressions::collate_expression::CollateExpression;
use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::sql::execution::expressions::comparison_expression::{
    ComparisonExpression, ComparisonType,
};
use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
use crate::sql::execution::expressions::convert_expression::ConvertExpression;
use crate::sql::execution::expressions::datetime_expression::DateTimeField;
use crate::sql::execution::expressions::exists_expression::ExistsExpression;
use crate::sql::execution::expressions::extract_expression::ExtractExpression;
use crate::sql::execution::expressions::extract_expression::ExtractField;
use crate::sql::execution::expressions::function_expression::FunctionExpression;
use crate::sql::execution::expressions::function_types::{
    self, AggregateFunctionType, FunctionType, ScalarFunctionType,
};
use crate::sql::execution::expressions::grouping_sets_expression::{
    GroupingSetsExpression, GroupingType,
};
use crate::sql::execution::expressions::in_expression::InExpression;
use crate::sql::execution::expressions::interval_expression::IntervalExpression;
use crate::sql::execution::expressions::interval_expression::IntervalField;
use crate::sql::execution::expressions::is_check_expression::{IsCheckExpression, IsCheckType};
use crate::sql::execution::expressions::is_distinct_expression::IsDistinctExpression;
use crate::sql::execution::expressions::literal_value_expression::LiteralValueExpression;
use crate::sql::execution::expressions::logic_expression::{LogicExpression, LogicType};
use crate::sql::execution::expressions::map_access_expression::{
    MapAccessExpression, MapAccessKey,
};
use crate::sql::execution::expressions::method_expression::MethodExpression;
use crate::sql::execution::expressions::overlay_expression::OverlayExpression;
use crate::sql::execution::expressions::position_expression::PositionExpression;
use crate::sql::execution::expressions::qualified_wildcard_expression::QualifiedWildcardExpression;
use crate::sql::execution::expressions::regex_expression::{RegexExpression, RegexOperator};
use crate::sql::execution::expressions::struct_expression::{StructExpression, StructField};
use crate::sql::execution::expressions::subquery_expression::SubqueryExpression;
use crate::sql::execution::expressions::subquery_expression::SubqueryType;
use crate::sql::execution::expressions::subscript_expression::{Subscript as InternalSubscript, SubscriptExpression};
use crate::sql::execution::expressions::substring_expression::SubstringExpression;
use crate::sql::execution::expressions::trim_expression::{TrimExpression, TrimType};
use crate::sql::execution::expressions::tuple_expression::TupleExpression;
use crate::sql::execution::expressions::typed_string_expression::TypedStringExpression;
use crate::sql::execution::expressions::unary_op_expression::UnaryOpExpression;
use crate::sql::execution::expressions::wildcard_expression::WildcardExpression;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use log::debug;
use parking_lot::RwLock;
use sqlparser::ast::{BinaryOperator, CastFormat, CeilFloorKind, DataType, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentClause, FunctionArguments, GroupByExpr, JoinConstraint, JoinOperator, ObjectName, ObjectNamePart, OrderByExpr, Query, Select, SelectItem, SetExpr, Subscript as SQLSubscript, TableFactor, TableObject, Value as SQLValue, WindowFrameBound, WindowType, AccessExpr, ValueWithSpan};
use std::sync::Arc;
use sqlparser::tokenizer::{Location, Span};

/// 1. Responsible for parsing SQL expressions into our internal expression types
pub struct ExpressionParser {
    catalog: Arc<RwLock<Catalog>>,
}

impl ExpressionParser {
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        debug!("Creating new ExpressionParser");
        Self { catalog }
    }

    pub fn catalog(&self) -> Arc<RwLock<Catalog>> {
        self.catalog.clone()
    }

    pub fn parse_expression(&self, expr: &Expr, schema: &Schema) -> Result<Expression, String> {
        debug!("Parsing expression: {:?}", expr);
        match expr {
            Expr::Identifier(ident) => {
                debug!("Parsing identifier: {}", ident.value);
                // Look up column in the provided schema
                let column_idx = schema
                    .get_column_index(&ident.value)
                    .ok_or_else(|| format!("Column {} not found in schema", ident.value))?;

                let column = schema
                    .get_column(column_idx)
                    .ok_or_else(|| format!("Failed to get column at index {}", column_idx))?;

                debug!("Found column '{}' at index {}", ident.value, column_idx);
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
            
                let table_alias = &parts[0].value;
                let column_name = &parts[1].value;
                let qualified_name = format!("{}.{}", table_alias, column_name);
            
                debug!("Parsing compound identifier: {}", qualified_name);
                debug!("Schema has {} columns:", schema.get_column_count());
                for i in 0..schema.get_column_count() as usize {
                    let col = schema.get_column(i).unwrap();
                    debug!(
                        "  Schema column {}: name='{}', type={:?}",
                        i,
                        col.get_name(),
                        col.get_type()
                    );
                }
            
                // First, try to find the column using the qualified name directly
                if let Some(column_idx) = schema.get_qualified_column_index(&qualified_name) {
                    debug!(
                        "Found qualified column '{}' at index {}",
                        qualified_name, column_idx
                    );
                    let column = schema.get_column(column_idx).unwrap().clone();
            
                    return Ok(Expression::ColumnRef(ColumnRefExpression::new(
                        0,
                        column_idx,
                        column,
                        vec![],
                    )));
                }
                debug!("Qualified column '{}' not found directly", qualified_name);
            
                // Second, try to find any column that matches the pattern table_alias.column_name
                for (idx, col) in schema.get_columns().iter().enumerate() {
                    let col_name = col.get_name();
                    debug!("Checking column '{}' for pattern match", col_name);
            
                    // Check if the column name is already qualified with the same table alias
                    if col_name.starts_with(&format!("{}.", table_alias)) {
                        let parts: Vec<&str> = col_name.split('.').collect();
                        if parts.len() == 2 && parts[1] == column_name {
                            debug!(
                                "Found column with matching pattern: '{}' at index {}",
                                col_name, idx
                            );
                            return Ok(Expression::ColumnRef(ColumnRefExpression::new(
                                0,
                                idx,
                                col.clone(),
                                vec![],
                            )));
                        }
                    }
                }
                debug!(
                    "No column found with pattern match for '{}'",
                    qualified_name
                );
            
                // Third, if the table alias matches a known alias and the column exists without qualification
                // This handles cases where the schema has unqualified column names but we're using qualified references
                let unqualified_idx = schema.get_column_index(column_name);
                if let Some(idx) = unqualified_idx {
                    debug!(
                        "Found unqualified column '{}' at index {}, will qualify with '{}'",
                        column_name, idx, table_alias
                    );
            
                    let mut column = schema.get_column(idx).unwrap().clone();
                    column.set_name(qualified_name);
            
                    return Ok(Expression::ColumnRef(ColumnRefExpression::new(
                        0,
                        idx,
                        column,
                        vec![],
                    )));
                }
                debug!("No unqualified column '{}' found", column_name);
            
                // If we get here, the column wasn't found
                Err(format!(
                    "Column {}.{} not found in schema",
                    table_alias, column_name
                ))
            }
            
            Expr::Value(value) => Ok(Expression::Literal(LiteralValueExpression::new(
                value.clone().into(),
            )?)),
            
            Expr::BinaryOp { left, op, right } => {
                let left_expr = Arc::new(self.parse_expression(left, schema)?);
                let right_expr = Arc::new(self.parse_expression(right, schema)?);
            
                // Convert arithmetic binary operators to ArithmeticExpression
                match op {
                    BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide => {
                        let left_type = left_expr.get_return_type().get_type();
                        let right_type = right_expr.get_return_type().get_type();
            
                        // Check if types are compatible for arithmetic
                        match (left_type, right_type) {
                            (TypeId::Integer, TypeId::Integer)
                            | (TypeId::Decimal, TypeId::Decimal)
                            | (TypeId::BigInt, TypeId::BigInt)
                            | (TypeId::Integer, TypeId::Decimal)
                            | (TypeId::Decimal, TypeId::Integer)
                            | (TypeId::BigInt, TypeId::Integer)
                            | (TypeId::Integer, TypeId::BigInt)
                            | (TypeId::BigInt, TypeId::Decimal)
                            | (TypeId::Decimal, TypeId::BigInt) => {
                                let arith_op = match op {
                                    BinaryOperator::Plus => ArithmeticOp::Add,
                                    BinaryOperator::Minus => ArithmeticOp::Subtract,
                                    BinaryOperator::Multiply => ArithmeticOp::Multiply,
                                    BinaryOperator::Divide => ArithmeticOp::Divide,
                                    _ => unreachable!(),
                                };
                                Ok(Expression::Arithmetic(ArithmeticExpression::new(
                                    arith_op,
                                    vec![left_expr, right_expr],
                                )))
                            }
                            _ => Err(format!(
                                "Cannot perform arithmetic operation between types {:?} and {:?}",
                                left_type, right_type
                            )),
                        }
                    }
                    // Handle logical operators
                    BinaryOperator::And => {
                        // Validate that both operands are boolean
                        let left_type = left_expr.get_return_type().get_type();
                        let right_type = right_expr.get_return_type().get_type();
            
                        if left_type != TypeId::Boolean || right_type != TypeId::Boolean {
                            return Err(format!(
                                "AND operator requires boolean operands, got {:?} AND {:?}",
                                left_type, right_type
                            ));
                        }
            
                        Ok(Expression::Logic(LogicExpression::new(
                            left_expr.clone(),
                            right_expr.clone(),
                            LogicType::And,
                            vec![left_expr, right_expr],
                        )))
                    }
                    BinaryOperator::Or => {
                        // Validate that both operands are boolean
                        let left_type = left_expr.get_return_type().get_type();
                        let right_type = right_expr.get_return_type().get_type();
            
                        if left_type != TypeId::Boolean || right_type != TypeId::Boolean {
                            return Err(format!(
                                "OR operator requires boolean operands, got {:?} OR {:?}",
                                left_type, right_type
                            ));
                        }
            
                        Ok(Expression::Logic(LogicExpression::new(
                            left_expr.clone(),
                            right_expr.clone(),
                            LogicType::Or,
                            vec![left_expr, right_expr],
                        )))
                    }
                    // Handle comparison operators
                    BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq => {
                        // Validate that the types are comparable
                        let left_type = left_expr.get_return_type().get_type();
                        let right_type = right_expr.get_return_type().get_type();
            
                        match (left_type, right_type) {
                            (TypeId::Integer, TypeId::Integer)
                            | (TypeId::Decimal, TypeId::Decimal)
                            | (TypeId::VarChar, TypeId::VarChar)
                            | (TypeId::Char, TypeId::Char)
                            | (TypeId::Boolean, TypeId::Boolean)
                            | (TypeId::Integer, TypeId::Decimal)
                            | (TypeId::Decimal, TypeId::Integer)
                            | (TypeId::BigInt, TypeId::BigInt)
                            | (TypeId::BigInt, TypeId::Integer)
                            | (TypeId::Integer, TypeId::BigInt)
                            | (TypeId::BigInt, TypeId::Decimal)
                            | (TypeId::Decimal, TypeId::BigInt) => {
                                let comp_type = match op {
                                    BinaryOperator::Eq => ComparisonType::Equal,
                                    BinaryOperator::NotEq => ComparisonType::NotEqual,
                                    BinaryOperator::Lt => ComparisonType::LessThan,
                                    BinaryOperator::LtEq => ComparisonType::LessThanOrEqual,
                                    BinaryOperator::Gt => ComparisonType::GreaterThan,
                                    BinaryOperator::GtEq => ComparisonType::GreaterThanOrEqual,
                                    _ => unreachable!(),
                                };
            
                                // Convert right_expr to ConstantExpression if it's a LiteralValueExpression
                                let right_expr = match right_expr.as_ref() {
                                    Expression::Literal(lit) => {
                                        Arc::new(Expression::Constant(ConstantExpression::new(
                                            lit.get_value().clone(),
                                            lit.get_return_type().clone(),
                                            vec![],
                                        )))
                                    }
                                    _ => right_expr,
                                };
            
                                Ok(Expression::Comparison(ComparisonExpression::new(
                                    left_expr.clone(),
                                    right_expr.clone(),
                                    comp_type,
                                    vec![left_expr, right_expr],
                                )))
                            }
                            _ => Err(format!(
                                "Type mismatch: Cannot compare values of types {:?} and {:?}",
                                left_type, right_type
                            )),
                        }
                    }
                    // Handle other binary operators
                    _ => Ok(Expression::BinaryOp(BinaryOpExpression::new(
                        left_expr.clone(),
                        right_expr.clone(),
                        op.clone(),
                        vec![left_expr, right_expr],
                    )?)),
                }
            }
            
            Expr::UnaryOp { op, expr } => {
                let inner_expr = Arc::new(self.parse_expression(expr, schema)?);
                Ok(Expression::UnaryOp(UnaryOpExpression::new(
                    inner_expr,
                    op.clone(),
                )?))
            }
            
            Expr::IsNull(expr) => {
                let inner_expr = Arc::new(self.parse_expression(expr, schema)?);
                Ok(Expression::IsCheck(IsCheckExpression::new(
                    inner_expr,
                    IsCheckType::Null { negated: false },
                    Column::new("is_null", TypeId::Boolean),
                )))
            }
            
            Expr::IsNotNull(expr) => {
                let inner_expr = Arc::new(self.parse_expression(expr, schema)?);
                Ok(Expression::IsCheck(IsCheckExpression::new(
                    inner_expr,
                    IsCheckType::Null { negated: true },
                    Column::new("is_not_null", TypeId::Boolean),
                )))
            }
            
            Expr::IsTrue(expr) => {
                let inner_expr = Arc::new(self.parse_expression(expr, schema)?);
                Ok(Expression::IsCheck(IsCheckExpression::new(
                    inner_expr,
                    IsCheckType::True { negated: false },
                    Column::new("is_true", TypeId::Boolean),
                )))
            }
            
            Expr::IsNotTrue(expr) => {
                let inner_expr = Arc::new(self.parse_expression(expr, schema)?);
                Ok(Expression::IsCheck(IsCheckExpression::new(
                    inner_expr,
                    IsCheckType::True { negated: true },
                    Column::new("is_not_true", TypeId::Boolean),
                )))
            }
            
            Expr::IsFalse(expr) => {
                let inner_expr = Arc::new(self.parse_expression(expr, schema)?);
                Ok(Expression::IsCheck(IsCheckExpression::new(
                    inner_expr,
                    IsCheckType::False { negated: false },
                    Column::new("is_false", TypeId::Boolean),
                )))
            }
            
            Expr::IsNotFalse(expr) => {
                let inner_expr = Arc::new(self.parse_expression(expr, schema)?);
                Ok(Expression::IsCheck(IsCheckExpression::new(
                    inner_expr,
                    IsCheckType::False { negated: true },
                    Column::new("is_not_false", TypeId::Boolean),
                )))
            }
            
            Expr::IsUnknown(expr) => {
                let inner_expr = Arc::new(self.parse_expression(expr, schema)?);
                Ok(Expression::IsCheck(IsCheckExpression::new(
                    inner_expr,
                    IsCheckType::Unknown { negated: false },
                    Column::new("is_unknown", TypeId::Boolean),
                )))
            }
            
            Expr::IsNotUnknown(expr) => {
                let inner_expr = Arc::new(self.parse_expression(expr, schema)?);
                Ok(Expression::IsCheck(IsCheckExpression::new(
                    inner_expr,
                    IsCheckType::Unknown { negated: true },
                    Column::new("is_not_unknown", TypeId::Boolean),
                )))
            }
            
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr = Arc::new(self.parse_expression(expr, schema)?);
                let low = Arc::new(self.parse_expression(low, schema)?);
                let high = Arc::new(self.parse_expression(high, schema)?);
            
                let low_compare = Expression::Comparison(ComparisonExpression::new(
                    expr.clone(),
                    low.clone(),
                    if *negated {
                        ComparisonType::LessThan
                    } else {
                        ComparisonType::GreaterThanOrEqual
                    },
                    vec![expr.clone(), low],
                ));
            
                let high_compare = Expression::Comparison(ComparisonExpression::new(
                    expr.clone(),
                    high.clone(),
                    if *negated {
                        ComparisonType::GreaterThan
                    } else {
                        ComparisonType::LessThanOrEqual
                    },
                    vec![expr.clone(), high],
                ));
            
                let result = Expression::Logic(LogicExpression::new(
                    Arc::new(low_compare.clone()),
                    Arc::new(high_compare.clone()),
                    LogicType::And,
                    vec![Arc::new(low_compare), Arc::new(high_compare)],
                ));
            
                Ok(result)
            }
            
            Expr::Function(func) => self.parse_function(func, schema),
            
            Expr::Case {
                operand,
                conditions,
                else_result,
            } => {
                // Parse the base expression if present
                let base_expr = match operand {
                    Some(expr) => Some(Arc::new(self.parse_expression(expr, schema)?)),
                    None => None,
                };
            
                // Extract both conditions and results from the conditions
                // The sqlparser::ast::Expr::Case struct combines WHEN/THEN pairs in a single "conditions" vector
                // where each entry is a (condition, result) pair
                let mut when_exprs = Vec::new();
                let mut then_exprs = Vec::new();
                
                for when_then in conditions {
                    // Each condition has a when (condition) and a then (result)
                    let when_expr = Arc::new(self.parse_expression(&when_then.condition, schema)?);
                    let then_expr = Arc::new(self.parse_expression(&when_then.result, schema)?);
                    
                    when_exprs.push(when_expr);
                    then_exprs.push(then_expr);
                }
            
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
            
            Expr::Cast {
                expr,
                data_type,
                format,
                ..
            } => {
                let inner_expr = Arc::new(self.parse_expression(expr, schema)?);
                let target_type = match data_type {
                    DataType::Int(_) | DataType::Integer(_) => {
                        TypeId::Integer
                    }
                    DataType::BigInt(_) => TypeId::BigInt,
                    DataType::Float(_) | DataType::Double(_) | DataType::Decimal(_) => TypeId::Decimal,
                    DataType::Char(_) => TypeId::Char,
                    DataType::Varchar(_) => TypeId::VarChar,
                    DataType::Boolean => TypeId::Boolean,
                    _ => return Err(format!("Unsupported cast target type: {:?}", data_type)),
                };
            
                let mut cast_expr = CastExpression::new(inner_expr, target_type);
            
                // Handle format if present
                if let Some(format_expr) = format {
                    match format_expr {
                        CastFormat::Value(format_str) => {
                            cast_expr = cast_expr.with_format(format_str.to_string());
                        }
                        CastFormat::ValueAtTimeZone(format_str, timezone) => {
                            // For now, ignore timezone and just use the format string
                            cast_expr = cast_expr.with_format(format_str.to_string());
                        }
                    }
                }
            
                Ok(Expression::Cast(cast_expr))
            }
            
            Expr::Nested(expr) => {
                // For nested expressions, just parse the inner expression
                self.parse_expression(expr, schema)
            }
            
            Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => Ok(self.parse_at_timezone(timestamp, time_zone, schema)?),
            
            Expr::SimilarTo {
                negated,
                expr,
                pattern,
                escape_char,
            } => {
                let parsed_expr = Arc::new(self.parse_expression(expr, schema)?);
                let parsed_pattern = Arc::new(self.parse_expression(pattern, schema)?);
            
                Ok(Expression::Regex(RegexExpression::new(
                    parsed_expr,
                    parsed_pattern,
                    if *negated {
                        RegexOperator::NotSimilarTo
                    } else {
                        RegexOperator::SimilarTo
                    },
                    escape_char.clone(),
                    Column::new("similar_to", TypeId::Boolean),
                )))
            }
            
            Expr::RLike {
                negated,
                expr,
                pattern,
                regexp: _,
            } => {
                let expr = Arc::new(self.parse_expression(expr, schema)?);
                let pattern = Arc::new(self.parse_expression(pattern, schema)?);
            
                Ok(Expression::Regex(RegexExpression::new(
                    expr,
                    pattern,
                    if *negated {
                        RegexOperator::NotRLike
                    } else {
                        RegexOperator::RLike
                    },
                    None,
                    Column::new("rlike", TypeId::Boolean),
                )))
            }
            
            Expr::AnyOp {
                left,
                compare_op,
                right,
                is_some,
            } => {
                let left_expr = Arc::new(self.parse_expression(left, schema)?);
                let right_expr = Arc::new(self.parse_expression(right, schema)?);
            
                Ok(Expression::Any(AnyExpression::new(
                    left_expr,
                    right_expr,
                    compare_op.clone(),
                    *is_some,
                )))
            }
            
            Expr::AllOp {
                left,
                compare_op,
                right,
            } => {
                let left_expr = Arc::new(self.parse_expression(left, schema)?);
                let right_expr = Arc::new(self.parse_expression(right, schema)?);
            
                Ok(Expression::All(AllExpression::new(
                    left_expr,
                    right_expr,
                    compare_op.clone(),
                )))
            }
            
            Expr::Convert {
                is_try,
                expr,
                data_type,
                charset,
                target_before_value: _,
                styles,
            } => {
                let inner_expr = Arc::new(self.parse_expression(&expr, schema)?);
            
                // Parse the target type if specified
                let target_type = match data_type {
                    Some(dtype) => Some(match dtype {
                        DataType::Int(_) | DataType::Integer(_) => TypeId::Integer,
                        DataType::BigInt(_) => TypeId::BigInt,
                        DataType::Float(_) | DataType::Double(_) | DataType::Decimal(_) => {
                            TypeId::Decimal
                        }
                        DataType::Char(_) => TypeId::Char,
                        DataType::Varchar(_) => TypeId::VarChar,
                        DataType::Boolean => TypeId::Boolean,
                        _ => {
                            return Err(format!("Unsupported conversion target type: {:?}", dtype))
                        }
                    }),
                    None => None,
                };
            
                // Parse the character set if specified
                let charset_str = charset.clone().map(|name| name.to_string());
            
                // Parse style expressions
                let style_exprs = styles
                    .iter()
                    .map(|style| self.parse_expression(style, schema))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .map(Arc::new)
                    .collect();
            
                // Determine return type
                let return_type = match target_type {
                    Some(typ) => Column::new("convert_result", typ),
                    None => match charset_str {
                        Some(_) => Column::new("convert_result", TypeId::VarChar),
                        None => inner_expr.get_return_type().clone(),
                    },
                };
            
                Ok(Expression::Convert(ConvertExpression::new(
                    inner_expr,
                    target_type,
                    charset_str,
                    *is_try,
                    style_exprs,
                    return_type,
                )))
            }
            
            Expr::Ceil { expr, field } => {
                let inner_expr = Arc::new(self.parse_expression(expr, schema)?);
            
                let datetime_field = match field {
                    CeilFloorKind::DateTimeField(field) => Some(match field {
                        sqlparser::ast::DateTimeField::Year => DateTimeField::Year,
                        sqlparser::ast::DateTimeField::Month => DateTimeField::Month,
                        sqlparser::ast::DateTimeField::Day => DateTimeField::Day,
                        sqlparser::ast::DateTimeField::Hour => DateTimeField::Hour,
                        sqlparser::ast::DateTimeField::Minute => DateTimeField::Minute,
                        sqlparser::ast::DateTimeField::Second => DateTimeField::Second,
                        sqlparser::ast::DateTimeField::Week(weekday) => {
                            DateTimeField::Week(Some(weekday.clone().unwrap().value))
                        }
                        sqlparser::ast::DateTimeField::DayOfWeek => DateTimeField::DayOfWeek,
                        sqlparser::ast::DateTimeField::DayOfYear => DateTimeField::DayOfYear,
                        sqlparser::ast::DateTimeField::Quarter => DateTimeField::Quarter,
                        sqlparser::ast::DateTimeField::Century => DateTimeField::Century,
                        sqlparser::ast::DateTimeField::Decade => DateTimeField::Decade,
                        sqlparser::ast::DateTimeField::Dow => DateTimeField::Dow,
                        sqlparser::ast::DateTimeField::Doy => DateTimeField::Doy,
                        sqlparser::ast::DateTimeField::Epoch => DateTimeField::Epoch,
                        sqlparser::ast::DateTimeField::Isodow => DateTimeField::Isodow,
                        sqlparser::ast::DateTimeField::Isoyear => DateTimeField::Isoyear,
                        sqlparser::ast::DateTimeField::Julian => DateTimeField::Julian,
                        sqlparser::ast::DateTimeField::Microsecond => DateTimeField::Microsecond,
                        sqlparser::ast::DateTimeField::Microseconds => DateTimeField::Microseconds,
                        sqlparser::ast::DateTimeField::Millennium => DateTimeField::Millennium,
                        sqlparser::ast::DateTimeField::Millisecond => DateTimeField::Millisecond,
                        sqlparser::ast::DateTimeField::Milliseconds => DateTimeField::Milliseconds,
                        sqlparser::ast::DateTimeField::Nanosecond => DateTimeField::Nanosecond,
                        sqlparser::ast::DateTimeField::Nanoseconds => DateTimeField::Nanoseconds,
                        sqlparser::ast::DateTimeField::Timezone => DateTimeField::Timezone,
                        sqlparser::ast::DateTimeField::TimezoneHour => DateTimeField::TimezoneHour,
                        sqlparser::ast::DateTimeField::TimezoneMinute => {
                            DateTimeField::TimezoneMinute
                        }
                        _ => return Err("Unsupported datetime field".to_string()),
                    }),
                    CeilFloorKind::Scale(scale_expr) => {
                        let scale_expr = Expr::Value(sqlparser::ast::ValueWithSpan {
                            value: scale_expr.clone(),
                             span: Span { start: Location::new(0, 0), end:  Location::new(0, 0)},
                        });
                        let scale = Arc::new(self.parse_expression(&scale_expr, schema)?);
                        return Ok(Expression::CeilFloor(CeilFloorExpression::new(
                            CeilFloorOperation::Ceil,
                            inner_expr,
                            Some(scale),
                            None,
                        )?));
                    }
                };
            
                Ok(Expression::CeilFloor(CeilFloorExpression::new(
                    CeilFloorOperation::Ceil,
                    inner_expr,
                    None,
                    datetime_field,
                )?))
            }
            
            Expr::Floor { expr, field } => {
                let inner_expr = Arc::new(self.parse_expression(expr, schema)?);
            
                let datetime_field = match field {
                    CeilFloorKind::DateTimeField(field) => Some(match field {
                        sqlparser::ast::DateTimeField::Year => DateTimeField::Year,
                        sqlparser::ast::DateTimeField::Month => DateTimeField::Month,
                        sqlparser::ast::DateTimeField::Day => DateTimeField::Day,
                        sqlparser::ast::DateTimeField::Hour => DateTimeField::Hour,
                        sqlparser::ast::DateTimeField::Minute => DateTimeField::Minute,
                        sqlparser::ast::DateTimeField::Second => DateTimeField::Second,
                        sqlparser::ast::DateTimeField::Week(weekday) => {
                            DateTimeField::Week(Some(weekday.clone().unwrap().value))
                        }
                        sqlparser::ast::DateTimeField::DayOfWeek => DateTimeField::DayOfWeek,
                        sqlparser::ast::DateTimeField::DayOfYear => DateTimeField::DayOfYear,
                        sqlparser::ast::DateTimeField::Quarter => DateTimeField::Quarter,
                        sqlparser::ast::DateTimeField::Century => DateTimeField::Century,
                        sqlparser::ast::DateTimeField::Decade => DateTimeField::Decade,
                        sqlparser::ast::DateTimeField::Dow => DateTimeField::Dow,
                        sqlparser::ast::DateTimeField::Doy => DateTimeField::Doy,
                        sqlparser::ast::DateTimeField::Epoch => DateTimeField::Epoch,
                        sqlparser::ast::DateTimeField::Isodow => DateTimeField::Isodow,
                        sqlparser::ast::DateTimeField::Isoyear => DateTimeField::Isoyear,
                        sqlparser::ast::DateTimeField::Julian => DateTimeField::Julian,
                        sqlparser::ast::DateTimeField::Microsecond => DateTimeField::Microsecond,
                        sqlparser::ast::DateTimeField::Microseconds => DateTimeField::Microseconds,
                        sqlparser::ast::DateTimeField::Millennium => DateTimeField::Millennium,
                        sqlparser::ast::DateTimeField::Millisecond => DateTimeField::Millisecond,
                        sqlparser::ast::DateTimeField::Milliseconds => DateTimeField::Milliseconds,
                        sqlparser::ast::DateTimeField::Nanosecond => DateTimeField::Nanosecond,
                        sqlparser::ast::DateTimeField::Nanoseconds => DateTimeField::Nanoseconds,
                        sqlparser::ast::DateTimeField::Timezone => DateTimeField::Timezone,
                        sqlparser::ast::DateTimeField::TimezoneHour => DateTimeField::TimezoneHour,
                        sqlparser::ast::DateTimeField::TimezoneMinute => {
                            DateTimeField::TimezoneMinute
                        }
                        _ => return Err("Unsupported datetime field".to_string()),
                    }),
                    CeilFloorKind::Scale(scale_expr) => {
                        let scale_expr = Expr::Value(ValueWithSpan {
                            value: scale_expr.clone(),
                            span: Span { start: Location::new(0,0), end: Location::new(0,0) },
                        });
                        let scale = Arc::new(self.parse_expression(&scale_expr, schema)?);
                        return Ok(Expression::CeilFloor(CeilFloorExpression::new(
                            CeilFloorOperation::Floor,
                            inner_expr,
                            Some(scale),
                            None,
                        )?));
                    }
                };
            
                Ok(Expression::CeilFloor(CeilFloorExpression::new(
                    CeilFloorOperation::Floor,
                    inner_expr,
                    None,
                    datetime_field,
                )?))
            }
            
            Expr::Position { expr, r#in } => {
                let substring_expr = Arc::new(self.parse_expression(expr, schema)?);
                let string_expr = Arc::new(self.parse_expression(r#in, schema)?);
            
                // Validate that both expressions return string types
                let substring_type = substring_expr.get_return_type().get_type();
                let string_type = string_expr.get_return_type().get_type();
            
                if !matches!(substring_type, TypeId::VarChar | TypeId::Char) {
                    return Err(format!(
                        "POSITION substring must be a string type, got {:?}",
                        substring_type
                    ));
                }
            
                if !matches!(string_type, TypeId::VarChar | TypeId::Char) {
                    return Err(format!(
                        "POSITION string must be a string type, got {:?}",
                        string_type
                    ));
                }
            
                Ok(Expression::Position(PositionExpression::new(
                    substring_expr,
                    string_expr,
                )))
            }
            
            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => {
                let base_expr = Arc::new(self.parse_expression(expr, schema)?);
                let overlay_what_expr = Arc::new(self.parse_expression(overlay_what, schema)?);
                let overlay_from_expr = Arc::new(self.parse_expression(overlay_from, schema)?);
            
                let overlay_for_expr = if let Some(for_expr) = overlay_for {
                    Some(Arc::new(self.parse_expression(for_expr, schema)?))
                } else {
                    None
                };
            
                // Determine return type (always a string)
                let return_type = Column::new("overlay_result", TypeId::VarChar);
            
                Ok(Expression::Overlay(OverlayExpression::new(
                    base_expr,
                    overlay_what_expr,
                    overlay_from_expr,
                    overlay_for_expr,
                    return_type,
                )))
            }
            
            Expr::IsDistinctFrom(left, right) => {
                let left_expr = Arc::new(self.parse_expression(left, schema)?);
                let right_expr = Arc::new(self.parse_expression(right, schema)?);
                Ok(Expression::IsDistinct(IsDistinctExpression::new(
                    left_expr,
                    right_expr,
                    true,
                    Column::new("is_distinct", TypeId::Boolean),
                )))
            }
            
            Expr::IsNotDistinctFrom(left, right) => {
                let left_expr = Arc::new(self.parse_expression(left, schema)?);
                let right_expr = Arc::new(self.parse_expression(right, schema)?);
                Ok(Expression::IsDistinct(IsDistinctExpression::new(
                    left_expr,
                    right_expr,
                    false,
                    Column::new("is_not_distinct", TypeId::Boolean),
                )))
            }
            
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let expr = Arc::new(self.parse_expression(expr, schema)?);
                let mut list_exprs = Vec::new();
                for item in list {
                    list_exprs.push(Arc::new(self.parse_expression(item, schema)?));
                }
                // Create a vector expression from the list
                let list_expr = Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new_vector(list_exprs.iter().map(|e| Value::new(Val::Null))),
                    Column::new("list", TypeId::Vector),
                    list_exprs,
                )));
                Ok(Expression::In(InExpression::new_list(
                    expr,
                    list_expr,
                    *negated,
                    Column::new("in_list", TypeId::Boolean),
                )))
            }
            
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let expr = Arc::new(self.parse_expression(expr, schema)?);
                // The subquery parameter is a Query, which is what parse_subquery expects
                let subquery_expr = Arc::new(self.parse_setexpr(subquery, schema)?);
                Ok(Expression::In(InExpression::new_subquery(
                    expr,
                    subquery_expr,
                    *negated,
                    Column::new("in_list", TypeId::Boolean),
                )))
            }
            
            Expr::InUnnest {
                expr,
                array_expr,
                negated,
            } => {
                let parsed_expr = self.parse_expression(expr, schema)?;
                let parsed_array = self.parse_expression(array_expr, schema)?;
                Ok(Expression::In(InExpression::new_unnest(
                    Arc::new(parsed_expr),
                    Arc::new(parsed_array),
                    *negated,
                    Column::new("in_unnest", TypeId::Boolean),
                )))
            }
            
            Expr::Like {
                negated,
                any,
                expr,
                pattern,
                escape_char,
            } => Ok(Expression::Regex(RegexExpression::new(
                Arc::new(self.parse_expression(expr, schema)?),
                Arc::new(self.parse_expression(pattern, schema)?),
                RegexOperator::RLike,
                escape_char.clone(),
                Column::new("like", TypeId::Boolean),
            ))),
            
            Expr::ILike {
                negated,
                any,
                expr,
                pattern,
                escape_char,
            } => Ok(Expression::Regex(RegexExpression::new(
                Arc::new(self.parse_expression(expr, schema)?),
                Arc::new(self.parse_expression(pattern, schema)?),
                RegexOperator::RLike,
                escape_char.clone(),
                Column::new("ilike", TypeId::Boolean),
            ))),
            
            Expr::Extract {
                field,
                syntax,
                expr,
            } => {
                let expr = Arc::new(self.parse_expression(expr, schema)?);
                let field = match field {
                    sqlparser::ast::DateTimeField::Year => ExtractField::Year,
                    sqlparser::ast::DateTimeField::Month => ExtractField::Month,
                    sqlparser::ast::DateTimeField::Day => ExtractField::Day,
                    sqlparser::ast::DateTimeField::Hour => ExtractField::Hour,
                    sqlparser::ast::DateTimeField::Minute => ExtractField::Minute,
                    sqlparser::ast::DateTimeField::Second => ExtractField::Second,
                    sqlparser::ast::DateTimeField::Timezone => ExtractField::Timezone,
                    sqlparser::ast::DateTimeField::Quarter => ExtractField::Quarter,
                    sqlparser::ast::DateTimeField::Week(weekday) => ExtractField::Week,
                    sqlparser::ast::DateTimeField::Dow => ExtractField::DayOfWeek,
                    sqlparser::ast::DateTimeField::Doy => ExtractField::DayOfYear,
                    sqlparser::ast::DateTimeField::Epoch => ExtractField::Epoch,
                    _ => return Err(format!("Unsupported EXTRACT field: {:?}", field)),
                };
                Ok(Expression::Extract(ExtractExpression::new(
                    field,
                    expr,
                    Column::new("extract", TypeId::Integer),
                )))
            }
            
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                special,
                shorthand
            } => {
                let string_expr = Arc::new(self.parse_expression(expr, schema)?);
            
                // Parse the FROM expression
                let from_expr = match substring_from {
                    Some(from) => Arc::new(self.parse_expression(from, schema)?),
                    None => return Err("SUBSTRING requires FROM clause".to_string()),
                };
            
                // Parse the optional FOR expression
                let for_expr = match substring_for {
                    Some(for_expr) => Some(Arc::new(self.parse_expression(for_expr, schema)?)),
                    None => None,
                };
            
                Ok(Expression::Substring(SubstringExpression::new(
                    string_expr,
                    from_expr,
                    for_expr,
                )))
            }
            
            Expr::Trim {
                expr,
                trim_where,
                trim_what,
                trim_characters,
            } => {
                let parsed_expr = Arc::new(self.parse_expression(expr, schema)?);
                let trim_type = match trim_where {
                    Some(sqlparser::ast::TrimWhereField::Leading) => TrimType::Leading,
                    Some(sqlparser::ast::TrimWhereField::Trailing) => TrimType::Trailing,
                    Some(sqlparser::ast::TrimWhereField::Both) => TrimType::Both,
                    None => TrimType::Both,
                };
            
                // Create the children vector with the main expression first
                let mut children = vec![parsed_expr];
            
                // Add trim characters if specified
                if let Some(chars) = trim_characters {
                    // If there are multiple trim characters, concatenate them into a single string
                    if chars.len() > 1 {
                        // Create a string literal expression that concatenates all trim characters
                        let concat_chars = chars
                            .iter()
                            .map(|expr| {
                                match expr {
                                    Expr::Value(ValueWithSpan { 
                                        value: sqlparser::ast::Value::SingleQuotedString(s),
                                        span: _,  // Use _ to ignore the span
                                    }) => {
                                        s.clone()
                                    }
                                    _ => String::new(), // Skip non-string literals
                                }
                            })
                            .collect::<String>();
            
                        children.push(Arc::new(self.parse_expression(
                            &Expr::Value(ValueWithSpan {
                                value: sqlparser::ast::Value::SingleQuotedString(concat_chars),
                                span: Span::new(Location::new(0,0), Location::new(0,0)),
                            }),
                            schema,
                        )?));
                    } else if let Some(first_char) = chars.first() {
                        // If there's only one expression, parse it directly
                        children.push(Arc::new(self.parse_expression(first_char, schema)?));
                    }
                }
            
                Ok(Expression::Trim(TrimExpression::new(
                    trim_type,
                    children,
                    Column::new("trim", TypeId::VarChar),
                )))
            }
            
            Expr::Collate { expr, collation } => {
                let expr = Arc::new(self.parse_expression(expr, schema)?);
                let collation_str = collation.to_string();
                Ok(Expression::Collate(CollateExpression::new(
                    expr,
                    collation_str,
                    Column::new("collate", TypeId::VarChar),
                )))
            }
            
            Expr::TypedString { data_type, value } => {
                Ok(Expression::TypedString(TypedStringExpression::new(
                    data_type.to_string(),
                    value.to_string(),
                    Column::new(
                        "typed_string",
                        match data_type {
                            DataType::Timestamp(_, _) => TypeId::Timestamp,
                            DataType::Date => TypeId::Timestamp,
                            DataType::Time(_, _) => TypeId::Timestamp,
                            _ => TypeId::VarChar,
                        },
                    ),
                )))
            }
            
            Expr::Exists { subquery, negated } => {
                // The schema parameter is now called _outer_schema in parse_subquery
                let subquery_expr = Arc::new(self.parse_subquery(subquery, schema)?);
                Ok(Expression::Exists(ExistsExpression::new(
                    subquery_expr,
                    *negated,
                    Column::new("exists", TypeId::Boolean),
                )))
            }
            
            Expr::Subquery(query) => {
                // The schema parameter is now called _outer_schema in parse_subquery
                self.parse_subquery(query, schema)
            }
            
            Expr::GroupingSets(groups) => {
                let mut parsed_groups = Vec::new();
                for group in groups {
                    let mut parsed_group = Vec::new();
                    for expr in group {
                        parsed_group.push(Arc::new(self.parse_expression(expr, schema)?));
                    }
                    parsed_groups.push(parsed_group);
                }
                Ok(Expression::GroupingSets(GroupingSetsExpression::new(
                    GroupingType::GroupingSets,
                    parsed_groups,
                    Column::new("grouping_sets", TypeId::Vector),
                )))
            }
            
            Expr::Cube(groups) => {
                let mut parsed_groups = Vec::new();
                for group in groups {
                    let mut parsed_group = Vec::new();
                    for expr in group {
                        parsed_group.push(Arc::new(self.parse_expression(expr, schema)?));
                    }
                    parsed_groups.push(parsed_group);
                }
                Ok(Expression::GroupingSets(GroupingSetsExpression::new(
                    GroupingType::Cube,
                    parsed_groups,
                    Column::new("cube", TypeId::Vector),
                )))
            }
            
            Expr::Rollup(groups) => {
                let mut parsed_groups = Vec::new();
                for group in groups {
                    let mut parsed_group = Vec::new();
                    for expr in group {
                        parsed_group.push(Arc::new(self.parse_expression(expr, schema)?));
                    }
                    parsed_groups.push(parsed_group);
                }
                Ok(Expression::GroupingSets(GroupingSetsExpression::new(
                    GroupingType::Rollup,
                    parsed_groups,
                    Column::new("rollup", TypeId::Vector),
                )))
            }
            
            Expr::Tuple(exprs) => {
                // Parse each expression in the tuple
                let parsed_exprs: Result<Vec<Arc<Expression>>, String> = exprs
                    .iter()
                    .map(|expr| self.parse_expression(expr, schema))
                    .collect::<Result<Vec<_>, _>>()
                    .map(|exprs| exprs.into_iter().map(Arc::new).collect());

                // Create the tuple expression with the parsed expressions
                parsed_exprs.map(|exprs| {
                    Expression::Tuple(TupleExpression::new(
                        exprs,
                        Column::new("tuple", TypeId::Vector),
                    ))
                })
            }
            
            Expr::Struct { values, fields } => {
                // Parse each value expression
                let mut parsed_values = Vec::new();
                for value in values {
                    parsed_values.push(Arc::new(self.parse_expression(value, schema)?));
                }

                // Convert SQLParser StructField to our StructField
                let mut struct_fields = Vec::new();
                for field in fields {
                    let field_name = field
                        .field_name
                        .as_ref()
                        .map(|ident| ident.value.clone())
                        .unwrap_or_else(|| String::from(""));

                    // Convert SQL DataType to our TypeId
                    let type_id = match field.field_type {
                        DataType::Int(_) | DataType::Integer(_) => TypeId::Integer,
                        DataType::BigInt(_) => TypeId::BigInt,
                        DataType::SmallInt(_) => TypeId::SmallInt,
                        DataType::TinyInt(_) => TypeId::TinyInt,
                        DataType::Float(_) | DataType::Double(_) | DataType::Decimal(_) => {
                            TypeId::Decimal
                        }
                        DataType::Char(_) => TypeId::Char,
                        DataType::Varchar(_) | DataType::Text => TypeId::VarChar,
                        DataType::Boolean => TypeId::Boolean,
                        DataType::Date | DataType::Time(_, _) | DataType::Timestamp(_, _) => {
                            TypeId::Timestamp
                        }
                        DataType::Array(_) => TypeId::Vector,
                        DataType::Struct(_, _) => TypeId::Struct,
                        _ => {
                            return Err(format!(
                                "Unsupported struct field type: {:?}",
                                field.field_type
                            ))
                        }
                    };

                    struct_fields.push(StructField::new(
                        field_name.clone(),
                        Column::new(&field_name, type_id),
                    ));
                }

                // Create return type for the struct
                let return_type = Column::new("struct", TypeId::Struct);

                // Create and return the StructExpression
                Ok(Expression::Struct(StructExpression::new(
                    parsed_values.clone(),
                    struct_fields,
                    return_type,
                )))
            }
            
            Expr::Array(array) => {
                // Parse each element in the array
                let mut elements = Vec::new();
                let mut element_type = TypeId::Invalid;

                // Parse each element expression
                for elem in &array.elem {
                    let parsed_elem = Arc::new(self.parse_expression(elem, schema)?);

                    // If this is the first element, use its type as the element type
                    if element_type == TypeId::Invalid {
                        element_type = parsed_elem.get_return_type().get_type();
                    } else {
                        // Check that all elements have compatible types
                        let curr_type = parsed_elem.get_return_type().get_type();
                        let type_instance = crate::types_db::types::get_instance(element_type);
                        if !type_instance.is_coercible_from(curr_type) {
                            return Err(format!(
                                "Array element type mismatch: cannot mix {:?} and {:?}",
                                element_type, curr_type
                            ));
                        }
                    }

                    elements.push(parsed_elem);
                }

                // If array is empty, default to integer array type
                if element_type == TypeId::Invalid {
                    element_type = TypeId::Integer;
                }

                Ok(Expression::Array(ArrayExpression::new(
                    elements,
                    Column::new("array", TypeId::Vector),
                )))
            }
            
            Expr::Interval(interval) => {
                // Parse the interval value expression
                let value_expr = self.parse_expression(&interval.value, schema)?;

                // Convert sqlparser::ast::DateTimeField to our IntervalField
                let field = match &interval.leading_field {
                    Some(sqlparser::ast::DateTimeField::Year) => IntervalField::Year,
                    Some(sqlparser::ast::DateTimeField::Month) => IntervalField::Month,
                    Some(sqlparser::ast::DateTimeField::Day) => IntervalField::Day,
                    Some(sqlparser::ast::DateTimeField::Hour) => IntervalField::Hour,
                    Some(sqlparser::ast::DateTimeField::Minute) => IntervalField::Minute,
                    Some(sqlparser::ast::DateTimeField::Second) => IntervalField::Second,
                    _ => return Err("Unsupported interval field".to_string()),
                };

                // Create interval expression
                Ok(Expression::Interval(IntervalExpression::new(
                    field,
                    Arc::new(value_expr),
                    Column::new("interval", TypeId::Struct),
                )))
            }
            
            Expr::Wildcard(token) => {
                // Create a wildcard expression that will return all columns as a vector
                Ok(Expression::Wildcard(WildcardExpression::new(Column::new(
                    "*",
                    TypeId::Vector,
                ))))
            }
            
            Expr::QualifiedWildcard(name, _) => {
                // Extract the qualifier parts (e.g., ["schema", "table"] from schema.table.*)
                let qualifier: Vec<String> = name.0.iter().map(|i| i.as_ident().unwrap().value.clone()).collect();

                // Create a qualified wildcard expression
                Ok(Expression::QualifiedWildcard(
                    QualifiedWildcardExpression::new(
                        qualifier,
                        Column::new(
                            &format!(
                                "{}.*",
                                name.0
                                    .iter()
                                    .map(|i| i.as_ident().unwrap().value.clone())
                                    .collect::<Vec<_>>()
                                    .join(".")
                            ),
                            TypeId::Vector,
                        ),
                    ),
                ))
            }
            
            Expr::CompoundFieldAccess { root, access_chain } => {
                // First, parse the root expression
                let mut expr = self.parse_expression(root, schema)?;
                
                // Apply each access in the chain sequentially
                for access in access_chain {
                    match access {
                        AccessExpr::Dot(field_expr) => {
                            // For dot notation, the field is usually an identifier
                            if let Expr::Identifier(ident) = field_expr {
                                let field_name = ident.value.clone();
                                
                                // Get the type of the current expression
                                let curr_type = expr.get_return_type().get_type();
                                
                                if curr_type == TypeId::Struct {
                                    // Access field in a struct
                                    expr = Expression::MapAccess(MapAccessExpression::new(
                                        Arc::new(expr),
                                        vec![MapAccessKey::String(field_name.clone())],
                                        Column::new(&format!("field_{}", field_name), TypeId::Invalid),
                                    ));
                                } else {
                                    return Err(format!(
                                        "Dot notation field access only supported for struct types, got {:?}",
                                        curr_type
                                    ));
                                }
                            } else {
                                return Err("Dot notation field access requires identifier".to_string());
                            }
                        },
                        AccessExpr::Subscript(subscript) => {
                            match subscript {
                                SQLSubscript::Index { index } => {
                                    // Parse the index expression
                                    let index_expr = Arc::new(self.parse_expression(&index, schema)?);
                                    let curr_type = expr.get_return_type().get_type();
                                    
                                    if curr_type == TypeId::Vector || curr_type == TypeId::Array {
                                        // For array/vector access
                                        expr = Expression::Subscript(SubscriptExpression::new(
                                            Arc::new(expr),
                                            InternalSubscript::Single(index_expr),
                                            Column::new("array_element", TypeId::Invalid),
                                        ));
                                    } else if curr_type == TypeId::Struct || curr_type == TypeId::JSON {
                                        // For map/struct access with bracket notation
                                        // Determine if this is a numeric or string index
                                        match &index {
                                            Expr::Value(value) => {
                                                if let SQLValue::SingleQuotedString(s) = &value.value {
                                                    // String key for map access
                                                    expr = Expression::MapAccess(MapAccessExpression::new(
                                                        Arc::new(expr),
                                                        vec![MapAccessKey::String(s.clone())],
                                                        Column::new(&format!("field_{}", s), TypeId::Invalid),
                                                    ));
                                                } else if let SQLValue::Number(n, _) = &value.value {
                                                    // Numeric index for array access within a struct
                                                    expr = Expression::MapAccess(MapAccessExpression::new(
                                                        Arc::new(expr),
                                                        vec![MapAccessKey::Number(n.parse::<i64>().unwrap_or(0))],
                                                        Column::new(&format!("index_{}", n), TypeId::Invalid),
                                                    ));
                                                } else {
                                                    return Err(format!(
                                                        "Unsupported map/struct index type: {:?}", 
                                                        value
                                                    ));
                                                }
                                            },
                                            Expr::Identifier(ident) => {
                                                // Identifier as a string key
                                                expr = Expression::MapAccess(MapAccessExpression::new(
                                                    Arc::new(expr),
                                                    vec![MapAccessKey::String(ident.value.clone())],
                                                    Column::new(&format!("field_{}", ident.value), TypeId::Invalid),
                                                ));
                                            },
                                            _ => {
                                                // For dynamic keys, we'll need to use a different approach
                                                // Let's wrap the index expression in a function or method call
                                                let func_name = format!("access_at_index");
                                                expr = Expression::Function(FunctionExpression::new(
                                                    func_name,
                                                    vec![Arc::new(expr), index_expr],
                                                    Column::new("dynamic_field", TypeId::Invalid),
                                                ));
                                            }
                                        }
                                    } else {
                                        return Err(format!(
                                            "Subscript access only supported for array, vector, struct, or JSON types, got {:?}",
                                            curr_type
                                        ));
                                    }
                                },
                                SQLSubscript::Slice { lower_bound, upper_bound, stride } => {
                                    // Handle array slicing - convert SQL expressions to our expressions
                                    let lower_expr = lower_bound.as_ref().map(|e| 
                                        Arc::new(self.parse_expression(e, schema).unwrap_or_else(|_| 
                                            Expression::Constant(ConstantExpression::new(
                                                Value::new(0),
                                                Column::new("lower_bound", TypeId::Integer),
                                                vec![],
                                            ))
                                        ))
                                    );
                                    
                                    let upper_expr = upper_bound.as_ref().map(|e| 
                                        Arc::new(self.parse_expression(e, schema).unwrap_or_else(|_| 
                                            Expression::Constant(ConstantExpression::new(
                                                Value::new(-1), // -1 indicates the end of array
                                                Column::new("upper_bound", TypeId::Integer),
                                                vec![],
                                            ))
                                        ))
                                    );
                                    
                                    // Create Range subscript
                                    let slice_subscript = InternalSubscript::Range {
                                        start: lower_expr,
                                        end: upper_expr,
                                    };
                                    
                                    expr = Expression::Subscript(SubscriptExpression::new(
                                        Arc::new(expr),
                                        slice_subscript,
                                        Column::new("array_slice", TypeId::Vector),
                                    ));
                                }
                            }
                        }
                    }
                }
                
                Ok(expr)
            }
            
            Expr::JsonAccess { .. } => {
                Err("JSON access expressions are not yet supported".to_string())
            }
            
            _ => Err(format!("Unsupported expression type: {:?}", expr)),
        }
    }

    pub fn prepare_table_scan(
        &self,
        select: &Box<Select>,
    ) -> Result<(String, Schema, u64), String> {
        debug!("Preparing table scan for select: {:?}", select);
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

    pub fn parse_select_statements(
        &self,
        select: &Box<Select>,
        schema: &Schema,
    ) -> Result<Expression, String> {
        debug!("Parsing select statements: {:?}", select);
        // Ensure we have at least one projection item
        if select.projection.is_empty() {
            return Err("Empty SELECT statement in subquery".to_string());
        }

        // For subqueries, we currently only support single-column results
        if select.projection.len() > 1 {
            return Err("Subqueries must return exactly one column".to_string());
        }

        // Parse the first (and only) projection item
        match &select.projection[0] {
            SelectItem::UnnamedExpr(expr) => self.parse_expression(expr, schema),
            SelectItem::ExprWithAlias { expr, .. } => self.parse_expression(expr, schema),
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                Err("Wildcards are not supported in subqueries".to_string())
            }
        }
    }

    /// Process a join operator and constraint to create a join predicate
    fn process_join_operator(
        &self,
        join_operator: &JoinOperator,
        left_schema: &Schema,
        right_schema: &Schema,
        left_alias: Option<&str>,
        right_alias: Option<&str>,
    ) -> Result<(Expression, JoinOperator), String> {
        let combined_schema =
            Schema::merge_with_aliases(left_schema, right_schema, left_alias, right_alias);

        match join_operator {
            JoinOperator::Inner(constraint) => {
                let predicate = self.process_join_constraint(
                    constraint,
                    left_schema,
                    right_schema,
                    &combined_schema,
                )?;
                Ok((predicate, join_operator.clone()))
            }
            JoinOperator::LeftOuter(constraint) => {
                let predicate = self.process_join_constraint(
                    constraint,
                    left_schema,
                    right_schema,
                    &combined_schema,
                )?;
                Ok((predicate, join_operator.clone()))
            }
            JoinOperator::RightOuter(constraint) => {
                let predicate = self.process_join_constraint(
                    constraint,
                    left_schema,
                    right_schema,
                    &combined_schema,
                )?;
                Ok((predicate, join_operator.clone()))
            }
            JoinOperator::FullOuter(constraint) => {
                let predicate = self.process_join_constraint(
                    constraint,
                    left_schema,
                    right_schema,
                    &combined_schema,
                )?;
                Ok((predicate, join_operator.clone()))
            }
            JoinOperator::CrossJoin => {
                // For CROSS JOIN, we create a constant TRUE predicate
                let predicate = Expression::Constant(ConstantExpression::new(
                    Value::new(true),
                    Column::new("TRUE", TypeId::Boolean),
                    vec![],
                ));
                Ok((predicate, join_operator.clone()))
            }
            JoinOperator::Semi(constraint) => {
                let predicate = self.process_join_constraint(
                    constraint,
                    left_schema,
                    right_schema,
                    &combined_schema,
                )?;
                Ok((predicate, join_operator.clone()))
            }
            JoinOperator::LeftSemi(constraint) => {
                let predicate = self.process_join_constraint(
                    constraint,
                    left_schema,
                    right_schema,
                    &combined_schema,
                )?;
                Ok((predicate, join_operator.clone()))
            }
            JoinOperator::RightSemi(constraint) => {
                let predicate = self.process_join_constraint(
                    constraint,
                    left_schema,
                    right_schema,
                    &combined_schema,
                )?;
                Ok((predicate, join_operator.clone()))
            }
            JoinOperator::Anti(constraint) => {
                let predicate = self.process_join_constraint(
                    constraint,
                    left_schema,
                    right_schema,
                    &combined_schema,
                )?;
                Ok((predicate, join_operator.clone()))
            }
            JoinOperator::LeftAnti(constraint) => {
                let predicate = self.process_join_constraint(
                    constraint,
                    left_schema,
                    right_schema,
                    &combined_schema,
                )?;
                Ok((predicate, join_operator.clone()))
            }
            JoinOperator::RightAnti(constraint) => {
                let predicate = self.process_join_constraint(
                    constraint,
                    left_schema,
                    right_schema,
                    &combined_schema,
                )?;
                Ok((predicate, join_operator.clone()))
            }
            JoinOperator::CrossApply => {
                // For CROSS APPLY, we create a constant TRUE predicate
                let predicate = Expression::Constant(ConstantExpression::new(
                    Value::new(true),
                    Column::new("TRUE", TypeId::Boolean),
                    vec![],
                ));
                Ok((predicate, join_operator.clone()))
            }
            JoinOperator::OuterApply => {
                // For OUTER APPLY, we create a constant TRUE predicate
                let predicate = Expression::Constant(ConstantExpression::new(
                    Value::new(true),
                    Column::new("TRUE", TypeId::Boolean),
                    vec![],
                ));
                Ok((predicate, join_operator.clone()))
            }
            JoinOperator::AsOf {
                match_condition,
                constraint,
            } => {
                // Parse the match condition
                let match_expr = self.parse_expression(match_condition, &combined_schema)?;

                // Parse the constraint
                let constraint_expr = self.process_join_constraint(
                    constraint,
                    left_schema,
                    right_schema,
                    &combined_schema,
                )?;

                // Combine the match condition and constraint with AND
                let left_expr = Arc::new(match_expr);
                let right_expr = Arc::new(constraint_expr);
                let predicate = Expression::Logic(LogicExpression::new(
                    left_expr.clone(),
                    right_expr.clone(),
                    LogicType::And,
                    vec![left_expr, right_expr],
                ));

                Ok((predicate, join_operator.clone()))
            }
            // Handle the additional JoinOperator variants
            JoinOperator::Join(constraint) => {
                // Treat like INNER JOIN
                let predicate = self.process_join_constraint(
                    constraint,
                    left_schema,
                    right_schema,
                    &combined_schema,
                )?;
                Ok((predicate, join_operator.clone()))
            }
            JoinOperator::Left(constraint) => {
                // Treat like LEFT OUTER JOIN
                let predicate = self.process_join_constraint(
                    constraint,
                    left_schema,
                    right_schema,
                    &combined_schema,
                )?;
                Ok((predicate, join_operator.clone()))
            }
            JoinOperator::Right(constraint) => {
                // Treat like RIGHT OUTER JOIN
                let predicate = self.process_join_constraint(
                    constraint,
                    left_schema,
                    right_schema,
                    &combined_schema,
                )?;
                Ok((predicate, join_operator.clone()))
            }
            JoinOperator::StraightJoin(constraint) => {
                // Treat like INNER JOIN
                let predicate = self.process_join_constraint(
                    constraint,
                    left_schema,
                    right_schema,
                    &combined_schema,
                )?;
                Ok((predicate, join_operator.clone()))
            }
        }
    }

    /// Process a join constraint to create a join predicate
    fn process_join_constraint(
        &self,
        constraint: &JoinConstraint,
        left_schema: &Schema,
        right_schema: &Schema,
        combined_schema: &Schema,
    ) -> Result<Expression, String> {
        match constraint {
            JoinConstraint::On(expr) => {
                // Parse the ON expression
                self.parse_expression(expr, combined_schema)
            }
            JoinConstraint::Using(columns) => {
                // For USING, we create equality predicates for each column
                let mut predicates = Vec::new();

                for ident in columns {
                    let column_name = match &ident.0[0] {
                        ObjectNamePart::Identifier(ident) => ident.value.clone(),
                    };

                    // Find the column in both schemas
                    let left_col_idx =
                        left_schema.get_column_index(&column_name).ok_or_else(|| {
                            format!("Column '{}' not found in left table", column_name)
                        })?;
                    let right_col_idx =
                        right_schema.get_column_index(&column_name).ok_or_else(|| {
                            format!("Column '{}' not found in right table", column_name)
                        })?;

                    // Create column references
                    let left_col = left_schema.get_column(left_col_idx).unwrap().clone();
                    let right_col = right_schema.get_column(right_col_idx).unwrap().clone();

                    let left_expr = Expression::ColumnRef(ColumnRefExpression::new(
                        left_col_idx,
                        0, // tuple index for left table
                        left_col.clone(),
                        vec![],
                    ));

                    let right_expr = Expression::ColumnRef(ColumnRefExpression::new(
                        right_col_idx + left_schema.get_column_count() as usize,
                        1, // tuple index for right table
                        right_col.clone(),
                        vec![],
                    ));

                    // Create equality predicate
                    let left_arc = Arc::new(left_expr);
                    let right_arc = Arc::new(right_expr);
                    let predicate = Expression::Comparison(ComparisonExpression::new(
                        left_arc.clone(),
                        right_arc.clone(),
                        ComparisonType::Equal,
                        vec![left_arc, right_arc],
                    ));

                    predicates.push(Arc::new(predicate));
                }

                if predicates.is_empty() {
                    return Err("USING clause must contain at least one column".to_string());
                }

                // Combine all predicates with AND
                if predicates.len() == 1 {
                    Ok(predicates[0].as_ref().clone())
                } else {
                    // Create a dummy left and right for the LogicExpression
                    let left = predicates[0].clone();
                    let right = predicates[1].clone();

                    Ok(Expression::Logic(LogicExpression::new(
                        left,
                        right,
                        LogicType::And,
                        predicates,
                    )))
                }
            }
            JoinConstraint::Natural => {
                // For NATURAL JOIN, find common columns in both schemas and create equality predicates
                let mut predicates = Vec::new();

                for left_idx in 0..left_schema.get_column_count() as usize {
                    let left_col = left_schema.get_column(left_idx).unwrap();
                    let column_name = left_col.get_name();

                    // Check if the column exists in the right schema
                    if let Some(right_idx) = right_schema.get_column_index(column_name) {
                        let right_col = right_schema.get_column(right_idx).unwrap();

                        // Create column references
                        let left_expr = Expression::ColumnRef(ColumnRefExpression::new(
                            left_idx,
                            0, // tuple index for left table
                            left_col.clone(),
                            vec![],
                        ));

                        let right_expr = Expression::ColumnRef(ColumnRefExpression::new(
                            right_idx + left_schema.get_column_count() as usize,
                            1, // tuple index for right table
                            right_col.clone(),
                            vec![],
                        ));

                        // Create equality predicate
                        let left_arc = Arc::new(left_expr);
                        let right_arc = Arc::new(right_expr);
                        let predicate = Expression::Comparison(ComparisonExpression::new(
                            left_arc.clone(),
                            right_arc.clone(),
                            ComparisonType::Equal,
                            vec![left_arc, right_arc],
                        ));

                        predicates.push(Arc::new(predicate));
                    }
                }

                if predicates.is_empty() {
                    return Err(
                        "NATURAL JOIN requires at least one common column between tables"
                            .to_string(),
                    );
                }

                // Combine all predicates with AND
                if predicates.len() == 1 {
                    Ok(predicates[0].as_ref().clone())
                } else {
                    // Create a dummy left and right for the LogicExpression
                    let left = predicates[0].clone();
                    let right = predicates[1].clone();

                    Ok(Expression::Logic(LogicExpression::new(
                        left,
                        right,
                        LogicType::And,
                        predicates,
                    )))
                }
            }
            JoinConstraint::None => {
                // For no constraint (CROSS JOIN), create a constant TRUE predicate
                Ok(Expression::Constant(ConstantExpression::new(
                    Value::new(true),
                    Column::new("TRUE", TypeId::Boolean),
                    vec![],
                )))
            }
        }
    }

    pub fn extract_table_name(&self, table_name: &ObjectName) -> Result<String, String> {
        debug!("Extracting table name from: {:?}", table_name);
        match table_name {
            ObjectName(parts) if parts.len() == 1 => {
                match &parts[0] {
                    ObjectNamePart::Identifier(ident) => Ok(ident.value.clone()),
                }
            },
            _ => Err("Only single table INSERT statements are supported".to_string()),
        }
    }

    pub fn get_table_schema(&self, table_name: &str) -> Result<Schema, String> {
        debug!("Getting schema for table: {}", table_name);
        let catalog = self.catalog.read();
        catalog
            .get_table_schema(table_name)
            .ok_or_else(|| format!("Table '{}' not found in catalog", table_name))
    }

    pub fn get_table_oid(&self, table_name: &str) -> Result<TableOidT, String> {
        debug!("Getting OID for table: {}", table_name);
        let catalog = self.catalog.read();
        catalog
            .get_table(table_name)
            .map(|table| table.get_table_oidt())
            .ok_or_else(|| format!("Table '{}' not found in catalog", table_name))
    }

    pub fn parse_join_condition(
        &self,
        expr: &Expr,
        left_schema: &Schema,
        right_schema: &Schema,
    ) -> Result<Expression, String> {
        debug!("Parsing join condition: {:?}", expr);
        // Create a combined schema for parsing the join condition
        let combined_schema = Schema::merge(left_schema, right_schema);

        // Parse the expression using the combined schema
        let parsed_expr = self.parse_expression(expr, &combined_schema)?;

        // Verify the expression is a valid join condition (should be a comparison)
        match &parsed_expr {
            Expression::Comparison(_) => Ok(parsed_expr),
            Expression::Logic(logic_expr) => {
                // Allow AND of multiple comparisons
                let children = logic_expr.get_children();
                for child in children {
                    match child.as_ref() {
                        Expression::Comparison(_) => (),
                        _ => {
                            return Err("Join condition must be a comparison or AND of comparisons"
                                .to_string())
                        }
                    }
                }
                Ok(parsed_expr)
            }
            _ => Err("Join condition must be a comparison expression".to_string()),
        }
    }

    pub fn determine_group_by_expressions(
        &self,
        select: &Box<Select>,
        schema: &Schema,
        has_group_by: bool,
    ) -> Result<Vec<Expression>, String> {
        debug!(
            "Determining group by expressions, has_group_by: {}",
            has_group_by
        );
        if !has_group_by {
            return Ok(Vec::new());
        }

        match &select.group_by {
            GroupByExpr::Expressions(exprs, _) => {
                let mut group_by_exprs = Vec::new();
                for expr in exprs {
                    let parsed_expr = self.parse_expression(expr, schema)?;
                    group_by_exprs.push(parsed_expr);
                }
                Ok(group_by_exprs)
            }
            GroupByExpr::All(_) => {
                // For GROUP BY ALL, include all non-aggregate columns
                let mut group_by_exprs = Vec::new();
                for i in 0..schema.get_column_count() {
                    let col = schema.get_column(i as usize).unwrap();
                    group_by_exprs.push(Expression::ColumnRef(ColumnRefExpression::new(
                        0,
                        i as usize,
                        col.clone(),
                        vec![],
                    )));
                }
                Ok(group_by_exprs)
            }
        }
    }

    pub fn resolve_column_ref_after_join(
        &self,
        table_alias: &str,
        column_name: &str,
        schema: &Schema,
    ) -> Result<usize, String> {
        debug!(
            "Resolving column reference after join: {}.{}",
            table_alias, column_name
        );
        log::debug!(
            "Resolving column reference: table_alias='{}', column_name='{}' in schema with {} columns",
            table_alias,
            column_name,
            schema.get_column_count()
        );

        // Debug: Print all columns in the schema
        for i in 0..schema.get_column_count() as usize {
            let col = schema.get_column(i).unwrap();
            log::debug!(
                "Schema column {}: name='{}', type={:?}",
                i,
                col.get_name(),
                col.get_type()
            );
        }

        // First, try to find a fully qualified column name
        let qualified_name = format!("{}.{}", table_alias, column_name);
        log::debug!("Looking for qualified name: '{}'", qualified_name);

        for i in 0..schema.get_column_count() as usize {
            let col = schema.get_column(i).unwrap();
            if col.get_name() == qualified_name {
                log::debug!("Found qualified column '{}' at index {}", qualified_name, i);
                return Ok(i);
            }
        }
        log::debug!("Qualified column '{}' not found", qualified_name);

        // Second, try to find any column that starts with the given table alias prefix
        log::debug!("Looking for columns with prefix: '{}'", table_alias);
        for i in 0..schema.get_column_count() as usize {
            let col = schema.get_column(i).unwrap();
            let col_name = col.get_name();
            if let Some(dot_pos) = col_name.find('.') {
                let prefix = &col_name[0..dot_pos];
                let suffix = &col_name[dot_pos + 1..];

                log::debug!(
                    "Checking column '{}': prefix='{}', suffix='{}'",
                    col_name,
                    prefix,
                    suffix
                );

                if prefix == table_alias && suffix == column_name {
                    log::debug!(
                        "Found column with matching prefix and suffix at index {}",
                        i
                    );
                    return Ok(i);
                }
            }
        }
        log::debug!("No column found with prefix '{}'", table_alias);

        // Third, try to find the column by its unqualified name
        log::debug!("Looking for unqualified column name: '{}'", column_name);
        for i in 0..schema.get_column_count() as usize {
            let col = schema.get_column(i).unwrap();
            if col.get_name() == column_name {
                log::debug!("Found unqualified column '{}' at index {}", column_name, i);
                return Ok(i);
            }
        }
        log::debug!("Unqualified column '{}' not found", column_name);

        // Fourth, check if the table alias refers to an actual table in the catalog
        log::debug!(
            "Checking if '{}' is a valid table in the catalog",
            table_alias
        );
        let catalog = self.catalog.read();
        if let Some(table) = catalog.get_table(table_alias) {
            log::debug!("Found table '{}' in catalog", table_alias);
            let table_schema = table.get_table_schema();

            // Debug: Print all columns in the table schema
            log::debug!(
                "Table '{}' schema has {} columns:",
                table_alias,
                table_schema.get_column_count()
            );
            for i in 0..table_schema.get_column_count() as usize {
                let col = table_schema.get_column(i).unwrap();
                log::debug!(
                    "  Table column {}: name='{}', type={:?}",
                    i,
                    col.get_name(),
                    col.get_type()
                );
            }

            if let Some(col_idx) = table_schema.get_column_index(column_name) {
                log::debug!(
                    "Found column '{}' in table '{}' schema at index {}",
                    column_name,
                    table_alias,
                    col_idx
                );

                // Now find this column in the joined schema
                for i in 0..schema.get_column_count() as usize {
                    let col = schema.get_column(i).unwrap();
                    let col_name = col.get_name();
                    log::debug!(
                        "Checking if schema column '{}' matches table column '{}.{}'",
                        col_name,
                        table_alias,
                        column_name
                    );

                    if col_name == column_name || col_name == qualified_name {
                        log::debug!("Found matching column at index {}", i);
                        return Ok(i);
                    }

                    if let Some(dot_pos) = col_name.find('.') {
                        let prefix = &col_name[0..dot_pos];
                        let suffix = &col_name[dot_pos + 1..];

                        if prefix == table_alias && suffix == column_name {
                            log::debug!("Found matching column with prefix at index {}", i);
                            return Ok(i);
                        }
                    }
                }
            } else {
                log::debug!(
                    "Column '{}' not found in table '{}' schema",
                    column_name,
                    table_alias
                );
            }
        } else {
            log::debug!("Table '{}' not found in catalog", table_alias);
        }

        // Fifth, try to find any column that ends with the column name, regardless of prefix
        log::debug!("Looking for any column ending with: '{}'", column_name);
        for i in 0..schema.get_column_count() as usize {
            let col = schema.get_column(i).unwrap();
            let col_name = col.get_name();
            if let Some(dot_pos) = col_name.find('.') {
                let suffix = &col_name[dot_pos + 1..];

                log::debug!("Checking column '{}': suffix='{}'", col_name, suffix);

                if suffix == column_name {
                    log::debug!("Found column with matching suffix at index {}", i);
                    return Ok(i);
                }
            }
        }
        log::debug!("No column found ending with '{}'", column_name);

        // If we get here, we couldn't find the column
        let error_msg = format!("Column {}.{} not found in schema", table_alias, column_name);
        log::debug!("{}", error_msg);
        Err(error_msg)
    }

    fn parse_at_timezone(
        &self,
        timestamp: &Expr,
        timezone: &Expr,
        schema: &Schema,
    ) -> Result<Expression, String> {
        debug!(
            "Parsing AT TIME ZONE expression: timestamp={:?}, timezone={:?}",
            timestamp, timezone
        );

        // For typed string timestamps, we need to ensure they output RFC3339 format
        let timestamp_expr = match timestamp {
            Expr::TypedString { data_type, value } => {
                debug!(
                    "Processing typed string timestamp: type={:?}, value={}",
                    data_type, value
                );
                // Only allow TIMESTAMP typed strings
                if !matches!(data_type, DataType::Timestamp(_, _)) {
                    debug!("Invalid timestamp data type: {:?}", data_type);
                    return Err(format!(
                        "AT TIME ZONE timestamp must be a timestamp type, got {:?}",
                        data_type
                    ));
                }
                // Create a TypedStringExpression that outputs VarChar for AT TIME ZONE
                Arc::new(Expression::TypedString(TypedStringExpression::new(
                    "TIMESTAMP".to_string(),
                    value.to_string(),
                    Column::new("timestamp", TypeId::Timestamp),
                )))
            }
            _ => {
                debug!("Processing regular timestamp expression");
                // For other expressions, parse normally
                let expr = self.parse_expression(timestamp, schema)?;

                // Verify the expression returns a timestamp type
                if expr.get_return_type().get_type() != TypeId::Timestamp {
                    debug!(
                        "Invalid timestamp expression type: {:?}",
                        expr.get_return_type().get_type()
                    );
                    return Err(format!(
                        "AT TIME ZONE timestamp expression must return timestamp type, got {:?}",
                        expr.get_return_type().get_type()
                    ));
                }

                Arc::new(expr)
            }
        };

        // Parse the timezone expression
        let timezone_expr = match timezone {
            Expr::Value(sqlparser::ast::ValueWithSpan { value: tz_value, span }) => {
                debug!("Processing timezone string literal: {:?}", tz_value);
                // Extract the string value from the sqlparser Value
                let tz_str = Value::from_sqlparser_value(&tz_value)
                    .map_err(|e| format!("Failed to extract timezone string: {}", e))?;
                
                // Create a constant expression for the timezone string
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(tz_str),
                    Column::new("timezone", TypeId::VarChar),
                    Vec::new(), // No children for constant expression
                )))
            }
            _ => {
                debug!("Processing regular timezone expression");
                // For other expressions, parse normally
                let expr = self.parse_expression(timezone, schema)?;

                // Verify the expression returns a string type
                if expr.get_return_type().get_type() != TypeId::VarChar {
                    debug!(
                        "Invalid timezone expression type: {:?}",
                        expr.get_return_type().get_type()
                    );
                    return Err(format!(
                        "AT TIME ZONE timezone expression must return string type, got {:?}",
                        expr.get_return_type().get_type()
                    ));
                }

                Arc::new(expr)
            }
        };

        debug!("Creating AT_TIMEZONE function with timestamp and timezone expressions");
        // Create the AT_TIMEZONE function expression
        Ok(Expression::Function(FunctionExpression::new(
            "AT_TIMEZONE".to_string(),
            vec![timestamp_expr, timezone_expr],
            Column::new("AT_TIMEZONE", TypeId::Timestamp),
        )))
    }

    fn parse_setexpr(&self, set_expr: &SetExpr, _outer_schema: &Schema) -> Result<Expression, String> {
        debug!("Parsing set_expr: {:?}", set_expr);

        // For now, we only support SELECT expressions
        match set_expr {
            SetExpr::Select(select) => {
                // Get the schema for the tables in the subquery's FROM clause
                let subquery_schema = if !select.from.is_empty() {
                    // Get the first table in the FROM clause
                    match &select.from[0].relation {
                        TableFactor::Table { name, .. } => {
                            // Extract the table name and get its schema
                            let table_name = self.extract_table_name(name)?;
                            debug!("Subquery references table: {}", table_name);
                            self.get_table_schema(&table_name)?
                        }
                        _ => {
                            debug!("Unsupported table factor in subquery, falling back to outer schema");
                            _outer_schema.clone()
                        }
                    }
                } else {
                    debug!("No FROM clause in subquery, using outer schema");
                    _outer_schema.clone()
                };

                debug!("Using schema for subquery: {:?}", subquery_schema);

                // Parse the SELECT statement with the correct schema
                let select_expr = Arc::new(self.parse_select_statements(select, &subquery_schema)?);

                // Determine the appropriate return type for the subquery
                let (subquery_type, return_type) =
                    self.determine_subquery_type_and_return_type(select, &subquery_schema)?;

                // Create a SubqueryExpression with appropriate type and return type
                Ok(Expression::Subquery(SubqueryExpression::new(
                    select_expr,
                    subquery_type,
                    return_type,
                )))
            }
            SetExpr::Values(_) => {
                Err("VALUES expressions in subqueries are not yet supported".to_string())
            }
            SetExpr::SetOperation { .. } => {
                Err("Set operations in subqueries are not yet supported".to_string())
            }
            SetExpr::Insert(_) => {
                Err("INSERT expressions in subqueries are not yet supported".to_string())
            }
            SetExpr::Query(_) => {
                Err("Nested queries in subqueries are not yet supported".to_string())
            }
            SetExpr::Table(_) => {
                Err("TABLE expressions in subqueries are not yet supported".to_string())
            }
            _ => Err("Unsupported subquery type".to_string()),
        }
    }

    fn parse_subquery(&self, query: &Query, _outer_schema: &Schema) -> Result<Expression, String> {
        debug!("Parsing subquery: {:?}", query);
        // Extract the body of the query
        let body = &query.body;

        // For now, we only support SELECT expressions
        self.parse_setexpr(body, _outer_schema)
    }

    // Helper method to determine the subquery type and return type
    fn determine_subquery_type_and_return_type(
        &self,
        select: &Box<Select>,
        schema: &Schema,
    ) -> Result<(SubqueryType, Column), String> {
        // Check if this is a scalar subquery (single column, likely with an aggregate)
        if select.projection.len() == 1 {
            // Check if it's an aggregate function
            if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0] {
                let func_name = self.extract_table_name(&func.name)?;

                // Handle common aggregate functions
                match func_name.to_uppercase().as_str() {
                    "AVG" => {
                        return Ok((
                            SubqueryType::Scalar,
                            Column::new("subquery_result", TypeId::Decimal),
                        ))
                    }
                    "SUM" => {
                        // Determine type based on argument
                        if let FunctionArguments::List(args) = &func.args {
                            if !args.args.is_empty() {
                                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) =
                                    &args.args[0]
                                {
                                    let arg_expr = self.parse_expression(expr, schema)?;
                                    let arg_type = arg_expr.get_return_type().get_type();

                                    let return_type = match arg_type {
                                        TypeId::Integer | TypeId::SmallInt | TypeId::TinyInt => {
                                            TypeId::Integer
                                        }
                                        TypeId::BigInt => TypeId::BigInt,
                                        TypeId::Decimal => TypeId::Decimal,
                                        _ => TypeId::Decimal, // Default to decimal for other types
                                    };

                                    return Ok((
                                        SubqueryType::Scalar,
                                        Column::new("subquery_result", return_type),
                                    ));
                                }
                            }
                        }

                        // Default to decimal if we can't determine the type
                        return Ok((
                            SubqueryType::Scalar,
                            Column::new("subquery_result", TypeId::Decimal),
                        ));
                    }
                    "COUNT" => {
                        return Ok((
                            SubqueryType::Scalar,
                            Column::new("subquery_result", TypeId::Integer),
                        ))
                    }
                    "MIN" | "MAX" => {
                        // Determine type based on argument
                        if let FunctionArguments::List(args) = &func.args {
                            if !args.args.is_empty() {
                                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) =
                                    &args.args[0]
                                {
                                    let arg_expr = self.parse_expression(expr, schema)?;
                                    let arg_type = arg_expr.get_return_type().get_type();
                                    return Ok((
                                        SubqueryType::Scalar,
                                        Column::new("subquery_result", arg_type),
                                    ));
                                }
                            }
                        }

                        // Default to integer if we can't determine the type
                        return Ok((
                            SubqueryType::Scalar,
                            Column::new("subquery_result", TypeId::Integer),
                        ));
                    }
                    _ => {
                        // For other functions, try to infer the return type
                        let expr = self.parse_expression(&Expr::Function(func.clone()), schema)?;
                        let return_type = expr.get_return_type().clone();
                        return Ok((SubqueryType::Scalar, return_type));
                    }
                }
            } else {
                // Single column but not an aggregate function
                let expr = self.parse_expression(
                    match &select.projection[0] {
                        SelectItem::UnnamedExpr(expr) => expr,
                        SelectItem::ExprWithAlias { expr, .. } => expr,
                        _ => {
                            return Ok((
                                SubqueryType::Scalar,
                                Column::new("subquery_result", TypeId::Vector),
                            ))
                        }
                    },
                    schema,
                )?;
                let return_type = expr.get_return_type().clone();
                return Ok((SubqueryType::Scalar, return_type));
            }
        }

        // Default to vector type for non-scalar subqueries
        Ok((
            SubqueryType::Scalar,
            Column::new("subquery_result", TypeId::Vector),
        ))
    }

    fn parse_order_by_expressions(
        &self,
        order_exprs: &[OrderByExpr],
        schema: &Schema,
    ) -> Result<Vec<Arc<Expression>>, String> {
        debug!("Parsing order by expressions: {:?}", order_exprs);
        let mut expressions = Vec::new();
        for order_expr in order_exprs {
            let parsed_expr = self.parse_expression(&order_expr.expr, schema)?;
            expressions.push(Arc::new(parsed_expr));
        }
        Ok(expressions)
    }

    fn parse_function_arguments(
        &self,
        args: &FunctionArguments,
        schema: &Schema,
    ) -> Result<Vec<Arc<Expression>>, String> {
        debug!("Parsing function arguments: {:?}", args);
        let mut children = Vec::new();

        match args {
            FunctionArguments::None => {
                // No arguments, return empty vector
                Ok(children)
            }
            FunctionArguments::Subquery(_) => {
                Err("Subquery function arguments not yet supported".to_string())
            }
            FunctionArguments::List(list) => {
                for arg in &list.args {
                    match arg {
                        FunctionArg::Named {
                            name: _,
                            arg,
                            operator: _,
                        } => {
                            return Err("Named function arguments not yet supported".to_string());
                        }
                        FunctionArg::Unnamed(arg) => {
                            match arg {
                                FunctionArgExpr::Expr(expr) => {
                                    let parsed_expr = self.parse_expression(expr, schema)?;
                                    children.push(Arc::new(parsed_expr));
                                }
                                FunctionArgExpr::QualifiedWildcard(_) => {
                                    return Err("Qualified wildcard in function arguments not yet supported".to_string());
                                }
                                FunctionArgExpr::Wildcard => {
                                    // Allow wildcard for COUNT(*), it will be handled specially in parse_function
                                    continue;
                                }
                            }
                        }
                        FunctionArg::ExprNamed {
                            name: _,
                            arg,
                            operator: _,
                        } => {
                            return Err("Named function arguments not yet supported".to_string());
                        }
                    }
                }

                // Handle additional clauses
                for clause in &list.clauses {
                    match clause {
                        FunctionArgumentClause::OrderBy(order_exprs) => {
                            children.extend(self.parse_order_by_expressions(order_exprs, schema)?);
                        }
                        FunctionArgumentClause::Limit(expr) => {
                            let parsed_expr = self.parse_expression(expr, schema)?;
                            children.push(Arc::new(parsed_expr));
                        }
                        _ => {}
                    }
                }

                Ok(children)
            }
        }
    }

    fn parse_sql_window_specification(
        &self,
        spec: &WindowType,
        schema: &Schema,
    ) -> Result<Vec<Arc<Expression>>, String> {
        debug!("Parsing SQL window specification: {:?}", spec);
        let mut children = Vec::new();

        // Extract the WindowSpec from WindowType
        let window_spec = match spec {
            WindowType::WindowSpec(spec) => spec,
            WindowType::NamedWindow(_) => {
                return Err("Named windows are not supported yet".to_string())
            }
        };

        // Handle PARTITION BY
        for partition_expr in &window_spec.partition_by {
            let parsed_expr = self.parse_expression(partition_expr, schema)?;
            children.push(Arc::new(parsed_expr));
        }

        // Handle ORDER BY
        if !window_spec.order_by.is_empty() {
            children.extend(self.parse_order_by_expressions(&window_spec.order_by, schema)?);
        }

        // Handle window frame
        if let Some(frame) = &window_spec.window_frame {
            match &frame.start_bound {
                WindowFrameBound::CurrentRow => {}
                WindowFrameBound::Preceding(expr) | WindowFrameBound::Following(expr) => {
                    if let Some(expr) = expr {
                        let parsed_expr = self.parse_expression(expr, schema)?;
                        children.push(Arc::new(parsed_expr));
                    }
                }
            }

            if let Some(end_bound) = &frame.end_bound {
                match end_bound {
                    WindowFrameBound::CurrentRow => {}
                    WindowFrameBound::Preceding(expr) | WindowFrameBound::Following(expr) => {
                        if let Some(expr) = expr {
                            let parsed_expr = self.parse_expression(expr, schema)?;
                            children.push(Arc::new(parsed_expr));
                        }
                    }
                }
            }
        }

        Ok(children)
    }

    fn parse_function(&self, func: &Function, schema: &Schema) -> Result<Expression, String> {
        debug!("Parsing function: {:?}", func);
        let function_name = func.name.to_string().to_uppercase();
        let function_type = function_types::get_function_type(&function_name);

        match function_type {
            FunctionType::Scalar(scalar_type) => {
                self.parse_scalar_function(func, scalar_type, schema)
            }
            FunctionType::Aggregate(agg_type) => {
                self.parse_aggregate_function(func, agg_type, schema)
            }
            FunctionType::Window(window_type) => {
                Err("Window functions are not yet supported".to_string())
            }
        }
    }

    fn parse_scalar_function(
        &self,
        func: &Function,
        scalar_type: ScalarFunctionType,
        schema: &Schema,
    ) -> Result<Expression, String> {
        debug!(
            "Parsing scalar function: {:?}, type: {:?}",
            func, scalar_type
        );
        let args = self.parse_function_arguments(&func.args, schema)?;
        let function_name = func.name.to_string();
        let return_type = self.infer_scalar_return_type(&function_name, &scalar_type, &args)?;

        Ok(Expression::Function(FunctionExpression::new(
            function_name,
            args,
            return_type,
        )))
    }

    fn parse_aggregate_function(
        &self,
        func: &Function,
        agg_type: AggregateFunctionType,
        schema: &Schema,
    ) -> Result<Expression, String> {
        debug!(
            "Parsing aggregate function: {:?}, type: {:?}",
            func, agg_type
        );
        let args = self.parse_function_arguments(&func.args, schema)?;
        let function_name = func.name.to_string().to_uppercase();

        let aggregate_type = match agg_type {
            AggregateFunctionType::Count => {
                if self.is_count_star(&func.args) {
                    AggregationType::CountStar
                } else if args.is_empty() {
                    return Err("COUNT() requires either * or a column argument".to_string());
                } else {
                    AggregationType::Count
                }
            }
            AggregateFunctionType::Sum => AggregationType::Sum,
            AggregateFunctionType::Avg => AggregationType::Avg,
            AggregateFunctionType::Min => AggregationType::Min,
            AggregateFunctionType::Max => AggregationType::Max,
            AggregateFunctionType::Statistical => match function_name.as_str() {
                "STDDEV" => AggregationType::StdDev,
                "VARIANCE" => AggregationType::Variance,
                _ => return Err(format!("Unknown statistical function: {}", function_name)),
            },
        };

        let return_type = self.infer_aggregate_return_type(&function_name, &args)?;

        Ok(Expression::Aggregate(AggregateExpression::new(
            aggregate_type,
            args,
            return_type,
            function_name,
        )))
    }

    fn infer_scalar_return_type(
        &self,
        func_name: &str,
        scalar_type: &ScalarFunctionType,
        args: &[Arc<Expression>],
    ) -> Result<Column, String> {
        debug!(
            "Inferring scalar return type for function: {}, type: {:?}",
            func_name, scalar_type
        );
        match scalar_type {
            ScalarFunctionType::String => Ok(Column::new(func_name, TypeId::VarChar)),
            ScalarFunctionType::Numeric => {
                if args.is_empty() {
                    return Err(format!("{} requires at least one argument", func_name));
                }
                // Use the same type as the input for numeric functions
                Ok(Column::new(func_name, args[0].get_return_type().get_type()))
            }
            ScalarFunctionType::DateTime => Ok(Column::new(func_name, TypeId::Timestamp)),
            ScalarFunctionType::Cast => {
                // Handle CAST separately as it requires the target type
                Err("CAST not implemented yet".to_string())
            }
            ScalarFunctionType::Other => {
                // Default to using FunctionExpression's existing type inference
                FunctionExpression::infer_return_type(func_name, args)
            }
        }
    }

    fn infer_aggregate_return_type(
        &self,
        func_name: &str,
        args: &[Arc<Expression>],
    ) -> Result<Column, String> {
        debug!(
            "Inferring aggregate return type for function: {}",
            func_name
        );
        match func_name.to_uppercase().as_str() {
            "COUNT" => Ok(Column::new(func_name, TypeId::BigInt)),
            "SUM" => {
                if args.is_empty() {
                    return Err(format!("{} requires an argument", func_name));
                }
                let arg_type = args[0].get_return_type().get_type();
                match arg_type {
                    TypeId::Integer | TypeId::BigInt | TypeId::SmallInt | TypeId::TinyInt => {
                        Ok(Column::new(func_name, TypeId::BigInt))
                    }
                    TypeId::Decimal => Ok(Column::new(func_name, TypeId::Decimal)),
                    _ => Err(format!("Invalid argument type for {}", func_name)),
                }
            }
            "AVG" => {
                if args.is_empty() {
                    return Err(format!("{} requires an argument", func_name));
                }
                let arg_type = args[0].get_return_type().get_type();
                match arg_type {
                    TypeId::Integer
                    | TypeId::BigInt
                    | TypeId::SmallInt
                    | TypeId::TinyInt
                    | TypeId::Decimal => Ok(Column::new(func_name, TypeId::Decimal)),
                    _ => Err(format!("Invalid argument type for {}", func_name)),
                }
            }
            "MIN" | "MAX" => {
                if args.is_empty() {
                    return Err(format!("{} requires an argument", func_name));
                }
                // Return type is same as input type
                Ok(Column::new(func_name, args[0].get_return_type().get_type()))
            }
            "STDDEV" | "VARIANCE" => Ok(Column::new(func_name, TypeId::Decimal)),
            _ => Err(format!("Unknown aggregate function: {}", func_name)),
        }
    }

    // Helper method to check if a function is COUNT(*)
    fn is_count_star(&self, args: &FunctionArguments) -> bool {
        match args {
            FunctionArguments::List(arg_list) => {
                if arg_list.args.len() != 1 {
                    return false;
                }
                if let FunctionArg::Unnamed(expr) = &arg_list.args[0] {
                    matches!(expr, FunctionArgExpr::Wildcard)
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    fn parse_window_expr(&self, expr: &Expr, schema: &Schema) -> Option<Arc<Expression>> {
        debug!("Parsing window expression: {:?}", expr);
        match self.parse_expression(expr, schema) {
            Ok(parsed_expr) => Some(Arc::new(parsed_expr)),
            Err(_) => None,
        }
    }

    fn create_count_star(&self) -> Result<Expression, String> {
        debug!("Creating COUNT(*) expression");
        // Create a constant expression for COUNT(*)
        let constant = Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("*", TypeId::Integer),
            vec![],
        ));

        Ok(Expression::Aggregate(AggregateExpression::new(
            AggregationType::CountStar,
            vec![Arc::new(constant)],
            Column::new("COUNT(*)", TypeId::BigInt),
            "COUNT".to_string(),
        )))
    }

    // Add a method to extract table name from TableObject
    pub fn extract_table_object_name(&self, table: &TableObject) -> Result<String, String> {
        debug!("Extracting table name from TableObject: {:?}", table);
        // Match on the TableObject enum to extract the ObjectName
        match table {
            TableObject::TableName(obj_name) => self.extract_table_name(obj_name),
            TableObject::TableFunction(_) => Err("Table functions are not supported".to_string()),
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
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use sqlparser::ast::Ident;
    use std::collections::HashMap;
    use tempfile::TempDir;

    struct TestContext {
        catalog: Arc<RwLock<Catalog>>,
        _temp_dir: TempDir,
        expression_parser: ExpressionParser,
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

            let expression_parser = ExpressionParser::new(catalog.clone());

            Self {
                catalog,
                _temp_dir: temp_dir,
                expression_parser,
            }
        }

        pub fn catalog(&self) -> Arc<RwLock<Catalog>> {
            self.catalog.clone()
        }

        pub fn expression_parser(&self) -> &ExpressionParser {
            &self.expression_parser
        }

        fn setup_test_schema(&self) -> Schema {
            Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("age", TypeId::Integer),
                Column::new("salary", TypeId::Decimal),
                Column::new("timestamp_column", TypeId::Timestamp),
            ])
        }

        fn parse_expression(&self, expr_str: &str, schema: &Schema) -> Result<Expression, String> {
            use sqlparser::dialect::GenericDialect;
            use sqlparser::parser::Parser;

            let dialect = GenericDialect {};
            let mut parser = Parser::new(&dialect)
                .try_with_sql(expr_str)
                .map_err(|e| e.to_string())?;
            let expr = parser.parse_expr().map_err(|e| e.to_string())?;
            self.expression_parser().parse_expression(&expr, schema)
        }
    }

    // Helper function to extract the first column reference from an expression
    fn extract_first_column_ref(expr: &Expression) -> Option<Column> {
        match expr {
            Expression::ColumnRef(col_ref) => Some(col_ref.get_return_type().clone()),
            Expression::Comparison(comp) => {
                let children = comp.get_children();
                if let Expression::ColumnRef(col_ref) = children[0].as_ref() {
                    Some(col_ref.get_return_type().clone())
                } else {
                    None
                }
            }
            Expression::Arithmetic(arith) => {
                let children = arith.get_children();
                for child in children {
                    if let Some(col) = extract_first_column_ref(child) {
                        return Some(col);
                    }
                }
                None
            }
            Expression::Logic(logic) => {
                let children = logic.get_children();
                for child in children {
                    if let Some(col) = extract_first_column_ref(child) {
                        return Some(col);
                    }
                }
                None
            }
            _ => None,
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
                .parse_expression(expr_str, &schema)
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
                .parse_expression(expr_str, &schema)
                .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", expr_str, e));

            match expr {
                Expression::Arithmetic(arith) => {
                    assert_eq!(
                        arith.get_op(),
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
                .parse_expression(expr_str, &schema)
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
                .parse_expression(expr_str, &schema)
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
                .parse_expression(expr_str, &schema)
                .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", expr_str, e));

            match expr {
                Expression::Comparison(comp) => {
                    let children = comp.get_children();
                    match children[1].as_ref() {
                        Expression::Constant(constant) => {
                            assert_eq!(
                                constant.get_return_type().get_type(),
                                expected_type,
                                "Constant in '{}' should have type {:?}",
                                expr_str,
                                expected_type
                            );
                        }
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
    fn test_parse_case_expressions() {
        let ctx = TestContext::new("test_parse_case_expressions");
        let schema = ctx.setup_test_schema();

        let test_cases = vec![
            "CASE WHEN age > 25 THEN 'Adult' ELSE 'Young' END",
            "CASE age WHEN 20 THEN 'Twenty' WHEN 30 THEN 'Thirty' ELSE 'Other' END",
        ];

        for expr_str in test_cases {
            let expr = ctx
                .parse_expression(expr_str, &schema)
                .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", expr_str, e));

            match expr {
                Expression::Case(case) => {
                    assert_eq!(
                        case.get_return_type().get_type(),
                        TypeId::VarChar,
                        "CASE expression '{}' should return {:?}",
                        expr_str,
                        TypeId::VarChar
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
            ("CAST(age AS VARCHAR)", TypeId::VarChar),
            ("CAST(salary AS INTEGER)", TypeId::Integer),
            ("CAST('123' AS INTEGER)", TypeId::Integer),
        ];

        for (expr_str, expected_type) in test_cases {
            let expr = ctx
                .parse_expression(expr_str, &schema)
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
            ("UPPER(name)", true),
            ("LOWER(name)", true),
            ("SUBSTRING(name FROM 1 FOR 3)", false),
        ];

        for (expr_str, is_function) in test_cases {
            let expr = ctx
                .parse_expression(expr_str, &schema)
                .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", expr_str, e));

            match (expr, is_function) {
                (Expression::Function(func), true) => {
                    assert_eq!(
                        func.get_return_type().get_type(),
                        TypeId::VarChar,
                        "String function '{}' should return {:?}",
                        expr_str,
                        TypeId::VarChar
                    );
                }
                (Expression::Substring(substr), false) => {
                    assert_eq!(
                        substr.get_return_type().get_type(),
                        TypeId::VarChar,
                        "SUBSTRING expression '{}' should return {:?}",
                        expr_str,
                        TypeId::VarChar
                    );
                }
                _ => panic!(
                    "Expected {} expression for '{}'",
                    if is_function { "Function" } else { "Substring" },
                    expr_str
                ),
            }
        }
    }

    #[test]
    fn test_parse_complex_expressions() {
        let ctx = TestContext::new("test_parse_complex_expressions");
        let schema = ctx.setup_test_schema();

        let test_cases = vec![
            (
                "CASE WHEN age > 25 AND salary > 50000 THEN 'High' ELSE 'Low' END",
                "case",
            ),
            ("(age + 5) * 2", "multiply"),
            ("UPPER(name) = 'JOHN'", "compare"),
        ];

        for (expr_str, expr_type) in test_cases {
            let expr = ctx
                .parse_expression(expr_str, &schema)
                .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", expr_str, e));

            match (expr, expr_type) {
                (Expression::Case(case), "case") => {
                    assert_eq!(
                        case.get_return_type().get_type(),
                        TypeId::VarChar,
                        "CASE expression '{}' should return {:?}",
                        expr_str,
                        TypeId::VarChar
                    );
                }
                (Expression::Arithmetic(arith), "multiply") => {
                    assert_eq!(
                        arith.get_op(),
                        ArithmeticOp::Multiply,
                        "Arithmetic expression '{}' should be {:?}",
                        expr_str,
                        ArithmeticOp::Multiply
                    );
                    // Verify the left child is an Add operation
                    let children = arith.get_children();
                    assert_eq!(children.len(), 2, "Multiply should have 2 children");
                    match children[0].as_ref() {
                        Expression::Arithmetic(add_expr) => {
                            assert_eq!(
                                add_expr.get_op(),
                                ArithmeticOp::Add,
                                "Left child of multiply should be Add"
                            );
                        }
                        _ => panic!("Expected Add expression as left child of multiply"),
                    }
                }
                (Expression::Comparison(comp), "compare") => {
                    assert_eq!(
                        comp.get_comp_type(),
                        ComparisonType::Equal,
                        "Comparison expression '{}' should be {:?}",
                        expr_str,
                        ComparisonType::Equal
                    );
                }
                _ => panic!("Unexpected expression type for '{}'", expr_str),
            }
        }
    }

    #[test]
    fn test_parse_aggregate_functions() {
        use crate::sql::execution::expressions::aggregate_expression::AggregationType;

        let ctx = TestContext::new("test_parse_aggregate_functions");
        let schema = ctx.setup_test_schema();

        let test_cases = vec![
            ("COUNT(*)", AggregationType::CountStar),
            ("SUM(salary)", AggregationType::Sum),
            ("AVG(age)", AggregationType::Avg),
            ("MIN(salary)", AggregationType::Min),
            ("MAX(age)", AggregationType::Max),
        ];

        for (expr_str, expected_type) in test_cases {
            let expr = ctx
                .parse_expression(expr_str, &schema)
                .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", expr_str, e));

            match expr {
                Expression::Aggregate(agg) => {
                    assert_eq!(
                        agg.get_agg_type(),
                        &expected_type,
                        "Aggregate expression '{}' should be {:?}",
                        expr_str,
                        expected_type
                    );
                }
                _ => panic!("Expected Aggregate expression for '{}'", expr_str),
            }
        }

        // Test invalid aggregates
        let invalid_cases = vec![
            "COUNT()",             // No arguments
            "SUM(*)",              // Can't sum *
            "AVG(name)",           // Can't average strings
            "MIN()",               // No arguments
            "MAX(invalid_column)", // Invalid column
        ];

        for expr_str in invalid_cases {
            assert!(
                ctx.parse_expression(expr_str, &schema).is_err(),
                "Expected error for invalid aggregate: {}",
                expr_str
            );
        }
    }

    #[test]
    fn test_parse_between_expressions() {
        let ctx = TestContext::new("test_parse_between_expressions");
        let schema = ctx.setup_test_schema();

        let test_cases = vec![
            "age BETWEEN 20 AND 30",
            "salary BETWEEN 30000 AND 50000",
            "age NOT BETWEEN 10 AND 18",
        ];

        for expr_str in test_cases {
            let expr = ctx
                .parse_expression(expr_str, &schema)
                .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", expr_str, e));

            match expr {
                Expression::Logic(logic) => {
                    assert_eq!(
                        logic.get_logic_type(),
                        LogicType::And,
                        "BETWEEN expression '{}' should use AND",
                        expr_str
                    );

                    let children = logic.get_children();
                    assert_eq!(
                        children.len(),
                        2,
                        "BETWEEN expression '{}' should have 2 comparison parts",
                        expr_str
                    );
                }
                _ => panic!("Expected Logic expression for BETWEEN '{}'", expr_str),
            }
        }
    }

    #[test]
    fn test_parse_nested_expressions() {
        let ctx = TestContext::new("test_parse_nested_expressions");
        let schema = ctx.setup_test_schema();

        let test_cases = vec![
            (
                "CASE WHEN (age > 25 AND salary > 50000) THEN 'High' ELSE 'Low' END",
                TypeId::VarChar,
            ),
            ("UPPER(LOWER(name))", TypeId::VarChar),
            ("(age + salary) * 2", TypeId::Decimal),
        ];

        for (expr_str, expected_type) in test_cases {
            let expr = ctx
                .parse_expression(expr_str, &schema)
                .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", expr_str, e));

            assert_eq!(
                expr.get_return_type().get_type(),
                expected_type,
                "Nested expression '{}' should return {:?}",
                expr_str,
                expected_type
            );
        }
    }

    #[test]
    fn test_parse_null_expressions() -> Result<(), String> {
        let ctx = TestContext::new("test_parse_null_expressions");
        let schema = ctx.setup_test_schema();

        let simple_null_checks = vec!["name IS NULL", "age IS NOT NULL"];

        for expr_str in simple_null_checks {
            let expr = ctx.parse_expression(expr_str, &schema)?;
            match expr {
                Expression::IsCheck(_) => (),
                _ => {
                    return Err(format!(
                        "Expected IsCheck expression for {}, got {:?}",
                        expr_str, expr
                    ))
                }
            }
        }

        // Test CASE expression with IS NULL check
        let case_expr_str = "CASE WHEN name IS NULL THEN 'Unknown' ELSE name END";
        let expr = ctx.parse_expression(case_expr_str, &schema)?;

        // Verify it's a CASE expression
        match &expr {
            Expression::Case(case_expr) => {
                // Verify return type is VARCHAR
                assert_eq!(case_expr.get_return_type().get_type(), TypeId::VarChar);

                // Get the first WHEN expression
                let when_expr = case_expr.get_children()[0].clone();
                // Verify it's an IS NULL check
                match *when_expr {
                    Expression::IsCheck(_) => (),
                    _ => {
                        return Err(format!(
                            "Expected IsCheck expression for WHEN condition, got {:?}",
                            when_expr
                        ))
                    }
                }
            }
            _ => return Err(format!("Expected Case expression, got {:?}", expr)),
        }

        Ok(())
    }

    #[test]
    fn test_parse_error_cases() {
        let ctx = TestContext::new("test_parse_error_cases");
        let schema = ctx.setup_test_schema();

        let test_cases = vec!["invalid_column > 5", "age + 'string'", "CASE WHEN THEN END"];

        for expr_str in test_cases {
            assert!(ctx.parse_expression(expr_str, &schema).is_err());
        }
    }

    #[test]
    fn test_parse_at_timezone() {
        let ctx = TestContext::new("test_parse_at_timezone");
        let schema = ctx.setup_test_schema();

        let expr_str = "timestamp_column AT TIME ZONE 'UTC'";
        let expr = ctx
            .parse_expression(expr_str, &schema)
            .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", expr_str, e));

        assert_eq!(
            expr.get_return_type().get_type(),
            TypeId::Timestamp,
            "AT TIME ZONE expression should return timestamp type"
        );
    }

    #[test]
    fn test_parse_at_timezone_invalid_types() {
        let ctx = TestContext::new("test_parse_at_timezone_invalid_types");
        let schema = ctx.setup_test_schema();

        let test_cases = vec!["age AT TIME ZONE 'UTC'", "name AT TIME ZONE '123'"];

        for expr_str in test_cases {
            assert!(ctx.parse_expression(expr_str, &schema).is_err());
        }
    }

    #[test]
    fn test_parse_join_operators() {
        let ctx = TestContext::new("test_parse_join_operators");
        let schema = ctx.setup_test_schema();

        // Create a second test schema
        let mut columns = Vec::new();
        columns.push(Column::new("id", TypeId::Integer));
        columns.push(Column::new("name", TypeId::VarChar));
        columns.push(Column::new("department", TypeId::VarChar));
        let second_schema = Schema::new(columns);

        // Test INNER JOIN with ON constraint
        let on_expr = sqlparser::ast::Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("id"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Identifier(Ident::new("id"))),
        };

        let inner_join = JoinOperator::Inner(JoinConstraint::On(on_expr.clone()));
        let result = ctx.expression_parser().process_join_operator(
            &inner_join,
            &schema,
            &second_schema,
            None,
            None,
        );
        assert!(
            result.is_ok(),
            "Failed to process INNER JOIN: {:?}",
            result.err()
        );

        // Test LEFT OUTER JOIN with ON constraint
        let left_join = JoinOperator::LeftOuter(JoinConstraint::On(on_expr.clone()));
        let result = ctx.expression_parser().process_join_operator(
            &left_join,
            &schema,
            &second_schema,
            None,
            None,
        );
        assert!(
            result.is_ok(),
            "Failed to process LEFT OUTER JOIN: {:?}",
            result.err()
        );

        // Test USING constraint
        let using_cols = vec![ObjectName(vec![ObjectNamePart::Identifier(Ident::new("id"))])];
        let using_join = JoinOperator::Inner(JoinConstraint::Using(using_cols));
        let result = ctx.expression_parser().process_join_operator(
            &using_join,
            &schema,
            &second_schema,
            None,
            None,
        );
        assert!(
            result.is_ok(),
            "Failed to process USING constraint: {:?}",
            result.err()
        );

        // Test NATURAL JOIN
        let natural_join = JoinOperator::Inner(JoinConstraint::Natural);
        let result = ctx.expression_parser().process_join_operator(
            &natural_join,
            &schema,
            &second_schema,
            None,
            None,
        );
        assert!(
            result.is_ok(),
            "Failed to process NATURAL JOIN: {:?}",
            result.err()
        );

        // Test CROSS JOIN
        let cross_join = JoinOperator::CrossJoin;
        let result = ctx.expression_parser().process_join_operator(
            &cross_join,
            &schema,
            &second_schema,
            None,
            None,
        );
        assert!(
            result.is_ok(),
            "Failed to process CROSS JOIN: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_qualified_column_references() {
        let ctx = TestContext::new("test_parse_qualified_column_references");

        // Create a schema with regular columns
        let base_schema = ctx.setup_test_schema();

        // Create schemas with table aliases
        let table1_schema =
            Schema::merge_with_aliases(&base_schema, &Schema::new(vec![]), Some("t1"), None);
        let table2_schema =
            Schema::merge_with_aliases(&base_schema, &Schema::new(vec![]), Some("t2"), None);

        // Merge the schemas to simulate a join
        let joined_schema = Schema::merge(&table1_schema, &table2_schema);

        // Test cases for qualified column references
        let test_cases = vec![
            // Basic table alias references
            ("t1.id = 1", "t1.id"),
            ("t2.name = 'John'", "t2.name"),
            ("t1.age > 25", "t1.age"),
            ("t2.salary < 50000", "t2.salary"),
            // Comparison between columns from different tables
            ("t1.id = t2.id", "t1.id"),
            ("t1.age > t2.age", "t1.age"),
            // Arithmetic with qualified columns
            ("t1.salary + t2.salary", "t1.salary"),
            ("t1.age * 2", "t1.age"),
            // Complex expressions with qualified columns
            ("t1.age > 25 AND t2.salary < 50000", "t1.age"),
            ("t1.id = 1 OR t2.name = 'John'", "t1.id"),
        ];

        for (expr_str, expected_column) in test_cases {
            let expr = ctx
                .parse_expression(expr_str, &joined_schema)
                .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", expr_str, e));

            // Extract the first column reference from the expression
            let column_ref = extract_first_column_ref(&expr);

            // Verify the column name matches the expected qualified name
            assert!(
                column_ref.is_some(),
                "Expected ColumnRef in expression: {}",
                expr_str
            );

            if let Some(col_ref) = column_ref {
                assert_eq!(
                    col_ref.get_name(),
                    expected_column,
                    "Column name in '{}' should be '{}'",
                    expr_str,
                    expected_column
                );
            }
        }

        // Test error cases
        let error_cases = vec![
            "unknown_table.unknown_column = 1", // Unknown table alias and column
            "t1.unknown_column = 1",            // Unknown column
            "t3.unknown_column = 1",            // Non-existent table
            "schema.table.column = 1",          // Three-part identifier not supported
        ];

        for expr_str in error_cases {
            assert!(
                ctx.parse_expression(expr_str, &joined_schema).is_err(),
                "Expected error for invalid expression: {}",
                expr_str
            );
        }
    }
}
