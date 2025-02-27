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
use crate::sql::execution::expressions::arithmetic_expression::ArithmeticExpression;
use crate::sql::execution::expressions::arithmetic_expression::ArithmeticOp;
use crate::sql::execution::expressions::at_timezone_expression::AtTimeZoneExpression;
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
use crate::sql::execution::expressions::in_expression::InExpression;
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
use crate::sql::execution::expressions::regex_expression::{RegexExpression, RegexOperator};
use crate::sql::execution::expressions::subquery_expression::SubqueryExpression;
use crate::sql::execution::expressions::subquery_expression::SubqueryType;
use crate::sql::execution::expressions::substring_expression::SubstringExpression;
use crate::sql::execution::expressions::trim_expression::{TrimExpression, TrimType};
use crate::sql::execution::expressions::typed_string_expression::TypedStringExpression;
use crate::sql::execution::expressions::unary_op_expression::UnaryOpExpression;
use crate::sql::planner::logical_plan::LogicalPlan;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use parking_lot::RwLock;
use sqlparser::ast::{
    BinaryOperator, CastFormat, CeilFloorKind, DataType, Expr, Function,
    FunctionArg, FunctionArgExpr, FunctionArgOperator, FunctionArgumentClause
    , FunctionArguments, GroupByExpr, JoinConstraint, JoinOperator
    , ObjectName, OrderByExpr, Query, Select, SelectItem, SetExpr,
    TableFactor, Value as SQLValue, WindowFrameBound, WindowType,
};
use std::sync::Arc;
use crate::sql::binder::expressions::bound_window::BoundWindow;
use crate::sql::binder::bound_expression::{BoundExpression, ExpressionType};
use crate::sql::binder::expressions::bound_constant::BoundConstant;
use crate::sql::binder::expressions::bound_column_ref::BoundColumnRef;
use crate::sql::execution::expressions::grouping_sets_expression::{GroupingSetsExpression, GroupingType};
use crate::sql::execution::expressions::tuple_expression::TupleExpression;
use crate::sql::execution::expressions::struct_expression::{StructExpression, StructField};

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

                let table_alias = &parts[0].value;
                let column_name = &parts[1].value;

                // Try different formats for column lookup
                let column_idx = schema
                    .get_column_index(&format!("{}.{}", table_alias, column_name))
                    .or_else(|| {
                        // Try looking up the original table name if alias doesn't work
                        let catalog = self.catalog.read();

                        // Try to find the table by alias
                        if let Some(table) = catalog.get_table(table_alias) {
                            // Check if column exists in this table
                            let table_schema = table.get_table_schema();
                            if let Some(_) = table_schema.get_column_index(column_name) {
                                // If found in original table, look for it in the schema
                                // Try both qualified and unqualified names
                                schema
                                    .get_column_index(&format!("{}.{}", table_alias, column_name))
                                    .or_else(|| schema.get_column_index(column_name))
                            } else {
                                None
                            }
                        } else {
                            // If not found by alias, try just the column name
                            // This is a fallback for cases where the schema might not use qualified names
                            schema.get_column_index(column_name)
                        }
                    })
                    .ok_or_else(|| {
                        format!("Column {}.{} not found in schema", table_alias, column_name)
                    })?;

                let column = schema
                    .get_column(column_idx)
                    .ok_or_else(|| format!("Failed to get column at index {}", column_idx))?;

                // Create a new column with the aliased name to preserve the table alias
                let aliased_column = Column::new(
                    &format!("{}.{}", table_alias, column_name),
                    column.get_type(),
                );

                Ok(Expression::ColumnRef(ColumnRefExpression::new(
                    0, // table index will be handled by the planner/executor
                    column_idx,
                    aliased_column,
                    vec![],
                )))
            }

            Expr::Value(value) => {
                Ok(Expression::Literal(LiteralValueExpression::new(
                    value.clone(),
                )?))
            },

            Expr::BinaryOp { left, op, right } => {
                let left_expr = Arc::new(self.parse_expression(left, schema)?);
                let right_expr = Arc::new(self.parse_expression(right, schema)?);

                // Convert arithmetic binary operators to ArithmeticExpression
                match op {
                    BinaryOperator::Plus => Ok(Expression::Arithmetic(ArithmeticExpression::new(
                        left_expr.clone(),
                        right_expr.clone(),
                        ArithmeticOp::Add,
                        vec![left_expr, right_expr],
                    ))),
                    BinaryOperator::Minus => Ok(Expression::Arithmetic(ArithmeticExpression::new(
                        left_expr.clone(),
                        right_expr.clone(),
                        ArithmeticOp::Subtract,
                        vec![left_expr, right_expr],
                    ))),
                    BinaryOperator::Multiply => {
                        Ok(Expression::Arithmetic(ArithmeticExpression::new(
                            left_expr.clone(),
                            right_expr.clone(),
                            ArithmeticOp::Multiply,
                            vec![left_expr, right_expr],
                        )))
                    }
                    BinaryOperator::Divide => {
                        Ok(Expression::Arithmetic(ArithmeticExpression::new(
                            left_expr.clone(),
                            right_expr.clone(),
                            ArithmeticOp::Divide,
                            vec![left_expr, right_expr],
                        )))
                    }
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
                    IsCheckType::Unknown { negated: false },
                    Column::new("is_null", TypeId::Boolean),
                )))
            }

            Expr::IsNotNull(expr) => {
                let inner_expr = Arc::new(self.parse_expression(expr, schema)?);
                Ok(Expression::IsCheck(IsCheckExpression::new(
                    inner_expr,
                    IsCheckType::Unknown { negated: true },
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
                    { LogicType::And },
                    vec![Arc::new(low_compare), Arc::new(high_compare)],
                ));

                Ok(result)
            }

            Expr::Function(func) => self.parse_function(func, schema),

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

            Expr::Cast {
                expr,
                data_type,
                format,
                ..
            } => {
                let inner_expr = Arc::new(self.parse_expression(expr, schema)?);
                let target_type = match data_type {
                    sqlparser::ast::DataType::Int(_) | sqlparser::ast::DataType::Integer(_) => {
                        TypeId::Integer
                    }
                    DataType::BigInt(_) => TypeId::BigInt,
                    DataType::Float(_) | DataType::Double | DataType::Decimal(_) => TypeId::Decimal,
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
                        DataType::Float(_) | DataType::Double | DataType::Decimal(_) => {
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
                        let scale_expr = Expr::Value(scale_expr.clone());
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
                        let scale_expr = Expr::Value(scale_expr.clone());
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
                let subquery = Arc::new(self.parse_subquery(subquery, schema)?);
                Ok(Expression::In(InExpression::new_subquery(
                    expr,
                    subquery,
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
            } => {
                Ok(Expression::Regex(RegexExpression::new(
                    Arc::new(self.parse_expression(expr, schema)?),
                    Arc::new(self.parse_expression(pattern, schema)?),
                    RegexOperator::RLike,
                    escape_char.clone(),
                    Column::new("like", TypeId::Boolean),
                )))
            },
            Expr::ILike {
                negated,
                any,
                expr,
                pattern,
                escape_char,
            } => {
                Ok(Expression::Regex(RegexExpression::new(
                    Arc::new(self.parse_expression(expr, schema)?),
                    Arc::new(self.parse_expression(pattern, schema)?),
                    RegexOperator::RLike,
                    escape_char.clone(),
                    Column::new("ilike", TypeId::Boolean),
                )))
            },
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
                                    Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) => {
                                        s.clone()
                                    }
                                    _ => String::new(), // Skip non-string literals
                                }
                            })
                            .collect::<String>();

                        children.push(Arc::new(self.parse_expression(
                            &Expr::Value(sqlparser::ast::Value::SingleQuotedString(concat_chars)),
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
            Expr::MapAccess { column, keys } => {
                let parsed_column = self.parse_expression(column, schema)?;
                let mut path = Vec::new();
                for key in keys {
                    // Parse the key expression
                    match &key.key {
                        Expr::Value(SQLValue::SingleQuotedString(s))
                        | Expr::Value(SQLValue::DoubleQuotedString(s)) => {
                            path.push(MapAccessKey::String(s.to_string()))
                        }
                        Expr::Value(SQLValue::Number(n, _)) => {
                            if let Ok(num) = n.parse::<i64>() {
                                path.push(MapAccessKey::Number(num))
                            } else {
                                return Err(format!("Invalid numeric key: {}", n));
                            }
                        }
                        _ => return Err(format!("Unsupported map access key type")),
                    }
                }
                Ok(Expression::MapAccess(MapAccessExpression::new(
                    Arc::new(parsed_column),
                    path,
                    Column::new("map_access", TypeId::VarChar), // Using VarChar as default type since map access typically returns string-like data
                )))
            }
            Expr::Method(method) => {
                let obj_expr = Arc::new(self.parse_expression(&method.expr, schema)?);
                let parsed_args = Vec::new();

                for func in &method.method_chain {
                    // Parse each function's arguments
                    self.parse_function(func, schema);
                }

                // Get the method name from the last function in the chain
                let method_name = if let Some(last_func) = method.method_chain.last() {
                    last_func
                        .name
                        .0
                        .last()
                        .map(|i| i.value.clone())
                        .unwrap_or_default()
                } else {
                    String::new()
                };

                // Determine return type based on method name
                let return_type = match method_name.as_str() {
                    "length" | "size" | "count" => Column::new(&method_name, TypeId::Integer),
                    "value" => Column::new(&method_name, TypeId::VarChar),
                    _ => Column::new(&method_name, TypeId::VarChar), // Default to VarChar
                };

                Ok(Expression::Method(MethodExpression::new(
                    obj_expr,
                    method_name,
                    parsed_args,
                    return_type,
                )))
            }
            Expr::Exists { subquery, negated } => {
                let subquery_expr = Arc::new(self.parse_subquery(subquery, schema)?);
                Ok(Expression::Exists(ExistsExpression::new(
                    subquery_expr,
                    *negated,
                    Column::new("exists", TypeId::Boolean),
                )))
            }
            Expr::Subquery(query) => self.parse_subquery(query, schema),
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
                    let field_name = field.field_name.as_ref()
                        .map(|ident| ident.value.clone())
                        .unwrap_or_else(|| String::from(""));

                    // Convert SQL DataType to our TypeId
                    let type_id = match field.field_type {
                        DataType::Int(_) | DataType::Integer(_) => TypeId::Integer,
                        DataType::BigInt(_) => TypeId::BigInt,
                        DataType::SmallInt(_) => TypeId::SmallInt,
                        DataType::TinyInt(_) => TypeId::TinyInt,
                        DataType::Float(_) | DataType::Double | DataType::Decimal(_) => TypeId::Decimal,
                        DataType::Char(_) => TypeId::Char,
                        DataType::Varchar(_) | DataType::Text => TypeId::VarChar,
                        DataType::Boolean => TypeId::Boolean,
                        DataType::Date | DataType::Time(_, _) | DataType::Timestamp(_, _) => TypeId::Timestamp,
                        DataType::Array(_) => TypeId::Vector,
                        DataType::Struct(_, _) => TypeId::Struct,
                        _ => return Err(format!("Unsupported struct field type: {:?}", field.field_type)),
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
            },
            Expr::Subscript { .. } => {
                Err("Subscript expressions are not yet supported".to_string())
            }
            Expr::Array(_) => Err("Array expressions are not yet supported".to_string()),
            Expr::Interval(_) => Err("Interval expressions are not yet supported".to_string()),
            Expr::Wildcard(_) => Err("Wildcard expressions are not yet supported".to_string()),
            Expr::QualifiedWildcard(_, _) => {
                Err("Qualified wildcard expressions are not yet supported".to_string())
            }
            _ => Err(format!("Unsupported expression type: {:?}", expr)),
        }
    }

    pub fn parse_query(query: Query) -> Result<Expression, String> {
        todo!()
    }

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

    pub fn get_table_schema(&self, table_name: &str) -> Result<Schema, String> {
        let catalog = self.catalog.read();
        catalog
            .get_table_schema(table_name)
            .ok_or_else(|| format!("Table '{}' not found in catalog", table_name))
    }

    pub fn get_table_oid(&self, table_name: &str) -> Result<TableOidT, String> {
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

    pub fn has_aggregate_functions(&self, projection: &[SelectItem]) -> bool {
        projection.iter().any(|item| match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                self.contains_aggregate_function(expr)
            }
            _ => false,
        })
    }

    pub fn determine_group_by_expressions(
        &self,
        select: &Box<Select>,
        schema: &Schema,
        has_group_by: bool,
    ) -> Result<Vec<Expression>, String> {
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

    pub fn parse_aggregates(
        &self,
        projection: &[SelectItem],
        schema: &Schema,
    ) -> Result<(Vec<Arc<Expression>>, Vec<String>), String> {
        let mut agg_exprs = Vec::new();
        let mut agg_names = Vec::new();

        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    if let Some(agg_expr) = self.try_parse_aggregate(expr, schema)? {
                        agg_exprs.push(Arc::new(agg_expr));
                        agg_names.push(expr.to_string());
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    if let Some(agg_expr) = self.try_parse_aggregate(expr, schema)? {
                        agg_exprs.push(Arc::new(agg_expr));
                        agg_names.push(alias.value.clone());
                    }
                }
                _ => {}
            }
        }

        Ok((agg_exprs, agg_names))
    }

    fn parse_at_timezone(
        &self,
        timestamp: &Expr,
        timezone: &Expr,
        schema: &Schema,
    ) -> Result<Expression, String> {
        // For typed string timestamps, we need to ensure they output RFC3339 format
        let timestamp_expr = match timestamp {
            Expr::TypedString { data_type, value } => {
                // Only allow TIMESTAMP typed strings
                if !matches!(data_type, DataType::Timestamp(_, _)) {
                    return Err(format!(
                        "AT TIME ZONE timestamp must be a timestamp type, got {:?}",
                        data_type
                    ));
                }
                // Create a TypedStringExpression that outputs VarChar for AT TIME ZONE
                Arc::new(Expression::TypedString(TypedStringExpression::new(
                    "TIMESTAMP".to_string(),
                    value.to_string(),
                    Column::new("timestamp", TypeId::VarChar),
                )))
            }
            _ => {
                // For non-TypedString expressions, parse and validate timestamp type
                let expr = Arc::new(self.parse_expression(timestamp, schema)?);
                let expr_type = expr.get_return_type().get_type();
                if expr_type != TypeId::Timestamp {
                    return Err(format!(
                        "AT TIME ZONE timestamp must be a timestamp type, got {:?}",
                        expr_type
                    ));
                }
                expr
            }
        };

        let timezone_expr = Arc::new(self.parse_expression(timezone, schema)?);

        // Validate that timezone expression returns a string type
        let timezone_type = timezone_expr.get_return_type().get_type();
        if !matches!(timezone_type, TypeId::VarChar | TypeId::Char) {
            return Err(format!(
                "AT TIME ZONE timezone must be a string type, got {:?}",
                timezone_type
            ));
        }

        // Create return type column - result is always a timestamp
        let return_type = Column::new("at_timezone", TypeId::Timestamp);

        Ok(Expression::AtTimeZone(AtTimeZoneExpression::new(
            timestamp_expr,
            timezone_expr,
            return_type,
        )))
    }

    fn are_types_comparable(left: TypeId, right: TypeId) -> bool {
        match (left, right) {
            // Same types are always comparable
            (a, b) if a == b => true,

            // Numeric types can be compared with each other
            (
                TypeId::Integer | TypeId::BigInt | TypeId::Decimal,
                TypeId::Integer | TypeId::BigInt | TypeId::Decimal,
            ) => true,

            // String types can be compared with each other
            (TypeId::Char | TypeId::VarChar, TypeId::Char | TypeId::VarChar) => true,

            // All types can be compared with NULL
            (_, TypeId::Invalid) | (TypeId::Invalid, _) => true,

            // Other type combinations are not comparable
            _ => false,
        }
    }

    fn parse_subquery(&self, query: &Query, schema: &Schema) -> Result<Expression, String> {
        // Extract the body of the query
        let body = &query.body;

        // For now, we only support SELECT expressions
        match body.as_ref() {
            SetExpr::Select(select) => {
                // Parse the SELECT statement
                let select_expr = Arc::new(self.parse_select_statement(select, schema)?);

                // Create a SubqueryExpression with appropriate type and return type
                Ok(Expression::Subquery(SubqueryExpression::new(
                    select_expr,
                    SubqueryType::Scalar, // Default to scalar for now
                    Column::new("subquery", TypeId::Vector), // Default to Vector type
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

    fn parse_select_statement(
        &self,
        select: &Box<Select>,
        schema: &Schema,
    ) -> Result<Expression, String> {
        // For now, just handle the first projection item
        if let Some(item) = select.projection.first() {
            match item {
                SelectItem::UnnamedExpr(expr) => self.parse_expression(expr, schema),
                SelectItem::ExprWithAlias { expr, .. } => self.parse_expression(expr, schema),
                _ => Err("Only simple expressions are supported in subqueries".to_string()),
            }
        } else {
            Err("Empty SELECT statement in subquery".to_string())
        }
    }

    fn parse_order_by_expressions(
        &self,
        order_exprs: &[OrderByExpr],
        schema: &Schema,
    ) -> Result<Vec<Arc<Expression>>, String> {
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
        let mut children = Vec::new();

        match args {
            FunctionArguments::None => {}
            FunctionArguments::Subquery(query) => {
                let parsed_expr = self.parse_subquery(query, schema)?;
                children.push(Arc::new(parsed_expr));
            }
            FunctionArguments::List(list) => {
                // Parse function arguments
                for arg in &list.args {
                    match arg {
                        FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                            match arg {
                                FunctionArgExpr::Expr(expr) => {
                                    let parsed_expr = self.parse_expression(expr, schema)?;
                                    children.push(Arc::new(parsed_expr));
                                }
                                FunctionArgExpr::QualifiedWildcard(_) => {
                                    return Err("Qualified wildcard in function arguments not yet supported".to_string());
                                }
                                FunctionArgExpr::Wildcard => {
                                    return Err("Wildcard in function arguments not yet supported"
                                        .to_string());
                                }
                            }
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
                                    return Err("Wildcard in function arguments not yet supported"
                                        .to_string());
                                }
                            }
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
            }
        }

        Ok(children)
    }

    fn parse_sql_window_specification(
        &self,
        spec: &WindowType,
        schema: &Schema,
    ) -> Result<Vec<Arc<Expression>>, String> {
        let mut children = Vec::new();

        // Extract the WindowSpec from WindowType
        let window_spec = match spec {
            WindowType::WindowSpec(spec) => spec,
            WindowType::NamedWindow(_) => return Err("Named windows are not supported yet".to_string()),
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
        // First check if this is an aggregate function
        let name = func.name.to_string().to_uppercase();
        if matches!(
            name.as_str(),
            "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "STDDEV" | "VARIANCE"
        ) {
            let args = self.parse_function_arguments(&func.args, schema)?;
            let agg_type = match name.as_str() {
                "COUNT" => AggregationType::Count,
                "SUM" => AggregationType::Sum,
                "AVG" => AggregationType::Avg,
                "MIN" => AggregationType::Min,
                "MAX" => AggregationType::Max,
                "STDDEV" => AggregationType::StdDev,
                "VARIANCE" => AggregationType::Variance,
                _ => return Err("Unknown aggregate function".to_string()),
            };

            let return_type = match agg_type {
                AggregationType::Count => Column::new(&name, TypeId::BigInt),
                AggregationType::Avg => Column::new(&name, TypeId::Decimal),
                _ if !args.is_empty() => {
                    let child_type = args[0].get_return_type().get_type();
                    Column::new(&name, child_type)
                }
                _ => return Err("Aggregate function requires arguments".to_string()),
            };

            return Ok(Expression::Aggregate(AggregateExpression::new(
                agg_type,
                    args,
                return_type,
            )));
        }

        // If not an aggregate, handle as a regular function
        let mut children = Vec::new();

        // Parse function arguments
        children.extend(self.parse_function_arguments(&func.args, schema)?);

        // Handle WITHIN GROUP clause if present
        if !func.within_group.is_empty() {
            children.extend(self.parse_order_by_expressions(&func.within_group, schema)?);
        }

        // Handle FILTER clause if present
        if let Some(filter_expr) = &func.filter {
            let parsed_expr = self.parse_expression(filter_expr, schema)?;
            children.push(Arc::new(parsed_expr));
        }

        // Handle OVER clause if present
        if let Some(spec) = &func.over {
            children.extend(self.parse_sql_window_specification(spec, schema)?);
        }

        // Create and return the regular function expression
        let return_type = FunctionExpression::infer_return_type(&func.name.to_string(), &children)?;
        Ok(Expression::Function(FunctionExpression::new(
            func.name.to_string(),
            children,
            return_type,
        )))
    }

    fn parse_window_expr(&self, expr: &Expr, schema: &Schema) -> Option<Arc<Expression>> {
        match self.parse_expression(expr, schema) {
            Ok(parsed_expr) => Some(Arc::new(parsed_expr)),
            Err(_) => None,
        }
    }

    fn contains_aggregate_function(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                matches!(
                    name.as_str(),
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "STDDEV" | "VARIANCE"
                )
            }
            Expr::BinaryOp { left, right, .. } => {
                self.contains_aggregate_function(left) || self.contains_aggregate_function(right)
            }
            Expr::UnaryOp { expr, .. } => self.contains_aggregate_function(expr),
            Expr::Nested(expr) => self.contains_aggregate_function(expr),
            _ => false,
        }
    }

    fn try_parse_aggregate(
        &self,
        expr: &Expr,
        schema: &Schema,
    ) -> Result<Option<Expression>, String> {
        match expr {
            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                match name.as_str() {
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "STDDEV" | "VARIANCE" => {
                        let args = self.parse_function_arguments(&func.args, schema)?;
                        let agg_type = match name.as_str() {
                            "COUNT" => AggregationType::Count,
                            "SUM" => AggregationType::Sum,
                            "AVG" => AggregationType::Avg,
                            "MIN" => AggregationType::Min,
                            "MAX" => AggregationType::Max,
                            "STDDEV" => AggregationType::StdDev,
                            "VARIANCE" => AggregationType::Variance,
                            _ => return Ok(None),
                        };

                        let return_type = match agg_type {
                            AggregationType::Count => Column::new(&name, TypeId::BigInt),
                            AggregationType::Avg => Column::new(&name, TypeId::Decimal),
                            _ if !args.is_empty() => {
                                let child_type = args[0].get_return_type().get_type();
                                Column::new(&name, child_type)
                            }
                            _ => return Err("Aggregate function requires arguments".to_string()),
                        };

                        Ok(Some(Expression::Aggregate(AggregateExpression::new(
                            agg_type,
                            args,
                            return_type,
                        ))))
                    }
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }
}

/// Structure to hold function argument information
#[derive(Clone)]
struct FunctionArgInfo {
    expr: Expression,
    name: Option<String>,
    operator: Option<FunctionArgOperator>,
    is_named: bool,
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
    use sqlparser::ast::{SetExpr, Statement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::collections::HashMap;
    use tempfile::TempDir;

    struct TestContext {
        catalog: Arc<RwLock<Catalog>>,
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

            Self {
                catalog,
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
            (
                "CASE WHEN age > 18 THEN 'Adult' ELSE 'Minor' END",
                TypeId::VarChar,
            ),
            // CASE with multiple WHEN clauses
            (
                "CASE WHEN age < 13 THEN 'Child' WHEN age < 20 THEN 'Teen' ELSE 'Adult' END",
                TypeId::VarChar,
            ),
            // CASE with expression
            (
                "CASE age WHEN 18 THEN 'New Adult' WHEN 21 THEN 'Drinking Age' ELSE 'Other' END",
                TypeId::VarChar,
            ),
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
            ("LOWER(UPPER(name))", TypeId::VarChar), // Nested function calls
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
            ("UPPER(CASE WHEN age < 18 THEN 'Minor' ELSE LOWER(name) END)", TypeId::VarChar),
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

    #[test]
    fn test_parse_aggregate_functions() {
        let ctx = TestContext::new("test_parse_aggregate_functions");
        let schema = ctx.setup_test_schema();

        let test_cases = vec![
            ("COUNT(*)", AggregationType::CountStar, TypeId::BigInt),
            ("COUNT(id)", AggregationType::Count, TypeId::BigInt),
            ("SUM(salary)", AggregationType::Sum, TypeId::Decimal),
            ("AVG(age)", AggregationType::Avg, TypeId::Integer),
            ("MIN(salary)", AggregationType::Min, TypeId::Decimal),
            ("MAX(age)", AggregationType::Max, TypeId::Integer),
        ];

        for (expr_str, expected_type, expected_return_type) in test_cases {
            let expr = ctx
                .parse_expression(expr_str, &schema, ctx.catalog())
                .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", expr_str, e));

            match expr {
                Expression::Aggregate(agg) => {
                    assert_eq!(
                        *agg.get_agg_type(),
                        expected_type,
                        "Aggregate function '{}' should be {:?}",
                        expr_str,
                        expected_type
                    );
                    assert_eq!(
                        agg.get_return_type().get_type(),
                        expected_return_type,
                        "Aggregate function '{}' should return {:?}",
                        expr_str,
                        expected_return_type
                    );
                }
                _ => panic!("Expected Aggregate expression for '{}'", expr_str),
            }
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
                .parse_expression(expr_str, &schema, ctx.catalog())
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
            // Nested arithmetic and comparison
            ("(age + 5) * 2 > salary / 1000", TypeId::Boolean),
            // Nested functions and CASE
            (
                "UPPER(CASE WHEN age < 18 THEN 'Minor' ELSE LOWER(name) END)",
                TypeId::VarChar,
            ),
            // Complex logical expression
            (
                "(age BETWEEN 20 AND 30) AND (LOWER(name) = 'john' OR salary > 50000)",
                TypeId::Boolean,
            ),
            // Nested aggregates with arithmetic
            ("SUM(salary) / COUNT(id) > 50000", TypeId::Boolean),
            // Multiple conditions with parentheses
            (
                "(age > 20 AND salary >= 30000) OR (age > 30 AND salary >= 50000)",
                TypeId::Boolean,
            ),
        ];

        for (expr_str, expected_type) in test_cases {
            let expr = ctx
                .parse_expression(expr_str, &schema, ctx.catalog())
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
    fn test_parse_null_expressions() {
        let ctx = TestContext::new("test_parse_null_expressions");
        let schema = ctx.setup_test_schema();

        let test_cases = vec![
            "name IS NULL",
            "age IS NOT NULL",
            "salary IS NULL AND age > 20",
            "name IS NOT NULL OR salary < 50000",
        ];

        for expr_str in test_cases {
            let expr = ctx
                .parse_expression(expr_str, &schema, ctx.catalog())
                .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", expr_str, e));

            match expr {
                Expression::Comparison(comp) => {
                    // For simple IS NULL / IS NOT NULL
                    let children = comp.get_children();
                    assert_eq!(children.len(), 2, "NULL check should have 2 operands");

                    // Verify second operand is NULL constant
                    match children[1].as_ref() {
                        Expression::Constant(c) => {
                            assert!(matches!(c.get_value().get_val(), Val::Null));
                        }
                        _ => {
                            // For compound expressions with AND/OR, we don't check the structure
                            // as it's already covered by other tests
                        }
                    }
                }
                Expression::Logic(_) => {
                    // For compound expressions with AND/OR, just verify it parsed successfully
                }
                _ => panic!("Expected Comparison or Logic expression for '{}'", expr_str),
            }
        }
    }

    #[test]
    fn test_parse_error_cases() {
        let ctx = TestContext::new("test_parse_error_cases");
        let schema = ctx.setup_test_schema();

        let test_cases = vec![
            // Invalid column reference
            "invalid_column > 10",
            // Invalid function
            "INVALID_FUNCTION(age)",
            // Invalid CAST
            "CAST(age AS INVALID_TYPE)",
            // Mismatched types
            "name > 10",
            // Invalid aggregate usage
            // "COUNT(COUNT(*))",
        ];

        for expr_str in test_cases {
            let result = ctx.parse_expression(expr_str, &schema, ctx.catalog());
            assert!(
                result.is_err(),
                "Expected error for invalid expression '{}', but got success",
                expr_str
            );
        }
    }

    #[test]
    fn test_parse_at_timezone() {
        let ctx = TestContext::new("test_parse_at_timezone");
        let schema = ctx.setup_test_schema();

        let test_cases = vec![
            // Test valid timestamp AT TIME ZONE expressions
            "timestamp '2020-01-01 12:00:00' AT TIME ZONE 'UTC'",
            "timestamp '2020-01-01 12:00:00' AT TIME ZONE 'GMT'",
        ];

        for expr_str in test_cases {
            let result = ctx.parse_expression(expr_str, &schema, ctx.catalog());
            assert!(
                result.is_ok(),
                "Failed to parse '{}': {:?}",
                expr_str,
                result.err()
            );
        }
    }

    #[test]
    fn test_parse_at_timezone_invalid_types() {
        let ctx = TestContext::new("test_parse_at_timezone_invalid_types");
        let schema = ctx.setup_test_schema();

        let test_cases = vec![
            // Test invalid timestamp expressions
            "123 AT TIME ZONE 'UTC'", // Invalid timestamp literal
            "'not a timestamp' AT TIME ZONE 'UTC'", // Invalid timestamp string
        ];

        for expr_str in test_cases {
            let result = ctx.parse_expression(expr_str, &schema, ctx.catalog());
            assert!(
                result.is_err(),
                "Expected error for invalid expression: {}",
                expr_str
            );
        }
    }
}
