use crate::catalog::catalog::Catalog;
use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::config::TableOidT;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::aggregate_expression::{
    AggregateExpression, AggregationType,
};
use crate::sql::execution::expressions::all_expression::AllExpression;
use crate::sql::execution::expressions::any_expression::AnyExpression;
use crate::sql::execution::expressions::at_timezone_expression::AtTimeZoneExpression;
use crate::sql::execution::expressions::binary_op_expression::BinaryOpExpression;
use crate::sql::execution::expressions::case_expression::CaseExpression;
use crate::sql::execution::expressions::cast_expression::CastExpression;
use crate::sql::execution::expressions::ceil_floor_expression::{
    CeilFloorExpression, CeilFloorOperation, DateTimeField,
};
use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::sql::execution::expressions::comparison_expression::{
    ComparisonExpression, ComparisonType,
};
use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
use crate::sql::execution::expressions::convert_expression::ConvertExpression;
use crate::sql::execution::expressions::is_check_expression::{IsCheckExpression, IsCheckType};
use crate::sql::execution::expressions::literal_value_expression::LiteralValueExpression;
use crate::sql::execution::expressions::logic_expression::{LogicExpression, LogicType};
use crate::sql::execution::expressions::overlay_expression::OverlayExpression;
use crate::sql::execution::expressions::position_expression::PositionExpression;
use crate::sql::execution::expressions::regex_expression::{RegexExpression, RegexOperator};
use crate::sql::execution::expressions::string_expression::{
    StringExpression, StringExpressionType,
};
use crate::sql::execution::expressions::substring_expression::SubstringExpression;
use crate::sql::execution::expressions::trim_expression::{TrimExpression, TrimType};
use crate::sql::execution::expressions::unary_op_expression::UnaryOpExpression;
use crate::sql::planner::logical_plan::LogicalPlan;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use log::debug;
use parking_lot::RwLock;
use sqlparser::ast::{
    CastFormat, CeilFloorKind, DataType, DuplicateTreatment, Expr, Function, FunctionArg,
    FunctionArgExpr, FunctionArgOperator, FunctionArgumentClause, FunctionArgumentList,
    FunctionArguments, GroupByExpr, JoinConstraint, JoinOperator, ListAggOnOverflow, NullTreatment,
    ObjectName, Query, Select, SelectItem, TableFactor,
};
use std::fmt;
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

            Expr::Value(value) => Ok(Expression::Literal(LiteralValueExpression::new(*value)?)),

            Expr::BinaryOp { left, op, right } => {
                let left_expr = Arc::new(self.parse_expression(left, schema)?);
                let right_expr = Arc::new(self.parse_expression(right, schema)?);

                Ok(Expression::BinaryOp(BinaryOpExpression::new(
                    left_expr.clone(),
                    right_expr.clone(),
                    op.clone(),
                    vec![left_expr, right_expr],
                )?))
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

            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                match name.as_str() {
                    "SUM" | "COUNT" | "AVG" | "MIN" | "MAX" => {
                        // Handle aggregate functions in HAVING clause
                        self.parse_aggregate_function(&func, schema)
                    }
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
                    "SUBSTRING" => self.parse_substring_function(&func, schema),
                    "TRIM" | "LTRIM" | "RTRIM" => self.parse_trim_function(&func, schema),
                    _ => Err(format!("Unsupported function: {}", name)),
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
                        CastFormat::Expr(format_expr) => {
                            // Parse format expression and evaluate it
                            let format_value = self.parse_expression(format_expr, schema)?;
                            // TODO: Evaluate format expression to get format string
                            // For now just use default format
                            cast_expr = cast_expr.with_format("default".to_string());
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
            } => self.parse_at_timezone(timestamp, time_zone, schema),

            Expr::SimilarTo {
                negated,
                expr,
                pattern,
                escape_char,
            } => {
                let expr = Arc::new(self.parse_expression(expr, schema)?);
                let pattern = Arc::new(self.parse_expression(pattern, schema)?);

                Ok(Expression::Regex(RegexExpression::new(
                    expr,
                    pattern,
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
                let charset_str = charset.map(|name| name.to_string());

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
                    is_try,
                    style_exprs,
                    return_type,
                )))
            }

            Expr::Ceil { expr, field } => {
                let inner_expr = Arc::new(self.parse_expression(expr, schema)?);

                let datetime_field = match field {
                    Some(CeilFloorKind::DateTimeField(field)) => Some(match field {
                        DateTimeField::Year => DateTimeField::Year,
                        DateTimeField::Month => DateTimeField::Month,
                        DateTimeField::Day => DateTimeField::Day,
                        DateTimeField::Hour => DateTimeField::Hour,
                        DateTimeField::Minute => DateTimeField::Minute,
                        DateTimeField::Second => DateTimeField::Second,
                    }),
                    Some(CeilFloorKind::Scale(scale_expr)) => {
                        let scale = Arc::new(self.parse_expression(scale_expr, schema)?);
                        return Ok(Expression::CeilFloor(CeilFloorExpression::new(
                            CeilFloorOperation::Ceil,
                            inner_expr,
                            Some(scale),
                            None,
                        )?));
                    }
                    None => None,
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
                    Some(CeilFloorKind::DateTimeField(field)) => Some(match field {
                        DateTimeField::Year => DateTimeField::Year,
                        DateTimeField::Month => DateTimeField::Month,
                        DateTimeField::Day => DateTimeField::Day,
                        DateTimeField::Hour => DateTimeField::Hour,
                        DateTimeField::Minute => DateTimeField::Minute,
                        DateTimeField::Second => DateTimeField::Second,
                    }),
                    Some(CeilFloorKind::Scale(scale_expr)) => {
                        let scale = Arc::new(self.parse_expression(scale_expr, schema)?);
                        return Ok(Expression::CeilFloor(CeilFloorExpression::new(
                            CeilFloorOperation::Floor,
                            inner_expr,
                            Some(scale),
                            None,
                        )?));
                    }
                    None => None,
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

            _ => Err(format!("Unsupported expression type: {:?}", expr)),
        }
    }

    fn parse_at_timezone(
        &self,
        timestamp: &Expr,
        timezone: &Expr,
        schema: &Schema,
    ) -> Result<Arc<Expression>, String> {
        let timestamp_expr = self.parse_expression(timestamp, schema)?;
        let timezone_expr = self.parse_expression(timezone, schema)?;

        // Validate that timestamp expression returns a timestamp type
        let timestamp_type = timestamp_expr.get_return_type().get_type();
        if !matches!(timestamp_type, TypeId::Timestamp) {
            return Err(ExpressionError::InvalidOperation(format!(
                "AT TIME ZONE requires timestamp input, got {:?}",
                timestamp_type
            ))
            .to_string());
        }

        // Validate that timezone expression returns a string type
        let timezone_type = timezone_expr.get_return_type().get_type();
        if !matches!(timezone_type, TypeId::VarChar) {
            return Err(ExpressionError::InvalidOperation(format!(
                "AT TIME ZONE requires string timezone, got {:?}",
                timezone_type
            ))
            .to_string());
        }

        // Create AtTimeZoneExpression
        Ok(Arc::new(Expression::AtTimeZone(AtTimeZoneExpression::new(
            Arc::from(timestamp_expr),
            Arc::from(timezone_expr),
            // Return type is always timestamp
            Column::new("at_timezone", TypeId::Timestamp),
        ))))
    }

    fn parse_aggregate_function(
        &self,
        func: &Function,
        schema: &Schema,
    ) -> Result<Expression, String> {
        let func_name = func.name.to_string().to_uppercase();

        match func_name.as_str() {
            "AVG" | "MAX" | "MIN" | "SUM" | "EVERY" | "ANY" | "SOME" | "COUNT" | "STDDEV_POP"
            | "STDDEV_SAMP" | "VAR_SAMP" | "VAR_POP" | "COLLECT" | "FUSION" | "INTERSECTION" => {
                self.parse_general_set_function(&func, schema)
            }
            "COVAR_POP" | "COVAR_SAMP" | "CORR" | "REGR_SLOPE" | "REGR_INTERCEPT"
            | "REGR_COUNT" | "REGR_R2" | "REGR_AVGX" | "REGR_AVGY" | "REGR_SXX" | "REGR_SYY"
            | "REGR_SXY" => self.parse_binary_set_function(&func, schema),
            "RANK" | "DENSE_RANK" | "PERCENT_RANK" | "CUME_DIST" | "PERCENTILE_CONT"
            | "PERCENTILE_DISC" | "LISTAGG" => self.parse_ordered_set_function(&func, schema),
            "ARRAY_AGG" => self.parse_array_aggregate_function(&func, schema),
            _ => Err(format!("Unsupported aggregate function: {}", func_name)),
        }
    }

    /// Parse function arguments and return a vector of parsed expressions along with their metadata
    fn parse_function_arguments(
        &self,
        args: &FunctionArgumentList,
        schema: &Schema,
    ) -> Result<Vec<FunctionArgInfo>, String> {
        let mut parsed_args = Vec::new();

        // Handle DISTINCT/ALL modifier if present
        let is_distinct = matches!(args.duplicate_treatment, Some(DuplicateTreatment::Distinct));

        // Parse each argument
        for arg in &args.args {
            let arg_info = self.parse_function_arg(arg, schema)?;
            parsed_args.push(arg_info);
        }

        // Handle additional clauses
        self.handle_function_clauses(&args.clauses, schema)?;

        Ok(parsed_args)
    }

    /// Parse a single function argument with its metadata
    fn parse_function_arg(
        &self,
        arg: &FunctionArg,
        schema: &Schema,
    ) -> Result<FunctionArgInfo, String> {
        match arg {
            FunctionArg::Named {
                name,
                arg,
                operator,
            } => {
                let parsed_expr = self.parse_function_arg_expr(arg, schema)?;
                Ok(FunctionArgInfo {
                    expr: parsed_expr,
                    name: Some(name.value.clone()),
                    operator: Some(operator.clone()),
                    is_named: true,
                })
            }
            FunctionArg::ExprNamed {
                name,
                arg,
                operator,
            } => {
                let parsed_expr = self.parse_function_arg_expr(arg, schema)?;
                let name_expr = self.parse_expression(name, schema)?;

                // For ExprNamed, we evaluate the name expression to get the parameter name
                let name_str = match &name_expr {
                    Expression::Constant(c) => c.get_value().to_string(),
                    Expression::ColumnRef(c) => c.get_return_type().get_name().to_string(),
                    _ => {
                        return Err(
                            "Function parameter name must be a constant or column reference"
                                .to_string(),
                        )
                    }
                };

                Ok(FunctionArgInfo {
                    expr: parsed_expr,
                    name: Some(name_str),
                    operator: Some(operator.clone()),
                    is_named: true,
                })
            }
            FunctionArg::Unnamed(arg_expr) => {
                let parsed_expr = self.parse_function_arg_expr(arg_expr, schema)?;
                Ok(FunctionArgInfo {
                    expr: parsed_expr,
                    name: None,
                    operator: None,
                    is_named: false,
                })
            }
        }
    }

    /// Handle function argument operators
    fn handle_function_arg_operator(
        &self,
        operator: &FunctionArgOperator,
        value: Expression,
    ) -> Result<Expression, String> {
        match operator {
            FunctionArgOperator::Equals => {
                // Simple assignment, return the value as is
                Ok(value)
            }
            FunctionArgOperator::RightArrow => {
                // Arrow operator might have special semantics in some contexts
                Ok(value)
            }
            FunctionArgOperator::Assignment => {
                // := operator, similar to equals but might have different precedence
                Ok(value)
            }
            FunctionArgOperator::Colon => {
                // : operator might indicate type casting or special handling
                Ok(value)
            }
            FunctionArgOperator::Value => {
                // VALUE keyword might have special meaning in some contexts
                Ok(value)
            }
        }
    }

    /// Handle additional function argument clauses
    fn handle_function_clauses(
        &self,
        clauses: &[FunctionArgumentClause],
        schema: &Schema,
    ) -> Result<Expression, String> {
        // If no clauses, return a null constant to indicate no special handling needed
        if clauses.is_empty() {
            return Ok(Expression::Constant(ConstantExpression::new(
                Value::new(Val::Null),
                Column::new("clause", TypeId::Invalid),
                vec![],
            )));
        }

        let mut clause_exprs = Vec::new();

        for clause in clauses {
            let clause_expr = match clause {
                FunctionArgumentClause::IgnoreOrRespectNulls(null_treatment) => {
                    // Create a constant expression to represent the null treatment
                    let value = match null_treatment {
                        NullTreatment::IgnoreNulls => "IGNORE NULLS",
                        NullTreatment::RespectNulls => "RESPECT NULLS",
                    };
                    Expression::Constant(ConstantExpression::new(
                        Value::new(value),
                        Column::new("null_treatment", TypeId::VarChar),
                        vec![],
                    ))
                }
                FunctionArgumentClause::OrderBy(order_by_exprs) => {
                    // Parse each ORDER BY expression
                    let mut order_exprs = Vec::new();
                    for order_expr in order_by_exprs {
                        let expr = self.parse_expression(&order_expr.expr, schema)?;
                        order_exprs.push(Arc::new(expr));
                    }
                    // Create a mock expression to hold the ORDER BY info
                    Expression::Constant(ConstantExpression::new(
                        Value::new("ORDER BY"),
                        Column::new("order_by", TypeId::VarChar),
                        order_exprs,
                    ))
                }
                FunctionArgumentClause::Limit(limit_expr) => {
                    // Parse the LIMIT expression
                    let expr = self.parse_expression(limit_expr, schema)?;
                    Expression::Constant(ConstantExpression::new(
                        Value::new("LIMIT"),
                        Column::new("limit", TypeId::VarChar),
                        vec![Arc::new(expr)],
                    ))
                }
                FunctionArgumentClause::OnOverflow(overflow_behavior) => match overflow_behavior {
                    ListAggOnOverflow::Error => Expression::Constant(ConstantExpression::new(
                        Value::new("ON OVERFLOW ERROR"),
                        Column::new("overflow", TypeId::VarChar),
                        vec![],
                    )),
                    ListAggOnOverflow::Truncate { filler, with_count } => {
                        let mut children = Vec::new();
                        if let Some(filler_expr) = filler {
                            let expr = self.parse_expression(filler_expr, schema)?;
                            children.push(Arc::new(expr));
                        }
                        Expression::Constant(ConstantExpression::new(
                            Value::new(if *with_count {
                                "ON OVERFLOW TRUNCATE WITH COUNT"
                            } else {
                                "ON OVERFLOW TRUNCATE"
                            }),
                            Column::new("overflow", TypeId::VarChar),
                            children,
                        ))
                    }
                },
                FunctionArgumentClause::Having(having_bound) => {
                    // Store having bound information as a constant
                    Expression::Constant(ConstantExpression::new(
                        Value::new(format!("HAVING {:?}", having_bound)),
                        Column::new("having", TypeId::VarChar),
                        vec![],
                    ))
                }
                FunctionArgumentClause::Separator(separator_value) => {
                    // Store separator value as a constant
                    Expression::Constant(ConstantExpression::new(
                        Value::new(separator_value.to_string()),
                        Column::new("separator", TypeId::VarChar),
                        vec![],
                    ))
                }
                FunctionArgumentClause::JsonNullClause(json_null_clause) => {
                    // Store JSON NULL clause information as a constant
                    Expression::Constant(ConstantExpression::new(
                        Value::new(format!("JSON NULL {:?}", json_null_clause)),
                        Column::new("json_null", TypeId::VarChar),
                        vec![],
                    ))
                }
            };
            clause_exprs.push(Arc::new(clause_expr));
        }

        // If there's only one clause, return it directly
        if clause_exprs.len() == 1 {
            return Ok((*clause_exprs[0]).clone());
        }

        // If multiple clauses, combine them using a mock expression
        Ok(Expression::Constant(ConstantExpression::new(
            Value::new("CLAUSES"),
            Column::new("clauses", TypeId::VarChar),
            clause_exprs,
        )))
    }

    /// Update the general set function parsing to use the new argument handling
    fn parse_general_set_function(
        &self,
        func: &Function,
        schema: &Schema,
    ) -> Result<Expression, String> {
        let func_name = func.name.to_string().to_uppercase();

        match &func.args {
            FunctionArguments::List(arg_list) => {
                let parsed_args = self.parse_function_arguments(arg_list, schema)?;

                if parsed_args.is_empty() {
                    return Err(format!("{} requires at least one argument", func_name));
                }

                let is_distinct = matches!(
                    arg_list.duplicate_treatment,
                    Some(DuplicateTreatment::Distinct)
                );

                // Get the first argument's expression
                let first_arg = &parsed_args[0].expr;

                let agg_type = match func_name.as_str() {
                    "COUNT" => {
                        if matches!(
                            arg_list.args.first(),
                            Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard))
                        ) {
                            AggregationType::CountStar
                        } else {
                            AggregationType::Count
                        }
                    }
                    "SUM" => AggregationType::Sum,
                    "AVG" => AggregationType::Avg,
                    "MIN" => AggregationType::Min,
                    "MAX" => AggregationType::Max,
                    _ => return Err(format!("Unsupported aggregate function: {}", func_name)),
                };

                let return_type = match agg_type {
                    AggregationType::Count | AggregationType::CountStar => TypeId::BigInt,
                    _ => first_arg.get_return_type().get_type(),
                };

                let mut agg_expr =
                    AggregateExpression::new(agg_type, Arc::new(first_arg.clone()), vec![])
                        .with_return_type(Column::new(&func_name, return_type));

                // Handle DISTINCT if present
                if is_distinct {
                    agg_expr = agg_expr.with_distinct(true);
                }

                Ok(Expression::Aggregate(agg_expr))
            }
            _ => Err(format!("{} requires arguments", func_name)),
        }
    }

    /// Parse binary set functions like COVAR_POP, CORR, etc.
    fn parse_binary_set_function(
        &self,
        func: &Function,
        schema: &Schema,
    ) -> Result<Expression, String> {
        let func_name = func.name.to_string().to_uppercase();

        match &func.args {
            FunctionArguments::List(arg_list) => {
                let parsed_args = self.parse_function_arguments(arg_list, schema)?;

                // Binary set functions require exactly 2 arguments
                if parsed_args.len() != 2 {
                    return Err(format!("{} requires exactly two arguments", func_name));
                }

                let first_arg = Arc::new(parsed_args[0].expr.clone());
                let second_arg = Arc::new(parsed_args[1].expr.clone());

                // Determine return type based on function
                let return_type = match func_name.as_str() {
                    "COVAR_POP" | "COVAR_SAMP" | "CORR" | "REGR_SLOPE" | "REGR_INTERCEPT"
                    | "REGR_R2" => TypeId::Decimal,
                    "REGR_COUNT" => TypeId::BigInt,
                    "REGR_AVGX" | "REGR_AVGY" | "REGR_SXX" | "REGR_SYY" | "REGR_SXY" => {
                        TypeId::Decimal
                    }
                    _ => return Err(format!("Unsupported binary set function: {}", func_name)),
                };

                let agg_type = match func_name.as_str() {
                    "COVAR_POP" => AggregationType::CovarPop,
                    "COVAR_SAMP" => AggregationType::CovarSamp,
                    "CORR" => AggregationType::Correlation,
                    "REGR_SLOPE" => AggregationType::RegrSlope,
                    "REGR_INTERCEPT" => AggregationType::RegrIntercept,
                    "REGR_COUNT" => AggregationType::RegrCount,
                    "REGR_R2" => AggregationType::RegrR2,
                    "REGR_AVGX" => AggregationType::RegrAvgX,
                    "REGR_AVGY" => AggregationType::RegrAvgY,
                    "REGR_SXX" => AggregationType::RegrSXX,
                    "REGR_SYY" => AggregationType::RegrSYY,
                    "REGR_SXY" => AggregationType::RegrSXY,
                    _ => return Err(format!("Unsupported binary set function: {}", func_name)),
                };

                Ok(Expression::Aggregate(
                    AggregateExpression::new(
                        agg_type,
                        first_arg.clone(),
                        vec![first_arg, second_arg],
                    )
                    .with_return_type(Column::new(&func_name, return_type)),
                ))
            }
            _ => Err(format!("{} requires a list of arguments", func_name)),
        }
    }

    /// Parse ordered set functions like RANK, DENSE_RANK, etc.
    fn parse_ordered_set_function(
        &self,
        func: &Function,
        schema: &Schema,
    ) -> Result<Expression, String> {
        let func_name = func.name.to_string().to_uppercase();

        match &func.args {
            FunctionArguments::List(arg_list) => {
                let parsed_args = self.parse_function_arguments(arg_list, schema)?;

                if parsed_args.is_empty() {
                    return Err(format!("{} requires at least one argument", func_name));
                }

                let first_arg = Arc::new(parsed_args[0].expr.clone());
                let mut children = vec![first_arg.clone()];

                // Add additional arguments if present
                for arg in parsed_args.iter().skip(1) {
                    children.push(Arc::new(arg.expr.clone()));
                }

                let return_type = match func_name.as_str() {
                    "RANK" | "DENSE_RANK" => TypeId::BigInt,
                    "PERCENT_RANK" | "CUME_DIST" => TypeId::Decimal,
                    "PERCENTILE_CONT" | "PERCENTILE_DISC" => first_arg.get_return_type().get_type(),
                    "LISTAGG" => TypeId::VarChar,
                    _ => return Err(format!("Unsupported ordered set function: {}", func_name)),
                };

                let agg_type = match func_name.as_str() {
                    "RANK" => AggregationType::Rank,
                    "DENSE_RANK" => AggregationType::DenseRank,
                    "PERCENT_RANK" => AggregationType::PercentRank,
                    "CUME_DIST" => AggregationType::CumeDist,
                    "PERCENTILE_CONT" => AggregationType::PercentileCont,
                    "PERCENTILE_DISC" => AggregationType::PercentileDisc,
                    "LISTAGG" => AggregationType::ListAgg,
                    _ => return Err(format!("Unsupported ordered set function: {}", func_name)),
                };

                Ok(Expression::Aggregate(
                    AggregateExpression::new(agg_type, first_arg, children)
                        .with_return_type(Column::new(&func_name, return_type)),
                ))
            }
            _ => Err(format!("{} requires a list of arguments", func_name)),
        }
    }

    /// Parse array aggregate functions
    fn parse_array_aggregate_function(
        &self,
        func: &Function,
        schema: &Schema,
    ) -> Result<Expression, String> {
        let func_name = func.name.to_string().to_uppercase();

        match &func.args {
            FunctionArguments::List(arg_list) => {
                let parsed_args = self.parse_function_arguments(arg_list, schema)?;

                if parsed_args.is_empty() {
                    return Err(format!("{} requires at least one argument", func_name));
                }

                let first_arg = Arc::new(parsed_args[0].expr.clone());
                let mut children = vec![first_arg.clone()];

                // Add additional arguments if present
                for arg in parsed_args.iter().skip(1) {
                    children.push(Arc::new(arg.expr.clone()));
                }

                // Array aggregate functions return a vector of the input type
                let element_type = first_arg.get_return_type().get_type();

                Ok(Expression::Aggregate(
                    AggregateExpression::new(AggregationType::ArrayAgg, first_arg, children)
                        .with_return_type(Column::new(&func_name, TypeId::Vector))
                        .with_element_type(element_type),
                ))
            }
            _ => Err(format!("{} requires a list of arguments", func_name)),
        }
    }

    pub fn parse_query(query: Query) -> Result<Expression, String> {
        todo!()
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
                SelectItem::UnnamedExpr(Expr::Function(func)) => {
                    let expr = self.parse_aggregate_function(&func, schema)?;
                    if let Expression::Aggregate(agg_expr) = &expr {
                        agg_types.push(agg_expr.get_agg_type().clone());
                        agg_exprs.push(Arc::new(expr));
                    }
                }
                SelectItem::ExprWithAlias {
                    expr: Expr::Function(func),
                    alias,
                } => {
                    let expr = self.parse_aggregate_function(&func, schema)?;
                    if let Expression::Aggregate(agg_expr) = &expr {
                        // Create new aggregate expression with aliased column
                        let orig_type = agg_expr.get_return_type().get_type();
                        let new_col = Column::new(&alias.value, orig_type);
                        let new_agg = AggregateExpression::new(
                            agg_expr.get_agg_type().clone(),
                            agg_expr.get_arg().clone(),
                            vec![],
                        )
                        .with_return_type(new_col);
                        agg_exprs.push(Arc::new(Expression::Aggregate(new_agg)));
                        agg_types.push(agg_expr.get_agg_type().clone());
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
                SelectItem::QualifiedWildcard(_, _) => {
                    // Handle qualified wildcard similar to regular wildcard
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
                // Add handling for non-aggregate columns in projection
                SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. } => {
                    // For non-aggregate expressions, just skip them
                    // They will be handled by the projection plan
                    continue;
                }
            }
        }

        debug!("Parsed aggregate expressions: {:?}", agg_exprs);
        debug!("Parsed aggregate types: {:?}", agg_types);

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

    /// Parse a single function argument expression
    fn parse_function_arg_expr(
        &self,
        arg_expr: &FunctionArgExpr,
        schema: &Schema,
    ) -> Result<Expression, String> {
        match arg_expr {
            FunctionArgExpr::Expr(expr) => {
                // Regular expression argument - parse using existing expression parser
                self.parse_expression(expr, schema)
            }
            FunctionArgExpr::QualifiedWildcard(object_name) => {
                // Handle qualified wildcard (table.*)
                // For aggregate functions like COUNT(table.*), we want to count all columns
                // from the specified table

                // Get the table name/alias
                let table_name = object_name.to_string();

                // Look up the table in the catalog
                let catalog = self.catalog.read();
                if let Some(_table) = catalog.get_table(&table_name) {
                    // For COUNT(*), return a constant 1 that will be counted
                    Ok(Expression::Constant(ConstantExpression::new(
                        Value::new(1),
                        Column::new(&format!("{}.* ", table_name), TypeId::Integer),
                        vec![],
                    )))
                } else {
                    // Try looking up as alias in the schema
                    let matching_columns: Vec<_> = (0..schema.get_column_count())
                        .filter_map(|i| {
                            let col = schema.get_column(i as usize).unwrap();
                            if col.get_name().starts_with(&format!("{}.", table_name)) {
                                Some(i)
                            } else {
                                None
                            }
                        })
                        .collect();

                    if matching_columns.is_empty() {
                        Err(format!("Table or alias '{}' not found", table_name))
                    } else {
                        // For COUNT(*), return a constant 1 that will be counted
                        Ok(Expression::Constant(ConstantExpression::new(
                            Value::new(1),
                            Column::new(&format!("{}.* ", table_name), TypeId::Integer),
                            vec![],
                        )))
                    }
                }
            }
            FunctionArgExpr::Wildcard => {
                // Handle unqualified wildcard (*)
                // For aggregate functions like COUNT(*), we want to count all rows
                // Return a constant 1 that will be counted
                Ok(Expression::Constant(ConstantExpression::new(
                    Value::new(1),
                    Column::new("*", TypeId::Integer),
                    vec![],
                )))
            }
        }
    }

    fn parse_substring_function(
        &self,
        func: &Function,
        schema: &Schema,
    ) -> Result<Expression, String> {
        match &func.args {
            FunctionArguments::List(arg_list) => {
                let args = &arg_list.args;

                // Check if we have at least one argument
                if args.is_empty() {
                    return Err("SUBSTRING requires at least one argument".to_string());
                }

                // Parse the string expression (first argument)
                let string_expr = match &args[0] {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                        Arc::new(self.parse_expression(expr, schema)?)
                    }
                    _ => return Err("Invalid first argument for SUBSTRING".to_string()),
                };

                // Check if we're using the SQL standard syntax with FROM/FOR keywords
                let mut from_expr = None;
                let mut for_expr = None;
                let mut is_standard_syntax = false;

                // Process remaining arguments
                for i in 1..args.len() {
                    match &args[i] {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                            // For the comma-separated syntax: SUBSTRING(str, start, length)
                            if is_standard_syntax {
                                return Err(
                                    "Cannot mix standard and non-standard SUBSTRING syntax"
                                        .to_string(),
                                );
                            }

                            if i == 1 {
                                // This is the start position
                                from_expr = Some(Arc::new(self.parse_expression(expr, schema)?));
                            } else if i == 2 {
                                // This is the length
                                for_expr = Some(Arc::new(self.parse_expression(expr, schema)?));
                            } else {
                                return Err("Too many arguments for SUBSTRING".to_string());
                            }
                        }
                        FunctionArg::Named { name, arg, .. } => {
                            // For the standard syntax: SUBSTRING(str FROM start FOR length)
                            is_standard_syntax = true;

                            match name.value.to_uppercase().as_str() {
                                "FROM" => {
                                    if let FunctionArgExpr::Expr(expr) = arg {
                                        from_expr =
                                            Some(Arc::new(self.parse_expression(expr, schema)?));
                                    } else {
                                        return Err(
                                            "Invalid FROM argument for SUBSTRING".to_string()
                                        );
                                    }
                                }
                                "FOR" => {
                                    if let FunctionArgExpr::Expr(expr) = arg {
                                        for_expr =
                                            Some(Arc::new(self.parse_expression(expr, schema)?));
                                    } else {
                                        return Err(
                                            "Invalid FOR argument for SUBSTRING".to_string()
                                        );
                                    }
                                }
                                _ => {
                                    return Err(format!(
                                        "Unknown named argument '{}' for SUBSTRING",
                                        name.value
                                    ))
                                }
                            }
                        }
                        _ => return Err("Invalid argument for SUBSTRING".to_string()),
                    }
                }

                // Ensure we have a FROM expression
                let from_expr =
                    from_expr.ok_or_else(|| "SUBSTRING requires a start position".to_string())?;

                // Create the SUBSTRING expression
                Ok(Expression::Substring(SubstringExpression::new(
                    string_expr,
                    from_expr,
                    for_expr,
                )))
            }
            _ => Err("SUBSTRING requires arguments".to_string()),
        }
    }

    fn parse_trim_function(&self, func: &Function, schema: &Schema) -> Result<Expression, String> {
        let func_name = func.name.to_string().to_uppercase();

        match &func.args {
            FunctionArguments::List(arg_list) => {
                let args = &arg_list.args;

                // Check if we have at least one argument
                if args.is_empty() {
                    return Err("TRIM requires at least one argument".to_string());
                }

                // Default trim type based on function name
                let mut trim_type = match func_name.as_str() {
                    "TRIM" => TrimType::Both,
                    "LTRIM" => TrimType::Leading,
                    "RTRIM" => TrimType::Trailing,
                    _ => return Err(format!("Unsupported trim function: {}", func_name)),
                };

                // Check for SQL standard syntax: TRIM([BOTH|LEADING|TRAILING] [chars FROM] string)
                if func_name == "TRIM" && args.len() >= 1 {
                    // Check if the first argument is a named argument specifying the trim type
                    if let FunctionArg::Named { name, arg, .. } = &args[0] {
                        match name.value.to_uppercase().as_str() {
                            "BOTH" => {
                                trim_type = TrimType::Both;

                                // The next argument should be either the string or "FROM string"
                                if args.len() < 2 {
                                    return Err("TRIM BOTH requires a string argument".to_string());
                                }

                                // Parse the string expression
                                let string_expr = match &args[1] {
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                                        Arc::new(self.parse_expression(expr, schema)?)
                                    }
                                    FunctionArg::Named { name, arg, .. }
                                        if name.value.to_uppercase() == "FROM" =>
                                    {
                                        if let FunctionArgExpr::Expr(expr) = arg {
                                            Arc::new(self.parse_expression(expr, schema)?)
                                        } else {
                                            return Err(
                                                "Invalid FROM argument for TRIM".to_string()
                                            );
                                        }
                                    }
                                    _ => {
                                        return Err(
                                            "Invalid argument after BOTH for TRIM".to_string()
                                        )
                                    }
                                };

                                // Create the TRIM expression with default whitespace characters
                                let return_type = Column::new("trim_result", TypeId::VarChar);
                                return Ok(Expression::Trim(TrimExpression::new(
                                    trim_type,
                                    vec![string_expr],
                                    return_type,
                                )));
                            }
                            "LEADING" => {
                                trim_type = TrimType::Leading;

                                // The next argument should be either the string or "FROM string"
                                if args.len() < 2 {
                                    return Err(
                                        "TRIM LEADING requires a string argument".to_string()
                                    );
                                }

                                // Parse the string expression
                                let string_expr = match &args[1] {
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                                        Arc::new(self.parse_expression(expr, schema)?)
                                    }
                                    FunctionArg::Named { name, arg, .. }
                                        if name.value.to_uppercase() == "FROM" =>
                                    {
                                        if let FunctionArgExpr::Expr(expr) = arg {
                                            Arc::new(self.parse_expression(expr, schema)?)
                                        } else {
                                            return Err(
                                                "Invalid FROM argument for TRIM".to_string()
                                            );
                                        }
                                    }
                                    _ => {
                                        return Err(
                                            "Invalid argument after LEADING for TRIM".to_string()
                                        )
                                    }
                                };

                                // Create the TRIM expression with default whitespace characters
                                let return_type = Column::new("trim_result", TypeId::VarChar);
                                return Ok(Expression::Trim(TrimExpression::new(
                                    trim_type,
                                    vec![string_expr],
                                    return_type,
                                )));
                            }
                            "TRAILING" => {
                                trim_type = TrimType::Trailing;

                                // The next argument should be either the string or "FROM string"
                                if args.len() < 2 {
                                    return Err(
                                        "TRIM TRAILING requires a string argument".to_string()
                                    );
                                }

                                // Parse the string expression
                                let string_expr = match &args[1] {
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                                        Arc::new(self.parse_expression(expr, schema)?)
                                    }
                                    FunctionArg::Named { name, arg, .. }
                                        if name.value.to_uppercase() == "FROM" =>
                                    {
                                        if let FunctionArgExpr::Expr(expr) = arg {
                                            Arc::new(self.parse_expression(expr, schema)?)
                                        } else {
                                            return Err(
                                                "Invalid FROM argument for TRIM".to_string()
                                            );
                                        }
                                    }
                                    _ => {
                                        return Err(
                                            "Invalid argument after TRAILING for TRIM".to_string()
                                        )
                                    }
                                };

                                // Create the TRIM expression with default whitespace characters
                                let return_type = Column::new("trim_result", TypeId::VarChar);
                                return Ok(Expression::Trim(TrimExpression::new(
                                    trim_type,
                                    vec![string_expr],
                                    return_type,
                                )));
                            }
                            "FROM" => {
                                // This is the "TRIM(chars FROM string)" syntax
                                if let FunctionArgExpr::Expr(chars_expr) = arg {
                                    // Parse the characters to trim
                                    let chars =
                                        Arc::new(self.parse_expression(chars_expr, schema)?);

                                    // The next argument should be the string
                                    if args.len() < 2 {
                                        return Err(
                                            "TRIM FROM requires a string argument".to_string()
                                        );
                                    }

                                    // Parse the string expression
                                    let string_expr = match &args[1] {
                                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                                            Arc::new(self.parse_expression(expr, schema)?)
                                        }
                                        _ => {
                                            return Err(
                                                "Invalid string argument for TRIM FROM".to_string()
                                            )
                                        }
                                    };

                                    // Create the TRIM expression with specified characters
                                    let return_type = Column::new("trim_result", TypeId::VarChar);
                                    return Ok(Expression::Trim(TrimExpression::new(
                                        trim_type,
                                        vec![string_expr, chars],
                                        return_type,
                                    )));
                                } else {
                                    return Err("Invalid FROM argument for TRIM".to_string());
                                }
                            }
                            _ => return Err(format!("Unsupported TRIM option: {}", name.value)),
                        }
                    }
                }

                // Simple syntax: TRIM(string [, chars])
                // Parse the string expression (first argument)
                let string_expr = match &args[0] {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                        Arc::new(self.parse_expression(expr, schema)?)
                    }
                    _ => return Err("Invalid first argument for TRIM".to_string()),
                };

                // Check if we have a second argument for characters to trim
                let mut children = vec![string_expr];

                if args.len() > 1 {
                    // Parse the characters to trim
                    let chars_expr = match &args[1] {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                            Arc::new(self.parse_expression(expr, schema)?)
                        }
                        _ => return Err("Invalid second argument for TRIM".to_string()),
                    };

                    children.push(chars_expr);
                }

                // Create the return type (always a string)
                let return_type = Column::new("trim_result", TypeId::VarChar);

                // Create the TRIM expression
                Ok(Expression::Trim(TrimExpression::new(
                    trim_type,
                    children,
                    return_type,
                )))
            }
            _ => Err("TRIM requires arguments".to_string()),
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
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        let parser = ExpressionParser::new(catalog);
        let schema = Schema::new(vec![]);

        // Create test expressions
        let timestamp = Expr::Value(SQLValue::Timestamp("2024-01-01 12:00:00".to_string()));
        let timezone = Expr::Value(SQLValue::SingleQuotedString("UTC".to_string()));

        let expr = Expr::AtTimeZone {
            timestamp: Box::new(timestamp),
            time_zone: Box::new(timezone),
        };

        // Parse expression
        let result = parser.parse_expression(&expr, &schema);
        assert!(result.is_ok());

        // Verify the parsed expression
        match result.unwrap() {
            Expression::AtTimeZone(at_tz) => {
                assert_eq!(at_tz.get_return_type().get_type(), TypeId::Timestamp);
            }
            _ => panic!("Expected AtTimeZone expression"),
        }
    }

    #[test]
    fn test_parse_at_timezone_invalid_types() {
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        let parser = ExpressionParser::new(catalog);
        let schema = Schema::new(vec![]);

        // Test with non-timestamp input
        let invalid_timestamp = Expr::Value(SQLValue::Number("123".to_string()));
        let timezone = Expr::Value(SQLValue::SingleQuotedString("UTC".to_string()));

        let expr = Expr::AtTimeZone {
            timestamp: Box::new(invalid_timestamp),
            time_zone: Box::new(timezone),
        };

        assert!(parser.parse_expression(&expr, &schema).is_err());

        // Test with non-string timezone
        let timestamp = Expr::Value(SQLValue::Timestamp("2024-01-01 12:00:00".to_string()));
        let invalid_timezone = Expr::Value(SQLValue::Number("123".to_string()));

        let expr = Expr::AtTimeZone {
            timestamp: Box::new(timestamp),
            time_zone: Box::new(invalid_timezone),
        };

        assert!(parser.parse_expression(&expr, &schema).is_err());
    }
}
