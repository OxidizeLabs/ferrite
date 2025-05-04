use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::datetime_expression::DateTimeField;
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::{Val, Value};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq)]
pub enum CeilFloorOperation {
    Ceil,
    Floor,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CeilFloorExpression {
    operation: CeilFloorOperation,
    expr: Arc<Expression>,
    scale: Option<Arc<Expression>>,
    datetime_field: Option<DateTimeField>,
    children: Vec<Arc<Expression>>,
    return_type: Column,
}

impl CeilFloorExpression {
    pub fn new(
        operation: CeilFloorOperation,
        expr: Arc<Expression>,
        scale: Option<Arc<Expression>>,
        datetime_field: Option<DateTimeField>,
    ) -> Result<Self, String> {
        let mut children = vec![expr.clone()];
        if let Some(scale_expr) = scale.clone() {
            children.push(scale_expr);
        }

        // Determine return type based on input
        let return_type = match expr.get_return_type().get_type() {
            TypeId::Decimal => Column::new("ceil_floor_result", TypeId::Decimal),
            TypeId::Integer | TypeId::BigInt | TypeId::SmallInt | TypeId::TinyInt => {
                expr.get_return_type().clone()
            }
            TypeId::Timestamp if datetime_field.is_some() => {
                Column::new("ceil_floor_result", TypeId::Timestamp)
            }
            _ => return Err("Invalid input type for CEIL/FLOOR operation".to_string()),
        };

        Ok(Self {
            operation,
            expr,
            scale,
            datetime_field,
            children,
            return_type,
        })
    }
}

impl ExpressionOps for CeilFloorExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let input_val = self.expr.evaluate(tuple, schema)?;

        // Handle NULL input
        if input_val.is_null() {
            return Ok(Value::new(Val::Null));
        }

        match input_val.get_type_id() {
            TypeId::Decimal => {
                let num = input_val.as_decimal().unwrap();
                let scale = if let Some(scale_expr) = &self.scale {
                    let scale_val = scale_expr.evaluate(tuple, schema)?;
                    if scale_val.is_null() {
                        return Ok(Value::new(Val::Null));
                    }
                    scale_val.as_integer().unwrap()
                } else {
                    0
                };

                let multiplier = 10f64.powi(scale);
                let scaled_num = num * multiplier;

                let result = match self.operation {
                    CeilFloorOperation::Ceil => scaled_num.ceil(),
                    CeilFloorOperation::Floor => scaled_num.floor(),
                };

                Ok(Value::new_with_type(
                    Val::Decimal(result / multiplier),
                    TypeId::Decimal,
                ))
            }
            TypeId::Integer | TypeId::BigInt | TypeId::SmallInt | TypeId::TinyInt => {
                // For integer types, just return the input since they're already whole numbers
                Ok(input_val)
            }
            TypeId::Timestamp => {
                if let Some(field) = &self.datetime_field {
                    let timestamp = input_val.get_val();
                    if let Val::Timestamp(ts) = timestamp {
                        let datetime = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(*ts);
                        let secs = datetime.duration_since(UNIX_EPOCH).unwrap().as_secs();

                        let result = match field {
                            DateTimeField::Second => {
                                // Already in seconds, no rounding needed
                                *ts
                            }
                            DateTimeField::Minute => {
                                let minutes = secs / 60;
                                match self.operation {
                                    CeilFloorOperation::Ceil => (minutes + 1) * 60,
                                    CeilFloorOperation::Floor => minutes * 60,
                                }
                            }
                            DateTimeField::Hour => {
                                let hours = secs / 3600;
                                match self.operation {
                                    CeilFloorOperation::Ceil => (hours + 1) * 3600,
                                    CeilFloorOperation::Floor => hours * 3600,
                                }
                            }
                            DateTimeField::Day => {
                                let days = secs / 86400;
                                match self.operation {
                                    CeilFloorOperation::Ceil => (days + 1) * 86400,
                                    CeilFloorOperation::Floor => days * 86400,
                                }
                            }
                            DateTimeField::Month => {
                                let (year, month, _day, _hour, _min, _sec) = timestamp_to_date(*ts);
                                match self.operation {
                                    CeilFloorOperation::Floor => {
                                        // First day of current month
                                        date_to_timestamp(year, month, 1, 0, 0, 0)
                                    }
                                    CeilFloorOperation::Ceil => {
                                        if month == 12 {
                                            // First day of next year
                                            date_to_timestamp(year + 1, 1, 1, 0, 0, 0)
                                        } else {
                                            // First day of next month
                                            date_to_timestamp(year, month + 1, 1, 0, 0, 0)
                                        }
                                    }
                                }
                            }
                            DateTimeField::Year => {
                                let (year, _month, _day, _hour, _min, _sec) =
                                    timestamp_to_date(*ts);
                                match self.operation {
                                    CeilFloorOperation::Floor => {
                                        // First day of current year
                                        date_to_timestamp(year, 1, 1, 0, 0, 0)
                                    }
                                    CeilFloorOperation::Ceil => {
                                        // First day of next year
                                        date_to_timestamp(year + 1, 1, 1, 0, 0, 0)
                                    }
                                }
                            }
                            _ => {
                                return Err(ExpressionError::InvalidOperation(format!(
                                    "Unsupported DateTimeField: {:?}",
                                    field
                                )))
                            }
                        };

                        Ok(Value::new_with_type(
                            Val::Timestamp(result),
                            TypeId::Timestamp,
                        ))
                    } else {
                        Err(ExpressionError::InvalidOperation(
                            "Expected timestamp value".to_string(),
                        ))
                    }
                } else {
                    Err(ExpressionError::InvalidOperation(
                        "DateTime CEIL/FLOOR requires a field specification".to_string(),
                    ))
                }
            }
            _ => Err(ExpressionError::InvalidOperation(format!(
                "Cannot perform CEIL/FLOOR on type {:?}",
                input_val.get_type_id()
            ))),
        }
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        _right_tuple: &Tuple,
        _right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        // For join evaluation, we'll just evaluate against the left tuple and schema
        self.evaluate(left_tuple, left_schema)
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        &self.children[child_idx]
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        let expr = children[0].clone();
        let scale = children.get(1).cloned();

        Arc::new(Expression::CeilFloor(
            CeilFloorExpression::new(
                self.operation.clone(),
                expr,
                scale,
                self.datetime_field.clone(),
            )
            .unwrap(),
        ))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate the input expression
        self.expr.validate(schema)?;

        // Validate scale expression if present
        if let Some(scale_expr) = &self.scale {
            scale_expr.validate(schema)?;

            // Scale must return an integer type
            match scale_expr.get_return_type().get_type() {
                TypeId::Integer | TypeId::SmallInt | TypeId::TinyInt => (),
                _ => {
                    return Err(ExpressionError::InvalidOperation(
                        "Scale parameter must be an integer type".to_string(),
                    ))
                }
            }
        }

        // Validate input type
        match self.expr.get_return_type().get_type() {
            TypeId::Decimal
            | TypeId::Integer
            | TypeId::BigInt
            | TypeId::SmallInt
            | TypeId::TinyInt => Ok(()),
            TypeId::Timestamp if self.datetime_field.is_some() => Ok(()),
            _ => Err(ExpressionError::InvalidOperation(
                "Invalid input type for CEIL/FLOOR operation".to_string(),
            )),
        }
    }
}

impl Display for CeilFloorExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let op_name = match self.operation {
            CeilFloorOperation::Ceil => "CEIL",
            CeilFloorOperation::Floor => "FLOOR",
        };

        write!(f, "{}(", op_name)?;
        write!(f, "{}", self.expr)?;

        if let Some(scale) = &self.scale {
            write!(f, ", {}", scale)?;
        }

        if let Some(field) = &self.datetime_field {
            write!(f, " TO {:?}", field)?;
        }

        write!(f, ")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;

    fn create_decimal_expr(value: f64) -> Arc<Expression> {
        Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(value),
            Column::new("test", TypeId::Decimal),
            vec![],
        )))
    }

    fn create_scale_expr(scale: i32) -> Arc<Expression> {
        Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(scale),
            Column::new("scale", TypeId::Integer),
            vec![],
        )))
    }

    #[test]
    fn test_ceil_floor_basic() -> Result<(), ExpressionError> {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], &schema, crate::common::rid::RID::new(0, 0));

        // Test ceiling
        let ceil_expr = CeilFloorExpression::new(
            CeilFloorOperation::Ceil,
            create_decimal_expr(3.14),
            None,
            None,
        )
        .unwrap();
        let result = ceil_expr.evaluate(&tuple, &schema)?;
        assert_eq!(result.as_decimal().unwrap(), 4.0);

        // Test floor
        let floor_expr = CeilFloorExpression::new(
            CeilFloorOperation::Floor,
            create_decimal_expr(3.14),
            None,
            None,
        )
        .unwrap();
        let result = floor_expr.evaluate(&tuple, &schema)?;
        assert_eq!(result.as_decimal().unwrap(), 3.0);

        Ok(())
    }

    #[test]
    fn test_ceil_floor_with_scale() -> Result<(), ExpressionError> {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], &schema, crate::common::rid::RID::new(0, 0));

        // Test ceiling with scale 1
        let ceil_expr = CeilFloorExpression::new(
            CeilFloorOperation::Ceil,
            create_decimal_expr(3.14),
            Some(create_scale_expr(1)),
            None,
        )
        .unwrap();
        let result = ceil_expr.evaluate(&tuple, &schema)?;
        assert_eq!(result.as_decimal().unwrap(), 3.2);

        // Test floor with scale 1
        let floor_expr = CeilFloorExpression::new(
            CeilFloorOperation::Floor,
            create_decimal_expr(3.14),
            Some(create_scale_expr(1)),
            None,
        )
        .unwrap();
        let result = floor_expr.evaluate(&tuple, &schema)?;
        assert_eq!(result.as_decimal().unwrap(), 3.1);

        Ok(())
    }

    #[test]
    fn test_ceil_floor_edge_cases() -> Result<(), ExpressionError> {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], &schema, crate::common::rid::RID::new(0, 0));

        // Test with integer input
        let ceil_expr = CeilFloorExpression::new(
            CeilFloorOperation::Ceil,
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(42),
                Column::new("test", TypeId::Integer),
                vec![],
            ))),
            None,
            None,
        )
        .unwrap();
        let result = ceil_expr.evaluate(&tuple, &schema)?;
        assert_eq!(result.as_integer().unwrap(), 42);

        // Test with NULL input
        let ceil_expr = CeilFloorExpression::new(
            CeilFloorOperation::Ceil,
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(Val::Null),
                Column::new("test", TypeId::Decimal),
                vec![],
            ))),
            None,
            None,
        )
        .unwrap();
        let result = ceil_expr.evaluate(&tuple, &schema)?;
        assert!(result.is_null());

        Ok(())
    }

    #[test]
    fn test_ceil_floor_validation() {
        let schema = Schema::new(vec![]);

        // Test invalid input type
        let result = CeilFloorExpression::new(
            CeilFloorOperation::Ceil,
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new("not a number"),
                Column::new("test", TypeId::VarChar),
                vec![],
            ))),
            None,
            None,
        );
        assert!(result.is_err());

        // Test invalid scale type
        let expr = CeilFloorExpression::new(
            CeilFloorOperation::Ceil,
            create_decimal_expr(3.14),
            Some(Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new("not a number"),
                Column::new("scale", TypeId::VarChar),
                vec![],
            )))),
            None,
        )
        .unwrap();
        assert!(expr.validate(&schema).is_err());
    }
}

// Add these helper functions at the module level
fn is_leap_year(year: i32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn days_in_month(year: i32, month: i32) -> i32 {
    match month {
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 31,
    }
}

fn timestamp_to_date(timestamp: u64) -> (i32, i32, i32, i32, i32, i32) {
    let secs = timestamp as i32;
    let mut days = secs / 86400;
    let mut year = 1970;
    let mut month = 1;

    // Calculate year
    while days >= (if is_leap_year(year) { 366 } else { 365 }) {
        days -= if is_leap_year(year) { 366 } else { 365 };
        year += 1;
    }

    // Calculate month
    while days >= days_in_month(year, month) {
        days -= days_in_month(year, month);
        month += 1;
    }

    // Calculate remaining time
    let day = days + 1;
    let secs_of_day = secs % 86400;
    let hour = secs_of_day / 3600;
    let min = (secs_of_day % 3600) / 60;
    let sec = secs_of_day % 60;

    (year, month, day, hour, min, sec)
}

fn date_to_timestamp(year: i32, month: i32, day: i32, hour: i32, min: i32, sec: i32) -> u64 {
    let mut timestamp = 0u64;

    // Add years
    for y in 1970..year {
        timestamp += if is_leap_year(y) { 366 } else { 365 } * 86400;
    }

    // Add months
    for m in 1..month {
        timestamp += days_in_month(year, m) as u64 * 86400;
    }

    // Add days and time
    timestamp += ((day - 1) as u64 * 86400) + (hour as u64 * 3600) + (min as u64 * 60) + sec as u64;

    timestamp
}
