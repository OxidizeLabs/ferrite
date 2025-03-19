use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Val;
use crate::types_db::value::Value;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct CastExpression {
    expr: Arc<Expression>,
    target_type: TypeId,
    ret_type: Column,
    format: Option<String>,
    children: Vec<Arc<Expression>>,
}

impl CastExpression {
    pub fn new(expr: Arc<Expression>, target_type: TypeId) -> Self {
        // For Char type, we need to specify a size using new_varlen
        let ret_type = match target_type {
            TypeId::Char => Column::new_varlen("<cast>", target_type, 255), // Default size for CHAR
            _ => Column::new("<cast>", target_type),
        };

        Self {
            expr: expr.clone(),
            target_type,
            ret_type,
            format: None,
            children: vec![expr.clone()],
        }
    }

    pub fn with_format(mut self, format: String) -> Self {
        self.format = Some(format);
        self
    }

    fn apply_format(&self, value: Value) -> Result<Value, ExpressionError> {
        if let Some(_format_str) = &self.format {
            match (value.get_type_id(), self.target_type) {
                (TypeId::Timestamp, TypeId::VarChar) => {
                    if let Val::Timestamp(ts) = value.get_val() {
                        Ok(Value::new(format!("{}", ts)))
                    } else {
                        Err(ExpressionError::CastError(
                            "Invalid timestamp value".to_string(),
                        ))
                    }
                }
                (TypeId::Integer | TypeId::Decimal | TypeId::BigInt, TypeId::VarChar) => {
                    Ok(Value::new(value.to_string()))
                }
                _ => Ok(value),
            }
        } else {
            Ok(value)
        }
    }
}

impl ExpressionOps for CastExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let value = self.expr.evaluate(tuple, schema)?;

        // Handle NULL values first - return NULL of target type
        if value.is_null() {
            return Ok(Value::new_with_type(Val::Null, self.target_type));
        }

        let cast_result = match (value.get_type_id(), self.target_type) {
            // Decimal to Integer casting
            (TypeId::Decimal, TypeId::Integer) => {
                if let Val::Decimal(d) = value.get_val() {
                    Ok(Value::new_with_type(
                        Val::Integer(d.round() as i32),
                        TypeId::Integer,
                    ))
                } else {
                    Err(ExpressionError::CastError(
                        "Invalid value for decimal to integer cast".to_string(),
                    ))
                }
            }

            // Integer to Decimal casting
            (TypeId::Integer, TypeId::Decimal) => {
                if let Val::Integer(i) = value.get_val() {
                    Ok(Value::new_with_type(
                        Val::Decimal(*i as f64),
                        TypeId::Decimal,
                    ))
                } else {
                    Err(ExpressionError::CastError(
                        "Invalid value for integer to decimal cast".to_string(),
                    ))
                }
            }

            // Integer to BigInt casting
            (TypeId::Integer, TypeId::BigInt) => {
                if let Val::Integer(i) = value.get_val() {
                    Ok(Value::new_with_type(Val::BigInt(*i as i64), TypeId::BigInt))
                } else {
                    Err(ExpressionError::CastError(
                        "Invalid value for integer to bigint cast".to_string(),
                    ))
                }
            }

            // VarChar to Char casting
            (TypeId::VarChar, TypeId::Char) => {
                if let Val::VarLen(s) = value.get_val() {
                    Ok(Value::new_with_type(Val::ConstLen(s.clone()), TypeId::Char))
                } else {
                    Err(ExpressionError::CastError(
                        "Invalid value for varchar to char cast".to_string(),
                    ))
                }
            }

            // Char to VarChar casting
            (TypeId::Char, TypeId::VarChar) => {
                if let Val::ConstLen(s) = value.get_val() {
                    Ok(Value::new_with_type(
                        Val::VarLen(s.clone()),
                        TypeId::VarChar,
                    ))
                } else {
                    Err(ExpressionError::CastError(
                        "Invalid value for char to varchar cast".to_string(),
                    ))
                }
            }

            // Same type - return as is
            (source, target) if source == target => Ok(value),

            // Invalid cast
            (source, target) => Err(ExpressionError::InvalidCast {
                from: source,
                to: target,
            }),
        }?;

        self.apply_format(cast_result)
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        let value = self
            .expr
            .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        value
            .cast_to(self.target_type)
            .map_err(|e| ExpressionError::CastError(e.to_string()))
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        &self.children[child_idx]
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.ret_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        if children.len() != 1 {
            panic!("CastExpression requires exactly one child");
        }
        Arc::new(Expression::Cast(CastExpression::new(
            children[0].clone(),
            self.target_type,
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate the inner expression
        self.expr.validate(schema)?;

        // Check if the cast is valid
        let from_type = self.expr.get_return_type().get_type();
        if !is_valid_cast(from_type, self.target_type) {
            return Err(ExpressionError::InvalidCast {
                from: from_type,
                to: self.target_type,
            });
        }
        Ok(())
    }
}

impl Display for CastExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // When alternate flag (#) is set, use the detailed format
        if f.alternate() {
            write!(f, "CAST({:#} AS {:?})", self.expr, self.target_type)
        } else {
            // For normal display, use the basic format
            write!(f, "CAST({} AS {:?})", self.expr, self.target_type)
        }
    }
}

fn is_valid_cast(from: TypeId, to: TypeId) -> bool {
    use TypeId::*;
    match (from, to) {
        // Numeric conversions
        (Integer, Decimal) | (Decimal, Integer) => true,
        (Integer, BigInt) | (BigInt, Integer) => true,
        (BigInt, Decimal) | (Decimal, BigInt) => true,

        // String conversions
        (VarChar, Char) | (Char, VarChar) => true,

        // Same type is always valid
        (a, b) if a == b => true,

        // NULL can be cast to any type
        (Invalid, _) => true,

        // String to numeric conversions are not allowed
        (VarChar | Char, Integer | BigInt | Decimal) => false,

        // All other conversions are invalid
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::logger::initialize_logger;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::storage::table::tuple::Tuple;
    use crate::types_db::value::Val;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("int_col", TypeId::Integer),
            Column::new("decimal_col", TypeId::Decimal),
            Column::new("varchar_col", TypeId::VarChar),
            Column::new("bigint_col", TypeId::BigInt),
        ])
    }

    fn create_test_tuple() -> (Tuple, Schema) {
        let schema = create_test_schema();
        let tuple = Tuple::new(
            &vec![
                Value::new(42),                     // Integer
                Value::new(3.14),                   // Decimal
                Value::new("test"),                 // VarChar
                Value::new(9223372036854775807i64), // BigInt (i64::MAX)
            ],
            schema.clone(),
            RID::new(0, 0),
        );
        (tuple, schema)
    }

    #[test]
    fn test_cast_integer_to_decimal() {
        let (tuple, schema) = create_test_tuple();
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("int_col", TypeId::Integer),
            vec![],
        )));

        let cast_expr = CastExpression::new(col_expr, TypeId::Decimal);
        let result = cast_expr.evaluate(&tuple, &schema).unwrap();

        assert_eq!(result.get_type_id(), TypeId::Decimal);
        match result.get_val() {
            Val::Decimal(d) => assert_eq!(*d, 42.0),
            _ => panic!("Expected Decimal value"),
        }
    }

    #[test]
    fn test_cast_decimal_to_integer() {
        initialize_logger();
        let (tuple, schema) = create_test_tuple();
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1, // Fixed: Using tuple_index 0 and column_index 1 to get the decimal value (3.14)
            Column::new("decimal_col", TypeId::Decimal),
            vec![],
        )));

        let cast_expr = CastExpression::new(col_expr, TypeId::Integer);
        let result = cast_expr.evaluate(&tuple, &schema).unwrap();

        assert_eq!(result.get_type_id(), TypeId::Integer);
        match result.get_val() {
            Val::Integer(i) => assert_eq!(*i, 3), // Expecting 3 (rounded from 3.14)
            _ => panic!("Expected Integer value"),
        }
    }

    #[test]
    fn test_cast_integer_to_bigint() {
        let (tuple, schema) = create_test_tuple();
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("int_col", TypeId::Integer),
            vec![],
        )));

        let cast_expr = CastExpression::new(col_expr, TypeId::BigInt);
        let result = cast_expr.evaluate(&tuple, &schema).unwrap();

        assert_eq!(result.get_type_id(), TypeId::BigInt);
        match result.get_val() {
            Val::BigInt(i) => assert_eq!(*i, 42i64),
            _ => panic!("Expected BigInt value"),
        }
    }

    #[test]
    fn test_cast_varchar_to_char() {
        initialize_logger();
        let (tuple, schema) = create_test_tuple();
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2, // Fixed: Using tuple_index 0 and column_index 2 to get the varchar value "test"
            Column::new("varchar_col", TypeId::VarChar),
            vec![],
        )));

        let cast_expr = CastExpression::new(col_expr, TypeId::Char);
        let result = cast_expr.evaluate(&tuple, &schema).unwrap();

        assert_eq!(result.get_type_id(), TypeId::Char);
        match result.get_val() {
            Val::ConstLen(s) => assert_eq!(s, "test"),
            _ => panic!("Expected Char value"),
        }
    }

    #[test]
    fn test_invalid_cast() {
        let (tuple, schema) = create_test_tuple();
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2, // Fixed: Using tuple_index 0 and column_index 2 to get the varchar value
            Column::new("varchar_col", TypeId::VarChar),
            vec![],
        )));

        let cast_expr = CastExpression::new(col_expr, TypeId::Integer);
        let result = cast_expr.evaluate(&tuple, &schema);

        assert!(result.is_err());
        match result {
            Err(ExpressionError::InvalidCast { .. }) => (), // Fixed: expecting InvalidCast error
            _ => panic!("Expected InvalidCast error"),
        }
    }

    #[test]
    fn test_cast_same_type() {
        let (tuple, schema) = create_test_tuple();
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("int_col", TypeId::Integer),
            vec![],
        )));

        let cast_expr = CastExpression::new(col_expr, TypeId::Integer);
        let result = cast_expr.evaluate(&tuple, &schema).unwrap();

        assert_eq!(result.get_type_id(), TypeId::Integer);
        match result.get_val() {
            Val::Integer(i) => assert_eq!(*i, 42),
            _ => panic!("Expected Integer value"),
        }
    }

    #[test]
    fn test_cast_null_value() {
        let schema = create_test_schema();
        let tuple = Tuple::new(
            &vec![Value::new(Val::Null)],
            Schema::new(vec![
                // Use Integer type for the column, even though we'll store NULL in it
                Column::new("null_col", TypeId::Integer),
            ]),
            RID::new(0, 0),
        );

        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            // Use Integer type for the column definition
            Column::new("null_col", TypeId::Integer),
            vec![],
        )));

        let cast_expr = CastExpression::new(col_expr, TypeId::Integer);
        let result = cast_expr.evaluate(&tuple, &schema).unwrap();

        assert!(result.is_null());
        assert_eq!(result.get_type_id(), TypeId::Integer);
    }

    #[test]
    fn test_cast_expression_display() {
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("int_col", TypeId::Integer),
            vec![],
        )));

        let cast_expr = CastExpression::new(col_expr, TypeId::Decimal);

        // Test normal display format
        assert_eq!(format!("{}", cast_expr), "CAST(int_col AS Decimal)");

        // Test alternate display format
        assert_eq!(format!("{:#}", cast_expr), "CAST(Col#0.0 AS Decimal)");
    }

    #[test]
    fn test_cast_expression_validation() {
        let schema = create_test_schema();

        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("int_col", TypeId::Integer),
            vec![],
        )));

        // Valid cast
        let valid_cast = CastExpression::new(col_expr.clone(), TypeId::Decimal);
        assert!(valid_cast.validate(&schema).is_ok());

        // Invalid cast
        let invalid_cast = CastExpression::new(col_expr.clone(), TypeId::Boolean);
        assert!(matches!(
            invalid_cast.validate(&schema),
            Err(ExpressionError::InvalidCast { .. })
        ));
    }
}
