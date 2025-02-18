use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::common::rid::RID;

#[derive(Debug, Clone, PartialEq)]
pub struct CastExpression {
    expr: Arc<Expression>,
    target_type: TypeId,
    ret_type: Column,
    children: Vec<Arc<Expression>>,
}

impl CastExpression {
    pub fn new(expr: Arc<Expression>, target_type: TypeId) -> Self {
        let ret_type = Column::new("<cast>", target_type);
        Self {
            expr: expr.clone(),
            target_type,
            ret_type,
            children: vec![expr.clone()],
        }
    }
}

impl ExpressionOps for CastExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let value = self.expr.evaluate(tuple, schema)?;
        value.cast_to(self.target_type)
            .map_err(|e| ExpressionError::CastError(e.to_string()))
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        let value = self.expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        value.cast_to(self.target_type)
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
        if f.alternate() {
            write!(f, "CAST({:#} AS {:?})", self.expr, self.target_type)
        } else {
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
        
        // All other conversions are invalid
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
                Value::new(42),                // Integer
                Value::new(3.14),              // Decimal
                Value::new("test"),            // VarChar
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
            0, 0,
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
        let (tuple, schema) = create_test_tuple();
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, 0,
            Column::new("decimal_col", TypeId::Decimal),
            vec![],
        )));

        let cast_expr = CastExpression::new(col_expr, TypeId::Integer);
        let result = cast_expr.evaluate(&tuple, &schema).unwrap();

        assert_eq!(result.get_type_id(), TypeId::Integer);
        match result.get_val() {
            Val::Integer(i) => assert_eq!(*i, 3), // Fixed: expecting 3 (truncated from 3.14)
            _ => panic!("Expected Integer value"),
        }
    }

    #[test]
    fn test_cast_integer_to_bigint() {
        let (tuple, schema) = create_test_tuple();
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0,
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
        let (tuple, schema) = create_test_tuple();
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            2, 0,
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
            2, 0,
            Column::new("varchar_col", TypeId::VarChar),
            vec![],
        )));

        let cast_expr = CastExpression::new(col_expr, TypeId::Integer);
        let result = cast_expr.evaluate(&tuple, &schema);

        assert!(result.is_err());
        match result {
            Err(ExpressionError::CastError(_)) => (),
            _ => panic!("Expected CastError"),
        }
    }

    #[test]
    fn test_cast_same_type() {
        let (tuple, schema) = create_test_tuple();
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0,
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
            Schema::new(vec![Column::new("null_col", TypeId::Invalid)]),
            RID::new(0, 0),
        );

        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0,
            Column::new("null_col", TypeId::Invalid),
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
            0, 0,
            Column::new("int_col", TypeId::Integer),
            vec![],
        )));

        let cast_expr = CastExpression::new(col_expr, TypeId::Decimal);
        
        assert_eq!(
            format!("{}", cast_expr),
            "CAST(Col#0.0 AS Decimal)"
        );
        assert_eq!(
            format!("{:#}", cast_expr),
            "CAST(Col#0.0 AS Decimal)"
        );
    }

    #[test]
    fn test_cast_expression_validation() {
        let schema = create_test_schema();
        
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0,
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