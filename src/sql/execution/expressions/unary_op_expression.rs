use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use sqlparser::ast::UnaryOperator;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct UnaryOpExpression {
    expr: Arc<Expression>,
    op: UnaryOperator,
    return_type: Column,
    children: Vec<Arc<Expression>>,
}

impl UnaryOpExpression {
    pub fn new(expr: Arc<Expression>, op: UnaryOperator) -> Result<Self, String> {
        let return_type = match op {
            UnaryOperator::Not => {
                let type_id = expr.get_return_type().get_type();
                if type_id != TypeId::Boolean && type_id != TypeId::Invalid {
                    return Err(format!(
                        "Cannot apply NOT operator to type {:?}, expected Boolean",
                        type_id
                    ));
                }
                Column::new("unary_not", TypeId::Boolean)
            }
            UnaryOperator::Plus | UnaryOperator::Minus => {
                let type_id = expr.get_return_type().get_type();
                if !matches!(
                    type_id,
                    TypeId::TinyInt
                        | TypeId::SmallInt
                        | TypeId::Integer
                        | TypeId::BigInt
                        | TypeId::Decimal
                        | TypeId::Invalid
                ) {
                    return Err(format!(
                        "Cannot apply unary operator {:?} to type {:?}",
                        op, type_id
                    ));
                }
                Column::new("unary_numeric", if type_id == TypeId::Invalid { TypeId::Integer } else { type_id })
            }
            _ => return Err(format!("Unsupported unary operator: {:?}", op)),
        };

        Ok(Self {
            expr: expr.clone(),
            op,
            return_type,
            children: vec![expr],
        })
    }
}

impl ExpressionOps for UnaryOpExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let val = self.expr.evaluate(tuple, schema)?;

        match self.op {
            UnaryOperator::Not => {
                if val.is_null() {
                    return Ok(Value::new(Val::Null));
                }
                match val.get_val() {
                    Val::Boolean(b) => Ok(Value::new(!b)),
                    _ => Err(ExpressionError::InvalidOperation(
                        "NOT operator can only be applied to boolean values".to_string(),
                    )),
                }
            }
            UnaryOperator::Plus => Ok(val), // Unary plus is a no-op
            UnaryOperator::Minus => {
                if val.is_null() {
                    return Ok(Value::new(Val::Null));
                }
                match val.get_val() {
                    Val::TinyInt(i) => Ok(Value::new(-i)),
                    Val::SmallInt(i) => Ok(Value::new(-i)),
                    Val::Integer(i) => Ok(Value::new(-i)),
                    Val::BigInt(i) => Ok(Value::new(-i)),
                    Val::Decimal(f) => Ok(Value::new(-f)),
                    _ => Err(ExpressionError::InvalidOperation(
                        "Unary minus can only be applied to numeric values".to_string(),
                    )),
                }
            }
            _ => Err(ExpressionError::InvalidOperation(format!(
                "Unsupported unary operator: {:?}",
                self.op
            ))),
        }
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        let val = self
            .expr
            .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;

        match self.op {
            UnaryOperator::Not => {
                if val.is_null() {
                    return Ok(Value::new(Val::Null));
                }
                match val.get_val() {
                    Val::Boolean(b) => Ok(Value::new(!b)),
                    _ => Err(ExpressionError::InvalidOperation(
                        "NOT operator can only be applied to boolean values".to_string(),
                    )),
                }
            }
            UnaryOperator::Plus => Ok(val),
            UnaryOperator::Minus => {
                if val.is_null() {
                    return Ok(Value::new(Val::Null));
                }
                match val.get_val() {
                    Val::TinyInt(i) => Ok(Value::new(-i)),
                    Val::SmallInt(i) => Ok(Value::new(-i)),
                    Val::Integer(i) => Ok(Value::new(-i)),
                    Val::BigInt(i) => Ok(Value::new(-i)),
                    Val::Decimal(f) => Ok(Value::new(-f)),
                    _ => Err(ExpressionError::InvalidOperation(
                        "Unary minus can only be applied to numeric values".to_string(),
                    )),
                }
            }
            _ => Err(ExpressionError::InvalidOperation(format!(
                "Unsupported unary operator: {:?}",
                self.op
            ))),
        }
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
        Arc::new(Expression::UnaryOp(UnaryOpExpression {
            expr: children[0].clone(),
            op: self.op.clone(),
            return_type: self.return_type.clone(),
            children,
        }))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        self.expr.validate(schema)?;

        match self.op {
            UnaryOperator::Not => {
                if self.expr.get_return_type().get_type() != TypeId::Boolean {
                    return Err(ExpressionError::InvalidOperation(
                        "NOT operator requires boolean operand".to_string(),
                    ));
                }
            }
            UnaryOperator::Plus | UnaryOperator::Minus => {
                let type_id = self.expr.get_return_type().get_type();
                if !matches!(
                    type_id,
                    TypeId::TinyInt
                        | TypeId::SmallInt
                        | TypeId::Integer
                        | TypeId::BigInt
                        | TypeId::Decimal
                ) {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "Numeric operator requires numeric operand, got {:?}",
                        type_id
                    )));
                }
            }
            _ => {
                return Err(ExpressionError::InvalidOperation(format!(
                    "Unsupported unary operator: {:?}",
                    self.op
                )))
            }
        }
        Ok(())
    }
}

impl Display for UnaryOpExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.op {
            UnaryOperator::Not => write!(f, "NOT {}", self.expr),
            UnaryOperator::Plus => write!(f, "+{}", self.expr),
            UnaryOperator::Minus => write!(f, "-{}", self.expr),
            _ => write!(f, "?{}", self.expr),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::value::Val;

    fn create_test_tuple() -> (Tuple, Schema) {
        let schema = Schema::new(vec![
            Column::new("bool_col", TypeId::Boolean),
            Column::new("int_col", TypeId::Integer),
            Column::new("decimal_col", TypeId::Decimal),
        ]);
        let values = vec![Value::new(true), Value::new(42), Value::new(3.14)];
        let tuple = Tuple::new(&values, schema.clone(), RID::new(0, 0));
        (tuple, schema)
    }

    #[test]
    fn test_not_operator() {
        let bool_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(true),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));

        let not_expr = UnaryOpExpression::new(bool_expr, UnaryOperator::Not).unwrap();
        let (tuple, schema) = create_test_tuple();

        let result = not_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(false));
    }

    #[test]
    fn test_minus_operator() {
        let num_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(42),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let minus_expr = UnaryOpExpression::new(num_expr, UnaryOperator::Minus).unwrap();
        let (tuple, schema) = create_test_tuple();

        let result = minus_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(-42));
    }

    #[test]
    fn test_plus_operator() {
        let num_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(42),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let plus_expr = UnaryOpExpression::new(num_expr, UnaryOperator::Plus).unwrap();
        let (tuple, schema) = create_test_tuple();

        let result = plus_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(42));
    }

    #[test]
    fn test_null_handling() {
        let null_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Null),
            Column::new("const", TypeId::Invalid),
            vec![],
        )));

        let not_expr = UnaryOpExpression::new(null_expr.clone(), UnaryOperator::Not).unwrap();
        let minus_expr = UnaryOpExpression::new(null_expr.clone(), UnaryOperator::Minus).unwrap();
        let (tuple, schema) = create_test_tuple();

        assert!(not_expr.evaluate(&tuple, &schema).unwrap().is_null());
        assert!(minus_expr.evaluate(&tuple, &schema).unwrap().is_null());
    }

    #[test]
    fn test_invalid_operations() {
        let bool_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(true),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));

        let num_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(42),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        // Test invalid NOT on numeric
        assert!(UnaryOpExpression::new(num_expr.clone(), UnaryOperator::Not).is_err());

        // Test invalid MINUS on boolean
        assert!(UnaryOpExpression::new(bool_expr.clone(), UnaryOperator::Minus).is_err());
    }

    #[test]
    fn test_validate() {
        let (_, schema) = create_test_tuple();

        let bool_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(true),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));

        let not_expr = UnaryOpExpression::new(bool_expr, UnaryOperator::Not).unwrap();
        assert!(not_expr.validate(&schema).is_ok());

        let num_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(42),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let minus_expr = UnaryOpExpression::new(num_expr, UnaryOperator::Minus).unwrap();
        assert!(minus_expr.validate(&schema).is_ok());
    }

    #[test]
    fn test_display() {
        let bool_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(true),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));

        let not_expr = UnaryOpExpression::new(bool_expr, UnaryOperator::Not).unwrap();
        assert_eq!(not_expr.to_string(), "NOT true");

        let num_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(42),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let minus_expr = UnaryOpExpression::new(num_expr.clone(), UnaryOperator::Minus).unwrap();
        let plus_expr = UnaryOpExpression::new(num_expr, UnaryOperator::Plus).unwrap();

        assert_eq!(minus_expr.to_string(), "-42");
        assert_eq!(plus_expr.to_string(), "+42");
    }
}
