use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::{Val, Value};
use sqlparser::ast::BinaryOperator;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct BinaryOpExpression {
    left: Arc<Expression>,
    right: Arc<Expression>,
    op: BinaryOperator,
    return_type: Column,
    children: Vec<Arc<Expression>>,
}

impl BinaryOpExpression {
    pub fn new(
        left: Arc<Expression>,
        right: Arc<Expression>,
        op: BinaryOperator,
        children: Vec<Arc<Expression>>,
    ) -> Result<Self, String> {
        let return_type = Self::infer_return_type(&left, &right, &op)?;

        Ok(Self {
            left,
            right,
            op,
            return_type,
            children,
        })
    }

    pub fn get_left(&self) -> &Arc<Expression> {
        &self.left
    }

    pub fn get_right(&self) -> &Arc<Expression> {
        &self.right
    }

    pub fn get_op(&self) -> &BinaryOperator {
        &self.op
    }

    fn infer_return_type(
        left: &Expression,
        right: &Expression,
        op: &BinaryOperator,
    ) -> Result<Column, String> {
        let left_type = left.get_return_type().get_type();
        let right_type = right.get_return_type().get_type();

        match op {
            // Comparison operators always return boolean
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq => Ok(Column::new("comparison_result", TypeId::Boolean)),

            // Logical operators always return boolean
            BinaryOperator::And | BinaryOperator::Or => {
                if left_type != TypeId::Boolean || right_type != TypeId::Boolean {
                    return Err("Logical operators require boolean operands".to_string());
                }
                Ok(Column::new("logical_result", TypeId::Boolean))
            }

            // Arithmetic operators
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide => match (left_type, right_type) {
                (TypeId::Integer, TypeId::Integer) => {
                    Ok(Column::new("arithmetic_result", TypeId::Integer))
                }
                (TypeId::Decimal, TypeId::Decimal)
                | (TypeId::Integer, TypeId::Decimal)
                | (TypeId::Decimal, TypeId::Integer) => {
                    Ok(Column::new("arithmetic_result", TypeId::Decimal))
                }
                (TypeId::BigInt, TypeId::BigInt) => {
                    Ok(Column::new("arithmetic_result", TypeId::BigInt))
                }
                (TypeId::BigInt, TypeId::Decimal) | (TypeId::Decimal, TypeId::BigInt) => {
                    Ok(Column::new("arithmetic_result", TypeId::Decimal))
                }
                _ => Err(format!(
                    "Invalid types for arithmetic operation: {:?} and {:?}",
                    left_type, right_type
                )),
            },

            // String concatenation
            BinaryOperator::StringConcat => {
                if left_type != TypeId::VarChar || right_type != TypeId::VarChar {
                    return Err("String concatenation requires string operands".to_string());
                }
                Ok(Column::new("concat_result", TypeId::VarChar))
            }

            _ => Err(format!("Unsupported binary operator: {:?}", op)),
        }
    }

    fn evaluate_arithmetic(
        &self,
        left_val: &Value,
        right_val: &Value,
    ) -> Result<Value, ExpressionError> {
        match self.op {
            BinaryOperator::Plus => left_val
                .add(right_val)
                .map_err(|e| ExpressionError::InvalidOperation(e)),
            BinaryOperator::Minus => left_val
                .subtract(right_val)
                .map_err(|e| ExpressionError::InvalidOperation(e)),
            BinaryOperator::Multiply => left_val
                .multiply(right_val)
                .map_err(|e| ExpressionError::InvalidOperation(e)),
            BinaryOperator::Divide => left_val
                .divide(right_val)
                .map_err(|e| ExpressionError::InvalidOperation(e)),
            _ => Err(ExpressionError::InvalidOperation(format!(
                "Invalid arithmetic operator: {:?}",
                self.op
            ))),
        }
    }

    fn evaluate_comparison(
        &self,
        left_val: &Value,
        right_val: &Value,
    ) -> Result<Value, ExpressionError> {
        let result = match self.op {
            BinaryOperator::Eq => left_val.compare_equals(right_val),
            BinaryOperator::NotEq => left_val.compare_not_equals(right_val),
            BinaryOperator::Gt => left_val.compare_greater_than(right_val),
            BinaryOperator::Lt => left_val.compare_less_than(right_val),
            BinaryOperator::GtEq => left_val.compare_greater_than_equals(right_val),
            BinaryOperator::LtEq => left_val.compare_less_than_equals(right_val),
            _ => {
                return Err(ExpressionError::InvalidOperation(format!(
                    "Invalid comparison operator: {:?}",
                    self.op
                )))
            }
        };
        Ok(Value::from(result))
    }

    fn evaluate_logical(
        &self,
        left_val: &Value,
        right_val: &Value,
    ) -> Result<Value, ExpressionError> {
        match (left_val.get_val(), right_val.get_val()) {
            (Val::Boolean(l), Val::Boolean(r)) => {
                let result = match self.op {
                    BinaryOperator::And => *l && *r,
                    BinaryOperator::Or => *l || *r,
                    _ => {
                        return Err(ExpressionError::InvalidOperation(format!(
                            "Invalid logical operator: {:?}",
                            self.op
                        )))
                    }
                };
                Ok(Value::from(result))
            }
            _ => Err(ExpressionError::InvalidOperation(
                "Logical operations require boolean operands".to_string(),
            )),
        }
    }
}

impl ExpressionOps for BinaryOpExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let left_val = self.left.evaluate(tuple, schema)?;
        let right_val = self.right.evaluate(tuple, schema)?;

        match self.op {
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide => self.evaluate_arithmetic(&left_val, &right_val),

            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq => self.evaluate_comparison(&left_val, &right_val),

            BinaryOperator::And | BinaryOperator::Or => {
                self.evaluate_logical(&left_val, &right_val)
            }

            BinaryOperator::StringConcat => match (left_val.get_val(), right_val.get_val()) {
                (Val::VarLen(l), Val::VarLen(r)) => Ok(Value::from(format!("{}{}", l, r))),
                _ => Err(ExpressionError::InvalidOperation(
                    "String concatenation requires string operands".to_string(),
                )),
            },

            _ => Err(ExpressionError::InvalidOperation(format!(
                "Unsupported binary operator: {:?}",
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
        let left_val =
            self.left
                .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        let right_val =
            self.right
                .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;

        // Create a schema that matches our values
        let temp_schema = Schema::new(vec![
            Column::new("left_val", left_val.get_type_id()),
            Column::new("right_val", right_val.get_type_id()),
        ]);

        self.evaluate(
            &Tuple::new(
                &[left_val, right_val],
                temp_schema.clone(),
                left_tuple.get_rid(),
            ),
            &temp_schema,
        )
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
        if children.len() != 2 {
            panic!("BinaryOpExpression requires exactly two children");
        }

        Arc::new(Expression::BinaryOp(
            BinaryOpExpression::new(
                children[0].clone(),
                children[1].clone(),
                self.op.clone(),
                children,
            )
            .unwrap(),
        ))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate children
        self.left.validate(schema)?;
        self.right.validate(schema)?;

        // Validate operator and types
        let left_type = self.left.get_return_type().get_type();
        let right_type = self.right.get_return_type().get_type();

        match self.op {
            BinaryOperator::And | BinaryOperator::Or => {
                if left_type != TypeId::Boolean || right_type != TypeId::Boolean {
                    return Err(ExpressionError::InvalidOperation(
                        "Logical operators require boolean operands".to_string(),
                    ));
                }
            }

            BinaryOperator::StringConcat => {
                if left_type != TypeId::VarChar || right_type != TypeId::VarChar {
                    return Err(ExpressionError::InvalidOperation(
                        "String concatenation requires string operands".to_string(),
                    ));
                }
            }

            _ => {
                // For other operators, just verify return type can be inferred
                Self::infer_return_type(&self.left, &self.right, &self.op)
                    .map_err(|e| ExpressionError::InvalidOperation(e))?;
            }
        }

        Ok(())
    }
}

impl Display for BinaryOpExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            write!(f, "({:#} {} {:#})", self.left, self.op, self.right)
        } else {
            write!(f, "({} {} {})", self.left, self.op, self.right)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("int_col", TypeId::Integer),
            Column::new("decimal_col", TypeId::Decimal),
            Column::new("bool_col", TypeId::Boolean),
            Column::new("string_col", TypeId::VarChar),
        ])
    }

    fn create_constant_expr(value: Value, type_id: TypeId) -> Arc<Expression> {
        Arc::new(Expression::Constant(ConstantExpression::new(
            value,
            Column::new("const", type_id),
            vec![],
        )))
    }

    #[test]
    fn test_arithmetic_operations() {
        let schema = create_test_schema();
        let tuple = Tuple::new(
            &[
                Value::new(0),         // int_col
                Value::new(0.0),       // decimal_col
                Value::new(false),     // bool_col
                Value::new(""),        // string_col
            ],
            schema.clone(),
            RID::new(0, 0)
        );

        // Test integer arithmetic
        let test_cases = vec![
            (5, 3, BinaryOperator::Plus, 8),
            (10, 4, BinaryOperator::Minus, 6),
            (6, 7, BinaryOperator::Multiply, 42),
            (15, 3, BinaryOperator::Divide, 5),
        ];

        for (left, right, op, expected) in test_cases {
            let left_expr = create_constant_expr(Value::new(left), TypeId::Integer);
            let right_expr = create_constant_expr(Value::new(right), TypeId::Integer);

            let binary_expr = BinaryOpExpression::new(
                left_expr,
                right_expr,
                op,
                vec![],
            ).unwrap();

            let result = binary_expr.evaluate(&tuple, &schema).unwrap();
            assert_eq!(result, Value::new(expected));
        }

        // Test decimal arithmetic
        let test_cases = vec![
            (5.5, 3.3, BinaryOperator::Plus, 8.8),
            (10.5, 4.2, BinaryOperator::Minus, 6.3),
            (6.5, 2.0, BinaryOperator::Multiply, 13.0),
            (15.0, 3.0, BinaryOperator::Divide, 5.0),
        ];

        for (left, right, op, expected) in test_cases {
            let left_expr = create_constant_expr(Value::new(left), TypeId::Decimal);
            let right_expr = create_constant_expr(Value::new(right), TypeId::Decimal);

            let binary_expr = BinaryOpExpression::new(
                left_expr,
                right_expr,
                op,
                vec![],
            ).unwrap();

            let result = binary_expr.evaluate(&tuple, &schema).unwrap();
            assert_eq!(result, Value::new(expected));
        }
    }

    #[test]
    fn test_comparison_operations() {
        let schema = create_test_schema();
        let tuple = Tuple::new(
            &[
                Value::new(0),         // int_col
                Value::new(0.0),       // decimal_col
                Value::new(false),     // bool_col
                Value::new(""),        // string_col
            ],
            schema.clone(),
            RID::new(0, 0)
        );

        let test_cases = vec![
            (5, 3, BinaryOperator::Gt, true),
            (3, 5, BinaryOperator::Lt, true),
            (5, 5, BinaryOperator::Eq, true),
            (5, 3, BinaryOperator::NotEq, true),
            (5, 5, BinaryOperator::GtEq, true),
            (3, 5, BinaryOperator::LtEq, true),
        ];

        for (left, right, op, expected) in test_cases {
            let left_expr = create_constant_expr(Value::new(left), TypeId::Integer);
            let right_expr = create_constant_expr(Value::new(right), TypeId::Integer);

            let binary_expr = BinaryOpExpression::new(
                left_expr,
                right_expr,
                op,
                vec![],
            ).unwrap();

            let result = binary_expr.evaluate(&tuple, &schema).unwrap();
            assert_eq!(result, Value::new(expected));
        }
    }

    #[test]
    fn test_logical_operations() {
        let schema = create_test_schema();
        let tuple = Tuple::new(
            &[
                Value::new(0),         // int_col
                Value::new(0.0),       // decimal_col
                Value::new(false),     // bool_col
                Value::new(""),        // string_col
            ],
            schema.clone(),
            RID::new(0, 0)
        );

        let test_cases = vec![
            (true, true, BinaryOperator::And, true),
            (true, false, BinaryOperator::And, false),
            (false, true, BinaryOperator::Or, true),
            (false, false, BinaryOperator::Or, false),
        ];

        for (left, right, op, expected) in test_cases {
            let left_expr = create_constant_expr(Value::new(left), TypeId::Boolean);
            let right_expr = create_constant_expr(Value::new(right), TypeId::Boolean);

            let binary_expr = BinaryOpExpression::new(
                left_expr,
                right_expr,
                op,
                vec![],
            ).unwrap();

            let result = binary_expr.evaluate(&tuple, &schema).unwrap();
            assert_eq!(result, Value::new(expected));
        }
    }

    #[test]
    fn test_string_concatenation() {
        let schema = create_test_schema();
        let tuple = Tuple::new(
            &[
                Value::new(0),         // int_col
                Value::new(0.0),       // decimal_col
                Value::new(false),     // bool_col
                Value::new(""),        // string_col
            ],
            schema.clone(),
            RID::new(0, 0)
        );

        let left_expr = create_constant_expr(Value::new("Hello"), TypeId::VarChar);
        let right_expr = create_constant_expr(Value::new("World"), TypeId::VarChar);

        let binary_expr = BinaryOpExpression::new(
            left_expr,
            right_expr,
            BinaryOperator::StringConcat,
            vec![],
        ).unwrap();

        let result = binary_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new("HelloWorld"));
    }

    #[test]
    fn test_type_validation() {
        // Test invalid type combinations
        let test_cases = vec![
            // Logical operations with non-boolean operands
            (TypeId::Integer, TypeId::Integer, BinaryOperator::And),
            // String concatenation with non-string operands
            (TypeId::Integer, TypeId::VarChar, BinaryOperator::StringConcat),
            // Arithmetic with mixed incompatible types
            (TypeId::Boolean, TypeId::Integer, BinaryOperator::Plus),
        ];

        for (left_type, right_type, op) in test_cases {
            let left_expr = create_constant_expr(Value::new(1), left_type);
            let right_expr = create_constant_expr(Value::new(1), right_type);

            let result = BinaryOpExpression::new(
                left_expr,
                right_expr,
                op.clone(),
                vec![],
            );

            assert!(result.is_err(), "Expected error for {:?} operation between {:?} and {:?}", op, left_type, right_type);
        }
    }

    #[test]
    fn test_division_by_zero() {
        let schema = create_test_schema();
        let tuple = Tuple::new(
            &[
                Value::new(0),         // int_col
                Value::new(0.0),       // decimal_col
                Value::new(false),     // bool_col
                Value::new(""),        // string_col
            ],
            schema.clone(),
            RID::new(0, 0)
        );

        let left_expr = create_constant_expr(Value::new(10), TypeId::Integer);
        let right_expr = create_constant_expr(Value::new(0), TypeId::Integer);

        let binary_expr = BinaryOpExpression::new(
            left_expr,
            right_expr,
            BinaryOperator::Divide,
            vec![],
        ).unwrap();

        let result = binary_expr.evaluate(&tuple, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_display_formatting() {
        let left_expr = create_constant_expr(Value::new(5), TypeId::Integer);
        let right_expr = create_constant_expr(Value::new(3), TypeId::Integer);

        let binary_expr = BinaryOpExpression::new(
            left_expr,
            right_expr,
            BinaryOperator::Plus,
            vec![],
        ).unwrap();

        assert_eq!(format!("{}", binary_expr), "(5 + 3)");
        assert_eq!(format!("{:#}", binary_expr), "(Constant(5) + Constant(3))");
    }

    #[test]
    fn test_evaluate_join() {
        let left_schema = Schema::new(vec![Column::new("left_col", TypeId::Integer)]);
        let right_schema = Schema::new(vec![Column::new("right_col", TypeId::Integer)]);

        let left_tuple = Tuple::new(&[Value::new(5)], left_schema.clone(), RID::new(0, 0));
        let right_tuple = Tuple::new(&[Value::new(3)], right_schema.clone(), RID::new(0, 0));

        let left_expr = create_constant_expr(Value::new(5), TypeId::Integer);
        let right_expr = create_constant_expr(Value::new(3), TypeId::Integer);

        let binary_expr = BinaryOpExpression::new(
            left_expr,
            right_expr,
            BinaryOperator::Plus,
            vec![],
        ).unwrap();

        let result = binary_expr.evaluate_join(&left_tuple, &left_schema, &right_tuple, &right_schema).unwrap();
        assert_eq!(result, Value::new(8));
    }
}
