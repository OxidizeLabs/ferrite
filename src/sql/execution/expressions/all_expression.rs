use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::Value;
use sqlparser::ast::BinaryOperator;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// Expression that evaluates a value against ALL values in a subquery/array
/// e.g. x > ALL(SELECT y FROM t) or x > ALL(ARRAY[1,2,3])
#[derive(Debug, Clone, PartialEq)]
pub struct AllExpression {
    /// The left expression to compare
    left: Arc<Expression>,
    /// The right expression (subquery or array) that produces multiple values
    right: Arc<Expression>,
    /// The comparison operator to use
    compare_op: BinaryOperator,
    /// Return type is always boolean
    return_type: Column,
    /// Child expressions
    children: Vec<Arc<Expression>>,
}

impl AllExpression {
    pub fn new(left: Arc<Expression>, right: Arc<Expression>, compare_op: BinaryOperator) -> Self {
        let children = vec![left.clone(), right.clone()];
        Self {
            left,
            right,
            compare_op,
            return_type: Column::new("all", TypeId::Boolean),
            children,
        }
    }
}

impl ExpressionOps for AllExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        // Evaluate the left expression
        let left_val = self.left.evaluate(tuple, schema)?;

        // Evaluate the right expression (should return a vector/array)
        let right_val = self.right.evaluate(tuple, schema)?;

        // Extract values from right expression result
        let right_values = match right_val.get_val() {
            crate::types_db::value::Val::Vector(values) => values,
            _ => {
                return Err(ExpressionError::InvalidOperation(
                    "ALL operation requires array/vector right operand".to_string(),
                ));
            },
        };

        if right_values.is_empty() {
            // ALL over empty set is true
            return Ok(Value::new(true));
        }

        // Compare left value with ALL right values
        for right_val in right_values {
            let result = match &self.compare_op {
                BinaryOperator::Eq => left_val.compare_equals(right_val),
                BinaryOperator::NotEq => left_val.compare_not_equals(right_val),
                BinaryOperator::Gt => left_val.compare_greater_than(right_val),
                BinaryOperator::GtEq => left_val.compare_greater_than_equals(right_val),
                BinaryOperator::Lt => left_val.compare_less_than(right_val),
                BinaryOperator::LtEq => left_val.compare_less_than_equals(right_val),
                _ => {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "Unsupported operator for ALL: {:?}",
                        self.compare_op
                    )));
                },
            };

            // If any comparison is false, the ALL operation is false
            if matches!(result, crate::types_db::types::CmpBool::CmpFalse) {
                return Ok(Value::new(false));
            }
            // If any comparison is null and we haven't found a false yet,
            // the result might be null
            if matches!(result, crate::types_db::types::CmpBool::CmpNull) {
                return Ok(Value::new(crate::types_db::value::Val::Null));
            }
        }

        // If we get here, all comparisons were true
        Ok(Value::new(true))
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        // Evaluate left expression using left tuple/schema
        let left_val =
            self.left
                .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;

        // Evaluate right expression using right tuple/schema
        let right_val =
            self.right
                .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;

        // Extract values from right expression result
        let right_values = match right_val.get_val() {
            crate::types_db::value::Val::Vector(values) => values,
            _ => {
                return Err(ExpressionError::InvalidOperation(
                    "ALL operation requires array/vector right operand".to_string(),
                ));
            },
        };

        if right_values.is_empty() {
            // ALL over empty set is true
            return Ok(Value::new(true));
        }

        // Compare left value with ALL right values
        for right_val in right_values {
            let result = match &self.compare_op {
                BinaryOperator::Eq => left_val.compare_equals(right_val),
                BinaryOperator::NotEq => left_val.compare_not_equals(right_val),
                BinaryOperator::Gt => left_val.compare_greater_than(right_val),
                BinaryOperator::GtEq => left_val.compare_greater_than_equals(right_val),
                BinaryOperator::Lt => left_val.compare_less_than(right_val),
                BinaryOperator::LtEq => left_val.compare_less_than_equals(right_val),
                _ => {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "Unsupported operator for ALL: {:?}",
                        self.compare_op
                    )));
                },
            };

            // If any comparison is false, the ALL operation is false
            if matches!(result, crate::types_db::types::CmpBool::CmpFalse) {
                return Ok(Value::new(false));
            }
            // If any comparison is null and we haven't found a false yet,
            // the result might be null
            if matches!(result, crate::types_db::types::CmpBool::CmpNull) {
                return Ok(Value::new(crate::types_db::value::Val::Null));
            }
        }

        // If we get here, all comparisons were true
        Ok(Value::new(true))
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
            panic!("AllExpression requires exactly 2 children");
        }
        Arc::new(Expression::All(AllExpression::new(
            children[0].clone(),
            children[1].clone(),
            self.compare_op.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate both child expressions
        self.left.validate(schema)?;
        self.right.validate(schema)?;

        // Verify the right expression returns a vector type
        if self.right.get_return_type().get_type() != TypeId::Vector {
            return Err(ExpressionError::InvalidOperation(
                "ALL operation requires vector/array right operand".to_string(),
            ));
        }

        Ok(())
    }
}

impl Display for AllExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} ALL({})", self.left, self.compare_op, self.right)
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
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::Integer),
        ]);
        let rid = RID::new(0, 0);
        let tuple = Tuple::new(&[Value::new(1), Value::new(10)], &schema, rid);
        (tuple, schema)
    }

    #[test]
    fn test_all_empty_vector() {
        let (tuple, schema) = create_test_tuple();

        // Create x > ALL([]) expression
        let left = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(5),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let right = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_vector::<Vec<Value>>(vec![]),
            Column::new("vector", TypeId::Vector),
            vec![],
        )));

        let expr = AllExpression::new(left, right, BinaryOperator::Gt);
        let result = expr.evaluate(&tuple, &schema).unwrap();

        // ALL over empty set should be true
        assert_eq!(result, Value::new(true));
    }

    #[test]
    fn test_all_comparison_true() {
        let (tuple, schema) = create_test_tuple();

        // Create 5 > ALL([1,2,3]) expression
        let left = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(5),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let right = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_vector(vec![Value::new(1), Value::new(2), Value::new(3)]),
            Column::new("vector", TypeId::Vector),
            vec![],
        )));

        let expr = AllExpression::new(left, right, BinaryOperator::Gt);
        let result = expr.evaluate(&tuple, &schema).unwrap();

        assert_eq!(result, Value::new(true));
    }

    #[test]
    fn test_all_comparison_false() {
        let (tuple, schema) = create_test_tuple();

        // Create 5 > ALL([1,2,6]) expression
        let left = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(5),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let right = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_vector(vec![Value::new(1), Value::new(2), Value::new(6)]),
            Column::new("vector", TypeId::Vector),
            vec![],
        )));

        let expr = AllExpression::new(left, right, BinaryOperator::Gt);
        let result = expr.evaluate(&tuple, &schema).unwrap();

        assert_eq!(result, Value::new(false));
    }

    #[test]
    fn test_all_with_null() {
        let (tuple, schema) = create_test_tuple();

        // Create 5 > ALL([1,null,3]) expression
        let left = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(5),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let right = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_vector(vec![Value::new(1), Value::new(Val::Null), Value::new(3)]),
            Column::new("vector", TypeId::Vector),
            vec![],
        )));

        let expr = AllExpression::new(left, right, BinaryOperator::Gt);
        let result = expr.evaluate(&tuple, &schema).unwrap();

        // Result should be NULL since we have a NULL comparison and no false comparisons
        assert_eq!(result, Value::new(Val::Null));
    }

    #[test]
    fn test_all_invalid_operator() {
        let (tuple, schema) = create_test_tuple();

        let left = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(5),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let right = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_vector(vec![Value::new(1)]),
            Column::new("vector", TypeId::Vector),
            vec![],
        )));

        // Try with an invalid operator
        let expr = AllExpression::new(left, right, BinaryOperator::Plus);
        assert!(expr.evaluate(&tuple, &schema).is_err());
    }

    #[test]
    fn test_all_invalid_right_operand() {
        let (tuple, schema) = create_test_tuple();

        let left = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(5),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let right = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1), // Not a vector
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let expr = AllExpression::new(left, right, BinaryOperator::Gt);
        assert!(expr.evaluate(&tuple, &schema).is_err());
    }

    #[test]
    fn test_validate() {
        let schema = Schema::new(vec![Column::new("test", TypeId::Integer)]);

        let left = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(5),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let right = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_vector(vec![Value::new(1)]),
            Column::new("vector", TypeId::Vector),
            vec![],
        )));

        let expr = AllExpression::new(left.clone(), right.clone(), BinaryOperator::Gt);
        assert!(expr.validate(&schema).is_ok());

        // Test with invalid right operand type
        let invalid_right = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let invalid_expr = AllExpression::new(left, invalid_right, BinaryOperator::Gt);
        assert!(invalid_expr.validate(&schema).is_err());
    }
}
