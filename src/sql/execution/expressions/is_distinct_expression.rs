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

#[derive(Clone, Debug, PartialEq)]
pub struct IsDistinctExpression {
    children: Vec<Arc<Expression>>,
    negated: bool,
    return_type: Column,
}

impl IsDistinctExpression {
    pub fn new(
        left: Arc<Expression>,
        right: Arc<Expression>,
        negated: bool,
        return_type: Column,
    ) -> Self {
        Self {
            children: vec![left, right],
            negated,
            return_type,
        }
    }

    fn left(&self) -> &Arc<Expression> {
        &self.children[0]
    }

    fn right(&self) -> &Arc<Expression> {
        &self.children[1]
    }
}

impl ExpressionOps for IsDistinctExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let left_val = self.left().evaluate(tuple, schema)?;
        let right_val = self.right().evaluate(tuple, schema)?;

        let is_distinct = if left_val.is_null() && right_val.is_null() {
            false
        } else if left_val.is_null() || right_val.is_null() {
            true
        } else {
            left_val != right_val
        };

        Ok(Value::new(if self.negated {
            !is_distinct
        } else {
            is_distinct
        }))
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        let left_val =
            self.left()
                .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        let right_val =
            self.right()
                .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;

        let is_distinct = if left_val.is_null() && right_val.is_null() {
            false // NULL IS NOT DISTINCT FROM NULL
        } else if left_val.is_null() || right_val.is_null() {
            true // NULL IS DISTINCT FROM any non-NULL value
        } else {
            left_val != right_val
        };

        Ok(Value::new(if self.negated {
            !is_distinct
        } else {
            is_distinct
        }))
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
            panic!("IsDistinctExpression requires exactly 2 children");
        }
        Arc::new(Expression::IsDistinct(IsDistinctExpression::new(
            children[0].clone(),
            children[1].clone(),
            self.negated,
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate both child expressions
        self.left().validate(schema)?;
        self.right().validate(schema)?;

        // Verify the return type is boolean
        if self.return_type.get_type() != TypeId::Boolean {
            return Err(ExpressionError::InvalidOperation(
                "IS DISTINCT FROM must return boolean".to_string(),
            ));
        }

        Ok(())
    }
}

impl Display for IsDistinctExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} IS{} DISTINCT FROM {}",
            self.left(),
            if self.negated { " NOT" } else { "" },
            self.right()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::value::Val;

    fn create_test_tuple() -> (Tuple, Schema) {
        let schema = Schema::new(vec![
            Column::new("col1", TypeId::Integer),
            Column::new("col2", TypeId::VarChar),
        ]);
        let tuple = Tuple::new(
            &[Value::new(42), Value::new("test")],
            &schema,
            Default::default(),
        );
        (tuple, schema)
    }

    fn create_constant_expr(val: Value) -> Arc<Expression> {
        Arc::new(Expression::Constant(ConstantExpression::new(
            val.clone(),
            Column::new("const", val.get_type_id()),
            vec![],
        )))
    }

    #[test]
    fn test_basic_distinct_comparison() {
        let (tuple, schema) = create_test_tuple();

        // Test 1 IS DISTINCT FROM 2 (should return true)
        let expr = IsDistinctExpression::new(
            create_constant_expr(Value::new(1)),
            create_constant_expr(Value::new(2)),
            false,
            Column::new("result", TypeId::Boolean),
        );

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(true));

        // Test 1 IS DISTINCT FROM 1 (should return false)
        let expr = IsDistinctExpression::new(
            create_constant_expr(Value::new(1)),
            create_constant_expr(Value::new(1)),
            false,
            Column::new("result", TypeId::Boolean),
        );

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(false));
    }

    #[test]
    fn test_not_distinct_comparison() {
        let (tuple, schema) = create_test_tuple();

        // Test 1 IS NOT DISTINCT FROM 1 (should return true)
        let expr = IsDistinctExpression::new(
            create_constant_expr(Value::new(1)),
            create_constant_expr(Value::new(1)),
            true,
            Column::new("result", TypeId::Boolean),
        );

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(true));

        // Test 1 IS NOT DISTINCT FROM 2 (should return false)
        let expr = IsDistinctExpression::new(
            create_constant_expr(Value::new(1)),
            create_constant_expr(Value::new(2)),
            true,
            Column::new("result", TypeId::Boolean),
        );

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(false));
    }

    #[test]
    fn test_null_comparisons() {
        let (tuple, schema) = create_test_tuple();

        // Test NULL IS DISTINCT FROM NULL (should return false)
        let expr = IsDistinctExpression::new(
            create_constant_expr(Value::new(Val::Null)),
            create_constant_expr(Value::new(Val::Null)),
            false,
            Column::new("result", TypeId::Boolean),
        );

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(false));

        // Test NULL IS DISTINCT FROM 1 (should return true)
        let expr = IsDistinctExpression::new(
            create_constant_expr(Value::new(Val::Null)),
            create_constant_expr(Value::new(1)),
            false,
            Column::new("result", TypeId::Boolean),
        );

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(true));
    }

    #[test]
    fn test_join_evaluation() {
        let (left_tuple, left_schema) = create_test_tuple();
        let (right_tuple, right_schema) = create_test_tuple();

        // Test join evaluation with constants
        let expr = IsDistinctExpression::new(
            create_constant_expr(Value::new(1)),
            create_constant_expr(Value::new(2)),
            false,
            Column::new("result", TypeId::Boolean),
        );

        let result = expr
            .evaluate_join(&left_tuple, &left_schema, &right_tuple, &right_schema)
            .unwrap();
        assert_eq!(result, Value::new(true));
    }

    #[test]
    fn test_validation() {
        let schema = Schema::new(vec![]);

        // Test with invalid return type
        let expr = IsDistinctExpression::new(
            create_constant_expr(Value::new(1)),
            create_constant_expr(Value::new(2)),
            false,
            Column::new("result", TypeId::Integer), // Should be Boolean
        );

        assert!(expr.validate(&schema).is_err());

        // Test with valid return type
        let expr = IsDistinctExpression::new(
            create_constant_expr(Value::new(1)),
            create_constant_expr(Value::new(2)),
            false,
            Column::new("result", TypeId::Boolean),
        );

        assert!(expr.validate(&schema).is_ok());
    }

    #[test]
    fn test_display() {
        let expr = IsDistinctExpression::new(
            create_constant_expr(Value::new(1)),
            create_constant_expr(Value::new(2)),
            false,
            Column::new("result", TypeId::Boolean),
        );

        assert_eq!(expr.to_string(), "1 IS DISTINCT FROM 2");

        let expr = IsDistinctExpression::new(
            create_constant_expr(Value::new(1)),
            create_constant_expr(Value::new(2)),
            true,
            Column::new("result", TypeId::Boolean),
        );

        assert_eq!(expr.to_string(), "1 IS NOT DISTINCT FROM 2");
    }

    #[test]
    fn test_clone_with_children() {
        let expr = IsDistinctExpression::new(
            create_constant_expr(Value::new(1)),
            create_constant_expr(Value::new(2)),
            false,
            Column::new("result", TypeId::Boolean),
        );

        let new_children = vec![
            create_constant_expr(Value::new(3)),
            create_constant_expr(Value::new(4)),
        ];

        let cloned = expr.clone_with_children(new_children);

        if let Expression::IsDistinct(cloned_expr) = cloned.as_ref() {
            assert_eq!(
                cloned_expr
                    .left()
                    .evaluate(&create_test_tuple().0, &create_test_tuple().1)
                    .unwrap(),
                Value::new(3)
            );
            assert_eq!(
                cloned_expr
                    .right()
                    .evaluate(&create_test_tuple().0, &create_test_tuple().1)
                    .unwrap(),
                Value::new(4)
            );
            assert_eq!(cloned_expr.negated, expr.negated);
            assert_eq!(cloned_expr.return_type, expr.return_type);
        } else {
            panic!("Expected IsDistinct expression");
        }
    }

    #[test]
    #[should_panic(expected = "IsDistinctExpression requires exactly 2 children")]
    fn test_clone_with_wrong_number_of_children() {
        let expr = IsDistinctExpression::new(
            create_constant_expr(Value::new(1)),
            create_constant_expr(Value::new(2)),
            false,
            Column::new("result", TypeId::Boolean),
        );

        expr.clone_with_children(vec![create_constant_expr(Value::new(1))]);
    }
}
