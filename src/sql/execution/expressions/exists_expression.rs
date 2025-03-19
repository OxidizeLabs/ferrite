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

#[derive(PartialEq, Clone, Debug)]
pub struct ExistsExpression {
    subquery: Arc<Expression>,
    negated: bool,
    return_type: Column,
    children: Vec<Arc<Expression>>,
}

impl ExistsExpression {
    pub fn new(subquery: Arc<Expression>, negated: bool, return_type: Column) -> Self {
        Self {
            subquery: subquery.clone(),
            negated,
            return_type,
            children: vec![subquery],
        }
    }
}

impl ExpressionOps for ExistsExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let exists = self.subquery.evaluate(tuple, schema)?;
        let result = if self.negated {
            exists.is_null()
        } else {
            !exists.is_null()
        };
        Ok(Value::new(result))
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        let exists =
            self.subquery
                .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        let result = if self.negated {
            exists.is_null()
        } else {
            !exists.is_null()
        };
        Ok(Value::new(result))
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        if child_idx == 0 {
            &self.subquery
        } else {
            panic!("Index out of bounds in ExistsExpression::get_child_at")
        }
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        if children.len() != 1 {
            panic!("ExistsExpression requires exactly one child");
        }

        Arc::new(Expression::Exists(ExistsExpression::new(
            children[0].clone(),
            self.negated,
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate the subquery expression
        self.subquery.validate(schema)?;

        // Ensure return type is boolean
        if self.return_type.get_type() != TypeId::Boolean {
            return Err(ExpressionError::InvalidReturnType(
                "EXISTS expression must return a boolean value".to_string(),
            ));
        }

        Ok(())
    }
}

impl Display for ExistsExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}EXISTS ({})",
            if self.negated { "NOT " } else { "" },
            self.subquery
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::value::Val;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ])
    }

    fn create_test_tuple() -> Tuple {
        let values = vec![
            Value::new(1),      // id
            Value::new("John"), // name
            Value::new(30),     // age
        ];
        Tuple::new(&values, create_test_schema(), RID::new(1, 1))
    }

    #[test]
    fn test_exists_expression_evaluate() {
        let schema = create_test_schema();
        let tuple = create_test_tuple();

        // Create a subquery that returns a non-null value (EXISTS should be true)
        let non_null_subquery = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        // Create a subquery that returns null (EXISTS should be false)
        let null_subquery = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Null),
            Column::new("const", TypeId::Invalid),
            vec![],
        )));

        // Test EXISTS with non-null subquery
        let exists_expr = ExistsExpression::new(
            non_null_subquery.clone(),
            false,
            Column::new("exists", TypeId::Boolean),
        );
        let result = exists_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(true));

        // Test NOT EXISTS with non-null subquery
        let not_exists_expr = ExistsExpression::new(
            non_null_subquery.clone(),
            true,
            Column::new("not_exists", TypeId::Boolean),
        );
        let result = not_exists_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(false));

        // Test EXISTS with null subquery
        let exists_null_expr = ExistsExpression::new(
            null_subquery.clone(),
            false,
            Column::new("exists", TypeId::Boolean),
        );
        let result = exists_null_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(false));

        // Test NOT EXISTS with null subquery
        let not_exists_null_expr = ExistsExpression::new(
            null_subquery,
            true,
            Column::new("not_exists", TypeId::Boolean),
        );
        let result = not_exists_null_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(true));
    }

    #[test]
    fn test_exists_expression_evaluate_join() {
        let left_schema = create_test_schema();
        let right_schema = Schema::new(vec![
            Column::new("dept_id", TypeId::Integer),
            Column::new("dept_name", TypeId::VarChar),
        ]);

        let left_tuple = create_test_tuple();
        let right_tuple = Tuple::new(
            &vec![Value::new(1), Value::new("Engineering")],
            right_schema.clone(),
            RID::new(2, 1),
        );

        // Create a subquery that returns a non-null value
        let non_null_subquery = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        // Test EXISTS with join evaluation
        let exists_expr = ExistsExpression::new(
            non_null_subquery,
            false,
            Column::new("exists", TypeId::Boolean),
        );

        let result = exists_expr
            .evaluate_join(&left_tuple, &left_schema, &right_tuple, &right_schema)
            .unwrap();

        assert_eq!(result, Value::new(true));
    }

    #[test]
    fn test_exists_expression_display() {
        let subquery = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let exists_expr = ExistsExpression::new(
            subquery.clone(),
            false,
            Column::new("exists", TypeId::Boolean),
        );

        let not_exists_expr =
            ExistsExpression::new(subquery, true, Column::new("not_exists", TypeId::Boolean));

        assert_eq!(exists_expr.to_string(), "EXISTS (1)");
        assert_eq!(not_exists_expr.to_string(), "NOT EXISTS (1)");
    }

    #[test]
    fn test_exists_expression_validate() {
        let schema = create_test_schema();

        let subquery = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        // Valid return type (Boolean)
        let valid_expr = ExistsExpression::new(
            subquery.clone(),
            false,
            Column::new("exists", TypeId::Boolean),
        );

        // Invalid return type (Integer)
        let invalid_expr =
            ExistsExpression::new(subquery, false, Column::new("exists", TypeId::Integer));

        assert!(valid_expr.validate(&schema).is_ok());
        assert!(invalid_expr.validate(&schema).is_err());
    }

    #[test]
    fn test_exists_expression_clone_with_children() {
        let original_subquery = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let new_subquery = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(2),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let exists_expr = ExistsExpression::new(
            original_subquery,
            false,
            Column::new("exists", TypeId::Boolean),
        );

        let cloned_expr = exists_expr.clone_with_children(vec![new_subquery]);

        match cloned_expr.as_ref() {
            Expression::Exists(e) => {
                assert_eq!(e.negated, false);
                assert_eq!(e.return_type.get_name(), "exists");

                // Verify the child was replaced
                match e.get_child_at(0).as_ref() {
                    Expression::Constant(c) => {
                        assert_eq!(c.get_value(), &Value::new(2));
                    }
                    _ => panic!("Expected Constant expression"),
                }
            }
            _ => panic!("Expected Exists expression"),
        }
    }

    #[test]
    #[should_panic(expected = "ExistsExpression requires exactly one child")]
    fn test_exists_expression_clone_with_invalid_children() {
        let subquery = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let exists_expr =
            ExistsExpression::new(subquery, false, Column::new("exists", TypeId::Boolean));

        // This should panic because ExistsExpression requires exactly one child
        let _ = exists_expr.clone_with_children(vec![]);
    }

    #[test]
    #[should_panic(expected = "Index out of bounds")]
    fn test_exists_expression_get_child_at_invalid_index() {
        let subquery = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let exists_expr =
            ExistsExpression::new(subquery, false, Column::new("exists", TypeId::Boolean));

        // This should panic because index 1 is out of bounds
        let _ = exists_expr.get_child_at(1);
    }
}
