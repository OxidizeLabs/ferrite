use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub struct CoalesceExpression {
    children: Vec<Arc<Expression>>,
    return_type: Column,
}

impl CoalesceExpression {
    pub fn new(children: Vec<Arc<Expression>>, return_type: Column) -> Self {
        Self {
            children,
            return_type,
        }
    }

    // Helper function to determine if types can be implicitly cast
    fn can_implicitly_cast(from: TypeId, to: TypeId) -> bool {
        match (from, to) {
            // Same types can always be cast
            (a, b) if a == b => true,

            // Numeric type casting rules
            (TypeId::Integer, TypeId::BigInt) => true,
            (TypeId::Integer, TypeId::Decimal) => true,
            (TypeId::BigInt, TypeId::Decimal) => true,

            // String type casting rules
            (TypeId::Char, TypeId::VarChar) => true,

            // All types can be cast to/from NULL
            (TypeId::Invalid, _) | (_, TypeId::Invalid) => true,

            // Other combinations are not allowed
            _ => false,
        }
    }
}

impl ExpressionOps for CoalesceExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        for child in &self.children {
            let result = child.evaluate(tuple, schema)?;
            if !result.is_null() {
                return Ok(result);
            }
        }
        // If all values are null, return null
        Ok(Value::new(Val::Null))
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        for child in &self.children {
            let result = child.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
            if !result.is_null() {
                return Ok(result);
            }
        }
        Ok(Value::new(Val::Null))
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
        Arc::new(Expression::Coalesce(CoalesceExpression::new(
            children,
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // First validate all child expressions
        for child in &self.children {
            child.validate(schema)?;
        }

        if self.children.is_empty() {
            return Err(ExpressionError::InvalidOperation(
                "COALESCE requires at least one argument".to_string(),
            ));
        }

        // Check that all child expressions can be cast to the return type
        let return_type = self.return_type.get_type();
        for child in &self.children {
            let child_type = child.get_return_type().get_type();

            // Skip type checking for NULL values
            if child_type == TypeId::Invalid {
                continue;
            }

            // Check if child type can be cast to return type
            if !Self::can_implicitly_cast(child_type, return_type) {
                return Err(ExpressionError::TypeMismatch {
                    expected: return_type,
                    actual: child_type,
                });
            }
        }

        Ok(())
    }
}

impl Display for CoalesceExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "COALESCE(")?;
        for (i, child) in self.children.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            // Handle NULL values specially when displaying
            if let Expression::Constant(constant) = child.as_ref()
                && constant.get_return_type().get_type() == TypeId::Invalid
            {
                write!(f, "NULL")?;
                continue;
            }
            write!(f, "{}", child)?;
        }
        write!(f, ")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;

    fn create_constant_expr(value: Value) -> Arc<Expression> {
        Arc::new(Expression::Constant(ConstantExpression::new(
            value.clone(),
            Column::new("const", value.get_type_id()),
            vec![],
        )))
    }

    #[test]
    fn test_coalesce_basic() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, crate::common::rid::RID::new(0, 0));

        // Test case: COALESCE(NULL, 1, 2)
        let expr = CoalesceExpression::new(
            vec![
                create_constant_expr(Value::new(Val::Null)),
                create_constant_expr(Value::new(1)),
                create_constant_expr(Value::new(2)),
            ],
            Column::new("result", TypeId::Integer),
        );

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(1));
    }

    #[test]
    fn test_coalesce_all_null() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, crate::common::rid::RID::new(0, 0));

        // Test case: COALESCE(NULL, NULL, NULL)
        let expr = CoalesceExpression::new(
            vec![
                create_constant_expr(Value::new(Val::Null)),
                create_constant_expr(Value::new(Val::Null)),
                create_constant_expr(Value::new(Val::Null)),
            ],
            Column::new("result", TypeId::Integer),
        );

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert!(result.is_null());
    }

    #[test]
    fn test_coalesce_mixed_types() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, crate::common::rid::RID::new(0, 0));

        // Test case: COALESCE(NULL, 1, 2.5)
        let expr = CoalesceExpression::new(
            vec![
                create_constant_expr(Value::new(Val::Null)),
                create_constant_expr(Value::new(1)),
                create_constant_expr(Value::new(2.5)),
            ],
            Column::new("result", TypeId::Decimal),
        );

        // Should validate successfully since Integer can be cast to Decimal
        assert!(expr.validate(&schema).is_ok());

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(1));
    }

    #[test]
    fn test_coalesce_validation_empty() {
        let schema = Schema::new(vec![]);

        // Test case: COALESCE()
        let expr = CoalesceExpression::new(vec![], Column::new("result", TypeId::Integer));

        let result = expr.validate(&schema);
        assert!(matches!(result, Err(ExpressionError::InvalidOperation(_))));
    }

    #[test]
    fn test_coalesce_validation_incompatible_types() {
        let schema = Schema::new(vec![]);

        // Test case: COALESCE(1, 'text') - mixing integer and varchar
        let expr = CoalesceExpression::new(
            vec![
                create_constant_expr(Value::new(1)),
                create_constant_expr(Value::new("text")),
            ],
            Column::new("result", TypeId::Integer),
        );

        let result = expr.validate(&schema);
        assert!(matches!(result, Err(ExpressionError::TypeMismatch { .. })));
    }

    #[test]
    fn test_coalesce_display() {
        let expr = CoalesceExpression::new(
            vec![
                create_constant_expr(Value::new(Val::Null)),
                create_constant_expr(Value::new(1)),
                create_constant_expr(Value::new(2)),
            ],
            Column::new("result", TypeId::Integer),
        );

        assert_eq!(expr.to_string(), "COALESCE(NULL, 1, 2)");
    }

    #[test]
    fn test_coalesce_clone_with_children() {
        let original = CoalesceExpression::new(
            vec![
                create_constant_expr(Value::new(Val::Null)),
                create_constant_expr(Value::new(1)),
            ],
            Column::new("result", TypeId::Integer),
        );

        let new_children = vec![
            create_constant_expr(Value::new(2)),
            create_constant_expr(Value::new(3)),
        ];

        let cloned = original.clone_with_children(new_children);

        match cloned.as_ref() {
            Expression::Coalesce(coalesce) => {
                assert_eq!(coalesce.get_children().len(), 2);
                assert_eq!(coalesce.get_return_type().get_type(), TypeId::Integer);
            },
            _ => panic!("Expected Coalesce expression"),
        }
    }

    #[test]
    fn test_coalesce_evaluate_join() {
        let left_schema = Schema::new(vec![]);
        let right_schema = Schema::new(vec![]);
        let left_tuple = Tuple::new(&[], &left_schema, crate::common::rid::RID::new(0, 0));
        let right_tuple = Tuple::new(&[], &right_schema, crate::common::rid::RID::new(0, 0));

        let expr = CoalesceExpression::new(
            vec![
                create_constant_expr(Value::new(Val::Null)),
                create_constant_expr(Value::new(1)),
                create_constant_expr(Value::new(2)),
            ],
            Column::new("result", TypeId::Integer),
        );

        let result = expr
            .evaluate_join(&left_tuple, &left_schema, &right_tuple, &right_schema)
            .unwrap();
        assert_eq!(result, Value::new(1));
    }
}
