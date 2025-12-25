use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::{Val, Value};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub enum IsCheckType {
    True { negated: bool },
    False { negated: bool },
    Unknown { negated: bool },
    Null { negated: bool },
}

#[derive(Clone, Debug, PartialEq)]
pub struct IsCheckExpression {
    expr: Arc<Expression>,
    check_type: IsCheckType,
    return_type: Column,
    children: Vec<Arc<Expression>>,
}

impl IsCheckExpression {
    pub fn new(expr: Arc<Expression>, check_type: IsCheckType, return_type: Column) -> Self {
        Self {
            expr: expr.clone(),
            check_type,
            return_type,
            children: vec![expr],
        }
    }

    pub fn check_type(&self) -> &IsCheckType {
        &self.check_type
    }
}

impl ExpressionOps for IsCheckExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let value = self.expr.evaluate(tuple, schema)?;
        let result = match self.check_type {
            IsCheckType::True { negated } => {
                let is_true = match value.get_val() {
                    Val::Boolean(b) => *b,
                    Val::Null => false,
                    _ => {
                        return Err(ExpressionError::InvalidOperation(
                            "IS TRUE can only be applied to boolean values".to_string(),
                        ));
                    },
                };
                if negated { !is_true } else { is_true }
            },
            IsCheckType::False { negated } => {
                let is_false = match value.get_val() {
                    Val::Boolean(b) => !*b,
                    Val::Null => false,
                    _ => {
                        return Err(ExpressionError::InvalidOperation(
                            "IS FALSE can only be applied to boolean values".to_string(),
                        ));
                    },
                };
                if negated { !is_false } else { is_false }
            },
            IsCheckType::Unknown { negated } => {
                let is_unknown = value.is_null();
                if negated { !is_unknown } else { is_unknown }
            },
            IsCheckType::Null { negated } => {
                let is_null = value.is_null();
                if negated { !is_null } else { is_null }
            },
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
        let value = self
            .expr
            .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        let result = match self.check_type {
            IsCheckType::True { negated } => {
                let is_true = match value.get_val() {
                    Val::Boolean(b) => *b,
                    Val::Null => false,
                    _ => {
                        return Err(ExpressionError::InvalidOperation(
                            "IS TRUE can only be applied to boolean values".to_string(),
                        ));
                    },
                };
                if negated { !is_true } else { is_true }
            },
            IsCheckType::False { negated } => {
                let is_false = match value.get_val() {
                    Val::Boolean(b) => !*b,
                    Val::Null => false,
                    _ => {
                        return Err(ExpressionError::InvalidOperation(
                            "IS FALSE can only be applied to boolean values".to_string(),
                        ));
                    },
                };
                if negated { !is_false } else { is_false }
            },
            IsCheckType::Unknown { negated } => {
                let is_unknown = value.is_null();
                if negated { !is_unknown } else { is_unknown }
            },
            IsCheckType::Null { negated } => {
                let is_null = value.is_null();
                if negated { !is_null } else { is_null }
            },
        };
        Ok(Value::new(result))
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
        assert_eq!(
            children.len(),
            1,
            "IsCheckExpression requires exactly one child"
        );
        Arc::new(Expression::IsCheck(IsCheckExpression::new(
            children[0].clone(),
            self.check_type.clone(),
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        self.expr.validate(schema)
    }
}

impl Display for IsCheckExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} IS ", self.expr)?;
        match self.check_type {
            IsCheckType::True { negated } => {
                if negated {
                    write!(f, "NOT ")?
                }
                write!(f, "TRUE")
            },
            IsCheckType::False { negated } => {
                if negated {
                    write!(f, "NOT ")?
                }
                write!(f, "FALSE")
            },
            IsCheckType::Unknown { negated } => {
                if negated {
                    write!(f, "NOT ")?
                }
                write!(f, "UNKNOWN")
            },
            IsCheckType::Null { negated } => {
                if negated {
                    write!(f, "NOT ")?
                }
                write!(f, "NULL")
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::type_id::TypeId;

    fn create_test_tuple() -> (Tuple, Schema) {
        let schema = Schema::new(vec![
            Column::new("bool_col", TypeId::Boolean),
            Column::new("int_col", TypeId::Integer),
        ]);
        let rid = RID::new(0, 0);
        let tuple = Tuple::new(&[Value::new(true), Value::new(42)], &schema, rid);
        (tuple, schema)
    }

    #[test]
    fn test_is_true() {
        let (tuple, schema) = create_test_tuple();

        // Test true value
        let true_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(true),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));
        let is_true = IsCheckExpression::new(
            true_expr,
            IsCheckType::True { negated: false },
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(is_true.evaluate(&tuple, &schema).unwrap(), Value::new(true));

        // Test false value
        let false_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(false),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));
        let is_true = IsCheckExpression::new(
            false_expr,
            IsCheckType::True { negated: false },
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(
            is_true.evaluate(&tuple, &schema).unwrap(),
            Value::new(false)
        );

        // Test NULL value
        let null_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Null),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));
        let is_true = IsCheckExpression::new(
            null_expr,
            IsCheckType::True { negated: false },
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(
            is_true.evaluate(&tuple, &schema).unwrap(),
            Value::new(false)
        );
    }

    #[test]
    fn test_is_not_true() {
        let (tuple, schema) = create_test_tuple();

        // Test true value
        let true_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(true),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));
        let is_not_true = IsCheckExpression::new(
            true_expr,
            IsCheckType::True { negated: true },
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(
            is_not_true.evaluate(&tuple, &schema).unwrap(),
            Value::new(false)
        );

        // Test false value
        let false_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(false),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));
        let is_not_true = IsCheckExpression::new(
            false_expr,
            IsCheckType::True { negated: true },
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(
            is_not_true.evaluate(&tuple, &schema).unwrap(),
            Value::new(true)
        );

        // Test NULL value
        let null_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Null),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));
        let is_not_true = IsCheckExpression::new(
            null_expr,
            IsCheckType::True { negated: true },
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(
            is_not_true.evaluate(&tuple, &schema).unwrap(),
            Value::new(true)
        );
    }

    #[test]
    fn test_is_false() {
        let (tuple, schema) = create_test_tuple();

        // Test false value
        let false_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(false),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));
        let is_false = IsCheckExpression::new(
            false_expr,
            IsCheckType::False { negated: false },
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(
            is_false.evaluate(&tuple, &schema).unwrap(),
            Value::new(true)
        );

        // Test true value
        let true_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(true),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));
        let is_false = IsCheckExpression::new(
            true_expr,
            IsCheckType::False { negated: false },
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(
            is_false.evaluate(&tuple, &schema).unwrap(),
            Value::new(false)
        );

        // Test NULL value
        let null_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Null),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));
        let is_false = IsCheckExpression::new(
            null_expr,
            IsCheckType::False { negated: false },
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(
            is_false.evaluate(&tuple, &schema).unwrap(),
            Value::new(false)
        );
    }

    #[test]
    fn test_is_unknown() {
        let (tuple, schema) = create_test_tuple();

        // Test NULL value
        let null_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Null),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));
        let is_unknown = IsCheckExpression::new(
            null_expr,
            IsCheckType::Unknown { negated: false },
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(
            is_unknown.evaluate(&tuple, &schema).unwrap(),
            Value::new(true)
        );

        // Test non-NULL value
        let non_null_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(true),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));
        let is_unknown = IsCheckExpression::new(
            non_null_expr,
            IsCheckType::Unknown { negated: false },
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(
            is_unknown.evaluate(&tuple, &schema).unwrap(),
            Value::new(false)
        );
    }

    #[test]
    fn test_invalid_type() {
        let (tuple, schema) = create_test_tuple();

        // Test non-boolean value for IS TRUE
        let int_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(42),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let is_true = IsCheckExpression::new(
            int_expr,
            IsCheckType::True { negated: false },
            Column::new("result", TypeId::Boolean),
        );
        assert!(is_true.evaluate(&tuple, &schema).is_err());
    }

    #[test]
    fn test_display() {
        let expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(true),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));

        let is_true = IsCheckExpression::new(
            expr.clone(),
            IsCheckType::True { negated: false },
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(is_true.to_string(), "true IS TRUE");

        let is_not_true = IsCheckExpression::new(
            expr.clone(),
            IsCheckType::True { negated: true },
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(is_not_true.to_string(), "true IS NOT TRUE");

        let is_unknown = IsCheckExpression::new(
            expr.clone(),
            IsCheckType::Unknown { negated: false },
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(is_unknown.to_string(), "true IS UNKNOWN");
    }

    #[test]
    fn test_clone_with_children() {
        let expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(true),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));
        let is_true = IsCheckExpression::new(
            expr.clone(),
            IsCheckType::True { negated: false },
            Column::new("result", TypeId::Boolean),
        );

        let new_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(false),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));
        let cloned = is_true.clone_with_children(vec![new_expr]);

        match cloned.as_ref() {
            Expression::IsCheck(is_check) => {
                assert_eq!(is_check.check_type, IsCheckType::True { negated: false });
                assert_eq!(is_check.return_type.get_name(), "result");
            },
            _ => panic!("Expected IsCheck expression"),
        }
    }

    #[test]
    fn test_is_null() {
        let (tuple, schema) = create_test_tuple();

        // Test NULL value
        let null_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Null),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));
        let is_null = IsCheckExpression::new(
            null_expr.clone(),
            IsCheckType::Null { negated: false },
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(is_null.evaluate(&tuple, &schema).unwrap(), Value::new(true));

        // Test non-NULL value
        let non_null_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(true),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));
        let is_null = IsCheckExpression::new(
            non_null_expr,
            IsCheckType::Null { negated: false },
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(
            is_null.evaluate(&tuple, &schema).unwrap(),
            Value::new(false)
        );
    }

    #[test]
    fn test_is_not_null() {
        let (tuple, schema) = create_test_tuple();

        // Test NULL value
        let null_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Null),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));
        let is_not_null = IsCheckExpression::new(
            null_expr.clone(),
            IsCheckType::Null { negated: true },
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(
            is_not_null.evaluate(&tuple, &schema).unwrap(),
            Value::new(false)
        );

        // Test non-NULL value
        let non_null_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(true),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));
        let is_not_null = IsCheckExpression::new(
            non_null_expr,
            IsCheckType::Null { negated: true },
            Column::new("result", TypeId::Boolean),
        );
        assert_eq!(
            is_not_null.evaluate(&tuple, &schema).unwrap(),
            Value::new(true)
        );
    }
}
