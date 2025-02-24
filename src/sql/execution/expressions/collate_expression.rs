use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub struct CollateExpression {
    expr: Arc<Expression>,
    collation: String,
    return_type: Column,
}

impl CollateExpression {
    pub fn new(expr: Arc<Expression>, collation: String, return_type: Column) -> Self {
        Self {
            expr,
            collation,
            return_type,
        }
    }
}

impl ExpressionOps for CollateExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let value = self.expr.evaluate(tuple, schema)?;
        // Apply collation rules
        Ok(value)
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
        Ok(value)
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        if child_idx == 0 {
            &self.expr
        } else {
            panic!("Child index out of bounds for CollateExpression")
        }
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        static EMPTY: Vec<Arc<Expression>> = Vec::new();
        &EMPTY
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        assert_eq!(
            children.len(),
            1,
            "CollateExpression requires exactly one child"
        );
        Arc::new(Expression::Collate(CollateExpression::new(
            children[0].clone(),
            self.collation.clone(),
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        self.expr.validate(schema)?;
        let child_type = self.expr.get_return_type().get_type();
        use crate::types_db::type_id::TypeId;
        match child_type {
            TypeId::VarChar | TypeId::Char => Ok(()),
            _ => Err(ExpressionError::InvalidOperation(format!(
                "COLLATE can only be applied to string types, got {:?}",
                child_type
            ))),
        }
    }
}

impl Display for CollateExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} COLLATE {}", self.expr, self.collation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::type_id::TypeId;

    #[test]
    fn test_collate_expression() {
        let string_val = Value::new("Hello");
        let const_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            string_val.clone(),
            Column::new("const", TypeId::VarChar),
            vec![],
        )));

        let collate_expr = CollateExpression::new(
            const_expr,
            "NOCASE".to_string(),
            Column::new("collated", TypeId::VarChar),
        );

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(
            &Vec::new(),
            schema.clone(),
            crate::common::rid::RID::new(0, 0),
        );

        let result = collate_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, string_val);

        assert_eq!(collate_expr.to_string(), "Hello COLLATE NOCASE");
    }

    #[test]
    fn test_validate_invalid_type() {
        let int_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(42),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let collate_expr = CollateExpression::new(
            int_expr,
            "NOCASE".to_string(),
            Column::new("collated", TypeId::Integer),
        );

        let schema = Schema::new(vec![]);
        let result = collate_expr.validate(&schema);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("can only be applied to string types"));
    }

    #[test]
    fn test_clone_with_children() {
        let string_val = Value::new("Hello");
        let const_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            string_val.clone(),
            Column::new("const", TypeId::VarChar),
            vec![],
        )));

        let original = CollateExpression::new(
            const_expr.clone(),
            "NOCASE".to_string(),
            Column::new("collated", TypeId::VarChar),
        );

        let new_const_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("World"),
            Column::new("const", TypeId::VarChar),
            vec![],
        )));

        let cloned = original.clone_with_children(vec![new_const_expr]);

        match cloned.as_ref() {
            Expression::Collate(collate) => {
                assert_eq!(collate.collation, "NOCASE");
                assert_eq!(collate.return_type.get_type(), TypeId::VarChar);
            }
            _ => panic!("Expected Collate expression"),
        }
    }

    #[test]
    #[should_panic(expected = "CollateExpression requires exactly one child")]
    fn test_clone_with_wrong_children_count() {
        let string_val = Value::new("Hello");
        let const_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            string_val.clone(),
            Column::new("const", TypeId::VarChar),
            vec![],
        )));

        let original = CollateExpression::new(
            const_expr.clone(),
            "NOCASE".to_string(),
            Column::new("collated", TypeId::VarChar),
        );

        original.clone_with_children(vec![]);
    }

    #[test]
    fn test_get_child_at() {
        let string_val = Value::new("Hello");
        let const_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            string_val.clone(),
            Column::new("const", TypeId::VarChar),
            vec![],
        )));

        let collate_expr = CollateExpression::new(
            const_expr.clone(),
            "NOCASE".to_string(),
            Column::new("collated", TypeId::VarChar),
        );

        assert_eq!(collate_expr.get_child_at(0), &const_expr);
    }

    #[test]
    #[should_panic(expected = "Child index out of bounds for CollateExpression")]
    fn test_get_child_at_invalid_index() {
        let string_val = Value::new("Hello");
        let const_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            string_val.clone(),
            Column::new("const", TypeId::VarChar),
            vec![],
        )));

        let collate_expr = CollateExpression::new(
            const_expr,
            "NOCASE".to_string(),
            Column::new("collated", TypeId::VarChar),
        );

        collate_expr.get_child_at(1);
    }
}
