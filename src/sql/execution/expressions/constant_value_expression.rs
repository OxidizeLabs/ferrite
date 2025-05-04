use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct ConstantExpression {
    value: Value,
    ret_type: Column,
    children: Vec<Arc<Expression>>,
}

impl ConstantExpression {
    pub fn new(value: Value, ret_type: Column, children: Vec<Arc<Expression>>) -> Self {
        Self {
            value,
            ret_type,
            children,
        }
    }

    pub fn get_value(&self) -> &Value {
        &self.value
    }
}

impl ExpressionOps for ConstantExpression {
    fn evaluate(&self, _tuple: &Tuple, _schema: &Schema) -> Result<Value, ExpressionError> {
        // For a constant expression, we simply return the stored value
        // regardless of the input tuple or schema
        Ok(self.value.clone())
    }

    fn evaluate_join(
        &self,
        _left_tuple: &Tuple,
        _left_schema: &Schema,
        _right_tuple: &Tuple,
        _right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        // For a constant expression, join evaluation also just returns
        // the stored value, as it's independent of the input tuples
        Ok(self.value.clone())
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        // Return reference to child at given index
        // Panic if index out of bounds (standard Vec behavior)
        &self.children[child_idx]
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.ret_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        Arc::new(Expression::Constant(ConstantExpression::new(
            self.value.clone(),
            self.ret_type.clone(),
            children,
        )))
    }

    fn validate(&self, _schema: &Schema) -> Result<(), ExpressionError> {
        // Constant expressions are always valid as they don't depend on the schema
        // Just verify that the value type matches the return type
        if self.value.get_type_id() != self.ret_type.get_type() {
            return Err(ExpressionError::TypeMismatch {
                expected: self.ret_type.get_type(),
                actual: self.value.get_type_id(),
            });
        }
        Ok(())
    }
}

impl Display for ConstantExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // Always just display the value, regardless of format
        if f.alternate() {
            write!(f, "Constant({:#})", self.value)
        } else {
            write!(f, "{}", self.value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::types_db::type_id::TypeId;

    #[test]
    fn test_constant_expression_evaluate() {
        let value = Value::new(42);
        let column = Column::new("test", TypeId::Integer);
        let expr = ConstantExpression::new(value.clone(), column, vec![]);

        let dummy_tuple = Tuple::new(&*vec![], &Schema::new(vec![]), RID::new(0, 0));
        let dummy_schema = Schema::new(vec![]);

        // Test basic evaluation
        let result = expr.evaluate(&dummy_tuple, &dummy_schema).unwrap();
        assert_eq!(result, value);

        // Test join evaluation
        let result = expr
            .evaluate_join(&dummy_tuple, &dummy_schema, &dummy_tuple, &dummy_schema)
            .unwrap();
        assert_eq!(result, value);
    }

    #[test]
    fn test_constant_expression_children() {
        let value = Value::new(42);
        let column = Column::new("test", TypeId::Integer);
        let children = vec![Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            column.clone(),
            vec![],
        )))];

        let expr = ConstantExpression::new(value, column, children.clone());

        // Test get_children
        assert_eq!(expr.get_children().len(), 1);

        // Test get_child_at
        let child = expr.get_child_at(0);
        match &**child {
            Expression::Constant(c) => {
                assert_eq!(c.get_value(), &Value::new(1));
            }
            _ => panic!("Expected Constant expression"),
        }

        // Test clone_with_children
        let new_children = vec![];
        let cloned = expr.clone_with_children(new_children);
        assert_eq!(cloned.get_children().len(), 0);
    }
}
