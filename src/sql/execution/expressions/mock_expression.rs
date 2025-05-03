use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::binder::bound_expression::{BoundExpression, ExpressionType};
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use std::any::Any;
use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct MockExpression {
    name: String,
    return_type: TypeId,
    children: Vec<Arc<Expression>>,
    return_value: Option<Value>,
}

impl MockExpression {
    pub fn new(name: String, return_type: TypeId) -> Self {
        let return_value = Some(match return_type {
            TypeId::Boolean => Value::new(true),
            TypeId::TinyInt => Value::new(42i8),
            TypeId::SmallInt => Value::new(42i16),
            TypeId::Integer => Value::new(42i32),
            TypeId::BigInt => Value::new(42i64),
            TypeId::Decimal => Value::new(42.0f64),
            TypeId::Timestamp => Value::new(42u64),
            TypeId::VarChar => Value::new(name.clone()),
            TypeId::Vector => Value::new_vector(vec![Value::new(1), Value::new(2), Value::new(3)]),
            TypeId::Invalid => Value::new(Val::Null),
            TypeId::Char => Value::new(name.clone()),
            TypeId::Struct => {
                let field_names = vec!["field1", "field2"];
                let values = vec![Value::new(42), Value::new("test")];
                Value::new_struct(field_names, values)
            },
            _ => todo!()
        });

        Self {
            name,
            return_type,
            children: Vec::new(),
            return_value,
        }
    }

    pub fn with_value(name: String, return_type: TypeId, value: Value) -> Self {
        Self {
            name,
            return_type,
            children: Vec::new(),
            return_value: Some(value),
        }
    }

    pub fn with_children(mut self, children: Vec<Arc<Expression>>) -> Self {
        self.children = children;
        self
    }

    pub fn get_type(&self) -> TypeId {
        self.return_type
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }
}

impl BoundExpression for MockExpression {
    fn expression_type(&self) -> ExpressionType {
        ExpressionType::MockExpression
    }

    fn has_aggregation(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_box(&self) -> Box<dyn BoundExpression> {
        Box::new(self.clone())
    }
}

impl Display for MockExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            write!(f, "MockExpression({}:{:?})", self.name, self.return_type)
        } else {
            write!(f, "{}", self.name)
        }
    }
}

impl ExpressionOps for MockExpression {
    fn evaluate(&self, _tuple: &Tuple, _schema: &Schema) -> Result<Value, ExpressionError> {
        self.return_value.clone().ok_or_else(|| {
            ExpressionError::UnsupportedOperation("Mock expression has no return value".to_string())
        })
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        _right_tuple: &Tuple,
        _right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        // For mock purposes, just use the left tuple evaluation
        self.evaluate(left_tuple, left_schema)
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        &self.children[child_idx]
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        static COLUMN: std::sync::OnceLock<Column> = std::sync::OnceLock::new();
        COLUMN.get_or_init(|| Column::new("mock_col", self.return_type))
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        Arc::new(Expression::Mock(self.clone().with_children(children)))
    }

    fn validate(&self, _schema: &Schema) -> Result<(), ExpressionError> {
        // Mock expressions are always valid for testing purposes
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;

    fn create_test_tuple() -> (Tuple, Schema) {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], schema.clone(), RID::new(0, 0));
        (tuple, schema)
    }

    #[test]
    fn test_mock_expression_types() {
        let test_cases = vec![
            (TypeId::Boolean, "true"),
            (TypeId::TinyInt, "42"),
            (TypeId::SmallInt, "42"),
            (TypeId::Integer, "42"),
            (TypeId::BigInt, "42"),
            (TypeId::Decimal, "42"),
            (TypeId::Timestamp, "0 days 0:0:42 UTC"),
            (TypeId::VarChar, "test"),
            (TypeId::Vector, "[1, 2, 3]"),
            (TypeId::Struct, "{field1: 42, field2: test}"),
        ];

        for (type_id, expected_str) in test_cases {
            let expr = MockExpression::new("test".to_string(), type_id);
            let (tuple, schema) = create_test_tuple();
            let result = expr.evaluate(&tuple, &schema).unwrap();
            assert_eq!(result.to_string(), expected_str);
        }
    }

    #[test]
    fn test_mock_expression_with_custom_value() {
        let value = Value::new(123);
        let expr = MockExpression::with_value("test".to_string(), TypeId::Integer, value);
        let (tuple, schema) = create_test_tuple();
        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(123));
    }

    #[test]
    fn test_mock_expression_with_children() {
        let child1 = Arc::new(Expression::Mock(MockExpression::new(
            "child1".to_string(),
            TypeId::Integer,
        )));
        let child2 = Arc::new(Expression::Mock(MockExpression::new(
            "child2".to_string(),
            TypeId::VarChar,
        )));

        let expr = MockExpression::new("parent".to_string(), TypeId::Integer)
            .with_children(vec![child1.clone(), child2.clone()]);

        assert_eq!(expr.get_children().len(), 2);
        assert_eq!(expr.get_child_at(0), &child1);
        assert_eq!(expr.get_child_at(1), &child2);
    }

    #[test]
    fn test_mock_expression_join_evaluation() {
        let expr = MockExpression::new("test".to_string(), TypeId::Integer);
        let (tuple, schema) = create_test_tuple();

        let result = expr
            .evaluate_join(&tuple, &schema, &tuple, &schema)
            .unwrap();
        assert_eq!(result, Value::new(42));
    }

    #[test]
    fn test_mock_expression_null() {
        let expr = MockExpression::new("test".to_string(), TypeId::Invalid);
        let (tuple, schema) = create_test_tuple();

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.get_val(), &Val::Null);
    }
}
