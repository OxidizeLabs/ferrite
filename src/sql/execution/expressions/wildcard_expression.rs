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
pub struct WildcardExpression {
    return_type: Column,
}

impl WildcardExpression {
    pub fn new(return_type: Column) -> Self {
        Self { return_type }
    }
}

impl ExpressionOps for WildcardExpression {
    fn evaluate(&self, tuple: &Tuple, _schema: &Schema) -> Result<Value, ExpressionError> {
        // Get all values from the tuple as a vector
        let values: Vec<Value> = tuple.get_values().to_vec();

        // Return the values as a vector type
        Ok(Value::new_vector(values))
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        _left_schema: &Schema,
        right_tuple: &Tuple,
        _right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        // For joins, combine values from both tuples
        let mut values = Vec::new();

        // Add values from left tuple
        values.extend(left_tuple.get_values().iter().cloned());

        // Add values from right tuple
        values.extend(right_tuple.get_values().iter().cloned());

        Ok(Value::new_vector(values))
    }

    fn get_child_at(&self, _child_idx: usize) -> &Arc<Expression> {
        panic!("WildcardExpression has no children")
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        // Wildcard expression has no children
        static EMPTY: Vec<Arc<Expression>> = Vec::new();
        &EMPTY
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        if !children.is_empty() {
            panic!("WildcardExpression cannot have children");
        }
        Arc::new(Expression::Wildcard(self.clone()))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // A wildcard is always valid as long as the schema has at least one column
        if schema.get_column_count() == 0 {
            return Err(ExpressionError::EvaluationError(
                "Cannot use wildcard on empty schema".to_string(),
            ));
        }
        Ok(())
    }
}

impl Display for WildcardExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "*")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Val;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ])
    }

    fn create_test_tuple(values: Vec<Value>, schema: &Schema) -> Tuple {
        Tuple::new(&values, &schema, RID::new(0, 0))
    }

    #[test]
    fn test_wildcard_evaluate() {
        let schema = create_test_schema();
        let values = vec![
            Value::new(1),       // id
            Value::new("Alice"), // name
            Value::new(25),      // age
        ];
        let tuple = create_test_tuple(values.clone(), &schema);

        let wildcard = WildcardExpression::new(Column::new("*", TypeId::Vector));
        let result = wildcard.evaluate(&tuple, &schema).unwrap();

        // Check that we got a vector value back
        assert_eq!(result.get_type_id(), TypeId::Vector);

        // Extract the vector and verify contents
        if let Val::Vector(result_values) = result.get_val() {
            assert_eq!(result_values.len(), 3);
            assert_eq!(result_values[0], values[0]);
            assert_eq!(result_values[1], values[1]);
            assert_eq!(result_values[2], values[2]);
        } else {
            panic!("Expected Vector value");
        }
    }

    #[test]
    fn test_wildcard_evaluate_join() {
        let schema = create_test_schema();

        // Create left tuple
        let left_values = vec![Value::new(1), Value::new("Alice"), Value::new(25)];
        let left_tuple = create_test_tuple(left_values.clone(), &schema);

        // Create right tuple
        let right_values = vec![Value::new(2), Value::new("Bob"), Value::new(30)];
        let right_tuple = create_test_tuple(right_values.clone(), &schema);

        let wildcard = WildcardExpression::new(Column::new("*", TypeId::Vector));
        let result = wildcard
            .evaluate_join(&left_tuple, &schema, &right_tuple, &schema)
            .unwrap();

        // Check that we got a vector value back
        assert_eq!(result.get_type_id(), TypeId::Vector);

        // Extract the vector and verify contents - should contain both tuples' values
        if let Val::Vector(result_values) = result.get_val() {
            assert_eq!(result_values.len(), 6); // 3 from left + 3 from right

            // Check left tuple values
            assert_eq!(result_values[0], left_values[0]);
            assert_eq!(result_values[1], left_values[1]);
            assert_eq!(result_values[2], left_values[2]);

            // Check right tuple values
            assert_eq!(result_values[3], right_values[0]);
            assert_eq!(result_values[4], right_values[1]);
            assert_eq!(result_values[5], right_values[2]);
        } else {
            panic!("Expected Vector value");
        }
    }

    #[test]
    fn test_wildcard_validate() {
        let schema = create_test_schema();
        let wildcard = WildcardExpression::new(Column::new("*", TypeId::Vector));

        // Valid schema should pass validation
        assert!(wildcard.validate(&schema).is_ok());

        // Empty schema should fail validation
        let empty_schema = Schema::new(vec![]);
        assert!(matches!(
            wildcard.validate(&empty_schema),
            Err(ExpressionError::EvaluationError(_))
        ));
    }

    #[test]
    fn test_wildcard_children() {
        let wildcard = WildcardExpression::new(Column::new("*", TypeId::Vector));

        // Should have no children
        assert!(wildcard.get_children().is_empty());

        // Should panic when trying to get child
        assert!(
            std::panic::catch_unwind(|| {
                wildcard.get_child_at(0);
            })
            .is_err()
        );

        // Should panic when trying to clone with children
        assert!(
            std::panic::catch_unwind(|| {
                wildcard
                    .clone_with_children(vec![Arc::new(Expression::Wildcard(wildcard.clone()))]);
            })
            .is_err()
        );

        // Should succeed when cloning with empty children vec
        assert!(
            std::panic::catch_unwind(|| {
                wildcard.clone_with_children(vec![]);
            })
            .is_ok()
        );
    }

    #[test]
    fn test_wildcard_display() {
        let wildcard = WildcardExpression::new(Column::new("*", TypeId::Vector));
        assert_eq!(wildcard.to_string(), "*");
    }

    #[test]
    fn test_wildcard_empty_tuple() {
        let schema = create_test_schema();
        // Create a tuple with NULL values for each column
        let null_values = vec![
            Value::new(Val::Null),
            Value::new(Val::Null),
            Value::new(Val::Null),
        ];
        let empty_tuple = create_test_tuple(null_values, &schema);
        let wildcard = WildcardExpression::new(Column::new("*", TypeId::Vector));

        let result = wildcard.evaluate(&empty_tuple, &schema).unwrap();

        // Should return a vector with NULL values
        if let Val::Vector(values) = result.get_val() {
            assert_eq!(values.len(), 3);
            for value in values {
                assert!(value.is_null());
            }
        } else {
            panic!("Expected Vector value");
        }
    }
}
