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
pub struct QualifiedWildcardExpression {
    qualifier: Vec<String>, // e.g., ["schema", "table"] for schema.table.*
    return_type: Column,
    children: Vec<Arc<Expression>>,
}

impl QualifiedWildcardExpression {
    pub fn new(qualifier: Vec<String>, return_type: Column) -> Self {
        Self {
            qualifier,
            return_type,
            children: Vec::new(),
        }
    }
}

impl ExpressionOps for QualifiedWildcardExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let mut values = Vec::new();
        let prefix = format!("{}.", self.qualifier.join("."));

        for i in 0..schema.get_column_count() {
            let column = schema.get_column(i as usize).ok_or_else(|| {
                ExpressionError::InvalidColumnReference(format!(
                    "Failed to get column at index {}",
                    i
                ))
            })?;

            if column.get_name().starts_with(&prefix) {
                let value = tuple.get_value(i as usize);
                values.push(value.clone());
            }
        }

        Ok(Value::new_vector(values))
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        let mut values = Vec::new();
        let prefix = format!("{}.", self.qualifier.join("."));

        // Check left schema
        for i in 0..left_schema.get_column_count() {
            let column = left_schema.get_column(i as usize).ok_or_else(|| {
                ExpressionError::InvalidColumnReference(format!(
                    "Failed to get column at index {} in left schema",
                    i
                ))
            })?;

            if column.get_name().starts_with(&prefix) {
                let value = left_tuple.get_value(i as usize);
                values.push(value.clone());
            }
        }

        // Check right schema
        for i in 0..right_schema.get_column_count() {
            let column = right_schema.get_column(i as usize).ok_or_else(|| {
                ExpressionError::InvalidColumnReference(format!(
                    "Failed to get column at index {} in right schema",
                    i
                ))
            })?;

            if column.get_name().starts_with(&prefix) {
                let value = right_tuple.get_value(i as usize);
                values.push(value.clone());
            }
        }

        Ok(Value::new_vector(values))
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
        Arc::new(Expression::QualifiedWildcard(QualifiedWildcardExpression {
            qualifier: self.qualifier.clone(),
            return_type: self.return_type.clone(),
            children,
        }))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Build the qualifier prefix
        let prefix = format!("{}.", self.qualifier.join("."));

        // Check if there's at least one column in the schema that matches this qualifier
        let mut found_match = false;
        for i in 0..schema.get_column_count() {
            if let Some(column) = schema.get_column(i as usize)
                && column.get_name().starts_with(&prefix)
            {
                found_match = true;
                break;
            }
        }

        if !found_match {
            return Err(ExpressionError::InvalidColumnReference(format!(
                "No columns found matching qualifier '{}'",
                prefix
            )));
        }

        Ok(())
    }
}

impl Display for QualifiedWildcardExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}.*", self.qualifier.join("."))
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
            Column::new("test.id", TypeId::Integer),
            Column::new("test.name", TypeId::VarChar),
            Column::new("test.age", TypeId::Integer),
            Column::new("other.id", TypeId::Integer),
            Column::new("other.data", TypeId::VarChar),
        ])
    }

    fn create_test_tuple() -> Tuple {
        let schema = create_test_schema();
        let values = vec![
            Value::new(1),      // test.id
            Value::new("John"), // test.name
            Value::new(25),     // test.age
            Value::new(2),      // other.id
            Value::new("data"), // other.data
        ];
        Tuple::new(&values, &schema, RID::new(0, 0))
    }

    #[test]
    fn test_evaluate_basic() {
        let schema = create_test_schema();
        let tuple = create_test_tuple();

        let expr = QualifiedWildcardExpression::new(
            vec!["test".to_string()],
            Column::new("test.*", TypeId::Vector),
        );

        let result = expr.evaluate(&tuple, &schema).unwrap();

        // Should get a vector with test.id, test.name, and test.age
        match result.get_val() {
            Val::Vector(values) => {
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], Value::new(1)); // test.id
                assert_eq!(values[1], Value::new("John")); // test.name
                assert_eq!(values[2], Value::new(25)); // test.age
            },
            _ => panic!("Expected Vector value"),
        }
    }

    #[test]
    fn test_evaluate_join() {
        let left_schema = Schema::new(vec![
            Column::new("left.id", TypeId::Integer),
            Column::new("left.name", TypeId::VarChar),
        ]);
        let right_schema = Schema::new(vec![
            Column::new("left.age", TypeId::Integer),
            Column::new("right.data", TypeId::VarChar),
        ]);

        let left_tuple = Tuple::new(
            &[Value::new(1), Value::new("John")],
            &left_schema,
            RID::new(0, 0),
        );
        let right_tuple = Tuple::new(
            &[Value::new(25), Value::new("data")],
            &right_schema,
            RID::new(0, 0),
        );

        let expr = QualifiedWildcardExpression::new(
            vec!["left".to_string()],
            Column::new("left.*", TypeId::Vector),
        );

        let result = expr
            .evaluate_join(&left_tuple, &left_schema, &right_tuple, &right_schema)
            .unwrap();

        // Should get a vector with left.id, left.name, and left.age
        match result.get_val() {
            Val::Vector(values) => {
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], Value::new(1)); // left.id
                assert_eq!(values[1], Value::new("John")); // left.name
                assert_eq!(values[2], Value::new(25)); // left.age
            },
            _ => panic!("Expected Vector value"),
        }
    }

    #[test]
    fn test_validate() {
        let schema = create_test_schema();

        // Valid qualifier
        let expr = QualifiedWildcardExpression::new(
            vec!["test".to_string()],
            Column::new("test.*", TypeId::Vector),
        );
        assert!(expr.validate(&schema).is_ok());

        // Invalid qualifier
        let invalid_expr = QualifiedWildcardExpression::new(
            vec!["invalid".to_string()],
            Column::new("invalid.*", TypeId::Vector),
        );
        assert!(invalid_expr.validate(&schema).is_err());
    }

    #[test]
    fn test_display() {
        let expr = QualifiedWildcardExpression::new(
            vec!["schema".to_string(), "table".to_string()],
            Column::new("schema.table.*", TypeId::Vector),
        );
        assert_eq!(expr.to_string(), "schema.table.*");
    }

    #[test]
    fn test_clone_with_children() {
        let expr = QualifiedWildcardExpression::new(
            vec!["test".to_string()],
            Column::new("test.*", TypeId::Vector),
        );

        let mock_child = Arc::new(Expression::Mock(
            crate::sql::execution::expressions::mock_expression::MockExpression::new(
                "test".to_string(),
                TypeId::Integer,
            ),
        ));

        let cloned = expr.clone_with_children(vec![mock_child]);

        match cloned.as_ref() {
            Expression::QualifiedWildcard(qw) => {
                assert_eq!(qw.qualifier, vec!["test".to_string()]);
                assert_eq!(qw.get_children().len(), 1);
            },
            _ => panic!("Expected QualifiedWildcard expression"),
        }
    }

    #[test]
    fn test_empty_qualifier() {
        let schema = create_test_schema();

        let expr = QualifiedWildcardExpression::new(vec![], Column::new("*", TypeId::Vector));

        // Empty qualifier should not match any columns
        assert!(expr.validate(&schema).is_err());
    }

    #[test]
    fn test_multi_level_qualifier() {
        let schema = Schema::new(vec![
            Column::new("db.schema.table.id", TypeId::Integer),
            Column::new("db.schema.table.name", TypeId::VarChar),
            Column::new("db.other.table.id", TypeId::Integer),
        ]);

        let expr = QualifiedWildcardExpression::new(
            vec!["db".to_string(), "schema".to_string(), "table".to_string()],
            Column::new("db.schema.table.*", TypeId::Vector),
        );

        assert!(expr.validate(&schema).is_ok());

        let tuple = Tuple::new(
            &vec![Value::new(1), Value::new("test"), Value::new(2)],
            &schema,
            RID::new(0, 0),
        );

        let result = expr.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::Vector(values) => {
                assert_eq!(values.len(), 2); // Should only get db.schema.table.* columns
                assert_eq!(values[0], Value::new(1));
                assert_eq!(values[1], Value::new("test"));
            },
            _ => panic!("Expected Vector value"),
        }
    }
}
