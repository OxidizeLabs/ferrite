use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use log::trace;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRefExpression {
    tuple_index: usize,
    column_index: usize,
    ret_type: Column,
    children: Vec<Arc<Expression>>,
}

impl ColumnRefExpression {
    pub fn new(
        tuple_index: usize,
        column_index: usize,
        ret_type: Column,
        children: Vec<Arc<Expression>>,
    ) -> Self {
        Self {
            tuple_index,
            column_index,
            ret_type,
            children,
        }
    }

    pub fn get_column_index(&self) -> usize {
        self.column_index
    }

    pub fn get_return_type(&self) -> &Column {
        &self.ret_type
    }

    pub fn get_tuple_index(&self) -> usize {
        self.tuple_index
    }
}

impl ExpressionOps for ColumnRefExpression {
    fn evaluate(&self, tuple: &Tuple, _schema: &Schema) -> Result<Value, ExpressionError> {
        // Get the values from the tuple
        let values = tuple.get_values();

        // Check if the column index is within bounds
        if self.column_index >= values.len() {
            return Err(ExpressionError::InvalidColumnIndex(self.column_index));
        }

        Ok(values[self.column_index].clone())
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        trace!(
            "ColumnRef evaluate_join - tuple_index: {}, column_index: {}",
            self.tuple_index, self.column_index
        );
        trace!(
            "Left tuple: {:?}, Right tuple: {:?}",
            left_tuple.get_values(),
            right_tuple.get_values()
        );

        // Get the appropriate tuple and schema based on tuple_index
        let (tuple, _schema, actual_column_index) = match self.tuple_index {
            0 => {
                // Left tuple - column_index should be within left schema bounds
                (left_tuple, left_schema, self.column_index)
            },
            1 => {
                // Right tuple - need to map combined schema index to right tuple index
                let left_column_count = left_schema.get_column_count() as usize;
                let actual_index = if self.column_index >= left_column_count {
                    // This is a right table column, map it to right tuple index
                    self.column_index - left_column_count
                } else {
                    // This shouldn't happen for tuple_index 1, but handle gracefully
                    self.column_index
                };
                (right_tuple, right_schema, actual_index)
            },
            idx => return Err(ExpressionError::InvalidTupleIndex(idx)),
        };

        // Get the value from the tuple using the actual column index
        let values = tuple.get_values();
        if actual_column_index >= values.len() {
            return Err(ExpressionError::InvalidColumnIndex(actual_column_index));
        }

        trace!(
            "Selected tuple values: {:?}, using actual_column_index: {} (original: {})",
            values, actual_column_index, self.column_index
        );
        Ok(values[actual_column_index].clone())
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        &self.children[child_idx]
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.ret_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        Arc::new(Expression::ColumnRef(ColumnRefExpression {
            tuple_index: self.tuple_index,
            column_index: self.column_index,
            ret_type: self.ret_type.clone(),
            children,
        }))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Check if the column index is within bounds of the schema
        trace!(
            "ColumnRefExpression::validate - column_index: {}, schema columns: {}",
            self.column_index,
            schema.get_column_count()
        );

        if self.column_index >= schema.get_column_count() as usize {
            let error = ExpressionError::InvalidColumnIndex(self.column_index);
            trace!(
                "ColumnRefExpression::validate - column index out of bounds, returning: {:?}",
                error
            );
            return Err(error);
        }

        // Get the column from schema and verify type matches
        let schema_column = schema.get_column(self.column_index).ok_or_else(|| {
            let error = ExpressionError::InvalidColumnIndex(self.column_index);
            trace!(
                "ColumnRefExpression::validate - column not found, returning: {:?}",
                error
            );
            error
        })?;

        if schema_column.get_type() != self.ret_type.get_type() {
            let error = ExpressionError::TypeMismatch {
                expected: self.ret_type.get_type(),
                actual: schema_column.get_type(),
            };
            trace!(
                "ColumnRefExpression::validate - type mismatch, returning: {:?}",
                error
            );
            return Err(error);
        }

        trace!("ColumnRefExpression::validate - validation successful");
        Ok(())
    }
}

impl Display for ColumnRefExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            // Detailed format: #0.1
            write!(
                f,
                "Col#{}.{}",
                self.get_tuple_index(),
                self.get_column_index()
            )
        } else {
            // Basic format: just the column name
            write!(f, "{}", self.ret_type.get_name())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;

    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    // Helper functions
    fn create_test_schema() -> Schema {
        let columns = vec![
            Column::new("col1", TypeId::Integer),
            Column::new("col2", TypeId::VarChar),
            Column::new("col3", TypeId::Boolean),
            Column::new("col4", TypeId::Decimal),
        ];
        Schema::new(columns)
    }

    fn create_test_tuple() -> Tuple {
        let columns = vec![
            Column::new("col1", TypeId::Integer),
            Column::new("col2", TypeId::VarChar),
            Column::new("col3", TypeId::Boolean),
            Column::new("col4", TypeId::Decimal),
        ];
        Tuple::new(
            &[
                Value::new(42),
                Value::new("test".to_string()),
                Value::new(true),
                Value::new(std::f64::consts::PI),
            ],
            &Schema::new(columns),
            RID::new(0, 0),
        )
    }

    mod construction {
        use super::*;

        #[test]
        fn test_new_column_ref() {
            let expr = ColumnRefExpression::new(0, 1, Column::new("test", TypeId::VarChar), vec![]);

            assert_eq!(expr.get_tuple_index(), 0);
            assert_eq!(expr.get_column_index(), 1);
            assert_eq!(expr.get_return_type().get_name(), "test");
            assert!(expr.get_children().is_empty());
        }
    }

    mod evaluation {
        use super::*;

        #[test]
        fn test_evaluate_basic() {
            let schema = create_test_schema();
            let tuple = create_test_tuple();

            // Test integer column
            let expr = ColumnRefExpression::new(0, 0, Column::new("col1", TypeId::Integer), vec![]);
            assert_eq!(expr.evaluate(&tuple, &schema).unwrap(), Value::new(42));

            // Test varchar column
            let expr = ColumnRefExpression::new(0, 1, Column::new("col2", TypeId::VarChar), vec![]);
            assert_eq!(
                expr.evaluate(&tuple, &schema).unwrap(),
                Value::new("test".to_string())
            );

            // Test boolean column
            let expr = ColumnRefExpression::new(0, 2, Column::new("col3", TypeId::Boolean), vec![]);
            assert_eq!(expr.evaluate(&tuple, &schema).unwrap(), Value::new(true));
        }

        #[test]
        fn test_evaluate_join() {
            let left_schema = create_test_schema();
            let right_schema = create_test_schema();

            let left_tuple = Tuple::new(
                &[
                    Value::new(1),
                    Value::new("left".to_string()),
                    Value::new(true),
                    Value::new(1.1),
                ],
                &left_schema,
                Default::default(),
            );

            let right_tuple = Tuple::new(
                &[
                    Value::new(2),
                    Value::new("right".to_string()),
                    Value::new(false),
                    Value::new(2.2),
                ],
                &right_schema,
                Default::default(),
            );

            // Test left tuple access
            let expr = ColumnRefExpression::new(0, 0, Column::new("col1", TypeId::Integer), vec![]);
            assert_eq!(
                expr.evaluate_join(&left_tuple, &left_schema, &right_tuple, &right_schema)
                    .unwrap(),
                Value::new(1)
            );

            // Test right tuple access
            let expr = ColumnRefExpression::new(1, 1, Column::new("col2", TypeId::VarChar), vec![]);
            assert_eq!(
                expr.evaluate_join(&left_tuple, &left_schema, &right_tuple, &right_schema)
                    .unwrap(),
                Value::new("right".to_string())
            );
        }
    }

    mod validation {
        use super::*;

        #[test]
        fn test_validate_success() {
            let schema = create_test_schema();
            let expr = ColumnRefExpression::new(0, 0, Column::new("col1", TypeId::Integer), vec![]);
            assert!(expr.validate(&schema).is_ok());
        }

        #[test]
        fn test_validate_invalid_column_index() {
            let schema = create_test_schema();
            let expr =
                ColumnRefExpression::new(0, 99, Column::new("invalid", TypeId::Integer), vec![]);
            assert!(matches!(
                expr.validate(&schema),
                Err(ExpressionError::InvalidColumnIndex(_))
            ));
        }

        #[test]
        fn test_validate_type_mismatch() {
            let schema = create_test_schema();
            let expr = ColumnRefExpression::new(
                0,
                0,
                Column::new("col1", TypeId::VarChar), // Wrong type for col1
                vec![],
            );

            let result = expr.validate(&schema);
            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(matches!(
                err,
                ExpressionError::TypeMismatch {
                    expected: TypeId::VarChar,
                    actual: TypeId::Integer
                }
            ));
        }
    }

    mod error_handling {
        use super::*;

        #[test]
        fn test_invalid_join_tuple_index() {
            let schema = create_test_schema();
            let tuple = create_test_tuple();

            let expr = ColumnRefExpression::new(
                2, // Invalid tuple index
                0,
                Column::new("col1", TypeId::Integer),
                vec![],
            );

            let result = expr.evaluate_join(&tuple, &schema, &tuple, &schema);
            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(matches!(err, ExpressionError::InvalidTupleIndex(2)));
        }

        #[test]
        fn test_invalid_column_index_in_join() {
            let schema = create_test_schema();
            let tuple = create_test_tuple();

            let expr = ColumnRefExpression::new(
                0,
                99, // Invalid column index
                Column::new("col1", TypeId::Integer),
                vec![],
            );

            let result = expr.evaluate_join(&tuple, &schema, &tuple, &schema);
            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(matches!(err, ExpressionError::InvalidColumnIndex(99)));
        }
    }

    mod display {
        use super::*;

        #[test]
        fn test_display_formatting() {
            let expr =
                ColumnRefExpression::new(1, 2, Column::new("test_col", TypeId::Integer), vec![]);

            // Test normal display (column name)
            assert_eq!(format!("{}", expr), "test_col");

            // Test alternate display (tuple.column format)
            assert_eq!(format!("{:#}", expr), "Col#1.2");
        }
    }
}
