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
pub struct StructField {
    pub name: String,
    pub type_info: Column,
}

impl StructField {
    pub fn new(name: String, type_info: Column) -> Self {
        Self { name, type_info }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StructExpression {
    values: Vec<Arc<Expression>>,
    fields: Vec<StructField>,
    return_type: Column,
    children: Vec<Arc<Expression>>,
}

impl StructExpression {
    pub fn new(
        values: Vec<Arc<Expression>>,
        fields: Vec<StructField>,
        return_type: Column,
    ) -> Self {
        Self {
            values: values.clone(),
            fields,
            return_type,
            children: values,
        }
    }
}

impl ExpressionOps for StructExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let mut field_values = Vec::new();

        // Evaluate each expression to get the field values
        for value in &self.values {
            field_values.push(value.evaluate(tuple, schema)?);
        }

        // Create a vector of values for the struct
        let struct_value = Value::new_vector(field_values);

        Ok(struct_value)
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        let mut field_values = Vec::new();

        // Evaluate each expression in the join context
        for value in &self.values {
            field_values.push(value.evaluate_join(
                left_tuple,
                left_schema,
                right_tuple,
                right_schema,
            )?);
        }

        // Create a vector of values for the struct
        let struct_value = Value::new_vector(field_values);

        Ok(struct_value)
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
        Arc::new(Expression::Struct(StructExpression::new(
            children,
            self.fields.clone(),
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate that the number of values matches the number of fields
        if self.values.len() != self.fields.len() {
            return Err(ExpressionError::InvalidOperation(format!(
                "Number of values ({}) does not match number of fields ({})",
                self.values.len(),
                self.fields.len()
            )));
        }

        // Validate each child expression
        for (i, value) in self.values.iter().enumerate() {
            value.validate(schema)?;

            // Optionally, validate that the value type matches the field type
            let value_type = value.get_return_type().get_type();
            let field_type = self.fields[i].type_info.get_type();

            if value_type != field_type && value_type != TypeId::Invalid {
                // Use the correct format for the TypeMismatch error
                return Err(ExpressionError::InvalidOperation(format!(
                    "Field '{}' expects type {:?} but got {:?}",
                    self.fields[i].name, field_type, value_type
                )));
            }
        }

        Ok(())
    }
}

impl Display for StructExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "STRUCT<{}>({})",
            self.fields
                .iter()
                .map(|f| format!("{}: {}", f.name, f.type_info))
                .collect::<Vec<_>>()
                .join(", "),
            self.values
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Val;

    fn create_test_schema() -> Schema {
        let columns = vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
            Column::new("salary", TypeId::Decimal),
        ];
        Schema::new(columns)
    }

    fn create_test_tuple() -> (Tuple, Schema) {
        let schema = create_test_schema();
        let values = vec![
            Value::new(1),          // id
            Value::new("John Doe"), // name
            Value::new(30),         // age
            Value::new(50000.0),    // salary
        ];
        let tuple = Tuple::new(&values, schema.clone(), RID::new(1, 1));
        (tuple, schema)
    }

    #[test]
    fn test_struct_expression_creation() {
        // Create field definitions
        let fields = vec![
            StructField::new("id".to_string(), Column::new("id", TypeId::Integer)),
            StructField::new("name".to_string(), Column::new("name", TypeId::VarChar)),
        ];

        // Create constant expressions for values
        let id_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let name_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("John"),
            Column::new("name", TypeId::VarChar),
            vec![],
        )));

        // Create struct expression
        let struct_expr = StructExpression::new(
            vec![id_expr, name_expr],
            fields,
            Column::new("person", TypeId::Vector),
        );

        // Verify struct expression properties
        assert_eq!(struct_expr.get_children().len(), 2);
        assert_eq!(struct_expr.get_return_type().get_type(), TypeId::Vector);
        assert_eq!(struct_expr.fields.len(), 2);
        assert_eq!(struct_expr.fields[0].name, "id");
        assert_eq!(struct_expr.fields[1].name, "name");
    }

    #[test]
    fn test_struct_expression_evaluation() {
        let (tuple, schema) = create_test_tuple();

        // Create field definitions
        let fields = vec![
            StructField::new("id".to_string(), Column::new("id", TypeId::Integer)),
            StructField::new("name".to_string(), Column::new("name", TypeId::VarChar)),
        ];

        // Create column reference expressions
        let id_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let name_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("name", TypeId::VarChar),
            vec![],
        )));

        // Create struct expression
        let struct_expr = StructExpression::new(
            vec![id_expr, name_expr],
            fields,
            Column::new("person", TypeId::Vector),
        );

        // Evaluate the struct expression
        let result = struct_expr.evaluate(&tuple, &schema).unwrap();

        // Verify the result is a vector
        if let Val::Vector(values) = result.get_val() {
            assert_eq!(values.len(), 2);

            // Check first value (id)
            if let Val::Integer(id) = values[0].get_val() {
                assert_eq!(*id, 1);
            } else {
                panic!("Expected Integer value for id");
            }

            // Check second value (name)
            if let Val::VarLen(name) = values[1].get_val() {
                assert_eq!(name, "John Doe");
            } else {
                panic!("Expected VarLen value for name");
            }
        } else {
            panic!("Expected Vector result");
        }
    }

    #[test]
    fn test_struct_expression_validation() {
        let schema = create_test_schema();

        // Create field definitions
        let fields = vec![
            StructField::new("id".to_string(), Column::new("id", TypeId::Integer)),
            StructField::new("name".to_string(), Column::new("name", TypeId::VarChar)),
        ];

        // Create column reference expressions
        let id_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let name_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("name", TypeId::VarChar),
            vec![],
        )));

        // Create struct expression with matching types
        let valid_struct_expr = StructExpression::new(
            vec![id_expr, name_expr],
            fields.clone(),
            Column::new("person", TypeId::Vector),
        );

        // Validation should succeed
        assert!(valid_struct_expr.validate(&schema).is_ok());

        // Create struct expression with mismatched types
        let id_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        // Using salary (Decimal) for a VarChar field
        let salary_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            3,
            Column::new("salary", TypeId::Decimal),
            vec![],
        )));

        let invalid_struct_expr = StructExpression::new(
            vec![id_expr, salary_expr],
            fields,
            Column::new("person", TypeId::Vector),
        );

        // Validation should fail due to type mismatch
        assert!(invalid_struct_expr.validate(&schema).is_err());
    }

    #[test]
    fn test_struct_expression_with_constants() {
        let (tuple, schema) = create_test_tuple();

        // Create field definitions
        let fields = vec![
            StructField::new("id".to_string(), Column::new("id", TypeId::Integer)),
            StructField::new("name".to_string(), Column::new("name", TypeId::VarChar)),
            StructField::new("active".to_string(), Column::new("active", TypeId::Boolean)),
        ];

        // Create expressions with a mix of column references and constants
        let id_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let name_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("name", TypeId::VarChar),
            vec![],
        )));

        let active_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(true),
            Column::new("active", TypeId::Boolean),
            vec![],
        )));

        // Create struct expression
        let struct_expr = StructExpression::new(
            vec![id_expr, name_expr, active_expr],
            fields,
            Column::new("person", TypeId::Vector),
        );

        // Evaluate the struct expression
        let result = struct_expr.evaluate(&tuple, &schema).unwrap();

        // Verify the result
        if let Val::Vector(values) = result.get_val() {
            assert_eq!(values.len(), 3);

            // Check constant value (active)
            if let Val::Boolean(active) = values[2].get_val() {
                assert_eq!(*active, true);
            } else {
                panic!("Expected Boolean value for active");
            }
        } else {
            panic!("Expected Vector result");
        }
    }

    #[test]
    fn test_struct_expression_display() {
        // Create field definitions
        let fields = vec![
            StructField::new("id".to_string(), Column::new("id", TypeId::Integer)),
            StructField::new("name".to_string(), Column::new("name", TypeId::VarChar)),
        ];

        // Create constant expressions for values
        let id_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let name_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("John"),
            Column::new("name", TypeId::VarChar),
            vec![],
        )));

        // Create struct expression
        let struct_expr = StructExpression::new(
            vec![id_expr, name_expr],
            fields,
            Column::new("person", TypeId::Vector),
        );

        // Test the Display implementation
        let display_str = struct_expr.to_string();
        assert!(display_str.contains("STRUCT<"));
        assert!(display_str.contains("id: id"));
        assert!(display_str.contains("name: name"));
        assert!(display_str.contains("1"));
        assert!(display_str.contains("John"));
    }

    #[test]
    fn test_struct_expression_clone_with_children() {
        // Create field definitions
        let fields = vec![
            StructField::new("id".to_string(), Column::new("id", TypeId::Integer)),
            StructField::new("name".to_string(), Column::new("name", TypeId::VarChar)),
        ];

        // Create constant expressions for values
        let id_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let name_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("John"),
            Column::new("name", TypeId::VarChar),
            vec![],
        )));

        // Create struct expression
        let struct_expr = StructExpression::new(
            vec![id_expr.clone(), name_expr.clone()],
            fields,
            Column::new("person", TypeId::Vector),
        );

        // Create new expressions for cloning
        let new_id_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(2),
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let new_name_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("Jane"),
            Column::new("name", TypeId::VarChar),
            vec![],
        )));

        // Clone with new children
        let cloned_expr = struct_expr.clone_with_children(vec![new_id_expr, new_name_expr]);

        // Verify the cloned expression
        if let Expression::Struct(cloned_struct) = cloned_expr.as_ref() {
            assert_eq!(cloned_struct.fields.len(), 2);
            assert_eq!(cloned_struct.fields[0].name, "id");
            assert_eq!(cloned_struct.fields[1].name, "name");
            assert_eq!(cloned_struct.get_children().len(), 2);
        } else {
            panic!("Expected Struct expression");
        }
    }
}
