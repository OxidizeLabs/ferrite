use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub enum IntervalField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IntervalExpression {
    field: IntervalField,
    children: Vec<Arc<Expression>>, // Store value expression in children vector
    return_type: Column,
}

impl IntervalExpression {
    pub fn new(field: IntervalField, value: Arc<Expression>, return_type: Column) -> Self {
        Self {
            field,
            children: vec![value], // Store value in children vector
            return_type,
        }
    }

    // Helper to get the value expression
    fn value(&self) -> &Arc<Expression> {
        &self.children[0]
    }
}

impl ExpressionOps for IntervalExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let value = self.value().evaluate(tuple, schema)?;

        // Convert value to numeric type if needed
        let numeric_value = match value.get_val() {
            Val::Integer(i) => *i as i64,
            Val::BigInt(i) => *i,
            Val::SmallInt(i) => *i as i64,
            Val::TinyInt(i) => *i as i64,
            Val::VarLen(s) | Val::ConstLen(s) => {
                // Parse string as number
                s.parse::<i64>().map_err(|_| {
                    ExpressionError::InvalidOperation(format!(
                        "Cannot parse '{}' as number for interval",
                        s
                    ))
                })?
            }
            _ => {
                return Err(ExpressionError::InvalidOperation(format!(
                    "Invalid value type {:?} for interval",
                    value.get_type_id()
                )));
            }
        };

        // Create a struct value with the interval field and amount
        let field_name = match self.field {
            IntervalField::Year => "years",
            IntervalField::Month => "months",
            IntervalField::Day => "days",
            IntervalField::Hour => "hours",
            IntervalField::Minute => "minutes",
            IntervalField::Second => "seconds",
        };

        // Create struct with field name and value
        Ok(Value::new_struct(
            vec![field_name],
            vec![Value::new(numeric_value)],
        ))
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        _right_tuple: &Tuple,
        _right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        // For join evaluation, we can just evaluate using the left tuple and schema
        // since interval expressions don't depend on join context
        self.evaluate(left_tuple, left_schema)
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        &self.children[child_idx] // Use children vector directly
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children // Return reference to children vector
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        assert_eq!(
            children.len(),
            1,
            "IntervalExpression requires exactly one child"
        );
        Arc::new(Expression::Interval(IntervalExpression::new(
            self.field.clone(),
            children[0].clone(),
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate the value expression
        self.value().validate(schema)?;

        // Validate that value expression returns a numeric or string type
        let value_type = self.value().get_return_type().get_type();
        match value_type {
            TypeId::Integer
            | TypeId::BigInt
            | TypeId::SmallInt
            | TypeId::TinyInt
            | TypeId::VarChar
            | TypeId::Char => Ok(()),
            _ => Err(ExpressionError::InvalidOperation(format!(
                "Interval value must be numeric or string type, got {:?}",
                value_type
            ))),
        }
    }
}

impl Display for IntervalExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "INTERVAL {} {:?}", self.value(), self.field)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::schema::Schema;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::literal_value_expression::LiteralValueExpression;
    use crate::storage::table::tuple::Tuple;
    use sqlparser::ast::Value as SQLValue;

    fn create_test_schema() -> Schema {
        Schema::new(vec![])
    }

    fn create_empty_tuple(schema: &Schema) -> Tuple {
        Tuple::new(&[], schema, RID::new(0, 0))
    }

    #[test]
    fn test_interval_with_integer() {
        let schema = create_test_schema();
        let tuple = create_empty_tuple(&schema);

        // Create interval expression with integer value
        let value_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(SQLValue::Number("5".to_string(), false)).unwrap(),
        ));
        let interval = IntervalExpression::new(
            IntervalField::Year,
            value_expr,
            Column::new("interval", TypeId::Struct),
        );

        // Evaluate interval
        let result = interval.evaluate(&tuple, &schema).unwrap();

        // Verify result
        assert_eq!(result.get_type_id(), TypeId::Struct);
        assert_eq!(result.get_struct_field_names(), vec!["years"]);
        assert_eq!(
            result.get_struct_field("years").unwrap().get_val(),
            &Val::BigInt(5)
        );
    }

    #[test]
    fn test_interval_with_string() {
        let schema = create_test_schema();
        let tuple = create_empty_tuple(&schema);

        // Create interval expression with string value
        let value_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(SQLValue::SingleQuotedString("10".to_string())).unwrap(),
        ));
        let interval = IntervalExpression::new(
            IntervalField::Month,
            value_expr,
            Column::new("interval", TypeId::Struct),
        );

        // Evaluate interval
        let result = interval.evaluate(&tuple, &schema).unwrap();

        // Verify result
        assert_eq!(result.get_type_id(), TypeId::Struct);
        assert_eq!(result.get_struct_field_names(), vec!["months"]);
        assert_eq!(
            result.get_struct_field("months").unwrap().get_val(),
            &Val::BigInt(10)
        );
    }

    #[test]
    fn test_interval_with_invalid_string() {
        let schema = create_test_schema();
        let tuple = create_empty_tuple(&schema);

        // Create interval expression with invalid string value
        let value_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(SQLValue::SingleQuotedString("invalid".to_string()))
                .unwrap(),
        ));
        let interval = IntervalExpression::new(
            IntervalField::Day,
            value_expr,
            Column::new("interval", TypeId::Struct),
        );

        // Evaluate interval - should fail
        let result = interval.evaluate(&tuple, &schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cannot parse"));
    }

    #[test]
    fn test_interval_with_invalid_type() {
        let schema = create_test_schema();
        let tuple = create_empty_tuple(&schema);

        // Create interval expression with boolean value (invalid type)
        let value_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(SQLValue::Boolean(true)).unwrap(),
        ));
        let interval = IntervalExpression::new(
            IntervalField::Hour,
            value_expr,
            Column::new("interval", TypeId::Struct),
        );

        // Evaluate interval - should fail
        let result = interval.evaluate(&tuple, &schema);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid value type")
        );
    }

    #[test]
    fn test_interval_display() {
        let value_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(SQLValue::Number("5".to_string(), false)).unwrap(),
        ));
        let interval = IntervalExpression::new(
            IntervalField::Minute,
            value_expr,
            Column::new("interval", TypeId::Struct),
        );

        assert_eq!(interval.to_string(), "INTERVAL 5 Minute");
    }

    #[test]
    fn test_interval_validation() {
        let schema = create_test_schema();

        // Test with valid numeric type
        let value_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(SQLValue::Number("5".to_string(), false)).unwrap(),
        ));
        let interval = IntervalExpression::new(
            IntervalField::Second,
            value_expr,
            Column::new("interval", TypeId::Struct),
        );
        assert!(interval.validate(&schema).is_ok());

        // Test with valid string type
        let value_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(SQLValue::SingleQuotedString("5".to_string())).unwrap(),
        ));
        let interval = IntervalExpression::new(
            IntervalField::Second,
            value_expr,
            Column::new("interval", TypeId::Struct),
        );
        assert!(interval.validate(&schema).is_ok());

        // Test with invalid type (boolean)
        let value_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(SQLValue::Boolean(true)).unwrap(),
        ));
        let interval = IntervalExpression::new(
            IntervalField::Second,
            value_expr,
            Column::new("interval", TypeId::Struct),
        );
        assert!(interval.validate(&schema).is_err());
    }

    #[test]
    fn test_interval_clone_with_children() {
        let original_value = Arc::new(Expression::Literal(
            LiteralValueExpression::new(SQLValue::Number("5".to_string(), false)).unwrap(),
        ));
        let interval = IntervalExpression::new(
            IntervalField::Year,
            original_value.clone(),
            Column::new("interval", TypeId::Struct),
        );

        let new_value = Arc::new(Expression::Literal(
            LiteralValueExpression::new(SQLValue::Number("10".to_string(), false)).unwrap(),
        ));
        let cloned = interval.clone_with_children(vec![new_value]);

        match cloned.as_ref() {
            Expression::Interval(i) => {
                assert_eq!(i.field, IntervalField::Year);
                match i.value().as_ref() {
                    Expression::Literal(l) => {
                        assert_eq!(l.get_value().get_val(), &Val::TinyInt(10));
                    }
                    _ => panic!("Expected Literal expression"),
                }
            }
            _ => panic!("Expected Interval expression"),
        }
    }

    #[test]
    #[should_panic(expected = "IntervalExpression requires exactly one child")]
    fn test_interval_clone_with_wrong_children_count() {
        let original_value = Arc::new(Expression::Literal(
            LiteralValueExpression::new(SQLValue::Number("5".to_string(), false)).unwrap(),
        ));
        let interval = IntervalExpression::new(
            IntervalField::Year,
            original_value.clone(),
            Column::new("interval", TypeId::Struct),
        );

        // Try to clone with wrong number of children
        interval.clone_with_children(vec![]);
    }
}
