use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use chrono::{Datelike, NaiveDateTime, Timelike};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub enum ExtractField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Timezone,
    Quarter,
    Week,
    DayOfWeek,
    DayOfYear,
    Epoch,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExtractExpression {
    field: ExtractField,
    expr: Arc<Expression>,
    return_type: Column,
    children: Vec<Arc<Expression>>,
}

impl ExtractExpression {
    pub fn new(field: ExtractField, expr: Arc<Expression>, return_type: Column) -> Self {
        Self {
            field,
            expr: expr.clone(),
            return_type,
            children: vec![expr],
        }
    }

    fn extract_from_timestamp(&self, timestamp_str: &str) -> Result<i32, ExpressionError> {
        // Parse the timestamp string into a NaiveDateTime
        let timestamp =
            NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S").map_err(|e| {
                ExpressionError::InvalidOperation(format!("Failed to parse timestamp: {}", e))
            })?;

        // Extract the requested field
        let result = match self.field {
            ExtractField::Year => timestamp.year(),
            ExtractField::Month => timestamp.month() as i32,
            ExtractField::Day => timestamp.day() as i32,
            ExtractField::Hour => timestamp.hour() as i32,
            ExtractField::Minute => timestamp.minute() as i32,
            ExtractField::Second => timestamp.second() as i32,
            ExtractField::Quarter => ((timestamp.month() - 1) / 3 + 1) as i32,
            ExtractField::Week => timestamp.iso_week().week() as i32,
            ExtractField::DayOfWeek => timestamp.weekday().num_days_from_monday() as i32 + 1,
            ExtractField::DayOfYear => timestamp.ordinal() as i32,
            ExtractField::Epoch => timestamp.and_utc().timestamp() as i32,
            ExtractField::Timezone => {
                return Err(ExpressionError::InvalidOperation(
                    "Timezone extraction not supported".to_string(),
                ));
            }
        };

        Ok(result)
    }
}

impl ExpressionOps for ExtractExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        // Evaluate the inner expression to get the timestamp
        let value = self.expr.evaluate(tuple, schema)?;

        // Convert the value to a string representation
        let timestamp_str = value.to_string();

        // Extract the field and return as an integer
        let extracted_value = self.extract_from_timestamp(&timestamp_str)?;
        Ok(Value::new(extracted_value))
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        // Evaluate the inner expression
        let value = self
            .expr
            .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;

        // Convert to string and extract
        let timestamp_str = value.to_string();
        let extracted_value = self.extract_from_timestamp(&timestamp_str)?;
        Ok(Value::new(extracted_value))
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
        if children.len() != 1 {
            panic!("ExtractExpression requires exactly one child");
        }
        Arc::new(Expression::Extract(ExtractExpression::new(
            self.field.clone(),
            children[0].clone(),
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate that the inner expression returns a timestamp
        let expr_type = self.expr.get_return_type().get_type();
        if expr_type != TypeId::Timestamp && expr_type != TypeId::VarChar {
            return Err(ExpressionError::InvalidOperation(format!(
                "EXTRACT requires timestamp or string input, got {:?}",
                expr_type
            )));
        }

        // Validate the inner expression
        self.expr.validate(schema)
    }
}

impl Display for ExtractExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "EXTRACT({:?} FROM {})", self.field, self.expr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types_db::value::Val;

    fn create_test_tuple(timestamp: &str) -> (Tuple, Schema) {
        let schema = Schema::new(vec![Column::new("ts", TypeId::Timestamp)]);
        let tuple = Tuple::new(
            &[Value::new(timestamp)],
            &schema,
            crate::common::rid::RID::new(0, 0),
        );
        (tuple, schema)
    }

    #[test]
    fn test_extract_year() {
        let (tuple, schema) = create_test_tuple("2024-03-15 14:30:00");

        let inner_expr = Arc::new(Expression::ColumnRef(
            crate::sql::execution::expressions::column_value_expression::ColumnRefExpression::new(
                0,
                0,
                Column::new("ts", TypeId::Timestamp),
                vec![],
            ),
        ));

        let extract_expr = ExtractExpression::new(
            ExtractField::Year,
            inner_expr,
            Column::new("year", TypeId::Integer),
        );

        let result = extract_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.get_val(), &Val::Integer(2024));
    }

    #[test]
    fn test_extract_month() {
        let (tuple, schema) = create_test_tuple("2024-03-15 14:30:00");

        let inner_expr = Arc::new(Expression::ColumnRef(
            crate::sql::execution::expressions::column_value_expression::ColumnRefExpression::new(
                0,
                0,
                Column::new("ts", TypeId::Timestamp),
                vec![],
            ),
        ));

        let extract_expr = ExtractExpression::new(
            ExtractField::Month,
            inner_expr,
            Column::new("month", TypeId::Integer),
        );

        let result = extract_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.get_val(), &Val::Integer(3));
    }

    #[test]
    fn test_extract_quarter() {
        let (tuple, schema) = create_test_tuple("2024-03-15 14:30:00");

        let inner_expr = Arc::new(Expression::ColumnRef(
            crate::sql::execution::expressions::column_value_expression::ColumnRefExpression::new(
                0,
                0,
                Column::new("ts", TypeId::Timestamp),
                vec![],
            ),
        ));

        let extract_expr = ExtractExpression::new(
            ExtractField::Quarter,
            inner_expr,
            Column::new("quarter", TypeId::Integer),
        );

        let result = extract_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.get_val(), &Val::Integer(1)); // March is in Q1
    }

    #[test]
    fn test_invalid_timestamp() {
        let (tuple, schema) = create_test_tuple("invalid-timestamp");

        let inner_expr = Arc::new(Expression::ColumnRef(
            crate::sql::execution::expressions::column_value_expression::ColumnRefExpression::new(
                0,
                0,
                Column::new("ts", TypeId::Timestamp),
                vec![],
            ),
        ));

        let extract_expr = ExtractExpression::new(
            ExtractField::Year,
            inner_expr,
            Column::new("year", TypeId::Integer),
        );

        assert!(extract_expr.evaluate(&tuple, &schema).is_err());
    }

    #[test]
    fn test_validate() {
        let schema = Schema::new(vec![
            Column::new("ts", TypeId::Timestamp),
            Column::new("int_col", TypeId::Integer),
        ]);

        // Valid timestamp column
        let valid_expr = Arc::new(Expression::ColumnRef(
            crate::sql::execution::expressions::column_value_expression::ColumnRefExpression::new(
                0,
                0,
                Column::new("ts", TypeId::Timestamp),
                vec![],
            ),
        ));

        let extract_expr = ExtractExpression::new(
            ExtractField::Year,
            valid_expr,
            Column::new("year", TypeId::Integer),
        );

        assert!(extract_expr.validate(&schema).is_ok());

        // Invalid integer column
        let invalid_expr = Arc::new(Expression::ColumnRef(
            crate::sql::execution::expressions::column_value_expression::ColumnRefExpression::new(
                0,
                1,
                Column::new("int_col", TypeId::Integer),
                vec![],
            ),
        ));

        let extract_expr = ExtractExpression::new(
            ExtractField::Year,
            invalid_expr,
            Column::new("year", TypeId::Integer),
        );

        assert!(extract_expr.validate(&schema).is_err());
    }
}
