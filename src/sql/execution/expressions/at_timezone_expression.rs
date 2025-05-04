use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use chrono::DateTime;
use chrono_tz::Tz;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub struct AtTimeZoneExpression {
    timestamp: Arc<Expression>,
    timezone: Arc<Expression>,
    return_type: Column,
    children: Vec<Arc<Expression>>,
}

impl AtTimeZoneExpression {
    pub fn new(timestamp: Arc<Expression>, timezone: Arc<Expression>, return_type: Column) -> Self {
        let children = vec![timestamp.clone(), timezone.clone()];

        Self {
            timestamp,
            timezone,
            return_type,
            children,
        }
    }
}

impl ExpressionOps for AtTimeZoneExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let timestamp_val = self.timestamp.evaluate(tuple, schema)?;
        let timezone_val = self.timezone.evaluate(tuple, schema)?;

        // Get timestamp as string
        let timestamp_str = timestamp_val.to_string();

        // Parse the timezone string using chrono_tz
        let timezone_str = timezone_val.to_string();
        let timezone = Tz::from_str(&timezone_str)
            .map_err(|e| ExpressionError::InvalidOperation(format!("Invalid timezone: {}", e)))?;

        // Parse the timestamp and convert to the target timezone
        let dt = DateTime::parse_from_rfc3339(&timestamp_str).map_err(|e| {
            ExpressionError::InvalidOperation(format!("Invalid timestamp format: {}", e))
        })?;

        // Convert to target timezone
        let converted = dt.with_timezone(&timezone);

        // Return as RFC3339 string
        Ok(Value::new(converted.to_rfc3339()))
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        _right_tuple: &Tuple,
        _right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        // For join evaluation, we can just use the regular evaluate
        self.evaluate(left_tuple, left_schema)
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        match child_idx {
            0 => &self.timestamp,
            1 => &self.timezone,
            _ => panic!("AtTimeZoneExpression has only 2 children"),
        }
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        assert_eq!(children.len(), 2);
        Arc::new(Expression::AtTimeZone(AtTimeZoneExpression::new(
            children[0].clone(),
            children[1].clone(),
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate children
        self.timestamp.validate(schema)?;
        self.timezone.validate(schema)?;

        // Validate types
        let timestamp_type = self.timestamp.get_return_type().get_type();
        if !matches!(timestamp_type, TypeId::Timestamp) {
            return Err(ExpressionError::InvalidOperation(format!(
                "AT TIME ZONE requires timestamp input, got {:?}",
                timestamp_type
            )));
        }

        let timezone_type = self.timezone.get_return_type().get_type();
        if !matches!(timezone_type, TypeId::VarChar) {
            return Err(ExpressionError::InvalidOperation(format!(
                "AT TIME ZONE requires string timezone, got {:?}",
                timezone_type
            )));
        }

        Ok(())
    }
}

impl Display for AtTimeZoneExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} AT TIME ZONE {}", self.timestamp, self.timezone)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;

    fn create_test_schema() -> Schema {
        Schema::new(vec![])
    }

    fn create_test_tuple(schema: &Schema) -> Tuple {
        let rid = RID::new(0, 0);
        Tuple::new(&*vec![], schema, rid)
    }

    #[test]
    fn test_at_timezone_conversion() {
        // Create test schema and tuple
        let schema = create_test_schema();
        let tuple = create_test_tuple(&schema);

        // Create timestamp expression (2024-01-01 12:00:00 UTC)
        let timestamp = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("2024-01-01T12:00:00Z"),
            Column::new("timestamp", TypeId::Timestamp),
            vec![],
        )));

        // Create timezone expression (PST)
        let timezone = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("America/Los_Angeles"),
            Column::new("timezone", TypeId::VarChar),
            vec![],
        )));

        // Create AT TIME ZONE expression
        let expr = AtTimeZoneExpression::new(
            timestamp,
            timezone,
            Column::new("result", TypeId::Timestamp),
        );

        // Evaluate expression
        let result = expr.evaluate(&tuple, &schema).unwrap();

        // The result should be "2024-01-01T04:00:00-08:00" (12:00 UTC = 04:00 PST)
        assert_eq!(result.to_string(), "2024-01-01T04:00:00-08:00");
    }

    #[test]
    fn test_invalid_timestamp() {
        let schema = create_test_schema();
        let tuple = create_test_tuple(&schema);

        // Create invalid timestamp expression
        let timestamp = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("not a timestamp"),
            Column::new("timestamp", TypeId::Timestamp),
            vec![],
        )));

        let timezone = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("UTC"),
            Column::new("timezone", TypeId::VarChar),
            vec![],
        )));

        let expr = AtTimeZoneExpression::new(
            timestamp,
            timezone,
            Column::new("result", TypeId::Timestamp),
        );

        // Evaluation should fail
        assert!(expr.evaluate(&tuple, &schema).is_err());
    }

    #[test]
    fn test_invalid_timezone() {
        let schema = create_test_schema();
        let tuple = create_test_tuple(&schema);

        let timestamp = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("2024-01-01T12:00:00Z"),
            Column::new("timestamp", TypeId::Timestamp),
            vec![],
        )));

        // Create invalid timezone expression
        let timezone = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("Invalid/Timezone"),
            Column::new("timezone", TypeId::VarChar),
            vec![],
        )));

        let expr = AtTimeZoneExpression::new(
            timestamp,
            timezone,
            Column::new("result", TypeId::Timestamp),
        );

        // Evaluation should fail
        assert!(expr.evaluate(&tuple, &schema).is_err());
    }

    #[test]
    fn test_display() {
        let timestamp = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("2024-01-01T12:00:00Z"),
            Column::new("timestamp", TypeId::Timestamp),
            vec![],
        )));

        let timezone = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("UTC"),
            Column::new("timezone", TypeId::VarChar),
            vec![],
        )));

        let expr = AtTimeZoneExpression::new(
            timestamp,
            timezone,
            Column::new("result", TypeId::Timestamp),
        );

        assert_eq!(expr.to_string(), "2024-01-01T12:00:00Z AT TIME ZONE UTC");
    }
}
