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
use chrono::{NaiveDate, NaiveTime, NaiveDateTime, Timelike, DateTime};

#[derive(Clone, Debug, PartialEq)]
pub struct TypedStringExpression {
    data_type: String,  // e.g., "DATE", "TIME", "TIMESTAMP"
    value: String,
    return_type: Column,
    children: Vec<Arc<Expression>>,
}

impl TypedStringExpression {
    pub fn new(data_type: String, value: String, return_type: Column) -> Self {
        Self {
            data_type,
            value,
            return_type,
            children: vec![],
        }
    }
    
    // Parse a date string in the format YYYY-MM-DD
    fn parse_date(&self, date_str: &str) -> Result<Value, ExpressionError> {
        match NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
            Ok(date) => {
                // Convert to timestamp (seconds since epoch)
                let timestamp = date.and_hms_opt(0, 0, 0)
                    .ok_or_else(|| ExpressionError::InvalidOperation(
                        format!("Failed to convert date '{}' to timestamp", date_str)
                    ))?
                    .and_utc().timestamp() as u64;
                Ok(Value::new_with_type(Val::Timestamp(timestamp), TypeId::Timestamp))
            },
            Err(e) => Err(ExpressionError::InvalidOperation(
                format!("Invalid date format '{}': {}", date_str, e)
            )),
        }
    }
    
    // Parse a time string in the format HH:MM:SS[.SSS]
    fn parse_time(&self, time_str: &str) -> Result<Value, ExpressionError> {
        // For time, we'll store the number of seconds since midnight
        let format = if time_str.contains('.') { "%H:%M:%S.%f" } else { "%H:%M:%S" };
        
        match NaiveTime::parse_from_str(time_str, format) {
            Ok(time) => {
                // Calculate seconds since midnight using Timelike trait
                let seconds_since_midnight = 
                    (time.hour() * 3600 + time.minute() * 60 + time.second()) as u64;
                Ok(Value::new_with_type(Val::Timestamp(seconds_since_midnight), TypeId::Timestamp))
            },
            Err(e) => Err(ExpressionError::InvalidOperation(
                format!("Invalid time format '{}': {}", time_str, e)
            )),
        }
    }
    
    // Parse a timestamp string in the format YYYY-MM-DD HH:MM:SS[.SSS]
    fn parse_timestamp(&self, timestamp_str: &str) -> Result<Value, ExpressionError> {
        let format = if timestamp_str.contains('.') { 
            "%Y-%m-%d %H:%M:%S.%f" 
        } else { 
            "%Y-%m-%d %H:%M:%S" 
        };
        
        match NaiveDateTime::parse_from_str(timestamp_str, format) {
            Ok(dt) => {
                // Convert to UTC DateTime and format as RFC3339
                let utc_dt = dt.and_utc();
                Ok(Value::new(utc_dt.to_rfc3339()))
            },
            Err(e) => Err(ExpressionError::InvalidOperation(
                format!("Invalid timestamp format '{}': {}", timestamp_str, e)
            )),
        }
    }
}

impl ExpressionOps for TypedStringExpression {
    fn evaluate(&self, _tuple: &Tuple, _schema: &Schema) -> Result<Value, ExpressionError> {
        // Parse string according to data type
        match self.data_type.to_uppercase().as_str() {
            "DATE" => self.parse_date(&self.value),
            "TIME" => self.parse_time(&self.value),
            "TIMESTAMP" => {
                let result = self.parse_timestamp(&self.value)?;
                // If the return type is VarChar (for AT TIME ZONE), return as is
                // Otherwise convert to timestamp
                if self.return_type.get_type() == TypeId::VarChar {
                    Ok(result)
                } else {
                    // Parse the RFC3339 string back to a timestamp value
                    match DateTime::parse_from_rfc3339(&result.to_string()) {
                        Ok(dt) => Ok(Value::new_with_type(Val::Timestamp(dt.timestamp() as u64), TypeId::Timestamp)),
                        Err(e) => Err(ExpressionError::InvalidOperation(format!("Failed to convert to timestamp: {}", e)))
                    }
                }
            },
            // For other data types, we'll return the string value and let the caller handle conversion
            _ => {
                // Create value with the correct return type directly
                let value = Value::new_with_type(Val::VarLen(self.value.clone()), self.return_type.get_type());
                // Try to cast to the return type if needed
                if value.get_type_id() != self.return_type.get_type() {
                    value.cast_to(self.return_type.get_type())
                        .map_err(|e| ExpressionError::InvalidOperation(
                            format!("Failed to cast '{}' to {}: {}", self.value, self.data_type, e)
                        ))
                } else {
                    Ok(value)
                }
            }
        }
    }

    fn evaluate_join(&self, _left_tuple: &Tuple, _left_schema: &Schema, _right_tuple: &Tuple, _right_schema: &Schema) -> Result<Value, ExpressionError> {
        // For typed string literals, the evaluation doesn't depend on tuple data
        self.evaluate(_left_tuple, _left_schema)
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
        Arc::new(Expression::TypedString(TypedStringExpression {
            data_type: self.data_type.clone(),
            value: self.value.clone(),
            return_type: self.return_type.clone(),
            children,
        }))
    }

    fn validate(&self, _schema: &Schema) -> Result<(), ExpressionError> {
        // Validate that the data type and value format are compatible
        match self.data_type.to_uppercase().as_str() {
            "DATE" => {
                if NaiveDate::parse_from_str(&self.value, "%Y-%m-%d").is_err() {
                    return Err(ExpressionError::InvalidOperation(
                        format!("Invalid DATE format: '{}'. Expected YYYY-MM-DD", self.value)
                    ));
                }
            },
            "TIME" => {
                let format = if self.value.contains('.') { "%H:%M:%S.%f" } else { "%H:%M:%S" };
                if NaiveTime::parse_from_str(&self.value, format).is_err() {
                    return Err(ExpressionError::InvalidOperation(
                        format!("Invalid TIME format: '{}'. Expected HH:MM:SS[.SSS]", self.value)
                    ));
                }
            },
            "TIMESTAMP" => {
                let format = if self.value.contains('.') { 
                    "%Y-%m-%d %H:%M:%S.%f" 
                } else { 
                    "%Y-%m-%d %H:%M:%S" 
                };
                if NaiveDateTime::parse_from_str(&self.value, format).is_err() {
                    return Err(ExpressionError::InvalidOperation(
                        format!("Invalid TIMESTAMP format: '{}'. Expected YYYY-MM-DD HH:MM:SS[.SSS]", self.value)
                    ));
                }
            },
            // For other types, we don't validate the format
            _ => {}
        }
        
        Ok(())
    }
}

impl Display for TypedStringExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} '{}'", self.data_type, self.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    
    #[test]
    fn test_date_parsing() {
        let expr = TypedStringExpression::new(
            "DATE".to_string(),
            "2023-01-15".to_string(),
            Column::new("date_col", TypeId::Timestamp)
        );
        
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], schema.clone(), RID::new(0, 0));
        
        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.get_type_id(), TypeId::Timestamp);
    }
    
    #[test]
    fn test_time_parsing() {
        let expr = TypedStringExpression::new(
            "TIME".to_string(),
            "14:30:45".to_string(),
            Column::new("time_col", TypeId::Timestamp)
        );
        
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], schema.clone(), RID::new(0, 0));
        
        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.get_type_id(), TypeId::Timestamp);
        
        // Time should be stored as seconds since midnight
        if let Val::Timestamp(seconds) = result.get_val() {
            // 14:30:45 = 14*3600 + 30*60 + 45 = 52245 seconds
            assert_eq!(*seconds, 14*3600 + 30*60 + 45);
        } else {
            panic!("Expected timestamp value");
        }
    }
    
    #[test]
    fn test_timestamp_parsing() {
        let expr = TypedStringExpression::new(
            "TIMESTAMP".to_string(),
            "2023-01-15 14:30:45".to_string(),
            Column::new("timestamp_col", TypeId::Timestamp)
        );
        
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], schema.clone(), RID::new(0, 0));
        
        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.get_type_id(), TypeId::Timestamp);
    }
    
    #[test]
    fn test_invalid_date_format() {
        let expr = TypedStringExpression::new(
            "DATE".to_string(),
            "2023/01/15".to_string(), // Wrong format
            Column::new("date_col", TypeId::Timestamp)
        );
        
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], schema.clone(), RID::new(0, 0));
        
        let result = expr.evaluate(&tuple, &schema);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_other_type() {
        let expr = TypedStringExpression::new(
            "INTEGER".to_string(),
            "123".to_string(),
            Column::new("int_col", TypeId::Integer)
        );
        
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], schema.clone(), RID::new(0, 0));
        
        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.get_type_id(), TypeId::Integer);
    }
    
    #[test]
    fn test_display() {
        let expr = TypedStringExpression::new(
            "DATE".to_string(),
            "2023-01-15".to_string(),
            Column::new("date_col", TypeId::Timestamp)
        );
        
        assert_eq!(expr.to_string(), "DATE '2023-01-15'");
    }
} 