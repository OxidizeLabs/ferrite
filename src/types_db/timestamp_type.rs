use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};
use std::time::{SystemTime, UNIX_EPOCH};

// Implementation for TimestampType
#[derive(Debug)]
pub struct TimestampType;

impl TimestampType {
    pub fn new() -> Self {
        TimestampType
    }
}

impl Type for TimestampType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Timestamp
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Timestamp(r) => CmpBool::from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    == *r,
            ),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_not_equals(&self, other: &Value) -> CmpBool {
        match self.compare_equals(other) {
            CmpBool::CmpTrue => CmpBool::CmpFalse,
            CmpBool::CmpFalse => CmpBool::CmpTrue,
            CmpBool::CmpNull => CmpBool::CmpNull,
        }
    }

    fn compare_less_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Timestamp(r) => CmpBool::from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    < *r,
            ),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Timestamp(r) => CmpBool::from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    <= *r,
            ),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Timestamp(r) => CmpBool::from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    > *r,
            ),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Timestamp(r) => CmpBool::from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    >= *r,
            ),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn add(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Timestamp(r) => Ok(Value::new(*r)),
            Val::Integer(r) => {
                let current = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                Ok(Value::new(current.saturating_add(*r as u64)))
            }
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot add non-numeric types to Timestamp".to_string()),
        }
    }

    fn subtract(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Timestamp(r) => Ok(Value::new(*r)),
            Val::Integer(r) => {
                let current = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                Ok(Value::new(current.saturating_sub(*r as u64)))
            }
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot subtract non-numeric types from Timestamp".to_string()),
        }
    }

    fn multiply(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot multiply timestamps".to_string())
    }

    fn divide(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot divide timestamps".to_string())
    }

    fn modulo(&self, _other: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn min(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Timestamp(r) => {
                let current = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                Value::new(current.min(*r))
            }
            _ => Value::new(Val::Null),
        }
    }

    fn max(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Timestamp(r) => {
                let current = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                Value::new(current.max(*r))
            }
            _ => Value::new(Val::Null),
        }
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_val() {
            Val::Timestamp(ts) => {
                // Display as Unix timestamp (seconds since epoch)
                ts.to_string()
            }
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }
}

pub static TIMESTAMP_TYPE_INSTANCE: TimestampType = TimestampType;

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_timestamp_comparisons() {
        let timestamp_type = TimestampType::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let future = now + 100;
        let past = now - 100;

        let current = Value::new(now);
        let future_val = Value::new(future);
        let past_val = Value::new(past);
        let null = Value::new(Val::Null);

        // Test equals
        assert_eq!(timestamp_type.compare_equals(&current), CmpBool::CmpTrue);
        assert_eq!(
            timestamp_type.compare_equals(&future_val),
            CmpBool::CmpFalse
        );
        assert_eq!(timestamp_type.compare_equals(&null), CmpBool::CmpNull);

        // Test less than
        assert_eq!(
            timestamp_type.compare_less_than(&future_val),
            CmpBool::CmpTrue
        );
        assert_eq!(
            timestamp_type.compare_less_than(&past_val),
            CmpBool::CmpFalse
        );
        assert_eq!(timestamp_type.compare_less_than(&null), CmpBool::CmpNull);
    }

    #[test]
    fn test_timestamp_arithmetic() {
        let timestamp_type = TimestampType::new();
        let seconds = Value::new(5i32);
        let null = Value::new(Val::Null);

        // Test addition
        let result = timestamp_type.add(&seconds).unwrap();
        sleep(Duration::from_secs(1));
        assert!(matches!(result.get_val(), Val::Timestamp(_)));

        // Test invalid operations
        assert!(timestamp_type.multiply(&seconds).is_err());
        assert!(timestamp_type.divide(&seconds).is_err());
        assert_eq!(timestamp_type.modulo(&seconds), Value::new(Val::Null));

        // Test null handling
        assert_eq!(timestamp_type.add(&null).unwrap(), Value::new(Val::Null));
    }

    #[test]
    fn test_timestamp_min_max() {
        let timestamp_type = TimestampType::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let future = now + 100;
        let past = now - 100;

        let future_val = Value::new(future);
        let past_val = Value::new(past);
        let null = Value::new(Val::Null);

        // Test min/max
        assert!(matches!(
            Type::min(&timestamp_type, &future_val).get_val(),
            Val::Timestamp(_)
        ));
        assert!(matches!(
            Type::min(&timestamp_type, &past_val).get_val(),
            Val::Timestamp(_)
        ));
        assert_eq!(Type::min(&timestamp_type, &null), Value::new(Val::Null));

        assert!(matches!(
            Type::max(&timestamp_type, &future_val).get_val(),
            Val::Timestamp(_)
        ));
        assert!(matches!(
            Type::max(&timestamp_type, &past_val).get_val(),
            Val::Timestamp(_)
        ));
        assert_eq!(Type::max(&timestamp_type, &null), Value::new(Val::Null));
    }

    #[test]
    fn test_timestamp_to_string() {
        let timestamp_type = TimestampType::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let current = Value::new(now);
        let null = Value::new(Val::Null);

        assert_eq!(timestamp_type.to_string(&current), now.to_string());
        assert_eq!(timestamp_type.to_string(&null), "NULL");
    }
}
