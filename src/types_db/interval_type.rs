use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};

/// Implementation for IntervalType (stored as i64 seconds)
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Debug)]
pub struct IntervalType;

impl Default for IntervalType {
    fn default() -> Self {
        Self::new()
    }
}

impl IntervalType {
    pub fn new() -> Self {
        IntervalType
    }

    /// Format interval in a human-readable way
    fn format_interval(seconds: i64) -> String {
        let abs_seconds = seconds.abs();
        let days = abs_seconds / (24 * 60 * 60);
        let hours = (abs_seconds % (24 * 60 * 60)) / 3600;
        let minutes = (abs_seconds % 3600) / 60;
        let secs = abs_seconds % 60;

        let mut result = String::new();
        if seconds < 0 {
            result.push('-');
        }

        if days > 0 {
            result.push_str(&format!("{} days ", days));
        }

        result.push_str(&format!("{:02}:{:02}:{:02}", hours, minutes, secs));
        result
    }
}

impl Type for IntervalType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Interval
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Interval(r) => CmpBool::from(0 == *r),
            Val::BigInt(r) => CmpBool::from(0 == *r),
            Val::Integer(r) => CmpBool::from(0 == (*r as i64)),
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
            Val::Interval(r) => CmpBool::from(0 < *r),
            Val::BigInt(r) => CmpBool::from(0 < *r),
            Val::Integer(r) => CmpBool::from(0 < (*r as i64)),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Interval(r) => CmpBool::from(0 <= *r),
            Val::BigInt(r) => CmpBool::from(0 <= *r),
            Val::Integer(r) => CmpBool::from(0 <= (*r as i64)),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Interval(r) => CmpBool::from(0 > *r),
            Val::BigInt(r) => CmpBool::from(0 > *r),
            Val::Integer(r) => CmpBool::from(0 > (*r as i64)),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Interval(r) => CmpBool::from(0 >= *r),
            Val::BigInt(r) => CmpBool::from(0 >= *r),
            Val::Integer(r) => CmpBool::from(0 >= (*r as i64)),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn add(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Interval(r) => {
                // Adding two intervals
                Ok(Value::new_with_type(Val::Interval(*r), TypeId::Interval))
            }
            Val::BigInt(r) => {
                // Adding seconds as BigInt
                Ok(Value::new_with_type(Val::Interval(*r), TypeId::Interval))
            }
            Val::Integer(r) => {
                // Adding seconds as Integer
                Ok(Value::new_with_type(
                    Val::Interval(*r as i64),
                    TypeId::Interval,
                ))
            }
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot add non-interval types to Interval".to_string()),
        }
    }

    fn subtract(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Interval(r) => {
                // Subtracting one interval from another
                Ok(Value::new_with_type(Val::Interval(-*r), TypeId::Interval))
            }
            Val::BigInt(r) => {
                // Subtracting seconds as BigInt
                Ok(Value::new_with_type(Val::Interval(-*r), TypeId::Interval))
            }
            Val::Integer(r) => {
                // Subtracting seconds as Integer
                Ok(Value::new_with_type(
                    Val::Interval(-(*r as i64)),
                    TypeId::Interval,
                ))
            }
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot subtract non-interval types from Interval".to_string()),
        }
    }

    fn multiply(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Integer(_r) => {
                // Multiplying empty interval by a scalar always gives zero interval
                Ok(Value::new_with_type(
                    Val::Interval(0),
                    TypeId::Interval,
                ))
            }
            Val::BigInt(_r) => {
                // Multiplying empty interval by a scalar always gives zero interval
                Ok(Value::new_with_type(
                    Val::Interval(0),
                    TypeId::Interval,
                ))
            }
            Val::Decimal(_r) => {
                // Multiplying empty interval by a scalar always gives zero interval
                Ok(Value::new_with_type(
                    Val::Interval(0),
                    TypeId::Interval,
                ))
            }
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot multiply Interval by non-numeric type".to_string()),
        }
    }

    fn divide(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Integer(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::BigInt(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::Decimal(r) if *r == 0.0 => Err("Division by zero".to_string()),
            Val::Integer(_r) => {
                // Dividing empty interval by a scalar always gives zero interval
                Ok(Value::new_with_type(
                    Val::Interval(0),
                    TypeId::Interval,
                ))
            }
            Val::BigInt(_r) => {
                // Dividing empty interval by a scalar always gives zero interval
                Ok(Value::new_with_type(
                    Val::Interval(0),
                    TypeId::Interval,
                ))
            }
            Val::Decimal(_r) => {
                // Dividing empty interval by a scalar always gives zero interval
                Ok(Value::new_with_type(
                    Val::Interval(0),
                    TypeId::Interval,
                ))
            }
            Val::Interval(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::Interval(_r) => {
                // Dividing empty interval by another interval gives zero scalar
                Ok(Value::new(0))
            }
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot divide Interval by non-numeric type".to_string()),
        }
    }

    fn modulo(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Integer(r) if *r == 0 => Value::new(Val::Null),
            Val::BigInt(r) if *r == 0 => Value::new(Val::Null),
            Val::Integer(_r) => {
                // Modulo of empty interval always gives zero interval
                Value::new_with_type(Val::Interval(0), TypeId::Interval)
            }
            Val::BigInt(_r) => Value::new_with_type(Val::Interval(0), TypeId::Interval),
            Val::Interval(r) if *r == 0 => Value::new(Val::Null),
            Val::Interval(_r) => Value::new_with_type(Val::Interval(0), TypeId::Interval),
            _ => Value::new(Val::Null),
        }
    }

    fn min(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Interval(r) => Value::new_with_type(Val::Interval(0.min(*r)), TypeId::Interval),
            Val::BigInt(r) => Value::new_with_type(Val::Interval(0.min(*r)), TypeId::Interval),
            Val::Integer(r) => {
                Value::new_with_type(Val::Interval(0.min(*r as i64)), TypeId::Interval)
            }
            _ => Value::new(Val::Null),
        }
    }

    fn max(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Interval(r) => Value::new_with_type(Val::Interval(0.max(*r)), TypeId::Interval),
            Val::BigInt(r) => Value::new_with_type(Val::Interval(0.max(*r)), TypeId::Interval),
            Val::Integer(r) => {
                Value::new_with_type(Val::Interval(0.max(*r as i64)), TypeId::Interval)
            }
            _ => Value::new(Val::Null),
        }
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_val() {
            Val::Interval(seconds) => IntervalType::format_interval(*seconds),
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }
}

pub static INTERVAL_TYPE_INSTANCE: IntervalType = IntervalType;
