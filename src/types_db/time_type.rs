use chrono::NaiveTime;

use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};

/// Implementation for TimeType (stored as i32 seconds from midnight)
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Debug)]
pub struct TimeType;

impl Default for TimeType {
    fn default() -> Self {
        Self::new()
    }
}

impl TimeType {
    /// Creates a new `TimeType` instance.
    pub fn new() -> Self {
        TimeType
    }

    /// Convert a time value to a NaiveTime
    fn as_naive_time(seconds_from_midnight: i32) -> Option<NaiveTime> {
        if !(0..24 * 60 * 60).contains(&seconds_from_midnight) {
            None
        } else {
            let hours = seconds_from_midnight / 3600;
            let minutes = (seconds_from_midnight % 3600) / 60;
            let seconds = seconds_from_midnight % 60;
            NaiveTime::from_hms_opt(hours as u32, minutes as u32, seconds as u32)
        }
    }
}

impl Type for TimeType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Time
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Time(r) => CmpBool::from(0 == *r),
            Val::Integer(r) => CmpBool::from(0 == *r),
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
            Val::Time(r) => CmpBool::from(0 < *r),
            Val::Integer(r) => CmpBool::from(0 < *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Time(r) => CmpBool::from(0 <= *r),
            Val::Integer(r) => CmpBool::from(0 <= *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Time(r) => CmpBool::from(0 > *r),
            Val::Integer(r) => CmpBool::from(0 > *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Time(r) => CmpBool::from(0 >= *r),
            Val::Integer(r) => CmpBool::from(0 >= *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn add(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Time(r) => {
                let total_seconds = *r;
                // Ensure the result is within 24 hours
                let normalized = total_seconds % (24 * 60 * 60);
                Ok(Value::new_with_type(Val::Time(normalized), TypeId::Time))
            },
            Val::Integer(r) => {
                let total_seconds = *r;
                // Ensure the result is within 24 hours
                let normalized = total_seconds % (24 * 60 * 60);
                Ok(Value::new_with_type(Val::Time(normalized), TypeId::Time))
            },
            Val::Interval(seconds) => {
                // Adding seconds to a time
                let total_seconds = *seconds as i32;
                // Ensure the result is within 24 hours
                let normalized = total_seconds % (24 * 60 * 60);
                Ok(Value::new_with_type(Val::Time(normalized), TypeId::Time))
            },
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot add non-time/interval types to Time".to_string()),
        }
    }

    fn subtract(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Time(r) => {
                // Subtracting two times yields an interval (in seconds)
                let diff = -*r;
                Ok(Value::new_with_type(
                    Val::Interval(diff as i64),
                    TypeId::Interval,
                ))
            },
            Val::Integer(r) => {
                // Subtracting seconds from a time
                let total_seconds = -*r;
                // Normalize to positive value within 24 hours
                let day_seconds = 24 * 60 * 60;
                let normalized = ((total_seconds % day_seconds) + day_seconds) % day_seconds;
                Ok(Value::new_with_type(Val::Time(normalized), TypeId::Time))
            },
            Val::Interval(seconds) => {
                // Subtracting an interval from a time
                let total_seconds = -(*seconds as i32);
                // Normalize to positive value within 24 hours
                let day_seconds = 24 * 60 * 60;
                let normalized = ((total_seconds % day_seconds) + day_seconds) % day_seconds;
                Ok(Value::new_with_type(Val::Time(normalized), TypeId::Time))
            },
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot subtract non-time/interval types from Time".to_string()),
        }
    }

    fn multiply(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot multiply Time".to_string()),
        }
    }

    fn divide(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot divide Time".to_string()),
        }
    }

    fn modulo(&self, _other: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn min(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Time(r) => Value::new_with_type(Val::Time(0.min(*r)), TypeId::Time),
            Val::Integer(r) => Value::new_with_type(Val::Time(0.min(*r)), TypeId::Time),
            _ => Value::new(Val::Null),
        }
    }

    fn max(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Time(r) => Value::new_with_type(Val::Time(0.max(*r)), TypeId::Time),
            Val::Integer(r) => Value::new_with_type(Val::Time(0.max(*r)), TypeId::Time),
            _ => Value::new(Val::Null),
        }
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_val() {
            Val::Time(seconds) => {
                // Format the time as HH:MM:SS
                if let Some(time) = TimeType::as_naive_time(*seconds) {
                    time.format("%H:%M:%S").to_string()
                } else {
                    format!("INVALID_TIME({})", seconds)
                }
            },
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }
}

pub static TIME_TYPE_INSTANCE: TimeType = TimeType;
