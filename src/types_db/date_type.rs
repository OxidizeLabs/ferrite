use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};
use chrono::NaiveDate;

/// Implementation for DateType (stored as i32 days from Unix epoch)
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Debug)]
pub struct DateType;

impl Default for DateType {
    fn default() -> Self {
        Self::new()
    }
}

impl DateType {
    /// Creates a new `DateType` instance.
    pub fn new() -> Self {
        DateType
    }

    /// Convert a date value to a NaiveDate
    fn as_naive_date(days: i32) -> Option<NaiveDate> {
        // Unix epoch is 1970-01-01
        NaiveDate::from_ymd_opt(1970, 1, 1)
            .and_then(|epoch| epoch.checked_add_days(chrono::Days::new(days as u64)))
    }
}

impl Type for DateType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Date
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Date(r) => CmpBool::from(0 == *r),
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
            Val::Date(r) => CmpBool::from(0 < *r),
            Val::Integer(r) => CmpBool::from(0 < *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Date(r) => CmpBool::from(0 <= *r),
            Val::Integer(r) => CmpBool::from(0 <= *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Date(r) => CmpBool::from(0 > *r),
            Val::Integer(r) => CmpBool::from(0 > *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Date(r) => CmpBool::from(0 >= *r),
            Val::Integer(r) => CmpBool::from(0 >= *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn add(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Date(r) => Ok(Value::new_with_type(Val::Date(*r), TypeId::Date)),
            Val::Integer(r) => Ok(Value::new_with_type(Val::Date(*r), TypeId::Date)),
            Val::Interval(days) => {
                // Adding days to a date
                Ok(Value::new_with_type(Val::Date(*days as i32), TypeId::Date))
            },
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot add non-date/interval types to Date".to_string()),
        }
    }

    fn subtract(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Date(r) => {
                // Subtracting two dates yields an interval (in days)
                Ok(Value::new_with_type(
                    Val::Interval(-(*r as i64)),
                    TypeId::Interval,
                ))
            },
            Val::Integer(r) => {
                // Subtracting days from a date
                Ok(Value::new_with_type(Val::Date(-*r), TypeId::Date))
            },
            Val::Interval(days) => {
                // Subtracting an interval from a date
                Ok(Value::new_with_type(
                    Val::Date(-(*days as i32)),
                    TypeId::Date,
                ))
            },
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot subtract non-date/interval types from Date".to_string()),
        }
    }

    fn multiply(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot multiply Date".to_string()),
        }
    }

    fn divide(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot divide Date".to_string()),
        }
    }

    fn modulo(&self, _other: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn min(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Date(r) => Value::new_with_type(Val::Date(0.min(*r)), TypeId::Date),
            Val::Integer(r) => Value::new_with_type(Val::Date(0.min(*r)), TypeId::Date),
            _ => Value::new(Val::Null),
        }
    }

    fn max(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Date(r) => Value::new_with_type(Val::Date(0.max(*r)), TypeId::Date),
            Val::Integer(r) => Value::new_with_type(Val::Date(0.max(*r)), TypeId::Date),
            _ => Value::new(Val::Null),
        }
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_val() {
            Val::Date(days) => {
                // Format the date as YYYY-MM-DD
                if let Some(date) = DateType::as_naive_date(*days) {
                    date.format("%Y-%m-%d").to_string()
                } else {
                    format!("INVALID_DATE({})", days)
                }
            },
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }
}

pub static DATE_TYPE_INSTANCE: DateType = DateType;
