use std::fmt;

use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};

// Implementation for VarCharType
#[derive(Debug, Clone, PartialEq)]
pub struct VarCharType;

impl Default for VarCharType {
    fn default() -> Self {
        Self::new()
    }
}

impl VarCharType {
    /// Creates a new `VarCharType` instance.
    pub fn new() -> Self {
        VarCharType
    }
}

impl Type for VarCharType {
    fn get_type_id(&self) -> TypeId {
        TypeId::VarChar
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::VarLen(r) | Val::ConstLen(r) => CmpBool::from("" == r.as_str()),
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
            Val::VarLen(r) | Val::ConstLen(r) => CmpBool::from("" < r.as_str()),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::VarLen(r) | Val::ConstLen(r) => CmpBool::from("" <= r.as_str()),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::VarLen(r) | Val::ConstLen(r) => CmpBool::from("" > r.as_str()),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::VarLen(r) | Val::ConstLen(r) => CmpBool::from("" >= r.as_str()),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn add(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::VarLen(r) | Val::ConstLen(r) => Ok(Value::new(r.clone())),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot add non-string types to VarChar".to_string()),
        }
    }

    fn subtract(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot subtract from strings".to_string())
    }

    fn multiply(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot multiply strings".to_string())
    }

    fn divide(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot divide strings".to_string())
    }

    fn modulo(&self, _other: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn min(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::VarLen(r) | Val::ConstLen(r) => {
                if r.is_empty() {
                    Value::new(r.clone())
                } else {
                    Value::new(r.chars().min().unwrap_or_default().to_string())
                }
            },
            _ => Value::new(Val::Null),
        }
    }

    fn max(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::VarLen(r) | Val::ConstLen(r) => {
                if r.is_empty() {
                    Value::new(r.clone())
                } else {
                    Value::new(r.chars().max().unwrap_or_default().to_string())
                }
            },
            _ => Value::new(Val::Null),
        }
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_val() {
            Val::VarLen(s) | Val::ConstLen(s) => s.clone(),
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }
}

impl fmt::Display for VarCharType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VARCHAR")
    }
}

pub static VARCHAR_TYPE_INSTANCE: VarCharType = VarCharType;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varchar_comparisons() {
        let varchar_type = VarCharType::new();
        let empty = Value::new("");
        let hello = Value::new("hello");
        let world = Value::new("world");
        let null = Value::new(Val::Null);

        // Test equals
        assert_eq!(varchar_type.compare_equals(&empty), CmpBool::CmpTrue);
        assert_eq!(varchar_type.compare_equals(&hello), CmpBool::CmpFalse);
        assert_eq!(varchar_type.compare_equals(&null), CmpBool::CmpNull);

        // Test less than
        assert_eq!(varchar_type.compare_less_than(&hello), CmpBool::CmpTrue);
        assert_eq!(varchar_type.compare_less_than(&world), CmpBool::CmpTrue);
        assert_eq!(varchar_type.compare_less_than(&empty), CmpBool::CmpFalse);
        assert_eq!(varchar_type.compare_less_than(&null), CmpBool::CmpNull);
    }

    #[test]
    fn test_varchar_arithmetic() {
        let varchar_type = VarCharType::new();
        let hello = Value::new("hello");
        let null = Value::new(Val::Null);

        // Test addition (concatenation)
        assert_eq!(varchar_type.add(&hello).unwrap(), Value::new("hello"));

        // Test invalid operations
        assert!(varchar_type.subtract(&hello).is_err());
        assert!(varchar_type.multiply(&hello).is_err());
        assert!(varchar_type.divide(&hello).is_err());
        assert_eq!(varchar_type.modulo(&hello), Value::new(Val::Null));

        // Test null handling
        assert_eq!(varchar_type.add(&null).unwrap(), Value::new(Val::Null));
    }

    #[test]
    fn test_varchar_min_max() {
        let varchar_type = VarCharType::new();
        let empty = Value::new("");
        let hello = Value::new("hello");
        let null = Value::new(Val::Null);

        // Test min/max
        assert_eq!(Type::min(&varchar_type, &empty), Value::new(""));
        assert_eq!(Type::min(&varchar_type, &hello), Value::new("e"));
        assert_eq!(Type::min(&varchar_type, &null), Value::new(Val::Null));

        assert_eq!(Type::max(&varchar_type, &empty), Value::new(""));
        assert_eq!(Type::max(&varchar_type, &hello), Value::new("o"));
        assert_eq!(Type::max(&varchar_type, &null), Value::new(Val::Null));
    }

    #[test]
    fn test_varchar_to_string() {
        let hello = Value::new("hello");
        let empty = Value::new("");
        let null = Value::new(Val::Null);

        assert_eq!(ToString::to_string(&hello), "hello");
        assert_eq!(ToString::to_string(&empty), "");
        assert_eq!(ToString::to_string(&null), "NULL");
    }
}
