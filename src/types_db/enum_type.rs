use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};

/// Implementation for EnumType
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Debug)]
pub struct EnumType;

impl Default for EnumType {
    fn default() -> Self {
        Self::new()
    }
}

impl EnumType {
    /// Creates a new `EnumType` instance.
    pub fn new() -> Self {
        EnumType
    }
}

impl Type for EnumType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Enum
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Enum(val, _) => CmpBool::from(0 == *val),
            Val::Integer(val) => CmpBool::from(0 == *val),
            Val::SmallInt(val) => CmpBool::from(0 == *val as i32),
            Val::TinyInt(val) => CmpBool::from(0 == *val as i32),
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
            Val::Enum(val, _) => CmpBool::from(0 < *val),
            Val::Integer(val) => CmpBool::from(0 < *val),
            Val::SmallInt(val) => CmpBool::from(0 < *val as i32),
            Val::TinyInt(val) => CmpBool::from(0 < *val as i32),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Enum(val, _) => CmpBool::from(0 <= *val),
            Val::Integer(val) => CmpBool::from(0 <= *val),
            Val::SmallInt(val) => CmpBool::from(0 <= *val as i32),
            Val::TinyInt(val) => CmpBool::from(0 <= *val as i32),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Enum(val, _) => CmpBool::from(0 > *val),
            Val::Integer(val) => CmpBool::from(0 > *val),
            Val::SmallInt(val) => CmpBool::from(0 > *val as i32),
            Val::TinyInt(val) => CmpBool::from(0 > *val as i32),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Enum(val, _) => CmpBool::from(0 >= *val),
            Val::Integer(val) => CmpBool::from(0 >= *val),
            Val::SmallInt(val) => CmpBool::from(0 >= *val as i32),
            Val::TinyInt(val) => CmpBool::from(0 >= *val as i32),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn add(&self, _other: &Value) -> Result<Value, String> {
        Err("Addition not supported for Enum type".to_string())
    }

    fn subtract(&self, _other: &Value) -> Result<Value, String> {
        Err("Subtraction not supported for Enum type".to_string())
    }

    fn multiply(&self, _other: &Value) -> Result<Value, String> {
        Err("Multiplication not supported for Enum type".to_string())
    }

    fn divide(&self, _other: &Value) -> Result<Value, String> {
        Err("Division not supported for Enum type".to_string())
    }

    fn modulo(&self, _other: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn min(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Enum(val, name) => {
                if 0 < *val {
                    Value::new(Val::Enum(0, String::new()))
                } else {
                    Value::new(Val::Enum(*val, name.clone()))
                }
            },
            _ => Value::new(Val::Null),
        }
    }

    fn max(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Enum(val, name) => {
                if 0 > *val {
                    Value::new(Val::Enum(0, String::new()))
                } else {
                    Value::new(Val::Enum(*val, name.clone()))
                }
            },
            _ => Value::new(Val::Null),
        }
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_val() {
            Val::Enum(_, name) => name.clone(),
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }
}

pub static ENUM_TYPE_INSTANCE: EnumType = EnumType;
