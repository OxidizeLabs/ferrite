use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};

/// Implementation for UUIDType
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Debug)]
pub struct UUIDType;

impl Default for UUIDType {
    fn default() -> Self {
        Self::new()
    }
}

impl UUIDType {
    pub fn new() -> Self {
        UUIDType
    }
}

impl Type for UUIDType {
    fn get_type_id(&self) -> TypeId {
        TypeId::UUID
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::UUID(r) => CmpBool::from(String::new() == *r),
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
            Val::UUID(r) => CmpBool::from(String::new() < *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::UUID(r) => CmpBool::from(String::new() <= *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::UUID(r) => CmpBool::from(String::new() > *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::UUID(r) => CmpBool::from(String::new() >= *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn add(&self, _other: &Value) -> Result<Value, String> {
        Err("Addition not supported for UUID type".to_string())
    }

    fn subtract(&self, _other: &Value) -> Result<Value, String> {
        Err("Subtraction not supported for UUID type".to_string())
    }

    fn multiply(&self, _other: &Value) -> Result<Value, String> {
        Err("Multiplication not supported for UUID type".to_string())
    }

    fn divide(&self, _other: &Value) -> Result<Value, String> {
        Err("Division not supported for UUID type".to_string())
    }

    fn modulo(&self, _other: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn min(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::UUID(r) => {
                if String::new() < *r {
                    Value::new(String::new())
                } else {
                    Value::new(r.clone())
                }
            }
            _ => Value::new(Val::Null),
        }
    }

    fn max(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::UUID(r) => {
                if String::new() > *r {
                    Value::new(String::new())
                } else {
                    Value::new(r.clone())
                }
            }
            _ => Value::new(Val::Null),
        }
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_val() {
            Val::UUID(u) => u.clone(),
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }
}

pub static UUID_TYPE_INSTANCE: UUIDType = UUIDType;
