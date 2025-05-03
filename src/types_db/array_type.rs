use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};

/// Implementation for ArrayType
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Debug)]
pub struct ArrayType;

impl ArrayType {
    pub fn new() -> Self {
        ArrayType
    }
}

impl Type for ArrayType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Array
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Array(r) => CmpBool::from(Vec::<Value>::new() == *r),
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
            Val::Array(r) => {
                // Compare lengths if both arrays have elements of the same type
                if r.is_empty() {
                    CmpBool::CmpFalse
                } else {
                    CmpBool::CmpTrue
                }
            }
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Array(r) => {
                if r.is_empty() {
                    CmpBool::CmpTrue
                } else {
                    CmpBool::CmpTrue
                }
            }
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Array(r) => {
                if r.is_empty() {
                    CmpBool::CmpFalse
                } else {
                    CmpBool::CmpFalse
                }
            }
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Array(r) => {
                if r.is_empty() {
                    CmpBool::CmpTrue
                } else {
                    CmpBool::CmpFalse
                }
            }
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn add(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Array(r) => {
                let mut result = Vec::new();
                result.extend_from_slice(r);
                Ok(Value::new(result))
            }
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot add non-array values to Array".to_string()),
        }
    }

    fn subtract(&self, _other: &Value) -> Result<Value, String> {
        Err("Subtraction not supported for Array type".to_string())
    }

    fn multiply(&self, _other: &Value) -> Result<Value, String> {
        Err("Multiplication not supported for Array type".to_string())
    }

    fn divide(&self, _other: &Value) -> Result<Value, String> {
        Err("Division not supported for Array type".to_string())
    }

    fn modulo(&self, _other: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn min(&self, _other: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn max(&self, _other: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_val() {
            Val::Array(a) => {
                let elements: Vec<String> = a.iter()
                    .map(|v| v.to_string())
                    .collect();
                format!("[{}]", elements.join(", "))
            }
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }
}

pub static ARRAY_TYPE_INSTANCE: ArrayType = ArrayType; 