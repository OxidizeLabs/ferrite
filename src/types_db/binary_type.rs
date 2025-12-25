use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};

/// Implementation for BinaryType
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Debug)]
pub struct BinaryType;

impl Default for BinaryType {
    fn default() -> Self {
        Self::new()
    }
}

impl BinaryType {
    /// Creates a new `BinaryType` instance.
    pub fn new() -> Self {
        BinaryType
    }
}

impl Type for BinaryType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Binary
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Binary(r) => CmpBool::from(Vec::<u8>::new() == *r),
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
            Val::Binary(r) => CmpBool::from(Vec::<u8>::new() < *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Binary(r) => CmpBool::from(Vec::<u8>::new() <= *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Binary(r) => CmpBool::from(Vec::<u8>::new() > *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Binary(r) => CmpBool::from(Vec::<u8>::new() >= *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn add(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Binary(r) => {
                let mut result = Vec::new();
                result.extend_from_slice(r);
                Ok(Value::new(result))
            },
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot add non-binary values to Binary".to_string()),
        }
    }

    fn subtract(&self, _other: &Value) -> Result<Value, String> {
        Err("Subtraction not supported for Binary type".to_string())
    }

    fn multiply(&self, _other: &Value) -> Result<Value, String> {
        Err("Multiplication not supported for Binary type".to_string())
    }

    fn divide(&self, _other: &Value) -> Result<Value, String> {
        Err("Division not supported for Binary type".to_string())
    }

    fn modulo(&self, _other: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn min(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Binary(r) => {
                if Vec::<u8>::new() < *r {
                    Value::new(Vec::<u8>::new())
                } else {
                    Value::new(r.clone())
                }
            },
            _ => Value::new(Val::Null),
        }
    }

    fn max(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Binary(r) => {
                if Vec::<u8>::new() > *r {
                    Value::new(Vec::<u8>::new())
                } else {
                    Value::new(r.clone())
                }
            },
            _ => Value::new(Val::Null),
        }
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_val() {
            Val::Binary(b) => format!("\\x{}", hex::encode(b)),
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }
}

pub static BINARY_TYPE_INSTANCE: BinaryType = BinaryType;
