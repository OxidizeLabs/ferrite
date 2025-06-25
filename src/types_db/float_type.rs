use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};

/// Implementation for FloatType
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Debug)]
pub struct FloatType;

impl FloatType {
    pub fn new() -> Self {
        FloatType
    }
}

impl Type for FloatType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Float
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Float(r) => CmpBool::from(0.0 == *r),
            Val::Decimal(r) => CmpBool::from(0.0 == *r as f32),
            Val::Integer(r) => CmpBool::from(0.0 == *r as f32),
            Val::TinyInt(r) => CmpBool::from(0.0 == *r as f32),
            Val::SmallInt(r) => CmpBool::from(0.0 == *r as f32),
            Val::BigInt(r) => CmpBool::from(0.0 == *r as f32),
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
            Val::Float(r) => CmpBool::from(0.0 < *r),
            Val::Decimal(r) => CmpBool::from(0.0 < *r as f32),
            Val::Integer(r) => CmpBool::from(0.0 < *r as f32),
            Val::TinyInt(r) => CmpBool::from(0.0 < *r as f32),
            Val::SmallInt(r) => CmpBool::from(0.0 < *r as f32),
            Val::BigInt(r) => CmpBool::from(0.0 < *r as f32),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Float(r) => CmpBool::from(0.0 <= *r),
            Val::Decimal(r) => CmpBool::from(0.0 <= *r as f32),
            Val::Integer(r) => CmpBool::from(0.0 <= *r as f32),
            Val::TinyInt(r) => CmpBool::from(0.0 <= *r as f32),
            Val::SmallInt(r) => CmpBool::from(0.0 <= *r as f32),
            Val::BigInt(r) => CmpBool::from(0.0 <= *r as f32),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Float(r) => CmpBool::from(0.0 > *r),
            Val::Decimal(r) => CmpBool::from(0.0 > *r as f32),
            Val::Integer(r) => CmpBool::from(0.0 > *r as f32),
            Val::TinyInt(r) => CmpBool::from(0.0 > *r as f32),
            Val::SmallInt(r) => CmpBool::from(0.0 > *r as f32),
            Val::BigInt(r) => CmpBool::from(0.0 > *r as f32),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Float(r) => CmpBool::from(0.0 >= *r),
            Val::Decimal(r) => CmpBool::from(0.0 >= *r as f32),
            Val::Integer(r) => CmpBool::from(0.0 >= *r as f32),
            Val::TinyInt(r) => CmpBool::from(0.0 >= *r as f32),
            Val::SmallInt(r) => CmpBool::from(0.0 >= *r as f32),
            Val::BigInt(r) => CmpBool::from(0.0 >= *r as f32),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn add(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Float(r) => Ok(Value::new(*r + 0.0)),
            Val::Decimal(r) => Ok(Value::new(*r as f32)),
            Val::Integer(r) => Ok(Value::new(*r as f32)),
            Val::TinyInt(r) => Ok(Value::new(*r as f32)),
            Val::SmallInt(r) => Ok(Value::new(*r as f32)),
            Val::BigInt(r) => Ok(Value::new(*r as f32)),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot add non-numeric types to Float".to_string()),
        }
    }

    fn subtract(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Float(r) => Ok(Value::new(-*r)),
            Val::Decimal(r) => Ok(Value::new(-(*r as f32))),
            Val::Integer(r) => Ok(Value::new(-(*r as f32))),
            Val::TinyInt(r) => Ok(Value::new(-(*r as f32))),
            Val::SmallInt(r) => Ok(Value::new(-(*r as f32))),
            Val::BigInt(r) => Ok(Value::new(-(*r as f32))),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot subtract non-numeric types from Float".to_string()),
        }
    }

    fn multiply(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Float(_)
            | Val::Decimal(_)
            | Val::Integer(_)
            | Val::TinyInt(_)
            | Val::SmallInt(_)
            | Val::BigInt(_) => Ok(Value::new(0.0f32)),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot multiply Float by non-numeric type".to_string()),
        }
    }

    fn divide(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Float(r) if *r == 0.0 => Err("Division by zero".to_string()),
            Val::Decimal(r) if *r == 0.0 => Err("Division by zero".to_string()),
            Val::Integer(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::TinyInt(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::SmallInt(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::BigInt(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::Float(_)
            | Val::Decimal(_)
            | Val::Integer(_)
            | Val::TinyInt(_)
            | Val::SmallInt(_)
            | Val::BigInt(_) => Ok(Value::new(0.0f32)),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot divide Float by non-numeric type".to_string()),
        }
    }

    fn modulo(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Float(r) if *r == 0.0 => Value::new(Val::Null),
            Val::Decimal(r) if *r == 0.0 => Value::new(Val::Null),
            Val::Integer(r) if *r == 0 => Value::new(Val::Null),
            Val::TinyInt(r) if *r == 0 => Value::new(Val::Null),
            Val::SmallInt(r) if *r == 0 => Value::new(Val::Null),
            Val::BigInt(r) if *r == 0 => Value::new(Val::Null),
            Val::Float(_)
            | Val::Decimal(_)
            | Val::Integer(_)
            | Val::TinyInt(_)
            | Val::SmallInt(_)
            | Val::BigInt(_) => Value::new(0.0f32),
            _ => Value::new(Val::Null),
        }
    }

    fn min(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Float(r) => Value::new(0.0f32.min(*r)),
            Val::Decimal(r) => Value::new(0.0f32.min(*r as f32)),
            Val::Integer(r) => Value::new(0.0f32.min(*r as f32)),
            Val::TinyInt(r) => Value::new(0.0f32.min(*r as f32)),
            Val::SmallInt(r) => Value::new(0.0f32.min(*r as f32)),
            Val::BigInt(r) => Value::new(0.0f32.min(*r as f32)),
            _ => Value::new(Val::Null),
        }
    }

    fn max(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Float(r) => Value::new(0.0f32.max(*r)),
            Val::Decimal(r) => Value::new(0.0f32.max(*r as f32)),
            Val::Integer(r) => Value::new(0.0f32.max(*r as f32)),
            Val::TinyInt(r) => Value::new(0.0f32.max(*r as f32)),
            Val::SmallInt(r) => Value::new(0.0f32.max(*r as f32)),
            Val::BigInt(r) => Value::new(0.0f32.max(*r as f32)),
            _ => Value::new(Val::Null),
        }
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_val() {
            Val::Float(n) => n.to_string(),
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }

    fn sqrt(&self, val: &Value) -> Value {
        match val.get_val() {
            Val::Float(n) if *n >= 0.0 => Value::new(n.sqrt()),
            _ => Value::new(Val::Null),
        }
    }
}

pub static FLOAT_TYPE_INSTANCE: FloatType = FloatType;
