use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};

pub struct BigIntType;

impl BigIntType {
    pub fn new() -> Self {
        BigIntType
    }
}

impl Type for BigIntType {
    fn get_type_id(&self) -> TypeId {
        TypeId::BigInt
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match (self.get_type_id().get_value().value_, other.get_value()) {
            (Val::BigInt(l), Val::BigInt(r)) => (l == *r).into(),
            (Val::BigInt(l), Val::Integer(r)) => (l == *r as i64).into(),
            (Val::BigInt(_), Val::Null) => CmpBool::CmpNull,
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
        match (self.get_type_id().get_value().value_, other.get_value()) {
            (Val::BigInt(l), Val::BigInt(r)) => (l < *r).into(),
            (Val::BigInt(l), Val::Integer(r)) => (l < *r as i64).into(),
            (Val::BigInt(_), Val::Null) => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match (self.get_type_id().get_value().value_, other.get_value()) {
            (Val::BigInt(l), Val::BigInt(r)) => (l <= *r).into(),
            (Val::BigInt(l), Val::Integer(r)) => (l <= *r as i64).into(),
            (Val::BigInt(_), Val::Null) | (Val::Null, Val::BigInt(_)) => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match (self.get_type_id().get_value().value_, other.get_value()) {
            (Val::BigInt(l), Val::BigInt(r)) => (l > *r).into(),
            (Val::BigInt(l), Val::Integer(r)) => (l > *r as i64).into(),
            (Val::BigInt(_), Val::Null) | (Val::Null, Val::BigInt(_)) => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match (self.get_type_id().get_value().value_, other.get_value()) {
            (Val::BigInt(l), Val::BigInt(r)) => (l >= *r).into(),
            (Val::BigInt(l), Val::Integer(r)) => (l >= *r as i64).into(),
            (Val::BigInt(_), Val::Null) | (Val::Null, Val::BigInt(_)) => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn add(&self, other: &Value) -> Result<Value, String> {
        self.add(other)
    }

    fn subtract(&self, other: &Value) -> Result<Value, String> {
        match (self.get_type_id().get_value().value_, other.get_value()) {
            (Val::BigInt(l), Val::BigInt(r)) => {
                l.checked_sub(*r)
                    .map(|result| Value::new(Val::BigInt(result)))
                    .ok_or_else(|| "BigInt subtraction overflow".to_string())
            }
            (Val::BigInt(l), Val::Integer(r)) => {
                l.checked_sub(*r as i64)
                    .map(|result| Value::new(Val::BigInt(result)))
                    .ok_or_else(|| "BigInt subtraction overflow".to_string())
            }
            _ => Err("Invalid types for BigInt subtraction".to_string()),
        }
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_value() {
            Val::BigInt(n) => n.to_string(),
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }
}
