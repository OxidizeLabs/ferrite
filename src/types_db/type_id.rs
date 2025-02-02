use crate::types_db::value::{Val, Value};
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

// Every possible SQL type ID
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub enum TypeId {
    Boolean,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Decimal,
    Timestamp,
    VarChar,
    Char,
    Vector,
    Invalid,
}

// Implement the Default trait for TypeId
impl Default for TypeId {
    fn default() -> Self {
        TypeId::Invalid
    }
}

impl Hash for TypeId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use the discriminant of the enum for hashing
        std::mem::discriminant(self).hash(state);
    }
}

impl TypeId {
    pub fn create_value(&self, val: Val) -> Value {
        Value::new_with_type(val, *self)
    }

    pub fn get_value(&self) -> Value {
        match self {
            TypeId::Boolean => Value::new(Val::Boolean(false)),
            TypeId::TinyInt => Value::new(Val::TinyInt(0)),
            TypeId::SmallInt => Value::new(Val::SmallInt(0)),
            TypeId::Integer => Value::new(Val::Integer(0)),
            TypeId::BigInt => Value::new(Val::BigInt(0)),
            TypeId::Decimal => Value::new(Val::Decimal(0.0)),
            TypeId::Timestamp => Value::new(Val::Timestamp(0)),
            TypeId::VarChar => Value::new(Val::VarLen(String::new())),
            TypeId::Char => Value::new(Val::ConstLen(String::new())),
            TypeId::Vector => Value::new(Val::Vector(Vec::new())),
            TypeId::Invalid => Value::new(Val::Null),
        }
    }

    pub fn from_value(value: &Value) -> Self {
        match value.get_val() {
            Val::Boolean(_) => TypeId::Boolean,
            Val::TinyInt(_) => TypeId::TinyInt,
            Val::SmallInt(_) => TypeId::SmallInt,
            Val::Integer(_) => TypeId::Integer,
            Val::BigInt(_) => TypeId::BigInt,
            Val::Decimal(_) => TypeId::Decimal,
            Val::Timestamp(_) => TypeId::Timestamp,
            Val::VarLen(_) => TypeId::VarChar,
            Val::ConstLen(_) => TypeId::Char,
            Val::Vector(_) => TypeId::Vector,
            Val::Null => TypeId::Invalid,
        }
    }
}
