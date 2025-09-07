use crate::types_db::value::{Val, Value};
use std::hash::{Hash, Hasher};
use bincode::{Encode, Decode};

// Every possible SQL type ID
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Encode, Decode, Default)]
pub enum TypeId {
    Boolean,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Decimal,
    Float,
    Timestamp,
    Date,
    Time,
    Interval,
    VarChar,
    Char,
    Binary,
    JSON,
    UUID,
    Vector,
    Array,
    Enum,
    Point,
    #[default]
    Invalid,
    Struct,
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
            TypeId::Float => Value::new(Val::Float(0.0)),
            TypeId::Timestamp => Value::new(Val::Timestamp(0)),
            TypeId::Date => Value::new(Val::Date(0)),
            TypeId::Time => Value::new(Val::Time(0)),
            TypeId::Interval => Value::new(Val::Interval(0)),
            TypeId::VarChar => Value::new(Val::VarLen(String::new())),
            TypeId::Char => Value::new(Val::ConstLen(String::new())),
            TypeId::Binary => Value::new(Val::Binary(Vec::new())),
            TypeId::JSON => Value::new(Val::JSON(String::new())),
            TypeId::UUID => Value::new(Val::UUID(String::new())),
            TypeId::Vector => Value::new(Val::Vector(Vec::new())),
            TypeId::Array => Value::new(Val::Array(Vec::new())),
            TypeId::Enum => Value::new(Val::Enum(0, String::new())),
            TypeId::Point => Value::new(Val::Point(0.0, 0.0)),
            TypeId::Invalid => Value::new(Val::Null),
            TypeId::Struct => Value::new(Val::Null),
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
            Val::Float(_) => TypeId::Float,
            Val::Timestamp(_) => TypeId::Timestamp,
            Val::Date(_) => TypeId::Date,
            Val::Time(_) => TypeId::Time,
            Val::Interval(_) => TypeId::Interval,
            Val::VarLen(_) => TypeId::VarChar,
            Val::ConstLen(_) => TypeId::Char,
            Val::Binary(_) => TypeId::Binary,
            Val::JSON(_) => TypeId::JSON,
            Val::UUID(_) => TypeId::UUID,
            Val::Vector(_) => TypeId::Vector,
            Val::Array(_) => TypeId::Array,
            Val::Enum(_, _) => TypeId::Enum,
            Val::Point(_, _) => TypeId::Point,
            Val::Null => TypeId::Invalid,
            Val::Struct => TypeId::Struct,
        }
    }
}
