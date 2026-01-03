use bincode::{Decode, Encode};

use crate::types_db::value::{Val, Value};

// Every possible SQL type ID
// NOTE: `TypeId` derives `bincode::Encode/Decode`. Changing variant order/shape will change the
// serialized representation.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Encode, Decode)]
pub enum TypeId {
    Boolean   = 0,
    TinyInt   = 1,
    SmallInt  = 2,
    Integer   = 3,
    BigInt    = 4,
    Decimal   = 5,
    Float     = 6,
    Timestamp = 7,
    Date      = 8,
    Time      = 9,
    Interval  = 10,
    VarChar   = 11,
    Char      = 12,
    Binary    = 13,
    JSON      = 14,
    UUID      = 15,
    Vector    = 16,
    Array     = 17,
    Enum      = 18,
    Point     = 19,
    #[default]
    Invalid   = 20,
    Struct    = 21,
}

impl TypeId {
    pub fn create_value(self, val: Val) -> Value {
        Value::new_with_type(val, self)
    }

    /// Returns a default "zero" value for this type.
    pub fn default_value(self) -> Value {
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
            TypeId::Struct => Value::new_struct(Vec::<String>::new(), Vec::<Value>::new()),
        }
    }

    #[deprecated(note = "use TypeId::default_value()")]
    pub fn get_value(self) -> Value {
        self.default_value()
    }

    pub fn from_value(value: &Value) -> Self {
        // Important: Value can carry a *typed* NULL (Val::Null with non-Invalid type_id_).
        value.get_type_id()
    }
}
