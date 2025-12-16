use crate::types_db::value::{Val, Value};
use bincode::{Decode, Encode};

// Every possible SQL type ID
//
// IMPORTANT: This enum is used in on-disk formats via bincode. Do not reorder variants without
// assigning explicit, stable IDs (see `stable_id()`/`from_stable_id()` and the custom bincode impls).
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash, Default)]
pub enum TypeId {
    Boolean = 0,
    TinyInt = 1,
    SmallInt = 2,
    Integer = 3,
    BigInt = 4,
    Decimal = 5,
    Float = 6,
    Timestamp = 7,
    Date = 8,
    Time = 9,
    Interval = 10,
    VarChar = 11,
    Char = 12,
    Binary = 13,
    JSON = 14,
    UUID = 15,
    Vector = 16,
    Array = 17,
    Enum = 18,
    Point = 19,
    #[default]
    Invalid = 20,
    Struct = 21,
}

impl TypeId {
    /// Returns the stable on-disk ID for this type.
    #[inline]
    pub const fn stable_id(self) -> u32 {
        self as u32
    }

    /// Reconstructs a `TypeId` from its stable on-disk ID.
    ///
    /// Unknown IDs are mapped to `TypeId::Invalid` for forward compatibility.
    #[inline]
    pub const fn from_stable_id(id: u32) -> Self {
        match id {
            0 => TypeId::Boolean,
            1 => TypeId::TinyInt,
            2 => TypeId::SmallInt,
            3 => TypeId::Integer,
            4 => TypeId::BigInt,
            5 => TypeId::Decimal,
            6 => TypeId::Float,
            7 => TypeId::Timestamp,
            8 => TypeId::Date,
            9 => TypeId::Time,
            10 => TypeId::Interval,
            11 => TypeId::VarChar,
            12 => TypeId::Char,
            13 => TypeId::Binary,
            14 => TypeId::JSON,
            15 => TypeId::UUID,
            16 => TypeId::Vector,
            17 => TypeId::Array,
            18 => TypeId::Enum,
            19 => TypeId::Point,
            20 => TypeId::Invalid,
            21 => TypeId::Struct,
            _ => TypeId::Invalid,
        }
    }

    pub fn create_value(&self, val: Val) -> Value {
        Value::new_with_type(val, *self)
    }

    /// Returns a default "zero" value for this type.
    pub fn default_value(&self) -> Value {
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
            TypeId::Struct => Value::new_struct(Vec::new(), Vec::new()),
        }
    }

    #[deprecated(note = "use TypeId::default_value()")]
    pub fn get_value(&self) -> Value {
        self.default_value()
    }

    pub fn from_value(value: &Value) -> Self {
        // Important: Value can carry a *typed* NULL (Val::Null with non-Invalid type_id_).
        value.get_type_id()
    }
}

// ===== bincode stable encoding =====
//
// We encode `TypeId` as a single `u32` stable ID so the on-disk format does not depend on enum
// variant order.
impl Encode for TypeId {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        self.stable_id().encode(encoder)
    }
}

impl<C> Decode<C> for TypeId {
    fn decode<D: bincode::de::Decoder<Context = C>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let id: u32 = Decode::decode(decoder)?;
        Ok(TypeId::from_stable_id(id))
    }
}

impl<'de, C> bincode::BorrowDecode<'de, C> for TypeId {
    fn borrow_decode<D>(decoder: &mut D) -> Result<Self, bincode::error::DecodeError>
    where
        D: bincode::de::BorrowDecoder<'de, Context = C>,
    {
        let id: u32 = bincode::BorrowDecode::borrow_decode(decoder)?;
        Ok(TypeId::from_stable_id(id))
    }
}
