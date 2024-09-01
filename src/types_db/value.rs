use crate::types_db::type_id::TypeId;
use crate::types_db::types::CmpBool::{CmpFalse, CmpTrue};
use crate::types_db::types::{get_type_size, CmpBool, Type};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Debug, Display, Formatter};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Val {
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Decimal(f64),
    Timestamp(u64),
    VarLen(String),
    ConstVarLen(String),
    Vector(Vec<i32>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Size {
    Length(usize),
    ElemTypeId(TypeId),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Value {
    value_: Val,
    size_: Size,
    manage_data_: bool,
    type_id_: TypeId,
}

impl Value {
    pub fn new<T: Into<Val>>(value: T) -> Self {
        let val = value.into();
        let type_id = match &val {
            Val::Boolean(_) => TypeId::Boolean,
            Val::TinyInt(_) => TypeId::TinyInt,
            Val::SmallInt(_) => TypeId::SmallInt,
            Val::Integer(_) => TypeId::Integer,
            Val::BigInt(_) => TypeId::BigInt,
            Val::Decimal(_) => TypeId::Decimal,
            Val::Timestamp(_) => TypeId::Timestamp,
            Val::VarLen(_) | Val::ConstVarLen(_) => TypeId::VarChar,
            Val::Vector(_) => TypeId::Vector,
        };
        Value {
            value_: val,
            size_: Size::Length(get_type_size(type_id) as usize),
            manage_data_: false,
            type_id_: type_id,
        }
    }

    pub fn get_value(&self) -> &Val {
        &self.value_
    }

    pub fn get_storage_size(&self) -> u32 {
        match &self.value_ {
            Val::Boolean(_) => 1,
            Val::TinyInt(_) => 1,
            Val::SmallInt(_) => 2,
            Val::Integer(_) => 4,
            Val::BigInt(_) => 8,
            Val::Decimal(_) => 8,
            Val::Timestamp(_) => 8,
            Val::VarLen(s) | Val::ConstVarLen(s) => s.len() as u32,
            Val::Vector(v) => 4 + v.len() as u32 * 4,
        }
    }

    pub fn serialize_to(&self, storage: &mut [u8]) {
        self.value_.serialize_to(storage)
    }

    pub fn deserialize_from(storage: &[u8], type_id: TypeId) -> Self {
        Value {
            value_: Val::deserialize_from(storage, type_id),
            size_: Size::Length(storage.len()),
            manage_data_: false,
            type_id_: type_id,
        }
    }
}

impl Type for Value {
    fn get_type_id(&self) -> TypeId {
        self.type_id_
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match (&self.value_, &other.value_) {
            (_l, _r) => (_l == _r).into()
        }
    }

    fn compare_not_equals(&self, other: &Value) -> CmpBool {
        match (&self.value_, &other.value_) {
            (_l, _r) => (_l != _r).into()
        }
    }

    fn compare_less_than(&self, other: &Value) -> CmpBool {
        match (&self.value_, &other.value_) {
            (Val::Boolean(l), Val::Boolean(r)) => (l < r).into(),
            (Val::TinyInt(l), Val::TinyInt(r)) => (l < r).into(),
            (Val::SmallInt(l), Val::SmallInt(r)) => (l < r).into(),
            (Val::Integer(l), Val::Integer(r)) => (l < r).into(),
            (Val::BigInt(l), Val::BigInt(r)) => (l < r).into(),
            (Val::Decimal(l), Val::Decimal(r)) => (l < r).into(),
            (Val::Timestamp(l), Val::Timestamp(r)) => (l < r).into(),
            (Val::VarLen(l), Val::VarLen(r)) => (l < r).into(),
            (Val::ConstVarLen(l), Val::ConstVarLen(r)) => (l < r).into(),
            _ => CmpFalse,
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match (&self.value_, &other.value_) {
            (Val::Boolean(l), Val::Boolean(r)) => (l > r).into(),
            (Val::TinyInt(l), Val::TinyInt(r)) => (l > r).into(),
            (Val::SmallInt(l), Val::SmallInt(r)) => (l > r).into(),
            (Val::Integer(l), Val::Integer(r)) => (l > r).into(),
            (Val::BigInt(l), Val::BigInt(r)) => (l > r).into(),
            (Val::Decimal(l), Val::Decimal(r)) => (l > r).into(),
            (Val::Timestamp(l), Val::Timestamp(r)) => (l > r).into(),
            (Val::VarLen(l), Val::VarLen(r)) => (l > r).into(),
            (Val::ConstVarLen(l), Val::ConstVarLen(r)) => (l > r).into(),
            _ => CmpFalse,
        }
    }

    fn serialize_to(&self, _val: &Value, storage: &mut [u8]) {
        self.serialize_to(storage);
    }

    fn deserialize_from(&self, storage: &mut [u8]) -> Value {
        Value::deserialize_from(storage, self.type_id_)
    }
}

impl Val {
    pub fn serialize_to(&self, storage: &mut [u8]) {
        match self {
            Val::Boolean(b) => storage[0] = *b as u8,
            Val::TinyInt(i) => storage[0] = *i as u8,
            Val::SmallInt(i) => storage[..2].copy_from_slice(&i.to_le_bytes()),
            Val::Integer(i) => storage[..4].copy_from_slice(&i.to_le_bytes()),
            Val::BigInt(i) => storage[..8].copy_from_slice(&i.to_le_bytes()),
            Val::Decimal(f) => storage[..8].copy_from_slice(&f.to_le_bytes()),
            Val::Timestamp(t) => storage[..8].copy_from_slice(&t.to_le_bytes()),
            Val::VarLen(s) | Val::ConstVarLen(s) => {
                let bytes = s.as_bytes();
                storage[..bytes.len()].copy_from_slice(bytes);
            }
            Val::Vector(v) => {
                let len = v.len() as u32;
                storage[..4].copy_from_slice(&len.to_le_bytes());
                for (i, val) in v.iter().enumerate() {
                    let start = 4 + i * 4;
                    let end = start + 4;
                    storage[start..end].copy_from_slice(&val.to_le_bytes());
                }
            }
        }
    }

    pub fn deserialize_from(storage: &[u8], type_id: TypeId) -> Self {
        match type_id {
            TypeId::Boolean => Val::Boolean(storage[0] != 0),
            TypeId::TinyInt => Val::TinyInt(storage[0] as i8),
            TypeId::SmallInt => Val::SmallInt(i16::from_le_bytes([storage[0], storage[1]])),
            TypeId::Integer => Val::Integer(i32::from_le_bytes([
                storage[0], storage[1], storage[2], storage[3],
            ])),
            TypeId::BigInt => Val::BigInt(i64::from_le_bytes([
                storage[0], storage[1], storage[2], storage[3], storage[4], storage[5], storage[6],
                storage[7],
            ])),
            TypeId::Decimal => Val::Decimal(f64::from_le_bytes([
                storage[0], storage[1], storage[2], storage[3], storage[4], storage[5], storage[6],
                storage[7],
            ])),
            TypeId::Timestamp => Val::Timestamp(u64::from_le_bytes([
                storage[0], storage[1], storage[2], storage[3], storage[4], storage[5], storage[6],
                storage[7],
            ])),
            TypeId::VarChar => Val::VarLen(String::from_utf8_lossy(storage).to_string()),
            TypeId::Vector => {
                let len =
                    u32::from_le_bytes([storage[0], storage[1], storage[2], storage[3]]) as usize;
                let mut v = Vec::with_capacity(len);
                for i in 0..len {
                    let start = 4 + i * 4;
                    let _end = start + 4;
                    v.push(i32::from_le_bytes([
                        storage[start],
                        storage[start + 1],
                        storage[start + 2],
                        storage[start + 3],
                    ]));
                }
                Val::Vector(v)
            }
            _ => panic!("Unsupported type for deserialization"),
        }
    }
}

impl From<bool> for Val {
    fn from(b: bool) -> Self {
        Val::Boolean(b)
    }
}

impl From<i8> for Val {
    fn from(i: i8) -> Self {
        Val::TinyInt(i)
    }
}

impl From<i16> for Val {
    fn from(i: i16) -> Self {
        Val::SmallInt(i)
    }
}

impl From<i32> for Val {
    fn from(i: i32) -> Self {
        Val::Integer(i)
    }
}

impl From<i64> for Val {
    fn from(i: i64) -> Self {
        Val::BigInt(i)
    }
}

impl From<f64> for Val {
    fn from(f: f64) -> Self {
        Val::Decimal(f)
    }
}

impl From<u64> for Val {
    fn from(t: u64) -> Self {
        Val::Timestamp(t)
    }
}

impl From<String> for Val {
    fn from(s: String) -> Self {
        Val::VarLen(s)
    }
}

impl From<&str> for Val {
    fn from(s: &str) -> Self {
        Val::VarLen(s.to_string())
    }
}

impl From<Vec<i32>> for Val {
    fn from(v: Vec<i32>) -> Self {
        Val::Vector(v)
    }
}

// Implement trait for each type conversion to Value
pub trait ToValue {
    fn to_value(self) -> Value;
}

impl ToValue for bool {
    fn to_value(self) -> Value {
        Value {
            value_: Val::Boolean(self),
            size_: Size::Length(1),
            manage_data_: false,
            type_id_: TypeId::Boolean,
        }
    }
}

impl ToValue for i8 {
    fn to_value(self) -> Value {
        Value {
            value_: Val::TinyInt(self),
            size_: Size::Length(1),
            manage_data_: false,
            type_id_: TypeId::TinyInt,
        }
    }
}

impl ToValue for i16 {
    fn to_value(self) -> Value {
        Value {
            value_: Val::SmallInt(self),
            size_: Size::Length(2),
            manage_data_: false,
            type_id_: TypeId::SmallInt,
        }
    }
}

impl ToValue for i32 {
    fn to_value(self) -> Value {
        Value {
            value_: Val::Integer(self),
            size_: Size::Length(4),
            manage_data_: false,
            type_id_: TypeId::Integer,
        }
    }
}

impl ToValue for i64 {
    fn to_value(self) -> Value {
        Value {
            value_: Val::BigInt(self),
            size_: Size::Length(8),
            manage_data_: false,
            type_id_: TypeId::BigInt,
        }
    }
}

impl ToValue for f64 {
    fn to_value(self) -> Value {
        Value {
            value_: Val::Decimal(self),
            size_: Size::Length(8),
            manage_data_: false,
            type_id_: TypeId::Decimal,
        }
    }
}

impl ToValue for u64 {
    fn to_value(self) -> Value {
        Value {
            value_: Val::Timestamp(self),
            size_: Size::Length(8),
            manage_data_: false,
            type_id_: TypeId::Timestamp,
        }
    }
}

impl ToValue for &str {
    fn to_value(self) -> Value {
        Value {
            value_: Val::VarLen(self.to_string()),
            size_: Size::Length(self.len()),
            manage_data_: false,
            type_id_: TypeId::VarChar,
        }
    }
}

impl ToValue for Vec<i32> {
    fn to_value(self) -> Value {
        Value {
            value_: Val::Vector(self.clone()),
            size_: Size::Length(4 + self.len() * 4), // 4 bytes for the length + 4 bytes per i32
            manage_data_: false,
            type_id_: TypeId::Vector,
        }
    }
}

impl ToValue for String {
    fn to_value(self) -> Value {
        Value {
            value_: Val::VarLen(self.clone()),
            size_: Size::Length(self.len()),
            manage_data_: false,
            type_id_: TypeId::VarChar,
        }
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        b.to_value()
    }
}

impl From<i8> for Value {
    fn from(i: i8) -> Self {
        i.to_value()
    }
}

impl From<i16> for Value {
    fn from(i: i16) -> Self {
        i.to_value()
    }
}

impl From<i32> for Value {
    fn from(i: i32) -> Self {
        i.to_value()
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        i.to_value()
    }
}

impl From<f64> for Value {
    fn from(d: f64) -> Self {
        d.to_value()
    }
}

impl From<u64> for Value {
    fn from(t: u64) -> Self {
        t.to_value()
    }
}

impl From<&str> for Value {
    fn from(data: &str) -> Self {
        data.to_value()
    }
}

impl From<Vec<i32>> for Value {
    fn from(data: Vec<i32>) -> Self {
        data.to_value()
    }
}

impl From<String> for Value {
    fn from(data: String) -> Self {
        data.to_value()
    }
}

impl From<bool> for CmpBool {
    fn from(val: bool) -> Self {
        if val {
            CmpTrue
        } else {
            CmpFalse
        }
    }
}

impl From<CmpBool> for Val {
    fn from(cmp_bool: CmpBool) -> Self {
        match cmp_bool {
            CmpTrue => Val::Boolean(true),
            CmpFalse => Val::Boolean(false),
            CmpBool::CmpNull => Val::Boolean(false),
        }
    }
}

impl From<CmpBool> for Value {
    fn from(cmp_bool: CmpBool) -> Self {
        Value::new(Val::from(cmp_bool))
    }
}

pub trait Serializable: Debug {
    fn serialize_to(&self, storage: &mut [u8]);
    fn deserialize_from(storage: &[u8]) -> Self
    where
        Self: Sized;
}

impl Serializable for i32 {
    fn serialize_to(&self, storage: &mut [u8]) {
        let bytes = self.to_le_bytes();
        storage[..4].copy_from_slice(&bytes);
    }

    fn deserialize_from(storage: &[u8]) -> Self {
        i32::from_le_bytes([storage[0], storage[1], storage[2], storage[3]])
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.value_)
    }
}
