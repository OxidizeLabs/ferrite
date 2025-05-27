use crate::types_db::type_id::TypeId;
use crate::types_db::types::{get_type_size, CmpBool, Type};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Val {
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Decimal(f64),
    Float(f32),
    Timestamp(u64),
    Date(i32),
    Time(i32),
    Interval(i64),
    VarLen(String),
    ConstLen(String),
    Binary(Vec<u8>),
    JSON(String),
    UUID(String),
    Vector(Vec<Value>),
    Array(Vec<Value>),
    Enum(i32, String),
    Point(f64, f64),
    Null,
    Struct,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, PartialOrd)]
pub enum Size {
    Length(usize),
    ElemTypeId(TypeId),
}

#[derive(Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Value {
    pub value_: Val,
    pub size_: Size,
    pub manage_data_: bool,
    pub type_id_: TypeId,
    pub struct_data: Option<Vec<Value>>,
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
        };
        Value {
            value_: val,
            size_: Size::Length(get_type_size(type_id) as usize),
            manage_data_: false,
            type_id_: type_id,
            struct_data: None,
        }
    }

    pub fn new_vector<I>(iter: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<Value>,
    {
        let vec: Vec<Value> = iter.into_iter().map(Into::into).collect();
        Value {
            value_: Val::Vector(vec.clone()),
            size_: Size::Length(vec.len()),
            manage_data_: false,
            type_id_: TypeId::Vector,
            struct_data: None,
        }
    }

    pub fn get_val(&self) -> &Val {
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
            Val::Float(_) => 4,
            Val::Timestamp(_) => 8,
            Val::Date(_) => 4,
            Val::Time(_) => 4,
            Val::Interval(_) => 8,
            Val::VarLen(s) => s.len() as u32,
            Val::ConstLen(s) => s.len() as u32,
            Val::Binary(b) => b.len() as u32,
            Val::JSON(j) => j.len() as u32,
            Val::UUID(_) => 16,
            Val::Vector(v) => 4 + v.len() as u32 * 4,
            Val::Array(a) => 4 + a.len() as u32 * 4,
            Val::Enum(_, s) => 4 + s.len() as u32,
            Val::Point(_, _) => 16,
            Val::Null => 1,
            Val::Struct => 8, // Pointer size for struct
        }
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        match &self.value_ {
            Val::Boolean(b) => bytes.extend_from_slice(&[*b as u8]),
            Val::TinyInt(i) => bytes.extend_from_slice(&i.to_le_bytes()),
            Val::SmallInt(i) => bytes.extend_from_slice(&i.to_le_bytes()),
            Val::Integer(i) => bytes.extend_from_slice(&i.to_le_bytes()),
            Val::BigInt(i) => bytes.extend_from_slice(&i.to_le_bytes()),
            Val::Decimal(f) => bytes.extend_from_slice(&f.to_le_bytes()),
            Val::Float(f) => bytes.extend_from_slice(&f.to_le_bytes()),
            Val::Timestamp(t) => bytes.extend_from_slice(&t.to_le_bytes()),
            Val::Date(d) => bytes.extend_from_slice(&d.to_le_bytes()),
            Val::Time(t) => bytes.extend_from_slice(&t.to_le_bytes()),
            Val::Interval(i) => bytes.extend_from_slice(&i.to_le_bytes()),
            Val::VarLen(s) | Val::ConstLen(s) => bytes.extend_from_slice(s.as_bytes()),
            Val::Binary(b) => bytes.extend_from_slice(b),
            Val::JSON(j) => bytes.extend_from_slice(j.as_bytes()),
            Val::UUID(u) => bytes.extend_from_slice(u.as_bytes()),
            Val::Vector(v) | Val::Array(v) => {
                for value in v {
                    bytes.extend(value.as_bytes());
                }
            }
            Val::Enum(i, s) => {
                bytes.extend_from_slice(&i.to_le_bytes());
                bytes.extend_from_slice(s.as_bytes());
            }
            Val::Point(x, y) => {
                bytes.extend_from_slice(&x.to_le_bytes());
                bytes.extend_from_slice(&y.to_le_bytes());
            }
            Val::Null => bytes.extend_from_slice(&[0u8]),
            Val::Struct => bytes.extend_from_slice(&[0u8]), // Placeholder for struct
        }
        bytes
    }

    /// Returns true if this value represents NULL
    pub fn is_null(&self) -> bool {
        matches!(self.value_, Val::Null)
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self.value_,
            Val::Decimal(_)
                | Val::Float(_)
                | Val::TinyInt(_)
                | Val::SmallInt(_)
                | Val::Integer(_)
                | Val::BigInt(_)
        )
    }

    pub fn new_with_type(val: Val, type_id: TypeId) -> Self {
        Self {
            value_: val,
            size_: type_id.get_value().size_,
            manage_data_: false,
            type_id_: type_id,
            struct_data: None,
        }
    }

    pub fn get_type_id(&self) -> TypeId {
        self.type_id_
    }

    fn is_zero(&self, val: &Value) -> bool {
        match val.get_val() {
            Val::Integer(i) => *i == 0,
            Val::BigInt(i) => *i == 0,
            Val::SmallInt(i) => *i == 0,
            Val::TinyInt(i) => *i == 0,
            Val::Decimal(f) => *f == 0.0,
            Val::Float(f) => *f == 0.0,
            Val::Boolean(_) => false,
            Val::Timestamp(_) => false,
            Val::Date(_) => false,
            Val::Time(_) => false,
            Val::Interval(_) => false,
            Val::VarLen(s) => s.is_empty(),
            Val::ConstLen(s) => s.is_empty(),
            Val::Binary(b) => b.is_empty(),
            Val::JSON(_) => false,
            Val::UUID(_) => false,
            Val::Vector(v) => v.is_empty(),
            Val::Array(a) => a.is_empty(),
            Val::Enum(_, _) => false,
            Val::Point(_, _) => false,
            Val::Null => false,
            Val::Struct => false,
        }
    }

    /// Deserializes a Value from a byte slice according to the provided TypeId.
    pub fn deserialize_from(data: &[u8], column_type: TypeId) -> Self {
        use crate::types_db::type_id::TypeId::*;
        match column_type {
            Boolean => {
                if data.len() >= 1 {
                    Value::new(data[0] != 0)
                } else {
                    Value::new(Val::Null)
                }
            }
            TinyInt => {
                if data.len() >= 1 {
                    let arr: [u8; 1] = data[0..1].try_into().expect("slice with incorrect length");
                    Value::new(i8::from_le_bytes(arr))
                } else {
                    Value::new(Val::Null)
                }
            }
            SmallInt => {
                if data.len() >= 2 {
                    let arr: [u8; 2] = data[0..2].try_into().expect("slice with incorrect length");
                    Value::new(i16::from_le_bytes(arr))
                } else {
                    Value::new(Val::Null)
                }
            }
            Integer => {
                if data.len() >= 4 {
                    let arr: [u8; 4] = data[0..4].try_into().expect("slice with incorrect length");
                    Value::new(i32::from_le_bytes(arr))
                } else {
                    Value::new(Val::Null)
                }
            }
            BigInt => {
                if data.len() >= 8 {
                    let arr: [u8; 8] = data[0..8].try_into().expect("slice with incorrect length");
                    Value::new(i64::from_le_bytes(arr))
                } else {
                    Value::new(Val::Null)
                }
            }
            Decimal => {
                if data.len() >= 8 {
                    let arr: [u8; 8] = data[0..8].try_into().expect("slice with incorrect length");
                    Value::new(f64::from_le_bytes(arr))
                } else {
                    Value::new(Val::Null)
                }
            }
            Float => {
                if data.len() >= 4 {
                    let arr: [u8; 4] = data[0..4].try_into().expect("slice with incorrect length");
                    Value::new(f32::from_le_bytes(arr))
                } else {
                    Value::new(Val::Null)
                }
            }
            Timestamp => {
                if data.len() >= 8 {
                    let arr: [u8; 8] = data[0..8].try_into().expect("slice with incorrect length");
                    Value::new(u64::from_le_bytes(arr))
                } else {
                    Value::new(Val::Null)
                }
            }
            Date => {
                if data.len() >= 4 {
                    let arr: [u8; 4] = data[0..4].try_into().expect("slice with incorrect length");
                    Value::new_with_type(Val::Date(i32::from_le_bytes(arr)), TypeId::Date)
                } else {
                    Value::new(Val::Null)
                }
            }
            Time => {
                if data.len() >= 4 {
                    let arr: [u8; 4] = data[0..4].try_into().expect("slice with incorrect length");
                    Value::new_with_type(Val::Time(i32::from_le_bytes(arr)), TypeId::Time)
                } else {
                    Value::new(Val::Null)
                }
            }
            Interval => {
                if data.len() >= 8 {
                    let arr: [u8; 8] = data[0..8].try_into().expect("slice with incorrect length");
                    Value::new_with_type(Val::Interval(i64::from_le_bytes(arr)), TypeId::Interval)
                } else {
                    Value::new(Val::Null)
                }
            }
            VarChar | Char => match std::str::from_utf8(data) {
                Ok(s) => {
                    if column_type == VarChar {
                        Value::new(s)
                    } else {
                        Value::new_with_type(Val::ConstLen(s.to_string()), TypeId::Char)
                    }
                }
                Err(_) => Value::new(Val::Null),
            },
            Binary => Value::new_with_type(Val::Binary(data.to_vec()), TypeId::Binary),
            JSON => match std::str::from_utf8(data) {
                Ok(s) => Value::new_with_type(Val::JSON(s.to_string()), TypeId::JSON),
                Err(_) => Value::new(Val::Null),
            },
            UUID => match std::str::from_utf8(data) {
                Ok(s) => Value::new_with_type(Val::UUID(s.to_string()), TypeId::UUID),
                Err(_) => Value::new(Val::Null),
            },
            Point => {
                if data.len() >= 16 {
                    let x_arr: [u8; 8] =
                        data[0..8].try_into().expect("slice with incorrect length");
                    let y_arr: [u8; 8] =
                        data[8..16].try_into().expect("slice with incorrect length");
                    let x = f64::from_le_bytes(x_arr);
                    let y = f64::from_le_bytes(y_arr);
                    Value::new_with_type(Val::Point(x, y), TypeId::Point)
                } else {
                    Value::new(Val::Null)
                }
            }
            Vector => Value::new_with_type(Val::Vector(Vec::new()), Vector),
            Array => Value::new_with_type(Val::Array(Vec::new()), Array),
            Enum => Value::new_with_type(Val::Enum(0, String::new()), Enum),
            Invalid => Value::new(Val::Null),
            Struct => Value::new_with_type(Val::Struct, Struct),
        }
    }

    /// Attempts to cast this value to the specified type
    pub fn cast_to(&self, target_type: TypeId) -> Result<Value, String> {
        // If types are the same, return clone
        if self.type_id_ == target_type {
            return Ok(self.clone());
        }

        // Handle NULL values
        if self.is_null() {
            return Ok(Value::new_with_type(Val::Null, target_type));
        }

        match (&self.value_, target_type) {
            // ===== BOOLEAN CONVERSIONS =====
            (Val::Boolean(b), TypeId::TinyInt) => Ok(Value::new_with_type(Val::TinyInt(if *b { 1 } else { 0 }), target_type)),
            (Val::Boolean(b), TypeId::SmallInt) => Ok(Value::new_with_type(Val::SmallInt(if *b { 1 } else { 0 }), target_type)),
            (Val::Boolean(b), TypeId::Integer) => Ok(Value::new_with_type(Val::Integer(if *b { 1 } else { 0 }), target_type)),
            (Val::Boolean(b), TypeId::BigInt) => Ok(Value::new_with_type(Val::BigInt(if *b { 1 } else { 0 }), target_type)),
            (Val::Boolean(b), TypeId::Decimal) => Ok(Value::new_with_type(Val::Decimal(if *b { 1.0 } else { 0.0 }), target_type)),
            (Val::Boolean(b), TypeId::Float) => Ok(Value::new_with_type(Val::Float(if *b { 1.0 } else { 0.0 }), target_type)),
            (Val::Boolean(b), TypeId::VarChar) => Ok(Value::new_with_type(Val::VarLen(b.to_string()), target_type)),
            (Val::Boolean(b), TypeId::Char) => Ok(Value::new_with_type(Val::ConstLen(b.to_string()), target_type)),

            // ===== NUMERIC CONVERSIONS =====
            // TinyInt conversions
            (Val::TinyInt(i), TypeId::Boolean) => Ok(Value::new_with_type(Val::Boolean(*i != 0), target_type)),
            (Val::TinyInt(i), TypeId::SmallInt) => Ok(Value::new_with_type(Val::SmallInt(*i as i16), target_type)),
            (Val::TinyInt(i), TypeId::Integer) => Ok(Value::new_with_type(Val::Integer(*i as i32), target_type)),
            (Val::TinyInt(i), TypeId::BigInt) => Ok(Value::new_with_type(Val::BigInt(*i as i64), target_type)),
            (Val::TinyInt(i), TypeId::Decimal) => Ok(Value::new_with_type(Val::Decimal(*i as f64), target_type)),
            (Val::TinyInt(i), TypeId::Float) => Ok(Value::new_with_type(Val::Float(*i as f32), target_type)),
            (Val::TinyInt(i), TypeId::VarChar) => Ok(Value::new_with_type(Val::VarLen(i.to_string()), target_type)),
            (Val::TinyInt(i), TypeId::Char) => Ok(Value::new_with_type(Val::ConstLen(i.to_string()), target_type)),

            // SmallInt conversions
            (Val::SmallInt(i), TypeId::Boolean) => Ok(Value::new_with_type(Val::Boolean(*i != 0), target_type)),
            (Val::SmallInt(i), TypeId::TinyInt) => {
                if *i >= i8::MIN as i16 && *i <= i8::MAX as i16 {
                    Ok(Value::new_with_type(Val::TinyInt(*i as i8), target_type))
                } else {
                    Err(format!("SmallInt value {} out of range for TinyInt", i))
                }
            }
            (Val::SmallInt(i), TypeId::Integer) => Ok(Value::new_with_type(Val::Integer(*i as i32), target_type)),
            (Val::SmallInt(i), TypeId::BigInt) => Ok(Value::new_with_type(Val::BigInt(*i as i64), target_type)),
            (Val::SmallInt(i), TypeId::Decimal) => Ok(Value::new_with_type(Val::Decimal(*i as f64), target_type)),
            (Val::SmallInt(i), TypeId::Float) => Ok(Value::new_with_type(Val::Float(*i as f32), target_type)),
            (Val::SmallInt(i), TypeId::VarChar) => Ok(Value::new_with_type(Val::VarLen(i.to_string()), target_type)),
            (Val::SmallInt(i), TypeId::Char) => Ok(Value::new_with_type(Val::ConstLen(i.to_string()), target_type)),

            // Integer conversions
            (Val::Integer(i), TypeId::Boolean) => Ok(Value::new_with_type(Val::Boolean(*i != 0), target_type)),
            (Val::Integer(i), TypeId::TinyInt) => {
                if *i >= i8::MIN as i32 && *i <= i8::MAX as i32 {
                    Ok(Value::new_with_type(Val::TinyInt(*i as i8), target_type))
                } else {
                    Err(format!("Integer value {} out of range for TinyInt", i))
                }
            }
            (Val::Integer(i), TypeId::SmallInt) => {
                if *i >= i16::MIN as i32 && *i <= i16::MAX as i32 {
                    Ok(Value::new_with_type(Val::SmallInt(*i as i16), target_type))
                } else {
                    Err(format!("Integer value {} out of range for SmallInt", i))
                }
            }
            (Val::Integer(i), TypeId::BigInt) => Ok(Value::new_with_type(Val::BigInt(*i as i64), target_type)),
            (Val::Integer(i), TypeId::Decimal) => Ok(Value::new_with_type(Val::Decimal(*i as f64), target_type)),
            (Val::Integer(i), TypeId::Float) => Ok(Value::new_with_type(Val::Float(*i as f32), target_type)),
            (Val::Integer(i), TypeId::Timestamp) => {
                if *i >= 0 {
                    Ok(Value::new_with_type(Val::Timestamp(*i as u64), target_type))
                } else {
                    Err(format!("Cannot convert negative integer {} to Timestamp", i))
                }
            }
            (Val::Integer(i), TypeId::VarChar) => Ok(Value::new_with_type(Val::VarLen(i.to_string()), target_type)),
            (Val::Integer(i), TypeId::Char) => Ok(Value::new_with_type(Val::ConstLen(i.to_string()), target_type)),

            // BigInt conversions
            (Val::BigInt(i), TypeId::Boolean) => Ok(Value::new_with_type(Val::Boolean(*i != 0), target_type)),
            (Val::BigInt(i), TypeId::TinyInt) => {
                if *i >= i8::MIN as i64 && *i <= i8::MAX as i64 {
                    Ok(Value::new_with_type(Val::TinyInt(*i as i8), target_type))
                } else {
                    Err(format!("BigInt value {} out of range for TinyInt", i))
                }
            }
            (Val::BigInt(i), TypeId::SmallInt) => {
                if *i >= i16::MIN as i64 && *i <= i16::MAX as i64 {
                    Ok(Value::new_with_type(Val::SmallInt(*i as i16), target_type))
                } else {
                    Err(format!("BigInt value {} out of range for SmallInt", i))
                }
            }
            (Val::BigInt(i), TypeId::Integer) => {
                if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 {
                    Ok(Value::new_with_type(Val::Integer(*i as i32), target_type))
                } else {
                    Err(format!("BigInt value {} out of range for Integer", i))
                }
            }
            (Val::BigInt(i), TypeId::Decimal) => Ok(Value::new_with_type(Val::Decimal(*i as f64), target_type)),
            (Val::BigInt(i), TypeId::Float) => Ok(Value::new_with_type(Val::Float(*i as f32), target_type)),
            (Val::BigInt(i), TypeId::Timestamp) => {
                if *i >= 0 {
                    Ok(Value::new_with_type(Val::Timestamp(*i as u64), target_type))
                } else {
                    Err(format!("Cannot convert negative bigint {} to Timestamp", i))
                }
            }
            (Val::BigInt(i), TypeId::VarChar) => Ok(Value::new_with_type(Val::VarLen(i.to_string()), target_type)),
            (Val::BigInt(i), TypeId::Char) => Ok(Value::new_with_type(Val::ConstLen(i.to_string()), target_type)),

            // Decimal conversions
            (Val::Decimal(f), TypeId::Boolean) => Ok(Value::new_with_type(Val::Boolean(*f != 0.0), target_type)),
            (Val::Decimal(f), TypeId::TinyInt) => Ok(Value::new_with_type(Val::TinyInt(*f as i8), target_type)),
            (Val::Decimal(f), TypeId::SmallInt) => Ok(Value::new_with_type(Val::SmallInt(*f as i16), target_type)),
            (Val::Decimal(f), TypeId::Integer) => Ok(Value::new_with_type(Val::Integer(*f as i32), target_type)),
            (Val::Decimal(f), TypeId::BigInt) => Ok(Value::new_with_type(Val::BigInt(*f as i64), target_type)),
            (Val::Decimal(f), TypeId::Float) => Ok(Value::new_with_type(Val::Float(*f as f32), target_type)),
            (Val::Decimal(f), TypeId::VarChar) => Ok(Value::new_with_type(Val::VarLen(f.to_string()), target_type)),
            (Val::Decimal(f), TypeId::Char) => Ok(Value::new_with_type(Val::ConstLen(f.to_string()), target_type)),

            // Float conversions
            (Val::Float(f), TypeId::Boolean) => Ok(Value::new_with_type(Val::Boolean(*f != 0.0), target_type)),
            (Val::Float(f), TypeId::TinyInt) => Ok(Value::new_with_type(Val::TinyInt(*f as i8), target_type)),
            (Val::Float(f), TypeId::SmallInt) => Ok(Value::new_with_type(Val::SmallInt(*f as i16), target_type)),
            (Val::Float(f), TypeId::Integer) => Ok(Value::new_with_type(Val::Integer(*f as i32), target_type)),
            (Val::Float(f), TypeId::BigInt) => Ok(Value::new_with_type(Val::BigInt(*f as i64), target_type)),
            (Val::Float(f), TypeId::Decimal) => Ok(Value::new_with_type(Val::Decimal(*f as f64), target_type)),
            (Val::Float(f), TypeId::VarChar) => Ok(Value::new_with_type(Val::VarLen(f.to_string()), target_type)),
            (Val::Float(f), TypeId::Char) => Ok(Value::new_with_type(Val::ConstLen(f.to_string()), target_type)),

            // ===== STRING CONVERSIONS =====
            (Val::VarLen(s), TypeId::Char) => Ok(Value::new_with_type(Val::ConstLen(s.clone()), target_type)),
            (Val::VarLen(s), TypeId::Boolean) => {
                match s.to_lowercase().as_str() {
                    "true" | "t" | "1" | "yes" | "y" => Ok(Value::new_with_type(Val::Boolean(true), target_type)),
                    "false" | "f" | "0" | "no" | "n" | "" => Ok(Value::new_with_type(Val::Boolean(false), target_type)),
                    _ => Err(format!("Cannot convert string '{}' to Boolean", s))
                }
            }
            (Val::VarLen(s), TypeId::TinyInt) => {
                s.parse::<i8>().map(|i| Value::new_with_type(Val::TinyInt(i), target_type))
                    .map_err(|e| format!("Cannot convert string '{}' to TinyInt: {}", s, e))
            }
            (Val::VarLen(s), TypeId::SmallInt) => {
                s.parse::<i16>().map(|i| Value::new_with_type(Val::SmallInt(i), target_type))
                    .map_err(|e| format!("Cannot convert string '{}' to SmallInt: {}", s, e))
            }
            (Val::VarLen(s), TypeId::Integer) => {
                s.parse::<i32>().map(|i| Value::new_with_type(Val::Integer(i), target_type))
                    .map_err(|e| format!("Cannot convert string '{}' to Integer: {}", s, e))
            }
            (Val::VarLen(s), TypeId::BigInt) => {
                s.parse::<i64>().map(|i| Value::new_with_type(Val::BigInt(i), target_type))
                    .map_err(|e| format!("Cannot convert string '{}' to BigInt: {}", s, e))
            }
            (Val::VarLen(s), TypeId::Decimal) => {
                s.parse::<f64>().map(|f| Value::new_with_type(Val::Decimal(f), target_type))
                    .map_err(|e| format!("Cannot convert string '{}' to Decimal: {}", s, e))
            }
            (Val::VarLen(s), TypeId::Float) => {
                s.parse::<f32>().map(|f| Value::new_with_type(Val::Float(f), target_type))
                    .map_err(|e| format!("Cannot convert string '{}' to Float: {}", s, e))
            }
            (Val::VarLen(s), TypeId::Timestamp) => {
                s.parse::<u64>().map(|t| Value::new_with_type(Val::Timestamp(t), target_type))
                    .map_err(|e| format!("Cannot convert string '{}' to Timestamp: {}", s, e))
            }
            (Val::VarLen(s), TypeId::Date) => {
                s.parse::<i32>().map(|d| Value::new_with_type(Val::Date(d), target_type))
                    .map_err(|e| format!("Cannot convert string '{}' to Date: {}", s, e))
            }
            (Val::VarLen(s), TypeId::Time) => {
                s.parse::<i32>().map(|t| Value::new_with_type(Val::Time(t), target_type))
                    .map_err(|e| format!("Cannot convert string '{}' to Time: {}", s, e))
            }
            (Val::VarLen(s), TypeId::Interval) => {
                s.parse::<i64>().map(|i| Value::new_with_type(Val::Interval(i), target_type))
                    .map_err(|e| format!("Cannot convert string '{}' to Interval: {}", s, e))
            }
            (Val::VarLen(s), TypeId::Binary) => Ok(Value::new_with_type(Val::Binary(s.as_bytes().to_vec()), target_type)),
            (Val::VarLen(s), TypeId::JSON) => Ok(Value::new_with_type(Val::JSON(s.clone()), target_type)),
            (Val::VarLen(s), TypeId::UUID) => Ok(Value::new_with_type(Val::UUID(s.clone()), target_type)),

            (Val::ConstLen(s), TypeId::VarChar) => Ok(Value::new_with_type(Val::VarLen(s.clone()), target_type)),
            (Val::ConstLen(s), TypeId::Boolean) => {
                match s.to_lowercase().as_str() {
                    "true" | "t" | "1" | "yes" | "y" => Ok(Value::new_with_type(Val::Boolean(true), target_type)),
                    "false" | "f" | "0" | "no" | "n" | "" => Ok(Value::new_with_type(Val::Boolean(false), target_type)),
                    _ => Err(format!("Cannot convert string '{}' to Boolean", s))
                }
            }
            (Val::ConstLen(s), TypeId::TinyInt) => {
                s.parse::<i8>().map(|i| Value::new_with_type(Val::TinyInt(i), target_type))
                    .map_err(|e| format!("Cannot convert string '{}' to TinyInt: {}", s, e))
            }
            (Val::ConstLen(s), TypeId::SmallInt) => {
                s.parse::<i16>().map(|i| Value::new_with_type(Val::SmallInt(i), target_type))
                    .map_err(|e| format!("Cannot convert string '{}' to SmallInt: {}", s, e))
            }
            (Val::ConstLen(s), TypeId::Integer) => {
                s.parse::<i32>().map(|i| Value::new_with_type(Val::Integer(i), target_type))
                    .map_err(|e| format!("Cannot convert string '{}' to Integer: {}", s, e))
            }
            (Val::ConstLen(s), TypeId::BigInt) => {
                s.parse::<i64>().map(|i| Value::new_with_type(Val::BigInt(i), target_type))
                    .map_err(|e| format!("Cannot convert string '{}' to BigInt: {}", s, e))
            }
            (Val::ConstLen(s), TypeId::Decimal) => {
                s.parse::<f64>().map(|f| Value::new_with_type(Val::Decimal(f), target_type))
                    .map_err(|e| format!("Cannot convert string '{}' to Decimal: {}", s, e))
            }
            (Val::ConstLen(s), TypeId::Float) => {
                s.parse::<f32>().map(|f| Value::new_with_type(Val::Float(f), target_type))
                    .map_err(|e| format!("Cannot convert string '{}' to Float: {}", s, e))
            }
            (Val::ConstLen(s), TypeId::Timestamp) => {
                s.parse::<u64>().map(|t| Value::new_with_type(Val::Timestamp(t), target_type))
                    .map_err(|e| format!("Cannot convert string '{}' to Timestamp: {}", s, e))
            }
            (Val::ConstLen(s), TypeId::Date) => {
                s.parse::<i32>().map(|d| Value::new_with_type(Val::Date(d), target_type))
                    .map_err(|e| format!("Cannot convert string '{}' to Date: {}", s, e))
            }
            (Val::ConstLen(s), TypeId::Time) => {
                s.parse::<i32>().map(|t| Value::new_with_type(Val::Time(t), target_type))
                    .map_err(|e| format!("Cannot convert string '{}' to Time: {}", s, e))
            }
            (Val::ConstLen(s), TypeId::Interval) => {
                s.parse::<i64>().map(|i| Value::new_with_type(Val::Interval(i), target_type))
                    .map_err(|e| format!("Cannot convert string '{}' to Interval: {}", s, e))
            }
            (Val::ConstLen(s), TypeId::Binary) => Ok(Value::new_with_type(Val::Binary(s.as_bytes().to_vec()), target_type)),
            (Val::ConstLen(s), TypeId::JSON) => Ok(Value::new_with_type(Val::JSON(s.clone()), target_type)),
            (Val::ConstLen(s), TypeId::UUID) => Ok(Value::new_with_type(Val::UUID(s.clone()), target_type)),

            // ===== TEMPORAL TYPE CONVERSIONS =====
            (Val::Timestamp(t), TypeId::VarChar) => Ok(Value::new_with_type(Val::VarLen(t.to_string()), target_type)),
            (Val::Timestamp(t), TypeId::Char) => Ok(Value::new_with_type(Val::ConstLen(t.to_string()), target_type)),
            (Val::Timestamp(t), TypeId::BigInt) => Ok(Value::new_with_type(Val::BigInt(*t as i64), target_type)),
            (Val::Timestamp(t), TypeId::Integer) => {
                if *t <= i32::MAX as u64 {
                    Ok(Value::new_with_type(Val::Integer(*t as i32), target_type))
                } else {
                    Err(format!("Timestamp value {} out of range for Integer", t))
                }
            }

            (Val::Date(d), TypeId::VarChar) => Ok(Value::new_with_type(Val::VarLen(d.to_string()), target_type)),
            (Val::Date(d), TypeId::Char) => Ok(Value::new_with_type(Val::ConstLen(d.to_string()), target_type)),
            (Val::Date(d), TypeId::Integer) => Ok(Value::new_with_type(Val::Integer(*d), target_type)),
            (Val::Date(d), TypeId::BigInt) => Ok(Value::new_with_type(Val::BigInt(*d as i64), target_type)),

            (Val::Time(t), TypeId::VarChar) => Ok(Value::new_with_type(Val::VarLen(t.to_string()), target_type)),
            (Val::Time(t), TypeId::Char) => Ok(Value::new_with_type(Val::ConstLen(t.to_string()), target_type)),
            (Val::Time(t), TypeId::Integer) => Ok(Value::new_with_type(Val::Integer(*t), target_type)),
            (Val::Time(t), TypeId::BigInt) => Ok(Value::new_with_type(Val::BigInt(*t as i64), target_type)),

            (Val::Interval(i), TypeId::VarChar) => Ok(Value::new_with_type(Val::VarLen(i.to_string()), target_type)),
            (Val::Interval(i), TypeId::Char) => Ok(Value::new_with_type(Val::ConstLen(i.to_string()), target_type)),
            (Val::Interval(i), TypeId::BigInt) => Ok(Value::new_with_type(Val::BigInt(*i), target_type)),
            (Val::Interval(i), TypeId::Integer) => {
                if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 {
                    Ok(Value::new_with_type(Val::Integer(*i as i32), target_type))
                } else {
                    Err(format!("Interval value {} out of range for Integer", i))
                }
            }

            // ===== BINARY CONVERSIONS =====
            (Val::Binary(b), TypeId::VarChar) => {
                match String::from_utf8(b.clone()) {
                    Ok(s) => Ok(Value::new_with_type(Val::VarLen(s), target_type)),
                    Err(_) => Err("Cannot convert binary data to string: invalid UTF-8".to_string())
                }
            }
            (Val::Binary(b), TypeId::Char) => {
                match String::from_utf8(b.clone()) {
                    Ok(s) => Ok(Value::new_with_type(Val::ConstLen(s), target_type)),
                    Err(_) => Err("Cannot convert binary data to string: invalid UTF-8".to_string())
                }
            }

            // ===== JSON CONVERSIONS =====
            (Val::JSON(j), TypeId::VarChar) => Ok(Value::new_with_type(Val::VarLen(j.clone()), target_type)),
            (Val::JSON(j), TypeId::Char) => Ok(Value::new_with_type(Val::ConstLen(j.clone()), target_type)),

            // ===== UUID CONVERSIONS =====
            (Val::UUID(u), TypeId::VarChar) => Ok(Value::new_with_type(Val::VarLen(u.clone()), target_type)),
            (Val::UUID(u), TypeId::Char) => Ok(Value::new_with_type(Val::ConstLen(u.clone()), target_type)),

            // ===== ENUM CONVERSIONS =====
            (Val::Enum(id, s), TypeId::Integer) => Ok(Value::new_with_type(Val::Integer(*id), target_type)),
            (Val::Enum(id, s), TypeId::BigInt) => Ok(Value::new_with_type(Val::BigInt(*id as i64), target_type)),
            (Val::Enum(id, s), TypeId::VarChar) => Ok(Value::new_with_type(Val::VarLen(s.clone()), target_type)),
            (Val::Enum(id, s), TypeId::Char) => Ok(Value::new_with_type(Val::ConstLen(s.clone()), target_type)),

            // ===== POINT CONVERSIONS =====
            (Val::Point(x, y), TypeId::VarChar) => Ok(Value::new_with_type(Val::VarLen(format!("({}, {})", x, y)), target_type)),
            (Val::Point(x, y), TypeId::Char) => Ok(Value::new_with_type(Val::ConstLen(format!("({}, {})", x, y)), target_type)),

            // ===== VECTOR/ARRAY CONVERSIONS =====
            (Val::Vector(v), TypeId::Array) => Ok(Value::new_with_type(Val::Array(v.clone()), target_type)),
            (Val::Array(a), TypeId::Vector) => Ok(Value::new_with_type(Val::Vector(a.clone()), target_type)),

            // ===== NULL HANDLING =====
            (Val::Null, _) => Ok(Value::new_with_type(Val::Null, target_type)),

            // ===== UNSUPPORTED CONVERSIONS =====
            // These conversions are not supported and will return errors
            (Val::Struct, _) => Err(format!("Cannot cast Struct to {:?}", target_type)),
            (Val::Vector(_), _) => Err(format!("Cannot cast Vector to {:?} (only Array is supported)", target_type)),
            (Val::Array(_), _) => Err(format!("Cannot cast Array to {:?} (only Vector is supported)", target_type)),
            (Val::Binary(_), _) => Err(format!("Cannot cast Binary to {:?} (only string types are supported)", target_type)),
            (Val::JSON(_), _) => Err(format!("Cannot cast JSON to {:?} (only string types are supported)", target_type)),
            (Val::UUID(_), _) => Err(format!("Cannot cast UUID to {:?} (only string types are supported)", target_type)),
            (Val::Enum(_, _), _) => Err(format!("Cannot cast Enum to {:?} (only Integer, BigInt, and string types are supported)", target_type)),
            (Val::Point(_, _), _) => Err(format!("Cannot cast Point to {:?} (only string types are supported)", target_type)),
            (Val::Timestamp(_), _) => Err(format!("Cannot cast Timestamp to {:?} (only string and integer types are supported)", target_type)),
            (Val::Date(_), _) => Err(format!("Cannot cast Date to {:?} (only string and integer types are supported)", target_type)),
            (Val::Time(_), _) => Err(format!("Cannot cast Time to {:?} (only string and integer types are supported)", target_type)),
            (Val::Interval(_), _) => Err(format!("Cannot cast Interval to {:?} (only string and integer types are supported)", target_type)),

            // Catch-all for any other type combinations
            (val, typ) => Err(format!("Cannot cast {:?} to {:?}", val, typ)),
        }
    }

    /// Creates a new struct value with the given field names and values
    pub fn new_struct<I, S>(field_names: Vec<S>, values: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<Value>,
        S: Into<String>,
    {
        let values: Vec<Value> = values.into_iter().map(Into::into).collect();

        // Store the field names in the first element of the vector
        let field_names_value = Value::new_vector(
            field_names
                .into_iter()
                .map(|name| Value::new(name.into()))
                .collect::<Vec<_>>(),
        );

        // Create a vector with field names as first element, followed by values
        let mut struct_vec = Vec::with_capacity(values.len() + 1);
        struct_vec.push(field_names_value);
        struct_vec.extend(values);

        Value {
            value_: Val::Struct,
            size_: Size::Length(struct_vec.len()),
            manage_data_: false,
            type_id_: TypeId::Struct,
            struct_data: Some(struct_vec),
        }
    }

    /// Gets a field from a struct by name
    pub fn get_struct_field(&self, field_name: &str) -> Option<&Value> {
        if let Some(struct_data) = &self.struct_data {
            if struct_data.is_empty() {
                return None;
            }

            // First element contains field names
            if let Val::Vector(field_names) = &struct_data[0].value_ {
                // Find the index of the field name
                for (i, name_value) in field_names.iter().enumerate() {
                    if let Val::VarLen(name) | Val::ConstLen(name) = &name_value.value_ {
                        if name == field_name {
                            // Return the value at the corresponding index (offset by 1)
                            return struct_data.get(i + 1);
                        }
                    }
                }
            }
        }
        None
    }

    /// Checks if this value is a struct
    pub fn is_struct(&self) -> bool {
        self.type_id_ == TypeId::Struct && self.struct_data.is_some()
    }

    /// Gets all field names of the struct
    pub fn get_struct_field_names(&self) -> Vec<String> {
        if let Some(struct_data) = &self.struct_data {
            if let Some(first) = struct_data.first() {
                if let Val::Vector(field_names) = &first.value_ {
                    return field_names
                        .iter()
                        .filter_map(|v| {
                            if let Val::VarLen(name) | Val::ConstLen(name) = &v.value_ {
                                Some(name.clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                }
            }
        }
        Vec::new()
    }

    /// Gets all field values of the struct
    pub fn get_struct_values(&self) -> Vec<&Value> {
        if let Some(struct_data) = &self.struct_data {
            // Skip the first element (field names) and return all values
            return struct_data.iter().skip(1).collect();
        }
        Vec::new()
    }

    /// An optimized clone that avoids deep copying when possible
    pub fn clone_optimized(&self) -> Value {
        // For simple scalar types, we don't need to clone deep structures
        match &self.value_ {
            // Primitive types can be copied inexpensively
            Val::Boolean(b) => Value::new(*b),
            Val::TinyInt(i) => Value::new(*i),
            Val::SmallInt(i) => Value::new(*i),
            Val::Integer(i) => Value::new(*i),
            Val::BigInt(i) => Value::new(*i),
            Val::Decimal(d) => Value::new(*d),
            Val::Float(f) => Value::new(*f),
            Val::Timestamp(t) => Value::new(*t),
            Val::Date(d) => Value::new_with_type(Val::Date(*d), TypeId::Date),
            Val::Time(t) => Value::new_with_type(Val::Time(*t), TypeId::Time),
            Val::Interval(i) => Value::new_with_type(Val::Interval(*i), TypeId::Interval),
            Val::Point(x, y) => Value::new_with_type(Val::Point(*x, *y), TypeId::Point),
            Val::Null => Value::new(Val::Null),

            // For these types, we need to clone but can potentially optimize later
            // with reference counting or other techniques
            Val::VarLen(s) => Value::new_with_type(Val::VarLen(s.clone()), TypeId::VarChar),
            Val::ConstLen(s) => Value::new_with_type(Val::ConstLen(s.clone()), TypeId::Char),
            Val::Binary(b) => Value::new_with_type(Val::Binary(b.clone()), TypeId::Binary),
            Val::JSON(j) => Value::new_with_type(Val::JSON(j.clone()), TypeId::JSON),
            Val::UUID(u) => Value::new_with_type(Val::UUID(u.clone()), TypeId::UUID),
            Val::Vector(v) => Value::new_with_type(Val::Vector(v.clone()), TypeId::Vector),
            Val::Array(a) => Value::new_with_type(Val::Array(a.clone()), TypeId::Array),
            Val::Enum(id, s) => Value::new_with_type(Val::Enum(*id, s.clone()), TypeId::Enum),
            Val::Struct => {
                let mut result = Value::new_with_type(Val::Struct, TypeId::Struct);
                result.struct_data = self.struct_data.clone();
                result
            }
        }
    }

    fn copy(&self, val: &Value) -> Value {
        val.clone_optimized()
    }

    /// Helper function to convert a sqlparser::ast::Value to a string
    pub fn from_sqlparser_value(value: &sqlparser::ast::Value) -> Result<String, String> {
        match value {
            sqlparser::ast::Value::SingleQuotedString(s) => Ok(s.clone()),
            sqlparser::ast::Value::DoubleQuotedString(s) => Ok(s.clone()),
            sqlparser::ast::Value::DollarQuotedString(s) => Ok(s.value.clone()),
            sqlparser::ast::Value::TripleSingleQuotedString(s) => Ok(s.clone()),
            sqlparser::ast::Value::TripleDoubleQuotedString(s) => Ok(s.clone()),
            sqlparser::ast::Value::EscapedStringLiteral(s) => Ok(s.clone()),
            sqlparser::ast::Value::UnicodeStringLiteral(s) => Ok(s.clone()),
            sqlparser::ast::Value::NationalStringLiteral(s) => Ok(s.clone()),
            #[cfg(not(feature = "bigdecimal"))]
            sqlparser::ast::Value::Number(s, _) => Ok(s.clone()),
            #[cfg(feature = "bigdecimal")]
            sqlparser::ast::Value::Number(s, _) => Ok(s.to_string()),
            sqlparser::ast::Value::Boolean(b) => Ok(b.to_string()),
            sqlparser::ast::Value::Null => Ok("NULL".to_string()),
            sqlparser::ast::Value::Placeholder(s) => Ok(s.clone()),
            _ => Err(format!("Cannot convert {:?} to string", value)),
        }
    }

    /// Returns a technical representation of the value (e.g., "TIMESTAMP(42)")
    pub fn technical_display(&self) -> String {
        // Use the appropriate type's technical_display method
        let type_instance = crate::types_db::types::get_instance(self.type_id_);
        type_instance.technical_display(self)
    }
}

impl Type for Value {
    fn get_type_id(&self) -> TypeId {
        self.type_id_
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match (&self.value_, &other.value_) {
            (Val::Null, _) | (_, Val::Null) => CmpBool::CmpNull,
            _ => {
                // Check if types match before comparing values
                if self.type_id_ != other.type_id_ {
                    CmpBool::CmpFalse
                } else {
                    (self.value_ == other.value_).into()
                }
            }
        }
    }

    fn compare_not_equals(&self, other: &Value) -> CmpBool {
        match (&self.value_, &other.value_) {
            (Val::Null, _) | (_, Val::Null) => CmpBool::CmpNull,
            _ => {
                // Check if types match before comparing values
                if self.type_id_ != other.type_id_ {
                    CmpBool::CmpTrue
                } else {
                    (self.value_ != other.value_).into()
                }
            }
        }
    }

    fn compare_less_than(&self, other: &Value) -> CmpBool {
        match (&self.value_, &other.value_) {
            (Val::Null, _) | (_, Val::Null) => CmpBool::CmpNull,
            // Vector and Array types don't support ordering comparisons
            (Val::Vector(_), _) | (_, Val::Vector(_)) => CmpBool::CmpFalse,
            (Val::Array(_), _) | (_, Val::Array(_)) => CmpBool::CmpFalse,
            // Handle cross-type numeric comparisons
            (Val::Integer(a), Val::Decimal(b)) => CmpBool::from((*a as f64) < *b),
            (Val::Decimal(a), Val::Integer(b)) => CmpBool::from(*a < (*b as f64)),
            (Val::Integer(a), Val::BigInt(b)) => CmpBool::from((*a as i64) < *b),
            (Val::BigInt(a), Val::Integer(b)) => CmpBool::from(*a < (*b as i64)),
            (Val::Decimal(a), Val::BigInt(b)) => CmpBool::from(*a < (*b as f64)),
            (Val::BigInt(a), Val::Decimal(b)) => CmpBool::from((*a as f64) < *b),
            (Val::SmallInt(a), Val::Integer(b)) => CmpBool::from((*a as i32) < *b),
            (Val::Integer(a), Val::SmallInt(b)) => CmpBool::from(*a < (*b as i32)),
            (Val::TinyInt(a), Val::Integer(b)) => CmpBool::from((*a as i32) < *b),
            (Val::Integer(a), Val::TinyInt(b)) => CmpBool::from(*a < (*b as i32)),
            _ => {
                // Check if types match before comparing values
                if self.type_id_ != other.type_id_ {
                    CmpBool::CmpFalse
                } else {
                    (self.value_ < other.value_).into()
                }
            }
        }
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match (&self.value_, &other.value_) {
            (Val::Null, _) | (_, Val::Null) => CmpBool::CmpNull,
            // Vector and Array types don't support ordering comparisons
            (Val::Vector(_), _) | (_, Val::Vector(_)) => CmpBool::CmpFalse,
            (Val::Array(_), _) | (_, Val::Array(_)) => CmpBool::CmpFalse,
            // Handle cross-type numeric comparisons
            (Val::Integer(a), Val::Decimal(b)) => CmpBool::from((*a as f64) <= *b),
            (Val::Decimal(a), Val::Integer(b)) => CmpBool::from(*a <= (*b as f64)),
            (Val::Integer(a), Val::BigInt(b)) => CmpBool::from((*a as i64) <= *b),
            (Val::BigInt(a), Val::Integer(b)) => CmpBool::from(*a <= (*b as i64)),
            (Val::Decimal(a), Val::BigInt(b)) => CmpBool::from(*a <= (*b as f64)),
            (Val::BigInt(a), Val::Decimal(b)) => CmpBool::from((*a as f64) <= *b),
            (Val::SmallInt(a), Val::Integer(b)) => CmpBool::from((*a as i32) <= *b),
            (Val::Integer(a), Val::SmallInt(b)) => CmpBool::from(*a <= (*b as i32)),
            (Val::TinyInt(a), Val::Integer(b)) => CmpBool::from((*a as i32) <= *b),
            (Val::Integer(a), Val::TinyInt(b)) => CmpBool::from(*a <= (*b as i32)),
            _ => {
                // Check if types match before comparing values
                if self.type_id_ != other.type_id_ {
                    CmpBool::CmpFalse
                } else {
                    (self.value_ <= other.value_).into()
                }
            }
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match (&self.value_, &other.value_) {
            (Val::Null, _) | (_, Val::Null) => CmpBool::CmpNull,
            // Vector and Array types don't support ordering comparisons
            (Val::Vector(_), _) | (_, Val::Vector(_)) => CmpBool::CmpFalse,
            (Val::Array(_), _) | (_, Val::Array(_)) => CmpBool::CmpFalse,
            // Handle cross-type numeric comparisons
            (Val::Integer(a), Val::Decimal(b)) => CmpBool::from(*a as f64 > *b),
            (Val::Decimal(a), Val::Integer(b)) => CmpBool::from(*a > *b as f64),
            (Val::Integer(a), Val::BigInt(b)) => CmpBool::from(*a as i64 > *b),
            (Val::BigInt(a), Val::Integer(b)) => CmpBool::from(*a > *b as i64),
            (Val::Decimal(a), Val::BigInt(b)) => CmpBool::from(*a > *b as f64),
            (Val::BigInt(a), Val::Decimal(b)) => CmpBool::from(*a as f64 > *b),
            (Val::SmallInt(a), Val::Integer(b)) => CmpBool::from(*a as i32 > *b),
            (Val::Integer(a), Val::SmallInt(b)) => CmpBool::from(*a > *b as i32),
            (Val::TinyInt(a), Val::Integer(b)) => CmpBool::from(*a as i32 > *b),
            (Val::Integer(a), Val::TinyInt(b)) => CmpBool::from(*a > *b as i32),
            _ => {
                // Check if types match before comparing values
                if self.type_id_ != other.type_id_ {
                    CmpBool::CmpFalse
                } else {
                    (self.value_ > other.value_).into()
                }
            }
        }
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match (&self.value_, &other.value_) {
            (Val::Null, _) | (_, Val::Null) => CmpBool::CmpNull,
            // Vector and Array types don't support ordering comparisons
            (Val::Vector(_), _) | (_, Val::Vector(_)) => CmpBool::CmpFalse,
            (Val::Array(_), _) | (_, Val::Array(_)) => CmpBool::CmpFalse,
            // Handle cross-type numeric comparisons
            (Val::Integer(a), Val::Decimal(b)) => CmpBool::from((*a as f64) >= *b),
            (Val::Decimal(a), Val::Integer(b)) => CmpBool::from(*a >= (*b as f64)),
            (Val::Integer(a), Val::BigInt(b)) => CmpBool::from((*a as i64) >= *b),
            (Val::BigInt(a), Val::Integer(b)) => CmpBool::from(*a >= (*b as i64)),
            (Val::Decimal(a), Val::BigInt(b)) => CmpBool::from(*a >= (*b as f64)),
            (Val::BigInt(a), Val::Decimal(b)) => CmpBool::from((*a as f64) >= *b),
            (Val::SmallInt(a), Val::Integer(b)) => CmpBool::from((*a as i32) >= *b),
            (Val::Integer(a), Val::SmallInt(b)) => CmpBool::from(*a >= (*b as i32)),
            (Val::TinyInt(a), Val::Integer(b)) => CmpBool::from((*a as i32) >= *b),
            (Val::Integer(a), Val::TinyInt(b)) => CmpBool::from(*a >= (*b as i32)),
            _ => {
                // Check if types match before comparing values
                if self.type_id_ != other.type_id_ {
                    CmpBool::CmpFalse
                } else {
                    (self.value_ >= other.value_).into()
                }
            }
        }
    }

    fn add(&self, other: &Value) -> Result<Value, String> {
        if self.is_null() || other.is_null() {
            return Ok(Value::new(Val::Null));
        }

        match (&self.value_, &other.value_) {
            (Val::Integer(a), Val::Integer(b)) => {
                Ok(Value::new_with_type(Val::Integer(a + b), TypeId::Integer))
            }
            (Val::BigInt(a), Val::BigInt(b)) => {
                Ok(Value::new_with_type(Val::BigInt(a + b), TypeId::BigInt))
            }
            (Val::Integer(a), Val::BigInt(b)) => Ok(Value::new_with_type(
                Val::BigInt(*a as i64 + b),
                TypeId::BigInt,
            )),
            (Val::BigInt(a), Val::Integer(b)) => Ok(Value::new_with_type(
                Val::BigInt(a + *b as i64),
                TypeId::BigInt,
            )),
            (Val::Decimal(a), Val::Decimal(b)) => {
                Ok(Value::new_with_type(Val::Decimal(a + b), TypeId::Decimal))
            }
            (Val::SmallInt(a), Val::SmallInt(b)) => {
                Ok(Value::new_with_type(Val::SmallInt(a + b), TypeId::SmallInt))
            }
            (Val::TinyInt(a), Val::TinyInt(b)) => {
                Ok(Value::new_with_type(Val::TinyInt(a + b), TypeId::TinyInt))
            }
            // Promote smaller types to larger ones
            (Val::TinyInt(a), Val::Integer(b)) => Ok(Value::new_with_type(
                Val::Integer(*a as i32 + b),
                TypeId::Integer,
            )),
            (Val::Integer(a), Val::TinyInt(b)) => Ok(Value::new_with_type(
                Val::Integer(a + *b as i32),
                TypeId::Integer,
            )),
            (Val::SmallInt(a), Val::Integer(b)) => Ok(Value::new_with_type(
                Val::Integer(*a as i32 + b),
                TypeId::Integer,
            )),
            (Val::Integer(a), Val::SmallInt(b)) => Ok(Value::new_with_type(
                Val::Integer(a + *b as i32),
                TypeId::Integer,
            )),

            (Val::Boolean(_), _) => Err("Cannot add Boolean values".to_string()),
            (Val::Float(a), Val::Float(b)) => {
                Ok(Value::new_with_type(Val::Float(a + b), TypeId::Float))
            }
            (Val::Timestamp(_), _) => Err("Cannot add Timestamp values".to_string()),
            (Val::Date(_), _) => Err("Cannot add Date values".to_string()),
            (Val::Time(_), _) => Err("Cannot add Time values".to_string()),
            (Val::Interval(a), Val::Interval(b)) => {
                Ok(Value::new_with_type(Val::Interval(a + b), TypeId::Interval))
            }
            (Val::VarLen(a), Val::VarLen(b)) => Ok(Value::new_with_type(
                Val::VarLen(a.clone() + b),
                TypeId::VarChar,
            )),
            (Val::ConstLen(a), Val::ConstLen(b)) => Ok(Value::new_with_type(
                Val::ConstLen(a.clone() + b),
                TypeId::Char,
            )),
            (Val::Binary(_), _) => Err("Cannot add Binary values".to_string()),
            (Val::JSON(_), _) => Err("Cannot add JSON values".to_string()),
            (Val::UUID(_), _) => Err("Cannot add UUID values".to_string()),
            (Val::Vector(_), _) => Err("Cannot add Vector values".to_string()),
            (Val::Array(_), _) => Err("Cannot add Array values".to_string()),
            (Val::Enum(_, _), _) => Err("Cannot add Enum values".to_string()),
            (Val::Point(_, _), _) => Err("Cannot add Point values".to_string()),
            (Val::Null, _) => Ok(Value::new(Val::Null)),
            (Val::Struct, _) => Err("Cannot add Struct values".to_string()),
            (_, Val::Boolean(_)) => Err("Cannot add to Boolean values".to_string()),
            (_, Val::Timestamp(_)) => Err("Cannot add to Timestamp values".to_string()),
            (_, Val::Date(_)) => Err("Cannot add to Date values".to_string()),
            (_, Val::Time(_)) => Err("Cannot add to Time values".to_string()),
            (_, Val::Binary(_)) => Err("Cannot add to Binary values".to_string()),
            (_, Val::JSON(_)) => Err("Cannot add to JSON values".to_string()),
            (_, Val::UUID(_)) => Err("Cannot add to UUID values".to_string()),
            (_, Val::Vector(_)) => Err("Cannot add to Vector values".to_string()),
            (_, Val::Array(_)) => Err("Cannot add to Array values".to_string()),
            (_, Val::Enum(_, _)) => Err("Cannot add to Enum values".to_string()),
            (_, Val::Point(_, _)) => Err("Cannot add to Point values".to_string()),
            (_, Val::Null) => Ok(Value::new(Val::Null)),
            (_, Val::Struct) => Err("Cannot add to Struct values".to_string()),
            // Catch-all for any other type combinations
            (a, b) => Err(format!("Cannot add {:?} and {:?}", a, b)),
        }
    }

    fn subtract(&self, other: &Value) -> Result<Value, String> {
        match (self.get_type_id(), other.get_type_id()) {
            (TypeId::Integer, TypeId::Integer) => {
                let a = self.as_integer()?;
                let b = other.as_integer()?;
                Ok(Value::new_with_type(Val::Integer(a - b), TypeId::Integer))
            }
            (TypeId::BigInt, TypeId::BigInt) => {
                let a = self.as_bigint()?;
                let b = other.as_bigint()?;
                Ok(Value::new_with_type(Val::BigInt(a - b), TypeId::BigInt))
            }
            (TypeId::SmallInt, TypeId::SmallInt) => {
                let a = self.as_smallint()?;
                let b = other.as_smallint()?;
                Ok(Value::new_with_type(Val::SmallInt(a - b), TypeId::SmallInt))
            }
            (TypeId::TinyInt, TypeId::TinyInt) => {
                let a = self.as_tinyint()?;
                let b = other.as_tinyint()?;
                Ok(Value::new_with_type(Val::TinyInt(a - b), TypeId::TinyInt))
            }
            (TypeId::Decimal, TypeId::Decimal) => {
                let a = self.as_decimal()?;
                let b = other.as_decimal()?;
                Ok(Value::new_with_type(Val::Decimal(a - b), TypeId::Decimal))
            }
            (TypeId::Boolean, _) => Err(format!("Cannot subtract from Boolean type")),
            (TypeId::Float, TypeId::Float) => {
                let a = self.get_val();
                let b = other.get_val();
                if let (Val::Float(a_val), Val::Float(b_val)) = (a, b) {
                    Ok(Value::new_with_type(
                        Val::Float(a_val - b_val),
                        TypeId::Float,
                    ))
                } else {
                    Err(format!("Invalid Float values for subtraction"))
                }
            }
            (TypeId::Timestamp, _) => Err(format!("Cannot subtract from Timestamp type")),
            (TypeId::Date, _) => Err(format!("Cannot subtract from Date type")),
            (TypeId::Time, _) => Err(format!("Cannot subtract from Time type")),
            (TypeId::Interval, TypeId::Interval) => {
                let a = self.get_val();
                let b = other.get_val();
                if let (Val::Interval(a_val), Val::Interval(b_val)) = (a, b) {
                    Ok(Value::new_with_type(
                        Val::Interval(a_val - b_val),
                        TypeId::Interval,
                    ))
                } else {
                    Err(format!("Invalid Interval values for subtraction"))
                }
            }
            (TypeId::VarChar, _) => Err(format!("Cannot subtract from VarChar type")),
            (TypeId::Char, _) => Err(format!("Cannot subtract from Char type")),
            (TypeId::Binary, _) => Err(format!("Cannot subtract from Binary type")),
            (TypeId::JSON, _) => Err(format!("Cannot subtract from JSON type")),
            (TypeId::UUID, _) => Err(format!("Cannot subtract from UUID type")),
            (TypeId::Vector, _) => Err(format!("Cannot subtract from Vector type")),
            (TypeId::Array, _) => Err(format!("Cannot subtract from Array type")),
            (TypeId::Enum, _) => Err(format!("Cannot subtract from Enum type")),
            (TypeId::Point, _) => Err(format!("Cannot subtract from Point type")),
            (TypeId::Invalid, _) => Err(format!("Cannot subtract from Invalid type")),
            (TypeId::Struct, _) => Err(format!("Cannot subtract from Struct type")),
            (_, _) => Err(format!(
                "Cannot subtract values of types {:?} and {:?}",
                self.get_type_id(),
                other.get_type_id()
            )),
        }
    }

    fn multiply(&self, other: &Value) -> Result<Value, String> {
        match (self.get_type_id(), other.get_type_id()) {
            (TypeId::Integer, TypeId::Integer) => {
                let a = self.as_integer()?;
                let b = other.as_integer()?;
                Ok(Value::new_with_type(Val::Integer(a * b), TypeId::Integer))
            }
            (TypeId::BigInt, TypeId::BigInt) => {
                let a = self.as_bigint()?;
                let b = other.as_bigint()?;
                Ok(Value::new_with_type(Val::BigInt(a * b), TypeId::BigInt))
            }
            (TypeId::SmallInt, TypeId::SmallInt) => {
                let a = self.as_smallint()?;
                let b = other.as_smallint()?;
                Ok(Value::new_with_type(Val::SmallInt(a * b), TypeId::SmallInt))
            }
            (TypeId::TinyInt, TypeId::TinyInt) => {
                let a = self.as_tinyint()?;
                let b = other.as_tinyint()?;
                Ok(Value::new_with_type(Val::TinyInt(a * b), TypeId::TinyInt))
            }
            (TypeId::Decimal, TypeId::Decimal) => {
                let a = self.as_decimal()?;
                let b = other.as_decimal()?;
                Ok(Value::new_with_type(Val::Decimal(a * b), TypeId::Decimal))
            }
            (TypeId::Boolean, _) => Err(format!("Cannot multiply Boolean type")),
            (TypeId::Float, TypeId::Float) => {
                let a = self.get_val();
                let b = other.get_val();
                if let (Val::Float(a_val), Val::Float(b_val)) = (a, b) {
                    Ok(Value::new_with_type(
                        Val::Float(a_val * b_val),
                        TypeId::Float,
                    ))
                } else {
                    Err(format!("Invalid Float values for multiplication"))
                }
            }
            (TypeId::Timestamp, _) => Err(format!("Cannot multiply Timestamp type")),
            (TypeId::Date, _) => Err(format!("Cannot multiply Date type")),
            (TypeId::Time, _) => Err(format!("Cannot multiply Time type")),
            (TypeId::Interval, _) => Err(format!("Cannot multiply Interval type")),
            (TypeId::VarChar, _) => Err(format!("Cannot multiply VarChar type")),
            (TypeId::Char, _) => Err(format!("Cannot multiply Char type")),
            (TypeId::Binary, _) => Err(format!("Cannot multiply Binary type")),
            (TypeId::JSON, _) => Err(format!("Cannot multiply JSON type")),
            (TypeId::UUID, _) => Err(format!("Cannot multiply UUID type")),
            (TypeId::Vector, _) => Err(format!("Cannot multiply Vector type")),
            (TypeId::Array, _) => Err(format!("Cannot multiply Array type")),
            (TypeId::Enum, _) => Err(format!("Cannot multiply Enum type")),
            (TypeId::Point, _) => Err(format!("Cannot multiply Point type")),
            (TypeId::Invalid, _) => Err(format!("Cannot multiply Invalid type")),
            (TypeId::Struct, _) => Err(format!("Cannot multiply Struct type")),
            (_, _) => Err(format!(
                "Cannot multiply values of types {:?} and {:?}",
                self.get_type_id(),
                other.get_type_id()
            )),
        }
    }

    fn divide(&self, other: &Value) -> Result<Value, String> {
        if self.is_zero(other) {
            return Err("Division by zero".to_string());
        }
        match (self.get_type_id(), other.get_type_id()) {
            (TypeId::Integer, TypeId::Integer) => {
                let a = self.as_integer()?;
                let b = other.as_integer()?;
                Ok(Value::new_with_type(Val::Integer(a / b), TypeId::Integer))
            }
            (TypeId::BigInt, TypeId::BigInt) => {
                let a = self.as_bigint()?;
                let b = other.as_bigint()?;
                Ok(Value::new_with_type(Val::BigInt(a / b), TypeId::BigInt))
            }
            (TypeId::SmallInt, TypeId::SmallInt) => {
                let a = self.as_smallint()?;
                let b = other.as_smallint()?;
                Ok(Value::new_with_type(Val::SmallInt(a / b), TypeId::SmallInt))
            }
            (TypeId::TinyInt, TypeId::TinyInt) => {
                let a = self.as_tinyint()?;
                let b = other.as_tinyint()?;
                Ok(Value::new_with_type(Val::TinyInt(a / b), TypeId::TinyInt))
            }
            (TypeId::Decimal, TypeId::Decimal) => {
                let a = self.as_decimal()?;
                let b = other.as_decimal()?;
                Ok(Value::new_with_type(Val::Decimal(a / b), TypeId::Decimal))
            }
            (TypeId::Boolean, _) => Err(format!("Cannot divide Boolean type")),
            (TypeId::Float, TypeId::Float) => {
                let a = self.get_val();
                let b = other.get_val();
                if let (Val::Float(a_val), Val::Float(b_val)) = (a, b) {
                    if *b_val == 0.0 {
                        return Err("Division by zero".to_string());
                    }
                    Ok(Value::new_with_type(
                        Val::Float(a_val / b_val),
                        TypeId::Float,
                    ))
                } else {
                    Err(format!("Invalid Float values for division"))
                }
            }
            (TypeId::Timestamp, _) => Err(format!("Cannot divide Timestamp type")),
            (TypeId::Date, _) => Err(format!("Cannot divide Date type")),
            (TypeId::Time, _) => Err(format!("Cannot divide Time type")),
            (TypeId::Interval, _) => Err(format!("Cannot divide Interval type")),
            (TypeId::VarChar, _) => Err(format!("Cannot divide VarChar type")),
            (TypeId::Char, _) => Err(format!("Cannot divide Char type")),
            (TypeId::Binary, _) => Err(format!("Cannot divide Binary type")),
            (TypeId::JSON, _) => Err(format!("Cannot divide JSON type")),
            (TypeId::UUID, _) => Err(format!("Cannot divide UUID type")),
            (TypeId::Vector, _) => Err(format!("Cannot divide Vector type")),
            (TypeId::Array, _) => Err(format!("Cannot divide Array type")),
            (TypeId::Enum, _) => Err(format!("Cannot divide Enum type")),
            (TypeId::Point, _) => Err(format!("Cannot divide Point type")),
            (TypeId::Invalid, _) => Err(format!("Cannot divide Invalid type")),
            (TypeId::Struct, _) => Err(format!("Cannot divide Struct type")),
            (_, _) => Err(format!(
                "Cannot divide values of types {:?} and {:?}",
                self.get_type_id(),
                other.get_type_id()
            )),
        }
    }

    fn modulo(&self, other: &Value) -> Value {
        if self.is_zero(other) {
            return Value::new(Val::Null);
        }
        match (self.get_type_id(), other.get_type_id()) {
            (TypeId::Integer, TypeId::Integer) => {
                if let (Ok(a), Ok(b)) = (self.as_integer(), other.as_integer()) {
                    Value::new_with_type(Val::Integer(a % b), TypeId::Integer)
                } else {
                    Value::new(Val::Null)
                }
            }
            _ => Value::new(Val::Null),
        }
    }

    fn min(&self, other: &Value) -> Value {
        match self.compare_less_than(other) {
            CmpBool::CmpTrue => self.clone_optimized(),
            CmpBool::CmpFalse => other.clone_optimized(),
            CmpBool::CmpNull => Value::new(Val::Null),
        }
    }

    fn max(&self, other: &Value) -> Value {
        match self.compare_greater_than(other) {
            CmpBool::CmpTrue => self.clone_optimized(),
            CmpBool::CmpFalse => other.clone_optimized(),
            CmpBool::CmpNull => Value::new(Val::Null),
        }
    }

    fn to_string(&self, val: &Value) -> String {
        ToString::to_string(&val)
    }

    fn as_integer(&self) -> Result<i32, String> {
        match &self.value_ {
            Val::Integer(i) => Ok(*i),
            Val::TinyInt(i) => Ok(*i as i32),
            Val::SmallInt(i) => Ok(*i as i32),
            Val::BigInt(i) => Ok(*i as i32),
            Val::Decimal(f) => Ok(*f as i32),
            Val::Float(f) => Ok(*f as i32),
            Val::Boolean(b) => Ok(if *b { 1 } else { 0 }),
            Val::Timestamp(t) => Ok(*t as i32),
            Val::Date(d) => Ok(*d),
            Val::Time(t) => Ok(*t),
            Val::Interval(i) => Ok(*i as i32),
            Val::VarLen(s) | Val::ConstLen(s) => s
                .parse()
                .map_err(|e| format!("Cannot convert string to integer: {}", e)),
            Val::Binary(_) => Err("Cannot convert binary data to integer".to_string()),
            Val::JSON(_) => Err("Cannot convert JSON to integer".to_string()),
            Val::UUID(_) => Err("Cannot convert UUID to integer".to_string()),
            Val::Vector(v) | Val::Array(v) => Ok(v.len() as i32),
            Val::Enum(id, _) => Ok(*id),
            Val::Point(_, _) => Err("Cannot convert point to integer".to_string()),
            Val::Null => Err("Cannot convert NULL to integer".to_string()),
            Val::Struct => Ok(1), // Struct is considered as 1
        }
    }

    fn as_bigint(&self) -> Result<i64, String> {
        match &self.value_ {
            Val::BigInt(i) => Ok(*i),
            Val::Integer(i) => Ok(*i as i64),
            Val::SmallInt(i) => Ok(*i as i64),
            Val::TinyInt(i) => Ok(*i as i64),
            Val::Decimal(f) => Ok(*f as i64),
            Val::Float(f) => Ok(*f as i64),
            Val::Boolean(b) => Ok(if *b { 1 } else { 0 }),
            Val::Timestamp(t) => Ok(*t as i64),
            Val::Date(d) => Ok(*d as i64),
            Val::Time(t) => Ok(*t as i64),
            Val::Interval(i) => Ok(*i),
            Val::VarLen(s) | Val::ConstLen(s) => s
                .parse()
                .map_err(|e| format!("Cannot convert string to bigint: {}", e)),
            Val::Binary(_) => Err("Cannot convert binary data to bigint".to_string()),
            Val::JSON(_) => Err("Cannot convert JSON to bigint".to_string()),
            Val::UUID(_) => Err("Cannot convert UUID to bigint".to_string()),
            Val::Vector(v) | Val::Array(v) => Ok(v.len() as i64),
            Val::Enum(id, _) => Ok(*id as i64),
            Val::Point(_, _) => Err("Cannot convert point to bigint".to_string()),
            Val::Null => Err("Cannot convert NULL to bigint".to_string()),
            Val::Struct => Ok(1), // Struct is considered as 1
        }
    }

    fn as_smallint(&self) -> Result<i16, String> {
        match &self.value_ {
            Val::SmallInt(i) => Ok(*i),
            Val::TinyInt(i) => Ok(*i as i16),
            Val::Integer(i) => Ok(*i as i16),
            Val::BigInt(i) => Ok(*i as i16),
            Val::Decimal(f) => Ok(*f as i16),
            Val::Float(f) => Ok(*f as i16),
            Val::Boolean(b) => Ok(if *b { 1 } else { 0 }),
            Val::Timestamp(t) => Ok(*t as i16),
            Val::Date(d) => Ok(*d as i16),
            Val::Time(t) => Ok(*t as i16),
            Val::Interval(i) => Ok(*i as i16),
            Val::VarLen(s) | Val::ConstLen(s) => s
                .parse()
                .map_err(|e| format!("Cannot convert string to smallint: {}", e)),
            Val::Binary(_) => Err("Cannot convert binary data to smallint".to_string()),
            Val::JSON(_) => Err("Cannot convert JSON to smallint".to_string()),
            Val::UUID(_) => Err("Cannot convert UUID to smallint".to_string()),
            Val::Vector(v) | Val::Array(v) => Ok(v.len() as i16),
            Val::Enum(id, _) => Ok(*id as i16),
            Val::Point(_, _) => Err("Cannot convert point to smallint".to_string()),
            Val::Null => Err("Cannot convert NULL to smallint".to_string()),
            Val::Struct => Ok(1), // Struct is considered as 1
        }
    }

    fn as_tinyint(&self) -> Result<i8, String> {
        match &self.value_ {
            Val::TinyInt(i) => Ok(*i),
            Val::SmallInt(i) => Ok(*i as i8),
            Val::Integer(i) => Ok(*i as i8),
            Val::BigInt(i) => Ok(*i as i8),
            Val::Decimal(f) => Ok(*f as i8),
            Val::Float(f) => Ok(*f as i8),
            Val::Boolean(b) => Ok(if *b { 1 } else { 0 }),
            Val::Timestamp(t) => Ok(*t as i8),
            Val::Date(d) => Ok(*d as i8),
            Val::Time(t) => Ok(*t as i8),
            Val::Interval(i) => Ok(*i as i8),
            Val::VarLen(s) | Val::ConstLen(s) => s
                .parse()
                .map_err(|e| format!("Cannot convert string to tinyint: {}", e)),
            Val::Binary(_) => Err("Cannot convert binary data to tinyint".to_string()),
            Val::JSON(_) => Err("Cannot convert JSON to tinyint".to_string()),
            Val::UUID(_) => Err("Cannot convert UUID to tinyint".to_string()),
            Val::Vector(v) | Val::Array(v) => Ok(v.len() as i8),
            Val::Enum(id, _) => Ok(*id as i8),
            Val::Point(_, _) => Err("Cannot convert point to tinyint".to_string()),
            Val::Null => Err("Cannot convert NULL to tinyint".to_string()),
            Val::Struct => Ok(1), // Struct is considered as 1
        }
    }

    fn as_decimal(&self) -> Result<f64, String> {
        match &self.value_ {
            Val::Decimal(d) => Ok(*d),
            Val::Float(f) => Ok(*f as f64),
            Val::Integer(i) => Ok(*i as f64),
            Val::BigInt(i) => Ok(*i as f64),
            Val::SmallInt(i) => Ok(*i as f64),
            Val::TinyInt(i) => Ok(*i as f64),
            Val::Boolean(b) => Ok(if *b { 1.0 } else { 0.0 }),
            Val::Timestamp(t) => Ok(*t as f64),
            Val::Date(d) => Ok(*d as f64),
            Val::Time(t) => Ok(*t as f64),
            Val::Interval(i) => Ok(*i as f64),
            Val::VarLen(s) | Val::ConstLen(s) => s
                .parse()
                .map_err(|e| format!("Cannot convert string to decimal: {}", e)),
            Val::Binary(_) => Err("Cannot convert binary data to decimal".to_string()),
            Val::JSON(_) => Err("Cannot convert JSON to decimal".to_string()),
            Val::UUID(_) => Err("Cannot convert UUID to decimal".to_string()),
            Val::Vector(v) | Val::Array(v) => Ok(v.len() as f64),
            Val::Enum(id, _) => Ok(*id as f64),
            Val::Point(x, y) => Err(format!("Cannot convert point ({}, {}) to decimal", x, y)),
            Val::Null => Err("Cannot convert NULL to decimal".to_string()),
            Val::Struct => Ok(1.0), // Struct is considered as 1.0
        }
    }

    fn as_bool(&self) -> Result<bool, String> {
        match &self.value_ {
            Val::Boolean(b) => Ok(*b),
            Val::Integer(i) => Ok(*i != 0),
            Val::BigInt(i) => Ok(*i != 0),
            Val::SmallInt(i) => Ok(*i != 0),
            Val::TinyInt(i) => Ok(*i != 0),
            Val::Decimal(f) => Ok(*f != 0.0),
            Val::Float(f) => Ok(*f != 0.0),
            Val::Timestamp(t) => Ok(*t != 0),
            Val::Date(d) => Ok(*d != 0),
            Val::Time(t) => Ok(*t != 0),
            Val::Interval(i) => Ok(*i != 0),
            Val::VarLen(s) | Val::ConstLen(s) => Ok(!s.is_empty()),
            Val::Binary(b) => Ok(!b.is_empty()),
            Val::JSON(j) => Ok(!j.is_empty()),
            Val::UUID(u) => Ok(!u.is_empty()),
            Val::Vector(v) | Val::Array(v) => Ok(!v.is_empty()),
            Val::Enum(_, _) => Ok(true), // Enums are always considered true
            Val::Point(_, _) => Ok(true), // Points are always considered true
            Val::Null => Ok(false),
            Val::Struct => Ok(true), // Struct is considered true as it's a non-null value
        }
    }
}

// Implement From<T> for Val
impl From<TypeId> for Val {
    fn from(value: TypeId) -> Self {
        match value {
            TypeId::Boolean => Val::Boolean(false),
            TypeId::TinyInt => Val::TinyInt(0),
            TypeId::SmallInt => Val::SmallInt(0),
            TypeId::Integer => Val::Integer(0),
            TypeId::BigInt => Val::BigInt(0),
            TypeId::Decimal => Val::Decimal(0.0),
            TypeId::Float => Val::Float(0.0),
            TypeId::Timestamp => Val::Timestamp(0),
            TypeId::Date => Val::Date(0),
            TypeId::Time => Val::Time(0),
            TypeId::Interval => Val::Interval(0),
            TypeId::VarChar => Val::VarLen(String::new()),
            TypeId::Char => Val::ConstLen(String::new()),
            TypeId::Binary => Val::Binary(Vec::new()),
            TypeId::JSON => Val::JSON(String::new()),
            TypeId::UUID => Val::UUID(String::new()),
            TypeId::Vector => Val::Vector(Vec::new()),
            TypeId::Array => Val::Array(Vec::new()),
            TypeId::Enum => Val::Enum(0, String::new()),
            TypeId::Point => Val::Point(0.0, 0.0),
            TypeId::Invalid => Val::Null,
            TypeId::Struct => Val::Struct,
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

impl From<Vec<Value>> for Val {
    fn from(v: Vec<Value>) -> Self {
        Val::Vector(v)
    }
}

impl From<CmpBool> for Val {
    fn from(cmp_bool: CmpBool) -> Self {
        match cmp_bool {
            CmpBool::CmpTrue => Val::Boolean(true),
            CmpBool::CmpFalse => Val::Boolean(false),
            CmpBool::CmpNull => Val::Null,
        }
    }
}

impl From<f32> for Val {
    fn from(f: f32) -> Self {
        Val::Float(f)
    }
}

// Add after the existing From<u64> for Val implementation
impl From<&[u8]> for Val {
    fn from(b: &[u8]) -> Self {
        Val::Binary(b.to_vec())
    }
}

impl From<Vec<u8>> for Val {
    fn from(b: Vec<u8>) -> Self {
        Val::Binary(b)
    }
}

impl<T: Into<Val>> From<T> for Value {
    fn from(t: T) -> Self {
        Value::new(t)
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // Special case for NULL values
        if let Val::Null = self.value_ {
            return write!(f, "NULL");
        }

        // Handle vectors directly to support mixed types
        if let Val::Vector(vec) | Val::Array(vec) = &self.value_ {
            write!(f, "[")?;
            for (i, item) in vec.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", item)?;
            }
            return write!(f, "]");
        }

        // Use the appropriate type's to_string method for human-readable display
        let type_instance = crate::types_db::types::get_instance(self.type_id_);
        write!(f, "{}", type_instance.to_string(self))
    }
}

// Add Debug implementation to show full type information
impl Debug for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Value {{ value_: {:?}, size_: {:?}, manage_data_: {}, type_id_: {:?}, struct_data: {:?} }}",
            self.value_, self.size_, self.manage_data_, self.type_id_, self.struct_data
        )
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.type_id_.hash(state);
        match &self.value_ {
            Val::Struct => {
                if let Some(struct_data) = &self.struct_data {
                    for value in struct_data {
                        value.hash(state);
                    }
                }
            }
            Val::Boolean(b) => b.hash(state),
            Val::TinyInt(i) => i.hash(state),
            Val::SmallInt(i) => i.hash(state),
            Val::Integer(i) => i.hash(state),
            Val::BigInt(i) => i.hash(state),
            Val::Decimal(f) => f.to_bits().hash(state),
            Val::Float(f) => f.to_bits().hash(state),
            Val::Timestamp(t) => t.hash(state),
            Val::Date(d) => d.hash(state),
            Val::Time(t) => t.hash(state),
            Val::Interval(i) => i.hash(state),
            Val::VarLen(s) | Val::ConstLen(s) => s.hash(state),
            Val::Binary(b) => b.hash(state),
            Val::JSON(j) => j.hash(state),
            Val::UUID(u) => u.hash(state),
            Val::Vector(v) | Val::Array(v) => {
                for value in v {
                    value.hash(state);
                }
            }
            Val::Enum(id, s) => {
                id.hash(state);
                s.hash(state);
            }
            Val::Point(x, y) => {
                x.to_bits().hash(state);
                y.to_bits().hash(state);
            }
            Val::Null => (), // Null doesn't need additional hashing
        }
    }
}

impl Eq for Value {}

#[cfg(test)]
mod unit_tests {
    use crate::container::hash_function::HashFunction;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::{Size, Val, Value};
    use std::hash::{DefaultHasher, Hash, Hasher};

    #[test]
    fn test_typeid_hash() {
        let type1 = TypeId::Integer;
        let type2 = TypeId::Integer;
        let type3 = TypeId::VarChar;

        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        let mut hasher3 = DefaultHasher::new();

        type1.hash(&mut hasher1);
        type2.hash(&mut hasher2);
        type3.hash(&mut hasher3);

        assert_eq!(hasher1.finish(), hasher2.finish());
        assert_ne!(hasher1.finish(), hasher3.finish());
    }

    #[test]
    fn test_value_hash() {
        let value1 = Value::new(42);
        let value2 = Value::new(42);
        let value3 = Value::new(43);

        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        let mut hasher3 = DefaultHasher::new();

        value1.hash(&mut hasher1);
        value2.hash(&mut hasher2);
        value3.hash(&mut hasher3);

        assert_eq!(hasher1.finish(), hasher2.finish());
        assert_ne!(hasher1.finish(), hasher3.finish());
    }

    #[test]
    fn test_value_hash_with_custom_hasher() {
        let hash_function = HashFunction::<Value>::new();

        let value1 = Value::new(42);
        let value2 = Value::new(42);
        let value3 = Value::new(43);

        let hash1 = hash_function.get_hash(&value1);
        let hash2 = hash_function.get_hash(&value2);
        let hash3 = hash_function.get_hash(&value3);

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_value_as_bytes() {
        let int_value = Value::new(42);
        let string_value = Value::new("Hello");
        let vector_value = Value::new_vector(vec![Value::new(1), Value::new(2)]);

        assert_eq!(int_value.as_bytes(), [42, 0, 0, 0]); // TypeId::Integer (3) + 42 as i32
        assert_eq!(string_value.as_bytes(), [72, 101, 108, 108, 111]); // TypeId::VarChar (7) + "Hello"
        assert_eq!(vector_value.as_bytes(), [1, 0, 0, 0, 2, 0, 0, 0]); // TypeId::Vector (9) + [TypeId::Integer (3), 1, TypeId::Integer (3), 2]
    }

    #[test]
    fn test_serialize_val() {
        // Test serialization of different Val variants
        let val_boolean = Val::Boolean(true);
        let serialized_boolean = bincode::serialize(&val_boolean).expect("Serialization failed");
        assert_eq!(serialized_boolean, vec![0, 0, 0, 0, 1]); // Check the binary representation as needed

        let val_integer = Val::Integer(42);
        let serialized_integer = bincode::serialize(&val_integer).expect("Serialization failed");
        assert_eq!(serialized_integer, vec![3, 0, 0, 0, 42, 0, 0, 0]); // Adjust this to match the actual binary format

        let val_string = Val::VarLen("Hello".to_string());
        let serialized_string = bincode::serialize(&val_string).expect("Serialization failed");
        assert_eq!(
            serialized_string,
            vec![11, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 72, 101, 108, 108, 111]
        ); // Binary format for the string

        let val_vector = Val::Vector(vec![
            Value::from(Val::Integer(1)),
            Value::from(Val::Integer(2)),
        ]);
        let serialized_vector = bincode::serialize(&val_vector).expect("Serialization failed");
        assert_eq!(
            serialized_vector,
            vec![
                16, 0, 0, 0, // Vector variant index (updated from 9 to 16)
                2, 0, 0, 0, 0, 0, 0, 0, // Vector length (2)
                3, 0, 0, 0, // First Value: Integer variant index
                1, 0, 0, 0, // First Value: value (1)
                0, 0, 0, 0, // First Value: Size::Length variant index
                4, 0, 0, 0, 0, 0, 0, 0, // First Value: size value (4)
                0, // First Value: manage_data_ (false)
                3, 0, 0, 0, // First Value: type_id_ (Integer)
                0, // First Value: struct_data (None)
                3, 0, 0, 0, // Second Value: Integer variant index
                2, 0, 0, 0, // Second Value: value (2)
                0, 0, 0, 0, // Second Value: Size::Length variant index
                4, 0, 0, 0, 0, 0, 0, 0, // Second Value: size value (4)
                0, // Second Value: manage_data_ (false)
                3, 0, 0, 0, // Second Value: type_id_ (Integer)
                0  // Second Value: struct_data (None)
            ]
        );
    }

    #[test]
    fn test_deserialize_val() {
        // Test deserialization of binary data into Val variants
        let binary_boolean = vec![0, 0, 0, 0, 1];
        let deserialized_boolean: Val =
            bincode::deserialize(&binary_boolean).expect("Deserialization failed");
        assert_eq!(deserialized_boolean, Val::Boolean(true));

        let binary_integer = vec![3, 0, 0, 0, 42, 0, 0, 0];
        let deserialized_integer: Val =
            bincode::deserialize(&binary_integer).expect("Deserialization failed");
        assert_eq!(deserialized_integer, Val::Integer(42));

        // Update the binary_string test with the correct variant index (11 for VarLen)
        let binary_string = vec![11, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 72, 101, 108, 108, 111];
        let deserialized_string: Val =
            bincode::deserialize(&binary_string).expect("Deserialization failed");
        assert_eq!(deserialized_string, Val::VarLen("Hello".to_string()));

        let binary_vector = bincode::serialize(&Val::Vector(vec![
            Value::from(Val::Integer(1)),
            Value::from(Val::Integer(2)),
        ]))
        .expect("Serialization failed");
        let deserialized_vector: Val =
            bincode::deserialize(&binary_vector).expect("Deserialization failed");
        assert_eq!(
            deserialized_vector,
            Val::Vector(vec![
                Value::from(Val::Integer(1)),
                Value::from(Val::Integer(2))
            ])
        );
    }

    #[test]
    fn test_serialize_value() {
        // Test serialization of Value struct
        let value = Value {
            value_: Val::Integer(42),
            size_: Size::Length(4),
            manage_data_: false,
            type_id_: TypeId::Integer,
            struct_data: None,
        };
        let serialized = bincode::serialize(&value).expect("Serialization failed");

        // Expected binary representation
        let expected_bytes: Vec<u8> = {
            // Serialize the individual components to get the expected bytes

            // Serialize Val::Integer(42)
            let mut value_bytes: Vec<u8> = Vec::new();
            // Val variant index for Integer is 3 (u32)
            value_bytes.extend(&3u32.to_le_bytes());
            // Integer value 42 (i32)
            value_bytes.extend(&42i32.to_le_bytes());

            // Serialize Size::Length(4)
            let mut size_bytes: Vec<u8> = Vec::new();
            // Size variant index for Length is 0 (u32)
            size_bytes.extend(&0u32.to_le_bytes());
            // Length value 4 (usize)
            let length_bytes = 4usize.to_le_bytes();
            size_bytes.extend(&length_bytes);

            // Serialize manage_data_: false (bool)
            let manage_data_bytes: Vec<u8> = vec![0x00]; // false

            // Serialize TypeId::Integer
            let mut type_id_bytes: Vec<u8> = Vec::new();
            // TypeId variant index for Integer is 3 (u32)
            type_id_bytes.extend(&3u32.to_le_bytes());

            // Serialize struct_data: None
            let struct_data_bytes: Vec<u8> = vec![0x00]; // None variant

            // Combine all bytes
            let mut expected: Vec<u8> = Vec::new();
            expected.extend(value_bytes);
            expected.extend(size_bytes);
            expected.extend(manage_data_bytes);
            expected.extend(type_id_bytes);
            expected.extend(struct_data_bytes);

            expected
        };

        // Now assert that the serialized data matches the expected bytes
        assert_eq!(serialized, expected_bytes);
    }

    #[test]
    fn test_deserialize_value() {
        // Test deserialization of binary data into Value struct
        let value = Value {
            value_: Val::Integer(42),
            size_: Size::Length(4),
            manage_data_: false,
            type_id_: TypeId::Integer, // Replace with an appropriate variant
            struct_data: None,
        };
        let serialized = bincode::serialize(&value).expect("Serialization failed");
        let deserialized_value: Value =
            bincode::deserialize(&serialized).expect("Deserialization failed");
        assert_eq!(deserialized_value, value);
    }

    #[test]
    fn test_round_trip_value() {
        // Round-trip tests for Value serialization and deserialization
        let original_value = Value {
            value_: Val::Decimal(123.456),
            size_: Size::Length(8),
            manage_data_: true,
            type_id_: TypeId::Decimal, // Replace with an appropriate variant
            struct_data: None,
        };
        let serialized = bincode::serialize(&original_value).expect("Serialization failed");
        let deserialized: Value =
            bincode::deserialize(&serialized).expect("Deserialization failed");
        assert_eq!(original_value, deserialized);
    }

    #[test]
    fn test_value_display() {
        let int_value = Value::new(42);
        let float_value = Value::new(3.14);
        let bool_value = Value::new(true);
        let string_value = Value::new("Hello");
        let vector_value =
            Value::new_vector(vec![Value::new(1), Value::new("two"), Value::new(3.0)]);

        // Test Display implementation - simple value representation
        assert_eq!(format!("{}", int_value), "42");
        assert_eq!(format!("{}", float_value), "3.14");
        assert_eq!(format!("{}", bool_value), "true");
        assert_eq!(format!("{}", string_value), "Hello");
        assert_eq!(format!("{}", vector_value), "[1, two, 3]");
    }

    #[test]
    fn test_value_debug() {
        // Test Debug implementation - detailed type information
        let int_value = Value::new(42);
        let string_value = Value::new("Hello");
        let vector_value = Value::new_vector(vec![Value::new(1), Value::new(2)]);

        assert_eq!(
            format!("{:?}", int_value),
            "Value { value_: Integer(42), size_: Length(4), manage_data_: false, type_id_: Integer, struct_data: None }"
        );

        assert_eq!(
            format!("{:?}", string_value),
            "Value { value_: VarLen(\"Hello\"), size_: Length(0), manage_data_: false, type_id_: VarChar, struct_data: None }"
        );

        // For vector values, just verify it contains the expected Debug format
        let debug_str = format!("{:?}", vector_value);
        assert!(debug_str.contains("Vector"));
        assert!(debug_str.contains("Integer(1)"));
        assert!(debug_str.contains("Integer(2)"));
    }

    #[test]
    fn test_value_creation() {
        let bool_val = Value::new(true);
        let int_val = Value::new(42i32);
        let string_val = Value::new("Hello");
        let null_val = Value::new(Val::Null);
        let vector_val = Value::new_vector(vec![Value::new(1), Value::new(2), Value::new(3)]);

        assert_eq!(bool_val.get_type_id(), TypeId::Boolean);
        assert_eq!(int_val.get_type_id(), TypeId::Integer);
        assert_eq!(string_val.get_type_id(), TypeId::VarChar);
        assert_eq!(null_val.get_type_id(), TypeId::Invalid);
        assert_eq!(vector_val.get_type_id(), TypeId::Vector);
    }

    #[test]
    fn test_value_storage_size() {
        assert_eq!(Value::new(true).get_storage_size(), 1);
        assert_eq!(Value::new(42i32).get_storage_size(), 4);
        assert_eq!(Value::new("Hello").get_storage_size(), 5);
        assert_eq!(Value::new(Val::Null).get_storage_size(), 1);
        assert_eq!(
            Value::new_vector(vec![Value::new(1), Value::new(2)]).get_storage_size(),
            12
        );
    }
}

#[cfg(test)]
mod basic_behavior_tests {
    use super::*;

    #[test]
    fn test_basic_null_comparisons() {
        let null1 = Value::new(Val::Null);
        let null2 = Value::new(Val::Null);
        let val = Value::new(5);

        // Equality
        assert_eq!(null1.compare_equals(&null2), CmpBool::CmpNull);
        assert_eq!(null1.compare_equals(&val), CmpBool::CmpNull);
        assert_eq!(val.compare_equals(&null1), CmpBool::CmpNull);

        // Inequality
        assert_eq!(null1.compare_not_equals(&null2), CmpBool::CmpNull);
        assert_eq!(null1.compare_not_equals(&val), CmpBool::CmpNull);
        assert_eq!(val.compare_not_equals(&null1), CmpBool::CmpNull);
    }

    #[test]
    fn test_basic_numeric_comparisons() {
        let val1 = Value::new(5);
        let val2 = Value::new(10);
        let val3 = Value::new(5);

        assert_eq!(val1.compare_equals(&val3), CmpBool::CmpTrue);
        assert_eq!(val1.compare_not_equals(&val2), CmpBool::CmpTrue);
        assert_eq!(val1.compare_less_than(&val2), CmpBool::CmpTrue);
        assert_eq!(val2.compare_greater_than(&val1), CmpBool::CmpTrue);
    }

    #[test]
    fn test_basic_string_comparisons() {
        let str1 = Value::new("abc");
        let str2 = Value::new("def");
        let str3 = Value::new("abc");

        assert_eq!(str1.compare_equals(&str3), CmpBool::CmpTrue);
        assert_eq!(str1.compare_not_equals(&str2), CmpBool::CmpTrue);
        assert_eq!(str1.compare_less_than(&str2), CmpBool::CmpTrue);
        assert_eq!(str2.compare_greater_than(&str1), CmpBool::CmpTrue);
    }

    #[test]
    fn test_basic_type_mismatch() {
        let int_val = Value::new(5);
        let str_val = Value::new("5");
        let bool_val = Value::new(true);

        assert_eq!(int_val.compare_equals(&str_val), CmpBool::CmpFalse);
        assert_eq!(int_val.compare_less_than(&bool_val), CmpBool::CmpFalse);
        assert_eq!(str_val.compare_greater_than(&bool_val), CmpBool::CmpFalse);
    }

    #[test]
    fn test_basic_comparisons() {
        // Integer comparisons
        let int1 = Value::new(5);
        let int2 = Value::new(10);
        let int3 = Value::new(5);

        assert_eq!(int1.compare_equals(&int3), CmpBool::CmpTrue);
        assert_eq!(int1.compare_not_equals(&int2), CmpBool::CmpTrue);
        assert_eq!(int1.compare_less_than(&int2), CmpBool::CmpTrue);
        assert_eq!(int2.compare_greater_than(&int1), CmpBool::CmpTrue);
    }

    #[test]
    fn test_string_comparisons() {
        let str1 = Value::new("abc");
        let str2 = Value::new("def");
        let str3 = Value::new("abc");

        assert_eq!(str1.compare_equals(&str3), CmpBool::CmpTrue);
        assert_eq!(str1.compare_not_equals(&str2), CmpBool::CmpTrue);
        assert_eq!(str1.compare_less_than(&str2), CmpBool::CmpTrue);
        assert_eq!(str2.compare_greater_than(&str1), CmpBool::CmpTrue);
    }

    #[test]
    fn test_null_comparisons() {
        let null1 = Value::new(Val::Null);
        let null2 = Value::new(Val::Null);
        let int1 = Value::new(5);
        let int2 = Value::new(10);

        // NULL = NULL -> NULL
        assert_eq!(null1.compare_equals(&null2), CmpBool::CmpNull);

        // NULL = value -> NULL
        assert_eq!(null1.compare_equals(&int1), CmpBool::CmpNull);

        // value = NULL -> NULL
        assert_eq!(int1.compare_equals(&null1), CmpBool::CmpNull);

        // NULL != NULL -> NULL
        assert_eq!(null1.compare_not_equals(&null2), CmpBool::CmpNull);

        // NULL != value -> NULL
        assert_eq!(null1.compare_not_equals(&int1), CmpBool::CmpNull);

        // value != NULL -> NULL
        assert_eq!(int1.compare_not_equals(&null1), CmpBool::CmpNull);

        // NULL < value -> NULL
        assert_eq!(null1.compare_less_than(&int1), CmpBool::CmpNull);

        // NULL < NULL -> NULL
        assert_eq!(null1.compare_less_than(&null2), CmpBool::CmpNull);

        // value < NULL -> NULL
        assert_eq!(int1.compare_less_than(&null1), CmpBool::CmpNull);

        // NULL > value -> NULL
        assert_eq!(null1.compare_greater_than(&int1), CmpBool::CmpNull);

        // NULL > NULL -> NULL
        assert_eq!(null1.compare_greater_than(&null2), CmpBool::CmpNull);

        // value > NULL -> NULL
        assert_eq!(int1.compare_greater_than(&null1), CmpBool::CmpNull);

        // NULL <= value -> NULL
        assert_eq!(null1.compare_less_than_equals(&int1), CmpBool::CmpNull);

        // NULL <= NULL -> NULL
        assert_eq!(null1.compare_less_than_equals(&null2), CmpBool::CmpNull);

        // value <= NULL -> NULL
        assert_eq!(int1.compare_less_than_equals(&null1), CmpBool::CmpNull);

        // NULL >= value -> NULL
        assert_eq!(null1.compare_greater_than_equals(&int1), CmpBool::CmpNull);

        // NULL >= NULL -> NULL
        assert_eq!(null1.compare_greater_than_equals(&null2), CmpBool::CmpNull);

        // value >= NULL -> NULL
        assert_eq!(int1.compare_greater_than_equals(&null1), CmpBool::CmpNull);

        // Verify non-NULL comparisons still work correctly
        assert_eq!(int1.compare_less_than(&int2), CmpBool::CmpTrue);
        assert_eq!(int2.compare_greater_than(&int1), CmpBool::CmpTrue);
        assert_eq!(int1.compare_equals(&int1), CmpBool::CmpTrue);
        assert_eq!(int1.compare_not_equals(&int2), CmpBool::CmpTrue);
    }

    #[test]
    fn test_mismatched_type_comparisons() {
        let int_val = Value::new(5);
        let str_val = Value::new("5");
        let bool_val = Value::new(true);

        // Different types should not be comparable
        assert_eq!(int_val.compare_equals(&str_val), CmpBool::CmpFalse);
        assert_eq!(int_val.compare_less_than(&bool_val), CmpBool::CmpFalse);
        assert_eq!(str_val.compare_greater_than(&bool_val), CmpBool::CmpFalse);
    }

    #[test]
    fn test_vector_comparisons() {
        let vec1 = Value::new_vector(vec![Value::new(1), Value::new(2)]);
        let vec2 = Value::new_vector(vec![Value::new(1), Value::new(2)]);
        let vec3 = Value::new_vector(vec![Value::new(2), Value::new(3)]);

        assert_eq!(vec1.compare_equals(&vec2), CmpBool::CmpTrue);
        assert_eq!(vec1.compare_not_equals(&vec3), CmpBool::CmpTrue);
        // Vector type doesn't support less/greater than comparisons
        assert_eq!(vec1.compare_less_than(&vec3), CmpBool::CmpFalse);
        assert_eq!(vec1.compare_greater_than(&vec3), CmpBool::CmpFalse);
    }

    #[test]
    fn test_decimal_comparisons() {
        let dec1 = Value::new(3.14);
        let dec2 = Value::new(3.14);
        let dec3 = Value::new(2.718);

        assert_eq!(dec1.compare_equals(&dec2), CmpBool::CmpTrue);
        assert_eq!(dec1.compare_not_equals(&dec3), CmpBool::CmpTrue);
        assert_eq!(dec3.compare_less_than(&dec1), CmpBool::CmpTrue);
        assert_eq!(dec1.compare_greater_than(&dec3), CmpBool::CmpTrue);
    }

    #[test]
    fn test_timestamp_comparisons() {
        let ts1 = Value::new(1000u64);
        let ts2 = Value::new(2000u64);
        let ts3 = Value::new(1000u64);

        assert_eq!(ts1.compare_equals(&ts3), CmpBool::CmpTrue);
        assert_eq!(ts1.compare_not_equals(&ts2), CmpBool::CmpTrue);
        assert_eq!(ts1.compare_less_than(&ts2), CmpBool::CmpTrue);
        assert_eq!(ts2.compare_greater_than(&ts1), CmpBool::CmpTrue);
    }
}

#[cfg(test)]
mod concurrency_tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_concurrent_value_creation() {
        let mut handles = vec![];
        let values = Arc::new(vec![1, 2, 3, 4, 5]);

        for _ in 0..3 {
            let values = Arc::clone(&values);
            let handle =
                thread::spawn(move || values.iter().map(|&x| Value::new(x)).collect::<Vec<_>>());
            handles.push(handle);
        }

        for handle in handles {
            let thread_values = handle.join().unwrap();
            assert_eq!(thread_values.len(), 5);
            for (i, value) in thread_values.iter().enumerate() {
                assert_eq!(*value, Value::new(i as i32 + 1));
            }
        }
    }

    #[test]
    fn test_concurrent_comparisons() {
        let mut handles = vec![];
        let val1 = Arc::new(Value::new(5));
        let val2 = Arc::new(Value::new(10));

        for _ in 0..3 {
            let val1 = Arc::clone(&val1);
            let val2 = Arc::clone(&val2);
            let handle = thread::spawn(move || {
                assert_eq!(val1.compare_less_than(&val2), CmpBool::CmpTrue);
                assert_eq!(val2.compare_greater_than(&val1), CmpBool::CmpTrue);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_concurrent_vector_operations() {
        let mut handles = vec![];
        let vector = Arc::new(Value::new_vector(vec![
            Value::new(1),
            Value::new(2),
            Value::new(3),
        ]));

        for _ in 0..3 {
            let vector = Arc::clone(&vector);
            let handle = thread::spawn(move || {
                let bytes = vector.as_bytes();
                assert!(!bytes.is_empty());
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}

#[cfg(test)]
mod edge_cases {
    use super::*;

    #[test]
    fn test_empty_values() {
        let empty_str = Value::new("");
        let empty_vec = Value::new_vector::<Vec<Value>>(vec![]);

        assert_eq!(empty_str.get_storage_size(), 0);
        assert_eq!(empty_vec.get_storage_size(), 4);
        assert_eq!(empty_str.compare_equals(&Value::new("")), CmpBool::CmpTrue);
        assert_eq!(
            empty_vec.compare_equals(&Value::new_vector::<Vec<Value>>(vec![])),
            CmpBool::CmpTrue
        );
    }

    #[test]
    fn test_extreme_values() {
        let min_tiny = Value::new(i8::MIN);
        let max_tiny = Value::new(i8::MAX);
        let min_int = Value::new(i32::MIN);
        let max_int = Value::new(i32::MAX);
        let min_big = Value::new(i64::MIN);
        let max_big = Value::new(i64::MAX);

        assert_eq!(min_tiny.compare_less_than(&max_tiny), CmpBool::CmpTrue);
        assert_eq!(min_int.compare_less_than(&max_int), CmpBool::CmpTrue);
        assert_eq!(min_big.compare_less_than(&max_big), CmpBool::CmpTrue);
    }

    #[test]
    fn test_decimal_edge_cases() {
        let inf = Value::new(f64::INFINITY);
        let neg_inf = Value::new(f64::NEG_INFINITY);
        let nan = Value::new(f64::NAN);

        assert_eq!(neg_inf.compare_less_than(&inf), CmpBool::CmpTrue);
        assert_eq!(inf.compare_greater_than(&neg_inf), CmpBool::CmpTrue);
        assert_eq!(nan.compare_equals(&nan), CmpBool::CmpFalse);
    }

    #[test]
    fn test_nested_vectors() {
        let nested = Value::new_vector(vec![
            Value::new_vector(vec![Value::new(1), Value::new(2)]),
            Value::new_vector(vec![Value::new(3), Value::new(4)]),
        ]);

        let same_nested = Value::new_vector(vec![
            Value::new_vector(vec![Value::new(1), Value::new(2)]),
            Value::new_vector(vec![Value::new(3), Value::new(4)]),
        ]);

        assert_eq!(nested.compare_equals(&same_nested), CmpBool::CmpTrue);
    }

    #[test]
    fn test_mixed_vector_types() {
        let mixed = Value::new_vector(vec![
            Value::new(1),
            Value::new("string"),
            Value::new(true),
            Value::new(3.14),
        ]);

        let same_mixed = Value::new_vector(vec![
            Value::new(1),
            Value::new("string"),
            Value::new(true),
            Value::new(3.14),
        ]);

        assert_eq!(mixed.compare_equals(&same_mixed), CmpBool::CmpTrue);
    }

    #[test]
    fn test_empty_vector_comparisons() {
        // Test empty vectors
        let empty_vec1 = Value::new_vector::<Vec<Value>>(vec![]);
        let empty_vec2 = Value::new_vector(vec![] as Vec<Value>);
        assert_eq!(empty_vec1.compare_equals(&empty_vec2), CmpBool::CmpTrue);
    }

    #[test]
    fn test_empty_string_comparisons() {
        let empty_str1 = Value::new("");
        let empty_str2 = Value::new("");
        assert_eq!(empty_str1.compare_equals(&empty_str2), CmpBool::CmpTrue);
    }

    #[test]
    fn test_boolean_comparisons() {
        let empty_str1 = Value::new("");
        let empty_str2 = Value::new("");
        assert_eq!(empty_str1.compare_equals(&empty_str2), CmpBool::CmpTrue);
    }
}

#[cfg(test)]
mod cast_tests {
    use super::*;

    #[test]
    fn test_numeric_casts() {
        let int_val = Value::new(42);
        let big_val = int_val.cast_to(TypeId::BigInt).unwrap();
        let dec_val = int_val.cast_to(TypeId::Decimal).unwrap();

        assert_eq!(big_val.get_type_id(), TypeId::BigInt);
        assert_eq!(dec_val.get_type_id(), TypeId::Decimal);

        match big_val.get_val() {
            Val::BigInt(i) => assert_eq!(*i, 42i64),
            _ => panic!("Expected BigInt"),
        }

        match dec_val.get_val() {
            Val::Decimal(f) => assert_eq!(*f, 42.0),
            _ => panic!("Expected Decimal"),
        }
    }

    #[test]
    fn test_string_casts() {
        let var_val = Value::new("test");
        let const_val = var_val.cast_to(TypeId::Char).unwrap();

        assert_eq!(const_val.get_type_id(), TypeId::Char);
        match const_val.get_val() {
            Val::ConstLen(s) => assert_eq!(s.as_str(), "test"),
            _ => panic!("Expected ConstLen"),
        }
    }

    #[test]
    fn test_null_casts() {
        let null_val = Value::new(Val::Null);
        let cast_null = null_val.cast_to(TypeId::Integer).unwrap();

        assert!(cast_null.is_null());
        assert_eq!(cast_null.get_type_id(), TypeId::Integer);
    }

    #[test]
    fn test_invalid_casts() {
        // Test truly invalid casts that should fail
        let str_val = Value::new("test");
        assert!(str_val.cast_to(TypeId::Integer).is_err());

        // Test struct casts (should fail)
        let struct_val = Value::new_with_type(Val::Struct, TypeId::Struct);
        assert!(struct_val.cast_to(TypeId::Integer).is_err());

        // Test invalid string to boolean
        let invalid_bool_str = Value::new("maybe");
        assert!(invalid_bool_str.cast_to(TypeId::Boolean).is_err());
    }
}

#[cfg(test)]
mod comprehensive_cast_tests {
    use super::*;

    #[test]
    fn test_boolean_casts() {
        let true_val = Value::new(true);
        let false_val = Value::new(false);

        // Boolean to numeric types
        assert_eq!(true_val.cast_to(TypeId::TinyInt).unwrap().get_val(), &Val::TinyInt(1));
        assert_eq!(false_val.cast_to(TypeId::TinyInt).unwrap().get_val(), &Val::TinyInt(0));
        assert_eq!(true_val.cast_to(TypeId::SmallInt).unwrap().get_val(), &Val::SmallInt(1));
        assert_eq!(true_val.cast_to(TypeId::Integer).unwrap().get_val(), &Val::Integer(1));
        assert_eq!(true_val.cast_to(TypeId::BigInt).unwrap().get_val(), &Val::BigInt(1));
        assert_eq!(true_val.cast_to(TypeId::Decimal).unwrap().get_val(), &Val::Decimal(1.0));
        assert_eq!(true_val.cast_to(TypeId::Float).unwrap().get_val(), &Val::Float(1.0));

        // Boolean to string types
        assert_eq!(true_val.cast_to(TypeId::VarChar).unwrap().get_val(), &Val::VarLen("true".to_string()));
        assert_eq!(false_val.cast_to(TypeId::Char).unwrap().get_val(), &Val::ConstLen("false".to_string()));
    }

    #[test]
    fn test_numeric_to_boolean_casts() {
        // Non-zero values should cast to true
        assert_eq!(Value::new(42i32).cast_to(TypeId::Boolean).unwrap().get_val(), &Val::Boolean(true));
        assert_eq!(Value::new(-1i8).cast_to(TypeId::Boolean).unwrap().get_val(), &Val::Boolean(true));
        assert_eq!(Value::new(3.14f64).cast_to(TypeId::Boolean).unwrap().get_val(), &Val::Boolean(true));

        // Zero values should cast to false
        assert_eq!(Value::new(0i32).cast_to(TypeId::Boolean).unwrap().get_val(), &Val::Boolean(false));
        assert_eq!(Value::new(0.0f64).cast_to(TypeId::Boolean).unwrap().get_val(), &Val::Boolean(false));
    }

    #[test]
    fn test_numeric_cross_casts() {
        let int_val = Value::new(42i32);
        
        // Integer to other numeric types
        assert_eq!(int_val.cast_to(TypeId::TinyInt).unwrap().get_val(), &Val::TinyInt(42));
        assert_eq!(int_val.cast_to(TypeId::SmallInt).unwrap().get_val(), &Val::SmallInt(42));
        assert_eq!(int_val.cast_to(TypeId::BigInt).unwrap().get_val(), &Val::BigInt(42));
        assert_eq!(int_val.cast_to(TypeId::Decimal).unwrap().get_val(), &Val::Decimal(42.0));
        assert_eq!(int_val.cast_to(TypeId::Float).unwrap().get_val(), &Val::Float(42.0));

        // Test range checking
        let large_int = Value::new(300i32);
        assert!(large_int.cast_to(TypeId::TinyInt).is_err()); // Out of range for i8
        
        let small_int = Value::new(100i32);
        assert!(small_int.cast_to(TypeId::TinyInt).is_ok()); // Within range for i8
    }

    #[test]
    fn test_string_to_numeric_casts() {
        let int_str = Value::new("42");
        let float_str = Value::new("3.14");
        let bool_str_true = Value::new("true");
        let bool_str_false = Value::new("false");

        // String to numeric types
        assert_eq!(int_str.cast_to(TypeId::Integer).unwrap().get_val(), &Val::Integer(42));
        assert_eq!(float_str.cast_to(TypeId::Decimal).unwrap().get_val(), &Val::Decimal(3.14));
        assert_eq!(bool_str_true.cast_to(TypeId::Boolean).unwrap().get_val(), &Val::Boolean(true));
        assert_eq!(bool_str_false.cast_to(TypeId::Boolean).unwrap().get_val(), &Val::Boolean(false));

        // Test various boolean string representations
        assert_eq!(Value::new("1").cast_to(TypeId::Boolean).unwrap().get_val(), &Val::Boolean(true));
        assert_eq!(Value::new("0").cast_to(TypeId::Boolean).unwrap().get_val(), &Val::Boolean(false));
        assert_eq!(Value::new("yes").cast_to(TypeId::Boolean).unwrap().get_val(), &Val::Boolean(true));
        assert_eq!(Value::new("no").cast_to(TypeId::Boolean).unwrap().get_val(), &Val::Boolean(false));

        // Invalid conversions should fail
        assert!(Value::new("not_a_number").cast_to(TypeId::Integer).is_err());
        assert!(Value::new("maybe").cast_to(TypeId::Boolean).is_err());
    }

    #[test]
    fn test_temporal_type_casts() {
        let timestamp = Value::new(1000u64);
        let date = Value::new_with_type(Val::Date(20230101), TypeId::Date);
        let time = Value::new_with_type(Val::Time(3600), TypeId::Time);
        let interval = Value::new_with_type(Val::Interval(86400), TypeId::Interval);

        // Temporal to string
        assert_eq!(timestamp.cast_to(TypeId::VarChar).unwrap().get_val(), &Val::VarLen("1000".to_string()));
        assert_eq!(date.cast_to(TypeId::VarChar).unwrap().get_val(), &Val::VarLen("20230101".to_string()));
        assert_eq!(time.cast_to(TypeId::VarChar).unwrap().get_val(), &Val::VarLen("3600".to_string()));
        assert_eq!(interval.cast_to(TypeId::VarChar).unwrap().get_val(), &Val::VarLen("86400".to_string()));

        // Temporal to integer types
        assert_eq!(timestamp.cast_to(TypeId::Integer).unwrap().get_val(), &Val::Integer(1000));
        assert_eq!(timestamp.cast_to(TypeId::BigInt).unwrap().get_val(), &Val::BigInt(1000));
        assert_eq!(date.cast_to(TypeId::Integer).unwrap().get_val(), &Val::Integer(20230101));
        assert_eq!(time.cast_to(TypeId::Integer).unwrap().get_val(), &Val::Integer(3600));
        assert_eq!(interval.cast_to(TypeId::BigInt).unwrap().get_val(), &Val::BigInt(86400));

        // String to temporal types
        assert_eq!(Value::new("1000").cast_to(TypeId::Timestamp).unwrap().get_val(), &Val::Timestamp(1000));
        assert_eq!(Value::new("20230101").cast_to(TypeId::Date).unwrap().get_val(), &Val::Date(20230101));
        assert_eq!(Value::new("3600").cast_to(TypeId::Time).unwrap().get_val(), &Val::Time(3600));
        assert_eq!(Value::new("86400").cast_to(TypeId::Interval).unwrap().get_val(), &Val::Interval(86400));

        // Integer/BigInt to Timestamp
        let int_val = Value::new(2000);
        let bigint_val = Value::new(3000i64);
        let negative_int = Value::new(-1000);

        assert_eq!(int_val.cast_to(TypeId::Timestamp).unwrap().get_val(), &Val::Timestamp(2000));
        assert_eq!(bigint_val.cast_to(TypeId::Timestamp).unwrap().get_val(), &Val::Timestamp(3000));
        assert!(negative_int.cast_to(TypeId::Timestamp).is_err());
    }

    #[test]
    fn test_binary_casts() {
        let text = "Hello, World!";
        let binary_val = Value::new_with_type(Val::Binary(text.as_bytes().to_vec()), TypeId::Binary);

        // Binary to string (valid UTF-8)
        assert_eq!(binary_val.cast_to(TypeId::VarChar).unwrap().get_val(), &Val::VarLen(text.to_string()));
        assert_eq!(binary_val.cast_to(TypeId::Char).unwrap().get_val(), &Val::ConstLen(text.to_string()));

        // String to binary
        let string_val = Value::new(text);
        assert_eq!(string_val.cast_to(TypeId::Binary).unwrap().get_val(), &Val::Binary(text.as_bytes().to_vec()));

        // Invalid UTF-8 should fail
        let invalid_utf8 = Value::new_with_type(Val::Binary(vec![0xFF, 0xFE, 0xFD]), TypeId::Binary);
        assert!(invalid_utf8.cast_to(TypeId::VarChar).is_err());
    }

    #[test]
    fn test_json_uuid_casts() {
        let json_val = Value::new_with_type(Val::JSON("{\"key\": \"value\"}".to_string()), TypeId::JSON);
        let uuid_val = Value::new_with_type(Val::UUID("550e8400-e29b-41d4-a716-446655440000".to_string()), TypeId::UUID);

        // JSON/UUID to string
        assert_eq!(json_val.cast_to(TypeId::VarChar).unwrap().get_val(), &Val::VarLen("{\"key\": \"value\"}".to_string()));
        assert_eq!(uuid_val.cast_to(TypeId::Char).unwrap().get_val(), &Val::ConstLen("550e8400-e29b-41d4-a716-446655440000".to_string()));

        // String to JSON/UUID
        let json_str = Value::new("{\"test\": true}");
        let uuid_str = Value::new("123e4567-e89b-12d3-a456-426614174000");
        assert_eq!(json_str.cast_to(TypeId::JSON).unwrap().get_val(), &Val::JSON("{\"test\": true}".to_string()));
        assert_eq!(uuid_str.cast_to(TypeId::UUID).unwrap().get_val(), &Val::UUID("123e4567-e89b-12d3-a456-426614174000".to_string()));
    }

    #[test]
    fn test_enum_casts() {
        let enum_val = Value::new_with_type(Val::Enum(1, "ACTIVE".to_string()), TypeId::Enum);

        // Enum to integer types
        assert_eq!(enum_val.cast_to(TypeId::Integer).unwrap().get_val(), &Val::Integer(1));
        assert_eq!(enum_val.cast_to(TypeId::BigInt).unwrap().get_val(), &Val::BigInt(1));

        // Enum to string types
        assert_eq!(enum_val.cast_to(TypeId::VarChar).unwrap().get_val(), &Val::VarLen("ACTIVE".to_string()));
        assert_eq!(enum_val.cast_to(TypeId::Char).unwrap().get_val(), &Val::ConstLen("ACTIVE".to_string()));
    }

    #[test]
    fn test_point_casts() {
        let point_val = Value::new_with_type(Val::Point(3.14, 2.71), TypeId::Point);

        // Point to string
        assert_eq!(point_val.cast_to(TypeId::VarChar).unwrap().get_val(), &Val::VarLen("(3.14, 2.71)".to_string()));
        assert_eq!(point_val.cast_to(TypeId::Char).unwrap().get_val(), &Val::ConstLen("(3.14, 2.71)".to_string()));
    }

    #[test]
    fn test_vector_array_casts() {
        let vector_val = Value::new_vector(vec![Value::new(1), Value::new(2), Value::new(3)]);
        let array_val = Value::new_with_type(Val::Array(vec![Value::new(4), Value::new(5)]), TypeId::Array);

        // Vector to Array
        if let Val::Array(arr) = vector_val.cast_to(TypeId::Array).unwrap().get_val() {
            assert_eq!(arr.len(), 3);
        } else {
            panic!("Expected Array");
        }

        // Array to Vector
        if let Val::Vector(vec) = array_val.cast_to(TypeId::Vector).unwrap().get_val() {
            assert_eq!(vec.len(), 2);
        } else {
            panic!("Expected Vector");
        }
    }

    #[test]
    fn test_null_casts() {
        let null_val = Value::new(Val::Null);

        // NULL can be cast to any type and remains NULL
        assert_eq!(null_val.cast_to(TypeId::Integer).unwrap().get_val(), &Val::Null);
        assert_eq!(null_val.cast_to(TypeId::VarChar).unwrap().get_val(), &Val::Null);
        assert_eq!(null_val.cast_to(TypeId::Boolean).unwrap().get_val(), &Val::Null);
        assert_eq!(null_val.cast_to(TypeId::Timestamp).unwrap().get_val(), &Val::Null);

        // Type should be updated correctly
        assert_eq!(null_val.cast_to(TypeId::Integer).unwrap().get_type_id(), TypeId::Integer);
        assert_eq!(null_val.cast_to(TypeId::VarChar).unwrap().get_type_id(), TypeId::VarChar);
    }

    #[test]
    fn test_same_type_casts() {
        let int_val = Value::new(42);
        let str_val = Value::new("hello");

        // Casting to same type should return clone
        let cast_int = int_val.cast_to(TypeId::Integer).unwrap();
        let cast_str = str_val.cast_to(TypeId::VarChar).unwrap();

        assert_eq!(cast_int.get_val(), &Val::Integer(42));
        assert_eq!(cast_str.get_val(), &Val::VarLen("hello".to_string()));
        assert_eq!(cast_int.get_type_id(), TypeId::Integer);
        assert_eq!(cast_str.get_type_id(), TypeId::VarChar);
    }

    #[test]
    fn test_unsupported_casts() {
        let struct_val = Value::new_with_type(Val::Struct, TypeId::Struct);
        let vector_val = Value::new_vector(vec![Value::new(1)]);

        // Struct cannot be cast to anything
        assert!(struct_val.cast_to(TypeId::Integer).is_err());
        assert!(struct_val.cast_to(TypeId::VarChar).is_err());

        // Vector can only be cast to Array
        assert!(vector_val.cast_to(TypeId::Array).is_ok());
        assert!(vector_val.cast_to(TypeId::Integer).is_err());
        assert!(vector_val.cast_to(TypeId::VarChar).is_err());
    }

    #[test]
    fn test_range_overflow_casts() {
        // Test integer overflow scenarios
        let large_int = Value::new(i32::MAX);
        let very_large_bigint = Value::new(i64::MAX);

        // These should fail due to range overflow
        assert!(large_int.cast_to(TypeId::TinyInt).is_err());
        assert!(large_int.cast_to(TypeId::SmallInt).is_err());
        assert!(very_large_bigint.cast_to(TypeId::Integer).is_err());

        // Test timestamp overflow
        let large_timestamp = Value::new(u64::MAX);
        assert!(large_timestamp.cast_to(TypeId::Integer).is_err());

        // These should succeed
        let small_int = Value::new(100i32);
        assert!(small_int.cast_to(TypeId::TinyInt).is_ok());
        assert!(small_int.cast_to(TypeId::SmallInt).is_ok());
    }

    #[test]
    fn test_string_type_conversions() {
        let varchar_val = Value::new("test");
        let char_val = Value::new_with_type(Val::ConstLen("test".to_string()), TypeId::Char);

        // VarChar to Char
        assert_eq!(varchar_val.cast_to(TypeId::Char).unwrap().get_val(), &Val::ConstLen("test".to_string()));
        
        // Char to VarChar
        assert_eq!(char_val.cast_to(TypeId::VarChar).unwrap().get_val(), &Val::VarLen("test".to_string()));

        // Both should work with all numeric conversions
        assert_eq!(varchar_val.cast_to(TypeId::Integer).unwrap_err(), "Cannot convert string 'test' to Integer: invalid digit found in string");
        assert_eq!(Value::new("42").cast_to(TypeId::Integer).unwrap().get_val(), &Val::Integer(42));
    }
}