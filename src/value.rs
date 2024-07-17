use std::fmt;
use type_id::TypeId;
use types::{get_instance, Type};

const DB_VALUE_NULL: u32 = u32::MAX;

#[derive(Debug, Clone)]
pub struct Value {
    value_: Val,
    size_: Size,
    manage_data_: bool,
    pub(crate) type_id_: TypeId
}

#[derive(Clone, Debug)]
enum Val {
    Boolean(i8),
    TinyInt(i8),
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Decimal(f64),
    Timestamp(u64),
    VarLen(*mut u8),
    ConstVarLen(*const u8),
}

#[derive(Debug, Clone)]
enum Size {
    Length(u32),
    ElemTypeId(TypeId),
}

impl Val {
    pub fn new_boolean(value: i8) -> Self {
        Val::Boolean(value)
    }

    pub fn new_tinyint(value: i8) -> Self {
        Val::TinyInt(value)
    }

    pub fn new_smallint(value: i16) -> Self {
        Val::SmallInt(value)
    }

    pub fn new_integer(value: i32) -> Self {
        Val::Integer(value)
    }

    pub fn new_bigint(value: i64) -> Self {
        Val::BigInt(value)
    }

    pub fn new_decimal(value: f64) -> Self {
        Val::Decimal(value)
    }

    pub fn new_timestamp(value: u64) -> Self {
        Val::Timestamp(value)
    }

    pub fn new_varlen(value: *mut u8) -> Self {
        Val::VarLen(value)
    }

    pub fn new_const_varlen(value: *const u8) -> Self {
        Val::ConstVarLen(value)
    }
}

impl Value {
    pub fn new(type_id: TypeId) -> Self {
        Self {
            value_: Val::Integer(0),  // Default to Integer with value 0
            size_: Size::Length(0),
            manage_data_: false,
            type_id_: type_id,
        }
    }

    pub fn from_boolean(type_id: TypeId, b: i8) -> Self {
        Self {
            value_: Val::new_boolean(b),
            size_: Size::Length(1),
            manage_data_: false,
            type_id_: type_id,
        }
    }

    pub fn from_tinyint(type_id: TypeId, i: i8) -> Self {
        Self {
            value_: Val::new_tinyint(i),
            size_: Size::Length(1),
            manage_data_: false,
            type_id_: TypeId::TinyInt,
        }
    }

    pub fn from_smallint(type_id: TypeId, i: i16) -> Self {
        Self {
            value_: Val::new_smallint(i),
            size_: Size::Length(2),
            manage_data_: false,
            type_id_: TypeId::SmallInt,
        }
    }

    pub fn from_integer(type_id: TypeId, i: i32) -> Self {
        Self {
            value_: Val::new_integer(i),
            size_: Size::Length(4),
            manage_data_: false,
            type_id_: TypeId::Integer,
        }
    }

    pub fn from_bigint(type_id: TypeId, i: i64) -> Self {
        Self {
            value_: Val::new_bigint(i),
            size_: Size::Length(8),
            manage_data_: false,
            type_id_: TypeId::BigInt,
        }
    }

    pub fn from_decimal(type_id: TypeId, d: f64) -> Self {
        Self {
            value_: Val::new_decimal(d),
            size_: Size::Length(8),
            manage_data_: false,
            type_id_: TypeId::Decimal,
        }
    }

    pub fn from_timestamp(type_id: TypeId, t: u64) -> Self {
        Self {
            value_: Val::new_timestamp(t),
            size_: Size::Length(8),
            manage_data_: false,
            type_id_: TypeId::Timestamp,
        }
    }

    pub fn from_str(type_id: TypeId, data: &str) -> Self {
        Self {
            value_: Val::new_varlen(data.as_ptr() as *mut u8),
            size_: Size::Length(data.len() as u32),
            manage_data_: false,
            type_id_: TypeId::VarChar,
        }
    }

    pub fn from_const_str(type_id: TypeId, data: &str) -> Self {
        Self {
            value_: Val::new_const_varlen(data.as_ptr()),
            size_: Size::Length(data.len() as u32),
            manage_data_: false,
            type_id_: TypeId::VarChar,
        }
    }

    pub fn check_integer(&self) -> bool {
        matches!(self.type_id_, TypeId::TinyInt | TypeId::SmallInt | TypeId::Integer | TypeId::BigInt)
    }

    pub fn check_comparable(&self, other: &Value) -> bool {
        self.type_id_ == other.type_id_
    }

    pub fn get_type_id(&self) -> TypeId {
        self.type_id_
    }

    pub fn get_storage_size(&self) -> u32 {
        get_instance(self.type_id_).get_storage_size(self)
    }

    pub fn get_data() {
        unimplemented!()
    }

    pub fn get_vector() {
        unimplemented!()
    }

    pub fn cast_as() {
        unimplemented!()
    }

    pub fn compare_exactly_equals() {
        unimplemented!()
    }

    pub fn compare_equals() {
        unimplemented!()
    }

    pub fn compare_not_equals() {
        unimplemented!()
    }

    pub fn compare_less_than() {
        unimplemented!()
    }

    pub fn compare_less_than_equals() {
        unimplemented!()
    }

    pub fn compare_greater_than() {
        unimplemented!()
    }

    pub fn compare_greater_than_equals() {
        unimplemented!()
    }

    pub fn add() {
        unimplemented!()
    }

    pub fn subtract() {
        unimplemented!()
    }

    pub fn multiply() {
        unimplemented!()
    }

    pub fn divide() {
        unimplemented!()
    }

    pub fn modulo() {
        unimplemented!()
    }

    pub fn min() {
        unimplemented!()
    }

    pub fn max() {
        unimplemented!()
    }

    pub fn sqrt() {
        unimplemented!()
    }

    pub fn operate_null() {
        unimplemented!()
    }

    pub fn is_zero() {
        unimplemented!()
    }

    pub fn is_null() {
        unimplemented!()
    }

    pub fn serialize_to() {
        unimplemented!()
    }

    pub fn serialize_from() {
        unimplemented!()
    }

    pub fn deserialize_from() {
        unimplemented!()
    }

    pub fn to_string() {
        unimplemented!()
    }

    pub fn copy() {
        unimplemented!()
    }
}

// Implement fmt::Display for Value to use with fmt::formatter
impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

// Example usage with fmt::Debug
// fn main() {
//     let val = Value::new(TypeId::Boolean);
//     println!("{:?}", val);
//     println!("{}", val.to_string());
// }
