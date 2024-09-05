use crate::types_db::type_id::TypeId;
use crate::types_db::types::{get_type_size, CmpBool, Type};
use serde::de::{SeqAccess, Visitor};
use serde::ser::{SerializeSeq, SerializeStruct};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::io::Write;
use std::sync::Arc;

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
    Vector(Vec<Value>),
    Null,
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
            Val::Null => TypeId::Invalid
        };
        Value {
            value_: val,
            size_: Size::Length(get_type_size(type_id) as usize),
            manage_data_: false,
            type_id_: type_id,
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
            Val::Null => 1
        }
    }
}

impl Type for Value {
    fn get_type_id(&self) -> TypeId {
        self.type_id_
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        (self.value_ == other.value_).into()
    }

    fn compare_not_equals(&self, other: &Value) -> CmpBool {
        (self.value_ != other.value_).into()
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
            _ => CmpBool::CmpFalse,
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
            _ => CmpBool::CmpFalse,
        }
    }
}

// Implement From<T> for Val
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
            CmpBool::CmpNull => Val::Null
        }
    }
}

impl<T: Into<Val>> From<T> for Value {
    fn from(t: T) -> Self {
        Value::new(t)
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            // Detailed representation (activated by {:?} or {:#})
            write!(f, "{:?}", self.value_)
        } else {
            // Simple representation (activated by {})
            match &self.value_ {
                Val::Integer(i) => write!(f, "{}", i),
                Val::Decimal(fl) => write!(f, "{}", fl),
                Val::Boolean(b) => write!(f, "{}", b),
                Val::VarLen(s) => write!(f, "{}", s),
                Val::TinyInt(t) => write!(f, "{}", t),
                Val::SmallInt(sm) => write!(f, "{}", sm),
                Val::BigInt(bi) => write!(f, "{}", bi),
                Val::Timestamp(ti) => write!(f, "{}", ti),
                Val::ConstVarLen(c) => write!(f, "{}", c),
                Val::Vector(v) => {
                    write!(f, "[")?;
                    let mut first = true;
                    for value in v.iter() {
                        if !first {
                            write!(f, ", ")?;
                        }
                        write!(f, "{}", value)?;
                        first = false;
                    }
                    write!(f, "]")
                },                Val::Null => write!(f, "Null"),
            }
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn value_creation() {
        let bool_val = Value::new(true);
        assert_eq!(bool_val.get_type_id(), TypeId::Boolean);

        let int_val = Value::new(42i32);
        assert_eq!(int_val.get_type_id(), TypeId::Integer);

        let string_val = Value::new("Hello");
        assert_eq!(string_val.get_type_id(), TypeId::VarChar);

        let vec_val = Value::new_vector(vec![Value::new(1), Value::new(2), Value::new(3)]);
        assert_eq!(vec_val.get_type_id(), TypeId::Vector);
    }

    #[test]
    fn tvalue_comparison() {
        let val1 = Value::new(5);
        let val2 = Value::new(10);
        let val3 = Value::new(5);

        assert_eq!(val1.compare_less_than(&val2), CmpBool::CmpTrue);
        assert_eq!(val1.compare_greater_than(&val2), CmpBool::CmpFalse);
        assert_eq!(val1.compare_equals(&val3), CmpBool::CmpTrue);
    }

    #[test]
    fn serialize_val() {
        // Test serialization of different Val variants
        let val_boolean = Val::Boolean(true);
        let serialized_boolean = bincode::serialize(&val_boolean).expect("Serialization failed");
        assert_eq!(serialized_boolean, vec![0, 0, 0, 0, 1]); // Check the binary representation as needed

        let val_integer = Val::Integer(42);
        let serialized_integer = bincode::serialize(&val_integer).expect("Serialization failed");
        assert_eq!(serialized_integer, vec![3, 0, 0, 0, 42, 0, 0, 0]); // Adjust this to match the actual binary format

        let val_string = Val::VarLen("Hello".to_string());
        let serialized_string = bincode::serialize(&val_string).expect("Serialization failed");
        assert_eq!(serialized_string, vec![7, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 72, 101, 108, 108, 111]); // Binary format for the string

        let val_vector = Val::Vector(vec![Value::from(Val::Integer(1)), Value::from(Val::Integer(2))]);
        let serialized_vector = bincode::serialize(&val_vector).expect("Serialization failed");
        // Adjust this based on the expected binary format
    }

    #[test]
    fn deserialize_val() {
        // Test deserialization of binary data into Val variants
        let binary_boolean = vec![0, 0, 0, 0, 1];
        let deserialized_boolean: Val = bincode::deserialize(&binary_boolean).expect("Deserialization failed");
        assert_eq!(deserialized_boolean, Val::Boolean(true));

        let binary_integer = vec![3, 0, 0, 0, 42, 0, 0, 0];
        let deserialized_integer: Val = bincode::deserialize(&binary_integer).expect("Deserialization failed");
        assert_eq!(deserialized_integer, Val::Integer(42));

        let binary_string = vec![7, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 72, 101, 108, 108, 111];
        let deserialized_string: Val = bincode::deserialize(&binary_string).expect("Deserialization failed");
        assert_eq!(deserialized_string, Val::VarLen("Hello".to_string()));

        let binary_vector = bincode::serialize(&Val::Vector(vec![Value::from(Val::Integer(1)), Value::from(Val::Integer(2))]))
            .expect("Serialization failed");
        let deserialized_vector: Val = bincode::deserialize(&binary_vector).expect("Deserialization failed");
        assert_eq!(
            deserialized_vector,
            Val::Vector(vec![Value::from(Val::Integer(1)), Value::from(Val::Integer(2))])
        );
    }

    #[test]
    fn round_trip_val() {
        // Round-trip test for Val serialization and deserialization
        let original_val = Val::BigInt(123456789);
        let serialized = bincode::serialize(&original_val).expect("Serialization failed");
        let deserialized: Val = bincode::deserialize(&serialized).expect("Deserialization failed");
        assert_eq!(original_val, deserialized);
    }

    #[test]
    fn serialize_value() {
        // Test serialization of Value struct
        let value = Value {
            value_: Val::Integer(42),
            size_: Size::Length(4),
            manage_data_: false,
            type_id_: TypeId::Integer,
        };
        let serialized = bincode::serialize(&value).expect("Serialization failed");
        // Add checks for expected binary representation
    }

    #[test]
    fn deserialize_value() {
        // Test deserialization of binary data into Value struct
        let value = Value {
            value_: Val::Integer(42),
            size_: Size::Length(4),
            manage_data_: false,
            type_id_: TypeId::Integer, // Replace with an appropriate variant
        };
        let serialized = bincode::serialize(&value).expect("Serialization failed");
        let deserialized_value: Value = bincode::deserialize(&serialized).expect("Deserialization failed");
        assert_eq!(deserialized_value, value);
    }

    #[test]
    fn round_trip_value() {
        // Round-trip test for Value serialization and deserialization
        let original_value = Value {
            value_: Val::Decimal(123.456),
            size_: Size::Length(8),
            manage_data_: true,
            type_id_: TypeId::Decimal, // Replace with an appropriate variant
        };
        let serialized = bincode::serialize(&original_value).expect("Serialization failed");
        let deserialized: Value = bincode::deserialize(&serialized).expect("Deserialization failed");
        assert_eq!(original_value, deserialized);
    }

    #[test]
    fn test_value_display() {
        let int_value = Value::new(42);
        let float_value = Value::new(3.14);
        let bool_value = Value::new(true);
        let string_value = Value::new("Hello");
        let vector_value = Value::new_vector(vec![
            Value::new(1),
            Value::new("two"),
            Value::new(3.0),
        ]);

        assert_eq!(format!("{}", int_value), "42");
        assert_eq!(format!("{}", float_value), "3.14");
        assert_eq!(format!("{}", bool_value), "true");
        assert_eq!(format!("{}", string_value), "Hello");
        assert_eq!(format!("{}", vector_value), "[1, two, 3]");

        assert_eq!(format!("{:#}", int_value), "Integer(42)");
        assert_eq!(format!("{:#}", float_value), "Decimal(3.14)");
        assert_eq!(format!("{:#}", bool_value), "Boolean(true)");
        assert_eq!(format!("{:#}", string_value), "VarLen(\"Hello\")");
        assert_eq!(format!("{:#}", vector_value), "Vector([Value { value_: Integer(1), size_: Length(4), manage_data_: false, type_id_: Integer }, Value { value_: VarLen(\"two\"), size_: Length(0), manage_data_: false, type_id_: VarChar }, Value { value_: Decimal(3.0), size_: Length(8), manage_data_: false, type_id_: Decimal }])");
    }

}