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
    Timestamp(u64),
    VarLen(String),
    ConstVarLen(String),
    Vector(Vec<Value>),
    Null,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, PartialOrd)]
pub enum Size {
    Length(usize),
    ElemTypeId(TypeId),
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Value {
    pub(crate) value_: Val,
    pub(crate) size_: Size,
    pub(crate) manage_data_: bool,
    pub(crate) type_id_: TypeId,
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
            Val::Null => TypeId::Invalid,
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
            Val::Null => 1,
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
            Val::Timestamp(t) => bytes.extend_from_slice(&t.to_le_bytes()),
            Val::VarLen(s) | Val::ConstVarLen(s) => bytes.extend_from_slice(s.as_bytes()),
            Val::Vector(v) => {
                for value in v {
                    bytes.extend(value.as_bytes());
                }
            }
            Val::Null => bytes.extend_from_slice(&[0u8]),
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
            Val::Decimal(_) |
            Val::TinyInt(_) |
            Val::SmallInt(_) |
            Val::Integer(_) |
            Val::BigInt(_)
        )
    }

    pub fn new_with_type(val: Val, type_id: TypeId) -> Self {
        Self {
            value_: val,
            size_: type_id.get_value().size_,
            manage_data_: false,
            type_id_: type_id,
        }
    }

    pub fn get_type_id(&self) -> TypeId {
        self.type_id_
    }

    fn add(&self, other: &Value) -> Result<Value, String> {
        if self.is_null() || other.is_null() {
            return Ok(Value::new(Val::Null));
        }

        match (&self.value_, &other.value_) {
            (Val::Integer(a), Val::Integer(b)) => {
                Ok(Value::new_with_type(Val::Integer(a + b), TypeId::Integer))
            },
            (Val::BigInt(a), Val::BigInt(b)) => {
                Ok(Value::new_with_type(Val::BigInt(a + b), TypeId::BigInt))
            },
            (Val::Integer(a), Val::BigInt(b)) => {
                Ok(Value::new_with_type(Val::BigInt(*a as i64 + b), TypeId::BigInt))
            },
            (Val::BigInt(a), Val::Integer(b)) => {
                Ok(Value::new_with_type(Val::BigInt(a + *b as i64), TypeId::BigInt))
            },
            (Val::Decimal(a), Val::Decimal(b)) => {
                Ok(Value::new_with_type(Val::Decimal(a + b), TypeId::Decimal))
            },
            (Val::SmallInt(a), Val::SmallInt(b)) => {
                Ok(Value::new_with_type(Val::SmallInt(a + b), TypeId::SmallInt))
            },
            (Val::TinyInt(a), Val::TinyInt(b)) => {
                Ok(Value::new_with_type(Val::TinyInt(a + b), TypeId::TinyInt))
            },
            // Promote smaller types to larger ones
            (Val::TinyInt(a), Val::Integer(b)) => {
                Ok(Value::new_with_type(Val::Integer(*a as i32 + b), TypeId::Integer))
            },
            (Val::Integer(a), Val::TinyInt(b)) => {
                Ok(Value::new_with_type(Val::Integer(a + *b as i32), TypeId::Integer))
            },
            (Val::SmallInt(a), Val::Integer(b)) => {
                Ok(Value::new_with_type(Val::Integer(*a as i32 + b), TypeId::Integer))
            },
            (Val::Integer(a), Val::SmallInt(b)) => {
                Ok(Value::new_with_type(Val::Integer(a + *b as i32), TypeId::Integer))
            },
            _ => Err(format!("Cannot add values of types {:?} and {:?}", 
                self.get_type_id(), other.get_type_id()))
        }
    }
}

impl Type for Value {
    fn get_type_id(&self) -> TypeId {
        self.type_id_
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match (&self.value_, &other.value_) {
            (Val::Null, _) | (_, Val::Null) => CmpBool::CmpNull,
            _ => (self.value_ == other.value_).into(),
        }
    }

    fn compare_not_equals(&self, other: &Value) -> CmpBool {
        match (&self.value_, &other.value_) {
            (Val::Null, _) | (_, Val::Null) => CmpBool::CmpNull,
            _ => (self.value_ != other.value_).into(),
        }
    }

    fn compare_less_than(&self, other: &Value) -> CmpBool {
        match (&self.value_, &other.value_) {
            (Val::Null, _) | (_, Val::Null) => CmpBool::CmpNull,
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

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match (&self.value_, &other.value_) {
            (Val::Null, _) | (_, Val::Null) => CmpBool::CmpNull,
            _ => match self.compare_greater_than(other) {
                CmpBool::CmpTrue => CmpBool::CmpFalse,
                CmpBool::CmpFalse => CmpBool::CmpTrue,
                CmpBool::CmpNull => CmpBool::CmpNull,
            },
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match (&self.value_, &other.value_) {
            (Val::Null, _) | (_, Val::Null) => CmpBool::CmpNull,
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

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match (&self.value_, &other.value_) {
            (Val::Null, _) | (_, Val::Null) => CmpBool::CmpNull,
            _ => match self.compare_less_than(other) {
                CmpBool::CmpTrue => CmpBool::CmpFalse,
                CmpBool::CmpFalse => CmpBool::CmpTrue,
                CmpBool::CmpNull => CmpBool::CmpNull,
            },
        }
    }

    fn add(&self, other: &Value) -> Result<Value, String> {
        match (self.get_type_id(), other.get_type_id()) {
            (TypeId::Integer, TypeId::Integer) => {
                let a = self.as_integer()?;
                let b = other.as_integer()?;
                Ok(Value::new_with_type(Val::Integer(a + b), TypeId::Integer))
            },
            (TypeId::BigInt, TypeId::BigInt) => {
                let a = self.as_bigint()?;
                let b = other.as_bigint()?;
                Ok(Value::new_with_type(Val::BigInt(a + b), TypeId::BigInt))
            },
            (TypeId::SmallInt, TypeId::SmallInt) => {
                let a = self.as_smallint()?;
                let b = other.as_smallint()?;
                Ok(Value::new_with_type(Val::SmallInt(a + b), TypeId::SmallInt))
            },
            (TypeId::TinyInt, TypeId::TinyInt) => {
                let a = self.as_tinyint()?;
                let b = other.as_tinyint()?;
                Ok(Value::new_with_type(Val::TinyInt(a + b), TypeId::TinyInt))
            },
            (TypeId::Decimal, TypeId::Decimal) => {
                let a = self.as_decimal()?;
                let b = other.as_decimal()?;
                Ok(Value::new_with_type(Val::Decimal(a + b), TypeId::Decimal))
            },
            // Handle type promotions
            (TypeId::TinyInt, TypeId::Integer) | (TypeId::Integer, TypeId::TinyInt) => {
                let a = self.as_integer()?;
                let b = other.as_integer()?;
                Ok(Value::new_with_type(Val::Integer(a + b), TypeId::Integer))
            },
            (TypeId::SmallInt, TypeId::Integer) | (TypeId::Integer, TypeId::SmallInt) => {
                let a = self.as_integer()?;
                let b = other.as_integer()?;
                Ok(Value::new_with_type(Val::Integer(a + b), TypeId::Integer))
            },
            (TypeId::Integer, TypeId::BigInt) | (TypeId::BigInt, TypeId::Integer) => {
                let a = self.as_bigint()?;
                let b = other.as_bigint()?;
                Ok(Value::new_with_type(Val::BigInt(a + b), TypeId::BigInt))
            },
            _ => Err(format!("Cannot add values of types {:?} and {:?}", 
                self.get_type_id(), other.get_type_id()))
        }
    }

    fn as_integer(&self) -> Result<i32, String> {
        match &self.value_ {
            Val::Integer(i) => Ok(*i),
            Val::TinyInt(i) => Ok(*i as i32),
            Val::SmallInt(i) => Ok(*i as i32),
            Val::BigInt(i) => Ok(*i as i32),
            _ => Err(format!("Cannot convert {:?} to integer", self.type_id_))
        }
    }

    fn as_bigint(&self) -> Result<i64, String> {
        match &self.value_ {
            Val::BigInt(i) => Ok(*i),
            Val::Integer(i) => Ok(*i as i64),
            Val::SmallInt(i) => Ok(*i as i64),
            Val::TinyInt(i) => Ok(*i as i64),
            _ => Err(format!("Cannot convert {:?} to bigint", self.type_id_))
        }
    }

    fn as_smallint(&self) -> Result<i16, String> {
        match &self.value_ {
            Val::SmallInt(i) => Ok(*i),
            Val::TinyInt(i) => Ok(*i as i16),
            _ => Err(format!("Cannot convert {:?} to smallint", self.type_id_))
        }
    }

    fn as_tinyint(&self) -> Result<i8, String> {
        match &self.value_ {
            Val::TinyInt(i) => Ok(*i),
            _ => Err(format!("Cannot convert {:?} to tinyint", self.type_id_))
        }
    }

    fn as_decimal(&self) -> Result<f64, String> {
        match &self.value_ {
            Val::Decimal(d) => Ok(*d),
            Val::Integer(i) => Ok(*i as f64),
            Val::BigInt(i) => Ok(*i as f64),
            Val::SmallInt(i) => Ok(*i as f64),
            Val::TinyInt(i) => Ok(*i as f64),
            _ => Err(format!("Cannot convert {:?} to decimal", self.type_id_))
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
            TypeId::Timestamp => Val::Timestamp(0),
            TypeId::VarChar => Val::VarLen(String::new()),
            TypeId::Vector => Val::Vector(Vec::new()),
            TypeId::Invalid => Val::Null,
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

impl<T: Into<Val>> From<T> for Value {
    fn from(t: T) -> Self {
        Value::new(t)
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.value_ {
            Val::Null => write!(f, "NULL"),
            Val::Boolean(b) => write!(f, "{}", b),
            Val::TinyInt(i) => write!(f, "{}", i),
            Val::SmallInt(i) => write!(f, "{}", i),
            Val::Integer(i) => write!(f, "{}", i),
            Val::BigInt(i) => write!(f, "{}", i),
            Val::Decimal(d) => write!(f, "{}", d),
            Val::Timestamp(ts) => write!(f, "{}", ts),
            Val::VarLen(s) => write!(f, "{}", s),
            Val::Vector(v) => write!(f, "{:?}", v),
            Val::ConstVarLen(s) => write!(f, "{}", s),
        }
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.type_id_.hash(state);
        match &self.value_ {
            Val::Boolean(b) => b.hash(state),
            Val::TinyInt(i) => i.hash(state),
            Val::SmallInt(i) => i.hash(state),
            Val::Integer(i) => i.hash(state),
            Val::BigInt(i) => i.hash(state),
            Val::Decimal(f) => f.to_bits().hash(state),
            Val::Timestamp(t) => t.hash(state),
            Val::VarLen(s) | Val::ConstVarLen(s) => s.hash(state),
            Val::Vector(v) => {
                for value in v {
                    value.hash(state);
                }
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
    use crate::types_db::types::Type;
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
            vec![7, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 72, 101, 108, 108, 111]
        ); // Binary format for the string

        let val_vector = Val::Vector(vec![
            Value::from(Val::Integer(1)),
            Value::from(Val::Integer(2)),
        ]);
        let serialized_vector = bincode::serialize(&val_vector).expect("Serialization failed");
        assert_eq!(
            serialized_vector,
            vec![
                9, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0,
                0, 0, 0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0,
                0, 0, 3, 0, 0, 0
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

        let binary_string = vec![7, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 72, 101, 108, 108, 111];
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

            // Combine all bytes
            let mut expected: Vec<u8> = Vec::new();
            expected.extend(value_bytes);
            expected.extend(size_bytes);
            expected.extend(manage_data_bytes);
            expected.extend(type_id_bytes);

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

    #[test]
    fn test_value_debug_display() {
        assert_eq!(format!("{:#}", Value::new(42)), "Integer(42)");
        assert_eq!(format!("{:#}", Value::new(3.14)), "Decimal(3.14)");
        assert_eq!(format!("{:#}", Value::new(true)), "Boolean(true)");
        assert_eq!(format!("{:#}", Value::new("Hello")), "VarLen(\"Hello\")");
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
