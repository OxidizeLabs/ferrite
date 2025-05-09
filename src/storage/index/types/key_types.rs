use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::{Val, Value};

/// Trait for types that can be used as keys in indexes
pub trait KeyType: Sized + Clone + PartialEq + PartialOrd {
    /// Convert this key to a Value for storage
    fn to_value(&self) -> Value;

    /// Create a key from a Value
    fn from_value(value: &Value) -> Result<Self, String>;

    /// Get the TypeId for this key type
    fn type_id() -> TypeId;
}

impl KeyType for i32 {
    fn to_value(&self) -> Value {
        Value::new_with_type(Val::Integer(*self), TypeId::Integer)
    }

    fn from_value(value: &Value) -> Result<Self, String> {
        value.as_integer()
    }

    fn type_id() -> TypeId {
        TypeId::Integer
    }
}

impl KeyType for String {
    fn to_value(&self) -> Value {
        Value::new_with_type(Val::VarLen(self.clone()), TypeId::VarChar)
    }

    fn from_value(value: &Value) -> Result<Self, String> {
        match value.get_val() {
            Val::VarLen(s) => Ok(s.clone()),
            Val::ConstLen(s) => Ok(s.clone()),
            _ => Err(format!("Cannot convert {:?} to String", value)),
        }
    }

    fn type_id() -> TypeId {
        TypeId::VarChar
    }
}
