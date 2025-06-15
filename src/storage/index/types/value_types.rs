use crate::common::rid::RID;
use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::{Val, Value};
use bincode;

/// Trait for types that can be used as values in indexes
pub trait ValueType: Sized + Clone {
    /// Convert this value to a Value for storage
    fn to_value(&self) -> Value;

    /// Create a value from a Value
    fn from_value(value: &Value) -> Result<Self, String>;
}

// Implementation for RID (Row ID)
impl ValueType for RID {
    fn to_value(&self) -> Value {
        // Assuming RID can be represented as a BigInt or Struct
        Value::new_with_type(Val::BigInt(self.to_i64()), TypeId::BigInt)
    }

    fn from_value(value: &Value) -> Result<Self, String> {
        let id = value.as_bigint()?;
        Ok(RID::from_i64(id))
    }
}
