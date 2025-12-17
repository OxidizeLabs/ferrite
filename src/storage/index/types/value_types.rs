use crate::common::rid::RID;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};

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
        // Store RID as fixed-width binary: [page_id: u64][slot_num: u32] (little-endian).
        Value::new_with_type(Val::Binary(self.to_bytes_le().to_vec()), TypeId::Binary)
    }

    fn from_value(value: &Value) -> Result<Self, String> {
        match value.get_val() {
            Val::Binary(bytes) => RID::try_deserialize(bytes).ok_or_else(|| {
                format!(
                    "Invalid RID binary encoding: expected at least {} bytes, got {}",
                    RID::ENCODED_LEN,
                    bytes.len()
                )
            }),
            other => Err(format!(
                "Cannot convert value to RID: expected Binary, got {other:?}"
            )),
        }
    }
}
