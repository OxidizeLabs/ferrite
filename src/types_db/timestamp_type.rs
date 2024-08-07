use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::{Val, Value};

// Implementation for TimestampType
pub struct TimestampType;

impl TimestampType {
    pub fn new() -> Self {
        TimestampType
    }
}

impl Type for TimestampType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Timestamp
    }

    fn is_coercible_from(&self, type_id: TypeId) -> bool {
        matches!(type_id, TypeId::Timestamp)
    }

    fn serialize_to(&self, val: &Value, storage: &mut [u8]) {
        if let Val::Timestamp(t) = val.get_value() {
            let bytes = t.to_le_bytes();
            storage[..8].copy_from_slice(&bytes);
        } else {
            panic!("Expected a Timestamp value");
        }
    }

    fn deserialize_from(&self, storage: &mut [u8]) -> Value {
        let val = u64::from_le_bytes([
            storage[0], storage[1], storage[2], storage[3], storage[4], storage[5], storage[6],
            storage[7],
        ]);
        Value::new(val)
    }
}

// Test main function to demonstrate usage
// fn main() {
//     // Example usage
//     let timestamp_type = TimestampType::new();
//
//     // Test serialization
//     let val = Value::new(1627842123_u64);
//     let mut storage = [0u8; 8];
//     timestamp_type.serialize_to(&val, &mut storage);
//     info!("Serialized storage: {:?}", storage);
//
//     // Test deserialization
//     let deserialized_val = timestamp_type.deserialize_from(&mut storage);
//     info!("Deserialized value: {:?}", deserialized_val.get_value());
// }
