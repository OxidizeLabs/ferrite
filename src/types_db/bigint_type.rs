use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::{Val, Value};

// Implementation for BigIntType
pub struct BigIntType;

impl BigIntType {
    pub fn new() -> Self {
        BigIntType
    }
}

impl Type for BigIntType {
    fn get_type_id(&self) -> TypeId {
        TypeId::BigInt
    }

    fn is_coercible_from(&self, type_id: TypeId) -> bool {
        matches!(type_id, TypeId::BigInt)
    }

    fn serialize_to(&self, val: &Value, storage: &mut [u8]) {
        if let Val::BigInt(i) = val.get_value() {
            let bytes = i.to_le_bytes();
            storage[..8].copy_from_slice(&bytes);
        } else {
            panic!("Expected a BigInt value");
        }
    }

    fn deserialize_from(&self, storage: &mut [u8]) -> Value {
        let val = i64::from_le_bytes([
            storage[0], storage[1], storage[2], storage[3], storage[4], storage[5], storage[6],
            storage[7],
        ]);
        Value::new(val)
    }
}

// Test main function to demonstrate usage
// fn main() {
//     // Example usage
//     let bigint_type = BigIntType::new();
//
//     // Test serialization
//     let val = Value::new(1234567890123456789_i64);
//     let mut storage = [0u8; 8];
//     bigint_type.serialize_to(&val, &mut storage);
//     println!("Serialized storage: {:?}", storage);
//
//     // Test deserialization
//     let deserialized_val = bigint_type.deserialize_from(&mut storage);
//     println!("Deserialized value: {:?}", deserialized_val.get_value());
// }
