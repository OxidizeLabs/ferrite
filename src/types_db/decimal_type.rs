use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::{Val, Value};

// Implementation for DecimalType
pub struct DecimalType;

impl DecimalType {
    pub fn new() -> Self {
        DecimalType
    }
}

impl Type for DecimalType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Decimal
    }

    fn is_coercible_from(&self, type_id: TypeId) -> bool {
        matches!(type_id, TypeId::Decimal)
    }

    fn serialize_to(&self, val: &Value, storage: &mut [u8]) {
        if let Val::Decimal(d) = val.get_value() {
            let bytes = d.to_le_bytes();
            storage[..8].copy_from_slice(&bytes);
        } else {
            panic!("Expected a Decimal value");
        }
    }

    fn deserialize_from(&self, storage: &mut [u8]) -> Value {
        let val = f64::from_le_bytes([
            storage[0], storage[1], storage[2], storage[3], storage[4], storage[5], storage[6],
            storage[7],
        ]);
        Value::new(val)
    }
}

// Test main function to demonstrate usage
// fn main() {
//     // Example usage
//     let decimal_type = DecimalType::new();
//
//     // Test serialization
//     let val = Value::new(12345.6789_f64);
//     let mut storage = [0u8; 8];
//     decimal_type.serialize_to(&val, &mut storage);
//     info!("Serialized storage: {:?}", storage);
//
//     // Test deserialization
//     let deserialized_val = decimal_type.deserialize_from(&mut storage);
//     info!("Deserialized value: {:?}", deserialized_val.get_value());
// }
