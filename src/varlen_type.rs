use type_id::TypeId;
use types::{Type};
use value::{Value, Val};

// Implementation for VarCharType
pub struct VarCharType;

impl VarCharType {
    pub fn new() -> Self {
        VarCharType
    }
}

impl Type for VarCharType {
    fn get_type_id(&self) -> TypeId {
        TypeId::VarChar
    }

    fn is_coercible_from(&self, type_id: TypeId) -> bool {
        matches!(type_id, TypeId::VarChar)
    }

    fn serialize_to(&self, val: &Value, storage: &mut [u8]) {
        if let Val::VarLen(s) = val.get_value() {
            let len = s.len() as u32;
            let len_bytes = len.to_le_bytes();
            let string_bytes = s.as_bytes();
            let total_len = len_bytes.len() + string_bytes.len();

            if storage.len() < total_len {
                panic!("Storage buffer is too small");
            }

            storage[..4].copy_from_slice(&len_bytes);
            storage[4..4+string_bytes.len()].copy_from_slice(&string_bytes);
        } else {
            panic!("Expected a VarChar value");
        }
    }

    fn deserialize_from(&self, storage: &mut [u8]) -> Value {
        let len = u32::from_le_bytes([storage[0], storage[1], storage[2], storage[3]]) as usize;
        let string_bytes = &storage[4..4+len];
        let s = String::from_utf8_lossy(string_bytes).to_string();
        Value::new(s)
    }
}

// Test main function to demonstrate usage
// fn main() {
//     // Example usage
//     let varchar_type = VarCharType::new();
//
//     // Test serialization
//     let val = Value::new("Hello, world!".to_string());
//     let mut storage = vec![0u8; 4 + "Hello, world!".len()];
//     varchar_type.serialize_to(&val, &mut storage);
//     println!("Serialized storage: {:?}", storage);
//
//     // Test deserialization
//     let deserialized_val = varchar_type.deserialize_from(&mut storage);
//     println!("Deserialized value: {:?}", deserialized_val.get_value());
// }
