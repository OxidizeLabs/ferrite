use type_id::TypeId;
use types::Type;
use value::{Val, Value};

// Implementation for VectorType
pub struct VectorType;

impl VectorType {
    pub fn new() -> Self {
        VectorType
    }
}

impl Type for VectorType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Vector
    }

    fn is_coercible_from(&self, type_id: TypeId) -> bool {
        matches!(type_id, TypeId::Vector)
    }

    fn serialize_to(&self, val: &Value, storage: &mut [u8]) {
        if let Val::Vector(v) = val.get_value() {
            let len = v.len() as u32;
            let len_bytes = len.to_le_bytes();
            let total_len = 4 + len * 4;

            if storage.len() < total_len as usize {
                panic!("Storage buffer is too small");
            }

            storage[..4].copy_from_slice(&len_bytes);
            for (i, &val) in v.iter().enumerate() {
                let bytes = val.to_le_bytes();
                let start = 4 + i * 4;
                let end = start + 4;
                storage[start..end].copy_from_slice(&bytes);
            }
        } else {
            panic!("Expected a Vector value");
        }
    }

    fn deserialize_from(&self, storage: &mut [u8]) -> Value {
        let len = u32::from_le_bytes([storage[0], storage[1], storage[2], storage[3]]) as usize;
        let mut v = Vec::with_capacity(len);
        for i in 0..len {
            let start = 4 + i * 4;
            let end = start + 4;
            let val = i32::from_le_bytes([
                storage[start],
                storage[start + 1],
                storage[start + 2],
                storage[start + 3],
            ]);
            v.push(val);
        }
        Value::new(v)
    }
}

// Test main function to demonstrate usage
// fn main() {
//     // Example usage
//     let vector_type = VectorType::new();
//
//     // Test serialization
//     let val = Value::new(vec![1, 2, 3, 4, 5]);
//     let mut storage = vec![0u8; 4 + 5 * 4];
//     vector_type.serialize_to(&val, &mut storage);
//     println!("Serialized storage: {:?}", storage);
//
//     // Test deserialization
//     let deserialized_val = vector_type.deserialize_from(&mut storage);
//     println!("Deserialized value: {:?}", deserialized_val.get_value());
// }
