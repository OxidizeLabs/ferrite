use type_id::TypeId;
use types::Type;
use value::{Val, Value};

// Implementation for TinyIntType
pub struct TinyIntType;

impl TinyIntType {
    pub fn new() -> Self {
        TinyIntType
    }
}

impl Type for TinyIntType {
    fn get_type_id(&self) -> TypeId {
        TypeId::TinyInt
    }

    fn is_coercible_from(&self, type_id: TypeId) -> bool {
        matches!(type_id, TypeId::TinyInt)
    }

    fn serialize_to(&self, val: &Value, storage: &mut [u8]) {
        if let Val::TinyInt(i) = val.get_value() {
            storage[0] = *i as u8;
        } else {
            panic!("Expected a TinyInt value");
        }
    }

    fn deserialize_from(&self, storage: &mut [u8]) -> Value {
        let val = storage[0] as i8;
        Value::new(val)
    }
}

// Test main function to demonstrate usage
// fn main() {
//     // Example usage
//     let tinyint_type = TinyIntType::new();
//
//     // Test serialization
//     let val = Value::new(123_i8);
//     let mut storage = [0u8; 1];
//     tinyint_type.serialize_to(&val, &mut storage);
//     println!("Serialized storage: {:?}", storage);
//
//     // Test deserialization
//     let deserialized_val = tinyint_type.deserialize_from(&mut storage);
//     println!("Deserialized value: {:?}", deserialized_val.get_value());
// }
