use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::{Val, Value};

// Implementation for BooleanType
pub struct BooleanType;

impl BooleanType {
    pub fn new() -> Self {
        BooleanType
    }
}

impl Type for BooleanType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Boolean
    }

    fn is_coercible_from(&self, type_id: TypeId) -> bool {
        matches!(type_id, TypeId::Boolean)
    }

    fn serialize_to(&self, val: &Value, storage: &mut [u8]) {
        if let Val::Boolean(b) = val.get_value() {
            storage[0] = *b as u8;
        } else {
            panic!("Expected a boolean value");
        }
    }

    fn deserialize_from(&self, storage: &mut [u8]) -> Value {
        let b = storage[0] != 0; // 0 is false, anything else is true
        Value::new(b)
    }
}

// fn main() {
//     // Example usage
//     let boolean_type = BooleanType::new();
//     info!("{:?}", boolean_type);
// }
