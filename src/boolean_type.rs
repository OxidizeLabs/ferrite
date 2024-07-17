use type_id::TypeId;
use types::Type;
use value::{Value, Val};

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
}

// fn main() {
//     // Example usage
//     let boolean_type = BooleanType::new();
//     println!("{:?}", boolean_type);
// }