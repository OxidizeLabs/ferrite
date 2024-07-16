use type_id::TypeId;
use types::{CmpBool, Type};
use value::Value;

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
}

// fn main() {
//     // Example usage
//     let boolean_type = BooleanType::new();
//     println!("{:?}", boolean_type);
// }