use type_id::TypeId;
use types::{CmpBool, Type};
use value::Value;

// Implementation for IntegerType
pub struct IntegerType;

impl IntegerType {
    pub fn new() -> Self {
        IntegerType
    }
}

impl Type for IntegerType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Integer
    }

    fn is_coercible_from(&self, type_id: TypeId) -> bool {
        matches!(type_id, TypeId::Integer)
    }
}

// fn main() {
//     // Example usage
//     let integer_type = IntegerType::new();
//     println!("{:?}", integer_type);
// }