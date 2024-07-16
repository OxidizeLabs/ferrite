use type_id::TypeId;
use types::{CmpBool, Type};
use value::Value;

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
}

// fn main() {
//     // Example usage
//     let vector_type = VectorType::new();
//     println!("{:?}", vector_type);
// }