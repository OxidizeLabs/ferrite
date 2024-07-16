use type_id::TypeId;
use types::{CmpBool, Type};
use value::Value;

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
}

// fn main() {
//     // Example usage
//     let tinyint_type = TinyInt::new();
//     println!("{:?}", tinyint_type);
// }