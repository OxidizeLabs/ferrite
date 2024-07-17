use type_id::TypeId;
use types::{Type};

// Implementation for VarcharType
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
}

// fn main() {
//     // Example usage
//     let varchar_type = VarcharType::new();
//     println!("{:?}", varchar_type);
// }