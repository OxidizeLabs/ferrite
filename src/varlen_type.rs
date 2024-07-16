use type_id::TypeId;
use types::{CmpBool, Type};
use value::Value;

// Implementation for VarcharType
pub struct VarcharType;

impl VarcharType {
    pub fn new() -> Self {
        VarcharType
    }
}

impl Type for VarcharType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Varchar
    }

    fn is_coercible_from(&self, type_id: TypeId) -> bool {
        matches!(type_id, TypeId::Varchar)
    }
}

// fn main() {
//     // Example usage
//     let varchar_type = VarcharType::new();
//     println!("{:?}", varchar_type);
// }