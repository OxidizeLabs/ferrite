use type_id::TypeId;
use types::{CmpBool, Type};
use value::Value;

// Implementation for DecimalType
pub struct DecimalType;

impl DecimalType {
    pub fn new() -> Self {
        DecimalType
    }
}

impl Type for DecimalType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Decimal
    }

    fn is_coercible_from(&self, type_id: TypeId) -> bool {
        matches!(type_id, TypeId::Decimal)
    }
}

// fn main() {
//     // Example usage
//     let decimal_type = DecimalType::new();
//     println!("{:?}", decimal_type);
// }