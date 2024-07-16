use type_id::TypeId;
use types::{CmpBool, Type};
use value::Value;

// Implementation for BigIntType
pub struct BigIntType;

impl BigIntType {
    pub fn new() -> Self {
        BigIntType
    }
}

impl Type for BigIntType {
    fn get_type_id(&self) -> TypeId {
        TypeId::BigInt
    }

    fn is_coercible_from(&self, type_id: TypeId) -> bool {
        matches!(type_id, TypeId::BigInt)
    }
}

// fn main() {
//     // Example usage
//     let bigint_type = BigIntType::new();
//     println!("{:?}", bigint_type);
// }