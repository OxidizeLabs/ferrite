use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::{Val, Value};

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
}
