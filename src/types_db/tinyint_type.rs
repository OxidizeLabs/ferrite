use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::{Val, Value};

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
}
