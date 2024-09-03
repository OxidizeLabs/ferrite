use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::{Val, Value};

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
}
