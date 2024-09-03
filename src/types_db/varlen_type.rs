use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::{Val, Value};

// Implementation for VarCharType
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
}
