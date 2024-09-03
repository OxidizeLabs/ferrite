use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;

// Implementation for SmallIntType
pub struct SmallIntType;

impl SmallIntType {
    pub fn new() -> Self {
        SmallIntType
    }
}

impl Type for SmallIntType {
    fn get_type_id(&self) -> TypeId {
        TypeId::SmallInt
    }
}
