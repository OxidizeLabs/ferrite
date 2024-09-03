use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;

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
}
