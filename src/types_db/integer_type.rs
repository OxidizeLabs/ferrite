use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;

// Implementation for IntegerType
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct IntegerType;

impl IntegerType {
    pub fn new() -> Self {
        IntegerType
    }
}

impl Type for IntegerType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Integer
    }
}
