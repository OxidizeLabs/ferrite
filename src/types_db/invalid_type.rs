use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::Value;

// Implementation for InvalidType
pub struct InvalidType;

impl InvalidType {
    pub fn new() -> Self {
        InvalidType
    }
}

impl Type for InvalidType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Invalid
    }

    fn to_string(&self, _val: &Value) -> String {
        "INVALID".to_string()
    }
}
