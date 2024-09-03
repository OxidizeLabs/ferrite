use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;

// Implementation for TimestampType
pub struct TimestampType;

impl TimestampType {
    pub fn new() -> Self {
        TimestampType
    }
}

impl Type for TimestampType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Timestamp
    }
}
