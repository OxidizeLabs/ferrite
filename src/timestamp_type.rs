use type_id::TypeId;
use types::{CmpBool, Type};
use value::Value;

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

    fn is_coercible_from(&self, type_id: TypeId) -> bool {
        matches!(type_id, TypeId::Timestamp)
    }
}

// fn main() {
//     // Example usage
//     let timestamp_type = TimestampType::new();
//     println!("{:?}", timestamp_type);
// }