use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::Value;

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

    fn is_coercible_from(&self, type_id: TypeId) -> bool {
        matches!(type_id, TypeId::SmallInt)
    }

    fn serialize_to(&self, val: &Value, storage: &mut [u8]) {
        todo!()
    }

    fn deserialize_from(&self, storage: &mut [u8]) -> Value {
        todo!()
    }
}

// fn main() {
//     // Example usage
//     let smallint_type = SmallIntType::new();
//     info!("{:?}", smallint_type);
// }
