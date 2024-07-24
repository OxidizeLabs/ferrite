use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::Val;
use crate::types_db::value::Value;

// Implementation for IntegerType
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

    fn is_coercible_from(&self, type_id: TypeId) -> bool {
        matches!(type_id, TypeId::Integer)
    }

    fn serialize_to(&self, val: &Value, storage: &mut [u8]) {
        if let Val::Integer(i) = val.get_value() {
            let bytes = i.to_le_bytes();
            storage[..4].copy_from_slice(&bytes);
        }
    }

    fn deserialize_from(&self, storage: &mut [u8]) -> Value {
        let val = i32::from_le_bytes([storage[0], storage[1], storage[2], storage[3]]);
        Value::new(val)
    }
}

// fn main() {
//     // Example usage
//     let integer_type = IntegerType::new();
//     println!("{:?}", integer_type);
// }
