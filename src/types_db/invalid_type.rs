use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};

#[derive(Debug)]
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

    fn compare_equals(&self, _other: &Value) -> CmpBool {
        CmpBool::CmpNull
    }

    fn compare_not_equals(&self, _other: &Value) -> CmpBool {
        CmpBool::CmpNull
    }

    fn compare_less_than(&self, _other: &Value) -> CmpBool {
        CmpBool::CmpNull
    }

    fn compare_less_than_equals(&self, _other: &Value) -> CmpBool {
        CmpBool::CmpNull
    }

    fn compare_greater_than(&self, _other: &Value) -> CmpBool {
        CmpBool::CmpNull
    }

    fn compare_greater_than_equals(&self, _other: &Value) -> CmpBool {
        CmpBool::CmpNull
    }

    fn add(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot perform arithmetic on invalid type".to_string())
    }

    fn subtract(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot perform arithmetic on invalid type".to_string())
    }

    fn multiply(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot perform arithmetic on invalid type".to_string())
    }

    fn divide(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot perform arithmetic on invalid type".to_string())
    }

    fn modulo(&self, _other: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn min(&self, _other: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn max(&self, _other: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn to_string(&self, _val: &Value) -> String {
        "INVALID".to_string()
    }
}

pub static INVALID_TYPE_INSTANCE: InvalidType = InvalidType;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_comparisons() {
        let invalid_type = InvalidType::new();
        let val = Value::new(1i32);
        let null = Value::new(Val::Null);

        // All comparisons should return CmpNull
        assert_eq!(invalid_type.compare_equals(&val), CmpBool::CmpNull);
        assert_eq!(invalid_type.compare_not_equals(&val), CmpBool::CmpNull);
        assert_eq!(invalid_type.compare_less_than(&val), CmpBool::CmpNull);
        assert_eq!(invalid_type.compare_less_than_equals(&val), CmpBool::CmpNull);
        assert_eq!(invalid_type.compare_greater_than(&val), CmpBool::CmpNull);
        assert_eq!(invalid_type.compare_greater_than_equals(&val), CmpBool::CmpNull);
        assert_eq!(invalid_type.compare_equals(&null), CmpBool::CmpNull);
    }

    #[test]
    fn test_invalid_arithmetic() {
        let invalid_type = InvalidType::new();
        let val = Value::new(1i32);

        // All arithmetic operations should return errors
        assert!(invalid_type.add(&val).is_err());
        assert!(invalid_type.subtract(&val).is_err());
        assert!(invalid_type.multiply(&val).is_err());
        assert!(invalid_type.divide(&val).is_err());

        // Modulo returns null
        assert_eq!(invalid_type.modulo(&val), Value::new(Val::Null));
    }

    #[test]
    fn test_invalid_min_max() {
        let invalid_type = InvalidType::new();
        let val = Value::new(1i32);

        // Min and max should return null
        assert_eq!(invalid_type.min(&val), Value::new(Val::Null));
        assert_eq!(invalid_type.max(&val), Value::new(Val::Null));
    }

    #[test]
    fn test_invalid_to_string() {
        let invalid_type = InvalidType::new();
        let val = Value::new(1i32);

        assert_eq!(invalid_type.to_string(&val), "INVALID");
    }
}
