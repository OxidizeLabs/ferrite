use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};

// Implementation for BooleanType
pub struct BooleanType;

impl BooleanType {
    pub fn new() -> Self {
        BooleanType
    }
}

impl Type for BooleanType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Boolean
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Boolean(r) => CmpBool::from(true == *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_not_equals(&self, other: &Value) -> CmpBool {
        match self.compare_equals(other) {
            CmpBool::CmpTrue => CmpBool::CmpFalse,
            CmpBool::CmpFalse => CmpBool::CmpTrue,
            CmpBool::CmpNull => CmpBool::CmpNull,
        }
    }

    fn compare_less_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Boolean(r) => CmpBool::from(!true && *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Boolean(r) => CmpBool::from(!true || *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Boolean(r) => CmpBool::from(true && !*r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Boolean(r) => CmpBool::from(true || !*r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_val() {
            Val::Boolean(b) => b.to_string(),
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }

    // Boolean type doesn't support arithmetic operations
    fn add(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot perform addition on boolean values".to_string())
    }

    fn subtract(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot perform subtraction on boolean values".to_string())
    }

    fn multiply(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot perform multiplication on boolean values".to_string())
    }

    fn divide(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot perform division on boolean values".to_string())
    }

    fn modulo(&self, _other: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn min(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Boolean(r) => Value::new(*r && true),
            _ => Value::new(Val::Null),
        }
    }

    fn max(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Boolean(r) => Value::new(*r || true),
            _ => Value::new(Val::Null),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_boolean_comparisons() {
        let bool_type = BooleanType::new();
        let true_val = Value::new(true);
        let false_val = Value::new(false);
        let null_val = Value::new(Val::Null);

        // Test equals
        assert_eq!(bool_type.compare_equals(&true_val), CmpBool::CmpTrue);
        assert_eq!(bool_type.compare_equals(&false_val), CmpBool::CmpFalse);
        assert_eq!(bool_type.compare_equals(&null_val), CmpBool::CmpNull);

        // Test less than
        assert_eq!(bool_type.compare_less_than(&true_val), CmpBool::CmpFalse);
        assert_eq!(bool_type.compare_less_than(&false_val), CmpBool::CmpFalse);
        assert_eq!(bool_type.compare_less_than(&null_val), CmpBool::CmpNull);

        // Test greater than
        assert_eq!(bool_type.compare_greater_than(&true_val), CmpBool::CmpFalse);
        assert_eq!(bool_type.compare_greater_than(&false_val), CmpBool::CmpTrue);
        assert_eq!(bool_type.compare_greater_than(&null_val), CmpBool::CmpNull);
    }

    #[test]
    fn test_boolean_arithmetic() {
        let bool_type = BooleanType::new();
        let true_val = Value::new(true);

        // Test arithmetic operations fail
        assert!(bool_type.add(&true_val).is_err());
        assert!(bool_type.subtract(&true_val).is_err());
        assert!(bool_type.multiply(&true_val).is_err());
        assert!(bool_type.divide(&true_val).is_err());
        assert_eq!(bool_type.modulo(&true_val), Value::new(Val::Null));
    }

    #[test]
    fn test_boolean_min_max() {
        let bool_type = BooleanType::new();
        let true_val = Value::new(true);
        let false_val = Value::new(false);
        let null_val = Value::new(Val::Null);

        // Test min
        assert_eq!(bool_type.min(&true_val), Value::new(true));
        assert_eq!(bool_type.min(&false_val), Value::new(false));
        assert_eq!(bool_type.min(&null_val), Value::new(Val::Null));

        // Test max
        assert_eq!(bool_type.max(&true_val), Value::new(true));
        assert_eq!(bool_type.max(&false_val), Value::new(true));
        assert_eq!(bool_type.max(&null_val), Value::new(Val::Null));
    }
}
