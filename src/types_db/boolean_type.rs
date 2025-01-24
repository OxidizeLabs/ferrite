use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};

#[derive(Debug)]
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
            Val::Boolean(r) => CmpBool::from(!true && *r), // false < true
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Boolean(r) => CmpBool::from(!true || *r), // false <= true or true <= true
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Boolean(r) => CmpBool::from(true && !*r), // true > false
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Boolean(r) => CmpBool::from(true || !*r), // true >= false or true >= true
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

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
            Val::Boolean(r) => Value::new(*r && true), // Logical AND for min
            _ => Value::new(Val::Null),
        }
    }

    fn max(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Boolean(r) => Value::new(*r || true), // Logical OR for max
            _ => Value::new(Val::Null),
        }
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_val() {
            Val::Boolean(b) => b.to_string(),
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
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

        // Test not equals
        assert_eq!(bool_type.compare_not_equals(&true_val), CmpBool::CmpFalse);
        assert_eq!(bool_type.compare_not_equals(&false_val), CmpBool::CmpTrue);
        assert_eq!(bool_type.compare_not_equals(&null_val), CmpBool::CmpNull);

        // Test less than (false < true)
        assert_eq!(bool_type.compare_less_than(&true_val), CmpBool::CmpFalse);
        assert_eq!(bool_type.compare_less_than(&false_val), CmpBool::CmpFalse);
        assert_eq!(bool_type.compare_less_than(&null_val), CmpBool::CmpNull);

        // Test greater than (true > false)
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

        // Test min (logical AND)
        assert_eq!(Type::min(&bool_type, &true_val), Value::new(true));
        assert_eq!(Type::min(&bool_type, &false_val), Value::new(false));
        assert_eq!(Type::min(&bool_type, &null_val), Value::new(Val::Null));

        // Test max (logical OR)
        assert_eq!(Type::max(&bool_type, &true_val), Value::new(true));
        assert_eq!(Type::max(&bool_type, &false_val), Value::new(true));
        assert_eq!(Type::max(&bool_type, &null_val), Value::new(Val::Null));
    }

    #[test]
    fn test_boolean_to_string() {
        let bool_type = BooleanType::new();
        let true_val = Value::new(true);
        let false_val = Value::new(false);
        let null_val = Value::new(Val::Null);

        assert_eq!(bool_type.to_string(&true_val), "true");
        assert_eq!(bool_type.to_string(&false_val), "false");
        assert_eq!(bool_type.to_string(&null_val), "NULL");
    }
}
