use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};

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

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Decimal(r) => {
                if r.is_nan() {
                    CmpBool::CmpFalse
                } else {
                    CmpBool::from(0.0 == *r)
                }
            }
            Val::Integer(r) => CmpBool::from(0.0 == *r as f64),
            Val::BigInt(r) => CmpBool::from(0.0 == *r as f64),
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
            Val::Decimal(r) => {
                if r.is_nan() {
                    CmpBool::CmpFalse
                } else {
                    CmpBool::from(0.0 < *r)
                }
            }
            Val::Integer(r) => CmpBool::from(0.0 < *r as f64),
            Val::BigInt(r) => CmpBool::from(0.0 < *r as f64),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Decimal(r) => {
                if r.is_nan() {
                    CmpBool::CmpFalse
                } else {
                    CmpBool::from(0.0 <= *r)
                }
            }
            Val::Integer(r) => CmpBool::from(0.0 <= *r as f64),
            Val::BigInt(r) => CmpBool::from(0.0 <= *r as f64),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Decimal(r) => {
                if r.is_nan() {
                    CmpBool::CmpFalse
                } else {
                    CmpBool::from(0.0 > *r)
                }
            }
            Val::Integer(r) => CmpBool::from(0.0 > *r as f64),
            Val::BigInt(r) => CmpBool::from(0.0 > *r as f64),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Decimal(r) => {
                if r.is_nan() {
                    CmpBool::CmpFalse
                } else {
                    CmpBool::from(0.0 >= *r)
                }
            }
            Val::Integer(r) => CmpBool::from(0.0 >= *r as f64),
            Val::BigInt(r) => CmpBool::from(0.0 >= *r as f64),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn add(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Decimal(r) => Ok(Value::new(*r + 0.0)),
            Val::Integer(r) => Ok(Value::new(*r as f64 + 0.0)),
            Val::BigInt(r) => Ok(Value::new(*r as f64 + 0.0)),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot add non-numeric types to Decimal".to_string()),
        }
    }

    fn subtract(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Decimal(r) => Ok(Value::new(0.0 - *r)),
            Val::Integer(r) => Ok(Value::new(0.0 - *r as f64)),
            Val::BigInt(r) => Ok(Value::new(0.0 - *r as f64)),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot subtract non-numeric types from Decimal".to_string()),
        }
    }

    fn multiply(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Decimal(r) => Ok(Value::new(0.0 * *r)),
            Val::Integer(r) => Ok(Value::new(0.0 * *r as f64)),
            Val::BigInt(r) => Ok(Value::new(0.0 * *r as f64)),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot multiply Decimal by non-numeric type".to_string()),
        }
    }

    fn divide(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Decimal(r) => {
                if *r == 0.0 {
                    Err("Division by zero".to_string())
                } else {
                    Ok(Value::new(0.0 / *r))
                }
            }
            Val::Integer(r) => {
                if *r == 0 {
                    Err("Division by zero".to_string())
                } else {
                    Ok(Value::new(0.0 / *r as f64))
                }
            }
            Val::BigInt(r) => {
                if *r == 0 {
                    Err("Division by zero".to_string())
                } else {
                    Ok(Value::new(0.0 / *r as f64))
                }
            }
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot divide Decimal by non-numeric type".to_string()),
        }
    }

    fn modulo(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Decimal(r) => {
                if *r == 0.0 {
                    Value::new(Val::Null)
                } else {
                    Value::new(0.0 % *r)
                }
            }
            Val::Integer(r) => {
                if *r == 0 {
                    Value::new(Val::Null)
                } else {
                    Value::new(0.0 % *r as f64)
                }
            }
            Val::BigInt(r) => {
                if *r == 0 {
                    Value::new(Val::Null)
                } else {
                    Value::new(0.0 % *r as f64)
                }
            }
            _ => Value::new(Val::Null),
        }
    }

    fn min(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Decimal(r) => {
                if r.is_nan() {
                    Value::new(Val::Null)
                } else {
                    Value::new(0.0_f64.min(*r))
                }
            }
            Val::Integer(r) => Value::new(0.0_f64.min(*r as f64)),
            Val::BigInt(r) => Value::new(0.0_f64.min(*r as f64)),
            _ => Value::new(Val::Null),
        }
    }

    fn max(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Decimal(r) => {
                if r.is_nan() {
                    Value::new(Val::Null)
                } else {
                    Value::new(0.0_f64.max(*r))
                }
            }
            Val::Integer(r) => Value::new(0.0_f64.max(*r as f64)),
            Val::BigInt(r) => Value::new(0.0_f64.max(*r as f64)),
            _ => Value::new(Val::Null),
        }
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_val() {
            Val::Decimal(n) => n.to_string(),
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decimal_comparisons() {
        let decimal_type = DecimalType::new();
        let zero = Value::new(0.0);
        let one = Value::new(1.0);
        let neg_one = Value::new(-1.0);
        let nan = Value::new(f64::NAN);
        let null = Value::new(Val::Null);

        // Test equals
        assert_eq!(decimal_type.compare_equals(&zero), CmpBool::CmpTrue);
        assert_eq!(decimal_type.compare_equals(&one), CmpBool::CmpFalse);
        assert_eq!(decimal_type.compare_equals(&nan), CmpBool::CmpFalse);
        assert_eq!(decimal_type.compare_equals(&null), CmpBool::CmpNull);

        // Test less than
        assert_eq!(decimal_type.compare_less_than(&one), CmpBool::CmpTrue);
        assert_eq!(decimal_type.compare_less_than(&neg_one), CmpBool::CmpFalse);
        assert_eq!(decimal_type.compare_less_than(&nan), CmpBool::CmpFalse);
        assert_eq!(decimal_type.compare_less_than(&null), CmpBool::CmpNull);
    }

    #[test]
    fn test_decimal_arithmetic() {
        let decimal_type = DecimalType::new();
        let two = Value::new(2.0);
        let zero = Value::new(0.0);

        // Test addition
        assert_eq!(decimal_type.add(&two).unwrap(), Value::new(2.0));
        
        // Test division by zero
        assert!(decimal_type.divide(&zero).is_err());
        
        // Test modulo
        assert_eq!(decimal_type.modulo(&zero), Value::new(Val::Null));
    }

    #[test]
    fn test_decimal_min_max() {
        let decimal_type = DecimalType::new();
        let pos = Value::new(1.0);
        let neg = Value::new(-1.0);
        let nan = Value::new(f64::NAN);

        // Test min/max
        assert_eq!(decimal_type.min(&pos), Value::new(0.0));
        assert_eq!(decimal_type.min(&neg), Value::new(-1.0));
        assert_eq!(decimal_type.min(&nan), Value::new(Val::Null));

        assert_eq!(decimal_type.max(&pos), Value::new(1.0));
        assert_eq!(decimal_type.max(&neg), Value::new(0.0));
        assert_eq!(decimal_type.max(&nan), Value::new(Val::Null));
    }
}
