use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};

#[derive(Debug)]
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
            Val::BigInt(r) => CmpBool::from(0.0 == *r as f64),
            Val::Integer(r) => CmpBool::from(0.0 == *r as f64),
            Val::SmallInt(r) => CmpBool::from(0.0 == *r as f64),
            Val::TinyInt(r) => CmpBool::from(0.0 == *r as f64),
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
            Val::BigInt(r) => CmpBool::from(0.0 < *r as f64),
            Val::Integer(r) => CmpBool::from(0.0 < *r as f64),
            Val::SmallInt(r) => CmpBool::from(0.0 < *r as f64),
            Val::TinyInt(r) => CmpBool::from(0.0 < *r as f64),
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
            Val::BigInt(r) => CmpBool::from(0.0 <= *r as f64),
            Val::Integer(r) => CmpBool::from(0.0 <= *r as f64),
            Val::SmallInt(r) => CmpBool::from(0.0 <= *r as f64),
            Val::TinyInt(r) => CmpBool::from(0.0 <= *r as f64),
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
            Val::BigInt(r) => CmpBool::from(0.0 > *r as f64),
            Val::Integer(r) => CmpBool::from(0.0 > *r as f64),
            Val::SmallInt(r) => CmpBool::from(0.0 > *r as f64),
            Val::TinyInt(r) => CmpBool::from(0.0 > *r as f64),
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
            Val::BigInt(r) => CmpBool::from(0.0 >= *r as f64),
            Val::Integer(r) => CmpBool::from(0.0 >= *r as f64),
            Val::SmallInt(r) => CmpBool::from(0.0 >= *r as f64),
            Val::TinyInt(r) => CmpBool::from(0.0 >= *r as f64),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn add(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Decimal(r) => Ok(Value::new(*r)),
            Val::BigInt(r) => Ok(Value::new(*r as f64)),
            Val::Integer(r) => Ok(Value::new(*r as f64)),
            Val::SmallInt(r) => Ok(Value::new(*r as f64)),
            Val::TinyInt(r) => Ok(Value::new(*r as f64)),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot add non-numeric types to Decimal".to_string()),
        }
    }

    fn subtract(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Decimal(r) => Ok(Value::new(-*r)),
            Val::BigInt(r) => Ok(Value::new(-(*r as f64))),
            Val::Integer(r) => Ok(Value::new(-(*r as f64))),
            Val::SmallInt(r) => Ok(Value::new(-(*r as f64))),
            Val::TinyInt(r) => Ok(Value::new(-(*r as f64))),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot subtract non-numeric types from Decimal".to_string()),
        }
    }

    fn multiply(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Decimal(_)
            | Val::BigInt(_)
            | Val::Integer(_)
            | Val::SmallInt(_)
            | Val::TinyInt(_) => Ok(Value::new(0.0f64)),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot multiply Decimal by non-numeric type".to_string()),
        }
    }

    fn divide(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Decimal(r) if *r == 0.0 => Err("Division by zero".to_string()),
            Val::BigInt(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::Integer(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::SmallInt(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::TinyInt(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::Decimal(_)
            | Val::BigInt(_)
            | Val::Integer(_)
            | Val::SmallInt(_)
            | Val::TinyInt(_) => Ok(Value::new(0.0f64)),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot divide Decimal by non-numeric type".to_string()),
        }
    }

    fn modulo(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Decimal(r) if *r == 0.0 => Value::new(Val::Null),
            Val::BigInt(r) if *r == 0 => Value::new(Val::Null),
            Val::Integer(r) if *r == 0 => Value::new(Val::Null),
            Val::SmallInt(r) if *r == 0 => Value::new(Val::Null),
            Val::TinyInt(r) if *r == 0 => Value::new(Val::Null),
            Val::Decimal(_)
            | Val::BigInt(_)
            | Val::Integer(_)
            | Val::SmallInt(_)
            | Val::TinyInt(_) => Value::new(0.0f64),
            _ => Value::new(Val::Null),
        }
    }

    fn min(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Decimal(r) => {
                if r.is_nan() {
                    Value::new(Val::Null)
                } else {
                    Value::new(0.0f64.min(*r))
                }
            }
            Val::BigInt(r) => Value::new(0.0f64.min(*r as f64)),
            Val::Integer(r) => Value::new(0.0f64.min(*r as f64)),
            Val::SmallInt(r) => Value::new(0.0f64.min(*r as f64)),
            Val::TinyInt(r) => Value::new(0.0f64.min(*r as f64)),
            _ => Value::new(Val::Null),
        }
    }

    fn max(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Decimal(r) => {
                if r.is_nan() {
                    Value::new(Val::Null)
                } else {
                    Value::new(0.0f64.max(*r))
                }
            }
            Val::BigInt(r) => Value::new(0.0f64.max(*r as f64)),
            Val::Integer(r) => Value::new(0.0f64.max(*r as f64)),
            Val::SmallInt(r) => Value::new(0.0f64.max(*r as f64)),
            Val::TinyInt(r) => Value::new(0.0f64.max(*r as f64)),
            _ => Value::new(Val::Null),
        }
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_val() {
            Val::Decimal(n) => {
                // Standard SQL decimal formatting
                if n.fract() == 0.0 {
                    // Whole numbers: show as integers without decimal point
                    format!("{}", *n as i64)
                } else {
                    // Numbers with decimals: format based on the actual value
                    // Use up to 2 decimal places, but remove unnecessary trailing zeros
                    let formatted = format!("{:.2}", n);
                    
                    if formatted.ends_with("0") && !formatted.ends_with(".00") {
                        // Remove single trailing zero for values like 1.50 -> 1.5
                        formatted.trim_end_matches('0').to_string()
                    } else {
                        formatted
                    }
                }
            }
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }
}

pub static DECIMAL_TYPE_INSTANCE: DecimalType = DecimalType;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decimal_comparisons() {
        let decimal_type = DecimalType::new();
        let zero = Value::new(0.0f64);
        let one = Value::new(1.0f64);
        let neg_one = Value::new(-1.0f64);
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
        let two = Value::new(2.0f64);
        let zero = Value::new(0.0f64);
        let infinity = Value::new(f64::INFINITY);

        // Test addition
        assert_eq!(decimal_type.add(&two).unwrap(), Value::new(2.0f64));

        // Test division by zero
        assert!(decimal_type.divide(&zero).is_err());

        // Test modulo
        assert_eq!(decimal_type.modulo(&zero), Value::new(Val::Null));

        // Test infinity
        assert!(decimal_type.add(&infinity).is_ok());
    }

    #[test]
    fn test_decimal_min_max() {
        let decimal_type = DecimalType::new();
        let pos = Value::new(1.0f64);
        let neg = Value::new(-1.0f64);
        let nan = Value::new(f64::NAN);

        // Test min/max
        assert_eq!(Type::min(&decimal_type, &pos), Value::new(0.0f64));
        assert_eq!(Type::min(&decimal_type, &neg), Value::new(-1.0f64));
        assert_eq!(Type::min(&decimal_type, &nan), Value::new(Val::Null));

        assert_eq!(Type::max(&decimal_type, &pos), Value::new(1.0f64));
        assert_eq!(Type::max(&decimal_type, &neg), Value::new(0.0f64));
        assert_eq!(Type::max(&decimal_type, &nan), Value::new(Val::Null));
    }

    #[test]
    fn test_decimal_formatting() {
        let decimal_type = DecimalType::new();
        
        // Test whole numbers - should show as integers without decimal point
        assert_eq!(decimal_type.to_string(&Value::new(2.0f64)), "2");
        assert_eq!(decimal_type.to_string(&Value::new(10.0f64)), "10");
        assert_eq!(decimal_type.to_string(&Value::new(20.0f64)), "20");
        assert_eq!(decimal_type.to_string(&Value::new(3.0f64)), "3");
        
        // Test numbers with decimal places
        assert_eq!(decimal_type.to_string(&Value::new(1.5f64)), "1.5");   // Remove trailing zero
        assert_eq!(decimal_type.to_string(&Value::new(20.5f64)), "20.5"); // Remove trailing zero
        assert_eq!(decimal_type.to_string(&Value::new(10.99f64)), "10.99"); // Keep both decimal places
        assert_eq!(decimal_type.to_string(&Value::new(15.75f64)), "15.75"); // Keep both decimal places
        assert_eq!(decimal_type.to_string(&Value::new(0.5f64)), "0.5");   // Remove trailing zero
        
        // Test NULL
        assert_eq!(decimal_type.to_string(&Value::new(Val::Null)), "NULL");
    }
}
