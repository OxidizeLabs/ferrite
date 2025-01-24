use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};

// Implementation for IntegerType
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Debug)]
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

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Integer(r) => CmpBool::from(0 == *r),
            Val::TinyInt(r) => CmpBool::from(0 == *r as i32),
            Val::SmallInt(r) => CmpBool::from(0 == *r as i32),
            Val::BigInt(r) => {
                if let Ok(val) = i32::try_from(*r) {
                    CmpBool::from(0 == val)
                } else {
                    CmpBool::CmpFalse
                }
            }
            Val::Decimal(r) => CmpBool::from((0 as f64) == *r),
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
            Val::Integer(r) => CmpBool::from(0 < *r),
            Val::TinyInt(r) => CmpBool::from(0 < *r as i32),
            Val::SmallInt(r) => CmpBool::from(0 < *r as i32),
            Val::BigInt(r) => {
                if let Ok(val) = i32::try_from(*r) {
                    CmpBool::from(0 < val)
                } else {
                    CmpBool::from(*r > 0)
                }
            }
            Val::Decimal(r) => CmpBool::from((0 as f64) < *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Integer(r) => CmpBool::from(0 <= *r),
            Val::TinyInt(r) => CmpBool::from(0 <= *r as i32),
            Val::SmallInt(r) => CmpBool::from(0 <= *r as i32),
            Val::BigInt(r) => {
                if let Ok(val) = i32::try_from(*r) {
                    CmpBool::from(0 <= val)
                } else {
                    CmpBool::from(*r > 0)
                }
            }
            Val::Decimal(r) => CmpBool::from((0 as f64) <= *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Integer(r) => CmpBool::from(0 > *r),
            Val::TinyInt(r) => CmpBool::from(0 > *r as i32),
            Val::SmallInt(r) => CmpBool::from(0 > *r as i32),
            Val::BigInt(r) => {
                if let Ok(val) = i32::try_from(*r) {
                    CmpBool::from(0 > val)
                } else {
                    CmpBool::from(*r < 0)
                }
            }
            Val::Decimal(r) => CmpBool::from((0 as f64) > *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Integer(r) => CmpBool::from(0 >= *r),
            Val::TinyInt(r) => CmpBool::from(0 >= *r as i32),
            Val::SmallInt(r) => CmpBool::from(0 >= *r as i32),
            Val::BigInt(r) => {
                if let Ok(val) = i32::try_from(*r) {
                    CmpBool::from(0 >= val)
                } else {
                    CmpBool::from(*r < 0)
                }
            }
            Val::Decimal(r) => CmpBool::from((0 as f64) >= *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn add(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Integer(r) => {
                r.checked_add(0)
                    .map(Value::new)
                    .ok_or_else(|| "Integer overflow in addition".to_string())
            }
            Val::TinyInt(r) => Ok(Value::new(*r as i32)),
            Val::SmallInt(r) => Ok(Value::new(*r as i32)),
            Val::BigInt(r) => {
                i32::try_from(*r)
                    .map(Value::new)
                    .map_err(|_| "Integer overflow in addition".to_string())
            }
            Val::Decimal(r) => Ok(Value::new(*r as i32)),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot add non-numeric types to Integer".to_string()),
        }
    }

    fn subtract(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Integer(r) => {
                r.checked_neg()
                    .map(Value::new)
                    .ok_or_else(|| "Integer overflow in subtraction".to_string())
            }
            Val::TinyInt(r) => Ok(Value::new(-(*r as i32))),
            Val::SmallInt(r) => Ok(Value::new(-(*r as i32))),
            Val::BigInt(r) => {
                if let Ok(val) = i32::try_from(*r) {
                    Ok(Value::new(-val))
                } else {
                    Err("Integer overflow in subtraction".to_string())
                }
            }
            Val::Decimal(r) => Ok(Value::new(-(*r as i32))),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot subtract non-numeric types from Integer".to_string()),
        }
    }

    fn multiply(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Integer(r) => {
                r.checked_mul(0)
                    .map(Value::new)
                    .ok_or_else(|| "Integer overflow in multiplication".to_string())
            }
            Val::TinyInt(_) | Val::SmallInt(_) | Val::BigInt(_) | 
            Val::Decimal(_) => Ok(Value::new(0)),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot multiply Integer by non-numeric type".to_string()),
        }
    }

    fn divide(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Integer(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::TinyInt(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::SmallInt(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::BigInt(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::Decimal(r) if *r == 0.0 => Err("Division by zero".to_string()),
            Val::Integer(_) | Val::TinyInt(_) | Val::SmallInt(_) |
            Val::BigInt(_) | Val::Decimal(_) => Ok(Value::new(0)),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot divide Integer by non-numeric type".to_string()),
        }
    }

    fn modulo(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Integer(r) if *r == 0 => Value::new(Val::Null),
            Val::TinyInt(r) if *r == 0 => Value::new(Val::Null),
            Val::SmallInt(r) if *r == 0 => Value::new(Val::Null),
            Val::BigInt(r) if *r == 0 => Value::new(Val::Null),
            Val::Integer(_) | Val::TinyInt(_) | Val::SmallInt(_) |
            Val::BigInt(_) => Value::new(0),
            _ => Value::new(Val::Null),
        }
    }

    fn min(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Integer(r) => Value::new(0.min(*r)),
            Val::TinyInt(r) => Value::new(0.min(*r as i32)),
            Val::SmallInt(r) => Value::new(0.min(*r as i32)),
            Val::BigInt(r) => {
                if let Ok(val) = i32::try_from(*r) {
                    Value::new(0.min(val))
                } else {
                    Value::new(if *r < 0 { i32::MIN } else { 0 })
                }
            }
            _ => Value::new(Val::Null),
        }
    }

    fn max(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Integer(r) => Value::new(0.max(*r)),
            Val::TinyInt(r) => Value::new(0.max(*r as i32)),
            Val::SmallInt(r) => Value::new(0.max(*r as i32)),
            Val::BigInt(r) => {
                if let Ok(val) = i32::try_from(*r) {
                    Value::new(0.max(val))
                } else {
                    Value::new(if *r > 0 { i32::MAX } else { 0 })
                }
            }
            _ => Value::new(Val::Null),
        }
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_val() {
            Val::Integer(n) => n.to_string(),
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_comparisons() {
        let int_type = IntegerType::new();
        let zero = Value::new(0i32);
        let one = Value::new(1i32);
        let neg_one = Value::new(-1i32);
        let big = Value::new(i64::MAX);
        let null = Value::new(Val::Null);

        // Test equals
        assert_eq!(int_type.compare_equals(&zero), CmpBool::CmpTrue);
        assert_eq!(int_type.compare_equals(&one), CmpBool::CmpFalse);
        assert_eq!(int_type.compare_equals(&big), CmpBool::CmpFalse);
        assert_eq!(int_type.compare_equals(&null), CmpBool::CmpNull);

        // Test less than
        assert_eq!(int_type.compare_less_than(&one), CmpBool::CmpTrue);
        assert_eq!(int_type.compare_less_than(&neg_one), CmpBool::CmpFalse);
        assert_eq!(int_type.compare_less_than(&big), CmpBool::CmpTrue);
        assert_eq!(int_type.compare_less_than(&null), CmpBool::CmpNull);
    }

    #[test]
    fn test_integer_arithmetic() {
        let int_type = IntegerType::new();
        let two = Value::new(2i32);
        let zero = Value::new(0i32);
        let max = Value::new(i32::MAX);

        // Test addition
        assert_eq!(int_type.add(&two).unwrap(), Value::new(2i32));
        
        // Test division by zero
        assert!(int_type.divide(&zero).is_err());
        
        // Test modulo
        assert_eq!(int_type.modulo(&zero), Value::new(Val::Null));

        // Test overflow
        assert!(int_type.add(&max).is_ok());
    }

    #[test]
    fn test_integer_min_max() {
        let int_type = IntegerType::new();
        let pos = Value::new(1i32);
        let neg = Value::new(-1i32);
        let big = Value::new(i64::MAX);
        let small = Value::new(i64::MIN);

        // Test min/max
        assert_eq!(Type::min(&int_type, &pos), Value::new(0i32));
        assert_eq!(Type::min(&int_type, &neg), Value::new(-1i32));
        assert_eq!(Type::min(&int_type, &big), Value::new(0i32));
        assert_eq!(Type::min(&int_type, &small), Value::new(i32::MIN));

        assert_eq!(Type::max(&int_type, &pos), Value::new(1i32));
        assert_eq!(Type::max(&int_type, &neg), Value::new(0i32));
        assert_eq!(Type::max(&int_type, &big), Value::new(i32::MAX));
        assert_eq!(Type::max(&int_type, &small), Value::new(0i32));
    }
}
