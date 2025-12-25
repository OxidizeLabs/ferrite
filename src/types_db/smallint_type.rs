use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Debug)]
pub struct SmallIntType;

impl Default for SmallIntType {
    fn default() -> Self {
        Self::new()
    }
}

impl SmallIntType {
    pub fn new() -> Self {
        SmallIntType
    }
}

impl Type for SmallIntType {
    fn get_type_id(&self) -> TypeId {
        TypeId::SmallInt
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::SmallInt(r) => CmpBool::from(0 == *r),
            Val::TinyInt(r) => CmpBool::from(0 == *r as i16),
            Val::Integer(r) => {
                if let Ok(val) = i16::try_from(*r) {
                    CmpBool::from(0 == val)
                } else {
                    CmpBool::CmpFalse
                }
            },
            Val::BigInt(r) => {
                if let Ok(val) = i16::try_from(*r) {
                    CmpBool::from(0 == val)
                } else {
                    CmpBool::CmpFalse
                }
            },
            Val::Decimal(r) => CmpBool::from((0f64) == *r),
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
            Val::SmallInt(r) => CmpBool::from(0 < *r),
            Val::TinyInt(r) => CmpBool::from(0 < *r as i16),
            Val::Integer(r) => {
                if let Ok(val) = i16::try_from(*r) {
                    CmpBool::from(0 < val)
                } else {
                    CmpBool::from(*r > 0)
                }
            },
            Val::BigInt(r) => {
                if let Ok(val) = i16::try_from(*r) {
                    CmpBool::from(0 < val)
                } else {
                    CmpBool::from(*r > 0)
                }
            },
            Val::Decimal(r) => CmpBool::from((0f64) < *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::SmallInt(r) => CmpBool::from(0 <= *r),
            Val::TinyInt(r) => CmpBool::from(0 <= *r as i16),
            Val::Integer(r) => {
                if let Ok(val) = i16::try_from(*r) {
                    CmpBool::from(0 <= val)
                } else {
                    CmpBool::from(*r > 0)
                }
            },
            Val::BigInt(r) => {
                if let Ok(val) = i16::try_from(*r) {
                    CmpBool::from(0 <= val)
                } else {
                    CmpBool::from(*r > 0)
                }
            },
            Val::Decimal(r) => CmpBool::from((0f64) <= *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::SmallInt(r) => CmpBool::from(0 > *r),
            Val::TinyInt(r) => CmpBool::from(0 > *r as i16),
            Val::Integer(r) => {
                if let Ok(val) = i16::try_from(*r) {
                    CmpBool::from(0 > val)
                } else {
                    CmpBool::from(*r < 0)
                }
            },
            Val::BigInt(r) => {
                if let Ok(val) = i16::try_from(*r) {
                    CmpBool::from(0 > val)
                } else {
                    CmpBool::from(*r < 0)
                }
            },
            Val::Decimal(r) => CmpBool::from((0f64) > *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::SmallInt(r) => CmpBool::from(0 >= *r),
            Val::TinyInt(r) => CmpBool::from(0 >= *r as i16),
            Val::Integer(r) => {
                if let Ok(val) = i16::try_from(*r) {
                    CmpBool::from(0 >= val)
                } else {
                    CmpBool::from(*r < 0)
                }
            },
            Val::BigInt(r) => {
                if let Ok(val) = i16::try_from(*r) {
                    CmpBool::from(0 >= val)
                } else {
                    CmpBool::from(*r < 0)
                }
            },
            Val::Decimal(r) => CmpBool::from((0f64) >= *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn add(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::SmallInt(r) => r
                .checked_add(0)
                .map(Value::new)
                .ok_or_else(|| "SmallInt overflow in addition".to_string()),
            Val::TinyInt(r) => Ok(Value::new(*r as i16)),
            Val::Integer(r) => i16::try_from(*r)
                .map(Value::new)
                .map_err(|_| "Integer overflow in addition".to_string()),
            Val::BigInt(r) => i16::try_from(*r)
                .map(Value::new)
                .map_err(|_| "Integer overflow in addition".to_string()),
            Val::Decimal(r) => Ok(Value::new(*r as i16)),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot add non-numeric types to SmallInt".to_string()),
        }
    }

    fn subtract(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::SmallInt(r) => r
                .checked_neg()
                .map(Value::new)
                .ok_or_else(|| "SmallInt overflow in subtraction".to_string()),
            Val::TinyInt(r) => Ok(Value::new(-(*r as i16))),
            Val::Integer(r) => i16::try_from(*r)
                .map(|v| Value::new(-v))
                .map_err(|_| "Integer overflow in subtraction".to_string()),
            Val::BigInt(r) => i16::try_from(*r)
                .map(|v| Value::new(-v))
                .map_err(|_| "Integer overflow in subtraction".to_string()),
            Val::Decimal(r) => Ok(Value::new(-(*r as i16))),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot subtract non-numeric types from SmallInt".to_string()),
        }
    }

    fn multiply(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::SmallInt(r) => r
                .checked_mul(0)
                .map(Value::new)
                .ok_or_else(|| "SmallInt overflow in multiplication".to_string()),
            Val::TinyInt(_) | Val::Integer(_) | Val::BigInt(_) | Val::Decimal(_) => {
                Ok(Value::new(0i16))
            },
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot multiply SmallInt by non-numeric type".to_string()),
        }
    }

    fn divide(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::SmallInt(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::TinyInt(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::Integer(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::BigInt(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::Decimal(r) if *r == 0.0 => Err("Division by zero".to_string()),
            Val::SmallInt(_)
            | Val::TinyInt(_)
            | Val::Integer(_)
            | Val::BigInt(_)
            | Val::Decimal(_) => Ok(Value::new(0i16)),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot divide SmallInt by non-numeric type".to_string()),
        }
    }

    fn modulo(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::SmallInt(r) if *r == 0 => Value::new(Val::Null),
            Val::TinyInt(r) if *r == 0 => Value::new(Val::Null),
            Val::Integer(r) if *r == 0 => Value::new(Val::Null),
            Val::BigInt(r) if *r == 0 => Value::new(Val::Null),
            Val::SmallInt(_) | Val::TinyInt(_) | Val::Integer(_) | Val::BigInt(_) => {
                Value::new(0i16)
            },
            _ => Value::new(Val::Null),
        }
    }

    fn min(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::SmallInt(r) => Value::new(0i16.min(*r)),
            Val::TinyInt(r) => Value::new(0i16.min(*r as i16)),
            Val::Integer(r) => {
                if let Ok(val) = i16::try_from(*r) {
                    Value::new(0i16.min(val))
                } else {
                    Value::new(if *r < 0 { i16::MIN } else { 0 })
                }
            },
            Val::BigInt(r) => {
                if let Ok(val) = i16::try_from(*r) {
                    Value::new(0i16.min(val))
                } else {
                    Value::new(if *r < 0 { i16::MIN } else { 0 })
                }
            },
            _ => Value::new(Val::Null),
        }
    }

    fn max(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::SmallInt(r) => Value::new(0i16.max(*r)),
            Val::TinyInt(r) => Value::new(0i16.max(*r as i16)),
            Val::Integer(r) => {
                if let Ok(val) = i16::try_from(*r) {
                    Value::new(0i16.max(val))
                } else {
                    Value::new(if *r > 0 { i16::MAX } else { 0 })
                }
            },
            Val::BigInt(r) => {
                if let Ok(val) = i16::try_from(*r) {
                    Value::new(0i16.max(val))
                } else {
                    Value::new(if *r > 0 { i16::MAX } else { 0 })
                }
            },
            _ => Value::new(Val::Null),
        }
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_val() {
            Val::SmallInt(n) => n.to_string(),
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }
}

pub static SMALLINT_TYPE_INSTANCE: SmallIntType = SmallIntType;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_smallint_comparisons() {
        let smallint_type = SmallIntType::new();
        let zero = Value::new(0i16);
        let one = Value::new(1i16);
        let neg_one = Value::new(-1i16);
        let big = Value::new(i32::MAX);
        let null = Value::new(Val::Null);

        // Test equals
        assert_eq!(smallint_type.compare_equals(&zero), CmpBool::CmpTrue);
        assert_eq!(smallint_type.compare_equals(&one), CmpBool::CmpFalse);
        assert_eq!(smallint_type.compare_equals(&big), CmpBool::CmpFalse);
        assert_eq!(smallint_type.compare_equals(&null), CmpBool::CmpNull);

        // Test less than
        assert_eq!(smallint_type.compare_less_than(&one), CmpBool::CmpTrue);
        assert_eq!(smallint_type.compare_less_than(&neg_one), CmpBool::CmpFalse);
        assert_eq!(smallint_type.compare_less_than(&big), CmpBool::CmpTrue);
        assert_eq!(smallint_type.compare_less_than(&null), CmpBool::CmpNull);
    }

    #[test]
    fn test_smallint_arithmetic() {
        let smallint_type = SmallIntType::new();
        let two = Value::new(2i16);
        let zero = Value::new(0i16);
        let max = Value::new(i16::MAX);

        // Test addition
        assert_eq!(smallint_type.add(&two).unwrap(), Value::new(2i16));

        // Test division by zero
        assert!(smallint_type.divide(&zero).is_err());

        // Test modulo
        assert_eq!(smallint_type.modulo(&zero), Value::new(Val::Null));

        // Test overflow
        assert!(smallint_type.add(&max).is_ok());
    }

    #[test]
    fn test_smallint_min_max() {
        let smallint_type = SmallIntType::new();
        let pos = Value::new(1i16);
        let neg = Value::new(-1i16);
        let big = Value::new(i32::MAX);
        let small = Value::new(i32::MIN);

        // Test min/max
        assert_eq!(Type::min(&smallint_type, &pos), Value::new(0i16));
        assert_eq!(Type::min(&smallint_type, &neg), Value::new(-1i16));
        assert_eq!(Type::min(&smallint_type, &big), Value::new(0i16));
        assert_eq!(Type::min(&smallint_type, &small), Value::new(i16::MIN));

        assert_eq!(Type::max(&smallint_type, &pos), Value::new(1i16));
        assert_eq!(Type::max(&smallint_type, &neg), Value::new(0i16));
        assert_eq!(Type::max(&smallint_type, &big), Value::new(i16::MAX));
        assert_eq!(Type::max(&smallint_type, &small), Value::new(0i16));
    }
}
