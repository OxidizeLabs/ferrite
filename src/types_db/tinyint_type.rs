use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Debug)]
pub struct TinyIntType;

impl Default for TinyIntType {
    fn default() -> Self {
        Self::new()
    }
}

impl TinyIntType {
    pub fn new() -> Self {
        TinyIntType
    }
}

impl Type for TinyIntType {
    fn get_type_id(&self) -> TypeId {
        TypeId::TinyInt
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::TinyInt(r) => CmpBool::from(0 == *r),
            Val::SmallInt(r) => {
                if let Ok(val) = i8::try_from(*r) {
                    CmpBool::from(0 == val)
                } else {
                    CmpBool::CmpFalse
                }
            },
            Val::Integer(r) => {
                if let Ok(val) = i8::try_from(*r) {
                    CmpBool::from(0 == val)
                } else {
                    CmpBool::CmpFalse
                }
            },
            Val::BigInt(r) => {
                if let Ok(val) = i8::try_from(*r) {
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
            Val::TinyInt(r) => CmpBool::from(0 < *r),
            Val::SmallInt(r) => {
                if let Ok(val) = i8::try_from(*r) {
                    CmpBool::from(0 < val)
                } else {
                    CmpBool::from(*r > 0)
                }
            },
            Val::Integer(r) => {
                if let Ok(val) = i8::try_from(*r) {
                    CmpBool::from(0 < val)
                } else {
                    CmpBool::from(*r > 0)
                }
            },
            Val::BigInt(r) => {
                if let Ok(val) = i8::try_from(*r) {
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
            Val::TinyInt(r) => CmpBool::from(0 <= *r),
            Val::SmallInt(r) => {
                if let Ok(val) = i8::try_from(*r) {
                    CmpBool::from(0 <= val)
                } else {
                    CmpBool::from(*r > 0)
                }
            },
            Val::Integer(r) => {
                if let Ok(val) = i8::try_from(*r) {
                    CmpBool::from(0 <= val)
                } else {
                    CmpBool::from(*r > 0)
                }
            },
            Val::BigInt(r) => {
                if let Ok(val) = i8::try_from(*r) {
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
            Val::TinyInt(r) => CmpBool::from(0 > *r),
            Val::SmallInt(r) => {
                if let Ok(val) = i8::try_from(*r) {
                    CmpBool::from(0 > val)
                } else {
                    CmpBool::from(*r < 0)
                }
            },
            Val::Integer(r) => {
                if let Ok(val) = i8::try_from(*r) {
                    CmpBool::from(0 > val)
                } else {
                    CmpBool::from(*r < 0)
                }
            },
            Val::BigInt(r) => {
                if let Ok(val) = i8::try_from(*r) {
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
            Val::TinyInt(r) => CmpBool::from(0 >= *r),
            Val::SmallInt(r) => {
                if let Ok(val) = i8::try_from(*r) {
                    CmpBool::from(0 >= val)
                } else {
                    CmpBool::from(*r < 0)
                }
            },
            Val::Integer(r) => {
                if let Ok(val) = i8::try_from(*r) {
                    CmpBool::from(0 >= val)
                } else {
                    CmpBool::from(*r < 0)
                }
            },
            Val::BigInt(r) => {
                if let Ok(val) = i8::try_from(*r) {
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
            Val::TinyInt(r) => r
                .checked_add(0)
                .map(Value::new)
                .ok_or_else(|| "TinyInt overflow in addition".to_string()),
            Val::SmallInt(r) => i8::try_from(*r)
                .map(Value::new)
                .map_err(|_| "SmallInt overflow in addition".to_string()),
            Val::Integer(r) => i8::try_from(*r)
                .map(Value::new)
                .map_err(|_| "Integer overflow in addition".to_string()),
            Val::BigInt(r) => i8::try_from(*r)
                .map(Value::new)
                .map_err(|_| "BigInt overflow in addition".to_string()),
            Val::Decimal(r) => Ok(Value::new(*r as i8)),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot add non-numeric types to TinyInt".to_string()),
        }
    }

    fn subtract(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::TinyInt(r) => r
                .checked_neg()
                .map(Value::new)
                .ok_or_else(|| "TinyInt overflow in subtraction".to_string()),
            Val::SmallInt(r) => i8::try_from(*r)
                .map(|v| Value::new(-v))
                .map_err(|_| "SmallInt overflow in subtraction".to_string()),
            Val::Integer(r) => i8::try_from(*r)
                .map(|v| Value::new(-v))
                .map_err(|_| "Integer overflow in subtraction".to_string()),
            Val::BigInt(r) => i8::try_from(*r)
                .map(|v| Value::new(-v))
                .map_err(|_| "BigInt overflow in subtraction".to_string()),
            Val::Decimal(r) => Ok(Value::new(-(*r as i8))),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot subtract non-numeric types from TinyInt".to_string()),
        }
    }

    fn multiply(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::TinyInt(r) => r
                .checked_mul(0)
                .map(Value::new)
                .ok_or_else(|| "TinyInt overflow in multiplication".to_string()),
            Val::SmallInt(_) | Val::Integer(_) | Val::BigInt(_) | Val::Decimal(_) => {
                Ok(Value::new(0i8))
            },
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot multiply TinyInt by non-numeric type".to_string()),
        }
    }

    fn divide(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::TinyInt(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::SmallInt(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::Integer(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::BigInt(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::Decimal(r) if *r == 0.0 => Err("Division by zero".to_string()),
            Val::TinyInt(_)
            | Val::SmallInt(_)
            | Val::Integer(_)
            | Val::BigInt(_)
            | Val::Decimal(_) => Ok(Value::new(0i8)),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot divide TinyInt by non-numeric type".to_string()),
        }
    }

    fn modulo(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::TinyInt(r) if *r == 0 => Value::new(Val::Null),
            Val::SmallInt(r) if *r == 0 => Value::new(Val::Null),
            Val::Integer(r) if *r == 0 => Value::new(Val::Null),
            Val::BigInt(r) if *r == 0 => Value::new(Val::Null),
            Val::TinyInt(_) | Val::SmallInt(_) | Val::Integer(_) | Val::BigInt(_) => {
                Value::new(0i8)
            },
            _ => Value::new(Val::Null),
        }
    }

    fn min(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::TinyInt(r) => Value::new(0i8.min(*r)),
            Val::SmallInt(r) => {
                if let Ok(val) = i8::try_from(*r) {
                    Value::new(0i8.min(val))
                } else {
                    Value::new(if *r < 0 { i8::MIN } else { 0 })
                }
            },
            Val::Integer(r) => {
                if let Ok(val) = i8::try_from(*r) {
                    Value::new(0i8.min(val))
                } else {
                    Value::new(if *r < 0 { i8::MIN } else { 0 })
                }
            },
            Val::BigInt(r) => {
                if let Ok(val) = i8::try_from(*r) {
                    Value::new(0i8.min(val))
                } else {
                    Value::new(if *r < 0 { i8::MIN } else { 0 })
                }
            },
            _ => Value::new(Val::Null),
        }
    }

    fn max(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::TinyInt(r) => Value::new(0i8.max(*r)),
            Val::SmallInt(r) => {
                if let Ok(val) = i8::try_from(*r) {
                    Value::new(0i8.max(val))
                } else {
                    Value::new(if *r > 0 { i8::MAX } else { 0 })
                }
            },
            Val::Integer(r) => {
                if let Ok(val) = i8::try_from(*r) {
                    Value::new(0i8.max(val))
                } else {
                    Value::new(if *r > 0 { i8::MAX } else { 0 })
                }
            },
            Val::BigInt(r) => {
                if let Ok(val) = i8::try_from(*r) {
                    Value::new(0i8.max(val))
                } else {
                    Value::new(if *r > 0 { i8::MAX } else { 0 })
                }
            },
            _ => Value::new(Val::Null),
        }
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_val() {
            Val::TinyInt(n) => n.to_string(),
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }
}

pub static TINYINT_TYPE_INSTANCE: TinyIntType = TinyIntType;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tinyint_comparisons() {
        let tinyint_type = TinyIntType::new();
        let zero = Value::new(0i8);
        let one = Value::new(1i8);
        let neg_one = Value::new(-1i8);
        let big = Value::new(i16::MAX);
        let null = Value::new(Val::Null);

        // Test equals
        assert_eq!(tinyint_type.compare_equals(&zero), CmpBool::CmpTrue);
        assert_eq!(tinyint_type.compare_equals(&one), CmpBool::CmpFalse);
        assert_eq!(tinyint_type.compare_equals(&big), CmpBool::CmpFalse);
        assert_eq!(tinyint_type.compare_equals(&null), CmpBool::CmpNull);

        // Test less than
        assert_eq!(tinyint_type.compare_less_than(&one), CmpBool::CmpTrue);
        assert_eq!(tinyint_type.compare_less_than(&neg_one), CmpBool::CmpFalse);
        assert_eq!(tinyint_type.compare_less_than(&big), CmpBool::CmpTrue);
        assert_eq!(tinyint_type.compare_less_than(&null), CmpBool::CmpNull);
    }

    #[test]
    fn test_tinyint_arithmetic() {
        let tinyint_type = TinyIntType::new();
        let two = Value::new(2i8);
        let zero = Value::new(0i8);
        let max = Value::new(i8::MAX);

        // Test addition
        assert_eq!(tinyint_type.add(&two).unwrap(), Value::new(2i8));

        // Test division by zero
        assert!(tinyint_type.divide(&zero).is_err());

        // Test modulo
        assert_eq!(tinyint_type.modulo(&zero), Value::new(Val::Null));

        // Test overflow
        assert!(tinyint_type.add(&max).is_ok());
    }

    #[test]
    fn test_tinyint_min_max() {
        let tinyint_type = TinyIntType::new();
        let pos = Value::new(1i8);
        let neg = Value::new(-1i8);
        let big = Value::new(i16::MAX);
        let small = Value::new(i16::MIN);

        // Test min/max
        assert_eq!(Type::min(&tinyint_type, &pos), Value::new(0i8));
        assert_eq!(Type::min(&tinyint_type, &neg), Value::new(-1i8));
        assert_eq!(Type::min(&tinyint_type, &big), Value::new(0i8));
        assert_eq!(Type::min(&tinyint_type, &small), Value::new(i8::MIN));

        assert_eq!(Type::max(&tinyint_type, &pos), Value::new(1i8));
        assert_eq!(Type::max(&tinyint_type, &neg), Value::new(0i8));
        assert_eq!(Type::max(&tinyint_type, &big), Value::new(i8::MAX));
        assert_eq!(Type::max(&tinyint_type, &small), Value::new(0i8));
    }
}
