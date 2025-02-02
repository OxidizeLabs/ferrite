use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};

pub struct BigIntType;

impl BigIntType {
    pub fn new() -> Self {
        BigIntType
    }
}

impl Type for BigIntType {
    fn get_type_id(&self) -> TypeId {
        TypeId::BigInt
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::BigInt(r) => CmpBool::from(0 == *r),
            Val::Integer(r) => CmpBool::from(0 == *r as i64),
            Val::SmallInt(r) => CmpBool::from(0 == *r as i64),
            Val::TinyInt(r) => CmpBool::from(0 == *r as i64),
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
            Val::BigInt(r) => CmpBool::from(0 < *r),
            Val::Integer(r) => CmpBool::from(0 < *r as i64),
            Val::SmallInt(r) => CmpBool::from(0 < *r as i64),
            Val::TinyInt(r) => CmpBool::from(0 < *r as i64),
            Val::Decimal(r) => CmpBool::from((0 as f64) < *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::BigInt(r) => CmpBool::from(0 <= *r),
            Val::Integer(r) => CmpBool::from(0 <= *r as i64),
            Val::SmallInt(r) => CmpBool::from(0 <= *r as i64),
            Val::TinyInt(r) => CmpBool::from(0 <= *r as i64),
            Val::Decimal(r) => CmpBool::from((0 as f64) <= *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::BigInt(r) => CmpBool::from(0 > *r),
            Val::Integer(r) => CmpBool::from(0 > *r as i64),
            Val::SmallInt(r) => CmpBool::from(0 > *r as i64),
            Val::TinyInt(r) => CmpBool::from(0 > *r as i64),
            Val::Decimal(r) => CmpBool::from((0 as f64) > *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::BigInt(r) => CmpBool::from(0 >= *r),
            Val::Integer(r) => CmpBool::from(0 >= *r as i64),
            Val::SmallInt(r) => CmpBool::from(0 >= *r as i64),
            Val::TinyInt(r) => CmpBool::from(0 >= *r as i64),
            Val::Decimal(r) => CmpBool::from((0 as f64) >= *r),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn add(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::BigInt(r) => Ok(Value::new(*r)),
            Val::Integer(r) => Ok(Value::new(*r as i64)),
            Val::SmallInt(r) => Ok(Value::new(*r as i64)),
            Val::TinyInt(r) => Ok(Value::new(*r as i64)),
            Val::Decimal(r) => Ok(Value::new(*r as i64)),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot add non-numeric types to BigInt".to_string()),
        }
    }

    fn subtract(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::BigInt(r) => {
                r.checked_neg()
                    .map(Value::new)
                    .ok_or_else(|| "BigInt overflow in subtraction".to_string())
            }
            Val::Integer(r) => Ok(Value::new(-(*r as i64))),
            Val::SmallInt(r) => Ok(Value::new(-(*r as i64))),
            Val::TinyInt(r) => Ok(Value::new(-(*r as i64))),
            Val::Decimal(r) => Ok(Value::new(-(*r as i64))),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot subtract non-numeric types from BigInt".to_string()),
        }
    }

    fn multiply(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::BigInt(_) | Val::Integer(_) | Val::SmallInt(_) |
            Val::TinyInt(_) | Val::Decimal(_) => Ok(Value::new(0i64)),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot multiply BigInt by non-numeric type".to_string()),
        }
    }

    fn divide(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::BigInt(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::Integer(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::SmallInt(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::TinyInt(r) if *r == 0 => Err("Division by zero".to_string()),
            Val::Decimal(r) if *r == 0.0 => Err("Division by zero".to_string()),
            Val::BigInt(_) | Val::Integer(_) | Val::SmallInt(_) |
            Val::TinyInt(_) | Val::Decimal(_) => Ok(Value::new(0i64)),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot divide BigInt by non-numeric type".to_string()),
        }
    }

    fn modulo(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::BigInt(r) if *r == 0 => Value::new(Val::Null),
            Val::Integer(r) if *r == 0 => Value::new(Val::Null),
            Val::SmallInt(r) if *r == 0 => Value::new(Val::Null),
            Val::TinyInt(r) if *r == 0 => Value::new(Val::Null),
            Val::BigInt(_) | Val::Integer(_) | Val::SmallInt(_) |
            Val::TinyInt(_) => Value::new(0i64),
            _ => Value::new(Val::Null),
        }
    }

    fn min(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::BigInt(r) => Value::new(0i64.min(*r)),
            Val::Integer(r) => Value::new(0i64.min(*r as i64)),
            Val::SmallInt(r) => Value::new(0i64.min(*r as i64)),
            Val::TinyInt(r) => Value::new(0i64.min(*r as i64)),
            _ => Value::new(Val::Null),
        }
    }

    fn max(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::BigInt(r) => Value::new(0i64.max(*r)),
            Val::Integer(r) => Value::new(0i64.max(*r as i64)),
            Val::SmallInt(r) => Value::new(0i64.max(*r as i64)),
            Val::TinyInt(r) => Value::new(0i64.max(*r as i64)),
            _ => Value::new(Val::Null),
        }
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_val() {
            Val::BigInt(n) => n.to_string(),
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }
}

pub static BIGINT_TYPE_INSTANCE: BigIntType = BigIntType;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bigint_comparisons() {
        let bigint_type = BigIntType::new();
        let zero = Value::new(0i64);
        let one = Value::new(1i64);
        let neg_one = Value::new(-1i64);
        let null = Value::new(Val::Null);

        // Test equals
        assert_eq!(bigint_type.compare_equals(&zero), CmpBool::CmpTrue);
        assert_eq!(bigint_type.compare_equals(&one), CmpBool::CmpFalse);
        assert_eq!(bigint_type.compare_equals(&null), CmpBool::CmpNull);

        // Test less than
        assert_eq!(bigint_type.compare_less_than(&one), CmpBool::CmpTrue);
        assert_eq!(bigint_type.compare_less_than(&neg_one), CmpBool::CmpFalse);
        assert_eq!(bigint_type.compare_less_than(&null), CmpBool::CmpNull);
    }

    #[test]
    fn test_bigint_arithmetic() {
        let bigint_type = BigIntType::new();
        let two = Value::new(2i64);
        let zero = Value::new(0i64);
        let max = Value::new(i64::MAX);

        // Test addition
        assert_eq!(bigint_type.add(&two).unwrap(), Value::new(2i64));

        // Test division by zero
        assert!(bigint_type.divide(&zero).is_err());

        // Test modulo
        assert_eq!(bigint_type.modulo(&zero), Value::new(Val::Null));

        // Test overflow
        assert!(bigint_type.add(&max).is_ok());
    }

    #[test]
    fn test_bigint_min_max() {
        let bigint_type = BigIntType::new();
        let pos = Value::new(1i64);
        let neg = Value::new(-1i64);
        let decimal = Value::new(1.5f64);

        // Test min/max
        assert_eq!(Type::min(&bigint_type, &pos), Value::new(0i64));
        assert_eq!(Type::min(&bigint_type, &neg), Value::new(-1i64));
        assert_eq!(Type::min(&bigint_type, &decimal), Value::new(Val::Null));

        assert_eq!(Type::max(&bigint_type, &pos), Value::new(1i64));
        assert_eq!(Type::max(&bigint_type, &neg), Value::new(0i64));
        assert_eq!(Type::max(&bigint_type, &decimal), Value::new(Val::Null));
    }
}
