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
            Val::Decimal(r) => Ok(Value::new(0.0 + *r)),
            Val::BigInt(r) => Ok(Value::new(0.0 + *r as f64)),
            Val::Integer(r) => Ok(Value::new(0.0 + *r as f64)),
            Val::SmallInt(r) => Ok(Value::new(0.0 + *r as f64)),
            Val::TinyInt(r) => Ok(Value::new(0.0 + *r as f64)),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot add non-numeric types to Decimal".to_string()),
        }
    }

    fn subtract(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Decimal(r) => Ok(Value::new(0.0 - *r)),
            Val::BigInt(r) => Ok(Value::new(0.0 - *r as f64)),
            Val::Integer(r) => Ok(Value::new(0.0 - *r as f64)),
            Val::SmallInt(r) => Ok(Value::new(0.0 - *r as f64)),
            Val::TinyInt(r) => Ok(Value::new(0.0 - *r as f64)),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot subtract non-numeric types from Decimal".to_string()),
        }
    }

    fn multiply(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Decimal(r) => Ok(Value::new(0.0 * *r)),
            Val::BigInt(r) => Ok(Value::new(0.0 * *r as f64)),
            Val::Integer(r) => Ok(Value::new(0.0 * *r as f64)),
            Val::SmallInt(r) => Ok(Value::new(0.0 * *r as f64)),
            Val::TinyInt(r) => Ok(Value::new(0.0 * *r as f64)),
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
            Val::Decimal(r) => Ok(Value::new(0.0 / *r)),
            Val::BigInt(r) => Ok(Value::new(0.0 / *r as f64)),
            Val::Integer(r) => Ok(Value::new(0.0 / *r as f64)),
            Val::SmallInt(r) => Ok(Value::new(0.0 / *r as f64)),
            Val::TinyInt(r) => Ok(Value::new(0.0 / *r as f64)),
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
            Val::Decimal(r) => Value::new(0.0 % *r),
            Val::BigInt(r) => Value::new(0.0 % *r as f64),
            Val::Integer(r) => Value::new(0.0 % *r as f64),
            Val::SmallInt(r) => Value::new(0.0 % *r as f64),
            Val::TinyInt(r) => Value::new(0.0 % *r as f64),
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
        self.to_string_with_precision(val, None, None)
    }
}

impl DecimalType {
    /// Format decimal with specific precision and scale
    fn to_string_with_precision(
        &self,
        val: &Value,
        precision: Option<u8>,
        scale: Option<u8>,
    ) -> String {
        match val.get_val() {
            Val::Decimal(n) => {
                if let Some(scale_val) = scale {
                    // Format with specified scale
                    format!("{:.1$}", n, scale_val as usize)
                } else {
                    // Default behavior: natural formatting that preserves backward compatibility
                    if n.fract() == 0.0 {
                        // Whole numbers: format as integers unless context requires decimal indication
                        format!("{}", *n as i64)
                    } else {
                        // Numbers with decimals: format with appropriate precision
                        let formatted = format!("{:.6}", n);
                        // Remove trailing zeros but keep at least one decimal place if needed
                        formatted
                            .trim_end_matches('0')
                            .trim_end_matches('.')
                            .to_string()
                    }
                }
            }
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }

    /// Public method to format decimal with precision and scale
    pub fn format_decimal(val: &Value, precision: Option<u8>, scale: Option<u8>) -> String {
        let decimal_type = DecimalType::new();
        decimal_type.to_string_with_precision(val, precision, scale)
    }

    /// Create a decimal value with precision and scale enforcement
    pub fn create_decimal_with_precision(
        value: f64,
        precision: Option<u8>,
        scale: Option<u8>,
    ) -> Result<Value, String> {
        // Validate precision and scale
        if let (Some(p), Some(s)) = (precision, scale) {
            if s > p {
                return Err(format!(
                    "Scale {} cannot be greater than precision {}",
                    s, p
                ));
            }
            if p > 38 {
                return Err(format!("Precision {} exceeds maximum of 38", p));
            }
        }

        // Apply scale rounding if specified
        let adjusted_value = if let Some(scale_val) = scale {
            let multiplier = 10f64.powi(scale_val as i32);
            (value * multiplier).round() / multiplier
        } else {
            value
        };

        // Validate precision if specified
        if let Some(precision_val) = precision {
            let total_digits = Self::count_total_digits(adjusted_value);
            if total_digits > precision_val {
                return Err(format!(
                    "Value {} exceeds precision {}",
                    adjusted_value, precision_val
                ));
            }
        }

        Ok(Value::new(adjusted_value))
    }

    /// Count total digits in a decimal number (excluding decimal point)
    fn count_total_digits(value: f64) -> u8 {
        if value == 0.0 {
            return 1;
        }

        let abs_value = value.abs();
        let integer_part = abs_value.trunc();
        let fractional_part = abs_value.fract();

        // Count integer digits
        let integer_digits = if integer_part == 0.0 {
            1
        } else {
            (integer_part.log10().floor() + 1.0) as u8
        };

        // Count fractional digits (up to 15 for f64 precision)
        let fractional_digits = if fractional_part == 0.0 {
            0
        } else {
            let frac_str = format!("{:.15}", fractional_part);
            let trimmed = frac_str.trim_end_matches('0');
            if trimmed.len() > 2 {
                // "0." prefix
                (trimmed.len() - 2) as u8
            } else {
                0
            }
        };

        integer_digits + fractional_digits
    }

    /// Perform arithmetic with precision and scale preservation
    pub fn add_with_precision(
        left: &Value,
        right: &Value,
        result_precision: Option<u8>,
        result_scale: Option<u8>,
    ) -> Result<Value, String> {
        let left_val = left
            .as_decimal()
            .map_err(|e| format!("Left operand error: {}", e))?;
        let right_val = right
            .as_decimal()
            .map_err(|e| format!("Right operand error: {}", e))?;

        let result = left_val + right_val;
        Self::create_decimal_with_precision(result, result_precision, result_scale)
    }

    /// Perform subtraction with precision and scale preservation
    pub fn subtract_with_precision(
        left: &Value,
        right: &Value,
        result_precision: Option<u8>,
        result_scale: Option<u8>,
    ) -> Result<Value, String> {
        let left_val = left
            .as_decimal()
            .map_err(|e| format!("Left operand error: {}", e))?;
        let right_val = right
            .as_decimal()
            .map_err(|e| format!("Right operand error: {}", e))?;

        let result = left_val - right_val;
        Self::create_decimal_with_precision(result, result_precision, result_scale)
    }

    /// Perform multiplication with precision and scale preservation
    pub fn multiply_with_precision(
        left: &Value,
        right: &Value,
        result_precision: Option<u8>,
        result_scale: Option<u8>,
    ) -> Result<Value, String> {
        let left_val = left
            .as_decimal()
            .map_err(|e| format!("Left operand error: {}", e))?;
        let right_val = right
            .as_decimal()
            .map_err(|e| format!("Right operand error: {}", e))?;

        let result = left_val * right_val;
        Self::create_decimal_with_precision(result, result_precision, result_scale)
    }

    /// Perform division with precision and scale preservation
    pub fn divide_with_precision(
        left: &Value,
        right: &Value,
        result_precision: Option<u8>,
        result_scale: Option<u8>,
    ) -> Result<Value, String> {
        let left_val = left
            .as_decimal()
            .map_err(|e| format!("Left operand error: {}", e))?;
        let right_val = right
            .as_decimal()
            .map_err(|e| format!("Right operand error: {}", e))?;

        if right_val == 0.0 {
            return Err("Division by zero".to_string());
        }

        let result = left_val / right_val;
        Self::create_decimal_with_precision(result, result_precision, result_scale)
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

        // Test whole numbers - should format as integers for backward compatibility
        assert_eq!(decimal_type.to_string(&Value::new(2.0f64)), "2");
        assert_eq!(decimal_type.to_string(&Value::new(10.0f64)), "10");
        assert_eq!(decimal_type.to_string(&Value::new(20.0f64)), "20");
        assert_eq!(decimal_type.to_string(&Value::new(3.0f64)), "3");

        // Test numbers with decimal places - preserve precision
        assert_eq!(decimal_type.to_string(&Value::new(1.5f64)), "1.5");
        assert_eq!(decimal_type.to_string(&Value::new(20.5f64)), "20.5");
        assert_eq!(decimal_type.to_string(&Value::new(0.123f64)), "0.123");
        assert_eq!(decimal_type.to_string(&Value::new(99.99f64)), "99.99");

        // Test edge cases
        assert_eq!(decimal_type.to_string(&Value::new(0.0f64)), "0");
        assert_eq!(decimal_type.to_string(&Value::new(-5.0f64)), "-5");
        assert_eq!(decimal_type.to_string(&Value::new(-2.5f64)), "-2.5");
    }
}
