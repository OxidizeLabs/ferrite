use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};
use std::fmt;

/// Default length for CHAR type when used in disk storage
pub const DEFAULT_CHAR_LENGTH: usize = 255;

/// Global instance of CharType for use in disk storage
pub static CHAR_TYPE_INSTANCE: CharType = CharType {
    length: DEFAULT_CHAR_LENGTH,
};

// Implementation for CharType (fixed-length strings)
#[derive(Debug, Clone, PartialEq)]
pub struct CharType {
    length: usize, // Fixed length for this CHAR type
}

impl CharType {
    /// Creates a new `CharType` instance with the specified fixed length.
    pub fn new(length: usize) -> Self {
        CharType { length }
    }

    // Pad or truncate string to fixed length
    fn normalize_string(&self, s: &str) -> String {
        let mut result = s.chars().take(self.length).collect::<String>();
        while result.len() < self.length {
            result.push(' ');
        }
        result
    }
}

impl Type for CharType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Char
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::VarLen(r) | Val::ConstLen(r) => {
                let normalized = self.normalize_string(r);
                let empty_normalized = self.normalize_string("");
                CmpBool::from(empty_normalized == normalized)
            },
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
            Val::VarLen(r) | Val::ConstLen(r) => {
                let normalized = self.normalize_string(r);
                let empty_normalized = self.normalize_string("");
                CmpBool::from(empty_normalized < normalized)
            },
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::VarLen(r) | Val::ConstLen(r) => {
                let normalized = self.normalize_string(r);
                let empty_normalized = self.normalize_string("");
                CmpBool::from(empty_normalized <= normalized)
            },
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::VarLen(r) | Val::ConstLen(r) => {
                let normalized = self.normalize_string(r);
                let empty_normalized = self.normalize_string("");
                CmpBool::from(empty_normalized > normalized)
            },
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::VarLen(r) | Val::ConstLen(r) => {
                let normalized = self.normalize_string(r);
                let empty_normalized = self.normalize_string("");
                CmpBool::from(empty_normalized >= normalized)
            },
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn add(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::VarLen(r) | Val::ConstLen(r) => {
                let normalized = self.normalize_string(r);
                Ok(Value::new(Val::ConstLen(normalized)))
            },
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot add non-string types to CHAR".to_string()),
        }
    }

    fn subtract(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot subtract from CHAR strings".to_string())
    }

    fn multiply(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot multiply CHAR strings".to_string())
    }

    fn divide(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot divide CHAR strings".to_string())
    }

    fn modulo(&self, _other: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn min(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::VarLen(r) | Val::ConstLen(r) => {
                let normalized = self.normalize_string(r);
                if normalized.trim().is_empty() {
                    Value::new(Val::ConstLen(normalized))
                } else {
                    Value::new(Val::ConstLen(
                        normalized
                            .chars()
                            .filter(|c| !c.is_whitespace())
                            .min()
                            .map(|c| self.normalize_string(&c.to_string()))
                            .unwrap_or_else(|| self.normalize_string("")),
                    ))
                }
            },
            _ => Value::new(Val::Null),
        }
    }

    fn max(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::VarLen(r) | Val::ConstLen(r) => {
                let normalized = self.normalize_string(r);
                if normalized.trim().is_empty() {
                    Value::new(Val::ConstLen(normalized))
                } else {
                    Value::new(Val::ConstLen(
                        normalized
                            .chars()
                            .filter(|c| !c.is_whitespace())
                            .max()
                            .map(|c| self.normalize_string(&c.to_string()))
                            .unwrap_or_else(|| self.normalize_string("")),
                    ))
                }
            },
            _ => Value::new(Val::Null),
        }
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_val() {
            Val::VarLen(s) | Val::ConstLen(s) => self.normalize_string(s),
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }
}

impl fmt::Display for CharType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CHAR({})", self.length)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_char_padding() {
        let char_type = CharType::new(5);
        let short = Value::new("abc");
        let exact = Value::new("12345");
        let long = Value::new("abcdefgh");

        // Test padding and truncation using Type trait's to_string
        assert_eq!(Type::to_string(&char_type, &short), "abc  ");
        assert_eq!(Type::to_string(&char_type, &exact), "12345");
        assert_eq!(Type::to_string(&char_type, &long), "abcde");
    }

    #[test]
    fn test_char_comparisons() {
        let char_type = CharType::new(5);
        let empty = Value::new("");
        let hello = Value::new("hello");
        let hi = Value::new("hi");
        let null = Value::new(Val::Null);

        // Test equals (with padding)
        assert_eq!(char_type.compare_equals(&empty), CmpBool::CmpTrue); // "     " == "     "
        assert_eq!(char_type.compare_equals(&hi), CmpBool::CmpFalse); // "     " != "hi   "
        assert_eq!(char_type.compare_equals(&null), CmpBool::CmpNull);

        // Test less than (with padding)
        assert_eq!(char_type.compare_less_than(&hello), CmpBool::CmpTrue);
        assert_eq!(char_type.compare_less_than(&empty), CmpBool::CmpFalse);
    }

    #[test]
    fn test_char_arithmetic() {
        let char_type = CharType::new(5);
        let hello = Value::new("hello");
        let null = Value::new(Val::Null);

        // Test addition (should maintain padding)
        let result = char_type.add(&hello).unwrap();
        assert_eq!(Type::to_string(&char_type, &result), "hello");

        // Test invalid operations
        assert!(char_type.subtract(&hello).is_err());
        assert!(char_type.multiply(&hello).is_err());
        assert!(char_type.divide(&hello).is_err());
        assert_eq!(char_type.modulo(&hello), Value::new(Val::Null));

        // Test null handling
        assert_eq!(char_type.add(&null).unwrap(), Value::new(Val::Null));
    }

    #[test]
    fn test_char_min_max() {
        let char_type = CharType::new(5);
        let empty = Value::new("");
        let hello = Value::new("hello");
        let null = Value::new(Val::Null);

        // Test min/max (should maintain padding)
        assert_eq!(
            Type::to_string(&char_type, &Type::min(&char_type, &empty)),
            "     "
        );
        assert_eq!(
            Type::to_string(&char_type, &Type::min(&char_type, &hello)),
            "e    "
        );
        assert_eq!(Type::min(&char_type, &null), Value::new(Val::Null));

        assert_eq!(
            Type::to_string(&char_type, &Type::max(&char_type, &empty)),
            "     "
        );
        assert_eq!(
            Type::to_string(&char_type, &Type::max(&char_type, &hello)),
            "o    "
        );
        assert_eq!(Type::max(&char_type, &null), Value::new(Val::Null));
    }
}
