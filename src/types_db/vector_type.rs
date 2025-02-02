use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};
use std::cmp::PartialEq;
use std::fmt;

/// Represents a vector of integers in the database type system.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorType;

impl VectorType {
    /// Creates a new `VectorType` instance.
    pub fn new() -> Self {
        VectorType
    }
}

impl Type for VectorType {
    /// Returns the type ID for `VectorType`.
    fn get_type_id(&self) -> TypeId {
        TypeId::Vector
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Vector(r) => {
                let empty_vec = Vec::<i32>::new();
                // Convert Value vector to i32 vector for comparison
                let other_vec: Vec<i32> = r.iter()
                    .filter_map(|v| match v.get_val() {
                        Val::Integer(i) => Some(*i),
                        _ => None,
                    })
                    .collect();
                CmpBool::from(empty_vec == other_vec)
            }
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
            Val::Vector(r) => {
                let empty_vec = Vec::<i32>::new();
                let other_vec: Vec<i32> = r.iter()
                    .filter_map(|v| match v.get_val() {
                        Val::Integer(i) => Some(*i),
                        _ => None,
                    })
                    .collect();
                if empty_vec.len() == other_vec.len() {
                    CmpBool::CmpFalse
                } else if empty_vec.is_empty() && !other_vec.is_empty() {
                    CmpBool::CmpTrue
                } else {
                    CmpBool::from(empty_vec.len() < other_vec.len())
                }
            }
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Vector(r) => {
                let empty_vec = Vec::<i32>::new();
                let other_vec: Vec<i32> = r.iter()
                    .filter_map(|v| match v.get_val() {
                        Val::Integer(i) => Some(*i),
                        _ => None,
                    })
                    .collect();
                CmpBool::from(empty_vec.len() <= other_vec.len())
            }
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Vector(r) => {
                let empty_vec = Vec::<i32>::new();
                let other_vec: Vec<i32> = r.iter()
                    .filter_map(|v| match v.get_val() {
                        Val::Integer(i) => Some(*i),
                        _ => None,
                    })
                    .collect();
                CmpBool::from(empty_vec.len() > other_vec.len())
            }
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Vector(r) => {
                let empty_vec = Vec::<i32>::new();
                let other_vec: Vec<i32> = r.iter()
                    .filter_map(|v| match v.get_val() {
                        Val::Integer(i) => Some(*i),
                        _ => None,
                    })
                    .collect();
                CmpBool::from(empty_vec.len() >= other_vec.len())
            }
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn add(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Vector(r) => {
                let mut result = Vec::new();
                // Clone the values instead of taking references
                result.extend(r.iter().cloned());
                Ok(Value::new(result))
            }
            Val::Integer(i) => {
                let mut result = Vec::new();
                result.push(Value::new(*i));
                Ok(Value::new(result))
            }
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot add non-vector/integer types to Vector".to_string()),
        }
    }

    fn subtract(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot subtract from vectors".to_string())
    }

    fn multiply(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot multiply vectors".to_string())
    }

    fn divide(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot divide vectors".to_string())
    }

    fn modulo(&self, _other: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn min(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Vector(r) => {
                // Extract integer values for comparison
                let min_val = r.iter()
                    .filter_map(|v| match v.get_val() {
                        Val::Integer(i) => Some(*i),
                        _ => None,
                    })
                    .min();

                if let Some(val) = min_val {
                    Value::new(val)
                } else {
                    Value::new(Val::Null)
                }
            }
            _ => Value::new(Val::Null),
        }
    }

    fn max(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Vector(r) => {
                // Extract integer values for comparison
                let max_val = r.iter()
                    .filter_map(|v| match v.get_val() {
                        Val::Integer(i) => Some(*i),
                        _ => None,
                    })
                    .max();

                if let Some(val) = max_val {
                    Value::new(val)
                } else {
                    Value::new(Val::Null)
                }
            }
            _ => Value::new(Val::Null),
        }
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_val() {
            Val::Vector(v) => {
                // Extract and format integer values
                let values: Vec<String> = v.iter()
                    .map(|value| match value.get_val() {
                        Val::Integer(i) => format!("{}", i),
                        Val::Null => "NULL".to_string(),
                        _ => "INVALID".to_string(),
                    })
                    .collect();
                format!("[{}]", values.join(", "))
            }
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }
}

impl fmt::Display for VectorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VECTOR")
    }
}

pub static VECTOR_TYPE_INSTANCE: VectorType = VectorType;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_comparisons() {
        let vector_type = VectorType::new();
        // Convert i32 vectors to Value vectors
        let empty = Value::new(Vec::<Value>::new());
        let single = Value::new(vec![Value::new(1i32)]);
        let multiple = Value::new(vec![Value::new(1i32), Value::new(2i32), Value::new(3i32)]);
        let null = Value::new(Val::Null);

        // Test equals
        assert_eq!(vector_type.compare_equals(&empty), CmpBool::CmpTrue);
        assert_eq!(vector_type.compare_equals(&single), CmpBool::CmpFalse);
        assert_eq!(vector_type.compare_equals(&null), CmpBool::CmpNull);

        // Test less than (based on length)
        assert_eq!(vector_type.compare_less_than(&single), CmpBool::CmpTrue);
        assert_eq!(vector_type.compare_less_than(&multiple), CmpBool::CmpTrue);
        assert_eq!(vector_type.compare_less_than(&empty), CmpBool::CmpFalse);
        assert_eq!(vector_type.compare_less_than(&null), CmpBool::CmpNull);
    }

    #[test]
    fn test_vector_arithmetic() {
        let vector_type = VectorType::new();
        let vec = Value::new(vec![Value::new(1i32), Value::new(2i32), Value::new(3i32)]);
        let num = Value::new(4i32);
        let null = Value::new(Val::Null);

        // Test addition (concatenation)
        let result = vector_type.add(&vec).unwrap();
        match result.get_val() {
            Val::Vector(v) => {
                let expected = vec![Value::new(1i32), Value::new(2i32), Value::new(3i32)];
                assert_eq!(v, &expected);
            }
            _ => panic!("Expected vector result"),
        }

        // Test adding integer
        let result = vector_type.add(&num).unwrap();
        match result.get_val() {
            Val::Vector(v) => {
                let expected = vec![Value::new(4i32)];
                assert_eq!(v, &expected);
            }
            _ => panic!("Expected vector result"),
        }

        // Test invalid operations
        assert!(vector_type.subtract(&vec).is_err());
        assert!(vector_type.multiply(&vec).is_err());
        assert!(vector_type.divide(&vec).is_err());
        assert_eq!(vector_type.modulo(&vec), Value::new(Val::Null));

        // Test null handling
        assert_eq!(vector_type.add(&null).unwrap(), Value::new(Val::Null));
    }

    #[test]
    fn test_vector_min_max() {
        let vector_type = VectorType::new();
        let empty = Value::new(Vec::<Value>::new());
        let vec = Value::new(vec![
            Value::new(1i32),
            Value::new(5i32),
            Value::new(3i32),
            Value::new(2i32),
            Value::new(4i32)
        ]);
        let null = Value::new(Val::Null);

        // Test min/max
        assert_eq!(Type::min(&vector_type, &vec), Value::new(1i32));
        assert_eq!(Type::min(&vector_type, &empty), Value::new(Val::Null));
        assert_eq!(Type::min(&vector_type, &null), Value::new(Val::Null));

        assert_eq!(Type::max(&vector_type, &vec), Value::new(5i32));
        assert_eq!(Type::max(&vector_type, &empty), Value::new(Val::Null));
        assert_eq!(Type::max(&vector_type, &null), Value::new(Val::Null));
    }

    #[test]
    fn test_vector_to_string() {
        let vec = Value::new(vec![Value::new(1i32), Value::new(2i32), Value::new(3i32)]);
        let empty = Value::new(Vec::<Value>::new());
        let null = Value::new(Val::Null);

        // Use explicit ToString trait
        assert_eq!(ToString::to_string(&vec), "[1, 2, 3]");
        assert_eq!(ToString::to_string(&empty), "[]");
        assert_eq!(ToString::to_string(&null), "NULL");
    }
}
