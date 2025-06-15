use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};

pub struct StructType;

impl Type for StructType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Struct
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        if other.is_null() {
            return CmpBool::CmpNull;
        }

        // For structs, we compare the underlying vector values
        if let Val::Vector(values) = other.get_val() {
            // A valid struct must have at least two elements:
            // 1. The field names vector
            // 2. At least one field value
            if values.len() < 2 {
                return CmpBool::CmpFalse;
            }

            // First element should be a vector of field names
            if let Val::Vector(field_names) = &values[0].value_ {
                // Check that we have field names and corresponding values
                if field_names.is_empty() || values.len() != field_names.len() + 1 {
                    return CmpBool::CmpFalse;
                }
                return CmpBool::CmpTrue;
            }
        }

        CmpBool::CmpFalse
    }

    fn compare_not_equals(&self, other: &Value) -> CmpBool {
        match self.compare_equals(other) {
            CmpBool::CmpTrue => CmpBool::CmpFalse,
            CmpBool::CmpFalse => CmpBool::CmpTrue,
            CmpBool::CmpNull => CmpBool::CmpNull,
        }
    }

    fn compare_less_than(&self, _other: &Value) -> CmpBool {
        // Structs don't have a natural ordering
        CmpBool::CmpFalse
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        // For structs, <= is the same as ==
        self.compare_equals(other)
    }

    fn compare_greater_than(&self, _other: &Value) -> CmpBool {
        // Structs don't have a natural ordering
        CmpBool::CmpFalse
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        // For structs, >= is the same as ==
        self.compare_equals(other)
    }

    fn add(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot add struct values".to_string())
    }

    fn subtract(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot subtract struct values".to_string())
    }

    fn multiply(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot multiply struct values".to_string())
    }

    fn divide(&self, _other: &Value) -> Result<Value, String> {
        Err("Cannot divide struct values".to_string())
    }

    fn modulo(&self, _other: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn min(&self, _other: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn max(&self, _other: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn to_string(&self, val: &Value) -> String {
        if val.is_null() {
            return "NULL".to_string();
        }

        if val.is_struct() {
            if let Some(_struct_data) = &val.struct_data {
                let mut result = String::from("{");

                let field_names = val.get_struct_field_names();
                let field_values = val.get_struct_values();

                for (i, (name, value)) in field_names.iter().zip(field_values.iter()).enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&format!("{}: {}", name, value));
                }

                result.push('}');
                return result;
            }
        } else if let Val::Vector(values) = val.get_val() {
            let mut result = String::from("STRUCT{");

            // Skip the first element which contains field names
            if values.len() > 1 {
                let field_names = if let Val::Vector(names) = &values[0].value_ {
                    names
                        .iter()
                        .map(|v| {
                            if let Val::VarLen(name) | Val::ConstLen(name) = &v.value_ {
                                name.clone()
                            } else {
                                "?".to_string()
                            }
                        })
                        .collect::<Vec<_>>()
                } else {
                    vec!["?".to_string(); values.len() - 1]
                };

                for i in 1..values.len() {
                    if i > 1 {
                        result.push_str(", ");
                    }

                    let field_name = if i - 1 < field_names.len() {
                        &field_names[i - 1]
                    } else {
                        "?"
                    };

                    result.push_str(&format!(
                        "{}: {}",
                        field_name,
                        ToString::to_string(&values[i])
                    ));
                }
            }

            result.push('}');
            return result;
        }

        "INVALID_STRUCT".to_string()
    }

    fn is_inlined(&self, _val: &Value) -> bool {
        false
    }
}

// Create a static instance
pub static STRUCT_TYPE_INSTANCE: StructType = StructType;

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_struct(fields: Vec<&str>, values: Vec<Value>) -> Value {
        let field_names = fields
            .into_iter()
            .map(|s| Value::new(Val::VarLen(s.to_string())))
            .collect::<Vec<_>>();

        let mut struct_values = vec![Value::new(Val::Vector(field_names))];
        struct_values.extend(values);

        Value::new(Val::Vector(struct_values))
    }

    #[test]
    fn test_struct_equality() {
        let struct_type = StructType;

        // Test empty struct
        let empty_struct = create_test_struct(vec![], vec![]);
        assert_eq!(struct_type.compare_equals(&empty_struct), CmpBool::CmpFalse);

        // Test simple struct
        let struct1 = create_test_struct(
            vec!["name", "age"],
            vec![
                Value::new(Val::VarLen("John".to_string())),
                Value::new(Val::Integer(30)),
            ],
        );

        assert_eq!(struct_type.compare_equals(&struct1), CmpBool::CmpTrue);

        // Test null comparison
        assert_eq!(
            struct_type.compare_equals(&Value::new(Val::Null)),
            CmpBool::CmpNull
        );

        // Test invalid value comparison
        assert_eq!(
            struct_type.compare_equals(&Value::new(Val::Integer(42))),
            CmpBool::CmpFalse
        );
    }

    #[test]
    fn test_struct_to_string() {
        let struct_type = StructType;

        // Test null
        assert_eq!(struct_type.to_string(&Value::new(Val::Null)), "NULL");

        // Test empty struct
        let empty_struct = create_test_struct(vec![], vec![]);
        assert_eq!(struct_type.to_string(&empty_struct), "STRUCT{}");

        // Test struct with fields
        let struct1 = create_test_struct(
            vec!["name", "age"],
            vec![
                Value::new(Val::VarLen("John".to_string())),
                Value::new(Val::Integer(30)),
            ],
        );
        assert_eq!(
            struct_type.to_string(&struct1),
            "STRUCT{name: John, age: 30}"
        );

        // Test invalid struct
        assert_eq!(
            struct_type.to_string(&Value::new(Val::Integer(42))),
            "INVALID_STRUCT"
        );
    }

    #[test]
    fn test_struct_operations() {
        let struct_type = StructType;
        let struct1 = create_test_struct(
            vec!["name"],
            vec![Value::new(Val::VarLen("John".to_string()))],
        );

        // Test arithmetic operations
        assert!(struct_type.add(&struct1).is_err());
        assert!(struct_type.subtract(&struct1).is_err());
        assert!(struct_type.multiply(&struct1).is_err());
        assert!(struct_type.divide(&struct1).is_err());

        // Test comparison operations
        assert_eq!(struct_type.compare_less_than(&struct1), CmpBool::CmpFalse);
        assert_eq!(
            struct_type.compare_greater_than(&struct1),
            CmpBool::CmpFalse
        );

        // Test that <= and >= are same as ==
        let equals_result = struct_type.compare_equals(&struct1);
        assert_eq!(
            struct_type.compare_less_than_equals(&struct1),
            equals_result
        );
        assert_eq!(
            struct_type.compare_greater_than_equals(&struct1),
            equals_result
        );
    }
}
