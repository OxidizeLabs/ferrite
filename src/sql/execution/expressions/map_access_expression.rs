use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use crate::common::rid::RID;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub enum MapAccessKey {
    String(String),
    Number(i64),
}

#[derive(Clone, Debug, PartialEq)]
pub struct MapAccessExpression {
    column: Arc<Expression>,
    keys: Vec<MapAccessKey>,
    return_type: Column,
    children: Vec<Arc<Expression>>,
}

impl MapAccessExpression {
    pub fn new(column: Arc<Expression>, keys: Vec<MapAccessKey>, return_type: Column) -> Self {
        let children = vec![column.clone()];
        Self {
            column,
            keys,
            return_type,
            children,
        }
    }

    // Helper method to access a map or array value using a key
    fn access_value(&self, value: &Value, key: &MapAccessKey) -> Result<Value, ExpressionError> {
        match value.get_val() {
            // For vector values (arrays), use numeric index
            Val::Vector(vec) => match key {
                MapAccessKey::Number(idx) => {
                    let idx = *idx as usize;
                    if idx >= vec.len() {
                        return Err(ExpressionError::IndexOutOfBounds {
                            idx,
                            size: vec.len(),
                        });
                    }
                    Ok(vec[idx].clone())
                }
                MapAccessKey::String(_) => Err(ExpressionError::InvalidOperation(
                    "Cannot access array with string key".to_string(),
                )),
            },
            // For JSON-like structures stored as strings
            Val::VarLen(s) | Val::ConstLen(s) => {
                // Try to parse as JSON
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(json_value) => match key {
                        MapAccessKey::String(key_str) => {
                            if let Some(obj) = json_value.as_object() {
                                if let Some(value) = obj.get(key_str) {
                                    self.json_to_value(value)
                                } else {
                                    Err(ExpressionError::KeyNotFound(key_str.clone()))
                                }
                            } else {
                                Err(ExpressionError::InvalidOperation(
                                    "Cannot access non-object with string key".to_string(),
                                ))
                            }
                        }
                        MapAccessKey::Number(idx) => {
                            if let Some(arr) = json_value.as_array() {
                                let idx = *idx as usize;
                                if idx >= arr.len() {
                                    return Err(ExpressionError::IndexOutOfBounds {
                                        idx,
                                        size: arr.len(),
                                    });
                                }
                                self.json_to_value(&arr[idx])
                            } else {
                                Err(ExpressionError::InvalidOperation(
                                    "Cannot access non-array with numeric key".to_string(),
                                ))
                            }
                        }
                    },
                    Err(_) => Err(ExpressionError::InvalidOperation(
                        "Failed to parse string as JSON".to_string(),
                    )),
                }
            }
            _ => Err(ExpressionError::InvalidOperation(format!(
                "Cannot access value of type {:?} with key",
                value.get_type_id()
            ))),
        }
    }

    // Helper to convert JSON value to our Value type
    fn json_to_value(&self, json_value: &serde_json::Value) -> Result<Value, ExpressionError> {
        match json_value {
            serde_json::Value::Null => Ok(Value::new(Val::Null)),
            serde_json::Value::Bool(b) => Ok(Value::new(*b)),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(Value::new(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(Value::new(f))
                } else {
                    Err(ExpressionError::InvalidOperation(
                        "Unsupported numeric value".to_string(),
                    ))
                }
            }
            serde_json::Value::String(s) => Ok(Value::new(s.clone())),
            serde_json::Value::Array(arr) => {
                let values: Result<Vec<Value>, ExpressionError> =
                    arr.iter().map(|v| self.json_to_value(v)).collect();
                Ok(Value::new_vector(values?))
            }
            serde_json::Value::Object(obj) => {
                // Convert JSON object to string representation
                Ok(Value::new(serde_json::to_string(obj).unwrap_or_default()))
            }
        }
    }
}

impl ExpressionOps for MapAccessExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let map_value = self.column.evaluate(tuple, schema)?;

        // Apply each key in sequence to drill down into the structure
        let mut current_value = map_value;
        for key in &self.keys {
            current_value = self.access_value(&current_value, key)?;
        }

        Ok(current_value)
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        // First evaluate the column expression in the join context
        let map_value =
            self.column
                .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;

        // Apply each key in sequence to drill down into the structure
        let mut current_value = map_value;
        for key in &self.keys {
            current_value = self.access_value(&current_value, key)?;
        }

        Ok(current_value)
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        &self.children[child_idx]
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        if children.len() != 1 {
            panic!("MapAccessExpression requires exactly one child");
        }

        Arc::new(Expression::MapAccess(MapAccessExpression::new(
            children[0].clone(),
            self.keys.clone(),
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate the column expression
        self.column.validate(schema)?;

        // Check that the column type is compatible with map/array access
        let column_type = self.column.get_return_type().get_type();
        match column_type {
            TypeId::VarChar | TypeId::Char | TypeId::Vector => Ok(()),
            _ => Err(ExpressionError::InvalidOperation(format!(
                "Cannot perform map access on type {:?}",
                column_type
            ))),
        }
    }
}

impl Display for MapAccessExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.column)?;
        for key in &self.keys {
            match key {
                MapAccessKey::String(s) => write!(f, "['{}']", s)?,
                MapAccessKey::Number(n) => write!(f, "[{}]", n)?,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::storage::table::tuple::Tuple;
    use crate::types_db::type_id::TypeId;
    use serde_json::json;
    use std::collections::HashMap;

    fn create_test_schema() -> Schema {
        Schema::new(vec![])
    }

    fn create_test_tuple() -> Tuple {
        Tuple::new(&[], create_test_schema(), RID::new(0, 0))
    }

    fn create_constant_expression(value: Value) -> Arc<Expression> {
        let column = Column::new("const", value.get_type_id());
        Arc::new(Expression::Constant(ConstantExpression::new(
            value, column, vec![],
        )))
    }

    #[test]
    fn test_vector_access_with_numeric_key() {
        // Create a vector value
        let vec_values = vec![
            Value::new(1),
            Value::new(2),
            Value::new(3),
            Value::new("test"),
        ];
        let vector_value = Value::new_vector(vec_values);
        
        // Create a constant expression with the vector value
        let column_expr = create_constant_expression(vector_value);
        
        // Create a MapAccessExpression to access the vector at index 2
        let return_type = Column::new("result", TypeId::Integer);
        let map_access = MapAccessExpression::new(
            column_expr,
            vec![MapAccessKey::Number(2)],
            return_type,
        );
        
        // Evaluate the expression
        let schema = create_test_schema();
        let tuple = create_test_tuple();
        let result = map_access.evaluate(&tuple, &schema).unwrap();
        
        // Check the result
        assert_eq!(result, Value::new(3));
    }

    #[test]
    fn test_vector_access_with_string_key_should_fail() {
        // Create a vector value
        let vec_values = vec![Value::new(1), Value::new(2)];
        let vector_value = Value::new_vector(vec_values);
        
        // Create a constant expression with the vector value
        let column_expr = create_constant_expression(vector_value);
        
        // Create a MapAccessExpression to access the vector with a string key
        let return_type = Column::new("result", TypeId::Integer);
        let map_access = MapAccessExpression::new(
            column_expr,
            vec![MapAccessKey::String("key".to_string())],
            return_type,
        );
        
        // Evaluate the expression - should fail
        let schema = create_test_schema();
        let tuple = create_test_tuple();
        let result = map_access.evaluate(&tuple, &schema);
        
        assert!(result.is_err());
        if let Err(ExpressionError::InvalidOperation(msg)) = result {
            assert_eq!(msg, "Cannot access array with string key");
        } else {
            panic!("Expected InvalidOperation error");
        }
    }

    #[test]
    fn test_vector_access_out_of_bounds() {
        // Create a vector value
        let vec_values = vec![Value::new(1), Value::new(2)];
        let vector_value = Value::new_vector(vec_values);
        
        // Create a constant expression with the vector value
        let column_expr = create_constant_expression(vector_value);
        
        // Create a MapAccessExpression to access the vector at an out-of-bounds index
        let return_type = Column::new("result", TypeId::Integer);
        let map_access = MapAccessExpression::new(
            column_expr,
            vec![MapAccessKey::Number(5)],
            return_type,
        );
        
        // Evaluate the expression - should fail
        let schema = create_test_schema();
        let tuple = create_test_tuple();
        let result = map_access.evaluate(&tuple, &schema);
        
        assert!(result.is_err());
        if let Err(ExpressionError::IndexOutOfBounds { idx, size }) = result {
            assert_eq!(idx, 5);
            assert_eq!(size, 2);
        } else {
            panic!("Expected IndexOutOfBounds error");
        }
    }

    #[test]
    fn test_json_object_access_with_string_key() {
        // Create a JSON object as a string
        let json_obj = json!({
            "name": "John",
            "age": 30,
            "address": {
                "city": "New York",
                "zip": 10001
            }
        });
        let json_str = serde_json::to_string(&json_obj).unwrap();
        let json_value = Value::new(json_str);
        
        // Create a constant expression with the JSON value
        let column_expr = create_constant_expression(json_value);
        
        // Create a MapAccessExpression to access the "name" field
        let return_type = Column::new("result", TypeId::VarChar);
        let map_access = MapAccessExpression::new(
            column_expr,
            vec![MapAccessKey::String("name".to_string())],
            return_type,
        );
        
        // Evaluate the expression
        let schema = create_test_schema();
        let tuple = create_test_tuple();
        let result = map_access.evaluate(&tuple, &schema).unwrap();
        
        // Check the result
        assert_eq!(result, Value::new("John"));
    }

    #[test]
    fn test_json_object_access_with_numeric_key_should_fail() {
        // Create a JSON object as a string
        let json_obj = json!({
            "name": "John",
            "age": 30
        });
        let json_str = serde_json::to_string(&json_obj).unwrap();
        let json_value = Value::new(json_str);
        
        // Create a constant expression with the JSON value
        let column_expr = create_constant_expression(json_value);
        
        // Create a MapAccessExpression to access with a numeric key
        let return_type = Column::new("result", TypeId::VarChar);
        let map_access = MapAccessExpression::new(
            column_expr,
            vec![MapAccessKey::Number(0)],
            return_type,
        );
        
        // Evaluate the expression - should fail
        let schema = create_test_schema();
        let tuple = create_test_tuple();
        let result = map_access.evaluate(&tuple, &schema);
        
        assert!(result.is_err());
        if let Err(ExpressionError::InvalidOperation(msg)) = result {
            assert_eq!(msg, "Cannot access non-array with numeric key");
        } else {
            panic!("Expected InvalidOperation error");
        }
    }

    #[test]
    fn test_json_array_access_with_numeric_key() {
        // Create a JSON array as a string
        let json_arr = json!(["apple", "banana", "cherry"]);
        let json_str = serde_json::to_string(&json_arr).unwrap();
        let json_value = Value::new(json_str);
        
        // Create a constant expression with the JSON value
        let column_expr = create_constant_expression(json_value);
        
        // Create a MapAccessExpression to access the array at index 1
        let return_type = Column::new("result", TypeId::VarChar);
        let map_access = MapAccessExpression::new(
            column_expr,
            vec![MapAccessKey::Number(1)],
            return_type,
        );
        
        // Evaluate the expression
        let schema = create_test_schema();
        let tuple = create_test_tuple();
        let result = map_access.evaluate(&tuple, &schema).unwrap();
        
        // Check the result
        assert_eq!(result, Value::new("banana"));
    }

    #[test]
    fn test_json_array_access_with_string_key_should_fail() {
        // Create a JSON array as a string
        let json_arr = json!(["apple", "banana", "cherry"]);
        let json_str = serde_json::to_string(&json_arr).unwrap();
        let json_value = Value::new(json_str);
        
        // Create a constant expression with the JSON value
        let column_expr = create_constant_expression(json_value);
        
        // Create a MapAccessExpression to access with a string key
        let return_type = Column::new("result", TypeId::VarChar);
        let map_access = MapAccessExpression::new(
            column_expr,
            vec![MapAccessKey::String("key".to_string())],
            return_type,
        );
        
        // Evaluate the expression - should fail
        let schema = create_test_schema();
        let tuple = create_test_tuple();
        let result = map_access.evaluate(&tuple, &schema);
        
        assert!(result.is_err());
        if let Err(ExpressionError::InvalidOperation(msg)) = result {
            assert_eq!(msg, "Cannot access non-object with string key");
        } else {
            panic!("Expected InvalidOperation error");
        }
    }

    #[test]
    fn test_nested_json_access() {
        // Create a nested JSON object as a string
        let json_obj = json!({
            "user": {
                "profile": {
                    "name": "John",
                    "contacts": ["email@example.com", "555-1234"]
                }
            }
        });
        let json_str = serde_json::to_string(&json_obj).unwrap();
        let json_value = Value::new(json_str);
        
        // Create a constant expression with the JSON value
        let column_expr = create_constant_expression(json_value);
        
        // Create a MapAccessExpression to access nested fields
        let return_type = Column::new("result", TypeId::VarChar);
        let map_access = MapAccessExpression::new(
            column_expr,
            vec![
                MapAccessKey::String("user".to_string()),
                MapAccessKey::String("profile".to_string()),
                MapAccessKey::String("contacts".to_string()),
                MapAccessKey::Number(1),
            ],
            return_type,
        );
        
        // Evaluate the expression
        let schema = create_test_schema();
        let tuple = create_test_tuple();
        let result = map_access.evaluate(&tuple, &schema).unwrap();
        
        // Check the result
        assert_eq!(result, Value::new("555-1234"));
    }

    #[test]
    fn test_key_not_found() {
        // Create a JSON object as a string
        let json_obj = json!({
            "name": "John",
            "age": 30
        });
        let json_str = serde_json::to_string(&json_obj).unwrap();
        let json_value = Value::new(json_str);
        
        // Create a constant expression with the JSON value
        let column_expr = create_constant_expression(json_value);
        
        // Create a MapAccessExpression to access a non-existent key
        let return_type = Column::new("result", TypeId::VarChar);
        let map_access = MapAccessExpression::new(
            column_expr,
            vec![MapAccessKey::String("address".to_string())],
            return_type,
        );
        
        // Evaluate the expression - should fail
        let schema = create_test_schema();
        let tuple = create_test_tuple();
        let result = map_access.evaluate(&tuple, &schema);
        
        assert!(result.is_err());
        if let Err(ExpressionError::KeyNotFound(key)) = result {
            assert_eq!(key, "address");
        } else {
            panic!("Expected KeyNotFound error");
        }
    }

    #[test]
    fn test_invalid_json_string() {
        // Create an invalid JSON string
        let invalid_json = Value::new("{ invalid json }");
        
        // Create a constant expression with the invalid JSON
        let column_expr = create_constant_expression(invalid_json);
        
        // Create a MapAccessExpression
        let return_type = Column::new("result", TypeId::VarChar);
        let map_access = MapAccessExpression::new(
            column_expr,
            vec![MapAccessKey::String("key".to_string())],
            return_type,
        );
        
        // Evaluate the expression - should fail
        let schema = create_test_schema();
        let tuple = create_test_tuple();
        let result = map_access.evaluate(&tuple, &schema);
        
        assert!(result.is_err());
        if let Err(ExpressionError::InvalidOperation(msg)) = result {
            assert_eq!(msg, "Failed to parse string as JSON");
        } else {
            panic!("Expected InvalidOperation error");
        }
    }

    #[test]
    fn test_json_to_value_conversion() {
        // Create a JSON object with various types
        let json_obj = json!({
            "null_value": null,
            "bool_value": true,
            "int_value": 42,
            "float_value": 3.14,
            "string_value": "hello",
            "array_value": [1, 2, 3],
            "object_value": {"key": "value"}
        });
        let json_str = serde_json::to_string(&json_obj).unwrap();
        let json_value = Value::new(json_str);
        
        // Create a constant expression with the JSON value
        let column_expr = create_constant_expression(json_value);
        
        // Test null conversion
        let map_access = MapAccessExpression::new(
            column_expr.clone(),
            vec![MapAccessKey::String("null_value".to_string())],
            Column::new("result", TypeId::Invalid),
        );
        let result = map_access.evaluate(&create_test_tuple(), &create_test_schema()).unwrap();
        assert!(matches!(result.get_val(), Val::Null));
        
        // Test boolean conversion
        let map_access = MapAccessExpression::new(
            column_expr.clone(),
            vec![MapAccessKey::String("bool_value".to_string())],
            Column::new("result", TypeId::Boolean),
        );
        let result = map_access.evaluate(&create_test_tuple(), &create_test_schema()).unwrap();
        assert_eq!(result, Value::new(true));
        
        // Test integer conversion
        let map_access = MapAccessExpression::new(
            column_expr.clone(),
            vec![MapAccessKey::String("int_value".to_string())],
            Column::new("result", TypeId::BigInt),
        );
        let result = map_access.evaluate(&create_test_tuple(), &create_test_schema()).unwrap();
        assert_eq!(result, Value::new(42i64));
        
        // Test float conversion
        let map_access = MapAccessExpression::new(
            column_expr.clone(),
            vec![MapAccessKey::String("float_value".to_string())],
            Column::new("result", TypeId::Decimal),
        );
        let result = map_access.evaluate(&create_test_tuple(), &create_test_schema()).unwrap();
        assert_eq!(result, Value::new(3.14));
        
        // Test string conversion
        let map_access = MapAccessExpression::new(
            column_expr.clone(),
            vec![MapAccessKey::String("string_value".to_string())],
            Column::new("result", TypeId::VarChar),
        );
        let result = map_access.evaluate(&create_test_tuple(), &create_test_schema()).unwrap();
        assert_eq!(result, Value::new("hello"));
        
        // Test array conversion
        let map_access = MapAccessExpression::new(
            column_expr.clone(),
            vec![MapAccessKey::String("array_value".to_string())],
            Column::new("result", TypeId::Vector),
        );
        let result = map_access.evaluate(&create_test_tuple(), &create_test_schema()).unwrap();
        if let Val::Vector(vec) = result.get_val() {
            assert_eq!(vec.len(), 3);
            assert_eq!(vec[0], Value::new(1i64));
            assert_eq!(vec[1], Value::new(2i64));
            assert_eq!(vec[2], Value::new(3i64));
        } else {
            panic!("Expected Vector value");
        }
    }

    #[test]
    fn test_validate_compatible_types() {
        // Test validation with compatible types
        let vector_value = Value::new_vector(vec![Value::new(1), Value::new(2)]);
        let column_expr = create_constant_expression(vector_value);
        let return_type = Column::new("result", TypeId::Integer);
        let map_access = MapAccessExpression::new(
            column_expr,
            vec![MapAccessKey::Number(0)],
            return_type,
        );
        
        let schema = create_test_schema();
        let result = map_access.validate(&schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_incompatible_types() {
        // Test validation with incompatible types
        let int_value = Value::new(42);
        let column_expr = create_constant_expression(int_value);
        let return_type = Column::new("result", TypeId::Integer);
        let map_access = MapAccessExpression::new(
            column_expr,
            vec![MapAccessKey::Number(0)],
            return_type,
        );
        
        let schema = create_test_schema();
        let result = map_access.validate(&schema);
        assert!(result.is_err());
        if let Err(ExpressionError::InvalidOperation(msg)) = result {
            assert!(msg.contains("Cannot perform map access on type"));
        } else {
            panic!("Expected InvalidOperation error");
        }
    }

    #[test]
    fn test_display_formatting() {
        // Test the Display implementation
        let vector_value = Value::new_vector(vec![Value::new(1), Value::new(2)]);
        let column_expr = create_constant_expression(vector_value);
        let return_type = Column::new("result", TypeId::Integer);
        
        // Test with numeric key
        let map_access = MapAccessExpression::new(
            column_expr.clone(),
            vec![MapAccessKey::Number(1)],
            return_type.clone(),
        );
        assert_eq!(format!("{}", map_access), "[1, 2][1]");
        
        // Test with string key
        let map_access = MapAccessExpression::new(
            column_expr.clone(),
            vec![MapAccessKey::String("key".to_string())],
            return_type.clone(),
        );
        assert_eq!(format!("{}", map_access), "[1, 2]['key']");
        
        // Test with multiple keys
        let map_access = MapAccessExpression::new(
            column_expr,
            vec![
                MapAccessKey::String("user".to_string()),
                MapAccessKey::Number(0),
                MapAccessKey::String("name".to_string()),
            ],
            return_type,
        );
        assert_eq!(format!("{}", map_access), "[1, 2]['user'][0]['name']");
    }

    #[test]
    fn test_evaluate_join() {
        // Create a vector value
        let vec_values = vec![Value::new(1), Value::new(2), Value::new(3)];
        let vector_value = Value::new_vector(vec_values);
        
        // Create a constant expression with the vector value
        let column_expr = create_constant_expression(vector_value);
        
        // Create a MapAccessExpression to access the vector at index 1
        let return_type = Column::new("result", TypeId::Integer);
        let map_access = MapAccessExpression::new(
            column_expr,
            vec![MapAccessKey::Number(1)],
            return_type,
        );
        
        // Evaluate the expression in a join context
        let left_schema = create_test_schema();
        let right_schema = create_test_schema();
        let left_tuple = create_test_tuple();
        let right_tuple = create_test_tuple();
        
        let result = map_access.evaluate_join(
            &left_tuple, 
            &left_schema, 
            &right_tuple, 
            &right_schema
        ).unwrap();
        
        // Check the result
        assert_eq!(result, Value::new(2));
    }

    #[test]
    fn test_clone_with_children() {
        // Create a vector value
        let vec_values = vec![Value::new(1), Value::new(2)];
        let vector_value = Value::new_vector(vec_values);
        
        // Create a constant expression with the vector value
        let column_expr = create_constant_expression(vector_value);
        
        // Create a MapAccessExpression
        let return_type = Column::new("result", TypeId::Integer);
        let map_access = MapAccessExpression::new(
            column_expr.clone(),
            vec![MapAccessKey::Number(1)],
            return_type.clone(),
        );
        
        // Create a new expression with different children
        let new_vector_value = Value::new_vector(vec![Value::new(3), Value::new(4)]);
        let new_column_expr = create_constant_expression(new_vector_value);
        let new_expr = map_access.clone_with_children(vec![new_column_expr]);
        
        // Evaluate both expressions
        let schema = create_test_schema();
        let tuple = create_test_tuple();
        
        let original_result = map_access.evaluate(&tuple, &schema).unwrap();
        
        if let Expression::MapAccess(new_map_access) = &*new_expr {
            let new_result = new_map_access.evaluate(&tuple, &schema).unwrap();
            
            // Original should return 2, new should return 4
            assert_eq!(original_result, Value::new(2));
            assert_eq!(new_result, Value::new(4));
        } else {
            panic!("Expected MapAccessExpression");
        }
    }

    #[test]
    #[should_panic(expected = "MapAccessExpression requires exactly one child")]
    fn test_clone_with_children_panic() {
        // Create a vector value
        let vec_values = vec![Value::new(1), Value::new(2)];
        let vector_value = Value::new_vector(vec_values);
        
        // Create a constant expression with the vector value
        let column_expr = create_constant_expression(vector_value);
        
        // Create a MapAccessExpression
        let return_type = Column::new("result", TypeId::Integer);
        let map_access = MapAccessExpression::new(
            column_expr,
            vec![MapAccessKey::Number(1)],
            return_type,
        );
        
        // Try to create with wrong number of children - should panic
        let _ = map_access.clone_with_children(vec![]);
    }
}
