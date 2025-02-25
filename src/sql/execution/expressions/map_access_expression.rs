use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
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
