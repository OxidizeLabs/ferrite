use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::common::rid::RID;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub struct MethodExpression {
    expr: Arc<Expression>,
    method_name: String,
    args: Vec<Arc<Expression>>,
    return_type: Column,
    children: Vec<Arc<Expression>>, // Store all children for consistent pattern
}

impl MethodExpression {
    pub fn new(
        expr: Arc<Expression>,
        method_name: String,
        args: Vec<Arc<Expression>>,
        return_type: Column,
    ) -> Self {
        // Create a combined children vector with expr as the first element
        // followed by all the args
        let mut children = vec![expr.clone()];
        children.extend(args.iter().cloned());

        Self {
            expr,
            method_name,
            args,
            return_type,
            children,
        }
    }
}

impl ExpressionOps for MethodExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let obj = self.expr.evaluate(tuple, schema)?;
        let mut arg_values = Vec::new();
        for arg in &self.args {
            arg_values.push(arg.evaluate(tuple, schema)?);
        }

        // Call method on object based on method name
        match self.method_name.as_str() {
            // String methods
            "length" | "len" => {
                if let Val::VarLen(s) = obj.get_val() {
                    Ok(Value::new(s.len() as i32))
                } else if let Val::ConstLen(s) = obj.get_val() {
                    Ok(Value::new(s.len() as i32))
                } else {
                    Err(ExpressionError::InvalidOperation(format!(
                        "Cannot call length() on non-string value: {:?}",
                        obj
                    )))
                }
            }

            // XML/JSON methods
            "value" => {
                // For XML/JSON value extraction
                // First argument is the path, second is the return type
                if arg_values.len() != 2 {
                    return Err(ExpressionError::InvalidOperation(
                        "value() method requires 2 arguments: path and return type".to_string(),
                    ));
                }

                let _path = match arg_values[0].get_val() {
                    Val::VarLen(s) | Val::ConstLen(s) => s.clone(),
                    _ => {
                        return Err(ExpressionError::InvalidOperation(
                            "First argument to value() must be a string path".to_string(),
                        ));
                    }
                };

                let type_str = match arg_values[1].get_val() {
                    Val::VarLen(s) | Val::ConstLen(s) => s.clone(),
                    _ => {
                        return Err(ExpressionError::InvalidOperation(
                            "Second argument to value() must be a string type".to_string(),
                        ));
                    }
                };

                // Extract value from XML/JSON using path
                // This is a simplified implementation - in a real system, you'd use a proper XML/JSON parser
                if let Val::VarLen(content) = obj.get_val() {
                    // Simple extraction logic - in reality, you'd use a proper XML/JSON parser
                    // For now, just return the content as the requested type
                    match type_str.to_uppercase().as_str() {
                        "NVARCHAR" | "VARCHAR" | "NVARCHAR(MAX)" | "VARCHAR(MAX)" => {
                            // Strip XML tags for simple cases (very simplified)
                            let result = if content.contains('<') && content.contains('>') {
                                // Very basic XML content extraction
                                let start = content.find('>').map(|i| i + 1).unwrap_or(0);
                                let end = content.rfind('<').unwrap_or(content.len());
                                if start < end {
                                    content[start..end].to_string()
                                } else {
                                    content.clone()
                                }
                            } else {
                                // Return as is for non-XML content
                                content.clone()
                            };
                            Ok(Value::new(result))
                        }
                        "INT" | "INTEGER" => {
                            // Try to parse content as integer
                            match content.parse::<i32>() {
                                Ok(i) => Ok(Value::new(i)),
                                Err(_) => Err(ExpressionError::InvalidOperation(format!(
                                    "Cannot convert '{}' to INTEGER",
                                    content
                                ))),
                            }
                        }
                        _ => Err(ExpressionError::InvalidOperation(format!(
                            "Unsupported type for value() method: {}",
                            type_str
                        ))),
                    }
                } else {
                    Err(ExpressionError::InvalidOperation(
                        "value() method can only be called on string/XML/JSON values".to_string(),
                    ))
                }
            }

            // Array/Vector methods
            "get" | "at" => {
                if arg_values.len() != 1 {
                    return Err(ExpressionError::InvalidOperation(
                        "get() method requires 1 index argument".to_string(),
                    ));
                }

                if let Val::Vector(vec) = obj.get_val() {
                    let index = match arg_values[0].get_val() {
                        Val::Integer(i) => *i as usize,
                        Val::BigInt(i) => *i as usize,
                        Val::SmallInt(i) => *i as usize,
                        Val::TinyInt(i) => *i as usize,
                        _ => {
                            return Err(ExpressionError::InvalidOperation(
                                "Index for get() must be an integer".to_string(),
                            ));
                        }
                    };

                    if index < vec.len() {
                        Ok(vec[index].clone())
                    } else {
                        Err(ExpressionError::InvalidOperation(format!(
                            "Index {} out of bounds for vector of length {}",
                            index,
                            vec.len()
                        )))
                    }
                } else {
                    Err(ExpressionError::InvalidOperation(
                        "get() method can only be called on vector values".to_string(),
                    ))
                }
            }

            "size" | "count" => {
                if !arg_values.is_empty() {
                    return Err(ExpressionError::InvalidOperation(
                        "size() method does not take arguments".to_string(),
                    ));
                }

                if let Val::Vector(vec) = obj.get_val() {
                    Ok(Value::new(vec.len() as i32))
                } else {
                    Err(ExpressionError::InvalidOperation(
                        "size() method can only be called on vector values".to_string(),
                    ))
                }
            }

            // Default case for unsupported methods
            _ => Err(ExpressionError::InvalidOperation(format!(
                "Unsupported method: {}",
                self.method_name
            ))),
        }
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        // First evaluate the expression on which the method is called
        let obj = self
            .expr
            .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;

        // Then evaluate all arguments
        let mut arg_values = Vec::new();
        for arg in &self.args {
            arg_values.push(arg.evaluate_join(
                left_tuple,
                left_schema,
                right_tuple,
                right_schema,
            )?);
        }

        // Create a merged schema and tuple with values from both tuples
        let temp_schema = Schema::merge(left_schema, right_schema);

        // Create values array for the merged tuple by combining values from both tuples
        let mut temp_values = Vec::new();
        temp_values.extend(left_tuple.get_values().iter().cloned());
        temp_values.extend(right_tuple.get_values().iter().cloned());

        let temp_tuple = Tuple::new(&temp_values, &temp_schema, RID::new(0, 0));

        // Create a temporary expression that just returns the already evaluated object
        let temp_expr = Expression::Constant(ConstantExpression::new(
            obj,
            self.expr.get_return_type().clone(),
            vec![],
        ));

        // Create a temporary method expression with the constant expression
        let temp_method = MethodExpression::new(
            Arc::new(temp_expr),
            self.method_name.clone(),
            self.args
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    Arc::new(Expression::Constant(ConstantExpression::new(
                        arg_values[i].clone(),
                        Column::new("arg", arg_values[i].get_type_id()),
                        vec![],
                    )))
                })
                .collect(),
            self.return_type.clone(),
        );

        // Evaluate the temporary method expression
        temp_method.evaluate(&temp_tuple, &temp_schema)
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        if child_idx < self.children.len() {
            &self.children[child_idx]
        } else {
            panic!("Index out of bounds in MethodExpression::get_child_at")
        }
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        if children.is_empty() {
            return Arc::new(Expression::Method(self.clone()));
        }

        let expr = children[0].clone();
        let args = children.iter().skip(1).cloned().collect();

        Arc::new(Expression::Method(MethodExpression::new(
            expr,
            self.method_name.clone(),
            args,
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate the expression on which the method is called
        self.expr.validate(schema)?;

        // Validate all arguments
        for arg in &self.args {
            arg.validate(schema)?;
        }

        // Additional validation based on method name
        match self.method_name.as_str() {
            "length" | "len" => {
                // Check that expr returns a string type
                let expr_type = self.expr.get_return_type().get_type();
                if expr_type != TypeId::VarChar && expr_type != TypeId::Char {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "length() method can only be called on string types, got {:?}",
                        expr_type
                    )));
                }

                // Check that there are no arguments
                if !self.args.is_empty() {
                    return Err(ExpressionError::InvalidOperation(
                        "length() method does not take arguments".to_string(),
                    ));
                }
            }

            "value" => {
                // Check that there are exactly 2 arguments
                if self.args.len() != 2 {
                    return Err(ExpressionError::InvalidOperation(
                        "value() method requires 2 arguments: path and return type".to_string(),
                    ));
                }

                // Check that both arguments are strings
                for (i, arg) in self.args.iter().enumerate() {
                    let arg_type = arg.get_return_type().get_type();
                    if arg_type != TypeId::VarChar && arg_type != TypeId::Char {
                        return Err(ExpressionError::InvalidOperation(format!(
                            "Argument {} to value() must be a string, got {:?}",
                            i + 1,
                            arg_type
                        )));
                    }
                }
            }

            "get" | "at" => {
                // Check that expr returns a vector type
                let expr_type = self.expr.get_return_type().get_type();
                if expr_type != TypeId::Vector {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "get() method can only be called on vector types, got {:?}",
                        expr_type
                    )));
                }

                // Check that there is exactly 1 argument
                if self.args.len() != 1 {
                    return Err(ExpressionError::InvalidOperation(
                        "get() method requires 1 index argument".to_string(),
                    ));
                }

                // Check that the argument is an integer type
                let arg_type = self.args[0].get_return_type().get_type();
                if arg_type != TypeId::Integer
                    && arg_type != TypeId::BigInt
                    && arg_type != TypeId::SmallInt
                    && arg_type != TypeId::TinyInt
                {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "Index for get() must be an integer type, got {:?}",
                        arg_type
                    )));
                }
            }

            "size" | "count" => {
                // Check that expr returns a vector type
                let expr_type = self.expr.get_return_type().get_type();
                if expr_type != TypeId::Vector {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "size() method can only be called on vector types, got {:?}",
                        expr_type
                    )));
                }

                // Check that there are no arguments
                if !self.args.is_empty() {
                    return Err(ExpressionError::InvalidOperation(
                        "size() method does not take arguments".to_string(),
                    ));
                }
            }

            // Add validation for other methods as needed
            _ => {
                // For unknown methods, we can either:
                // 1. Reject them outright
                // 2. Allow them but warn
                // 3. Allow them without warning

                // For now, we'll allow unknown methods but return a warning
                eprintln!("Warning: Unknown method '{}' being used", self.method_name);
            }
        }

        Ok(())
    }
}

impl Display for MethodExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}({})",
            self.expr,
            self.method_name,
            self.args
                .iter()
                .map(|a| a.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::type_id::TypeId;

    fn create_test_tuple() -> (Tuple, Schema) {
        let schema = Schema::new(vec![
            Column::new("str_col", TypeId::VarChar),
            Column::new("int_col", TypeId::Integer),
        ]);

        let values = vec![Value::new("test string"), Value::new(42)];

        let tuple = Tuple::new(&values, &schema, RID::new(1, 1));
        (tuple, schema)
    }

    #[test]
    fn test_string_length_method() {
        let (tuple, schema) = create_test_tuple();

        // Create a constant string expression
        let string_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("hello world"),
            Column::new("str", TypeId::VarChar),
            vec![],
        )));

        // Create a method expression for string.length()
        let method_expr = MethodExpression::new(
            string_expr,
            "length".to_string(),
            vec![],
            Column::new("length", TypeId::Integer),
        );

        // Evaluate the expression
        let result = method_expr.evaluate(&tuple, &schema).unwrap();

        // Verify the result is 11 (length of "hello world")
        match result.get_val() {
            Val::Integer(len) => assert_eq!(*len, 11),
            _ => panic!("Expected integer result for length method"),
        }
    }

    #[test]
    fn test_xml_value_method() {
        let (tuple, schema) = create_test_tuple();

        // Create a constant XML string expression
        let xml_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("<book>Database Systems</book>"),
            Column::new("xml", TypeId::VarChar),
            vec![],
        )));

        // Create path argument
        let path_arg = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("."),
            Column::new("path", TypeId::VarChar),
            vec![],
        )));

        // Create type argument
        let type_arg = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("VARCHAR"),
            Column::new("type", TypeId::VarChar),
            vec![],
        )));

        // Create a method expression for xml.value(path, type)
        let method_expr = MethodExpression::new(
            xml_expr,
            "value".to_string(),
            vec![path_arg, type_arg],
            Column::new("value", TypeId::VarChar),
        );

        // Evaluate the expression
        let result = method_expr.evaluate(&tuple, &schema).unwrap();

        // Verify the result is "Database Systems" (content between XML tags)
        match result.get_val() {
            Val::VarLen(s) => assert_eq!(s, "Database Systems"),
            _ => panic!("Expected string result for XML value method"),
        }
    }

    #[test]
    fn test_vector_get_method() {
        let (tuple, schema) = create_test_tuple();

        // Create a vector expression
        let vector = Value::new_vector(vec![
            Value::new("first"),
            Value::new("second"),
            Value::new("third"),
        ]);

        let vector_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            vector,
            Column::new("vec", TypeId::Vector),
            vec![],
        )));

        // Create index argument
        let index_arg = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1), // Get the second element (index 1)
            Column::new("index", TypeId::Integer),
            vec![],
        )));

        // Create a method expression for vector.get(index)
        let method_expr = MethodExpression::new(
            vector_expr,
            "get".to_string(),
            vec![index_arg],
            Column::new("element", TypeId::VarChar),
        );

        // Evaluate the expression
        let result = method_expr.evaluate(&tuple, &schema).unwrap();

        // Verify the result is "second" (element at index 1)
        match result.get_val() {
            Val::VarLen(s) => assert_eq!(s, "second"),
            _ => panic!("Expected string result for vector get method"),
        }
    }

    #[test]
    fn test_vector_size_method() {
        let (tuple, schema) = create_test_tuple();

        // Create a vector expression
        let vector = Value::new_vector(vec![
            Value::new("first"),
            Value::new("second"),
            Value::new("third"),
            Value::new("fourth"),
        ]);

        let vector_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            vector,
            Column::new("vec", TypeId::Vector),
            vec![],
        )));

        // Create a method expression for vector.size()
        let method_expr = MethodExpression::new(
            vector_expr,
            "size".to_string(),
            vec![],
            Column::new("size", TypeId::Integer),
        );

        // Evaluate the expression
        let result = method_expr.evaluate(&tuple, &schema).unwrap();

        // Verify the result is 4 (size of the vector)
        match result.get_val() {
            Val::Integer(size) => assert_eq!(*size, 4),
            _ => panic!("Expected integer result for vector size method"),
        }
    }

    #[test]
    fn test_invalid_method_call() {
        let (tuple, schema) = create_test_tuple();

        // Create a constant integer expression
        let int_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(42),
            Column::new("int", TypeId::Integer),
            vec![],
        )));

        // Try to call length() on an integer (should fail)
        let method_expr = MethodExpression::new(
            int_expr,
            "length".to_string(),
            vec![],
            Column::new("length", TypeId::Integer),
        );

        // Evaluate the expression - should return an error
        let result = method_expr.evaluate(&tuple, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_method_with_column_reference() {
        let (tuple, schema) = create_test_tuple();

        // Create a column reference expression for the string column
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0, // Index of str_col
            Column::new("str_col", TypeId::VarChar),
            vec![],
        )));

        // Create a method expression for column.length()
        let method_expr = MethodExpression::new(
            col_expr,
            "length".to_string(),
            vec![],
            Column::new("length", TypeId::Integer),
        );

        // Evaluate the expression
        let result = method_expr.evaluate(&tuple, &schema).unwrap();

        // Verify the result is 11 (length of "test string")
        match result.get_val() {
            Val::Integer(len) => assert_eq!(*len, 11),
            _ => panic!("Expected integer result for length method"),
        }
    }

    #[test]
    fn test_method_display() {
        // Create a constant string expression
        let string_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("hello"),
            Column::new("str", TypeId::VarChar),
            vec![],
        )));

        // Create arguments
        let arg1 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("arg1"),
            Column::new("arg1", TypeId::VarChar),
            vec![],
        )));

        let arg2 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(42),
            Column::new("arg2", TypeId::Integer),
            vec![],
        )));

        // Create a method expression with arguments
        let method_expr = MethodExpression::new(
            string_expr,
            "test_method".to_string(),
            vec![arg1, arg2],
            Column::new("result", TypeId::VarChar),
        );

        // Test the display implementation
        let display_str = method_expr.to_string();
        assert_eq!(display_str, "hello.test_method(arg1, 42)");
    }

    #[test]
    fn test_method_validate() {
        let schema = Schema::new(vec![
            Column::new("str_col", TypeId::VarChar),
            Column::new("vec_col", TypeId::Vector),
        ]);

        // Test valid string.length() validation
        let str_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("str_col", TypeId::VarChar),
            vec![],
        )));

        let length_method = MethodExpression::new(
            str_expr,
            "length".to_string(),
            vec![],
            Column::new("length", TypeId::Integer),
        );

        assert!(length_method.validate(&schema).is_ok());

        // Test invalid vector.length() validation (length only works on strings)
        let vec_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("vec_col", TypeId::Vector),
            vec![],
        )));

        let invalid_length = MethodExpression::new(
            vec_expr.clone(),
            "length".to_string(),
            vec![],
            Column::new("length", TypeId::Integer),
        );

        assert!(invalid_length.validate(&schema).is_err());

        // Test valid vector.size() validation
        let size_method = MethodExpression::new(
            vec_expr,
            "size".to_string(),
            vec![],
            Column::new("size", TypeId::Integer),
        );

        assert!(size_method.validate(&schema).is_ok());
    }

    #[test]
    fn test_method_evaluate_join() {
        // Create schemas for left and right relations
        let left_schema = Schema::new(vec![Column::new("left_str", TypeId::VarChar)]);

        let right_schema = Schema::new(vec![Column::new("right_str", TypeId::VarChar)]);

        // Create tuples
        let left_tuple = Tuple::new(&[Value::new("left value")], &left_schema, RID::new(1, 1));

        let right_tuple = Tuple::new(&[Value::new("right value")], &right_schema, RID::new(2, 1));

        // Create a column reference to the right relation
        let right_col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, // Right table index
            0, // Column index
            Column::new("right_str", TypeId::VarChar),
            vec![],
        )));

        // Create a method expression for right_str.length()
        let method_expr = MethodExpression::new(
            right_col_expr,
            "length".to_string(),
            vec![],
            Column::new("length", TypeId::Integer),
        );

        // Evaluate the join expression
        let result = method_expr
            .evaluate_join(&left_tuple, &left_schema, &right_tuple, &right_schema)
            .unwrap();

        // Verify the result is 11 (length of "right value")
        match result.get_val() {
            Val::Integer(len) => assert_eq!(*len, 11),
            _ => panic!("Expected integer result for length method in join"),
        }
    }
}
