use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use std::sync::Arc;
use std::fmt::{self, Display};

#[derive(Clone, Debug, PartialEq)]
pub struct FunctionExpression {
    name: String,
    children: Vec<Arc<Expression>>,
    return_type: Column,
}

impl FunctionExpression {
    pub fn new(name: String, children: Vec<Arc<Expression>>, return_type: Column) -> Self {
        Self {
            name,
            children,
            return_type,
        }
    }
}

impl Display for FunctionExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name.to_uppercase())?;
        
        for (i, child) in self.children.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", child)?;
        }
        
        write!(f, ")")
    }
}

impl ExpressionOps for FunctionExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        // Evaluate all child expressions first
        let mut child_values = Vec::with_capacity(self.children.len());
        for child in &self.children {
            let value = child.evaluate(tuple, schema)?;
            child_values.push(value);
        }

        // Handle different function types based on name
        match self.name.to_uppercase().as_str() {
            "LOWER" => {
                if child_values.len() != 1 {
                    return Err(ExpressionError::InvalidOperation(
                        "LOWER function requires exactly one argument".to_string(),
                    ));
                }
                
                let arg = &child_values[0];
                if arg.is_null() {
                    return Ok(Value::new(crate::types_db::value::Val::Null));
                }
                
                match arg.get_val() {
                    crate::types_db::value::Val::VarLen(s) | crate::types_db::value::Val::ConstLen(s) => {
                        Ok(Value::new(s.to_lowercase()))
                    }
                    _ => Err(ExpressionError::InvalidOperation(
                        "LOWER function requires string argument".to_string(),
                    )),
                }
            }
            "UPPER" => {
                if child_values.len() != 1 {
                    return Err(ExpressionError::InvalidOperation(
                        "UPPER function requires exactly one argument".to_string(),
                    ));
                }
                
                let arg = &child_values[0];
                if arg.is_null() {
                    return Ok(Value::new(crate::types_db::value::Val::Null));
                }
                
                match arg.get_val() {
                    crate::types_db::value::Val::VarLen(s) | crate::types_db::value::Val::ConstLen(s) => {
                        Ok(Value::new(s.to_uppercase()))
                    }
                    _ => Err(ExpressionError::InvalidOperation(
                        "UPPER function requires string argument".to_string(),
                    )),
                }
            }
            "CONCAT" => {
                // Check if any argument is NULL
                if child_values.iter().any(|v| v.is_null()) {
                    return Ok(Value::new(crate::types_db::value::Val::Null));
                }
                
                let mut result = String::new();
                for value in child_values {
                    match value.get_val() {
                        crate::types_db::value::Val::VarLen(s) | crate::types_db::value::Val::ConstLen(s) => {
                            result.push_str(s);
                        }
                        _ => {
                            // Convert non-string values to string
                            result.push_str(&value.to_string());
                        }
                    }
                }
                Ok(Value::new(result))
            }
            "LENGTH" | "LEN" => {
                if child_values.len() != 1 {
                    return Err(ExpressionError::InvalidOperation(
                        "LENGTH function requires exactly one argument".to_string(),
                    ));
                }
                
                let arg = &child_values[0];
                if arg.is_null() {
                    return Ok(Value::new(crate::types_db::value::Val::Null));
                }
                
                match arg.get_val() {
                    crate::types_db::value::Val::VarLen(s) | crate::types_db::value::Val::ConstLen(s) => {
                        Ok(Value::new(s.len() as i32))
                    }
                    _ => Err(ExpressionError::InvalidOperation(
                        "LENGTH function requires string argument".to_string(),
                    )),
                }
            }
            "ABS" => {
                if child_values.len() != 1 {
                    return Err(ExpressionError::InvalidOperation(
                        "ABS function requires exactly one argument".to_string(),
                    ));
                }
                
                let arg = &child_values[0];
                if arg.is_null() {
                    return Ok(Value::new(crate::types_db::value::Val::Null));
                }
                
                match arg.get_val() {
                    crate::types_db::value::Val::Integer(i) => Ok(Value::new(i.abs())),
                    crate::types_db::value::Val::BigInt(i) => Ok(Value::new(i.abs())),
                    crate::types_db::value::Val::SmallInt(i) => Ok(Value::new(i.abs() as i16)),
                    crate::types_db::value::Val::TinyInt(i) => Ok(Value::new(i.abs() as i8)),
                    crate::types_db::value::Val::Decimal(f) => Ok(Value::new(f.abs())),
                    _ => Err(ExpressionError::InvalidOperation(
                        "ABS function requires numeric argument".to_string(),
                    )),
                }
            }
            "ROUND" => {
                if child_values.len() < 1 || child_values.len() > 2 {
                    return Err(ExpressionError::InvalidOperation(
                        "ROUND function requires one or two arguments".to_string(),
                    ));
                }
                
                let arg = &child_values[0];
                if arg.is_null() {
                    return Ok(Value::new(crate::types_db::value::Val::Null));
                }
                
                // Default precision is 0
                let precision = if child_values.len() > 1 {
                    match child_values[1].get_val() {
                        crate::types_db::value::Val::Integer(i) => *i,
                        crate::types_db::value::Val::BigInt(i) => *i as i32,
                        crate::types_db::value::Val::SmallInt(i) => *i as i32,
                        crate::types_db::value::Val::TinyInt(i) => *i as i32,
                        _ => 0,
                    }
                } else {
                    0
                };
                
                match arg.get_val() {
                    crate::types_db::value::Val::Decimal(f) => {
                        let factor = 10.0_f64.powi(precision);
                        Ok(Value::new((f * factor).round() / factor))
                    }
                    crate::types_db::value::Val::Integer(i) => Ok(Value::new(*i)),
                    crate::types_db::value::Val::BigInt(i) => Ok(Value::new(*i)),
                    crate::types_db::value::Val::SmallInt(i) => Ok(Value::new(*i)),
                    crate::types_db::value::Val::TinyInt(i) => Ok(Value::new(*i)),
                    _ => Err(ExpressionError::InvalidOperation(
                        "ROUND function requires numeric argument".to_string(),
                    )),
                }
            }
            "COALESCE" => {
                if child_values.is_empty() {
                    return Err(ExpressionError::InvalidOperation(
                        "COALESCE function requires at least one argument".to_string(),
                    ));
                }
                
                for value in &child_values {
                    if !value.is_null() {
                        return Ok(value.clone());
                    }
                }
                
                // All values were NULL
                Ok(Value::new(crate::types_db::value::Val::Null))
            }
            // Add more functions as needed
            _ => Err(ExpressionError::InvalidOperation(
                format!("Unsupported function: {}", self.name),
            )),
        }
    }

    fn evaluate_join(&self, left_tuple: &Tuple, left_schema: &Schema, right_tuple: &Tuple, right_schema: &Schema) -> Result<Value, ExpressionError> {
        // Evaluate all child expressions first
        let mut child_values = Vec::with_capacity(self.children.len());
        for child in &self.children {
            let value = child.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
            child_values.push(value);
        }

        // Reuse the same function evaluation logic as in evaluate()
        // Handle different function types based on name
        match self.name.to_uppercase().as_str() {
            "LOWER" => {
                if child_values.len() != 1 {
                    return Err(ExpressionError::InvalidOperation(
                        "LOWER function requires exactly one argument".to_string(),
                    ));
                }
                
                let arg = &child_values[0];
                if arg.is_null() {
                    return Ok(Value::new(crate::types_db::value::Val::Null));
                }
                
                match arg.get_val() {
                    crate::types_db::value::Val::VarLen(s) | crate::types_db::value::Val::ConstLen(s) => {
                        Ok(Value::new(s.to_lowercase()))
                    }
                    _ => Err(ExpressionError::InvalidOperation(
                        "LOWER function requires string argument".to_string(),
                    )),
                }
            }
            // Same implementations as in evaluate() for other functions
            "UPPER" => {
                if child_values.len() != 1 {
                    return Err(ExpressionError::InvalidOperation(
                        "UPPER function requires exactly one argument".to_string(),
                    ));
                }
                
                let arg = &child_values[0];
                if arg.is_null() {
                    return Ok(Value::new(crate::types_db::value::Val::Null));
                }
                
                match arg.get_val() {
                    crate::types_db::value::Val::VarLen(s) | crate::types_db::value::Val::ConstLen(s) => {
                        Ok(Value::new(s.to_uppercase()))
                    }
                    _ => Err(ExpressionError::InvalidOperation(
                        "UPPER function requires string argument".to_string(),
                    )),
                }
            }
            // Add other function implementations as in evaluate()
            _ => Err(ExpressionError::InvalidOperation(
                format!("Unsupported function: {}", self.name),
            )),
        }
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
        Arc::new(Expression::Function(FunctionExpression::new(
            self.name.clone(),
            children,
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate all child expressions
        for child in &self.children {
            child.validate(schema)?;
        }
        
        // Validate function-specific requirements
        match self.name.to_uppercase().as_str() {
            "LOWER" | "UPPER" | "LENGTH" | "LEN" => {
                if self.children.len() != 1 {
                    return Err(ExpressionError::InvalidOperation(
                        format!("{} function requires exactly one argument", self.name),
                    ));
                }
                
                // Check that the argument is or can be converted to a string
                let child_type = self.children[0].get_return_type().get_type();
                if !matches!(child_type, crate::types_db::type_id::TypeId::VarChar | crate::types_db::type_id::TypeId::Char) {
                    // We'll allow it but might want to add a warning
                }
            }
            "CONCAT" => {
                if self.children.is_empty() {
                    return Err(ExpressionError::InvalidOperation(
                        "CONCAT function requires at least one argument".to_string(),
                    ));
                }
            }
            "ABS" | "ROUND" => {
                if self.children.is_empty() || self.children.len() > 2 {
                    return Err(ExpressionError::InvalidOperation(
                        format!("{} function requires one or two arguments", self.name),
                    ));
                }
                
                // Check that the first argument is numeric
                let child_type = self.children[0].get_return_type().get_type();
                if !matches!(
                    child_type,
                    crate::types_db::type_id::TypeId::Integer
                        | crate::types_db::type_id::TypeId::BigInt
                        | crate::types_db::type_id::TypeId::SmallInt
                        | crate::types_db::type_id::TypeId::TinyInt
                        | crate::types_db::type_id::TypeId::Decimal
                ) {
                    return Err(ExpressionError::InvalidOperation(
                        format!("{} function requires numeric argument", self.name),
                    ));
                }
                
                // If there's a second argument for ROUND, check that it's an integer
                if self.name.to_uppercase() == "ROUND" && self.children.len() > 1 {
                    let precision_type = self.children[1].get_return_type().get_type();
                    if !matches!(
                        precision_type,
                        crate::types_db::type_id::TypeId::Integer
                            | crate::types_db::type_id::TypeId::BigInt
                            | crate::types_db::type_id::TypeId::SmallInt
                            | crate::types_db::type_id::TypeId::TinyInt
                    ) {
                        return Err(ExpressionError::InvalidOperation(
                            "ROUND function's second argument must be an integer".to_string(),
                        ));
                    }
                }
            }
            "COALESCE" => {
                if self.children.is_empty() {
                    return Err(ExpressionError::InvalidOperation(
                        "COALESCE function requires at least one argument".to_string(),
                    ));
                }
            }
            _ => {
                // For unsupported functions, we'll validate at runtime
            }
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::{Val, Value};

    fn create_test_tuple() -> (Tuple, Schema) {
        let schema = Schema::new(vec![
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
            Column::new("salary", TypeId::Decimal),
        ]);
        
        let values = vec![
            Value::new("John Doe"),
            Value::new(30),
            Value::new(50000.50),
        ];
        
        let tuple = Tuple::new(&values, schema.clone(), RID::new(1, 1));
        (tuple, schema)
    }

    #[test]
    fn test_function_display() {
        // Create constant expressions as children
        let const_str = Arc::new(Expression::Constant(
            ConstantExpression::new(Value::new("test"), Column::new("str", TypeId::VarChar), vec![])
        ));
        let const_int = Arc::new(Expression::Constant(
            ConstantExpression::new(Value::new(42), Column::new("int", TypeId::Integer), vec![])
        ));
        
        // Test single argument function
        let upper_func = FunctionExpression::new(
            "UPPER".to_string(),
            vec![const_str.clone()],
            Column::new("result", TypeId::VarChar),
        );
        assert_eq!(upper_func.to_string(), "UPPER(test)");
        
        // Test multiple argument function
        let concat_func = FunctionExpression::new(
            "CONCAT".to_string(),
            vec![const_str.clone(), const_int.clone()],
            Column::new("result", TypeId::VarChar),
        );
        assert_eq!(concat_func.to_string(), "CONCAT(test, 42)");
    }

    #[test]
    fn test_string_functions() {
        let (tuple, schema) = create_test_tuple();
        
        // Test UPPER function
        let name_const = Arc::new(Expression::Constant(
            ConstantExpression::new(Value::new("john"), Column::new("name", TypeId::VarChar), vec![])
        ));
        let upper_func = FunctionExpression::new(
            "UPPER".to_string(),
            vec![name_const],
            Column::new("result", TypeId::VarChar),
        );
        
        let result = upper_func.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::VarLen(s) => assert_eq!(s, "JOHN"),
            _ => panic!("Expected VarLen string"),
        }
        
        // Test LOWER function
        let upper_const = Arc::new(Expression::Constant(
            ConstantExpression::new(Value::new("HELLO"), Column::new("text", TypeId::VarChar), vec![])
        ));
        let lower_func = FunctionExpression::new(
            "LOWER".to_string(),
            vec![upper_const],
            Column::new("result", TypeId::VarChar),
        );
        
        let result = lower_func.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::VarLen(s) => assert_eq!(s, "hello"),
            _ => panic!("Expected VarLen string"),
        }
        
        // Test LENGTH function
        let str_const = Arc::new(Expression::Constant(
            ConstantExpression::new(Value::new("hello world"), Column::new("text", TypeId::VarChar), vec![])
        ));
        let len_func = FunctionExpression::new(
            "LENGTH".to_string(),
            vec![str_const],
            Column::new("result", TypeId::Integer),
        );
        
        let result = len_func.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::Integer(i) => assert_eq!(*i, 11),
            _ => panic!("Expected Integer"),
        }
    }

    #[test]
    fn test_numeric_functions() {
        let (tuple, schema) = create_test_tuple();
        
        // Test ABS function with negative number
        let neg_const = Arc::new(Expression::Constant(
            ConstantExpression::new(Value::new(-42), Column::new("num", TypeId::Integer), vec![])
        ));
        let abs_func = FunctionExpression::new(
            "ABS".to_string(),
            vec![neg_const],
            Column::new("result", TypeId::Integer),
        );
        
        let result = abs_func.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::Integer(i) => assert_eq!(*i, 42),
            _ => panic!("Expected Integer"),
        }
        
        // Test ROUND function with decimal
        let dec_const = Arc::new(Expression::Constant(
            ConstantExpression::new(Value::new(3.14159), Column::new("num", TypeId::Decimal), vec![])
        ));
        let precision_const = Arc::new(Expression::Constant(
            ConstantExpression::new(Value::new(2), Column::new("precision", TypeId::Integer), vec![])
        ));
        let round_func = FunctionExpression::new(
            "ROUND".to_string(),
            vec![dec_const, precision_const],
            Column::new("result", TypeId::Decimal),
        );
        
        let result = round_func.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::Decimal(d) => assert_eq!(*d, 3.14),
            _ => panic!("Expected Decimal"),
        }
    }

    #[test]
    fn test_coalesce_function() {
        let (tuple, schema) = create_test_tuple();
        
        // Test COALESCE with NULL as first argument
        let null_const = Arc::new(Expression::Constant(
            ConstantExpression::new(Value::new(Val::Null), Column::new("null", TypeId::Integer), vec![])
        ));
        let int_const = Arc::new(Expression::Constant(
            ConstantExpression::new(Value::new(42), Column::new("num", TypeId::Integer), vec![])
        ));
        let coalesce_func = FunctionExpression::new(
            "COALESCE".to_string(),
            vec![null_const, int_const],
            Column::new("result", TypeId::Integer),
        );
        
        let result = coalesce_func.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::Integer(i) => assert_eq!(*i, 42),
            _ => panic!("Expected Integer"),
        }
        
        // Test COALESCE with non-NULL as first argument
        let str_const = Arc::new(Expression::Constant(
            ConstantExpression::new(Value::new("hello"), Column::new("str", TypeId::VarChar), vec![])
        ));
        let fallback_const = Arc::new(Expression::Constant(
            ConstantExpression::new(Value::new("fallback"), Column::new("fallback", TypeId::VarChar), vec![])
        ));
        let coalesce_func = FunctionExpression::new(
            "COALESCE".to_string(),
            vec![str_const, fallback_const],
            Column::new("result", TypeId::VarChar),
        );
        
        let result = coalesce_func.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::VarLen(s) => assert_eq!(s, "hello"),
            _ => panic!("Expected VarLen string"),
        }
    }

    #[test]
    fn test_concat_function() {
        let (tuple, schema) = create_test_tuple();
        
        // Test CONCAT with string arguments
        let str1_const = Arc::new(Expression::Constant(
            ConstantExpression::new(Value::new("Hello, "), Column::new("str1", TypeId::VarChar), vec![])
        ));
        let str2_const = Arc::new(Expression::Constant(
            ConstantExpression::new(Value::new("World!"), Column::new("str2", TypeId::VarChar), vec![])
        ));
        let concat_func = FunctionExpression::new(
            "CONCAT".to_string(),
            vec![str1_const, str2_const],
            Column::new("result", TypeId::VarChar),
        );
        
        let result = concat_func.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::VarLen(s) => assert_eq!(s, "Hello, World!"),
            _ => panic!("Expected VarLen string"),
        }
        
        // Test CONCAT with mixed type arguments
        let str_const = Arc::new(Expression::Constant(
            ConstantExpression::new(Value::new("The answer is "), Column::new("str", TypeId::VarChar), vec![])
        ));
        let int_const = Arc::new(Expression::Constant(
            ConstantExpression::new(Value::new(42), Column::new("num", TypeId::Integer), vec![])
        ));
        let concat_func = FunctionExpression::new(
            "CONCAT".to_string(),
            vec![str_const, int_const],
            Column::new("result", TypeId::VarChar),
        );
        
        let result = concat_func.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::VarLen(s) => assert_eq!(s, "The answer is 42"),
            _ => panic!("Expected VarLen string"),
        }
    }

    #[test]
    fn test_function_validation() {
        let schema = Schema::new(vec![
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ]);
        
        // Valid UPPER function
        let str_const = Arc::new(Expression::Constant(
            ConstantExpression::new(Value::new("test"), Column::new("str", TypeId::VarChar), vec![])
        ));
        let upper_func = FunctionExpression::new(
            "UPPER".to_string(),
            vec![str_const],
            Column::new("result", TypeId::VarChar),
        );
        assert!(upper_func.validate(&schema).is_ok());
        
        // Invalid UPPER function (no arguments)
        let invalid_upper = FunctionExpression::new(
            "UPPER".to_string(),
            vec![],
            Column::new("result", TypeId::VarChar),
        );
        assert!(invalid_upper.validate(&schema).is_err());
        
        // Valid ABS function
        let int_const = Arc::new(Expression::Constant(
            ConstantExpression::new(Value::new(-10), Column::new("num", TypeId::Integer), vec![])
        ));
        let abs_func = FunctionExpression::new(
            "ABS".to_string(),
            vec![int_const],
            Column::new("result", TypeId::Integer),
        );
        assert!(abs_func.validate(&schema).is_ok());
        
        // Invalid ABS function (string argument)
        let str_const = Arc::new(Expression::Constant(
            ConstantExpression::new(Value::new("test"), Column::new("str", TypeId::VarChar), vec![])
        ));
        let invalid_abs = FunctionExpression::new(
            "ABS".to_string(),
            vec![str_const],
            Column::new("result", TypeId::Integer),
        );
        assert!(invalid_abs.validate(&schema).is_err());
    }
} 