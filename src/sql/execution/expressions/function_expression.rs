use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use std::fmt::{self, Display};
use std::sync::Arc;

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

    /// Infer the return type of a function based on its name and arguments
    pub fn infer_return_type(
        func_name: &str,
        children: &[Arc<Expression>],
    ) -> Result<Column, String> {
        match func_name {
            "COUNT" => Ok(Column::new(func_name, TypeId::BigInt)),
            "SUM" | "AVG" => {
                if children.is_empty() {
                    return Err(format!("{} requires at least one argument", func_name));
                }
                let arg_type = children[0].get_return_type().get_type();
                match arg_type {
                    TypeId::Integer | TypeId::BigInt | TypeId::SmallInt | TypeId::TinyInt => {
                        Ok(Column::new(func_name, TypeId::BigInt))
                    }
                    TypeId::Decimal | TypeId::Float => Ok(Column::new(func_name, TypeId::Decimal)),
                    _ => Err(format!("Invalid argument type for {}", func_name)),
                }
            }
            "MIN" | "MAX" => {
                if children.is_empty() {
                    return Err(format!("{} requires at least one argument", func_name));
                }
                // Return type is same as input type
                Ok(Column::new(
                    func_name,
                    children[0].get_return_type().get_type(),
                ))
            }
            "LOWER" | "UPPER" | "TRIM" | "LTRIM" | "RTRIM" => {
                Ok(Column::new(func_name, TypeId::VarChar))
            }
            "SUBSTRING" => Ok(Column::new(func_name, TypeId::VarChar)),
            "LENGTH" | "POSITION" => Ok(Column::new(func_name, TypeId::Integer)),
            "ROUND" | "ABS" => {
                if children.is_empty() {
                    return Err(format!("{} requires at least one argument", func_name));
                }
                let arg_type = children[0].get_return_type().get_type();
                match arg_type {
                    TypeId::Integer | TypeId::BigInt | TypeId::SmallInt | TypeId::TinyInt => {
                        Ok(Column::new(func_name, arg_type))
                    }
                    TypeId::Decimal => Ok(Column::new(func_name, TypeId::Decimal)),
                    _ => Err(format!("Invalid argument type for {}", func_name)),
                }
            }
            "COALESCE" => {
                if children.is_empty() {
                    return Err("COALESCE requires at least one argument".to_string());
                }
                // Return type is the type of the first non-null argument
                Ok(Column::new(
                    func_name,
                    children[0].get_return_type().get_type(),
                ))
            }
            _ => Err(format!("Unknown function: {}", func_name)),
        }
    }
}

impl Display for FunctionExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
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
        // First evaluate all child expressions
        let mut child_values = Vec::new();
        for child in &self.children {
            child_values.push(child.evaluate(tuple, schema)?);
        }

        // Now evaluate the function based on its name
        match self.name.to_uppercase().as_str() {
            "LOWER" => {
                if child_values.len() != 1 {
                    return Err(ExpressionError::InvalidOperation(
                        "LOWER function requires exactly one argument".to_string(),
                    ));
                }

                let arg = &child_values[0];
                if arg.is_null() {
                    return Ok(Value::new(Val::Null));
                }

                match arg.get_val() {
                    Val::VarLen(s) | Val::ConstLen(s) => Ok(Value::new(s.to_lowercase())),
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
                    return Ok(Value::new(Val::Null));
                }

                match arg.get_val() {
                    Val::VarLen(s) | Val::ConstLen(s) => Ok(Value::new(s.to_uppercase())),
                    _ => Err(ExpressionError::InvalidOperation(
                        "UPPER function requires string argument".to_string(),
                    )),
                }
            }
            _ => Err(ExpressionError::InvalidOperation(format!(
                "Unsupported function: {}",
                self.name
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
        // For functions, we need to evaluate each child in the join context
        let mut child_values = Vec::new();
        for child in &self.children {
            child_values.push(child.evaluate_join(
                left_tuple,
                left_schema,
                right_tuple,
                right_schema,
            )?);
        }

        // Now evaluate the function based on its name
        match self.name.to_uppercase().as_str() {
            "LOWER" => {
                if child_values.len() != 1 {
                    return Err(ExpressionError::InvalidOperation(
                        "LOWER function requires exactly one argument".to_string(),
                    ));
                }

                let arg = &child_values[0];
                if arg.is_null() {
                    return Ok(Value::new(Val::Null));
                }

                match arg.get_val() {
                    Val::VarLen(s) | Val::ConstLen(s) => Ok(Value::new(s.to_lowercase())),
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
                    return Ok(Value::new(Val::Null));
                }

                match arg.get_val() {
                    Val::VarLen(s) | Val::ConstLen(s) => Ok(Value::new(s.to_uppercase())),
                    _ => Err(ExpressionError::InvalidOperation(
                        "UPPER function requires string argument".to_string(),
                    )),
                }
            }
            _ => Err(ExpressionError::InvalidOperation(format!(
                "Unsupported function: {}",
                self.name
            ))),
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
        // Validate all child expressions first
        for child in &self.children {
            child.validate(schema)?;
        }

        // Then validate function-specific requirements
        match self.name.to_uppercase().as_str() {
            "LOWER" | "UPPER" => {
                if self.children.len() != 1 {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "{} function requires exactly one argument",
                        self.name
                    )));
                }
                // Check that the argument is or can be converted to a string
                let child_type = self.children[0].get_return_type().get_type();
                if !matches!(child_type, TypeId::VarChar | TypeId::Char) {
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
                    return Err(ExpressionError::InvalidOperation(format!(
                        "{} function requires one or two arguments",
                        self.name
                    )));
                }

                // Check that the first argument is numeric
                let child_type = self.children[0].get_return_type().get_type();
                if !matches!(
                    child_type,
                    TypeId::Integer
                        | TypeId::BigInt
                        | TypeId::SmallInt
                        | TypeId::TinyInt
                        | TypeId::Decimal
                ) {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "{} function requires numeric argument",
                        self.name
                    )));
                }

                // If there's a second argument for ROUND, check that it's an integer
                if self.name.to_uppercase() == "ROUND" && self.children.len() > 1 {
                    let precision_type = self.children[1].get_return_type().get_type();
                    if !matches!(
                        precision_type,
                        TypeId::Integer | TypeId::BigInt | TypeId::SmallInt | TypeId::TinyInt
                    ) {
                        return Err(ExpressionError::InvalidOperation(
                            "ROUND function's second argument must be an integer".to_string(),
                        ));
                    }
                }
            }
            _ => {} // Other functions will be validated at runtime
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;

    fn create_test_tuple() -> (Tuple, Schema) {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("price", TypeId::Decimal),
        ]);

        let tuple = Tuple::new(
            &vec![Value::new(1), Value::new("test"), Value::new(10.5)],
            &schema,
            RID::new(0, 0),
        );

        (tuple, schema)
    }

    #[test]
    fn test_function_display() {
        let func = FunctionExpression::new(
            "LOWER".to_string(),
            vec![Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new("TEST"),
                Column::new("arg", TypeId::VarChar),
                vec![],
            )))],
            Column::new("result", TypeId::VarChar),
        );

        assert_eq!(func.to_string(), "LOWER(TEST)");
    }

    #[test]
    fn test_string_functions() {
        let (tuple, schema) = create_test_tuple();

        // Test LOWER function
        let lower_func = FunctionExpression::new(
            "LOWER".to_string(),
            vec![Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new("TEST"),
                Column::new("arg", TypeId::VarChar),
                vec![],
            )))],
            Column::new("result", TypeId::VarChar),
        );

        assert_eq!(
            lower_func.evaluate(&tuple, &schema).unwrap(),
            Value::new("test")
        );

        // Test UPPER function
        let upper_func = FunctionExpression::new(
            "UPPER".to_string(),
            vec![Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new("test"),
                Column::new("arg", TypeId::VarChar),
                vec![],
            )))],
            Column::new("result", TypeId::VarChar),
        );

        assert_eq!(
            upper_func.evaluate(&tuple, &schema).unwrap(),
            Value::new("TEST")
        );
    }
}
