use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// Expression that represents the SQL SUBSTRING function
///
/// Syntax:
/// ```sql
/// SUBSTRING(expr FROM start [FOR length])
/// ```
/// or
/// ```sql
/// SUBSTRING(expr, start [, length])
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct SubstringExpression {
    /// The string expression to extract substring from
    string_expr: Arc<Expression>,

    /// The starting position (1-indexed in SQL standard)
    from_expr: Arc<Expression>,

    /// Optional length of the substring
    for_expr: Option<Arc<Expression>>,

    /// Return type of the expression (always a string type)
    return_type: Column,

    /// Child expressions
    children: Vec<Arc<Expression>>,
}

impl SubstringExpression {
    pub fn new(
        string_expr: Arc<Expression>,
        from_expr: Arc<Expression>,
        for_expr: Option<Arc<Expression>>,
    ) -> Self {
        let mut children = vec![string_expr.clone(), from_expr.clone()];
        if let Some(for_expr) = for_expr.clone() {
            children.push(for_expr);
        }

        // Return type is always the same type as the input string
        let return_type = string_expr.get_return_type().clone();

        Self {
            string_expr,
            from_expr,
            for_expr,
            return_type,
            children,
        }
    }
}

impl ExpressionOps for SubstringExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        // Evaluate the string expression
        let string_val = self.string_expr.evaluate(tuple, schema)?;
        if string_val.is_null() {
            return Ok(Value::new(Val::Null));
        }

        // Get the string value
        let string = match string_val.get_val() {
            Val::VarLen(s) | Val::ConstLen(s) => s.clone(),
            _ => {
                return Err(ExpressionError::InvalidOperation(format!(
                    "SUBSTRING requires string input, got {:?}",
                    string_val.get_type_id()
                )));
            }
        };

        // Evaluate the FROM expression (start position)
        let from_val = self.from_expr.evaluate(tuple, schema)?;
        if from_val.is_null() {
            return Ok(Value::new(Val::Null));
        }

        // Get the start position (1-indexed in SQL)
        let start_pos = match from_val.get_val() {
            Val::Integer(i) => *i,
            Val::BigInt(i) => *i as i32,
            Val::SmallInt(i) => *i as i32,
            Val::TinyInt(i) => *i as i32,
            _ => {
                return Err(ExpressionError::InvalidOperation(format!(
                    "SUBSTRING FROM requires numeric input, got {:?}",
                    from_val.get_type_id()
                )));
            }
        };

        // Convert to 0-indexed for Rust
        // If start_pos is negative, count from the end of the string
        let start_idx = if start_pos > 0 {
            (start_pos - 1) as usize
        } else if start_pos < 0 {
            let abs_pos = (-start_pos) as usize;
            if abs_pos > string.len() {
                0 // If negative position is beyond string start, start from beginning
            } else {
                string.len() - abs_pos
            }
        } else {
            0 // If start_pos is 0, start from beginning (SQL standard says this is invalid, but many DBs allow it)
        };

        // If start is beyond the end of the string, return empty string
        if start_idx >= string.len() {
            return Ok(Value::new(""));
        }

        // If FOR expression is provided, evaluate it
        if let Some(for_expr) = &self.for_expr {
            let for_val = for_expr.evaluate(tuple, schema)?;
            if for_val.is_null() {
                return Ok(Value::new(Val::Null));
            }

            // Get the length
            let length = match for_val.get_val() {
                Val::Integer(i) => *i,
                Val::BigInt(i) => *i as i32,
                Val::SmallInt(i) => *i as i32,
                Val::TinyInt(i) => *i as i32,
                _ => {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "SUBSTRING FOR requires numeric input, got {:?}",
                        for_val.get_type_id()
                    )));
                }
            };

            // If length is negative, return empty string (SQL standard behavior)
            if length <= 0 {
                return Ok(Value::new(""));
            }

            // Calculate end index (exclusive)
            let end_idx = std::cmp::min(start_idx + length as usize, string.len());

            // Extract substring
            let result = string[start_idx..end_idx].to_string();
            Ok(Value::new(result))
        } else {
            // If no FOR expression, return rest of string
            let result = string[start_idx..].to_string();
            Ok(Value::new(result))
        }
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        // Evaluate the string expression
        let string_val =
            self.string_expr
                .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        if string_val.is_null() {
            return Ok(Value::new(Val::Null));
        }

        // Get the string value
        let string = match string_val.get_val() {
            Val::VarLen(s) | Val::ConstLen(s) => s.clone(),
            _ => {
                return Err(ExpressionError::InvalidOperation(format!(
                    "SUBSTRING requires string input, got {:?}",
                    string_val.get_type_id()
                )));
            }
        };

        // Evaluate the FROM expression (start position)
        let from_val =
            self.from_expr
                .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        if from_val.is_null() {
            return Ok(Value::new(Val::Null));
        }

        // Get the start position (1-indexed in SQL)
        let start_pos = match from_val.get_val() {
            Val::Integer(i) => *i,
            Val::BigInt(i) => *i as i32,
            Val::SmallInt(i) => *i as i32,
            Val::TinyInt(i) => *i as i32,
            _ => {
                return Err(ExpressionError::InvalidOperation(format!(
                    "SUBSTRING FROM requires numeric input, got {:?}",
                    from_val.get_type_id()
                )));
            }
        };

        // Convert to 0-indexed for Rust
        // If start_pos is negative, count from the end of the string
        let start_idx = if start_pos > 0 {
            (start_pos - 1) as usize
        } else if start_pos < 0 {
            let abs_pos = (-start_pos) as usize;
            if abs_pos > string.len() {
                0 // If negative position is beyond string start, start from beginning
            } else {
                string.len() - abs_pos
            }
        } else {
            0 // If start_pos is 0, start from beginning (SQL standard says this is invalid, but many DBs allow it)
        };

        // If start is beyond the end of the string, return empty string
        if start_idx >= string.len() {
            return Ok(Value::new(""));
        }

        // If FOR expression is provided, evaluate it
        if let Some(for_expr) = &self.for_expr {
            let for_val =
                for_expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
            if for_val.is_null() {
                return Ok(Value::new(Val::Null));
            }

            // Get the length
            let length = match for_val.get_val() {
                Val::Integer(i) => *i,
                Val::BigInt(i) => *i as i32,
                Val::SmallInt(i) => *i as i32,
                Val::TinyInt(i) => *i as i32,
                _ => {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "SUBSTRING FOR requires numeric input, got {:?}",
                        for_val.get_type_id()
                    )));
                }
            };

            // If length is negative, return empty string (SQL standard behavior)
            if length <= 0 {
                return Ok(Value::new(""));
            }

            // Calculate end index (exclusive)
            let end_idx = std::cmp::min(start_idx + length as usize, string.len());

            // Extract substring
            let result = string[start_idx..end_idx].to_string();
            Ok(Value::new(result))
        } else {
            // If no FOR expression, return rest of string
            let result = string[start_idx..].to_string();
            Ok(Value::new(result))
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
        if children.len() < 2 || children.len() > 3 {
            panic!("SubstringExpression requires 2 or 3 children");
        }

        let for_expr = if children.len() == 3 {
            Some(children[2].clone())
        } else {
            None
        };

        Arc::new(Expression::Substring(SubstringExpression::new(
            children[0].clone(),
            children[1].clone(),
            for_expr,
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate the string expression
        self.string_expr.validate(schema)?;

        // Validate the FROM expression
        self.from_expr.validate(schema)?;

        // Validate the FOR expression if present
        if let Some(for_expr) = &self.for_expr {
            for_expr.validate(schema)?;
        }

        // Check that the string expression returns a string type
        let string_type = self.string_expr.get_return_type().get_type();
        if string_type != TypeId::VarChar && string_type != TypeId::Char {
            return Err(ExpressionError::InvalidOperation(format!(
                "SUBSTRING requires string input, got {:?}",
                string_type
            )));
        }

        // Check that the FROM expression returns a numeric type
        let from_type = self.from_expr.get_return_type().get_type();
        if !matches!(
            from_type,
            TypeId::TinyInt | TypeId::SmallInt | TypeId::Integer | TypeId::BigInt | TypeId::Decimal
        ) {
            return Err(ExpressionError::InvalidOperation(format!(
                "SUBSTRING FROM requires numeric input, got {:?}",
                from_type
            )));
        }

        // Check that the FOR expression returns a numeric type if present
        if let Some(for_expr) = &self.for_expr {
            let for_type = for_expr.get_return_type().get_type();
            if !matches!(
                for_type,
                TypeId::TinyInt
                    | TypeId::SmallInt
                    | TypeId::Integer
                    | TypeId::BigInt
                    | TypeId::Decimal
            ) {
                return Err(ExpressionError::InvalidOperation(format!(
                    "SUBSTRING FOR requires numeric input, got {:?}",
                    for_type
                )));
            }
        }

        Ok(())
    }
}

impl Display for SubstringExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SUBSTRING({} FROM {}", self.string_expr, self.from_expr)?;
        if let Some(for_expr) = &self.for_expr {
            write!(f, " FOR {}", for_expr)?;
        }
        write!(f, ")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;

    #[test]
    fn test_basic_substring() {
        // Test SUBSTRING('hello', 2, 3) -> 'ell'
        let string_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("hello"),
            Column::new("str", TypeId::VarChar),
            vec![],
        )));

        let from_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(2),
            Column::new("from", TypeId::Integer),
            vec![],
        )));

        let for_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(3),
            Column::new("for", TypeId::Integer),
            vec![],
        )));

        let substring_expr = SubstringExpression::new(string_expr, from_expr, Some(for_expr));

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], &schema, RID::new(0, 0));

        let result = substring_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.to_string(), "ell");
    }

    #[test]
    fn test_substring_without_length() {
        // Test SUBSTRING('hello', 3) -> 'llo'
        let string_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("hello"),
            Column::new("str", TypeId::VarChar),
            vec![],
        )));

        let from_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(3),
            Column::new("from", TypeId::Integer),
            vec![],
        )));

        let substring_expr = SubstringExpression::new(string_expr, from_expr, None);

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], &schema, RID::new(0, 0));

        let result = substring_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.to_string(), "llo");
    }

    #[test]
    fn test_substring_with_negative_position() {
        // Test SUBSTRING('hello', -2) -> 'lo'
        let string_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("hello"),
            Column::new("str", TypeId::VarChar),
            vec![],
        )));

        let from_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(-2),
            Column::new("from", TypeId::Integer),
            vec![],
        )));

        let substring_expr = SubstringExpression::new(string_expr, from_expr, None);

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], &schema, RID::new(0, 0));

        let result = substring_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.to_string(), "lo");
    }

    #[test]
    fn test_substring_with_out_of_bounds() {
        // Test SUBSTRING('hello', 10, 3) -> ''
        let string_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("hello"),
            Column::new("str", TypeId::VarChar),
            vec![],
        )));

        let from_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(10),
            Column::new("from", TypeId::Integer),
            vec![],
        )));

        let for_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(3),
            Column::new("for", TypeId::Integer),
            vec![],
        )));

        let substring_expr = SubstringExpression::new(string_expr, from_expr, Some(for_expr));

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], &schema, RID::new(0, 0));

        let result = substring_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.to_string(), "");
    }

    #[test]
    fn test_substring_with_negative_length() {
        // Test SUBSTRING('hello', 2, -1) -> ''
        let string_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("hello"),
            Column::new("str", TypeId::VarChar),
            vec![],
        )));

        let from_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(2),
            Column::new("from", TypeId::Integer),
            vec![],
        )));

        let for_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(-1),
            Column::new("for", TypeId::Integer),
            vec![],
        )));

        let substring_expr = SubstringExpression::new(string_expr, from_expr, Some(for_expr));

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], &schema, RID::new(0, 0));

        let result = substring_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.to_string(), "");
    }

    #[test]
    fn test_substring_with_null_input() {
        // Test SUBSTRING(NULL, 2, 3) -> NULL
        let string_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Null),
            Column::new("str", TypeId::VarChar),
            vec![],
        )));

        let from_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(2),
            Column::new("from", TypeId::Integer),
            vec![],
        )));

        let for_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(3),
            Column::new("for", TypeId::Integer),
            vec![],
        )));

        let substring_expr = SubstringExpression::new(string_expr, from_expr, Some(for_expr));

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], &schema, RID::new(0, 0));

        let result = substring_expr.evaluate(&tuple, &schema).unwrap();
        assert!(result.is_null());
    }
}
