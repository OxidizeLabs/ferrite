use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub enum TrimType {
    Both,
    Leading,
    Trailing,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TrimExpression {
    trim_type: TrimType,
    // First child is the string to trim, second child (optional) is the characters to trim
    children: Vec<Arc<Expression>>,
    return_type: Column,
}

impl TrimExpression {
    pub fn new(trim_type: TrimType, children: Vec<Arc<Expression>>, return_type: Column) -> Self {
        Self {
            trim_type,
            children,
            return_type,
        }
    }
}

impl ExpressionOps for TrimExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let string_value = self.children[0].evaluate(tuple, schema)?;

        // If the input is NULL, the result is NULL
        if string_value.is_null() {
            return Ok(Value::new(crate::types_db::value::Val::Null));
        }

        // Get the string to trim
        let string_to_trim = match string_value.get_val() {
            crate::types_db::value::Val::VarLen(s) | crate::types_db::value::Val::ConstLen(s) => {
                s.clone()
            }
            _ => {
                return Err(ExpressionError::InvalidOperation(format!(
                    "TRIM requires string input, got {:?}",
                    string_value.get_type_id()
                )))
            }
        };

        // Get the characters to trim (if specified)
        let chars_to_trim = if self.children.len() > 1 {
            let chars_value = self.children[1].evaluate(tuple, schema)?;

            // If the trim characters are NULL, the result is NULL
            if chars_value.is_null() {
                return Ok(Value::new(crate::types_db::value::Val::Null));
            }

            match chars_value.get_val() {
                crate::types_db::value::Val::VarLen(s)
                | crate::types_db::value::Val::ConstLen(s) => s.clone(),
                _ => {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "TRIM characters must be a string, got {:?}",
                        chars_value.get_type_id()
                    )))
                }
            }
        } else {
            // Default is to trim whitespace
            " \t\n\r".to_string()
        };

        // Perform the trim operation based on the trim type
        let result = match self.trim_type {
            TrimType::Both => string_to_trim
                .trim_matches(|c| chars_to_trim.contains(c))
                .to_string(),
            TrimType::Leading => string_to_trim
                .trim_start_matches(|c| chars_to_trim.contains(c))
                .to_string(),
            TrimType::Trailing => string_to_trim
                .trim_end_matches(|c| chars_to_trim.contains(c))
                .to_string(),
        };

        Ok(Value::new(result))
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        let string_value =
            self.children[0].evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;

        // If the input is NULL, the result is NULL
        if string_value.is_null() {
            return Ok(Value::new(crate::types_db::value::Val::Null));
        }

        // Get the string to trim
        let string_to_trim = match string_value.get_val() {
            crate::types_db::value::Val::VarLen(s) | crate::types_db::value::Val::ConstLen(s) => {
                s.clone()
            }
            _ => {
                return Err(ExpressionError::InvalidOperation(format!(
                    "TRIM requires string input, got {:?}",
                    string_value.get_type_id()
                )))
            }
        };

        // Get the characters to trim (if specified)
        let chars_to_trim = if self.children.len() > 1 {
            let chars_value = self.children[1].evaluate_join(
                left_tuple,
                left_schema,
                right_tuple,
                right_schema,
            )?;

            // If the trim characters are NULL, the result is NULL
            if chars_value.is_null() {
                return Ok(Value::new(crate::types_db::value::Val::Null));
            }

            match chars_value.get_val() {
                crate::types_db::value::Val::VarLen(s)
                | crate::types_db::value::Val::ConstLen(s) => s.clone(),
                _ => {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "TRIM characters must be a string, got {:?}",
                        chars_value.get_type_id()
                    )))
                }
            }
        } else {
            // Default is to trim whitespace
            " \t\n\r".to_string()
        };

        // Perform the trim operation based on the trim type
        let result = match self.trim_type {
            TrimType::Both => string_to_trim
                .trim_matches(|c| chars_to_trim.contains(c))
                .to_string(),
            TrimType::Leading => string_to_trim
                .trim_start_matches(|c| chars_to_trim.contains(c))
                .to_string(),
            TrimType::Trailing => string_to_trim
                .trim_end_matches(|c| chars_to_trim.contains(c))
                .to_string(),
        };

        Ok(Value::new(result))
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
        Arc::new(Expression::Trim(TrimExpression::new(
            self.trim_type.clone(),
            children,
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate that we have at least one child (the string to trim)
        if self.children.is_empty() {
            return Err(ExpressionError::InvalidOperation(
                "TRIM expression requires at least one child".to_string(),
            ));
        }

        // Validate that the first child returns a string type
        let child_type = self.children[0].get_return_type().get_type();
        if !matches!(
            child_type,
            crate::types_db::type_id::TypeId::VarChar | crate::types_db::type_id::TypeId::Char
        ) {
            return Err(ExpressionError::InvalidOperation(format!(
                "TRIM requires string input, got {:?}",
                child_type
            )));
        }

        // If we have a second child, validate that it returns a string type
        if self.children.len() > 1 {
            let chars_type = self.children[1].get_return_type().get_type();
            if !matches!(
                chars_type,
                crate::types_db::type_id::TypeId::VarChar | crate::types_db::type_id::TypeId::Char
            ) {
                return Err(ExpressionError::InvalidOperation(format!(
                    "TRIM characters must be a string, got {:?}",
                    chars_type
                )));
            }
        }

        // Validate all children
        for child in &self.children {
            child.validate(schema)?;
        }

        Ok(())
    }
}

impl Display for TrimExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.trim_type {
            TrimType::Both => write!(f, "TRIM({})", self.children[0]),
            TrimType::Leading => write!(f, "LTRIM({})", self.children[0]),
            TrimType::Trailing => write!(f, "RTRIM({})", self.children[0]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::schema::Schema;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::storage::table::tuple::Tuple;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Val;

    fn create_test_tuple() -> (Tuple, Schema) {
        let schema = Schema::new(vec![
            Column::new("col1", TypeId::VarChar),
            Column::new("col2", TypeId::VarChar),
            Column::new("col3", TypeId::VarChar),
        ]);

        let values = vec![
            Value::new("  hello  "),
            Value::new("xxxhelloxxx"),
            Value::new(Val::Null),
        ];

        let tuple = Tuple::new(&values, schema.clone(), RID::new(0, 0));
        (tuple, schema)
    }

    #[test]
    fn test_trim_both() {
        let (tuple, schema) = create_test_tuple();

        // Create a column reference expression for the first column
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("col1", TypeId::VarChar),
            vec![],
        )));

        // Create a TRIM expression to trim whitespace from both ends
        let trim_expr = TrimExpression::new(
            TrimType::Both,
            vec![col_expr],
            Column::new("result", TypeId::VarChar),
        );

        // Evaluate the expression
        let result = trim_expr.evaluate(&tuple, &schema).unwrap();

        // Check that whitespace was trimmed from both ends
        assert_eq!(result.to_string(), "hello");
    }

    #[test]
    fn test_trim_leading() {
        let (tuple, schema) = create_test_tuple();

        // Create a column reference expression for the first column
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("col1", TypeId::VarChar),
            vec![],
        )));

        // Create a TRIM expression to trim whitespace from the start
        let trim_expr = TrimExpression::new(
            TrimType::Leading,
            vec![col_expr],
            Column::new("result", TypeId::VarChar),
        );

        // Evaluate the expression
        let result = trim_expr.evaluate(&tuple, &schema).unwrap();

        // Check that whitespace was trimmed from the start only
        assert_eq!(result.to_string(), "hello  ");
    }

    #[test]
    fn test_trim_trailing() {
        let (tuple, schema) = create_test_tuple();

        // Create a column reference expression for the first column
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("col1", TypeId::VarChar),
            vec![],
        )));

        // Create a TRIM expression to trim whitespace from the end
        let trim_expr = TrimExpression::new(
            TrimType::Trailing,
            vec![col_expr],
            Column::new("result", TypeId::VarChar),
        );

        // Evaluate the expression
        let result = trim_expr.evaluate(&tuple, &schema).unwrap();

        // Check that whitespace was trimmed from the end only
        assert_eq!(result.to_string(), "  hello");
    }

    #[test]
    fn test_trim_specific_chars() {
        let (tuple, schema) = create_test_tuple();

        // Create a column reference expression for the second column
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("col2", TypeId::VarChar),
            vec![],
        )));

        // Create a constant expression for the characters to trim
        let chars_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("x"),
            Column::new("chars", TypeId::VarChar),
            vec![],
        )));

        // Create a TRIM expression to trim 'x' characters from both ends
        let trim_expr = TrimExpression::new(
            TrimType::Both,
            vec![col_expr, chars_expr],
            Column::new("result", TypeId::VarChar),
        );

        // Evaluate the expression
        let result = trim_expr.evaluate(&tuple, &schema).unwrap();

        // Check that 'x' characters were trimmed from both ends
        assert_eq!(result.to_string(), "hello");
    }

    #[test]
    fn test_trim_null_input() {
        let (tuple, schema) = create_test_tuple();

        // Create a column reference expression for the third column (NULL)
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2,
            Column::new("col3", TypeId::VarChar),
            vec![],
        )));

        // Create a TRIM expression
        let trim_expr = TrimExpression::new(
            TrimType::Both,
            vec![col_expr],
            Column::new("result", TypeId::VarChar),
        );

        // Evaluate the expression
        let result = trim_expr.evaluate(&tuple, &schema).unwrap();

        // Check that the result is NULL
        assert!(result.is_null());
    }

    #[test]
    fn test_trim_null_chars() {
        let (tuple, schema) = create_test_tuple();

        // Create a column reference expression for the first column
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("col1", TypeId::VarChar),
            vec![],
        )));

        // Create a NULL constant expression for the characters to trim
        let chars_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Null),
            Column::new("chars", TypeId::VarChar),
            vec![],
        )));

        // Create a TRIM expression
        let trim_expr = TrimExpression::new(
            TrimType::Both,
            vec![col_expr, chars_expr],
            Column::new("result", TypeId::VarChar),
        );

        // Evaluate the expression
        let result = trim_expr.evaluate(&tuple, &schema).unwrap();

        // Check that the result is NULL
        assert!(result.is_null());
    }

    #[test]
    fn test_trim_empty_string() {
        let schema = Schema::new(vec![Column::new("col1", TypeId::VarChar)]);
        let values = vec![Value::new("")];
        let tuple = Tuple::new(&values, schema.clone(), RID::new(0, 0));

        // Create a column reference expression
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("col1", TypeId::VarChar),
            vec![],
        )));

        // Create a TRIM expression
        let trim_expr = TrimExpression::new(
            TrimType::Both,
            vec![col_expr],
            Column::new("result", TypeId::VarChar),
        );

        // Evaluate the expression
        let result = trim_expr.evaluate(&tuple, &schema).unwrap();

        // Check that the result is an empty string
        assert_eq!(result.to_string(), "");
    }

    #[test]
    fn test_trim_multiple_chars() {
        let schema = Schema::new(vec![Column::new("col1", TypeId::VarChar)]);
        let values = vec![Value::new("abc123abc")];
        let tuple = Tuple::new(&values, schema.clone(), RID::new(0, 0));

        // Create a column reference expression
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("col1", TypeId::VarChar),
            vec![],
        )));

        // Create a constant expression for multiple characters to trim
        let chars_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("abc"),
            Column::new("chars", TypeId::VarChar),
            vec![],
        )));

        // Create a TRIM expression
        let trim_expr = TrimExpression::new(
            TrimType::Both,
            vec![col_expr, chars_expr],
            Column::new("result", TypeId::VarChar),
        );

        // Evaluate the expression
        let result = trim_expr.evaluate(&tuple, &schema).unwrap();

        // Check that all specified characters were trimmed
        assert_eq!(result.to_string(), "123");
    }

    #[test]
    fn test_display() {
        // Test the Display implementation for TrimExpression
        let col_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("col1", TypeId::VarChar),
            vec![],
        )));

        let trim_both = TrimExpression::new(
            TrimType::Both,
            vec![col_expr.clone()],
            Column::new("result", TypeId::VarChar),
        );

        let trim_leading = TrimExpression::new(
            TrimType::Leading,
            vec![col_expr.clone()],
            Column::new("result", TypeId::VarChar),
        );

        let trim_trailing = TrimExpression::new(
            TrimType::Trailing,
            vec![col_expr.clone()],
            Column::new("result", TypeId::VarChar),
        );

        assert_eq!(trim_both.to_string(), "TRIM(col1)");
        assert_eq!(trim_leading.to_string(), "LTRIM(col1)");
        assert_eq!(trim_trailing.to_string(), "RTRIM(col1)");
    }
}
