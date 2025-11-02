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

/// Expression that finds the position of a substring within a string
/// Corresponds to SQL's POSITION(substring IN string) function
#[derive(Debug, Clone, PartialEq)]
pub struct PositionExpression {
    /// The substring to search for
    substring: Arc<Expression>,
    /// The string to search in
    string: Arc<Expression>,
    /// Return type (always an integer)
    return_type: Column,
    /// Child expressions
    children: Vec<Arc<Expression>>,
}

impl PositionExpression {
    pub fn new(substring: Arc<Expression>, string: Arc<Expression>) -> Self {
        let children = vec![substring.clone(), string.clone()];
        Self {
            substring,
            string,
            return_type: Column::new("position", TypeId::Integer),
            children,
        }
    }
}

impl ExpressionOps for PositionExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        // Evaluate the substring and string expressions
        let substring_val = self.substring.evaluate(tuple, schema)?;
        let string_val = self.string.evaluate(tuple, schema)?;

        // Handle NULL values
        if substring_val.is_null() || string_val.is_null() {
            return Ok(Value::new(Val::Null));
        }

        // Extract string values
        let substring_str = match substring_val.get_val() {
            Val::VarLen(s) | Val::ConstLen(s) => s,
            _ => {
                return Err(ExpressionError::InvalidOperation(
                    "POSITION substring must be a string".to_string(),
                ));
            }
        };

        let string_str = match string_val.get_val() {
            Val::VarLen(s) | Val::ConstLen(s) => s,
            _ => {
                return Err(ExpressionError::InvalidOperation(
                    "POSITION string must be a string".to_string(),
                ));
            }
        };

        // Find the position (1-indexed as per SQL standard)
        let position = match string_str.find(substring_str) {
            Some(pos) => (pos + 1) as i32, // Convert to 1-indexed
            None => 0,                     // Not found
        };

        Ok(Value::new(position))
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        // Evaluate the substring and string expressions
        let substring_val =
            self.substring
                .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        let string_val =
            self.string
                .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;

        // Handle NULL values
        if substring_val.is_null() || string_val.is_null() {
            return Ok(Value::new(Val::Null));
        }

        // Extract string values
        let substring_str = match substring_val.get_val() {
            Val::VarLen(s) | Val::ConstLen(s) => s,
            _ => {
                return Err(ExpressionError::InvalidOperation(
                    "POSITION substring must be a string".to_string(),
                ));
            }
        };

        let string_str = match string_val.get_val() {
            Val::VarLen(s) | Val::ConstLen(s) => s,
            _ => {
                return Err(ExpressionError::InvalidOperation(
                    "POSITION string must be a string".to_string(),
                ));
            }
        };

        // Find the position (1-indexed as per SQL standard)
        let position = match string_str.find(substring_str) {
            Some(pos) => (pos + 1) as i32, // Convert to 1-indexed
            None => 0,                     // Not found
        };

        Ok(Value::new(position))
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
        if children.len() != 2 {
            panic!("PositionExpression requires exactly 2 children");
        }
        Arc::new(Expression::Position(PositionExpression::new(
            children[0].clone(),
            children[1].clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate child expressions
        self.substring.validate(schema)?;
        self.string.validate(schema)?;

        // Ensure substring and string expressions return string types
        let substring_type = self.substring.get_return_type().get_type();
        let string_type = self.string.get_return_type().get_type();

        if !matches!(substring_type, TypeId::VarChar | TypeId::Char) {
            return Err(ExpressionError::InvalidOperation(format!(
                "POSITION substring must be a string type, got {:?}",
                substring_type
            )));
        }

        if !matches!(string_type, TypeId::VarChar | TypeId::Char) {
            return Err(ExpressionError::InvalidOperation(format!(
                "POSITION string must be a string type, got {:?}",
                string_type
            )));
        }

        Ok(())
    }
}

impl Display for PositionExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "POSITION({} IN {})", self.substring, self.string)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;

    #[test]
    fn test_position_expression() {
        // Create test expressions
        let substring = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("lo"),
            Column::new("substring", TypeId::VarChar),
            vec![],
        )));

        let string = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("Hello"),
            Column::new("string", TypeId::VarChar),
            vec![],
        )));

        let position_expr = PositionExpression::new(substring, string);

        // Create a schema and tuple for evaluation
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        // Evaluate the expression
        let result = position_expr.evaluate(&tuple, &schema).unwrap();

        // Position should be 4 (1-indexed)
        assert_eq!(result, Value::new(4));
    }

    #[test]
    fn test_position_not_found() {
        // Create test expressions
        let substring = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("xyz"),
            Column::new("substring", TypeId::VarChar),
            vec![],
        )));

        let string = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("Hello"),
            Column::new("string", TypeId::VarChar),
            vec![],
        )));

        let position_expr = PositionExpression::new(substring, string);

        // Create a schema and tuple for evaluation
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        // Evaluate the expression
        let result = position_expr.evaluate(&tuple, &schema).unwrap();

        // Position should be 0 (not found)
        assert_eq!(result, Value::new(0));
    }

    #[test]
    fn test_position_with_null() {
        // Create test expressions with NULL
        let substring = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Null),
            Column::new("substring", TypeId::VarChar),
            vec![],
        )));

        let string = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("Hello"),
            Column::new("string", TypeId::VarChar),
            vec![],
        )));

        let position_expr = PositionExpression::new(substring, string);

        // Create a schema and tuple for evaluation
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        // Evaluate the expression
        let result = position_expr.evaluate(&tuple, &schema).unwrap();

        // Result should be NULL
        assert!(result.is_null());
    }
}
