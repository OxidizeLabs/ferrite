use crate::binder::bound_expression::{BoundExpression, ExpressionType};
use std::any::Any;
use std::fmt;
use std::fmt::Display;

/// Represents a bound column reference, e.g., `y.x` in the SELECT list.
#[derive(Debug, Clone)]
pub struct BoundColumnRef {
    /// The name of the column.
    col_name: Vec<String>,
}

impl BoundColumnRef {
    /// Creates a new BoundColumnRef.
    pub fn new(col_names: Vec<String>) -> Self {
        Self { col_name: col_names }
    }

    /// Prepends a prefix to the column name.
    pub fn prepend(mut self, prefix: String) -> Self {
        let mut new_col_name = vec![prefix];
        new_col_name.append(&mut self.col_name);
        Self { col_name: new_col_name }
    }
}

impl BoundExpression for BoundColumnRef {
    fn expression_type(&self) -> ExpressionType {
        ExpressionType::ColumnRef
    }

    fn has_aggregation(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_box(&self) -> Box<dyn BoundExpression> {
        Box::new(self.clone())
    }
}

impl Display for BoundColumnRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.col_name.join("."))
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn bound_column_ref() {
        let col_ref = BoundColumnRef::new(vec!["y".to_string(), "x".to_string()]);

        assert_eq!(col_ref.expression_type(), ExpressionType::ColumnRef);
        assert!(!col_ref.has_aggregation());
        assert_eq!(col_ref.to_string(), "y.x");

        let prepended_col_ref = col_ref.prepend("z".to_string());
        assert_eq!(prepended_col_ref.to_string(), "z.y.x");
    }
}