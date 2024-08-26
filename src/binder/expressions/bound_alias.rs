use std::fmt;

use crate::binder::bound_expression::{BoundExpression, ExpressionType};

/// Represents an alias in SELECT list, e.g., `SELECT count(x) AS y`, where `y` is an alias.
pub struct BoundAlias {
    /// Alias name.
    pub alias: String,
    /// The actual expression.
    pub child: Box<dyn BoundExpression>,
}

impl BoundAlias {
    /// Creates a new BoundAlias.
    pub fn new(alias: String, child: Box<dyn BoundExpression>) -> Self {
        Self { alias, child }
    }
}

impl BoundExpression for BoundAlias {
    fn expression_type(&self) -> ExpressionType {
        ExpressionType::Alias
    }

    fn has_aggregation(&self) -> bool {
        self.child.has_aggregation()
    }

    fn has_window_function(&self) -> bool {
        self.child.has_window_function()
    }
}

impl fmt::Display for BoundAlias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} as {})", self.child, self.alias)
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    struct TestExpression;

    impl BoundExpression for TestExpression {
        fn expression_type(&self) -> ExpressionType {
            ExpressionType::Constant
        }

        fn has_aggregation(&self) -> bool {
            true
        }

        fn has_window_function(&self) -> bool {
            false
        }
    }

    impl fmt::Display for TestExpression {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "test_expr")
        }
    }

    #[test]
    fn bound_alias() {
        let alias = BoundAlias::new("y".to_string(), Box::new(TestExpression));

        assert_eq!(alias.expression_type(), ExpressionType::Alias);
        assert!(alias.has_aggregation());
        assert!(!alias.has_window_function());
        assert_eq!(alias.to_string(), "(test_expr as y)");
    }
}