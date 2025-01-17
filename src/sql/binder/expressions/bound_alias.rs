use crate::sql::binder::bound_expression::{BoundExpression, ExpressionType};
use std::any::Any;
use std::fmt;
use std::fmt::Display;

/// Represents an alias in SELECT list, e.g., `SELECT count(x) AS y`, where `y` is an alias.
#[derive(Clone)]
pub struct BoundAlias {
    /// Alias name.
    pub alias: String,
    /// The actual expression.
    pub expr: Box<dyn BoundExpression>,
}

impl BoundAlias {
    /// Creates a new BoundAlias.
    pub fn new(alias: String, expr: Box<dyn BoundExpression>) -> Self {
        Self { alias, expr }
    }
}

impl BoundExpression for BoundAlias {
    fn expression_type(&self) -> ExpressionType {
        ExpressionType::Alias
    }

    fn has_aggregation(&self) -> bool {
        self.expr.has_aggregation()
    }

    fn has_window_function(&self) -> bool {
        self.expr.has_window_function()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_box(&self) -> Box<dyn BoundExpression> {
        Box::new(Self {
            expr: self.expr.clone_box(),
            alias: self.alias.clone(),
        })
    }
}

impl Display for BoundAlias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} AS {}", self.expr, self.alias)
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[derive(Clone)]
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

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn clone_box(&self) -> Box<dyn BoundExpression> {
            Box::new(self.clone())
        }
    }

    impl Display for TestExpression {
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
        assert_eq!(alias.to_string(), "test_expr AS y");
    }
}
