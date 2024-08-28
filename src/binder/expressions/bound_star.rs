use crate::binder::bound_expression::{BoundExpression, ExpressionType};
use std::any::Any;
use std::fmt;
use std::fmt::Display;

/// Represents the star (*) in SELECT statements, e.g., `SELECT * FROM x`.
#[derive(Clone)]
pub struct BoundStar;

impl BoundStar {
    /// Creates a new BoundStar.
    pub fn new() -> Self {
        Self
    }
}

impl BoundExpression for BoundStar {
    fn expression_type(&self) -> ExpressionType {
        ExpressionType::Star
    }

    fn has_aggregation(&self) -> bool {
        panic!("`has_aggregation` should not have been called on `BoundStar`.")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_box(&self) -> Box<dyn BoundExpression> {
        Box::new(self.clone())
    }
}

impl Display for BoundStar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "*")
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn bound_star() {
        let star = BoundStar::new();
        assert_eq!(star.expression_type(), ExpressionType::Star);
        assert_eq!(star.to_string(), "*");
    }

    #[test]
    #[should_panic(expected = "`has_aggregation` should not have been called on `BoundStar`.")]
    fn bound_star_has_aggregation() {
        let star = BoundStar::new();
        star.has_aggregation();
    }
}