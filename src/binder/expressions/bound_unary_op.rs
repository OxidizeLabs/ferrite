use std::fmt;

use crate::binder::bound_expression::{BoundExpression, ExpressionType};
use crate::binder::expressions::bound_constant::BoundConstant;

/// Represents a bound unary operation, e.g., `-x`.
pub struct BoundUnaryOp {
    /// Operator name.
    pub op_name: String,
    /// Argument of the op.
    pub arg: Box<dyn BoundExpression>,
}

impl BoundUnaryOp {
    /// Creates a new BoundUnaryOp.
    pub fn new(op_name: String, arg: Box<dyn BoundExpression>) -> Self {
        Self { op_name, arg }
    }
}

impl BoundExpression for BoundUnaryOp {
    fn expression_type(&self) -> ExpressionType {
        ExpressionType::UnaryOp
    }

    fn has_aggregation(&self) -> bool {
        self.arg.has_aggregation()
    }
}

impl fmt::Display for BoundUnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}{})", self.op_name, self.arg)
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    struct TestExpression(bool);

    #[test]
    fn bound_unary_op() {
        let unary_op = BoundUnaryOp::new(
            "-".to_string(),
            Box::new(BoundConstant::new(42)),
        );

        assert_eq!(unary_op.expression_type(), ExpressionType::UnaryOp);
        assert!(!unary_op.has_aggregation());
        assert_eq!(unary_op.to_string(), "(-42)");

        let nested_unary_op = BoundUnaryOp::new(
            "NOT".to_string(),
            Box::new(BoundUnaryOp::new(
                "-".to_string(),
                Box::new(BoundConstant::new(10)),
            )),
        );

        assert_eq!(nested_unary_op.to_string(), "(NOT(-10))");
    }
}