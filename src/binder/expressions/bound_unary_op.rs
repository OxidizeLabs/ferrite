use crate::binder::bound_expression::{BoundExpression, ExpressionType};
use sqlparser::ast::UnaryOperator;
use std::any::Any;
use std::fmt;
use std::fmt::Display;

/// Represents a bound unary operation, e.g., `-x`.
#[derive(Clone)]
pub struct BoundUnaryOp {
    op: UnaryOperator, // Assuming you have a UnaryOperator enum
    expr: Box<dyn BoundExpression>,
}

impl BoundUnaryOp {
    /// Creates a new BoundUnaryOp.
    pub fn new(op: UnaryOperator, expr: Box<dyn BoundExpression>) -> Self {
        Self { op, expr }
    }
}


impl BoundExpression for BoundUnaryOp {
    fn expression_type(&self) -> ExpressionType {
        ExpressionType::UnaryOp
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
            op: self.op.clone(),
            expr: self.expr.clone_box(),
        })
    }
}

impl Display for BoundUnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}{})", self.op, self.expr)
    }
}

#[cfg(test)]
mod unit_tests {
    use crate::binder::expressions::bound_constant::BoundConstant;
    use super::*;

    #[test]
    fn bound_unary_op() {
        let unary_op = BoundUnaryOp::new(
            UnaryOperator::Minus,
            Box::new(BoundConstant::new(42)),
        );

        assert_eq!(unary_op.expression_type(), ExpressionType::UnaryOp);
        assert!(!unary_op.has_aggregation());
        assert_eq!(unary_op.to_string(), "(-42)");

        let nested_unary_op = BoundUnaryOp::new(
            UnaryOperator::Not,
            Box::new(BoundUnaryOp::new(
                UnaryOperator::Minus,
                Box::new(BoundConstant::new(10)),
            )),
        );

        assert_eq!(nested_unary_op.to_string(), "(NOT(-10))");
    }
}