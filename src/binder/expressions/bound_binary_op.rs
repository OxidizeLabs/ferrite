use crate::binder::bound_expression::{BoundExpression, ExpressionType};
use sqlparser::ast::BinaryOperator;
use std::any::Any;
use std::fmt;
use std::fmt::Display;

/// Represents a bound binary operator, e.g., `a+b`.
#[derive(Clone)]
pub struct BoundBinaryOp {
    op: BinaryOperator,
    left: Box<dyn BoundExpression>,
    right: Box<dyn BoundExpression>,
}


impl BoundBinaryOp {
    /// Creates a new BoundBinaryOp.
    pub fn new(op: &BinaryOperator, left: Box<dyn BoundExpression>, right: Box<dyn BoundExpression>) -> Self {
        Self { op: op.clone(), left, right }
    }
}

impl BoundExpression for BoundBinaryOp {
    fn expression_type(&self) -> ExpressionType {
        ExpressionType::BinaryOp
    }

    fn has_aggregation(&self) -> bool {
        self.left.has_aggregation() || self.right.has_aggregation()
    }

    fn has_window_function(&self) -> bool {
        self.left.has_window_function() || self.right.has_window_function()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_box(&self) -> Box<dyn BoundExpression> {
        Box::new(Self {
            op: self.op.clone(),
            left: self.left.clone_box(),
            right: self.right.clone_box(),
        })
    }
}

impl Display for BoundBinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} {} {})", self.left, self.op, self.right)
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[derive(Clone)]
    struct TestExpression(bool);

    impl BoundExpression for TestExpression {
        fn expression_type(&self) -> ExpressionType {
            ExpressionType::Constant
        }

        fn has_aggregation(&self) -> bool {
            self.0
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
    fn bound_binary_op() {
        let binary_op = BoundBinaryOp::new(
            &BinaryOperator::Plus,
            Box::new(TestExpression(false)),
            Box::new(TestExpression(true)),
        );

        assert_eq!(binary_op.expression_type(), ExpressionType::BinaryOp);
        assert!(binary_op.has_aggregation());
        assert_eq!(binary_op.to_string(), "(test_expr + test_expr)");

        let binary_op_no_agg = BoundBinaryOp::new(
            &BinaryOperator::Multiply,
            Box::new(TestExpression(false)),
            Box::new(TestExpression(false)),
        );

        assert!(!binary_op_no_agg.has_aggregation());
        assert_eq!(binary_op_no_agg.to_string(), "(test_expr * test_expr)");
    }
}