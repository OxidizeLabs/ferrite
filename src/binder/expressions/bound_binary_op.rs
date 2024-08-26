use std::fmt;

use crate::binder::bound_expression::{BoundExpression, ExpressionType};

/// Represents a bound binary operator, e.g., `a+b`.
pub struct BoundBinaryOp {
    /// Operator name.
    pub op_name: String,
    /// Left argument of the op.
    pub larg: Box<dyn BoundExpression>,
    /// Right argument of the op.
    pub rarg: Box<dyn BoundExpression>,
}

impl BoundBinaryOp {
    /// Creates a new BoundBinaryOp.
    pub fn new(op_name: String, larg: Box<dyn BoundExpression>, rarg: Box<dyn BoundExpression>) -> Self {
        Self { op_name, larg, rarg }
    }
}

impl BoundExpression for BoundBinaryOp {
    fn expression_type(&self) -> ExpressionType {
        ExpressionType::BinaryOp
    }

    fn has_aggregation(&self) -> bool {
        self.larg.has_aggregation() || self.rarg.has_aggregation()
    }
}

impl fmt::Display for BoundBinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}{}{})", self.larg, self.op_name, self.rarg)
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    struct TestExpression(bool);

    impl BoundExpression for TestExpression {
        fn expression_type(&self) -> ExpressionType {
            ExpressionType::Constant
        }

        fn has_aggregation(&self) -> bool {
            self.0
        }
    }

    impl fmt::Display for TestExpression {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "test_expr")
        }
    }

    #[test]
    fn bound_binary_op() {
        let binary_op = BoundBinaryOp::new(
            "+".to_string(),
            Box::new(TestExpression(false)),
            Box::new(TestExpression(true)),
        );

        assert_eq!(binary_op.expression_type(), ExpressionType::BinaryOp);
        assert!(binary_op.has_aggregation());
        assert_eq!(binary_op.to_string(), "(test_expr+test_expr)");

        let binary_op_no_agg = BoundBinaryOp::new(
            "*".to_string(),
            Box::new(TestExpression(false)),
            Box::new(TestExpression(false)),
        );

        assert!(!binary_op_no_agg.has_aggregation());
        assert_eq!(binary_op_no_agg.to_string(), "(test_expr*test_expr)");
    }
}