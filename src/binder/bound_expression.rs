use std::any::Any;
use std::fmt;
use std::fmt::Display;

/// Represents different types of expressions in the binder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpressionType {
    Invalid,
    Constant,
    ColumnRef,
    TypeCast,
    Function,
    AggCall,
    Star,
    UnaryOp,
    BinaryOp,
    Alias,
    FuncCall,
    Window,
    GroupingSets,
    Cube,
    Rollup,
    MockExpression,
}

/// Trait for bound expressions.
pub trait BoundExpression: Display {
    /// Returns the type of this expression.
    fn expression_type(&self) -> ExpressionType;

    /// Checks if the expression is invalid.
    fn is_invalid(&self) -> bool {
        self.expression_type() == ExpressionType::Invalid
    }

    /// Checks if the expression has aggregation.
    fn has_aggregation(&self) -> bool {
        panic!("has_aggregation should have been implemented!")
    }

    /// Checks if the expression has a window function.
    fn has_window_function(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn Any;

    fn clone_box(&self) -> Box<dyn BoundExpression>;
}

/// A default implementation for BoundExpression that can be used as a base
/// for concrete expression types.
#[derive(Debug, Clone)]
pub struct DefaultBoundExpression {
    expression_type: ExpressionType,
}

impl DefaultBoundExpression {
    /// Creates a new DefaultBoundExpression with the given ExpressionType.
    pub fn new(expression_type: ExpressionType) -> Self {
        Self { expression_type }
    }
}

impl BoundExpression for DefaultBoundExpression {
    fn expression_type(&self) -> ExpressionType {
        self.expression_type
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_box(&self) -> Box<dyn BoundExpression> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn BoundExpression> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

impl Display for DefaultBoundExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DefaultBoundExpression({})", self.expression_type)
    }
}

impl Display for ExpressionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            ExpressionType::Invalid => "Invalid",
            ExpressionType::Constant => "Constant",
            ExpressionType::ColumnRef => "ColumnRef",
            ExpressionType::TypeCast => "TypeCast",
            ExpressionType::Function => "Function",
            ExpressionType::AggCall => "AggregationCall",
            ExpressionType::Star => "Star",
            ExpressionType::UnaryOp => "UnaryOperation",
            ExpressionType::BinaryOp => "BinaryOperation",
            ExpressionType::Alias => "Alias",
            ExpressionType::FuncCall => "FuncCall",
            ExpressionType::Window => "Window",
            ExpressionType::GroupingSets => "GroupingSets",
            ExpressionType::Cube => "Cub",
            ExpressionType::Rollup => "Rollup",
            ExpressionType::MockExpression => "MockExpression",
        };
        write!(f, "{}", name)
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    // Custom bound expression implementation for testing
    #[derive(Debug, Clone)]
    struct TestBoundExpression {
        expr_type: ExpressionType,
        has_agg: bool,
    }

    impl TestBoundExpression {
        fn new(expr_type: ExpressionType, has_agg: bool) -> Self {
            Self { expr_type, has_agg }
        }
    }

    impl BoundExpression for TestBoundExpression {
        fn expression_type(&self) -> ExpressionType {
            self.expr_type
        }

        fn has_aggregation(&self) -> bool {
            self.has_agg
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn clone_box(&self) -> Box<dyn BoundExpression> {
            Box::new(self.clone())
        }
    }

    impl Display for TestBoundExpression {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "TestBoundExpression({})", self.expr_type)
        }
    }

    #[test]
    fn test_expression_type_display() {
        let test_cases = vec![
            (ExpressionType::Invalid, "Invalid"),
            (ExpressionType::Constant, "Constant"),
            (ExpressionType::ColumnRef, "ColumnRef"),
            (ExpressionType::TypeCast, "TypeCast"),
            (ExpressionType::Function, "Function"),
            (ExpressionType::AggCall, "AggregationCall"),
            (ExpressionType::Star, "Star"),
            (ExpressionType::UnaryOp, "UnaryOperation"),
            (ExpressionType::BinaryOp, "BinaryOperation"),
            (ExpressionType::Alias, "Alias"),
            (ExpressionType::FuncCall, "FuncCall"),
            (ExpressionType::Window, "Window"),
            (ExpressionType::GroupingSets, "GroupingSets"),
            (ExpressionType::Cube, "Cub"),
            (ExpressionType::Rollup, "Rollup"),
        ];

        for (expr_type, expected) in test_cases {
            assert_eq!(format!("{}", expr_type), expected);
        }
    }

    #[test]
    fn test_default_bound_expression_creation() {
        let expr_types = vec![
            ExpressionType::Constant,
            ExpressionType::ColumnRef,
            ExpressionType::TypeCast,
            ExpressionType::Function,
            ExpressionType::Star,
        ];

        for expr_type in expr_types {
            let expr = DefaultBoundExpression::new(expr_type);
            assert_eq!(expr.expression_type(), expr_type);
            assert_eq!(
                format!("{}", expr),
                format!("DefaultBoundExpression({})", expr_type)
            );
        }
    }

    #[test]
    fn test_invalid_expression() {
        let expr = DefaultBoundExpression::new(ExpressionType::Invalid);
        assert!(expr.is_invalid());
        assert_eq!(expr.expression_type(), ExpressionType::Invalid);
    }

    #[test]
    fn test_expression_cloning() {
        let original = DefaultBoundExpression::new(ExpressionType::Constant);
        let boxed: Box<dyn BoundExpression> = Box::new(original.clone());
        let cloned = boxed.clone();

        assert_eq!(boxed.expression_type(), cloned.expression_type());
        assert_eq!(format!("{}", boxed), format!("{}", cloned));
    }

    #[test]
    fn test_as_any_conversion() {
        let expr = DefaultBoundExpression::new(ExpressionType::Constant);
        let as_any = expr.as_any();
        let downcast_result = as_any.downcast_ref::<DefaultBoundExpression>();
        assert!(downcast_result.is_some());
        assert_eq!(
            downcast_result.unwrap().expression_type(),
            ExpressionType::Constant
        );
    }

    // #[test]
    // #[should_panic(expected = "has_aggregation should have been implemented!")]
    // fn test_has_aggregation_panic() {
    //     let expr = DefaultBoundExpression::new(ExpressionType::Constant);
    //     expr.has_aggregation();
    // }

    #[test]
    fn test_has_window_function() {
        let expr = DefaultBoundExpression::new(ExpressionType::Window);
        assert!(
            !expr.has_window_function(),
            "Default implementation should return false"
        );
    }

    #[test]
    fn test_expression_type_equality() {
        assert_eq!(ExpressionType::Constant, ExpressionType::Constant);
        assert_ne!(ExpressionType::Constant, ExpressionType::ColumnRef);

        let expr1 = DefaultBoundExpression::new(ExpressionType::Constant);
        let expr2 = DefaultBoundExpression::new(ExpressionType::Constant);
        let expr3 = DefaultBoundExpression::new(ExpressionType::ColumnRef);

        assert_eq!(expr1.expression_type(), expr2.expression_type());
        assert_ne!(expr1.expression_type(), expr3.expression_type());
    }

    #[test]
    fn test_custom_bound_expression() {
        let expr = TestBoundExpression::new(ExpressionType::AggCall, true);
        assert_eq!(expr.expression_type(), ExpressionType::AggCall);
        assert!(expr.has_aggregation());
        assert!(!expr.has_window_function());
        assert!(!expr.is_invalid());
    }

    #[test]
    fn test_bound_expression_boxing() {
        let expr: Box<dyn BoundExpression> =
            Box::new(TestBoundExpression::new(ExpressionType::Function, false));
        assert_eq!(expr.expression_type(), ExpressionType::Function);

        let cloned = expr.clone();
        assert_eq!(cloned.expression_type(), ExpressionType::Function);
    }
}
