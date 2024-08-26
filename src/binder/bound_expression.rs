use std::fmt;

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
}

impl fmt::Display for ExpressionType {
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
        };
        write!(f, "{}", name)
    }
}

/// Trait for bound expressions.
pub trait BoundExpression: fmt::Display {
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
}

/// A default implementation for BoundExpression that can be used as a base
/// for concrete expression types.
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
}

impl fmt::Display for DefaultBoundExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "")
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn expression_type_display() {
        assert_eq!(format!("{}", ExpressionType::Constant), "Constant");
        assert_eq!(format!("{}", ExpressionType::ColumnRef), "ColumnRef");
        assert_eq!(format!("{}", ExpressionType::Invalid), "Invalid");
    }

    #[test]
    fn default_bound_expression() {
        let expr = DefaultBoundExpression::new(ExpressionType::Constant);
        assert_eq!(expr.expression_type(), ExpressionType::Constant);
        assert!(!expr.is_invalid());
        assert_eq!(format!("{}", expr), "");
    }
}