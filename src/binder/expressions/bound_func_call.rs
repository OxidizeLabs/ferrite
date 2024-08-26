use std::fmt;

use crate::binder::bound_expression::{BoundExpression, ExpressionType};
use crate::binder::expressions::bound_constant::BoundConstant;

/// Represents a bound function call, e.g., `lower(x)`.
pub struct BoundFuncCall {
    /// Function name.
    pub func_name: String,
    /// Arguments of the function call.
    pub args: Vec<Box<dyn BoundExpression>>,
}

impl BoundFuncCall {
    /// Creates a new BoundFuncCall.
    pub fn new(func_name: String, args: Vec<Box<dyn BoundExpression>>) -> Self {
        Self { func_name, args }
    }
}

impl BoundExpression for BoundFuncCall {
    fn expression_type(&self) -> ExpressionType {
        ExpressionType::FuncCall
    }

    fn has_aggregation(&self) -> bool {
        false
    }
}

impl fmt::Display for BoundFuncCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.func_name)?;
        for (i, arg) in self.args.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", arg)?;
        }
        write!(f, ")")
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn bound_func_call() {
        let func_call = BoundFuncCall::new(
            "lower".to_string(),
            vec![Box::new(BoundConstant::new("HELLO"))],
        );

        assert_eq!(func_call.expression_type(), ExpressionType::FuncCall);
        assert!(!func_call.has_aggregation());
        assert_eq!(func_call.to_string(), "lower(\"HELLO\")");

        let multi_arg_func_call = BoundFuncCall::new(
            "concat".to_string(),
            vec![
                Box::new(BoundConstant::new("Hello")),
                Box::new(BoundConstant::new(", ")),
                Box::new(BoundConstant::new("World!")),
            ],
        );

        assert_eq!(multi_arg_func_call.to_string(), "concat(\"Hello\", \", \", \"World!\")");
    }
}