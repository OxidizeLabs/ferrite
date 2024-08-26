use std::fmt;

use crate::binder::bound_expression::{BoundExpression, ExpressionType};
use crate::types_db::types::Type;
use crate::types_db::value::{Value, Val};

/// Represents a bound constant, e.g., `1`.
pub struct BoundConstant {
    /// The constant being bound.
    pub val: Value,
}

impl BoundConstant {
    /// Creates a new BoundConstant.
    pub fn new<T: Into<Value>>(val: T) -> Self {
        Self { val: val.into() }
    }
}

impl BoundExpression for BoundConstant {
    fn expression_type(&self) -> ExpressionType {
        ExpressionType::Constant
    }

    fn has_aggregation(&self) -> bool {
        false
    }
}

impl fmt::Display for BoundConstant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.val.get_value() {
            Val::Boolean(b) => write!(f, "{}", b),
            Val::TinyInt(i) => write!(f, "{}", i),
            Val::SmallInt(i) => write!(f, "{}", i),
            Val::Integer(i) => write!(f, "{}", i),
            Val::BigInt(i) => write!(f, "{}", i),
            Val::Decimal(d) => write!(f, "{}", d),
            Val::Timestamp(t) => write!(f, "{}", t),
            Val::VarLen(s) | Val::ConstVarLen(s) => write!(f, "\"{}\"", s),
            Val::Vector(v) => write!(f, "{:?}", v),
        }
    }
}


#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn bound_constant() {
        let constant = BoundConstant::new(42);

        assert_eq!(constant.expression_type(), ExpressionType::Constant);
        assert!(!constant.has_aggregation());
        assert_eq!(constant.to_string(), "42");

        let string_constant = BoundConstant::new("Hello");
        assert_eq!(string_constant.to_string(), "\"Hello\"");

        let bool_constant = BoundConstant::new(true);
        assert_eq!(bool_constant.to_string(), "true");

        let float_constant = BoundConstant::new(3.14);
        assert_eq!(float_constant.to_string(), "3.14");

        let vector_constant = BoundConstant::new(vec![1, 2, 3]);
        assert_eq!(vector_constant.to_string(), "[1, 2, 3]");
    }
}