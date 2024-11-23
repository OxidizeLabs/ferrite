use crate::binder::bound_expression::{BoundExpression, ExpressionType};
use crate::types_db::value::{Val, Value};
use std::any::Any;
use std::fmt;
use std::fmt::Display;

/// Represents a bound constant, e.g., `1`.
#[derive(Clone)]
pub struct BoundConstant {
    /// The constant being bound.
    val: Value,
}

impl BoundConstant {
    pub fn new<T>(val: T) -> Self
    where
        T: Into<Value>,
    {
        Self { val: val.into() }
    }

    pub fn new_vector<I>(iter: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<Value>,
    {
        Self {
            val: Value::new_vector(iter),
        }
    }
}

impl BoundExpression for BoundConstant {
    fn expression_type(&self) -> ExpressionType {
        ExpressionType::Constant
    }

    fn has_aggregation(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_box(&self) -> Box<dyn BoundExpression> {
        Box::new(self.clone())
    }
}

impl Display for BoundConstant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.val.get_value() {
            Val::Boolean(b) => write!(f, "{}", b),
            Val::TinyInt(i) => write!(f, "{}", i),
            Val::SmallInt(i) => write!(f, "{}", i),
            Val::Integer(i) => write!(f, "{}", i),
            Val::BigInt(i) => write!(f, "{}", i),
            Val::Decimal(d) => write!(f, "{}", d),
            Val::Timestamp(t) => write!(f, "{}", t),
            Val::VarLen(s) | Val::ConstVarLen(s) => write!(f, "\"{}\"", s.replace("\"", "\\\"")),
            Val::Vector(v) => write!(f, "{:?}", v),
            Val::Null => write!(f, "Null"),
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

        let int_vector_constant = BoundConstant::new_vector(vec![1, 2, 3]);
        assert!(
            int_vector_constant.to_string().contains("1")
                && int_vector_constant.to_string().contains("2")
                && int_vector_constant.to_string().contains("3")
        );

        let mixed_vector_constant =
            BoundConstant::new_vector(vec![Value::new(1), Value::new("two"), Value::new(3.0)]);
        assert!(
            mixed_vector_constant.to_string().contains("1")
                && mixed_vector_constant.to_string().contains("\"two\"")
                && mixed_vector_constant.to_string().contains("3")
        );
    }
}
