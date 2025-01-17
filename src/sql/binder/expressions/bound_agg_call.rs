use crate::sql::binder::bound_expression::{BoundExpression, ExpressionType};
use std::any::Any;
use std::fmt;
use std::fmt::Display;

/// Represents a bound aggregate call, e.g., `sum(x)`.
#[derive(Clone)]
pub struct BoundAggCall {
    /// Function name.
    pub func_name: String,
    /// Is distinct aggregation.
    pub is_distinct: bool,
    /// Arguments of the agg call.
    pub args: Vec<Box<dyn BoundExpression>>,
}

impl BoundAggCall {
    /// Creates a new BoundAggCall.
    pub fn new(func_name: String, is_distinct: bool, args: Vec<Box<dyn BoundExpression>>) -> Self {
        Self {
            func_name,
            is_distinct,
            args,
        }
    }
}

impl BoundExpression for BoundAggCall {
    fn expression_type(&self) -> ExpressionType {
        ExpressionType::AggCall
    }

    fn has_aggregation(&self) -> bool {
        true
    }
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_box(&self) -> Box<dyn BoundExpression> {
        Box::new(Self {
            func_name: self.func_name.clone(),
            is_distinct: self.is_distinct,
            args: self.args.iter().map(|arg| arg.clone_box()).collect(),
        })
    }
}

impl Display for BoundAggCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}({})",
            self.func_name,
            if self.is_distinct { " DISTINCT" } else { "" },
            self.args
                .iter()
                .map(|arg| arg.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[derive(Clone)]
    struct TestExpression(String);

    impl BoundExpression for TestExpression {
        fn expression_type(&self) -> ExpressionType {
            ExpressionType::Constant
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
            write!(f, "{}", self.0)
        }
    }

    #[test]
    fn bound_agg_call() {
        let agg_call = BoundAggCall::new(
            "sum".to_string(),
            false,
            vec![Box::new(TestExpression("x".to_string()))],
        );

        assert_eq!(agg_call.expression_type(), ExpressionType::AggCall);
        assert!(agg_call.has_aggregation());
        assert_eq!(agg_call.to_string(), "sum(x)");

        let distinct_agg_call = BoundAggCall::new(
            "count".to_string(),
            true,
            vec![
                Box::new(TestExpression("a".to_string())),
                Box::new(TestExpression("b".to_string())),
            ],
        );

        assert_eq!(distinct_agg_call.to_string(), "count DISTINCT(a, b)");
    }
}
