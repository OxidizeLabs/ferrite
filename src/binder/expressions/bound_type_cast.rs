use crate::binder::bound_expression::{BoundExpression, ExpressionType};
use sqlparser::ast::DataType;
use std::any::Any;
use std::fmt;
use std::fmt::Display;

#[derive(Clone)]
pub struct BoundTypeCast {
    expr: Box<dyn BoundExpression>,
    target_type: DataType,
}

impl BoundExpression for BoundTypeCast {
    fn expression_type(&self) -> ExpressionType {
        ExpressionType::TypeCast
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
            expr: self.expr.clone_box(),
            target_type: self.target_type.clone(),
        })
    }
}

impl Display for BoundTypeCast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CAST({} AS {})", self.expr, self.target_type)
    }
}
