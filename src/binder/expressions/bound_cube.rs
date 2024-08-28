use crate::binder::bound_expression::{BoundExpression, ExpressionType};
use std::any::Any;
use std::fmt;
use std::fmt::Display;

#[derive(Clone)]
pub struct BoundCube {
    exprs: Vec<Box<dyn BoundExpression>>,
}

impl BoundExpression for BoundCube {
    fn expression_type(&self) -> ExpressionType {
        ExpressionType::Cube
    }

    fn has_aggregation(&self) -> bool {
        self.exprs.iter().any(|expr| expr.has_aggregation())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_box(&self) -> Box<dyn BoundExpression> {
        Box::new(Self {
            exprs: self.exprs.iter().map(|expr| expr.clone_box()).collect(),
        })
    }
}

impl Display for BoundCube {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CUBE (")?;
        for (i, expr) in self.exprs.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", expr)?;
        }
        write!(f, ")")
    }
}