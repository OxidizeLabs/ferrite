use crate::sql::binder::bound_expression::{BoundExpression, ExpressionType};
use std::any::Any;
use std::fmt;
use std::fmt::Display;

#[derive(Clone)]
pub struct BoundGroupingSets {
    sets: Vec<Vec<Box<dyn BoundExpression>>>,
}

impl BoundExpression for BoundGroupingSets {
    fn expression_type(&self) -> ExpressionType {
        ExpressionType::GroupingSets
    }

    fn has_aggregation(&self) -> bool {
        self.sets
            .iter()
            .any(|set| set.iter().any(|expr| expr.has_aggregation()))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_box(&self) -> Box<dyn BoundExpression> {
        Box::new(Self {
            sets: self
                .sets
                .iter()
                .map(|set| set.iter().map(|expr| expr.clone_box()).collect())
                .collect(),
        })
    }
}

impl Display for BoundGroupingSets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GROUPING SETS (")?;
        for (i, set) in self.sets.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "(")?;
            for (j, expr) in set.iter().enumerate() {
                if j > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", expr)?;
            }
            write!(f, ")")?;
        }
        write!(f, ")")
    }
}
