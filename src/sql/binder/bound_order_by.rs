use crate::sql::binder::bound_expression::BoundExpression;
use std::fmt;
use std::fmt::{Display, Formatter};

/// All types of order-bys in binder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderByType {
    Asc,
    Desc,
}

#[derive(Clone)]
pub struct BoundOrderBy {
    pub order_type: OrderByType,
    pub expr: Box<dyn BoundExpression>,
}

impl BoundOrderBy {
    pub fn new(order_type: OrderByType, expr: Box<dyn BoundExpression>) -> Self {
        Self { order_type, expr }
    }
}

impl Display for BoundOrderBy {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BoundOrderBy {{ type={:?}, expr={} }}",
            self.order_type, self.expr
        )
    }
}
