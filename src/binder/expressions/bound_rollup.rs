use crate::binder::bound_expression::BoundExpression;

#[derive(Clone)]
pub struct BoundRollup {
    exprs: Vec<Box<dyn BoundExpression>>,
}

