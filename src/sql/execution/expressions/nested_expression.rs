use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub struct NestedExpression {
    expr: Arc<Expression>,
    return_type: Column,
}

impl NestedExpression {
    pub fn new(expr: Arc<Expression>, return_type: Column) -> Self {
        Self {
            expr,
            return_type,
        }
    }
}

impl ExpressionOps for NestedExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        self.expr.evaluate(tuple, schema)
    }

    fn evaluate_join(&self, left_tuple: &Tuple, left_schema: &Schema, right_tuple: &Tuple, right_schema: &Schema) -> Result<Value, ExpressionError> {
        self.expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        if child_idx == 0 {
            &self.expr
        } else {
            panic!("NestedExpression has only one child")
        }
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        static EMPTY: Vec<Arc<Expression>> = Vec::new();
        &EMPTY
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        if children.len() != 1 {
            panic!("NestedExpression requires exactly one child");
        }
        
        Arc::new(Expression::Nested(NestedExpression::new(
            children[0].clone(),
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate the inner expression
        self.expr.validate(schema)
    }
}

impl Display for NestedExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "({})", self.expr)
    }
} 