use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub struct FunctionExpression {
    name: String,
    children: Vec<Arc<Expression>>,
    return_type: Column,
}

impl FunctionExpression {
    pub fn new(name: String, children: Vec<Arc<Expression>>, return_type: Column) -> Self {
        Self {
            name,
            children,
            return_type,
        }
    }
}

impl ExpressionOps for FunctionExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        todo!()
    }

    fn evaluate_join(&self, left_tuple: &Tuple, left_schema: &Schema, right_tuple: &Tuple, right_schema: &Schema) -> Result<Value, ExpressionError> {
        todo!()
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        todo!()
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        todo!()
    }

    fn get_return_type(&self) -> &Column {
        todo!()
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        todo!()
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        todo!()
    }
    // Implement required trait methods
    // ...
} 