use crate::catalogue::column::Column;
use crate::catalogue::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::rc::Rc;

#[derive(Debug, Clone, PartialEq)]
pub struct ConstantExpression {
    value: Value,
    ret_type: Column,
    children: Vec<Rc<Expression>>,
}

impl ConstantExpression {
    pub fn new(value: Value, ret_type: Column, children: Vec<Rc<Expression>>) -> Self {
        Self {
            value,
            ret_type,
            children,
        }
    }

    pub fn get_value(&self) -> &Value {
        &self.value
    }
}

impl ExpressionOps for ConstantExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        todo!()
    }

    fn evaluate_join(&self, left_tuple: &Tuple, left_schema: &Schema, right_tuple: &Tuple, right_schema: &Schema) -> Result<Value, ExpressionError> {
        todo!()
    }

    fn get_child_at(&self, child_idx: usize) -> &Rc<Expression> {
        todo!()
    }

    fn get_children(&self) -> &Vec<Rc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.ret_type
    }

    fn clone_with_children(&self, children: Vec<Rc<Expression>>) -> Rc<Expression> {
        Rc::new(Expression::Constant(ConstantExpression::new(
            self.value.clone(),
            self.ret_type.clone(),
            children,
        )))
    }
}

impl Display for ConstantExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}
