use std::fmt;
use std::fmt::{Display, Formatter};
use std::rc::Rc;
use crate::catalogue::column::Column;
use crate::catalogue::schema::Schema;
use crate::common::exception::ArithmeticExpressionError::{DivisionByZero, Unknown};
use crate::common::exception::ExpressionError;
use crate::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::execution::expressions::arithmetic_expression::{ArithmeticExpression, ArithmeticOp};
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::{Val, Value};

#[derive(Debug, Clone)]
pub struct ColumnRefExpression {
    column_index: usize,
    ret_type: Column,
    children: Vec<Rc<Expression>>,
}

impl ColumnRefExpression {
    pub fn new(column_index: usize, ret_type: Column, children: Vec<Rc<Expression>>) -> Self {
        Self {
            column_index,
            ret_type,
            children
        }
    }

    pub fn get_column_index(&self) -> usize {
        self.column_index
    }

    pub fn get_return_type(&self) -> &Column {
        &self.ret_type
    }
}

impl ExpressionOps for ColumnRefExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        todo!()
    }

    fn evaluate_join(&self, left_tuple: &Tuple, left_schema: &Schema, right_tuple: &Tuple, right_schema: &Schema) -> Result<Value, ExpressionError> {
        let column_index = self.get_column_index();
        if column_index < left_schema.get_column_count() as usize {
            Ok(left_tuple.get_value(column_index).clone())
        } else {
            let right_index = column_index - right_schema.get_column_count() as usize;
            Ok(right_tuple.get_value(right_index).clone())
        }
    }

    fn get_child_at(&self, child_idx: usize) -> &Rc<Expression> {
        &self.children[child_idx]
    }

    fn get_children(&self) -> &Vec<Rc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.ret_type
    }

    fn clone_with_children(&self, children: Vec<Rc<Expression>>) -> Rc<Expression> {
        Rc::new(Expression::ColumnRef(ColumnRefExpression {
            column_index: self.column_index,
            ret_type: self.ret_type.clone(),
            children,
        }))
    }
}

impl Display for ColumnRefExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
       write!(f, "Col#{}", self.column_index)
    }
}
