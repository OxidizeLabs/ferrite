use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRefExpression {
    tuple_index: usize,
    column_index: usize,
    ret_type: Column,
    children: Vec<Arc<Expression>>,
}

impl ColumnRefExpression {
    pub fn new(
        tuple_index: usize,
        column_index: usize,
        ret_type: Column,
        children: Vec<Arc<Expression>>,
    ) -> Self {
        Self {
            tuple_index,
            column_index,
            ret_type,
            children,
        }
    }

    pub fn get_column_index(&self) -> usize {
        self.column_index
    }

    pub fn get_return_type(&self) -> &Column {
        &self.ret_type
    }

    pub fn get_tuple_index(&self) -> usize {
        self.tuple_index
    }
}

impl ExpressionOps for ColumnRefExpression {
    fn evaluate(&self, tuple: &Tuple, _schema: &Schema) -> Result<Value, ExpressionError> {
        Ok(tuple.get_value(self.column_index).clone())
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        let column_index = self.get_column_index();
        if column_index < left_schema.get_column_count() as usize {
            Ok(left_tuple.get_value(column_index).clone())
        } else {
            let right_index = column_index - right_schema.get_column_count() as usize;
            Ok(right_tuple.get_value(right_index).clone())
        }
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        &self.children[child_idx]
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.ret_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        Arc::new(Expression::ColumnRef(ColumnRefExpression {
            tuple_index: self.tuple_index,
            column_index: self.column_index,
            ret_type: self.ret_type.clone(),
            children,
        }))
    }
}

impl Display for ColumnRefExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            // Detailed format: #0.1
            write!(f, "Col#{}.{}", self.get_tuple_index(), self.get_column_index())
        } else {
            // Basic format: just the column name
            write!(f, "{}", self.ret_type.get_name())
        }
    }
}
