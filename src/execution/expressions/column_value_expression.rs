use std::fmt;
use std::fmt::{Display, Formatter};
use crate::catalogue::column::Column;

#[derive(Debug, Clone)]
pub struct ColumnRefExpression {
    column_index: usize,
    ret_type: Column,
}

impl ColumnRefExpression {
    pub fn new(column_index: usize, ret_type: Column) -> Self {
        Self {
            column_index,
            ret_type
        }
    }

    pub fn get_column_index(&self) -> usize {
        self.column_index
    }

    pub fn get_ret_type(&self) -> &Column {
        &self.ret_type
    }
}

impl Display for ColumnRefExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
       write!(f, "Col#{}", self.column_index)
    }
}
