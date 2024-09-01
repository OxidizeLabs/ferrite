use std::fmt;
use std::fmt::{Display, Formatter};
use crate::catalogue::column::Column;
use crate::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::types_db::value::{Val, Value};

#[derive(Debug, Clone)]
pub struct ConstantExpression {
    value: Value,
    ret_type: Column,
}

impl ConstantExpression {
    pub fn new(value: Value, ret_type: Column) -> Self {
        Self {
            value,
            ret_type
        }
    }

    pub fn get_value(&self) -> &Value {
        &self.value
    }

    pub fn get_ret_type(&self) -> &Column {
        &self.ret_type
    }
}

impl Display for ConstantExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}
