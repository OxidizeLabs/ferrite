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
pub enum MapAccessKey {
    String(String),
    Number(i64),
}

#[derive(Clone, Debug, PartialEq)]
pub struct MapAccessExpression {
    column: Arc<Expression>,
    keys: Vec<MapAccessKey>,
    return_type: Column,
}

impl MapAccessExpression {
    pub fn new(column: Arc<Expression>, keys: Vec<MapAccessKey>, return_type: Column) -> Self {
        Self {
            column,
            keys,
            return_type,
        }
    }
}

impl ExpressionOps for MapAccessExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let map_value = self.column.evaluate(tuple, schema)?;
        // Access map/array using keys
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

    // Implement other required trait methods...
}

impl Display for MapAccessExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.column)?;
        for key in &self.keys {
            match key {
                MapAccessKey::String(s) => write!(f, "['{}']", s)?,
                MapAccessKey::Number(n) => write!(f, "[{}]", n)?,
            }
        }
        Ok(())
    }
} 