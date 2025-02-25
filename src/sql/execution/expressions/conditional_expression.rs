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
pub enum ConditionalType {
    NullIf,
    Greatest,
    Least,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConditionalExpression {
    cond_type: ConditionalType,
    children: Vec<Arc<Expression>>,
    return_type: Column,
}

impl ConditionalExpression {
    pub fn new(cond_type: ConditionalType, children: Vec<Arc<Expression>>, return_type: Column) -> Self {
        Self {
            cond_type,
            children,
            return_type,
        }
    }
}

impl ExpressionOps for ConditionalExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        match self.cond_type {
            ConditionalType::NullIf => {
                let val1 = self.children[0].evaluate(tuple, schema)?;
                let val2 = self.children[1].evaluate(tuple, schema)?;
                if val1 == val2 {
                    Ok(Value::new_null())
                } else {
                    Ok(val1)
                }
            }
            ConditionalType::Greatest => {
                // Find maximum value among children
                todo!()
            }
            ConditionalType::Least => {
                // Find minimum value among children
                todo!()
            }
        }
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

impl Display for ConditionalExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.cond_type {
            ConditionalType::NullIf => write!(f, "NULLIF({}, {})", self.children[0], self.children[1]),
            ConditionalType::Greatest => write!(f, "GREATEST({})", self.children.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")),
            ConditionalType::Least => write!(f, "LEAST({})", self.children.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ")),
        }
    }
} 