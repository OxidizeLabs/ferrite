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
pub enum ListOperation {
    Create,     // Create a new list
    Append,     // Append element to list
    Prepend,    // Prepend element to list
    Concat,     // Concatenate two lists
    GetElement, // Get element at index
    Length,     // Get list length
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListExpression {
    operation: ListOperation,
    args: Vec<Arc<Expression>>,
    return_type: Column,
}

impl ListExpression {
    pub fn new(operation: ListOperation, args: Vec<Arc<Expression>>, return_type: Column) -> Self {
        Self {
            operation,
            args,
            return_type,
        }
    }

    fn evaluate_list_operation(
        &self,
        operation: &ListOperation,
        args: &[Value],
    ) -> Result<Value, ExpressionError> {
        match operation {
            ListOperation::Create => {
                // Create a new list from the arguments
                Ok(Value::new(args.to_vec()))
            }
            ListOperation::Append => {
                let mut list = args[0].as_list()?.clone();
                list.push(args[1].clone());
                Ok(Value::new(list))
            }
            ListOperation::Prepend => {
                let mut list = args[0].as_list()?.clone();
                list.insert(0, args[1].clone());
                Ok(Value::new(list))
            }
            ListOperation::Concat => {
                let mut list1 = args[0].as_list()?.clone();
                let list2 = args[1].as_list()?;
                list1.extend_from_slice(list2);
                Ok(Value::new(list1))
            }
            ListOperation::GetElement => {
                let list = args[0].as_list()?;
                let index = args[1].as_i64()? as usize;
                list.get(index)
                    .cloned()
                    .ok_or_else(|| ExpressionError::InvalidOperation("Index out of bounds".to_string()))
            }
            ListOperation::Length => {
                let list = args[0].as_list()?;
                Ok(Value::new(list.len() as i64))
            }
        }
    }
}

impl ExpressionOps for ListExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let mut evaluated_args = Vec::new();
        for arg in &self.args {
            evaluated_args.push(arg.evaluate(tuple, schema)?);
        }

        self.evaluate_list_operation(&self.operation, &evaluated_args)
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

impl Display for ListExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.operation {
            ListOperation::Create => {
                write!(f, "ARRAY[{}]",
                       self.args.iter()
                           .map(|arg| arg.to_string())
                           .collect::<Vec<_>>()
                           .join(", ")
                )
            }
            ListOperation::GetElement => {
                write!(f, "{}[{}]", self.args[0], self.args[1])
            }
            ListOperation::Length => {
                write!(f, "ARRAY_LENGTH({})", self.args[0])
            }
            _ => write!(f, "ARRAY_{}({})",
                        format!("{:?}", self.operation).to_uppercase(),
                        self.args.iter()
                            .map(|arg| arg.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
            ),
        }
    }
} 