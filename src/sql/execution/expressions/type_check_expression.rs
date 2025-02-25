use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub enum TypeCheckOperation {
    IsNumeric,
    IsDate,
    IsBoolean,
    IsJson,
    IsNull,
    TypeOf,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TypeCheckExpression {
    expr: Arc<Expression>,
    operation: TypeCheckOperation,
    return_type: Column,
}

impl TypeCheckExpression {
    pub fn new(expr: Arc<Expression>, operation: TypeCheckOperation, return_type: Column) -> Self {
        Self {
            expr,
            operation,
            return_type,
        }
    }

    fn check_type(&self, value: &Value) -> bool {
        match self.operation {
            TypeCheckOperation::IsNumeric => {
                matches!(value.get_type_id(), 
                    TypeId::Integer | TypeId::BigInt | TypeId::Decimal)
            }
            TypeCheckOperation::IsDate => {
                matches!(value.get_type_id(),
                    TypeId::Date | TypeId::Timestamp)
            }
            TypeCheckOperation::IsBoolean => {
                value.get_type_id() == TypeId::Boolean
            }
            TypeCheckOperation::IsJson => {
                value.get_type_id() == TypeId::Json
            }
            TypeCheckOperation::IsNull => {
                value.is_null()
            }
            TypeCheckOperation::TypeOf => {
                // TypeOf is handled differently in evaluate()
                false
            }
        }
    }
}

impl ExpressionOps for TypeCheckExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let value = self.expr.evaluate(tuple, schema)?;

        match self.operation {
            TypeCheckOperation::TypeOf => {
                // Return the type name as a string
                Ok(Value::new(value.get_type_id().to_string()))
            }
            _ => {
                // For all other operations, return a boolean
                Ok(Value::new(self.check_type(&value)))
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

impl Display for TypeCheckExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.operation {
            TypeCheckOperation::TypeOf => write!(f, "TYPEOF({})", self.expr),
            _ => write!(f, "{} IS {:?}", self.expr, self.operation),
        }
    }
} 