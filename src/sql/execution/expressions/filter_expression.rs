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
use crate::types_db::value::Val::Null;

#[derive(Clone, Debug, PartialEq)]
pub struct FilterExpression {
    aggregate: Arc<Expression>,
    predicate: Arc<Expression>,
    return_type: Column,
    children: Vec<Arc<Expression>>,
}

impl FilterExpression {
    pub fn new(aggregate: Arc<Expression>, predicate: Arc<Expression>, return_type: Column) -> Self {
        let children = vec![aggregate.clone(), predicate.clone()];
        
        Self {
            aggregate,
            predicate,
            return_type,
            children,
        }
    }
}

impl ExpressionOps for FilterExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let pred_result = self.predicate.evaluate(tuple, schema);
        if pred_result.is_ok() {
            self.aggregate.evaluate(tuple, schema)
        } else {
            Ok(Value::new(Null))
        }
    }

    fn evaluate_join(&self, left_tuple: &Tuple, left_schema: &Schema, right_tuple: &Tuple, right_schema: &Schema) -> Result<Value, ExpressionError> {
        let pred_result = self.predicate.evaluate_join(left_tuple, left_schema, right_tuple, right_schema);
        if pred_result.is_ok() {
            self.aggregate.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
        } else {
            Ok(Value::new(Null))
        }
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        match child_idx {
            0 => &self.aggregate,
            1 => &self.predicate,
            _ => panic!("Invalid child index {} for FilterExpression", child_idx),
        }
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        if children.len() != 2 {
            panic!("FilterExpression requires exactly 2 children, got {}", children.len());
        }
        
        Arc::new(Expression::Filter(FilterExpression::new(
            children[0].clone(),
            children[1].clone(),
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        self.aggregate.validate(schema)?;
        self.predicate.validate(schema)?;
        
        let pred_type = self.predicate.get_return_type();
        if pred_type.get_type() != TypeId::Boolean {
            return Err(ExpressionError::TypeMismatch {
                expected: TypeId::Boolean,
                actual: pred_type.get_type(),
            });
        }
        
        Ok(())
    }
}

impl Display for FilterExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} FILTER (WHERE {})", self.aggregate, self.predicate)
    }
} 