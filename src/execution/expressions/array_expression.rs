use crate::catalogue::column::Column;
use crate::catalogue::schema::Schema;
use crate::common::exception::{ArrayExpressionError, ExpressionError};
use crate::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::execution::expressions::constant_value_expression::ConstantExpression;
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::{Val, Value};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct ArrayExpression {
    children: Vec<Arc<Expression>>,
    ret_type: Column,
}

impl ArrayExpression {
    pub fn new(children: Vec<Arc<Expression>>) -> Self {
        Self {
            ret_type: Column::new("<val>", TypeId::Vector),
            children,
        }
    }

    fn evaluate_children<F>(&self, mut eval_func: F) -> Result<Value, ArrayExpressionError>
    where
        F: FnMut(&Expression) -> Result<Value, ExpressionError>,
    {
        // Collect the evaluated values into a Vec<Value>
        let values: Result<Vec<Value>, ArrayExpressionError> = self.children
            .iter()
            .map(|expr| {
                let val = eval_func(expr).map_err(|e| ArrayExpressionError::ChildEvaluationError(e.to_string()))?;
                match val.get_value() {
                    Val::Decimal(d) => {
                        // Convert f64 to i32, handling potential loss of precision
                        let rounded_value = d.round() as i32;
                        Ok(Value::new(Val::Integer(rounded_value))) // Convert to Val::Integer
                    }
                    _ => Err(ArrayExpressionError::NonDecimalType),
                }
            })
            .collect();

        // If all values are successfully evaluated, return them wrapped in Val::Vector
        values.map(|v| Value::new(Val::Vector(v)))
    }
}

impl ExpressionOps for ArrayExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        self.evaluate_children(|child| child.evaluate(tuple, schema))
            .map_err(|e| ExpressionError::Array(e))
    }

    fn evaluate_join(&self, left_tuple: &Tuple, left_schema: &Schema, right_tuple: &Tuple, right_schema: &Schema) -> Result<Value, ExpressionError> {
        self.evaluate_children(|child| child.evaluate_join(left_tuple, left_schema, right_tuple, right_schema))
            .map_err(|e| ExpressionError::Array(e))
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
        Arc::new(Expression::Array(ArrayExpression {
            ret_type: self.ret_type.clone(),
            children,
        }))
    }
}

impl Display for ArrayExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        let mut iter = self.children.iter();
        if let Some(first) = iter.next() {
            write!(f, "{}", first)?;
            for child in iter {
                write!(f, ", {}", child)?;
            }
        }
        write!(f, "]")
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;

    #[test]
    fn array_expression() {
        let children = vec![
            Arc::new(Expression::Constant(ConstantExpression::new(Value::new(Val::Decimal(1.0)), Column::new("const", TypeId::Decimal), vec![]))),
            Arc::new(Expression::Constant(ConstantExpression::new(Value::new(Val::Decimal(2.0)), Column::new("const", TypeId::Decimal), vec![]))),
            Arc::new(Expression::Constant(ConstantExpression::new(Value::new(Val::Decimal(3.0)), Column::new("const", TypeId::Decimal), vec![]))),
        ];
        let expr = Expression::Array(ArrayExpression::new(children));

        let schema = Schema::new(vec![]);
        let rid = RID::new(0, 0);
        let tuple = Tuple::new(vec![], schema.clone(), rid);

        let result = expr.evaluate(&tuple, &schema).expect("Evaluation should succeed");
        assert_eq!(result.get_type_id(), TypeId::Vector);
        if let Val::Vector(vec) = result.get_value() {
            assert_eq!(vec.len(), 3);
            assert_eq!(vec[0].get_value(), &Val::Integer(1));
            assert_eq!(vec[1].get_value(), &Val::Integer(2));
            assert_eq!(vec[2].get_value(), &Val::Integer(3));
        } else {
            panic!("Expected Vector value");
        }
    }

    #[test]
    fn array_expression_invalid_type() {
        let children = vec![
            Arc::new(Expression::Constant(ConstantExpression::new(Value::new(Val::Decimal(1.0)), Column::new("const", TypeId::Decimal), vec![]))),
            Arc::new(Expression::Constant(ConstantExpression::new(Value::new(Val::Integer(2)), Column::new("const", TypeId::Integer), vec![]))),
        ];
        let expr = Expression::Array(ArrayExpression::new(children));

        let schema = Schema::new(vec![]);
        let rid = RID::new(0, 0);
        let tuple = Tuple::new(vec![], schema.clone(), rid);

        let result = expr.evaluate(&tuple, &schema);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ExpressionError::Array(_)));
    }
}