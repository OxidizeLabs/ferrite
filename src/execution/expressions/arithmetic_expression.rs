use crate::catalogue::column::Column;
use crate::catalogue::schema::Schema;
use crate::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::rc::Rc;
use crate::common::exception::ExpressionError;
use crate::common::exception::ArithmeticExpressionError::{DivisionByZero, Unknown};

#[derive(Debug, Clone, Copy)]
pub enum ArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
}

#[derive(Debug, Clone)]
pub struct ArithmeticExpression {
    left: Rc<Expression>,
    right: Rc<Expression>,
    op: ArithmeticOp,
    ret_type: Column,
    children: Vec<Rc<Expression>>
}

impl ArithmeticExpression {
    pub fn new(left: Rc<Expression>, right: Rc<Expression>, op: ArithmeticOp, children: Vec<Rc<Expression>>) -> Self {
        let ret_type = Self::infer_return_type(left.get_return_type(), right.get_return_type()).unwrap();
        Self {
            left,
            right,
            op,
            ret_type,
            children
        }
    }

    pub fn get_left(&self) -> &Rc<Expression> {
        &self.left
    }

    pub fn get_right(&self) -> &Rc<Expression> {
        &self.right
    }

    pub fn get_op(&self) -> ArithmeticOp {
        self.op
    }

    fn infer_return_type(left: &Column, right: &Column) -> Result<Column, String> {
        match (left.get_type(), right.get_type()) {
            (TypeId::Integer, TypeId::Integer) => Ok(Column::new("arithmetic_result", TypeId::Integer)),
            (TypeId::Decimal, TypeId::Decimal) => Ok(Column::new("arithmetic_result", TypeId::Decimal)),
            (TypeId::Integer, TypeId::Decimal) | (TypeId::Decimal, TypeId::Integer) => {
                Ok(Column::new("arithmetic_result", TypeId::Decimal))
            }
            _ => Err(format!("Invalid types for arithmetic operation: {:?} and {:?}", left, right)),
        }
    }
}

impl ExpressionOps for ArithmeticExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let left_value = self.left.evaluate(tuple, schema)?;
        let right_value = self.right.evaluate(tuple, schema)?;

        match (left_value.get_value(), right_value.get_value()) {
            (Val::Integer(l), Val::Integer(r)) => {
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(*r),
                    ArithmeticOp::Subtract => l.checked_sub(*r),
                    ArithmeticOp::Multiply => l.checked_mul(*r),
                    ArithmeticOp::Divide => l.checked_div(*r),
                };
                result.map(Value::from).ok_or_else(|| ExpressionError::ArithmeticError(Unknown))
            }
            (Val::BigInt(l), Val::BigInt(r)) => {
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(*r),
                    ArithmeticOp::Subtract => l.checked_sub(*r),
                    ArithmeticOp::Multiply => l.checked_mul(*r),
                    ArithmeticOp::Divide => l.checked_div(*r),
                };
                result.map(Value::from).ok_or_else(|| ExpressionError::ArithmeticError(Unknown))
            }
            (Val::Decimal(l), Val::Decimal(r)) => {
                let result = match self.op {
                    ArithmeticOp::Add => *l + *r,
                    ArithmeticOp::Subtract => *l - *r,
                    ArithmeticOp::Multiply => *l * *r,
                    ArithmeticOp::Divide => if *r != 0.0 { Ok(*l / *r) } else { Err(ExpressionError::ArithmeticError(DivisionByZero)) }?,
                };
                Ok(Value::from(result))
            }
            (Val::Integer(l), Val::Decimal(r)) | (Val::Decimal(r), Val::Integer(l)) => {
                let l = *l as f64;
                let r = *r;
                let result = match self.op {
                    ArithmeticOp::Add => l + r,
                    ArithmeticOp::Subtract => l - r,
                    ArithmeticOp::Multiply => l * r,
                    ArithmeticOp::Divide => if r != 0.0 { Ok(l / r) } else { Err(ExpressionError::ArithmeticError(DivisionByZero)) }?,
                };
                Ok(Value::from(result))
            }
            (Val::BigInt(l), Val::Decimal(r)) | (Val::Decimal(r), Val::BigInt(l)) => {
                let l = *l as f64;
                let r = *r;
                let result = match self.op {
                    ArithmeticOp::Add => l + r,
                    ArithmeticOp::Subtract => l - r,
                    ArithmeticOp::Multiply => l * r,
                    ArithmeticOp::Divide => if r != 0.0 { Ok(l / r) } else { Err(ExpressionError::ArithmeticError(DivisionByZero)) }?,
                };
                Ok(Value::from(result))
            }
            _ => Err(ExpressionError::ArithmeticError(Unknown)),
        }
    }

    fn evaluate_join(&self, left_tuple: &Tuple, left_schema: &Schema, right_tuple: &Tuple, right_schema: &Schema) -> Result<Value, ExpressionError> {
        let left_value = self.left.evaluate_join(left_tuple, left_schema, right_tuple, right_schema).unwrap();
        let right_value = self.right.evaluate_join(left_tuple, left_schema, right_tuple, right_schema).unwrap();

        match (left_value.get_value(), right_value.get_value()) {
            (Val::Integer(l), Val::Integer(r)) => {
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(*r),
                    ArithmeticOp::Subtract => l.checked_sub(*r),
                    ArithmeticOp::Multiply => l.checked_mul(*r),
                    ArithmeticOp::Divide => l.checked_div(*r),
                };
                result.map(Value::from).ok_or_else(|| ExpressionError::ArithmeticError(Unknown))

            }
            (Val::BigInt(l), Val::BigInt(r)) => {
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(*r),
                    ArithmeticOp::Subtract => l.checked_sub(*r),
                    ArithmeticOp::Multiply => l.checked_mul(*r),
                    ArithmeticOp::Divide => l.checked_div(*r),
                };
                result.map(Value::from).ok_or_else(|| ExpressionError::ArithmeticError(Unknown))
            }
            (Val::Decimal(l), Val::Decimal(r)) => {
                let result = match self.op {
                    ArithmeticOp::Add => l + *r,
                    ArithmeticOp::Subtract => l - *r,
                    ArithmeticOp::Multiply => l * *r,
                    ArithmeticOp::Divide => if *r != 0.0 { Ok(l / *r) } else { Err(ExpressionError::ArithmeticError(DivisionByZero)) }?,
                };
                Ok(Value::from(result))
            }
            (Val::Integer(l), Val::Decimal(r)) | (Val::Decimal(r), Val::Integer(l)) => {
                let l = *l as f64;
                let r = *r;
                let result = match self.op {
                    ArithmeticOp::Add => l + r,
                    ArithmeticOp::Subtract => l - r,
                    ArithmeticOp::Multiply => l * r,
                    ArithmeticOp::Divide => if r != 0.0 { Ok(l / r) } else { Err(ExpressionError::ArithmeticError(DivisionByZero)) }?,
                };
                Ok(Value::from(result))
            }
            (Val::BigInt(l), Val::Decimal(r)) | (Val::Decimal(r), Val::BigInt(l)) => {
                let l = *l as f64;
                let r = *r;
                let result = match self.op {
                    ArithmeticOp::Add => l + r,
                    ArithmeticOp::Subtract => l - r,
                    ArithmeticOp::Multiply => l * r,
                    ArithmeticOp::Divide => if r != 0.0 { Ok(l / r) } else { Err(ExpressionError::ArithmeticError(DivisionByZero)) }?,
                };
                Ok(Value::from(result))
            }
            _ => Err(ExpressionError::ArithmeticError(Unknown)),
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
        if children.len() != 2 {
            panic!("ArithmeticExpression requires exactly two children");
        }

        Rc::new(Expression::Arithmetic(ArithmeticExpression {
            left: children[0].clone(),
            right: children[1].clone(),
            op: self.op,
            ret_type: self.ret_type.clone(),
            children,
        }))
    }
}

impl Display for ArithmeticOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ArithmeticOp::Add => write!(f, "+"),
            ArithmeticOp::Subtract => write!(f, "-"),
            ArithmeticOp::Multiply => write!(f, "*"),
            ArithmeticOp::Divide => write!(f, "/"),
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn arithmetic_expression() {
        let schema = Schema::new(vec![
            Column::new("col1", TypeId::Integer),
            Column::new("col2", TypeId::Decimal),
        ]);

        let tuple = Tuple::new(vec![Value::new(5), Value::new(2.5)], schema.clone(), 0);

        let col1 = Rc::new(Expression::ColumnRef(ColumnRefExpression::new(0, schema.get_column(0).unwrap().clone(), vec![]))
        );
        let col2 = Rc::new(Expression::ColumnRef(ColumnRefExpression::new(1, schema.get_column(1).unwrap().clone(), vec![]))
        );

        let add_expr = Expression::Arithmetic(ArithmeticExpression::new(
            col1.clone(),
            col2.clone(),
            ArithmeticOp::Add,
            vec![]
        ));

        let result = add_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(7.5));

        let mul_expr = Expression::Arithmetic(ArithmeticExpression::new(
            col1,
            col2,
            ArithmeticOp::Multiply,
            vec![]
        ));

        let result = mul_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(12.5));

        assert_eq!(add_expr.to_string(), "(Col#0 + Col#1)");
        assert_eq!(mul_expr.to_string(), "(Col#0 * Col#1)");
    }
}
