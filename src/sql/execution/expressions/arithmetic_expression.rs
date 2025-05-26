use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ArithmeticExpressionError::{DivisionByZero, Overflow, Unknown};
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArithmeticExpression {
    op: ArithmeticOp,
    return_type: Column,
    children: Vec<Arc<Expression>>,
}

impl ArithmeticExpression {
    pub fn new(op: ArithmeticOp, children: Vec<Arc<Expression>>) -> Self {
        // Infer return type, defaulting to Integer if inference fails
        // This allows construction to succeed but validation will fail later
        let return_type =
            Self::infer_return_type(children[0].get_return_type(), children[1].get_return_type())
                .unwrap_or_else(|_| Column::new("arithmetic_result", TypeId::Integer));

        Self {
            op,
            return_type,
            children,
        }
    }

    pub fn get_left(&self) -> &Arc<Expression> {
        &self.children[0]
    }

    pub fn get_right(&self) -> &Arc<Expression> {
        &self.children[1]
    }

    pub fn get_op(&self) -> ArithmeticOp {
        self.op
    }

    fn infer_return_type(left: &Column, right: &Column) -> Result<Column, String> {
        match (left.get_type(), right.get_type()) {
            // Same type operations
            (TypeId::TinyInt, TypeId::TinyInt) => {
                Ok(Column::new("arithmetic_result", TypeId::TinyInt))
            }
            (TypeId::SmallInt, TypeId::SmallInt) => {
                Ok(Column::new("arithmetic_result", TypeId::SmallInt))
            }
            (TypeId::Integer, TypeId::Integer) => {
                Ok(Column::new("arithmetic_result", TypeId::Integer))
            }
            (TypeId::BigInt, TypeId::BigInt) => {
                Ok(Column::new("arithmetic_result", TypeId::BigInt))
            }
            (TypeId::Float, TypeId::Float) => Ok(Column::new("arithmetic_result", TypeId::Float)),
            (TypeId::Decimal, TypeId::Decimal) => {
                Ok(Column::new("arithmetic_result", TypeId::Decimal))
            }

            // Mixed integer type operations - promote to larger type
            (TypeId::TinyInt, TypeId::SmallInt) | (TypeId::SmallInt, TypeId::TinyInt) => {
                Ok(Column::new("arithmetic_result", TypeId::SmallInt))
            }
            (TypeId::TinyInt, TypeId::Integer) | (TypeId::Integer, TypeId::TinyInt) => {
                Ok(Column::new("arithmetic_result", TypeId::Integer))
            }
            (TypeId::TinyInt, TypeId::BigInt) | (TypeId::BigInt, TypeId::TinyInt) => {
                Ok(Column::new("arithmetic_result", TypeId::BigInt))
            }
            (TypeId::SmallInt, TypeId::Integer) | (TypeId::Integer, TypeId::SmallInt) => {
                Ok(Column::new("arithmetic_result", TypeId::Integer))
            }
            (TypeId::SmallInt, TypeId::BigInt) | (TypeId::BigInt, TypeId::SmallInt) => {
                Ok(Column::new("arithmetic_result", TypeId::BigInt))
            }
            (TypeId::Integer, TypeId::BigInt) | (TypeId::BigInt, TypeId::Integer) => {
                Ok(Column::new("arithmetic_result", TypeId::BigInt))
            }

            // Floating point operations - promote to Decimal for precision
            (TypeId::TinyInt, TypeId::Float) | (TypeId::Float, TypeId::TinyInt) => {
                Ok(Column::new("arithmetic_result", TypeId::Float))
            }
            (TypeId::SmallInt, TypeId::Float) | (TypeId::Float, TypeId::SmallInt) => {
                Ok(Column::new("arithmetic_result", TypeId::Float))
            }
            (TypeId::Integer, TypeId::Float) | (TypeId::Float, TypeId::Integer) => {
                Ok(Column::new("arithmetic_result", TypeId::Float))
            }
            (TypeId::BigInt, TypeId::Float) | (TypeId::Float, TypeId::BigInt) => {
                Ok(Column::new("arithmetic_result", TypeId::Float))
            }
            (TypeId::Float, TypeId::Decimal) | (TypeId::Decimal, TypeId::Float) => {
                Ok(Column::new("arithmetic_result", TypeId::Decimal))
            }

            // Decimal operations - promote to Decimal for precision
            (TypeId::TinyInt, TypeId::Decimal) | (TypeId::Decimal, TypeId::TinyInt) => {
                Ok(Column::new("arithmetic_result", TypeId::Decimal))
            }
            (TypeId::SmallInt, TypeId::Decimal) | (TypeId::Decimal, TypeId::SmallInt) => {
                Ok(Column::new("arithmetic_result", TypeId::Decimal))
            }
            (TypeId::Integer, TypeId::Decimal) | (TypeId::Decimal, TypeId::Integer) => {
                Ok(Column::new("arithmetic_result", TypeId::Decimal))
            }
            (TypeId::BigInt, TypeId::Decimal) | (TypeId::Decimal, TypeId::BigInt) => {
                Ok(Column::new("arithmetic_result", TypeId::Decimal))
            }

            _ => Err(format!(
                "Invalid types for arithmetic operation: {}({:?}) and {}({:?})",
                left.get_name(),
                left.get_type(),
                right.get_name(),
                right.get_type()
            )),
        }
    }
}

impl ExpressionOps for ArithmeticExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let left_value = self.get_left().evaluate(tuple, schema)?;
        let right_value = self.get_right().evaluate(tuple, schema)?;

        match (left_value.get_val(), right_value.get_val()) {
            // TinyInt operations
            (Val::TinyInt(l), Val::TinyInt(r)) => {
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(*r),
                    ArithmeticOp::Subtract => l.checked_sub(*r),
                    ArithmeticOp::Multiply => l.checked_mul(*r),
                    ArithmeticOp::Divide => {
                        if *r == 0 {
                            return Err(ExpressionError::ArithmeticError(DivisionByZero));
                        }
                        l.checked_div(*r)
                    }
                };
                result
                    .map(|v| Value::new_with_type(Val::TinyInt(v), TypeId::TinyInt))
                    .ok_or_else(|| ExpressionError::ArithmeticError(Overflow))
            }

            // SmallInt operations
            (Val::SmallInt(l), Val::SmallInt(r)) => {
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(*r),
                    ArithmeticOp::Subtract => l.checked_sub(*r),
                    ArithmeticOp::Multiply => l.checked_mul(*r),
                    ArithmeticOp::Divide => {
                        if *r == 0 {
                            return Err(ExpressionError::ArithmeticError(DivisionByZero));
                        }
                        l.checked_div(*r)
                    }
                };
                result
                    .map(|v| Value::new_with_type(Val::SmallInt(v), TypeId::SmallInt))
                    .ok_or_else(|| ExpressionError::ArithmeticError(Overflow))
            }

            // Integer operations
            (Val::Integer(l), Val::Integer(r)) => {
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(*r),
                    ArithmeticOp::Subtract => l.checked_sub(*r),
                    ArithmeticOp::Multiply => l.checked_mul(*r),
                    ArithmeticOp::Divide => {
                        if *r == 0 {
                            return Err(ExpressionError::ArithmeticError(DivisionByZero));
                        }
                        l.checked_div(*r)
                    }
                };
                result
                    .map(Value::from)
                    .ok_or_else(|| ExpressionError::ArithmeticError(Overflow))
            }

            // BigInt operations
            (Val::BigInt(l), Val::BigInt(r)) => {
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(*r),
                    ArithmeticOp::Subtract => l.checked_sub(*r),
                    ArithmeticOp::Multiply => l.checked_mul(*r),
                    ArithmeticOp::Divide => {
                        if *r == 0 {
                            return Err(ExpressionError::ArithmeticError(DivisionByZero));
                        }
                        l.checked_div(*r)
                    }
                };
                result
                    .map(Value::from)
                    .ok_or_else(|| ExpressionError::ArithmeticError(Overflow))
            }

            // Float operations
            (Val::Float(l), Val::Float(r)) => {
                let result = match self.op {
                    ArithmeticOp::Add => *l + *r,
                    ArithmeticOp::Subtract => *l - *r,
                    ArithmeticOp::Multiply => *l * *r,
                    ArithmeticOp::Divide => {
                        if *r != 0.0 {
                            Ok(*l / *r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::new_with_type(Val::Float(result), TypeId::Float))
            }

            // Decimal operations
            (Val::Decimal(l), Val::Decimal(r)) => {
                let result = match self.op {
                    ArithmeticOp::Add => *l + *r,
                    ArithmeticOp::Subtract => *l - *r,
                    ArithmeticOp::Multiply => *l * *r,
                    ArithmeticOp::Divide => {
                        if *r != 0.0 {
                            Ok(*l / *r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::from(result))
            }

            // Mixed integer type operations
            (Val::TinyInt(l), Val::SmallInt(r)) => {
                let l = *l as i16;
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(*r),
                    ArithmeticOp::Subtract => l.checked_sub(*r),
                    ArithmeticOp::Multiply => l.checked_mul(*r),
                    ArithmeticOp::Divide => {
                        if *r == 0 {
                            return Err(ExpressionError::ArithmeticError(DivisionByZero));
                        }
                        l.checked_div(*r)
                    }
                };
                result
                    .map(|v| Value::new_with_type(Val::SmallInt(v), TypeId::SmallInt))
                    .ok_or_else(|| ExpressionError::ArithmeticError(Overflow))
            }
            (Val::SmallInt(l), Val::TinyInt(r)) => {
                let r = *r as i16;
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(r),
                    ArithmeticOp::Subtract => l.checked_sub(r),
                    ArithmeticOp::Multiply => l.checked_mul(r),
                    ArithmeticOp::Divide => {
                        if r == 0 {
                            return Err(ExpressionError::ArithmeticError(DivisionByZero));
                        }
                        l.checked_div(r)
                    }
                };
                result
                    .map(|v| Value::new_with_type(Val::SmallInt(v), TypeId::SmallInt))
                    .ok_or_else(|| ExpressionError::ArithmeticError(Overflow))
            }
            (Val::TinyInt(l), Val::Integer(r)) => {
                let l = *l as i32;
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(*r),
                    ArithmeticOp::Subtract => l.checked_sub(*r),
                    ArithmeticOp::Multiply => l.checked_mul(*r),
                    ArithmeticOp::Divide => {
                        if *r == 0 {
                            return Err(ExpressionError::ArithmeticError(DivisionByZero));
                        }
                        l.checked_div(*r)
                    }
                };
                result
                    .map(Value::from)
                    .ok_or_else(|| ExpressionError::ArithmeticError(Overflow))
            }
            (Val::Integer(l), Val::TinyInt(r)) => {
                let r = *r as i32;
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(r),
                    ArithmeticOp::Subtract => l.checked_sub(r),
                    ArithmeticOp::Multiply => l.checked_mul(r),
                    ArithmeticOp::Divide => {
                        if r == 0 {
                            return Err(ExpressionError::ArithmeticError(DivisionByZero));
                        }
                        l.checked_div(r)
                    }
                };
                result
                    .map(Value::from)
                    .ok_or_else(|| ExpressionError::ArithmeticError(Overflow))
            }
            (Val::SmallInt(l), Val::Integer(r)) => {
                let l = *l as i32;
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(*r),
                    ArithmeticOp::Subtract => l.checked_sub(*r),
                    ArithmeticOp::Multiply => l.checked_mul(*r),
                    ArithmeticOp::Divide => {
                        if *r == 0 {
                            return Err(ExpressionError::ArithmeticError(DivisionByZero));
                        }
                        l.checked_div(*r)
                    }
                };
                result
                    .map(Value::from)
                    .ok_or_else(|| ExpressionError::ArithmeticError(Overflow))
            }
            (Val::Integer(l), Val::SmallInt(r)) => {
                let r = *r as i32;
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(r),
                    ArithmeticOp::Subtract => l.checked_sub(r),
                    ArithmeticOp::Multiply => l.checked_mul(r),
                    ArithmeticOp::Divide => {
                        if r == 0 {
                            return Err(ExpressionError::ArithmeticError(DivisionByZero));
                        }
                        l.checked_div(r)
                    }
                };
                result
                    .map(Value::from)
                    .ok_or_else(|| ExpressionError::ArithmeticError(Overflow))
            }
            (Val::TinyInt(l), Val::BigInt(r)) => {
                let l = *l as i64;
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(*r),
                    ArithmeticOp::Subtract => l.checked_sub(*r),
                    ArithmeticOp::Multiply => l.checked_mul(*r),
                    ArithmeticOp::Divide => {
                        if *r == 0 {
                            return Err(ExpressionError::ArithmeticError(DivisionByZero));
                        }
                        l.checked_div(*r)
                    }
                };
                result
                    .map(Value::from)
                    .ok_or_else(|| ExpressionError::ArithmeticError(Overflow))
            }
            (Val::BigInt(l), Val::TinyInt(r)) => {
                let r = *r as i64;
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(r),
                    ArithmeticOp::Subtract => l.checked_sub(r),
                    ArithmeticOp::Multiply => l.checked_mul(r),
                    ArithmeticOp::Divide => {
                        if r == 0 {
                            return Err(ExpressionError::ArithmeticError(DivisionByZero));
                        }
                        l.checked_div(r)
                    }
                };
                result
                    .map(Value::from)
                    .ok_or_else(|| ExpressionError::ArithmeticError(Overflow))
            }
            (Val::SmallInt(l), Val::BigInt(r)) => {
                let l = *l as i64;
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(*r),
                    ArithmeticOp::Subtract => l.checked_sub(*r),
                    ArithmeticOp::Multiply => l.checked_mul(*r),
                    ArithmeticOp::Divide => {
                        if *r == 0 {
                            return Err(ExpressionError::ArithmeticError(DivisionByZero));
                        }
                        l.checked_div(*r)
                    }
                };
                result
                    .map(Value::from)
                    .ok_or_else(|| ExpressionError::ArithmeticError(Overflow))
            }
            (Val::BigInt(l), Val::SmallInt(r)) => {
                let r = *r as i64;
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(r),
                    ArithmeticOp::Subtract => l.checked_sub(r),
                    ArithmeticOp::Multiply => l.checked_mul(r),
                    ArithmeticOp::Divide => {
                        if r == 0 {
                            return Err(ExpressionError::ArithmeticError(DivisionByZero));
                        }
                        l.checked_div(r)
                    }
                };
                result
                    .map(Value::from)
                    .ok_or_else(|| ExpressionError::ArithmeticError(Overflow))
            }
            (Val::BigInt(l), Val::Integer(r)) => {
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(*r as i64),
                    ArithmeticOp::Subtract => l.checked_sub(*r as i64),
                    ArithmeticOp::Multiply => l.checked_mul(*r as i64),
                    ArithmeticOp::Divide => {
                        if *r == 0 {
                            return Err(ExpressionError::ArithmeticError(DivisionByZero));
                        }
                        l.checked_div(*r as i64)
                    }
                };
                result
                    .map(Value::from)
                    .ok_or_else(|| ExpressionError::ArithmeticError(Overflow))
            }
            (Val::Integer(l), Val::BigInt(r)) => {
                let result = match self.op {
                    ArithmeticOp::Add => (*l as i64).checked_add(*r),
                    ArithmeticOp::Subtract => (*l as i64).checked_sub(*r),
                    ArithmeticOp::Multiply => (*l as i64).checked_mul(*r),
                    ArithmeticOp::Divide => {
                        if *r == 0 {
                            return Err(ExpressionError::ArithmeticError(DivisionByZero));
                        }
                        (*l as i64).checked_div(*r)
                    }
                };
                result
                    .map(Value::from)
                    .ok_or_else(|| ExpressionError::ArithmeticError(Overflow))
            }

            // Float with integer types
            (Val::TinyInt(l), Val::Float(r)) => {
                let l = *l as f32;
                let result = match self.op {
                    ArithmeticOp::Add => l + *r,
                    ArithmeticOp::Subtract => l - *r,
                    ArithmeticOp::Multiply => l * *r,
                    ArithmeticOp::Divide => {
                        if *r != 0.0 {
                            Ok(l / *r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::new_with_type(Val::Float(result), TypeId::Float))
            }
            (Val::Float(l), Val::TinyInt(r)) => {
                let r = *r as f32;
                let result = match self.op {
                    ArithmeticOp::Add => *l + r,
                    ArithmeticOp::Subtract => *l - r,
                    ArithmeticOp::Multiply => *l * r,
                    ArithmeticOp::Divide => {
                        if r != 0.0 {
                            Ok(*l / r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::new_with_type(Val::Float(result), TypeId::Float))
            }
            (Val::SmallInt(l), Val::Float(r)) => {
                let l = *l as f32;
                let result = match self.op {
                    ArithmeticOp::Add => l + *r,
                    ArithmeticOp::Subtract => l - *r,
                    ArithmeticOp::Multiply => l * *r,
                    ArithmeticOp::Divide => {
                        if *r != 0.0 {
                            Ok(l / *r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::new_with_type(Val::Float(result), TypeId::Float))
            }
            (Val::Float(l), Val::SmallInt(r)) => {
                let r = *r as f32;
                let result = match self.op {
                    ArithmeticOp::Add => *l + r,
                    ArithmeticOp::Subtract => *l - r,
                    ArithmeticOp::Multiply => *l * r,
                    ArithmeticOp::Divide => {
                        if r != 0.0 {
                            Ok(*l / r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::new_with_type(Val::Float(result), TypeId::Float))
            }
            (Val::Integer(l), Val::Float(r)) => {
                let l = *l as f32;
                let result = match self.op {
                    ArithmeticOp::Add => l + *r,
                    ArithmeticOp::Subtract => l - *r,
                    ArithmeticOp::Multiply => l * *r,
                    ArithmeticOp::Divide => {
                        if *r != 0.0 {
                            Ok(l / *r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::new_with_type(Val::Float(result), TypeId::Float))
            }
            (Val::Float(l), Val::Integer(r)) => {
                let r = *r as f32;
                let result = match self.op {
                    ArithmeticOp::Add => *l + r,
                    ArithmeticOp::Subtract => *l - r,
                    ArithmeticOp::Multiply => *l * r,
                    ArithmeticOp::Divide => {
                        if r != 0.0 {
                            Ok(*l / r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::new_with_type(Val::Float(result), TypeId::Float))
            }
            (Val::BigInt(l), Val::Float(r)) => {
                let l = *l as f32;
                let result = match self.op {
                    ArithmeticOp::Add => l + *r,
                    ArithmeticOp::Subtract => l - *r,
                    ArithmeticOp::Multiply => l * *r,
                    ArithmeticOp::Divide => {
                        if *r != 0.0 {
                            Ok(l / *r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::new_with_type(Val::Float(result), TypeId::Float))
            }
            (Val::Float(l), Val::BigInt(r)) => {
                let r = *r as f32;
                let result = match self.op {
                    ArithmeticOp::Add => *l + r,
                    ArithmeticOp::Subtract => *l - r,
                    ArithmeticOp::Multiply => *l * r,
                    ArithmeticOp::Divide => {
                        if r != 0.0 {
                            Ok(*l / r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::new_with_type(Val::Float(result), TypeId::Float))
            }

            // Decimal with integer types
            (Val::TinyInt(l), Val::Decimal(r)) => {
                let l = *l as f64;
                let result = match self.op {
                    ArithmeticOp::Add => l + *r,
                    ArithmeticOp::Subtract => l - *r,
                    ArithmeticOp::Multiply => l * *r,
                    ArithmeticOp::Divide => {
                        if *r != 0.0 {
                            Ok(l / *r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::from(result))
            }
            (Val::Decimal(l), Val::TinyInt(r)) => {
                let r = *r as f64;
                let result = match self.op {
                    ArithmeticOp::Add => *l + r,
                    ArithmeticOp::Subtract => *l - r,
                    ArithmeticOp::Multiply => *l * r,
                    ArithmeticOp::Divide => {
                        if r != 0.0 {
                            Ok(*l / r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::from(result))
            }
            (Val::SmallInt(l), Val::Decimal(r)) => {
                let l = *l as f64;
                let result = match self.op {
                    ArithmeticOp::Add => l + *r,
                    ArithmeticOp::Subtract => l - *r,
                    ArithmeticOp::Multiply => l * *r,
                    ArithmeticOp::Divide => {
                        if *r != 0.0 {
                            Ok(l / *r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::from(result))
            }
            (Val::Decimal(l), Val::SmallInt(r)) => {
                let r = *r as f64;
                let result = match self.op {
                    ArithmeticOp::Add => *l + r,
                    ArithmeticOp::Subtract => *l - r,
                    ArithmeticOp::Multiply => *l * r,
                    ArithmeticOp::Divide => {
                        if r != 0.0 {
                            Ok(*l / r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::from(result))
            }
            (Val::Integer(l), Val::Decimal(r)) => {
                let l = *l as f64;
                let result = match self.op {
                    ArithmeticOp::Add => l + *r,
                    ArithmeticOp::Subtract => l - *r,
                    ArithmeticOp::Multiply => l * *r,
                    ArithmeticOp::Divide => {
                        if *r != 0.0 {
                            Ok(l / *r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::from(result))
            }
            (Val::Decimal(l), Val::Integer(r)) => {
                let r = *r as f64;
                let result = match self.op {
                    ArithmeticOp::Add => *l + r,
                    ArithmeticOp::Subtract => *l - r,
                    ArithmeticOp::Multiply => *l * r,
                    ArithmeticOp::Divide => {
                        if r != 0.0 {
                            Ok(*l / r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::from(result))
            }
            (Val::BigInt(l), Val::Decimal(r)) => {
                let l = *l as f64;
                let result = match self.op {
                    ArithmeticOp::Add => l + *r,
                    ArithmeticOp::Subtract => l - *r,
                    ArithmeticOp::Multiply => l * *r,
                    ArithmeticOp::Divide => {
                        if *r != 0.0 {
                            Ok(l / *r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::from(result))
            }
            (Val::Decimal(l), Val::BigInt(r)) => {
                let r = *r as f64;
                let result = match self.op {
                    ArithmeticOp::Add => *l + r,
                    ArithmeticOp::Subtract => *l - r,
                    ArithmeticOp::Multiply => *l * r,
                    ArithmeticOp::Divide => {
                        if r != 0.0 {
                            Ok(*l / r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::from(result))
            }

            // Float with Decimal
            (Val::Float(l), Val::Decimal(r)) => {
                let l = *l as f64;
                let result = match self.op {
                    ArithmeticOp::Add => l + *r,
                    ArithmeticOp::Subtract => l - *r,
                    ArithmeticOp::Multiply => l * *r,
                    ArithmeticOp::Divide => {
                        if *r != 0.0 {
                            Ok(l / *r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::from(result))
            }
            (Val::Decimal(l), Val::Float(r)) => {
                let r = *r as f64;
                let result = match self.op {
                    ArithmeticOp::Add => *l + r,
                    ArithmeticOp::Subtract => *l - r,
                    ArithmeticOp::Multiply => *l * r,
                    ArithmeticOp::Divide => {
                        if r != 0.0 {
                            Ok(*l / r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::from(result))
            }

            _ => Err(ExpressionError::ArithmeticError(Unknown)),
        }
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        let left_value = self
            .get_left()
            .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            .unwrap();
        let right_value = self
            .get_right()
            .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            .unwrap();

        match (left_value.get_val(), right_value.get_val()) {
            (Val::Integer(l), Val::Integer(r)) => {
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(*r),
                    ArithmeticOp::Subtract => l.checked_sub(*r),
                    ArithmeticOp::Multiply => l.checked_mul(*r),
                    ArithmeticOp::Divide => l.checked_div(*r),
                };
                result
                    .map(Value::from)
                    .ok_or_else(|| ExpressionError::ArithmeticError(Overflow))
            }
            (Val::BigInt(l), Val::BigInt(r)) => {
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(*r),
                    ArithmeticOp::Subtract => l.checked_sub(*r),
                    ArithmeticOp::Multiply => l.checked_mul(*r),
                    ArithmeticOp::Divide => l.checked_div(*r),
                };
                result
                    .map(Value::from)
                    .ok_or_else(|| ExpressionError::ArithmeticError(Overflow))
            }
            (Val::Decimal(l), Val::Decimal(r)) => {
                let result = match self.op {
                    ArithmeticOp::Add => l + *r,
                    ArithmeticOp::Subtract => l - *r,
                    ArithmeticOp::Multiply => l * *r,
                    ArithmeticOp::Divide => {
                        if *r != 0.0 {
                            Ok(l / *r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
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
                    ArithmeticOp::Divide => {
                        if r != 0.0 {
                            Ok(l / r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::from(result))
            }
            (Val::BigInt(l), Val::Integer(r)) => {
                let result = match self.op {
                    ArithmeticOp::Add => l.checked_add(*r as i64),
                    ArithmeticOp::Subtract => l.checked_sub(*r as i64),
                    ArithmeticOp::Multiply => l.checked_mul(*r as i64),
                    ArithmeticOp::Divide => {
                        if *r == 0 {
                            return Err(ExpressionError::ArithmeticError(DivisionByZero));
                        }
                        l.checked_div(*r as i64)
                    }
                };
                result
                    .map(Value::from)
                    .ok_or_else(|| ExpressionError::ArithmeticError(Overflow))
            }
            (Val::Integer(l), Val::BigInt(r)) => {
                let result = match self.op {
                    ArithmeticOp::Add => (*l as i64).checked_add(*r),
                    ArithmeticOp::Subtract => (*l as i64).checked_sub(*r),
                    ArithmeticOp::Multiply => (*l as i64).checked_mul(*r),
                    ArithmeticOp::Divide => {
                        if *r == 0 {
                            return Err(ExpressionError::ArithmeticError(DivisionByZero));
                        }
                        (*l as i64).checked_div(*r)
                    }
                };
                result
                    .map(Value::from)
                    .ok_or_else(|| ExpressionError::ArithmeticError(Overflow))
            }
            (Val::BigInt(l), Val::Decimal(r)) | (Val::Decimal(r), Val::BigInt(l)) => {
                let l = *l as f64;
                let r = *r;
                let result = match self.op {
                    ArithmeticOp::Add => l + r,
                    ArithmeticOp::Subtract => l - r,
                    ArithmeticOp::Multiply => l * r,
                    ArithmeticOp::Divide => {
                        if r != 0.0 {
                            Ok(l / r)
                        } else {
                            Err(ExpressionError::ArithmeticError(DivisionByZero))
                        }?
                    }
                };
                Ok(Value::from(result))
            }
            _ => Err(ExpressionError::ArithmeticError(Unknown)),
        }
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        &self.children[child_idx]
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        if children.len() != 2 {
            panic!("ArithmeticExpression requires exactly two children");
        }

        Arc::new(Expression::Arithmetic(ArithmeticExpression {
            op: self.op,
            return_type: self.return_type.clone(),
            children,
        }))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate left and right child expressions
        self.get_left().validate(schema)?;
        self.get_right().validate(schema)?;

        // Check if the types are compatible for arithmetic operations
        let left_type = self.get_left().get_return_type().get_type();
        let right_type = self.get_right().get_return_type().get_type();

        match (left_type, right_type) {
            // Same type operations
            (TypeId::TinyInt, TypeId::TinyInt)
            | (TypeId::SmallInt, TypeId::SmallInt)
            | (TypeId::Integer, TypeId::Integer)
            | (TypeId::BigInt, TypeId::BigInt)
            | (TypeId::Float, TypeId::Float)
            | (TypeId::Decimal, TypeId::Decimal)

            // Mixed integer type operations
            | (TypeId::TinyInt, TypeId::SmallInt) | (TypeId::SmallInt, TypeId::TinyInt)
            | (TypeId::TinyInt, TypeId::Integer) | (TypeId::Integer, TypeId::TinyInt)
            | (TypeId::TinyInt, TypeId::BigInt) | (TypeId::BigInt, TypeId::TinyInt)
            | (TypeId::SmallInt, TypeId::Integer) | (TypeId::Integer, TypeId::SmallInt)
            | (TypeId::SmallInt, TypeId::BigInt) | (TypeId::BigInt, TypeId::SmallInt)
            | (TypeId::Integer, TypeId::BigInt) | (TypeId::BigInt, TypeId::Integer)

            // Float operations
            | (TypeId::TinyInt, TypeId::Float) | (TypeId::Float, TypeId::TinyInt)
            | (TypeId::SmallInt, TypeId::Float) | (TypeId::Float, TypeId::SmallInt)
            | (TypeId::Integer, TypeId::Float) | (TypeId::Float, TypeId::Integer)
            | (TypeId::BigInt, TypeId::Float) | (TypeId::Float, TypeId::BigInt)
            | (TypeId::Float, TypeId::Decimal) | (TypeId::Decimal, TypeId::Float)

            // Decimal operations
            | (TypeId::TinyInt, TypeId::Decimal) | (TypeId::Decimal, TypeId::TinyInt)
            | (TypeId::SmallInt, TypeId::Decimal) | (TypeId::Decimal, TypeId::SmallInt)
            | (TypeId::Integer, TypeId::Decimal) | (TypeId::Decimal, TypeId::Integer)
            | (TypeId::BigInt, TypeId::Decimal) | (TypeId::Decimal, TypeId::BigInt) => Ok(()),

            _ => Err(ExpressionError::ArithmeticError(Unknown)),
        }
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

impl Display for ArithmeticExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let op_str = match self.op {
            ArithmeticOp::Add => "+",
            ArithmeticOp::Subtract => "-",
            ArithmeticOp::Multiply => "*",
            ArithmeticOp::Divide => "/",
        };

        if f.alternate() {
            write!(
                f,
                "({:#} {} {:#})",
                self.get_left(),
                op_str,
                self.get_right()
            )
        } else {
            write!(f, "({} {} {})", self.get_left(), op_str, self.get_right())
        }
    }
}

#[cfg(test)]
mod basic_functionality {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;

    #[test]
    fn test_basic_arithmetic() {
        let schema = Schema::new(vec![
            Column::new("col1", TypeId::Integer),
            Column::new("col2", TypeId::Decimal),
        ]);
        let rid = RID::new(0, 0);
        let tuple = Tuple::new(&*vec![Value::new(5), Value::new(2.5)], &schema, rid);

        let col1 = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            schema.get_column(0).unwrap().clone(),
            vec![],
        )));
        let col2 = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            schema.get_column(1).unwrap().clone(),
            vec![],
        )));

        let add_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![col1.clone(), col2.clone()],
        ));

        let result = add_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(7.5));

        let mul_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Multiply,
            vec![col1, col2],
        ));

        let result = mul_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(12.5));
    }

    

    #[test]
    fn test_nested_arithmetic() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        // Create (2 + 3) * 4
        let two = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(2),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let three = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(3),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let four = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(4),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let inner_expr = Arc::new(Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![two, three],
        )));

        let outer_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Multiply,
            vec![inner_expr, four],
        ));

        let result = outer_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(20));
    }

    #[test]
    fn test_complex_nested_expressions() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        // Create ((2 + 3) * 4) - (10 / 2)
        let two = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(2),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let three = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(3),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let four = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(4),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let ten = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(10),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        // (2 + 3)
        let add_expr = Arc::new(Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![two, three],
        )));

        // (2 + 3) * 4
        let mul_expr = Arc::new(Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Multiply,
            vec![add_expr, four.clone()],
        )));

        // 10 / 2
        let div_expr = Arc::new(Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Divide,
            vec![
                ten,
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(2),
                    Column::new("const", TypeId::Integer),
                    vec![],
                ))),
            ],
        )));

        // ((2 + 3) * 4) - (10 / 2)
        let final_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Subtract,
            vec![mul_expr, div_expr],
        ));

        let result = final_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(15)); // (5 * 4) - 5 = 20 - 5 = 15
    }

    #[test]
    fn test_clone_with_children() {
        let schema = Schema::new(vec![
            Column::new("a", TypeId::Integer),
            Column::new("b", TypeId::Integer),
        ]);

        let col_a = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            schema.get_column(0).unwrap().clone(),
            vec![],
        )));
        let col_b = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            schema.get_column(1).unwrap().clone(),
            vec![],
        )));

        let original_expr =
            ArithmeticExpression::new(ArithmeticOp::Add, vec![col_a.clone(), col_b.clone()]);

        // Create new children
        let new_col_a = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(5),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let new_col_b = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(10),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let cloned_expr = original_expr.clone_with_children(vec![new_col_a, new_col_b]);

        // Verify the cloned expression has the same operation but new children
        if let Expression::Arithmetic(arith_expr) = cloned_expr.as_ref() {
            assert_eq!(arith_expr.get_op(), ArithmeticOp::Add);
            assert_eq!(arith_expr.get_children().len(), 2);
        } else {
            panic!("Expected ArithmeticExpression");
        }
    }

    #[test]
    fn test_evaluate_join_functionality() {
        let left_schema = Schema::new(vec![Column::new("left_col", TypeId::Integer)]);
        let right_schema = Schema::new(vec![Column::new("right_col", TypeId::Integer)]);

        let left_tuple = Tuple::new(&[Value::new(10)], &left_schema, RID::new(0, 0));
        let right_tuple = Tuple::new(&[Value::new(5)], &right_schema, RID::new(0, 0));

        let left_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            left_schema.get_column(0).unwrap().clone(),
            vec![],
        )));
        let right_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1,
            0,
            right_schema.get_column(0).unwrap().clone(),
            vec![],
        )));

        let add_expr = ArithmeticExpression::new(ArithmeticOp::Add, vec![left_col, right_col]);

        let result = add_expr
            .evaluate_join(&left_tuple, &left_schema, &right_tuple, &right_schema)
            .unwrap();
        assert_eq!(result, Value::new(15));
    }

    #[test]
    fn test_get_child_at() {
        let schema = Schema::new(vec![
            Column::new("a", TypeId::Integer),
            Column::new("b", TypeId::Integer),
        ]);

        let col_a = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            schema.get_column(0).unwrap().clone(),
            vec![],
        )));
        let col_b = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            schema.get_column(1).unwrap().clone(),
            vec![],
        )));

        let expr =
            ArithmeticExpression::new(ArithmeticOp::Multiply, vec![col_a.clone(), col_b.clone()]);

        assert_eq!(
            expr.get_child_at(0).as_ref() as *const _,
            col_a.as_ref() as *const _
        );
        assert_eq!(
            expr.get_child_at(1).as_ref() as *const _,
            col_b.as_ref() as *const _
        );
    }
}

#[cfg(test)]
mod error_handling {
    use super::*;
    use crate::common::exception::ArithmeticExpressionError::Overflow;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;

    #[test]
    fn test_division_by_zero() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        let const5 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(5),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let const0 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(0),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Divide,
            vec![const5, const0],
        ));

        let result = expr.evaluate(&tuple, &schema);
        assert!(matches!(
            result,
            Err(ExpressionError::ArithmeticError(DivisionByZero))
        ));
    }

    #[test]
    fn test_division_by_zero_all_types() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        // Test Integer division by zero
        let int_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(5),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let int_zero = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(0),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let int_div_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Divide,
            vec![int_val, int_zero],
        ));
        assert!(matches!(
            int_div_expr.evaluate(&tuple, &schema),
            Err(ExpressionError::ArithmeticError(DivisionByZero))
        ));

        // Test BigInt division by zero
        let bigint_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(100i64),
            Column::new("const", TypeId::BigInt),
            vec![],
        )));
        let bigint_zero = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(0i64),
            Column::new("const", TypeId::BigInt),
            vec![],
        )));

        let bigint_div_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Divide,
            vec![bigint_val, bigint_zero],
        ));
        assert!(matches!(
            bigint_div_expr.evaluate(&tuple, &schema),
            Err(ExpressionError::ArithmeticError(DivisionByZero))
        ));

        // Test Decimal division by zero
        let decimal_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(5.5),
            Column::new("const", TypeId::Decimal),
            vec![],
        )));
        let decimal_zero = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(0.0),
            Column::new("const", TypeId::Decimal),
            vec![],
        )));

        let decimal_div_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Divide,
            vec![decimal_val, decimal_zero],
        ));
        assert!(matches!(
            decimal_div_expr.evaluate(&tuple, &schema),
            Err(ExpressionError::ArithmeticError(DivisionByZero))
        ));

        // Test mixed type division by zero (BigInt / Integer zero)
        let bigint_val2 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(100i64),
            Column::new("const", TypeId::BigInt),
            vec![],
        )));
        let int_zero2 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(0),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let mixed_div_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Divide,
            vec![bigint_val2, int_zero2],
        ));
        assert!(matches!(
            mixed_div_expr.evaluate(&tuple, &schema),
            Err(ExpressionError::ArithmeticError(DivisionByZero))
        ));
    }

    #[test]
    fn test_overflow_conditions() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        // Test integer overflow on multiplication
        let max_int = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(i32::MAX),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let two = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(2),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let overflow_mul_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Multiply,
            vec![max_int.clone(), two.clone()],
        ));
        assert!(matches!(
            overflow_mul_expr.evaluate(&tuple, &schema),
            Err(ExpressionError::ArithmeticError(Overflow))
        ));

        // Test integer underflow on subtraction
        let min_int = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(i32::MIN),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let one = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let underflow_sub_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Subtract,
            vec![min_int, one],
        ));
        assert!(matches!(
            underflow_sub_expr.evaluate(&tuple, &schema),
            Err(ExpressionError::ArithmeticError(Overflow))
        ));

        // Test BigInt overflow
        let max_bigint = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(i64::MAX),
            Column::new("const", TypeId::BigInt),
            vec![],
        )));
        let bigint_one = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1i64),
            Column::new("const", TypeId::BigInt),
            vec![],
        )));

        let bigint_overflow_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![max_bigint, bigint_one],
        ));
        assert!(matches!(
            bigint_overflow_expr.evaluate(&tuple, &schema),
            Err(ExpressionError::ArithmeticError(Overflow))
        ));
    }
}

#[cfg(test)]
mod display_and_formatting {
    use crate::catalog::{Column, Schema};
    use crate::sql::execution::expressions::abstract_expression::Expression;
    use crate::sql::execution::expressions::arithmetic_expression::{
        ArithmeticExpression, ArithmeticOp,
    };
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::types_db::type_id::TypeId;
    use std::sync::Arc;

    #[test]
    fn test_display_formatting() {
        let schema = Schema::new(vec![
            Column::new("a", TypeId::Integer),
            Column::new("b", TypeId::Integer),
        ]);

        let col_a = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            schema.get_column(0).unwrap().clone(),
            vec![],
        )));
        let col_b = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            schema.get_column(1).unwrap().clone(),
            vec![],
        )));

        let expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![col_a, col_b],
        ));

        // Default format should use column names
        assert_eq!(format!("{}", expr), "(a + b)");

        // Alternate format should use tuple/column indices
        assert_eq!(format!("{:#}", expr), "(Col#0.0 + Col#0.1)");
    }

    #[test]
    fn test_arithmetic_operator_display() {
        assert_eq!(format!("{}", ArithmeticOp::Add), "+");
        assert_eq!(format!("{}", ArithmeticOp::Subtract), "-");
        assert_eq!(format!("{}", ArithmeticOp::Multiply), "*");
        assert_eq!(format!("{}", ArithmeticOp::Divide), "/");
    }
}

#[cfg(test)]
mod type_system {
    use crate::catalog::{Column, Schema};
    use crate::common::exception::ArithmeticExpressionError::Overflow;
    use crate::common::exception::ExpressionError;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
    use crate::sql::execution::expressions::arithmetic_expression::{
        ArithmeticExpression, ArithmeticOp,
    };
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::storage::table::tuple::Tuple;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use std::sync::Arc;

    #[test]
    fn test_type_inference() {
        // Test integer + integer = integer
        let result = ArithmeticExpression::infer_return_type(
            &Column::new("a", TypeId::Integer),
            &Column::new("b", TypeId::Integer),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap().get_type(), TypeId::Integer);

        // Test decimal + decimal = decimal
        let result = ArithmeticExpression::infer_return_type(
            &Column::new("a", TypeId::Decimal),
            &Column::new("b", TypeId::Decimal),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap().get_type(), TypeId::Decimal);

        // Test integer + decimal = decimal
        let result = ArithmeticExpression::infer_return_type(
            &Column::new("a", TypeId::Integer),
            &Column::new("b", TypeId::Decimal),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap().get_type(), TypeId::Decimal);

        // Test invalid combination
        let result = ArithmeticExpression::infer_return_type(
            &Column::new("a", TypeId::VarChar),
            &Column::new("b", TypeId::Integer),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_arithmetic_validation() {
        let schema = Schema::new(vec![
            Column::new("a", TypeId::Integer),
            Column::new("b", TypeId::VarChar),
        ]);

        // Valid arithmetic expression
        let valid_expr = ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![
                Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                    0,
                    0,
                    Column::new("a", TypeId::Integer),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(5),
                    Column::new("const", TypeId::Integer),
                    vec![],
                ))),
            ],
        );
        assert!(valid_expr.validate(&schema).is_ok());

        // Invalid type combination
        let invalid_expr = ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![
                Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                    0,
                    0,
                    Column::new("a", TypeId::Integer),
                    vec![],
                ))),
                Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                    0,
                    1,
                    Column::new("b", TypeId::VarChar),
                    vec![],
                ))),
            ],
        );
        assert!(invalid_expr.validate(&schema).is_err());
    }

    #[test]
    fn test_arithmetic_overflow() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        // Test integer overflow
        let max_int = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(i32::MAX),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let one = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let overflow_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![max_int, one],
        ));

        let result = overflow_expr.evaluate(&tuple, &schema);
        assert!(matches!(
            result,
            Err(ExpressionError::ArithmeticError(Overflow))
        ));
    }

    #[test]
    fn test_nested_arithmetic() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        // Create (2 + 3) * 4
        let two = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(2),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let three = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(3),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let four = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(4),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let inner_expr = Arc::new(Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![two, three],
        )));

        let outer_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Multiply,
            vec![inner_expr, four],
        ));

        let result = outer_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(20));
    }

    #[test]
    fn test_operator_precedence_display() {
        let schema = Schema::new(vec![
            Column::new("a", TypeId::Integer),
            Column::new("b", TypeId::Integer),
            Column::new("c", TypeId::Integer),
        ]);

        // Create a * b + c
        let col_a = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            schema.get_column(0).unwrap().clone(),
            vec![],
        )));
        let col_b = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            schema.get_column(1).unwrap().clone(),
            vec![],
        )));
        let col_c = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2,
            schema.get_column(2).unwrap().clone(),
            vec![],
        )));

        let mul_expr = Arc::new(Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Multiply,
            vec![col_a, col_b],
        )));

        let add_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![mul_expr, col_c],
        ));

        assert_eq!(format!("{}", add_expr), "((a * b) + c)");
        assert_eq!(format!("{:#}", add_expr), "((Col#0.0 * Col#0.1) + Col#0.2)");
    }
}

#[cfg(test)]
mod specific_data_types {
    use crate::catalog::{Column, Schema};
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
    use crate::sql::execution::expressions::arithmetic_expression::{
        ArithmeticExpression, ArithmeticOp,
    };
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::storage::table::tuple::Tuple;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use std::sync::Arc;

    #[test]
    fn test_bigint_operations() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        let bigint_val1 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1000000000000i64),
            Column::new("const", TypeId::BigInt),
            vec![],
        )));
        let bigint_val2 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(2000000000000i64),
            Column::new("const", TypeId::BigInt),
            vec![],
        )));

        // Test BigInt addition
        let add_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![bigint_val1.clone(), bigint_val2.clone()],
        ));
        let result = add_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(3000000000000i64));

        // Test BigInt subtraction
        let sub_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Subtract,
            vec![bigint_val2.clone(), bigint_val1.clone()],
        ));
        let result = sub_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(1000000000000i64));

        // Test BigInt multiplication
        let mul_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Multiply,
            vec![
                bigint_val1.clone(),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(2i64),
                    Column::new("const", TypeId::BigInt),
                    vec![],
                ))),
            ],
        ));
        let result = mul_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(2000000000000i64));

        // Test BigInt division
        let div_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Divide,
            vec![
                bigint_val1,
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(2i64),
                    Column::new("const", TypeId::BigInt),
                    vec![],
                ))),
            ],
        ));
        let result = div_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(500000000000i64));
    }
}

#[cfg(test)]
mod extended_numeric_types {
    use crate::catalog::{Column, Schema};
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
    use crate::sql::execution::expressions::arithmetic_expression::{
        ArithmeticExpression, ArithmeticOp,
    };
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::storage::table::tuple::Tuple;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::{Val, Value};
    use std::sync::Arc;

    #[test]
    fn test_tinyint_operations() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        let tinyint_val1 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_with_type(Val::TinyInt(10), TypeId::TinyInt),
            Column::new("const", TypeId::TinyInt),
            vec![],
        )));
        let tinyint_val2 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_with_type(Val::TinyInt(5), TypeId::TinyInt),
            Column::new("const", TypeId::TinyInt),
            vec![],
        )));

        // Test TinyInt addition
        let add_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![tinyint_val1.clone(), tinyint_val2.clone()],
        ));
        let result = add_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(
            result,
            Value::new_with_type(Val::TinyInt(15), TypeId::TinyInt)
        );

        // Test TinyInt subtraction
        let sub_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Subtract,
            vec![tinyint_val1.clone(), tinyint_val2.clone()],
        ));
        let result = sub_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(
            result,
            Value::new_with_type(Val::TinyInt(5), TypeId::TinyInt)
        );

        // Test TinyInt multiplication
        let mul_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Multiply,
            vec![tinyint_val1.clone(), tinyint_val2.clone()],
        ));
        let result = mul_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(
            result,
            Value::new_with_type(Val::TinyInt(50), TypeId::TinyInt)
        );

        // Test TinyInt division
        let div_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Divide,
            vec![tinyint_val1, tinyint_val2],
        ));
        let result = div_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(
            result,
            Value::new_with_type(Val::TinyInt(2), TypeId::TinyInt)
        );
    }

    #[test]
    fn test_smallint_operations() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        let smallint_val1 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_with_type(Val::SmallInt(1000), TypeId::SmallInt),
            Column::new("const", TypeId::SmallInt),
            vec![],
        )));
        let smallint_val2 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_with_type(Val::SmallInt(250), TypeId::SmallInt),
            Column::new("const", TypeId::SmallInt),
            vec![],
        )));

        // Test SmallInt addition
        let add_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![smallint_val1.clone(), smallint_val2.clone()],
        ));
        let result = add_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(
            result,
            Value::new_with_type(Val::SmallInt(1250), TypeId::SmallInt)
        );

        // Test SmallInt subtraction
        let sub_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Subtract,
            vec![smallint_val1.clone(), smallint_val2.clone()],
        ));
        let result = sub_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(
            result,
            Value::new_with_type(Val::SmallInt(750), TypeId::SmallInt)
        );

        // Test SmallInt multiplication (this will overflow, so we expect an error)
        let mul_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Multiply,
            vec![smallint_val1.clone(), smallint_val2.clone()],
        ));
        let result = mul_expr.evaluate(&tuple, &schema);
        assert!(result.is_err()); // Should overflow since 1000 * 250 = 250000 > i16::MAX

        // Test SmallInt division
        let div_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Divide,
            vec![smallint_val1, smallint_val2],
        ));
        let result = div_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(
            result,
            Value::new_with_type(Val::SmallInt(4), TypeId::SmallInt)
        );
    }

    #[test]
    fn test_float_operations() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        let float_val1 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_with_type(Val::Float(5.5), TypeId::Float),
            Column::new("const", TypeId::Float),
            vec![],
        )));
        let float_val2 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_with_type(Val::Float(2.5), TypeId::Float),
            Column::new("const", TypeId::Float),
            vec![],
        )));

        // Test Float addition
        let add_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![float_val1.clone(), float_val2.clone()],
        ));
        let result = add_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new_with_type(Val::Float(8.0), TypeId::Float));

        // Test Float subtraction
        let sub_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Subtract,
            vec![float_val1.clone(), float_val2.clone()],
        ));
        let result = sub_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new_with_type(Val::Float(3.0), TypeId::Float));

        // Test Float multiplication
        let mul_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Multiply,
            vec![float_val1.clone(), float_val2.clone()],
        ));
        let result = mul_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(
            result,
            Value::new_with_type(Val::Float(13.75), TypeId::Float)
        );

        // Test Float division
        let div_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Divide,
            vec![float_val1, float_val2],
        ));
        let result = div_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new_with_type(Val::Float(2.2), TypeId::Float));
    }
}

#[cfg(test)]
mod mixed_type_operations {
    use crate::catalog::{Column, Schema};
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
    use crate::sql::execution::expressions::arithmetic_expression::{
        ArithmeticExpression, ArithmeticOp,
    };
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::storage::table::tuple::Tuple;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::{Val, Value};
    use std::sync::Arc;

    #[test]
    fn test_mixed_numeric_type_operations() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        // Test Integer + TinyInt
        let int_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(100),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let tinyint_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_with_type(Val::TinyInt(5), TypeId::TinyInt),
            Column::new("const", TypeId::TinyInt),
            vec![],
        )));

        let mixed_expr1 = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![int_val.clone(), tinyint_val.clone()],
        ));
        let result = mixed_expr1.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(105)); // 100 + 5 = 105, promoted to Integer

        // Test SmallInt + BigInt
        let smallint_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_with_type(Val::SmallInt(1000), TypeId::SmallInt),
            Column::new("const", TypeId::SmallInt),
            vec![],
        )));
        let bigint_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(5000000000i64),
            Column::new("const", TypeId::BigInt),
            vec![],
        )));

        let mixed_expr2 = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![smallint_val.clone(), bigint_val.clone()],
        ));
        let result = mixed_expr2.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(5000001000i64)); // 1000 + 5000000000 = 5000001000, promoted to BigInt

        // Test Float + Decimal
        let float_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_with_type(Val::Float(3.14), TypeId::Float),
            Column::new("const", TypeId::Float),
            vec![],
        )));
        let decimal_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(2.71),
            Column::new("const", TypeId::Decimal),
            vec![],
        )));

        let mixed_expr3 = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![float_val.clone(), decimal_val.clone()],
        ));
        let result = mixed_expr3.evaluate(&tuple, &schema).unwrap();
        // Note: Due to floating-point precision, we need to check the approximate value
        if let Val::Decimal(val) = result.get_val() {
            assert!((val - 5.85).abs() < 0.001); // Check within tolerance
        } else {
            panic!("Expected Decimal result");
        }

        // Test TinyInt + SmallInt
        let mixed_expr4 = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Multiply,
            vec![tinyint_val, smallint_val],
        ));
        let result = mixed_expr4.evaluate(&tuple, &schema).unwrap();
        assert_eq!(
            result,
            Value::new_with_type(Val::SmallInt(5000), TypeId::SmallInt)
        ); // 5 * 1000 = 5000, promoted to SmallInt
    }

    #[test]
    fn test_bigint_integer_mixed_operations() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        let bigint_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1000000000000i64),
            Column::new("const", TypeId::BigInt),
            vec![],
        )));
        let int_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(5),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        // Test BigInt + Integer
        let add_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![bigint_val.clone(), int_val.clone()],
        ));
        let result = add_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(1000000000005i64));

        // Test Integer + BigInt
        let add_expr2 = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![int_val.clone(), bigint_val.clone()],
        ));
        let result = add_expr2.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(1000000000005i64));

        // Test BigInt - Integer
        let sub_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Subtract,
            vec![bigint_val.clone(), int_val.clone()],
        ));
        let result = sub_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(999999999995i64));

        // Test BigInt * Integer
        let mul_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Multiply,
            vec![bigint_val.clone(), int_val.clone()],
        ));
        let result = mul_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(5000000000000i64));

        // Test BigInt / Integer
        let div_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Divide,
            vec![bigint_val, int_val],
        ));
        let result = div_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(200000000000i64));
    }

    #[test]
    fn test_bigint_decimal_mixed_operations() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        let bigint_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(100i64),
            Column::new("const", TypeId::BigInt),
            vec![],
        )));
        let decimal_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(2.5),
            Column::new("const", TypeId::Decimal),
            vec![],
        )));

        // Test BigInt + Decimal
        let add_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![bigint_val.clone(), decimal_val.clone()],
        ));
        let result = add_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(102.5));

        // Test Decimal + BigInt
        let add_expr2 = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![decimal_val.clone(), bigint_val.clone()],
        ));
        let result = add_expr2.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(102.5));

        // Test BigInt * Decimal
        let mul_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Multiply,
            vec![bigint_val.clone(), decimal_val.clone()],
        ));
        let result = mul_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(250.0));

        // Test BigInt / Decimal
        let div_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Divide,
            vec![bigint_val, decimal_val],
        ));
        let result = div_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(40.0));
    }

    #[test]
    fn test_complex_nested_expressions() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        // Create ((2 + 3) * 4) - (10 / 2)
        let two = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(2),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let three = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(3),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let four = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(4),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let ten = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(10),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        // (2 + 3)
        let add_expr = Arc::new(Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![two, three],
        )));

        // (2 + 3) * 4
        let mul_expr = Arc::new(Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Multiply,
            vec![add_expr, four.clone()],
        )));

        // 10 / 2
        let div_expr = Arc::new(Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Divide,
            vec![
                ten,
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(2),
                    Column::new("const", TypeId::Integer),
                    vec![],
                ))),
            ],
        )));

        // ((2 + 3) * 4) - (10 / 2)
        let final_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Subtract,
            vec![mul_expr, div_expr],
        ));

        let result = final_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(15)); // (5 * 4) - 5 = 20 - 5 = 15
    }
}

#[cfg(test)]
mod invalid_operations {
    use crate::catalog::{Column, Schema};
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
    use crate::sql::execution::expressions::arithmetic_expression::{
        ArithmeticExpression, ArithmeticOp,
    };
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::storage::table::tuple::Tuple;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::{Val, Value};
    use std::sync::Arc;

    #[test]
    fn test_invalid_type_combinations() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        // Test String + Integer
        let string_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_with_type(Val::VarLen("hello".to_string()), TypeId::VarChar),
            Column::new("const", TypeId::VarChar),
            vec![],
        )));
        let int_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(42),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let invalid_expr1 = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![string_val.clone(), int_val.clone()],
        ));
        let result = invalid_expr1.evaluate(&tuple, &schema);
        assert!(result.is_err());

        // Test Boolean + Decimal
        let bool_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_with_type(Val::Boolean(true), TypeId::Boolean),
            Column::new("const", TypeId::Boolean),
            vec![],
        )));
        let decimal_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(3.14),
            Column::new("const", TypeId::Decimal),
            vec![],
        )));

        let invalid_expr2 = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Multiply,
            vec![bool_val.clone(), decimal_val.clone()],
        ));
        let result = invalid_expr2.evaluate(&tuple, &schema);
        assert!(result.is_err());

        // Test Timestamp + Integer
        let timestamp_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_with_type(Val::Timestamp(1234567890), TypeId::Timestamp),
            Column::new("const", TypeId::Timestamp),
            vec![],
        )));

        let invalid_expr3 = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![timestamp_val.clone(), int_val.clone()],
        ));
        let result = invalid_expr3.evaluate(&tuple, &schema);
        assert!(result.is_err());

        // Test UUID + BigInt
        let uuid_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_with_type(
                Val::UUID("550e8400-e29b-41d4-a716-446655440000".to_string()),
                TypeId::UUID,
            ),
            Column::new("const", TypeId::UUID),
            vec![],
        )));
        let bigint_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1000000i64),
            Column::new("const", TypeId::BigInt),
            vec![],
        )));

        let invalid_expr4 = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Divide,
            vec![uuid_val, bigint_val],
        ));
        let result = invalid_expr4.evaluate(&tuple, &schema);
        assert!(result.is_err());

        // Test Array + Vector
        let array_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_with_type(
                Val::Array(vec![Value::new(1), Value::new(2)]),
                TypeId::Array,
            ),
            Column::new("const", TypeId::Array),
            vec![],
        )));
        let vector_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_with_type(
                Val::Vector(vec![Value::new(3), Value::new(4)]),
                TypeId::Vector,
            ),
            Column::new("const", TypeId::Vector),
            vec![],
        )));

        let invalid_expr5 = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Subtract,
            vec![array_val, vector_val],
        ));
        let result = invalid_expr5.evaluate(&tuple, &schema);
        assert!(result.is_err());

        // Test JSON + Point
        let json_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_with_type(Val::JSON("{\"key\": \"value\"}".to_string()), TypeId::JSON),
            Column::new("const", TypeId::JSON),
            vec![],
        )));
        let point_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_with_type(Val::Point(1.0, 2.0), TypeId::Point),
            Column::new("const", TypeId::Point),
            vec![],
        )));

        let invalid_expr6 = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![json_val, point_val],
        ));
        let result = invalid_expr6.evaluate(&tuple, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_null_value_operations() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        // Test Null + Integer
        let null_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_with_type(Val::Null, TypeId::Invalid),
            Column::new("const", TypeId::Invalid),
            vec![],
        )));
        let int_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(42),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let null_expr1 = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![null_val.clone(), int_val.clone()],
        ));
        let result = null_expr1.evaluate(&tuple, &schema);
        assert!(result.is_err());

        // Test Integer + Null
        let null_expr2 = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Multiply,
            vec![int_val.clone(), null_val.clone()],
        ));
        let result = null_expr2.evaluate(&tuple, &schema);
        assert!(result.is_err());

        // Test Null + Null
        let null_expr3 = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Subtract,
            vec![null_val.clone(), null_val.clone()],
        ));
        let result = null_expr3.evaluate(&tuple, &schema);
        assert!(result.is_err());

        // Test Null / Decimal
        let decimal_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(3.14),
            Column::new("const", TypeId::Decimal),
            vec![],
        )));

        let null_expr4 = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Divide,
            vec![null_val, decimal_val],
        ));
        let result = null_expr4.evaluate(&tuple, &schema);
        assert!(result.is_err());
    }

    #[test]
    #[should_panic(expected = "ArithmeticExpression requires exactly two children")]
    fn test_clone_with_wrong_number_of_children() {
        let schema = Schema::new(vec![Column::new("a", TypeId::Integer)]);
        let col_a = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            schema.get_column(0).unwrap().clone(),
            vec![],
        )));

        let expr = ArithmeticExpression::new(ArithmeticOp::Add, vec![col_a.clone(), col_a.clone()]);

        // This should panic because we're providing only one child
        expr.clone_with_children(vec![col_a]);
    }
}

#[cfg(test)]
mod type_inference {
    use crate::catalog::Column;
    use crate::sql::execution::expressions::arithmetic_expression::ArithmeticExpression;
    use crate::types_db::type_id::TypeId;

    #[test]
    fn test_type_inference_comprehensive() {
        // Test all valid type combinations
        let valid_combinations = vec![
            (TypeId::Integer, TypeId::Integer, TypeId::Integer),
            (TypeId::Decimal, TypeId::Decimal, TypeId::Decimal),
            (TypeId::Integer, TypeId::Decimal, TypeId::Decimal),
            (TypeId::Decimal, TypeId::Integer, TypeId::Decimal),
            (TypeId::BigInt, TypeId::BigInt, TypeId::BigInt),
            (TypeId::BigInt, TypeId::Integer, TypeId::BigInt),
            (TypeId::Integer, TypeId::BigInt, TypeId::BigInt),
            (TypeId::BigInt, TypeId::Decimal, TypeId::Decimal),
            (TypeId::Decimal, TypeId::BigInt, TypeId::Decimal),
        ];

        for (left_type, right_type, expected_type) in valid_combinations {
            let result = ArithmeticExpression::infer_return_type(
                &Column::new("left", left_type),
                &Column::new("right", right_type),
            );
            assert!(
                result.is_ok(),
                "Failed for {:?} + {:?}",
                left_type,
                right_type
            );
            assert_eq!(result.unwrap().get_type(), expected_type);
        }

        // Test invalid combinations
        let invalid_combinations = vec![
            (TypeId::VarChar, TypeId::Integer),
            (TypeId::Integer, TypeId::VarChar),
            (TypeId::Boolean, TypeId::Integer),
            (TypeId::VarChar, TypeId::Decimal),
            (TypeId::Boolean, TypeId::Boolean),
        ];

        for (left_type, right_type) in invalid_combinations {
            let result = ArithmeticExpression::infer_return_type(
                &Column::new("left", left_type),
                &Column::new("right", right_type),
            );
            assert!(
                result.is_err(),
                "Should fail for {:?} + {:?}",
                left_type,
                right_type
            );
        }
    }

    #[test]
    fn test_type_inference_for_unsupported_types() {
        // Test type inference for TinyInt combinations
        let result = ArithmeticExpression::infer_return_type(
            &Column::new("a", TypeId::TinyInt),
            &Column::new("b", TypeId::TinyInt),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap().get_type(), TypeId::TinyInt);

        let result = ArithmeticExpression::infer_return_type(
            &Column::new("a", TypeId::TinyInt),
            &Column::new("b", TypeId::Integer),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap().get_type(), TypeId::Integer);

        // Test type inference for SmallInt combinations
        let result = ArithmeticExpression::infer_return_type(
            &Column::new("a", TypeId::SmallInt),
            &Column::new("b", TypeId::SmallInt),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap().get_type(), TypeId::SmallInt);

        let result = ArithmeticExpression::infer_return_type(
            &Column::new("a", TypeId::SmallInt),
            &Column::new("b", TypeId::BigInt),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap().get_type(), TypeId::BigInt);

        // Test type inference for Float combinations
        let result = ArithmeticExpression::infer_return_type(
            &Column::new("a", TypeId::Float),
            &Column::new("b", TypeId::Float),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap().get_type(), TypeId::Float);

        let result = ArithmeticExpression::infer_return_type(
            &Column::new("a", TypeId::Float),
            &Column::new("b", TypeId::Decimal),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap().get_type(), TypeId::Decimal);

        // Test type inference for completely invalid combinations
        let result = ArithmeticExpression::infer_return_type(
            &Column::new("a", TypeId::VarChar),
            &Column::new("b", TypeId::Boolean),
        );
        assert!(result.is_err());

        let result = ArithmeticExpression::infer_return_type(
            &Column::new("a", TypeId::Timestamp),
            &Column::new("b", TypeId::Date),
        );
        assert!(result.is_err());

        let result = ArithmeticExpression::infer_return_type(
            &Column::new("a", TypeId::UUID),
            &Column::new("b", TypeId::JSON),
        );
        assert!(result.is_err());

        let result = ArithmeticExpression::infer_return_type(
            &Column::new("a", TypeId::Array),
            &Column::new("b", TypeId::Vector),
        );
        assert!(result.is_err());

        let result = ArithmeticExpression::infer_return_type(
            &Column::new("a", TypeId::Point),
            &Column::new("b", TypeId::Binary),
        );
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod validation {
    use crate::catalog::{Column, Schema};
    use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
    use crate::sql::execution::expressions::arithmetic_expression::{
        ArithmeticExpression, ArithmeticOp,
    };
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use std::sync::Arc;

    #[test]
    fn test_validation_with_invalid_schema() {
        let schema = Schema::new(vec![Column::new("valid_col", TypeId::Integer)]);

        // Create expression referencing non-existent column
        let invalid_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            5, // Invalid column index
            Column::new("invalid", TypeId::Integer),
            vec![],
        )));
        let valid_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            schema.get_column(0).unwrap().clone(),
            vec![],
        )));

        let expr = ArithmeticExpression::new(ArithmeticOp::Add, vec![invalid_col, valid_col]);

        // This should fail during validation of the invalid column reference
        assert!(expr.validate(&schema).is_err());
    }

    #[test]
    fn test_validation_with_unsupported_types() {
        let schema = Schema::new(vec![
            Column::new("tinyint_col", TypeId::TinyInt),
            Column::new("smallint_col", TypeId::SmallInt),
            Column::new("float_col", TypeId::Float),
            Column::new("string_col", TypeId::VarChar),
            Column::new("bool_col", TypeId::Boolean),
        ]);

        // Test validation with TinyInt
        let tinyint_expr = ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![
                Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                    0,
                    0,
                    Column::new("tinyint_col", TypeId::TinyInt),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(5),
                    Column::new("const", TypeId::Integer),
                    vec![],
                ))),
            ],
        );
        assert!(tinyint_expr.validate(&schema).is_ok()); // Now supported

        // Test validation with SmallInt
        let smallint_expr = ArithmeticExpression::new(
            ArithmeticOp::Multiply,
            vec![
                Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                    0,
                    1,
                    Column::new("smallint_col", TypeId::SmallInt),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(2.5),
                    Column::new("const", TypeId::Decimal),
                    vec![],
                ))),
            ],
        );
        assert!(smallint_expr.validate(&schema).is_ok()); // Now supported

        // Test validation with Float
        let float_expr = ArithmeticExpression::new(
            ArithmeticOp::Divide,
            vec![
                Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                    0,
                    2,
                    Column::new("float_col", TypeId::Float),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(1000i64),
                    Column::new("const", TypeId::BigInt),
                    vec![],
                ))),
            ],
        );
        assert!(float_expr.validate(&schema).is_ok()); // Now supported

        // Test validation with String
        let string_expr = ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![
                Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                    0,
                    3,
                    Column::new("string_col", TypeId::VarChar),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(42),
                    Column::new("const", TypeId::Integer),
                    vec![],
                ))),
            ],
        );
        assert!(string_expr.validate(&schema).is_err());

        // Test validation with Boolean
        let bool_expr = ArithmeticExpression::new(
            ArithmeticOp::Subtract,
            vec![
                Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                    0,
                    4,
                    Column::new("bool_col", TypeId::Boolean),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(3.14),
                    Column::new("const", TypeId::Decimal),
                    vec![],
                ))),
            ],
        );
        assert!(bool_expr.validate(&schema).is_err());
    }
}

#[cfg(test)]
mod edge_cases {
    use crate::catalog::{Column, Schema};
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
    use crate::sql::execution::expressions::arithmetic_expression::{
        ArithmeticExpression, ArithmeticOp,
    };
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::storage::table::tuple::Tuple;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::{Val, Value};
    use std::sync::Arc;

    #[test]
    fn test_edge_case_decimal_operations() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        // Test very small decimal numbers
        let small_decimal1 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(0.000001),
            Column::new("const", TypeId::Decimal),
            vec![],
        )));
        let small_decimal2 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(0.000002),
            Column::new("const", TypeId::Decimal),
            vec![],
        )));

        let add_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![small_decimal1, small_decimal2],
        ));

        let result = add_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(0.000003));

        // Test negative decimal operations
        let neg_decimal = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(-5.5),
            Column::new("const", TypeId::Decimal),
            vec![],
        )));
        let pos_decimal = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(3.2),
            Column::new("const", TypeId::Decimal),
            vec![],
        )));

        let sub_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Subtract,
            vec![neg_decimal, pos_decimal],
        ));

        let result = sub_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(-8.7));
    }

    #[test]
    fn test_extreme_value_edge_cases() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        // Test with maximum values
        let max_int = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(i32::MAX),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let max_bigint = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(i64::MAX),
            Column::new("const", TypeId::BigInt),
            vec![],
        )));

        // Test Integer max + BigInt max (should overflow)
        let overflow_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![max_int.clone(), max_bigint.clone()],
        ));
        let result = overflow_expr.evaluate(&tuple, &schema);
        assert!(result.is_err());

        // Test with minimum values
        let min_int = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(i32::MIN),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        // Test Integer min - BigInt max (should underflow)
        let underflow_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Subtract,
            vec![min_int, max_bigint],
        ));
        let result = underflow_expr.evaluate(&tuple, &schema);
        assert!(result.is_err());

        // Test very large decimal values
        let large_decimal1 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(f64::MAX / 2.0),
            Column::new("const", TypeId::Decimal),
            vec![],
        )));
        let large_decimal2 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(f64::MAX / 2.0),
            Column::new("const", TypeId::Decimal),
            vec![],
        )));

        // Test large decimal multiplication (might overflow to infinity)
        let large_mul_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Multiply,
            vec![large_decimal1, large_decimal2],
        ));
        let result = large_mul_expr.evaluate(&tuple, &schema).unwrap();
        // Result should be infinity
        if let Val::Decimal(val) = result.get_val() {
            assert!(val.is_infinite());
        } else {
            panic!("Expected decimal result");
        }

        // Test very small decimal values
        let tiny_decimal = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(f64::MIN_POSITIVE),
            Column::new("const", TypeId::Decimal),
            vec![],
        )));
        let large_divisor = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(f64::MAX),
            Column::new("const", TypeId::Decimal),
            vec![],
        )));

        // Test tiny decimal / large decimal (should approach zero)
        let tiny_div_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Divide,
            vec![tiny_decimal, large_divisor],
        ));
        let result = tiny_div_expr.evaluate(&tuple, &schema).unwrap();
        if let Val::Decimal(val) = result.get_val() {
            assert!(*val >= 0.0 && *val < 1e-300); // Very close to zero
        } else {
            panic!("Expected decimal result");
        }
    }
}
