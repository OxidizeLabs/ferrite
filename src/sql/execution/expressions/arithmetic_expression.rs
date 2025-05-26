use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ArithmeticExpressionError::{DivisionByZero, Unknown};
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
            (TypeId::Integer, TypeId::Integer) => {
                Ok(Column::new("arithmetic_result", TypeId::Integer))
            }
            (TypeId::Decimal, TypeId::Decimal) => {
                Ok(Column::new("arithmetic_result", TypeId::Decimal))
            }
            (TypeId::Integer, TypeId::Decimal) | (TypeId::Decimal, TypeId::Integer) => {
                Ok(Column::new("arithmetic_result", TypeId::Decimal))
            }
            (TypeId::BigInt, TypeId::BigInt) => {
                Ok(Column::new("arithmetic_result", TypeId::BigInt))
            }
            (TypeId::BigInt, TypeId::Integer) | (TypeId::Integer, TypeId::BigInt) => {
                Ok(Column::new("arithmetic_result", TypeId::BigInt))
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
                    .ok_or_else(|| ExpressionError::ArithmeticError(Unknown))
            }
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
                    .ok_or_else(|| ExpressionError::ArithmeticError(Unknown))
            }
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
                    .ok_or_else(|| ExpressionError::ArithmeticError(Unknown))
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
                    .ok_or_else(|| ExpressionError::ArithmeticError(Unknown))
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
                    .ok_or_else(|| ExpressionError::ArithmeticError(Unknown))
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
                    .ok_or_else(|| ExpressionError::ArithmeticError(Unknown))
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
                    .ok_or_else(|| ExpressionError::ArithmeticError(Unknown))
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
                    .ok_or_else(|| ExpressionError::ArithmeticError(Unknown))
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
            (TypeId::Integer, TypeId::Integer)
            | (TypeId::Decimal, TypeId::Decimal)
            | (TypeId::Integer, TypeId::Decimal)
            | (TypeId::Decimal, TypeId::Integer)
            | (TypeId::BigInt, TypeId::BigInt)
            | (TypeId::BigInt, TypeId::Integer)
            | (TypeId::Integer, TypeId::BigInt)
            | (TypeId::BigInt, TypeId::Decimal)
            | (TypeId::Decimal, TypeId::BigInt) => Ok(()),
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
mod unit_tests {
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
    fn test_integer_operations() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        let const5 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(5),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let const3 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(3),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        // Test all arithmetic operations
        let ops = vec![
            (ArithmeticOp::Add, 8),
            (ArithmeticOp::Subtract, 2),
            (ArithmeticOp::Multiply, 15),
            (ArithmeticOp::Divide, 1),
        ];

        for (op, expected) in ops {
            let expr = Expression::Arithmetic(ArithmeticExpression::new(
                op,
                vec![const5.clone(), const3.clone()],
            ));
            let result = expr.evaluate(&tuple, &schema).unwrap();
            assert_eq!(result, Value::new(expected));
        }
    }

    #[test]
    fn test_decimal_operations() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        let const5_5 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(5.5),
            Column::new("const", TypeId::Decimal),
            vec![],
        )));
        let const2_5 = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(2.5),
            Column::new("const", TypeId::Decimal),
            vec![],
        )));

        let ops = vec![
            (ArithmeticOp::Add, 8.0),
            (ArithmeticOp::Subtract, 3.0),
            (ArithmeticOp::Multiply, 13.75),
            (ArithmeticOp::Divide, 2.2),
        ];

        for (op, expected) in ops {
            let expr = Expression::Arithmetic(ArithmeticExpression::new(
                op,
                vec![const5_5.clone(), const2_5.clone()],
            ));
            let result = expr.evaluate(&tuple, &schema).unwrap();
            assert_eq!(result, Value::new(expected));
        }
    }

    #[test]
    fn test_mixed_type_operations() {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));

        let int_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(5),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let decimal_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(2.5),
            Column::new("const", TypeId::Decimal),
            vec![],
        )));

        let ops = vec![
            (ArithmeticOp::Add, 7.5),
            (ArithmeticOp::Subtract, 2.5),
            (ArithmeticOp::Multiply, 12.5),
            (ArithmeticOp::Divide, 2.0),
        ];

        for (op, expected) in ops {
            let expr = Expression::Arithmetic(ArithmeticExpression::new(
                op,
                vec![int_val.clone(), decimal_val.clone()],
            ));
            let result = expr.evaluate(&tuple, &schema).unwrap();
            assert_eq!(result, Value::new(expected));
        }
    }

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
            Err(ExpressionError::ArithmeticError(Unknown))
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
            vec![bigint_val1.clone(), Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(2i64),
                Column::new("const", TypeId::BigInt),
                vec![],
            )))],
        ));
        let result = mul_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(2000000000000i64));

        // Test BigInt division
        let div_expr = Expression::Arithmetic(ArithmeticExpression::new(
            ArithmeticOp::Divide,
            vec![bigint_val1, Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(2i64),
                Column::new("const", TypeId::BigInt),
                vec![],
            )))],
        ));
        let result = div_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(500000000000i64));
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
            Err(ExpressionError::ArithmeticError(Unknown))
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
            Err(ExpressionError::ArithmeticError(Unknown))
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
            Err(ExpressionError::ArithmeticError(Unknown))
        ));
    }

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
            assert!(result.is_ok(), "Failed for {:?} + {:?}", left_type, right_type);
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
            assert!(result.is_err(), "Should fail for {:?} + {:?}", left_type, right_type);
        }
    }

    #[test]
    fn test_evaluate_join_functionality() {
        let left_schema = Schema::new(vec![
            Column::new("left_col", TypeId::Integer),
        ]);
        let right_schema = Schema::new(vec![
            Column::new("right_col", TypeId::Integer),
        ]);

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

        let add_expr = ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![left_col, right_col],
        );

        let result = add_expr.evaluate_join(&left_tuple, &left_schema, &right_tuple, &right_schema).unwrap();
        assert_eq!(result, Value::new(15));
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

        let original_expr = ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![col_a.clone(), col_b.clone()],
        );

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
    #[should_panic(expected = "ArithmeticExpression requires exactly two children")]
    fn test_clone_with_wrong_number_of_children() {
        let schema = Schema::new(vec![Column::new("a", TypeId::Integer)]);
        let col_a = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            schema.get_column(0).unwrap().clone(),
            vec![],
        )));

        let expr = ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![col_a.clone(), col_a.clone()],
        );

        // This should panic because we're providing only one child
        expr.clone_with_children(vec![col_a]);
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

        let expr = ArithmeticExpression::new(
            ArithmeticOp::Multiply,
            vec![col_a.clone(), col_b.clone()],
        );

        assert_eq!(expr.get_child_at(0).as_ref() as *const _, col_a.as_ref() as *const _);
        assert_eq!(expr.get_child_at(1).as_ref() as *const _, col_b.as_ref() as *const _);
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
            vec![ten, Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(2),
                Column::new("const", TypeId::Integer),
                vec![],
            )))],
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
    fn test_arithmetic_operator_display() {
        assert_eq!(format!("{}", ArithmeticOp::Add), "+");
        assert_eq!(format!("{}", ArithmeticOp::Subtract), "-");
        assert_eq!(format!("{}", ArithmeticOp::Multiply), "*");
        assert_eq!(format!("{}", ArithmeticOp::Divide), "/");
    }

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
    fn test_validation_with_invalid_schema() {
        let schema = Schema::new(vec![
            Column::new("valid_col", TypeId::Integer),
        ]);

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

        let expr = ArithmeticExpression::new(
            ArithmeticOp::Add,
            vec![invalid_col, valid_col],
        );

        // This should fail during validation of the invalid column reference
        assert!(expr.validate(&schema).is_err());
    }
}
