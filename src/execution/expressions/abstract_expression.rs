use crate::catalogue::column::Column;
use crate::catalogue::schema::Schema;
use crate::execution::expressions::arithmetic_expression::ArithmeticExpression;
// use crate::execution::expressions::array_expression::ArrayExpression;
use crate::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::execution::expressions::comparison_expression::ComparisonExpression;
use crate::execution::expressions::constant_value_expression::ConstantExpression;
use crate::execution::expressions::logic_expression::LogicExpression;
use crate::execution::expressions::string_expression::StringExpression;
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use std::fmt;
use std::fmt::Display;
use std::rc::Rc;
use crate::common::exception::{ArrayExpressionError, ExpressionError};

#[derive(Debug, Clone)]
pub enum Expression {
    Constant(ConstantExpression),
    ColumnRef(ColumnRefExpression),
    Arithmetic(ArithmeticExpression),
    Comparison(ComparisonExpression),
    Logic(LogicExpression),
    String(StringExpression),
    // Array(ArrayExpression),
}

pub trait ExpressionOps {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError>;
    fn evaluate_join(&self, left_tuple: &Tuple, left_schema: &Schema, right_tuple: &Tuple, right_schema: &Schema) -> Result<Value, ExpressionError>;
    fn get_child_at(&self, child_idx: usize) -> &Rc<Expression>;
    fn get_children(&self) -> &Vec<Rc<Expression>>;
    fn get_return_type(&self) -> &Column;
    fn clone_with_children(&self, children: Vec<Rc<Expression>>) -> Rc<Expression>;
}

impl ExpressionOps for Expression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        match self {
            Self::Constant(expr) => Ok(expr.get_value().clone()),
            Self::ColumnRef(expr) => Ok(tuple.get_value(expr.get_column_index()).clone()),
            Self::Arithmetic(expr) => expr.evaluate(tuple, schema),
            Self::Comparison(expr) => expr.evaluate(tuple, schema),
            Self::Logic(expr) => expr.evaluate(tuple, schema),
            Self::String(expr) => expr.evaluate(tuple, schema)
        }
            // Self::Array(expr) => expr.evaluate(tuple, schema).map_err(|e| e),
    }

    fn evaluate_join(&self, left_tuple: &Tuple, left_schema: &Schema, right_tuple: &Tuple, right_schema: &Schema) -> Result<Value, ExpressionError> {
        match self {
            Self::Constant(expr) => Ok(expr.get_value().clone()),
            Self::ColumnRef(expr) => expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema),
            Self::Arithmetic(expr) => expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema),
            Self::Comparison(expr) => expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema),
            Self::Logic(expr) => expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema),
            Self::String(expr) => expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema),
            // Self::Array(expr) => expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema).map_err(|e| e),
        }
    }

    fn get_child_at(&self, child_idx: usize) -> &Rc<Expression> {
        match self {
            Self::Arithmetic(expr) => expr.get_child_at(child_idx),
            Self::Constant(expr) => expr.get_child_at(child_idx),
            Self::ColumnRef(expr) => expr.get_child_at(child_idx),
            Self::Comparison(expr) => expr.get_child_at(child_idx),
            Self::Logic(expr) => expr.get_child_at(child_idx),
            Self::String(expr) => expr.get_child_at(child_idx),
        }
    }

    fn get_children(&self) -> &Vec<Rc<Expression>> {
        match self {
            Self::Arithmetic(expr) => expr.get_children(),
            Self::Constant(expr) => expr.get_children(),
            Self::ColumnRef(expr) => expr.get_children(),
            Self::Comparison(expr) => expr.get_children(),
            Self::Logic(expr) => expr.get_children(),
            Self::String(expr) => expr.get_children(),
        }
    }

    fn get_return_type(&self) -> &Column {
        match self {
            Self::Constant(expr) => expr.get_return_type(),
            Self::ColumnRef(expr) => expr.get_return_type(),
            Self::Arithmetic(expr) => expr.get_return_type(),
            Self::Comparison(expr) => expr.get_return_type(),
            Self::Logic(expr) => expr.get_return_type(),
            Self::String(expr) => expr.get_return_type(),
            // Self::Array(expr) => expr.get_return_type(),
        }
    }

    fn clone_with_children(&self, children: Vec<Rc<Expression>>) -> Rc<Expression> {
        match self {
            Self::Arithmetic(expr) => expr.clone_with_children(children),
            Self::Constant(expr) => expr.clone_with_children(children),
            Self::ColumnRef(expr) => expr.clone_with_children(children),
            Self::Comparison(expr) => expr.clone_with_children(children),
            Self::Logic(expr) => expr.clone_with_children(children),
            Self::String(expr) => expr.clone_with_children(children),
        }
    }
}

impl Expression {
   pub fn get_children(&self) -> Vec<&Expression> {
        match self {
            Self::Constant(_) | Self::ColumnRef(_) => vec![],
            Self::Arithmetic(expr) => vec![expr.get_left(), expr.get_right()],
            Self::Comparison(expr) => vec![expr.get_left(), expr.get_right()],
            Self::Logic(expr) => vec![expr.get_left(), expr.get_right()],
            Self::String(expr) => vec![expr.get_arg()],
            // Self::Array(expr) => expr.get_children().iter().map(AsRef::as_ref).collect(),
        }
    }

   pub fn clone_with_children(&self, children: Vec<Rc<Expression>>) -> Result<Rc<Expression>, ExpressionError> {
        match self {
            Self::Constant(expr) => Ok(Rc::new(Self::Constant(expr.clone()))),
            Self::ColumnRef(expr) => Ok(Rc::new(Self::ColumnRef(expr.clone()))),
            Self::Arithmetic(expr) => Ok(Rc::new(Self::Arithmetic(ArithmeticExpression::new(
                children[0].clone(),
                children[1].clone(),
                expr.get_op(),
                children
            )))),
            Self::Comparison(expr) => Ok(Rc::new(Self::Comparison(ComparisonExpression::new(
                children[0].clone(),
                children[1].clone(),
                expr.get_comp_type(),
                children
            )))),
            Self::Logic(expr) => Ok(Rc::new(Self::Logic(LogicExpression::new(
                children[0].clone(),
                children[1].clone(),
                expr.get_logic_type(),
                children
            )))),
            Self::String(expr) => Ok(Rc::new(Self::String(StringExpression::new(
                children[0].clone(),
                expr.get_expr_type(),
                children
            )))),
            // Self::Array(_) => Ok(Rc::new(Self::Array(ArrayExpression::new(children)))),
        }
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Constant(expr) => write!(f, "Constant({})", expr.get_value()),
            Self::ColumnRef(expr) => write!(f, "{}", expr.get_column_index()),
            Self::Arithmetic(expr) => write!(f, "(Col#{} {} Col#{})", expr.get_left(), expr.get_op(), expr.get_right()),
            Self::Comparison(expr) => write!(f, "(Col#{} {} Col#{})", expr.get_left(), expr.get_comp_type(), expr.get_right()),
            Self::Logic(expr) => write!(f, "({} {} {})", expr.get_left(), expr.get_logic_type(), expr.get_right()),
            Self::String(expr) => write!(f, "{}({})", expr.get_expr_type(), expr.get_arg()),
            // Self::Array(expr) => {
            //     write!(f, "[")?;
            //     let mut iter = expr.get_children().iter();
            //     if let Some(first) = iter.next() {
            //         write!(f, "{}", first)?;
            //         for child in iter {
            //             write!(f, ", {}", child)?;
            //         }
            //     }
            //     write!(f, "]")
            // }
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn constant_expression() {
        let value = Value::new(42);
        let ret_type = Column::new("const", TypeId::Integer);
        let expr = Expression::Constant(ConstantExpression::new(value.clone(), ret_type, vec![]));

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(vec![], schema.clone(), 0);

        assert_eq!(expr.evaluate(&tuple, &schema).unwrap(), value);
        assert_eq!(expr.get_children().len(), 0);
        assert_eq!(expr.to_string(), "Constant(Integer(42))");
    }
}