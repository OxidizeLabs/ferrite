use crate::catalogue::column::Column;
use crate::catalogue::schema::Schema;
use crate::execution::expressions::arithmetic_expression::ArithmeticExpression;
use crate::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::execution::expressions::comparison_expression::ComparisonExpression;
use crate::execution::expressions::constant_value_expression::ConstantExpression;
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::rc::Rc;


#[derive(Debug, Clone)]
pub enum Expression {
    Constant(ConstantExpression),
    ColumnRef(ColumnRefExpression),
    Arithmetic(ArithmeticExpression),
    Comparison(ComparisonExpression),
}

impl Expression {
    pub fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, String> {
        match self {
            Expression::Constant(expr) => Ok(expr.get_value().clone()),
            Expression::ColumnRef(expr) => Ok(tuple.get_value(expr.get_column_index()).clone()),
            Expression::Arithmetic(expr) => expr.evaluate(tuple, schema),
            Expression::Comparison(expr) => expr.evaluate(tuple, schema),
        }
    }

    pub fn evaluate_join(&self, left_tuple: &Tuple, left_schema: &Schema, right_tuple: &Tuple, right_schema: &Schema) -> Result<Value, String> {
        match self {
            Expression::Constant(expr) => Ok(expr.get_value().clone()),
            Expression::ColumnRef(expr) => {
                // Determine which tuple to use based on the column index
                if expr.get_column_index() < left_schema.get_column_count() as usize {
                    let l_value = left_tuple.get_value(expr.get_column_index());
                    Ok(l_value.clone())
                } else {
                    let r_value = right_tuple.get_value(expr.get_column_index() - left_schema.get_column_count() as usize);
                    Ok(r_value.clone())
                }
            }
            Expression::Arithmetic(expr) => expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema),
            Expression::Comparison(expr) => expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema),
            _ => Err("Evaluate Join Error".to_string())
        }
    }

    pub fn get_children(&self) -> Vec<&Expression> {
        match self {
            Expression::Constant(_) | Expression::ColumnRef(_) => vec![],
            Expression::Arithmetic(expr) => vec![&expr.get_left(), &expr.get_right()],
            Expression::Comparison(expr) => vec![&expr.get_left(), &expr.get_right()],
        }
    }

    pub fn get_return_type(&self) -> &Column {
        match self {
            Expression::Constant(expr) => &expr.get_ret_type(),
            Expression::ColumnRef(expr) => &expr.get_ret_type(),
            Expression::Arithmetic(expr) => &expr.get_ret_type(),
            Expression::Comparison(expr) => &expr.get_ret_type(),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            Expression::Constant(expr) => format!("Constant({})", expr.get_value()),
            Expression::ColumnRef(expr) => format!("{}", expr.get_column_index()),
            Expression::Arithmetic(expr) => format!("(Col#{} {} Col#{})", expr.get_left(), expr.get_op(), expr.get_right()),
            Expression::Comparison(expr) => format!("(Col#{} {} Col#{})", expr.get_left(), expr.get_comp_type(), expr.get_right()),
        }
    }

    pub fn clone_with_children(&self, children: Vec<Rc<Expression>>) -> Rc<Expression> {
        match self {
            Expression::Constant(expr) => Rc::new(Expression::Constant(expr.clone())),
            Expression::ColumnRef(expr) => Rc::new(Expression::ColumnRef(expr.clone())),
            Expression::Arithmetic(expr) => Rc::new(Expression::Arithmetic(ArithmeticExpression::new(
                children[0].clone(),
                children[1].clone(),
                expr.get_op()).unwrap()
            )),
            Expression::Comparison(expr) => Rc::new(Expression::Comparison(ComparisonExpression::new(
                children[0].clone(),
                children[1].clone(),
                expr.get_comp_type(),
            )))
        }
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn constant_expression() {
        let value = Value::new(42);
        let ret_type = Column::new("const", TypeId::Integer);
        let expr = Expression::Constant(ConstantExpression::new(value.clone(), ret_type));

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(vec![], schema.clone(), 0);

        assert_eq!(expr.evaluate(&tuple, &schema).unwrap(), value);
        assert_eq!(expr.get_children().len(), 0);
        assert_eq!(expr.to_string(), "Constant(Integer(42))");
    }
}