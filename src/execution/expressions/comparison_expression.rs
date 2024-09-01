use crate::catalogue::column::Column;
use crate::catalogue::schema::Schema;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::Value;
use std::fmt::Display;
use std::rc::Rc;

#[derive(Debug, Clone, Copy)]
pub enum ComparisonType {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

#[derive(Debug, Clone)]
pub struct ComparisonExpression {
    left: Rc<Expression>,
    right: Rc<Expression>,
    comp_type: ComparisonType,
    ret_type: Column,
}

impl ComparisonExpression {
    pub fn new(left: Rc<Expression>, right: Rc<Expression>, comp_type: ComparisonType) -> Self {
        let ret_type = Column::new("<val>", TypeId::Boolean);
        Self {
            left,
            right,
            comp_type,
            ret_type,
        }
    }

    pub fn get_left(&self) -> &Rc<Expression> {
        &self.left
    }

    pub fn get_right(&self) -> &Rc<Expression> {
        &self.right
    }

    pub fn get_comp_type(&self) -> ComparisonType {
        self.comp_type
    }

    pub fn get_ret_type(&self) -> &Column {
        &self.ret_type
    }

    pub fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, String> {
        let lhs = self.left.evaluate(tuple, schema)?;
        let rhs = self.right.evaluate(tuple, schema)?;
        let comparison_result = self.perform_comparison(&lhs, &rhs)?;
        Ok(Value::new(comparison_result))
    }

    pub fn evaluate_join(&self, left_tuple: &Tuple, left_schema: &Schema, right_tuple: &Tuple, right_schema: &Schema) -> Result<Value, String> {
        let lhs = self.left.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        let rhs = self.right.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        let comparison_result = self.perform_comparison(&lhs, &rhs)?;
        Ok(Value::new(comparison_result))
    }

    fn perform_comparison(&self, lhs: &Value, rhs: &Value) -> Result<CmpBool, String> {
        match self.comp_type {
            ComparisonType::Equal => Ok(lhs.compare_equals(rhs)),
            ComparisonType::NotEqual => Ok(lhs.compare_not_equals(rhs)),
            ComparisonType::LessThan => Ok(lhs.compare_less_than(rhs)),
            ComparisonType::LessThanOrEqual => Ok(lhs.compare_less_than_equals(rhs)),
            ComparisonType::GreaterThan => Ok(lhs.compare_greater_than(rhs)),
            ComparisonType::GreaterThanOrEqual => Ok(lhs.compare_greater_than_equals(rhs)),
        }
    }
}

impl Display for ComparisonType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComparisonType::Equal => write!(f, "="),
            ComparisonType::NotEqual => write!(f, "!="),
            ComparisonType::LessThan => write!(f, "<"),
            ComparisonType::LessThanOrEqual => write!(f, "<="),
            ComparisonType::GreaterThan => write!(f, ">"),
            ComparisonType::GreaterThanOrEqual => write!(f, ">="),
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_comparison_expression() {
        let schema = Schema::new(vec![
            Column::new("col1", TypeId::Integer),
            Column::new("col2", TypeId::Integer),
        ]);

        let tuple = Tuple::new(vec![Value::new(5), Value::new(10)], schema.clone(), 0);

        let col1 = Rc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            schema.get_column(0).unwrap().clone())));

        let col2 = Rc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1,
            schema.get_column(1).unwrap().clone())));

        let less_than_expr = Expression::Comparison(ComparisonExpression::new(
            col1.clone(),
            col2.clone(),
            ComparisonType::LessThan,
        ));

        let result = less_than_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(true));

        let equal_expr = Expression::Comparison(ComparisonExpression::new(
            col1,
            col2,
            ComparisonType::Equal,
        ));

        let result = equal_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(false));

        assert_eq!(less_than_expr.to_string(), "(Col#0 < Col#1)");
        assert_eq!(equal_expr.to_string(), "(Col#0 = Col#1)");
    }
}

impl ComparisonExpression {}


