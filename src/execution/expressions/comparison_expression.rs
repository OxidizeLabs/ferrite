use crate::catalogue::column::Column;
use crate::catalogue::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::Value;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ComparisonType {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ComparisonExpression {
    left: Arc<Expression>,
    right: Arc<Expression>,
    comp_type: ComparisonType,
    ret_type: Column,
    children: Vec<Arc<Expression>>,
}

impl ComparisonExpression {
    pub fn new(
        left: Arc<Expression>,
        right: Arc<Expression>,
        comp_type: ComparisonType,
        children: Vec<Arc<Expression>>,
    ) -> Self {
        let ret_type = Column::new("<val>", TypeId::Boolean);
        Self {
            left,
            right,
            comp_type,
            ret_type,
            children,
        }
    }

    pub fn get_left(&self) -> &Arc<Expression> {
        &self.left
    }

    pub fn get_right(&self) -> &Arc<Expression> {
        &self.right
    }

    pub fn get_comp_type(&self) -> ComparisonType {
        self.comp_type
    }

    fn perform_comparison(&self, lhs: &Value, rhs: &Value) -> Result<CmpBool, ExpressionError> {
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

impl ExpressionOps for ComparisonExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let lhs = self.left.evaluate(tuple, schema)?;
        let rhs = self.right.evaluate(tuple, schema)?;
        let comparison_result = self.perform_comparison(&lhs, &rhs)?;
        Ok(Value::new(comparison_result))
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        let lhs = self
            .left
            .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        let rhs = self
            .right
            .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        let comparison_result = self.perform_comparison(&lhs, &rhs)?;
        Ok(Value::new(comparison_result))
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
        if children.len() != 2 {
            panic!("ComparisonExpression requires exactly two children");
        }

        Arc::new(Expression::Comparison(ComparisonExpression {
            left: children[0].clone(),
            right: children[1].clone(),
            ret_type: self.ret_type.clone(),
            children,
            comp_type: ComparisonType::Equal,
        }))
    }
}

impl Display for ComparisonType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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
    use crate::common::rid::RID;
    use crate::execution::expressions::column_value_expression::ColumnRefExpression;

    #[test]
    fn comparison_expression() {
        let schema = Schema::new(vec![
            Column::new("col1", TypeId::Integer),
            Column::new("col2", TypeId::Integer),
        ]);
        let rid = RID::new(0, 0);

        let tuple = Tuple::new(&*vec![Value::new(5), Value::new(10)], schema.clone(), rid);

        let col1 = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            schema.get_column(0).unwrap().clone(),
            vec![],
        )));

        let col2 = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1,
            1,
            schema.get_column(1).unwrap().clone(),
            vec![],
        )));

        let less_than_expr = Expression::Comparison(ComparisonExpression::new(
            col1.clone(),
            col2.clone(),
            ComparisonType::LessThan,
            vec![],
        ));

        let result = less_than_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(true));

        let equal_expr = Expression::Comparison(ComparisonExpression::new(
            col1,
            col2,
            ComparisonType::Equal,
            vec![],
        ));

        let result = equal_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(false));

        assert_eq!(less_than_expr.to_string(), "(Col#0.0 < Col#1.1)");
        assert_eq!(equal_expr.to_string(), "(Col#0.0 = Col#1.1)");
    }
}
