use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::Val::Null;
use crate::types_db::value::Value;
use log::debug;
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
    IsNotNull,
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
            ComparisonType::IsNotNull => write!(f, "IS NOT NULL"),
        }
    }
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
        // Handle IS NOT NULL comparison first, as it needs to handle NULL values explicitly
        if self.comp_type == ComparisonType::IsNotNull {
            return Ok(if lhs.is_null() {
                CmpBool::CmpFalse
            } else {
                CmpBool::CmpTrue
            });
        }

        // Handle null values for other comparison types
        if lhs.is_null() || rhs.is_null() {
            return Ok(CmpBool::CmpNull);
        }

        // If types are different, try casting to make them comparable
        if lhs.get_type_id() != rhs.get_type_id() {
            // Try casting left to right type first
            if let Ok(cast_lhs) = lhs.cast_to(rhs.get_type_id()) {
                let cast_result = match self.comp_type {
                    ComparisonType::Equal => cast_lhs.compare_equals(rhs),
                    ComparisonType::NotEqual => cast_lhs.compare_not_equals(rhs),
                    ComparisonType::LessThan => cast_lhs.compare_less_than(rhs),
                    ComparisonType::LessThanOrEqual => cast_lhs.compare_less_than_equals(rhs),
                    ComparisonType::GreaterThan => cast_lhs.compare_greater_than(rhs),
                    ComparisonType::GreaterThanOrEqual => cast_lhs.compare_greater_than_equals(rhs),
                    ComparisonType::IsNotNull => unreachable!(),
                };
                return Ok(cast_result);
            }

            // Try casting right to left type
            if let Ok(cast_rhs) = rhs.cast_to(lhs.get_type_id()) {
                let cast_result = match self.comp_type {
                    ComparisonType::Equal => lhs.compare_equals(&cast_rhs),
                    ComparisonType::NotEqual => lhs.compare_not_equals(&cast_rhs),
                    ComparisonType::LessThan => lhs.compare_less_than(&cast_rhs),
                    ComparisonType::LessThanOrEqual => lhs.compare_less_than_equals(&cast_rhs),
                    ComparisonType::GreaterThan => lhs.compare_greater_than(&cast_rhs),
                    ComparisonType::GreaterThanOrEqual => {
                        lhs.compare_greater_than_equals(&cast_rhs)
                    }
                    ComparisonType::IsNotNull => unreachable!(),
                };
                return Ok(cast_result);
            }

            // If casting fails, types are incompatible for comparison
            return Err(ExpressionError::TypeMismatch {
                expected: lhs.get_type_id(),
                actual: rhs.get_type_id(),
            });
        }

        // Types are the same, perform direct comparison
        let result = match self.comp_type {
            ComparisonType::Equal => lhs.compare_equals(rhs),
            ComparisonType::NotEqual => lhs.compare_not_equals(rhs),
            ComparisonType::LessThan => lhs.compare_less_than(rhs),
            ComparisonType::LessThanOrEqual => lhs.compare_less_than_equals(rhs),
            ComparisonType::GreaterThan => lhs.compare_greater_than(rhs),
            ComparisonType::GreaterThanOrEqual => lhs.compare_greater_than_equals(rhs),
            ComparisonType::IsNotNull => unreachable!(),
        };

        Ok(result)
    }
}

impl ExpressionOps for ComparisonExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let lhs = self.left.evaluate(tuple, schema)?;
        let rhs = self.right.evaluate(tuple, schema)?;
        let comparison_result = self.perform_comparison(&lhs, &rhs)?;

        let result = match comparison_result {
            CmpBool::CmpTrue => Value::new(true),
            CmpBool::CmpFalse => Value::new(false),
            CmpBool::CmpNull => Value::new(Null),
        };

        debug!(
            "Evaluating comparison - lhs: {:?}, rhs: {:?}, comp_type: {:?}, result: {:?}",
            lhs, rhs, self.comp_type, result
        );

        Ok(result)
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

        let result = match comparison_result {
            CmpBool::CmpTrue => Value::new(true),
            CmpBool::CmpFalse => Value::new(false),
            CmpBool::CmpNull => Value::new(Null),
        };

        Ok(result)
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
            comp_type: self.comp_type,
        }))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate left and right expressions
        self.left.validate(schema)?;
        self.right.validate(schema)?;

        // Check if the types are comparable
        let left_type = self.left.get_return_type().get_type();
        let right_type = self.right.get_return_type().get_type();

        // For now, we'll allow comparison between same types and numeric types
        match (left_type, right_type) {
            (TypeId::Integer, TypeId::Integer)
            | (TypeId::Decimal, TypeId::Decimal)
            | (TypeId::VarChar, TypeId::VarChar)
            | (TypeId::Char, TypeId::Char)
            | (TypeId::Boolean, TypeId::Boolean)
            | (TypeId::Integer, TypeId::Decimal)
            | (TypeId::Decimal, TypeId::Integer)
            | (TypeId::BigInt, TypeId::BigInt)
            | (TypeId::BigInt, TypeId::Integer)
            | (TypeId::Integer, TypeId::BigInt)
            | (TypeId::BigInt, TypeId::Decimal)
            | (TypeId::Decimal, TypeId::BigInt)
            | (TypeId::TinyInt, TypeId::TinyInt)
            | (TypeId::TinyInt, TypeId::Integer)
            | (TypeId::Integer, TypeId::TinyInt)
            | (TypeId::TinyInt, TypeId::Decimal)
            | (TypeId::Decimal, TypeId::TinyInt)
            | (TypeId::TinyInt, TypeId::BigInt)
            | (TypeId::BigInt, TypeId::TinyInt)
            | (TypeId::Float, TypeId::Float)
            | (TypeId::Float, TypeId::Decimal)
            | (TypeId::Decimal, TypeId::Float)
            | (TypeId::Float, TypeId::Integer)
            | (TypeId::Integer, TypeId::Float)
            | (TypeId::Float, TypeId::BigInt)
            | (TypeId::BigInt, TypeId::Float)
            | (TypeId::Float, TypeId::TinyInt)
            | (TypeId::TinyInt, TypeId::Float) => Ok(()),
            _ => Err(ExpressionError::TypeMismatch {
                expected: left_type,
                actual: right_type,
            }),
        }
    }
}

impl Display for ComparisonExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let op_str = match self.comp_type {
            ComparisonType::Equal => "=",
            ComparisonType::NotEqual => "!=",
            ComparisonType::LessThan => "<",
            ComparisonType::LessThanOrEqual => "<=",
            ComparisonType::GreaterThan => ">",
            ComparisonType::GreaterThanOrEqual => ">=",
            ComparisonType::IsNotNull => "IS NOT NULL",
        };

        if f.alternate() {
            write!(f, "({:#} {} {:#})", self.left, op_str, self.right)
        } else {
            write!(f, "({} {} {})", self.left, op_str, self.right)
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::common::logger::initialize_logger;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;

    #[test]
    fn comparison_expression() {
        initialize_logger();

        let schema = Schema::new(vec![
            Column::new("col1", TypeId::Integer),
            Column::new("col2", TypeId::Integer),
        ]);
        let rid = RID::new(0, 0);

        let left_tuple = Tuple::new(&*vec![Value::new(5), Value::new(10)], &schema, rid);
        let right_tuple = Tuple::new(&*vec![Value::new(10), Value::new(15)], &schema, rid);

        let col1 = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, // tuple_index
            0, // column_index
            schema.get_column(0).unwrap().clone(),
            vec![],
        )));

        let col2 = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, // tuple_index
            1, // column_index
            schema.get_column(1).unwrap().clone(),
            vec![],
        )));

        let less_than_expr = Expression::Comparison(ComparisonExpression::new(
            col1.clone(),
            col2.clone(),
            ComparisonType::LessThan,
            vec![],
        ));

        let result = less_than_expr
            .evaluate_join(&left_tuple, &schema, &right_tuple, &schema)
            .unwrap();
        assert_eq!(result, Value::new(true));

        let equal_expr = Expression::Comparison(ComparisonExpression::new(
            col1,
            col2,
            ComparisonType::Equal,
            vec![],
        ));

        let result = equal_expr
            .evaluate_join(&left_tuple, &schema, &right_tuple, &schema)
            .unwrap();
        assert_eq!(result, Value::new(false));

        assert_eq!(less_than_expr.to_string(), "(col1 < col2)");
        assert_eq!(equal_expr.to_string(), "(col1 = col2)");

        assert_eq!(format!("{:#}", less_than_expr), "(Col#0.0 < Col#1.1)");
        assert_eq!(format!("{:#}", equal_expr), "(Col#0.0 = Col#1.1)");
    }

    #[test]
    fn test_comparison_expression_indices() {
        let left_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, // column index
            0, // tuple index for left table
            Column::new("id", TypeId::Integer),
            vec![],
        )));
        let right_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1, // column index
            0, // tuple index for right table
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let comp = ComparisonExpression::new(
            left_col.clone(),
            right_col.clone(),
            ComparisonType::Equal,
            vec![],
        );

        // Verify the tuple indices are preserved
        if let Expression::ColumnRef(left_expr) = comp.get_left().as_ref() {
            assert_eq!(left_expr.get_tuple_index(), 0);
        }
        if let Expression::ColumnRef(right_expr) = comp.get_right().as_ref() {
            assert_eq!(right_expr.get_tuple_index(), 1);
        }
    }
}
