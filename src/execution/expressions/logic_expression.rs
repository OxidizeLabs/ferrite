use crate::catalogue::column::Column;
use crate::catalogue::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::types::CmpBool;
use crate::types_db::value::{Val, Value};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LogicType {
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicExpression {
    left: Arc<Expression>,
    right: Arc<Expression>,
    logic_type: LogicType,
    ret_type: Column,
    children: Vec<Arc<Expression>>,
}

impl LogicExpression {
    pub fn new(left: Arc<Expression>, right: Arc<Expression>, logic_type: LogicType, children: Vec<Arc<Expression>>,
    ) -> Self {
        Self {
            left,
            right,
            logic_type,
            ret_type: Column::new("logic_result", TypeId::Boolean),
            children,
        }
    }

    pub fn get_left(&self) -> &Arc<Expression> {
        &self.left
    }

    pub fn get_right(&self) -> &Arc<Expression> {
        &self.right
    }

    pub fn get_logic_type(&self) -> LogicType {
        self.logic_type
    }

    pub fn get_return_type(&self) -> &Column {
        &self.ret_type
    }

    fn perform_computation(&self, lhs: &Value, rhs: &Value) -> CmpBool {
        let l = Self::get_bool_as_cmp_bool(lhs);
        let r = Self::get_bool_as_cmp_bool(rhs);
        match self.logic_type {
            LogicType::And => Self::perform_and(l, r),
            LogicType::Or => Self::perform_or(l, r),
        }
    }

    fn get_bool_as_cmp_bool(val: &Value) -> CmpBool {
        match val.get_value() {
            Val::Boolean(b) => {
                if *b {
                    CmpBool::CmpTrue
                } else {
                    CmpBool::CmpFalse
                }
            }
            _ => CmpBool::CmpNull,
        }
    }

    fn perform_and(l: CmpBool, r: CmpBool) -> CmpBool {
        match (l, r) {
            (CmpBool::CmpFalse, _) | (_, CmpBool::CmpFalse) => CmpBool::CmpFalse,
            (CmpBool::CmpTrue, CmpBool::CmpTrue) => CmpBool::CmpTrue,
            _ => CmpBool::CmpNull,
        }
    }

    fn perform_or(l: CmpBool, r: CmpBool) -> CmpBool {
        match (l, r) {
            (CmpBool::CmpTrue, _) | (_, CmpBool::CmpTrue) => CmpBool::CmpTrue,
            (CmpBool::CmpFalse, CmpBool::CmpFalse) => CmpBool::CmpFalse,
            _ => CmpBool::CmpNull,
        }
    }
}

impl ExpressionOps for LogicExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let lhs = self.left.evaluate(tuple, schema)?;
        let rhs = self.right.evaluate(tuple, schema)?;
        let comparison_result = self.perform_computation(&lhs, &rhs);
        Ok(Value::new(comparison_result))
    }

    fn evaluate_join(&self, left_tuple: &Tuple, left_schema: &Schema, right_tuple: &Tuple, right_schema: &Schema) -> Result<Value, ExpressionError> {
        let lhs = self.left.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        let rhs = self.right.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        let comparison_result = self.perform_computation(&lhs, &rhs);
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
            panic!("ArithmeticExpression requires exactly two children");
        }

        Arc::new(Expression::Logic(LogicExpression {
            left: children[0].clone(),
            right: children[1].clone(),
            ret_type: self.ret_type.clone(),
            children,
            logic_type: self.logic_type,
        }))
    }
}


impl Display for LogicType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            LogicType::And => write!(f, "AND"),
            LogicType::Or => write!(f, "OR"),
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::value::Val::Null;

    #[test]
    fn logic_expression_and() {
        let left = Arc::new(Expression::Constant(ConstantExpression::new(Value::from(true), Column::new("const", TypeId::Boolean), vec![])));
        let right = Arc::new(Expression::Constant(ConstantExpression::new(Value::from(false), Column::new("const", TypeId::Boolean), vec![])));
        let expr = Expression::Logic(LogicExpression::new(left, right, LogicType::And, vec![]));

        let schema = Schema::new(vec![]);
        let rid = RID::new(0, 0);
        let tuple = Tuple::new(vec![], schema.clone(), rid);

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::from(false));
    }

    #[test]
    fn logic_expression_or() {
        let left = Arc::new(Expression::Constant(ConstantExpression::new(Value::from(true), Column::new("const", TypeId::Boolean), vec![])));
        let right = Arc::new(Expression::Constant(ConstantExpression::new(Value::from(false), Column::new("const", TypeId::Boolean), vec![])));
        let expr = Expression::Logic(LogicExpression::new(left, right, LogicType::Or, vec![]));

        let schema = Schema::new(vec![]);
        let rid = RID::new(0, 0);
        let tuple = Tuple::new(vec![], schema.clone(), rid);

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::from(true));
    }

    #[test]
    fn logic_expression_with_null() {
        let left = Arc::new(Expression::Constant(ConstantExpression::new(Value::from(1), Column::new("const", TypeId::Boolean), vec![])));
        let right = Arc::new(Expression::Constant(ConstantExpression::new(Value::from(true), Column::new("const", TypeId::Boolean), vec![])));
        let expr = Expression::Logic(LogicExpression::new(left, right, LogicType::And, vec![]));

        let schema = Schema::new(vec![]);
        let rid = RID::new(0, 0);
        let tuple = Tuple::new(vec![], schema.clone(), rid);

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::from(Null));
    }

    #[test]
    fn logic_expression_invalid_types() {
        let left = Arc::new(Expression::Constant(ConstantExpression::new(Value::from(1), Column::new("const", TypeId::Integer), vec![])));
        let right = Arc::new(Expression::Constant(ConstantExpression::new(Value::from(true), Column::new("const", TypeId::Boolean), vec![])));
        let result = Expression::Logic(LogicExpression::new(left, right, LogicType::Or, vec![]));
        assert_eq!(result.get_return_type().get_type(), TypeId::Boolean);
    }
}