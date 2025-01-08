use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StringExpressionType {
    Lower,
    Upper,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StringExpression {
    arg: Arc<Expression>,
    children: Vec<Arc<Expression>>,
    expr_type: StringExpressionType,
    ret_type: Column,
}

impl StringExpression {
    pub fn new(
        arg: Arc<Expression>,
        expr_type: StringExpressionType,
        children: Vec<Arc<Expression>>,
    ) -> Self {
        Self {
            arg,
            children,
            expr_type,
            ret_type: Column::new("<val>", TypeId::VarChar),
        }
    }

    pub fn get_expr_type(&self) -> StringExpressionType {
        self.expr_type
    }

    pub fn get_arg(&self) -> &Arc<Expression> {
        &self.arg
    }

    fn perform_computation(&self, val: &str) -> String {
        match self.expr_type {
            StringExpressionType::Lower => val.to_lowercase(),
            StringExpressionType::Upper => val.to_uppercase(),
        }
    }
}

impl ExpressionOps for StringExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let arg_value = self.arg.evaluate(tuple, schema)?;
        match arg_value.get_value() {
            Val::VarLen(s) | Val::ConstVarLen(s) => {
                let computed = self.perform_computation(s);
                Ok(Value::new(Val::VarLen(computed)))
            }
            _ => Err(ExpressionError::InvalidStringExpressionType),
        }
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        let arg_value =
            self.arg
                .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        match arg_value.get_value() {
            Val::VarLen(s) | Val::ConstVarLen(s) => {
                let computed = self.perform_computation(s);
                Ok(Value::new(Val::VarLen(computed)))
            }
            _ => Err(ExpressionError::InvalidStringExpressionType),
        }
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
        if children.len() != 1 {
            panic!("StringExpression requires exactly one child");
        }

        Arc::new(Expression::String(StringExpression {
            arg: children[0].clone(),
            ret_type: self.ret_type.clone(),
            children,
            expr_type: self.expr_type,
        }))
    }
}

impl Display for StringExpressionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            StringExpressionType::Lower => write!(f, "lower"),
            StringExpressionType::Upper => write!(f, "upper"),
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::execution::expressions::constant_value_expression::ConstantExpression;

    #[test]
    fn string_expression_lower() {
        let arg = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::VarLen("HELLO".to_string())),
            Column::new("const", TypeId::VarChar),
            vec![],
        )));
        let expr = Expression::String(StringExpression::new(
            arg,
            StringExpressionType::Lower,
            vec![],
        ));

        let schema = Schema::new(vec![]);
        let rid = RID::new(0, 0);
        let tuple = Tuple::new(&*vec![], schema.clone(), rid);

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.get_value(), &Val::VarLen("hello".to_string()));
    }

    #[test]
    fn string_expression_upper() {
        let arg = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::VarLen("hello".to_string())),
            Column::new("const", TypeId::VarChar),
            vec![],
        )));
        let expr = Expression::String(StringExpression::new(
            arg,
            StringExpressionType::Upper,
            vec![],
        ));

        let schema = Schema::new(vec![]);
        let rid = RID::new(0, 0);
        let tuple = Tuple::new(&*vec![], schema.clone(), rid);

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.get_value(), &Val::VarLen("HELLO".to_string()));
    }

    #[test]
    fn string_expression_invalid_type() {
        let arg = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Integer(42)),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let expr = Expression::String(StringExpression::new(
            arg,
            StringExpressionType::Lower,
            vec![],
        ));

        let schema = Schema::new(vec![]);
        let rid = RID::new(0, 0);
        let tuple = Tuple::new(&*vec![], schema.clone(), rid);

        let result = expr.evaluate(&tuple, &schema);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ExpressionError::InvalidStringExpressionType
        ));
    }
}
