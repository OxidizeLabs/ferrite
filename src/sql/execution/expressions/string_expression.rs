use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
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
        match arg_value.get_val() {
            Val::VarLen(s) | Val::ConstLen(s) => {
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
        match arg_value.get_val() {
            Val::VarLen(s) | Val::ConstLen(s) => {
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

impl Display for StringExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // Format as function call: function_name(argument)
        if f.alternate() {
            // Detailed format - pass alternate flag to argument
            write!(f, "{}({:#})", self.expr_type, self.arg)
        } else {
            // Basic format
            write!(f, "{}({})", self.expr_type, self.arg)
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
        assert_eq!(result.get_val(), &Val::VarLen("hello".to_string()));
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
        assert_eq!(result.get_val(), &Val::VarLen("HELLO".to_string()));
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

    #[test]
    fn test_string_expression_display() {
        // Test LOWER function
        let arg = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::VarLen("HELLO".to_string())),
            Column::new("text", TypeId::VarChar),
            vec![],
        )));
        let lower_expr = Expression::String(StringExpression::new(
            arg.clone(),
            StringExpressionType::Lower,
            vec![],
        ));

        // Basic format
        assert_eq!(lower_expr.to_string(), "lower(HELLO)");
        // Detailed format
        assert_eq!(format!("{:#}", lower_expr), "lower(Constant(HELLO))");

        // Test UPPER function
        let upper_expr = Expression::String(StringExpression::new(
            arg,
            StringExpressionType::Upper,
            vec![],
        ));

        // Basic format
        assert_eq!(upper_expr.to_string(), "upper(HELLO)");
        // Detailed format
        assert_eq!(format!("{:#}", upper_expr), "upper(Constant(HELLO))");

        // Test with column reference
        let col_arg = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("name", TypeId::VarChar),
            vec![],
        )));
        let col_expr = Expression::String(StringExpression::new(
            col_arg,
            StringExpressionType::Lower,
            vec![],
        ));

        // Basic format
        assert_eq!(col_expr.to_string(), "lower(name)");
        // Detailed format
        assert_eq!(format!("{:#}", col_expr), "lower(Col#0.0)");
    }
}
