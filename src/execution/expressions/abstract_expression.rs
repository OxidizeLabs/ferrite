use crate::catalogue::column::Column;
use crate::catalogue::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::execution::expressions::arithmetic_expression::ArithmeticExpression;
use crate::execution::expressions::array_expression::ArrayExpression;
use crate::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::execution::expressions::comparison_expression::ComparisonExpression;
use crate::execution::expressions::constant_value_expression::ConstantExpression;
use crate::execution::expressions::logic_expression::LogicExpression;
use crate::execution::expressions::mock_expression::MockExpression;
use crate::execution::expressions::string_expression::StringExpression;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Constant(ConstantExpression),
    ColumnRef(ColumnRefExpression),
    Arithmetic(ArithmeticExpression),
    Comparison(ComparisonExpression),
    Logic(LogicExpression),
    String(StringExpression),
    Array(ArrayExpression),
    Mock(MockExpression),
}

pub trait ExpressionOps {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError>;
    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError>;
    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression>;
    fn get_children(&self) -> &Vec<Arc<Expression>>;
    fn get_return_type(&self) -> &Column;
    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression>;
}

impl ExpressionOps for Expression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        match self {
            Self::Constant(expr) => expr.evaluate(tuple, schema),
            Self::ColumnRef(expr) => expr.evaluate(tuple, schema),
            Self::Arithmetic(expr) => expr.evaluate(tuple, schema),
            Self::Comparison(expr) => expr.evaluate(tuple, schema),
            Self::Logic(expr) => expr.evaluate(tuple, schema),
            Self::String(expr) => expr.evaluate(tuple, schema),
            Self::Array(expr) => expr.evaluate(tuple, schema),
            Self::Mock(expr) => expr.evaluate(tuple, schema),
        }
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        match self {
            Self::Constant(expr) => Ok(expr.get_value().clone()),
            Self::ColumnRef(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            }
            Self::Arithmetic(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            }
            Self::Comparison(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            }
            Self::Logic(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            }
            Self::String(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            }
            Self::Array(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            }
            Self::Mock(expr) => {
                // For mock expressions, we'll just use the regular evaluate
                expr.evaluate(left_tuple, left_schema)
            }
        }
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        match self {
            Self::Arithmetic(expr) => expr.get_child_at(child_idx),
            Self::Constant(expr) => expr.get_child_at(child_idx),
            Self::ColumnRef(expr) => expr.get_child_at(child_idx),
            Self::Comparison(expr) => expr.get_child_at(child_idx),
            Self::Logic(expr) => expr.get_child_at(child_idx),
            Self::String(expr) => expr.get_child_at(child_idx),
            Self::Array(expr) => expr.get_child_at(child_idx),
            Self::Mock(expr) => expr.get_child_at(child_idx),
        }
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        match self {
            Self::Arithmetic(expr) => expr.get_children(),
            Self::Constant(expr) => expr.get_children(),
            Self::ColumnRef(expr) => expr.get_children(),
            Self::Comparison(expr) => expr.get_children(),
            Self::Logic(expr) => expr.get_children(),
            Self::String(expr) => expr.get_children(),
            Self::Array(expr) => expr.get_children(),
            Self::Mock(expr) => expr.get_children(),
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
            Self::Array(expr) => expr.get_return_type(),
            Self::Mock(expr) => expr.get_return_type(),
        }
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        match self {
            Self::Arithmetic(expr) => expr.clone_with_children(children),
            Self::Constant(expr) => expr.clone_with_children(children),
            Self::ColumnRef(expr) => expr.clone_with_children(children),
            Self::Comparison(expr) => expr.clone_with_children(children),
            Self::Logic(expr) => expr.clone_with_children(children),
            Self::String(expr) => expr.clone_with_children(children),
            Self::Array(expr) => expr.clone_with_children(children),
            Self::Mock(expr) => expr.clone_with_children(children),
        }
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Constant(expr) => write!(f, "{}", expr.to_string()),
            Self::ColumnRef(expr) => write!(f, "{}", expr.to_string()),
            Self::Arithmetic(expr) => write!(
                f,
                "(Col{} {} Col{})",
                expr.get_left(),
                expr.get_op(),
                expr.get_right()
            ),
            Self::Comparison(expr) => write!(
                f,
                "(Col{} {} Col{})",
                expr.get_left(),
                expr.get_comp_type(),
                expr.get_right()
            ),
            Self::Logic(expr) => write!(
                f,
                "({} {} {})",
                expr.get_left(),
                expr.get_logic_type(),
                expr.get_right()
            ),
            Self::String(expr) => write!(f, "{}({})", expr.get_expr_type(), expr.get_arg()),
            Self::Array(expr) => {
                write!(f, "[")?;
                let mut iter = expr.get_children().iter();
                if let Some(first) = iter.next() {
                    write!(f, "{}", first)?;
                    for child in iter {
                        write!(f, ", {}", child)?;
                    }
                }
                write!(f, "]")
            }
            Self::Mock(expr) => write!(f, "{}", expr),
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::execution::expressions::mock_expression::MockExpression;
    use crate::types_db::type_id::TypeId;

    #[test]
    fn constant_expression() {
        let value = Value::new(42);
        let ret_type = Column::new("const", TypeId::Integer);
        let expr = Expression::Constant(ConstantExpression::new(value.clone(), ret_type, vec![]));

        let schema = Schema::new(vec![]);
        let rid = RID::new(0, 0);
        let tuple = Tuple::new(&*vec![], schema.clone(), rid);

        assert_eq!(expr.evaluate(&tuple, &schema).unwrap(), value);
        assert_eq!(expr.get_children().len(), 0);
        assert_eq!(expr.to_string(), "Constant(42)");
    }

    #[test]
    fn test_mock_expression() {
        let mock = MockExpression::new("test".to_string(), TypeId::Integer);
        let expr = Expression::Mock(mock);

        let schema = Schema::new(vec![]);
        let rid = RID::new(0, 0);
        let tuple = Tuple::new(&*vec![], schema.clone(), rid);

        // The actual evaluation will depend on your MockExpression implementation
        assert!(expr.evaluate(&tuple, &schema).is_ok());
        assert_eq!(expr.get_children().len(), 0);
        assert!(expr.to_string().contains("test"));
    }

    #[test]
    fn test_mock_expression_in_children() {
        let mock = MockExpression::new("test".to_string(), TypeId::Integer);
        let mock_expr = Arc::new(Expression::Mock(mock));

        // Create an array expression with mock child
        let array_expr = Expression::Array(ArrayExpression::new(vec![mock_expr]));

        assert_eq!(array_expr.get_children().len(), 1);
        match &array_expr.get_children()[0].as_ref() {
            Expression::Mock(_) => (),
            _ => panic!("Expected Mock expression"),
        }
    }
}
