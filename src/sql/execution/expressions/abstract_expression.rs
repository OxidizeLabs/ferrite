use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::aggregate_expression::AggregateExpression;
use crate::sql::execution::expressions::arithmetic_expression::ArithmeticExpression;
use crate::sql::execution::expressions::array_expression::ArrayExpression;
use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::sql::execution::expressions::comparison_expression::ComparisonExpression;
use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
use crate::sql::execution::expressions::logic_expression::LogicExpression;
use crate::sql::execution::expressions::mock_expression::MockExpression;
use crate::sql::execution::expressions::string_expression::StringExpression;
use crate::sql::execution::expressions::window_expression::WindowExpression;
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
    Aggregate(AggregateExpression),
    Window(WindowExpression),
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
            Self::Aggregate(expr) => expr.evaluate(tuple, schema),
            Self::Window(expr) => expr.evaluate(tuple, schema),
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
            Self::Aggregate(expr) => {
                expr.evaluate(left_tuple, right_schema)
            }
            Self::Window(expr) => {
                expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
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
            Self::Aggregate(expr) => expr.get_child_at(child_idx),
            Self::Window(expr) => expr.get_child_at(child_idx),
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
            Self::Aggregate(expr) => expr.get_children(),
            Self::Window(expr) => expr.get_children(),
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
            Self::Aggregate(expr) => expr.get_return_type(),
            Self::Window(expr) => expr.get_return_type(),
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
            Self::Aggregate(expr) => expr.clone_with_children(children),
            Self::Window(expr) => expr.clone_with_children(children),
        }
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Constant(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            }
            Self::ColumnRef(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            }
            Self::Arithmetic(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            }
            Self::Comparison(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            }
            Self::Logic(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            }
            Self::String(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            }
            Self::Array(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            }
            Self::Mock(expr) => write!(f, "{}", expr),
            Self::Aggregate(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            }
            Self::Window(expr) => {
                if f.alternate() {
                    write!(f, "{:#}", expr)
                } else {
                    write!(f, "{}", expr)
                }
            }
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::mock_expression::MockExpression;
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
        assert_eq!(expr.to_string(), "42");
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

    #[test]
    fn test_window_expression() {
        use crate::sql::execution::plans::window_plan::WindowFunctionType;

        let function_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("salary", TypeId::Integer),
            vec![],
        )));

        let window_expr = Expression::Window(WindowExpression::new(
            WindowFunctionType::Sum,
            function_expr,
            vec![],
            vec![],
            Column::new("total", TypeId::Integer),
        ));

        let schema = Schema::new(vec![Column::new("salary", TypeId::Integer)]);
        let tuple = Tuple::new(&*vec![Value::new(100)], schema.clone(), RID::new(0, 0));

        // Window functions can't be evaluated on a single tuple
        assert!(window_expr.evaluate(&tuple, &schema).is_err());
        assert_eq!(window_expr.to_string(), "Sum(salary)");
    }
}
