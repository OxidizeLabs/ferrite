use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::plans::window_plan::WindowFunctionType;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct WindowExpression {
    /// The type of window function (RANK, ROW_NUMBER, etc.)
    function_type: WindowFunctionType,
    /// The expression to compute the window function on
    function_expr: Arc<Expression>,
    /// PARTITION BY expressions
    partition_by: Vec<Arc<Expression>>,
    /// ORDER BY expressions
    order_by: Vec<Arc<Expression>>,
    /// The return type of this window function
    return_type: Column,
    /// Child expressions
    children: Vec<Arc<Expression>>,
}

impl WindowExpression {
    pub fn new(
        function_type: WindowFunctionType,
        function_expr: Arc<Expression>,
        partition_by: Vec<Arc<Expression>>,
        order_by: Vec<Arc<Expression>>,
        return_type: Column,
    ) -> Self {
        // Collect all child expressions
        let mut children = vec![function_expr.clone()];
        children.extend(partition_by.iter().cloned());
        children.extend(order_by.iter().cloned());

        Self {
            function_type,
            function_expr,
            partition_by,
            order_by,
            return_type,
            children,
        }
    }

    pub fn get_window_type(&self) -> WindowFunctionType {
        self.function_type
    }

    pub fn get_function_expr(&self) -> &Arc<Expression> {
        &self.function_expr
    }

    pub fn get_partition_by(&self) -> &Vec<Arc<Expression>> {
        &self.partition_by
    }

    pub fn get_order_by(&self) -> &Vec<Arc<Expression>> {
        &self.order_by
    }
}

impl ExpressionOps for WindowExpression {
    fn evaluate(&self, _tuple: &Tuple, _schema: &Schema) -> Result<Value, ExpressionError> {
        // Window functions can't be evaluated on a single tuple
        // They need the context of the entire window frame
        Err(ExpressionError::InvalidOperation(
            "Window functions cannot be evaluated on a single tuple".to_string(),
        ))
    }

    fn evaluate_join(
        &self,
        _left_tuple: &Tuple,
        _left_schema: &Schema,
        _right_tuple: &Tuple,
        _right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        // Window functions can't be evaluated on joined tuples directly
        Err(ExpressionError::InvalidOperation(
            "Window functions cannot be evaluated on joined tuples".to_string(),
        ))
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        &self.children[child_idx]
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        if children.is_empty() {
            panic!("WindowExpression requires at least one child expression");
        }

        let function_expr = Arc::clone(&children[0]);
        let mut partition_by = Vec::new();
        let mut order_by = Vec::new();

        // Distribute remaining children between partition_by and order_by
        let partition_count = self.partition_by.len();
        let order_count = self.order_by.len();

        for (i, child) in children[1..].iter().enumerate() {
            if i < partition_count {
                partition_by.push(Arc::clone(child));
            } else if i < partition_count + order_count {
                order_by.push(Arc::clone(child));
            }
        }

        Arc::new(Expression::Window(WindowExpression::new(
            self.function_type,
            function_expr,
            partition_by,
            order_by,
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate the function expression
        self.function_expr.validate(schema)?;

        // Validate partition by expressions
        for expr in &self.partition_by {
            expr.validate(schema)?;
        }

        // Validate order by expressions
        for expr in &self.order_by {
            expr.validate(schema)?;
        }

        Ok(())
    }
}

impl Display for WindowExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}({})", self.function_type, self.function_expr)?;

        if !self.partition_by.is_empty() {
            write!(f, " PARTITION BY ")?;
            for (i, expr) in self.partition_by.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", expr)?;
            }
        }

        if !self.order_by.is_empty() {
            write!(f, " ORDER BY ")?;
            for (i, expr) in self.order_by.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", expr)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::types_db::type_id::TypeId;

    #[test]
    fn test_window_expression_creation() {
        let function_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("salary", TypeId::Integer),
            vec![],
        )));

        let partition_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("department", TypeId::VarChar),
            vec![],
        )));

        let order_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("salary", TypeId::Integer),
            vec![],
        )));

        let window_expr = WindowExpression::new(
            WindowFunctionType::Rank,
            function_expr,
            vec![partition_expr],
            vec![order_expr],
            Column::new("rank", TypeId::Integer),
        );

        assert_eq!(window_expr.get_window_type(), WindowFunctionType::Rank);
        assert_eq!(window_expr.get_partition_by().len(), 1);
        assert_eq!(window_expr.get_order_by().len(), 1);
        assert_eq!(window_expr.get_children().len(), 3); // function_expr + partition_by + order_by
    }

    #[test]
    fn test_window_expression_display() {
        let function_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("salary", TypeId::Integer),
            vec![],
        )));

        let window_expr = WindowExpression::new(
            WindowFunctionType::Sum,
            function_expr,
            vec![],
            vec![],
            Column::new("total", TypeId::Integer),
        );

        assert_eq!(window_expr.to_string(), "Sum(salary)");
    }
}
