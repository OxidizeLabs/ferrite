use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use crate::catalog::schema::Schema;
use crate::sql::execution::expressions::abstract_expression::Expression;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};

/// WindowFunctionType enumerates all the possible window functions in our system
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFunctionType {
    RowNumber,
    Rank,
    DenseRank,
    FirstValue,
    LastValue,
    Lead,
    Lag,
    Sum,
    Min,
    Max,
    Count,
    Average,
}

/// WindowFunction represents a single window function in the plan
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFunction {
    function_type: WindowFunctionType,
    function_expr: Arc<Expression>,
    partition_by: Vec<Arc<Expression>>,
    order_by: Vec<Arc<Expression>>,
}

impl WindowFunction {
    pub fn new(
        function_type: WindowFunctionType,
        function_expr: Arc<Expression>,
        partition_by: Vec<Arc<Expression>>,
        order_by: Vec<Arc<Expression>>,
    ) -> Self {
        Self {
            function_type,
            function_expr,
            partition_by,
            order_by,
        }
    }

    pub fn get_function_type(&self) -> WindowFunctionType {
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

/// WindowNode represents a plan for window function execution
#[derive(Clone, PartialEq, Debug)]
pub struct WindowNode {
    output_schema: Schema,
    window_functions: Vec<WindowFunction>,
    children: Vec<PlanNode>,
}

impl WindowNode {
    pub fn new(
        output_schema: Schema,
        window_functions: Vec<WindowFunction>,
        children: Vec<PlanNode>,
    ) -> Self {
        Self {
            output_schema,
            window_functions,
            children,
        }
    }

    pub fn get_window_functions(&self) -> &Vec<WindowFunction> {
        &self.window_functions
    }
}

impl AbstractPlanNode for WindowNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::Window
    }
}

impl Display for WindowNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ Window")?;

        if f.alternate() {
            for (i, window_func) in self.window_functions.iter().enumerate() {
                write!(
                    f,
                    "\n   Window Function {}: {:?}",
                    i + 1,
                    window_func.function_type
                )?;
                write!(f, "\n     Expression: {}", window_func.function_expr)?;

                if !window_func.partition_by.is_empty() {
                    write!(f, "\n     Partition By: [")?;
                    for (i, expr) in window_func.partition_by.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{}", expr)?;
                    }
                    write!(f, "]")?;
                }

                if !window_func.order_by.is_empty() {
                    write!(f, "\n     Order By: [")?;
                    for (i, expr) in window_func.order_by.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{}", expr)?;
                    }
                    write!(f, "]")?;
                }
            }

            write!(f, "\n   Schema: {}", self.output_schema)?;

            // Format children with proper indentation
            for (i, child) in self.children.iter().enumerate() {
                writeln!(f)?;
                write!(f, "    Child {}: {:#}", i + 1, child)?;
            }
        }

        Ok(())
    }
}

impl Display for WindowFunctionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            WindowFunctionType::RowNumber => write!(f, "ROW_NUMBER"),
            WindowFunctionType::Rank => write!(f, "RANK"),
            WindowFunctionType::DenseRank => write!(f, "DENSE_RANK"),
            WindowFunctionType::FirstValue => write!(f, "FIRST_VALUE"),
            WindowFunctionType::LastValue => write!(f, "LAST_VALUE"),
            WindowFunctionType::Lead => write!(f, "LEAD"),
            WindowFunctionType::Lag => write!(f, "LAG"),
            WindowFunctionType::Sum => write!(f, "SUM"),
            WindowFunctionType::Min => write!(f, "MIN"),
            WindowFunctionType::Max => write!(f, "MAX"),
            WindowFunctionType::Count => write!(f, "COUNT"),
            WindowFunctionType::Average => write!(f, "AVG"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::types_db::type_id::TypeId;

    #[test]
    fn test_window_node_creation() {
        let schema = Schema::new(vec![
            Column::new("department", TypeId::VarChar),
            Column::new("salary", TypeId::Integer),
            Column::new("rank", TypeId::Integer),
        ]);

        // Create partition by expression (department)
        let partition_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("department", TypeId::VarChar),
            vec![],
        )));

        // Create order by expression (salary)
        let order_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("salary", TypeId::Integer),
            vec![],
        )));

        // Create window function (RANK)
        let window_func = WindowFunction::new(
            WindowFunctionType::Rank,
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                2,
                Column::new("rank", TypeId::Integer),
                vec![],
            ))),
            vec![partition_expr],
            vec![order_expr],
        );

        let node = WindowNode::new(schema.clone(), vec![window_func], vec![]);

        assert_eq!(node.get_output_schema(), &schema);
        assert_eq!(node.get_window_functions().len(), 1);
        assert_eq!(
            node.get_window_functions()[0].get_function_type(),
            WindowFunctionType::Rank
        );
    }
}
