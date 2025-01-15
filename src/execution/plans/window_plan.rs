use crate::binder::bound_order_by::OrderByType;
use crate::binder::table_ref::bound_base_table_ref::BoundBaseTableRef;
use crate::catalog::schema::Schema;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use crate::execution::plans::values_plan::ValuesNode;

/// WindowFunctionType enumerates all the possible window functions in our system
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFunctionType {
    CountStarAggregate,
    CountAggregate,
    SumAggregate,
    MinAggregate,
    MaxAggregate,
    Rank,
}

/// WindowFunction represents a single window function in the plan
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFunction {
    function: Arc<Expression>,
    type_: WindowFunctionType,
    partition_by: Vec<Arc<Expression>>,
    order_by: Vec<(OrderByType, Arc<Expression>)>,
}

/// WindowFunctionPlanNode represents a plan for window function execution
#[derive(Clone, PartialEq, Debug)]
pub struct WindowNode {
    output_schema: Schema,
    columns: Vec<Arc<Expression>>,
    window_functions: HashMap<u32, WindowFunction>,
    children: Vec<PlanNode>,
}

impl WindowNode {
    /// Creates a new WindowFunctionPlanNode
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        output_schema: Schema,
        child: Box<PlanNode>,
        window_func_indexes: Vec<u32>,
        columns: Vec<Arc<Expression>>,
        partition_bys: Vec<Vec<Arc<Expression>>>,
        order_bys: Vec<Vec<(OrderByType, Arc<Expression>)>>,
        functions: Vec<Arc<Expression>>,
        window_func_types: Vec<WindowFunctionType>,
    ) -> Self {
        let mut window_functions = HashMap::new();
        for (i, &index) in window_func_indexes.iter().enumerate() {
            window_functions.insert(
                index,
                WindowFunction {
                    function: functions[i].clone(),
                    type_: window_func_types[i],
                    partition_by: partition_bys[i].clone(),
                    order_by: order_bys[i].clone(),
                },
            );
        }

        Self {
            output_schema,
            columns,
            window_functions,
            children: vec![],
        }
    }

    /// Infers the window schema from the given columns
    pub fn infer_scan_schema(table_ref: &BoundBaseTableRef) -> Result<Schema, String> {
        // This is a placeholder implementation. You should replace this with actual schema inference logic.
        let schema = table_ref.get_schema();
        Ok(schema.clone())
    }
}

impl AbstractPlanNode for WindowNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    /// Returns a reference to the child nodes of this node.
    ///
    /// Note: Currently, DeleteNode only has one child, but this method
    /// returns an empty vector for consistency with the trait definition.
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
            write!(f, "\n   Functions: [")?;
            for (i, (col, func)) in self.window_functions.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}={}", col, func)?;
            }
            write!(f, "]")?;
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
            WindowFunctionType::CountStarAggregate => write!(f, "count_star"),
            WindowFunctionType::CountAggregate => write!(f, "count"),
            WindowFunctionType::SumAggregate => write!(f, "sum"),
            WindowFunctionType::MinAggregate => write!(f, "min"),
            WindowFunctionType::MaxAggregate => write!(f, "max"),
            WindowFunctionType::Rank => write!(f, "rank"),
        }
    }
}

impl Display for WindowFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{ function_arg={}, type={}, partition_by={:?}, order_by={:?} }}",
            self.function.to_string(),
            self.type_,
            self.partition_by,
            self.order_by
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_function_type_display() {
        assert_eq!(
            WindowFunctionType::CountStarAggregate.to_string(),
            "count_star"
        );
        assert_eq!(WindowFunctionType::SumAggregate.to_string(), "sum");
        assert_eq!(WindowFunctionType::Rank.to_string(), "rank");
    }
}
