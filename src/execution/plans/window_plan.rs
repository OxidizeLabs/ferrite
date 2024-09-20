use crate::binder::bound_order_by::OrderByType;
use crate::binder::table_ref::bound_base_table_ref::BoundBaseTableRef;
use crate::catalogue::schema::Schema;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

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
#[derive(Clone, PartialEq)]
pub struct WindowNode {
    output_schema: Schema,
    child: Box<PlanNode>,
    columns: Vec<Arc<Expression>>,
    window_functions: HashMap<u32, WindowFunction>,
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
            child,
            columns,
            window_functions,
        }
    }

    /// Returns the child plan node
    pub fn get_child_plan(&self) -> &PlanNode {
        &*self.child
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
        static CHILDREN: Vec<PlanNode> = Vec::new();
        &CHILDREN
    }

    fn get_type(&self) -> PlanType {
        PlanType::Window
    }

    fn to_string(&self, with_schema: bool) -> String {
        todo!()
    }

    /// Returns a string representation of this node, including the schema.
    fn plan_node_to_string(&self) -> String {
        self.to_string(true)
    }

    fn children_to_string(&self, indent: usize) -> String {
        todo!()
    }
}

impl Debug for WindowNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("WindowFunctionPlanNode")
            .field("output_schema", &self.output_schema)
            .field("child", &self.child)
            .field("columns", &self.columns)
            .field("window_functions", &self.window_functions)
            .finish()
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
        assert_eq!(WindowFunctionType::CountStarAggregate.to_string(), "count_star");
        assert_eq!(WindowFunctionType::SumAggregate.to_string(), "sum");
        assert_eq!(WindowFunctionType::Rank.to_string(), "rank");
    }
}