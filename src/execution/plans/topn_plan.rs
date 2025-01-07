use crate::catalogue::schema::Schema;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct TopNNode {
    output_schema: Schema,
    order_bys: Vec<Arc<Expression>>,
    k: usize,
    children: Vec<PlanNode>,
}

impl TopNNode {
    pub fn new(output_schema: Schema,
               order_bys: Vec<Arc<Expression>>,
               k: usize,
               children: Vec<PlanNode>, ) -> Self {
        Self {
            output_schema,
            order_bys,
            k,
            children,
        }
    }
    pub fn get_k(&self) -> usize {
        self.k
    }
    pub fn get_sort_order_by(&self) -> &Vec<Arc<Expression>> {
        &self.order_bys
    }
}

impl AbstractPlanNode for TopNNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::Sort
    }

    /// Returns a string representation of this node.
    ///
    /// # Arguments
    ///
    /// * `with_schema` - If true, includes the schema in the string representation.
    fn to_string(&self, with_schema: bool) -> String {
        let mut result = String::from("TopNNode");
        if with_schema {
            result.push_str(&format!(" [{}]", self.output_schema));
        }
        result
    }

    /// Returns a string representation of this node, including the schema.
    fn plan_node_to_string(&self) -> String {
        self.to_string(true)
    }

    /// Returns a string representation of this node's children.
    ///
    /// # Arguments
    ///
    /// * `indent` - The number of spaces to indent the output.
    fn children_to_string(&self, indent: usize) -> String {
        let indent_str = " ".repeat(indent);
        let mut result = String::new();

        // Add left child
        result.push_str(&format!("{}Left Child: {}\n", indent_str, self.get_children()[0].plan_node_to_string()));
        result.push_str(&self.get_children()[0].children_to_string(indent + 2));

        // Add right child
        result.push_str(&format!("{}Right Child: {}\n", indent_str, self.get_children()[1].plan_node_to_string()));
        result.push_str(&self.get_children()[1].children_to_string(indent + 2));

        result
    }
}
