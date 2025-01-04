use crate::catalogue::schema::Schema;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use sqlparser::ast::Join;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct NestedLoopJoinNode {
    output_schema: Schema,
    left_key_expressions: Vec<Arc<Expression>>,
    right_key_expressions: Vec<Arc<Expression>>,
    join: Join,
    children: Vec<PlanNode>,
}

impl NestedLoopJoinNode {
    pub fn new(
        output_schema: Schema,
        left_key_expressions: Vec<Arc<Expression>>,
        right_key_expressions: Vec<Arc<Expression>>,
        join: Join,
        children: Vec<PlanNode>,
    ) -> Self {
        Self {
            output_schema,
            left_key_expressions,
            right_key_expressions,
            join,
            children,
        }
    }

    pub fn get_join(&self) -> &Join {
        &self.join
    }

    pub fn get_left_child(&self) -> &PlanNode {
        &self.children[0]
    }

    pub fn get_right_child(&self) -> &PlanNode {
        &self.children[1]
    }
}

impl AbstractPlanNode for NestedLoopJoinNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }
    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::NestedLoopJoin
    }

    fn to_string(&self, with_schema: bool) -> String {
        let mut result = String::from("NestedLoopJoinNode");
        if with_schema {
            result.push_str(&format!(" [{}]", self.output_schema));
        }
        result
    }

    /// Returns a string representation of this node, including the schema.
    fn plan_node_to_string(&self) -> String {
        self.to_string(true)
    }

    fn children_to_string(&self, indent: usize) -> String {
        let indent_str = " ".repeat(indent);
        let mut result = String::new();

        // Add left child
        result.push_str(&format!(
            "{}Left Child: {}\n",
            indent_str,
            self.get_children()[0].plan_node_to_string()
        ));
        result.push_str(&self.get_children()[0].children_to_string(indent + 2));

        // Add right child
        result.push_str(&format!(
            "{}Right Child: {}\n",
            indent_str,
            self.get_children()[1].plan_node_to_string()
        ));
        result.push_str(&self.get_children()[1].children_to_string(indent + 2));

        result
    }
}
