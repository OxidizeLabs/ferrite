use crate::catalogue::schema::Schema;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::sync::Arc;
use crate::common::config::TableOidT;
use crate::execution::expressions::abstract_expression::Expression;

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateNode {
    output_schema: Arc<Schema>,
    table_id: TableOidT,
    target_expressions: Vec<Arc<Expression>>,
    child: Box<PlanNode>,
}

impl AbstractPlanNode for UpdateNode {
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
        PlanType::Update
    }

    /// Returns a string representation of this node.
    ///
    /// # Arguments
    ///
    /// * `with_schema` - If true, includes the schema in the string representation.
    fn to_string(&self, with_schema: bool) -> String {
        let mut result = String::from("UpdateNode");
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
        format!("{}Child: {}", indent_str, self.child.plan_node_to_string())
    }
}
