use crate::catalog::schema::Schema;
use crate::common::config::TableOidT;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateNode {
    output_schema: Schema,
    table_name: String,
    table_id: TableOidT,
    target_expressions: Vec<Arc<Expression>>,
    children: Vec<PlanNode>,
}

impl UpdateNode {
    pub fn new(output_schema: Schema,
               table_name: String,
               table_id: TableOidT,
               target_expressions: Vec<Arc<Expression>>,
               children: Vec<PlanNode>,
    ) -> Self {
        Self {
            output_schema,
            table_name,
            table_id,
            target_expressions,
            children,
        }
    }
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
