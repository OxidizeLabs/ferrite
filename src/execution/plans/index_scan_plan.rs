use crate::catalogue::schema::Schema;
use crate::common::config::{IndexOidT, TableOidT};
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct IndexScanNode {
    output_schema: Schema,
    table_name: String,
    table_id: TableOidT,
    index_name: String,
    index_id: IndexOidT,
    predicate_keys: Vec<Arc<Expression>>,
    children: Vec<PlanNode>,
}

impl IndexScanNode {
    pub fn new(
        output_schema: Schema,
        table_name: String,
        table_id: TableOidT,
        index_name: String,
        index_id: IndexOidT,
        predicate_keys: Vec<Arc<Expression>>,
        children: Vec<PlanNode>,
    ) -> Self{
        Self {
            output_schema,
            table_name,
            table_id,
            index_name,
            index_id,
            predicate_keys,
            children
        }
    }

    pub fn get_index_name(&self) -> String {
        self.index_name.to_string()
    }

}

impl AbstractPlanNode for IndexScanNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::IndexScan
    }

    /// Returns a string representation of this node.
    ///
    /// # Arguments
    ///
    /// * `with_schema` - If true, includes the schema in the string representation.
    fn to_string(&self, with_schema: bool) -> String {
        let mut result = String::from("IndexScanNode");
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
