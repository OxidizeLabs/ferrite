use crate::catalogue::schema::Schema;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::sync::Arc;
use crate::common::config::{IndexOidT, TableOidT};
use crate::execution::expressions::abstract_expression::Expression;

#[derive(Debug, Clone, PartialEq)]
pub struct IndexScanNode {
    output_schema: Arc<Schema>,
    table_id: TableOidT,
    index_id: IndexOidT,
    predicate_keys: Vec<Arc<Expression>>,
    filter_predicate: Option<Arc<Expression>>,
}

impl AbstractPlanNode for IndexScanNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        static CHILDREN: Vec<PlanNode> = Vec::new();
        &CHILDREN
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
        format!("{}Child: {}", indent_str, self.plan_node_to_string())
    }
}