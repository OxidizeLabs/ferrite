use crate::binder::table_ref::bound_join_ref::JoinType;
use crate::catalogue::schema::Schema;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct HashJoinNode {
    output_schema: Arc<Schema>,
    left_key_expressions: Vec<Arc<Expression>>,
    right_key_expressions: Vec<Arc<Expression>>,
    join_type: Vec<JoinType>,
    children: Vec<PlanNode>,
}

impl AbstractPlanNode for HashJoinNode {
    /// Returns a reference to the output schema of this node.
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

    /// Returns the type of this plan node.
    fn get_type(&self) -> PlanType {
        PlanType::HashJoin
    }

    /// Returns a string representation of this node.
    ///
    /// # Arguments
    ///
    /// * `with_schema` - If true, includes the schema in the string representation.
    fn to_string(&self, with_schema: bool) -> String {
        let mut result = String::from("HashJoinNode");
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
        unimplemented!()
    }
}
