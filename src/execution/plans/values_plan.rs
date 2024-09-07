use crate::catalogue::schema::Schema;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::sync::Arc;
use crate::execution::expressions::abstract_expression::Expression;

#[derive(Debug, Clone, PartialEq)]
pub struct ValuesNode {
    output_schema: Arc<Schema>,
    values: Vec<Arc<Expression>>,
    child: Box<PlanNode>,
}

impl AbstractPlanNode for ValuesNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        todo!()
    }

    fn get_type(&self) -> PlanType {
        PlanType::Values
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
