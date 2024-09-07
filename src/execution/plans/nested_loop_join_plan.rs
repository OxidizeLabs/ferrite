use crate::catalogue::schema::Schema;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct NestedLoopJoinNode {
    output_schema: Arc<Schema>,
    left: Box<PlanNode>,
    right: Box<PlanNode>,
}

impl NestedLoopJoinNode {
    pub fn new(output_schema: Arc<Schema>, left: Box<PlanNode>, right: Box<PlanNode>) -> Self {
        Self {
            output_schema,
            left,
            right,
        }
    }

    pub fn get_left(&self) -> &Box<PlanNode> {
        &self.left
    }
    pub fn get_right(&self) -> &Box<PlanNode> {
        &self.right
    }
    pub fn get_output_schema(&self) -> &Arc<Schema> {
        &self.output_schema
    }
}

impl AbstractPlanNode for NestedLoopJoinNode {
    fn get_output_schema(&self) -> &Schema {
        todo!()
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        todo!()
    }

    fn get_type(&self) -> PlanType {
        todo!()
    }

    fn to_string(&self, with_schema: bool) -> String {
        todo!()
    }

    fn plan_node_to_string(&self) -> String {
        todo!()
    }

    fn children_to_string(&self, indent: usize) -> String {
        todo!()
    }
}

