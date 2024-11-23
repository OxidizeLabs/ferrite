use crate::binder::table_ref::bound_join_ref::JoinType;
use crate::catalogue::schema::Schema;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct NestedLoopJoinNode {
    output_schema: Arc<Schema>,
    left: Box<PlanNode>,
    right: Box<PlanNode>,
    predicate: Arc<Expression>,
    join_type: JoinType,
}

impl NestedLoopJoinNode {
    pub fn new(
        output_schema: Arc<Schema>,
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        predicate: Arc<Expression>,
        join_type: JoinType,
    ) -> Self {
        Self {
            output_schema,
            left,
            right,
            predicate,
            join_type,
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
        &self.output_schema
    }
    fn get_children(&self) -> &Vec<PlanNode> {
        todo!()
    }

    fn get_type(&self) -> PlanType {
        PlanType::NestedLoopJoin
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
