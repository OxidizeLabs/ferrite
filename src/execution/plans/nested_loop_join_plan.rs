use std::rc::Rc;
use crate::catalogue::schema::Schema;
use crate::execution::plans::abstract_plan::PlanNode;

#[derive(Debug, Clone)]
pub struct NestedLoopJoinNode {
    output_schema: Rc<Schema>,
    left: Box<PlanNode>,
    right: Box<PlanNode>,
}