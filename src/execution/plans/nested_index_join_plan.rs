use std::rc::Rc;
use crate::catalogue::schema::Schema;
use crate::execution::plans::abstract_plan::PlanNode;

#[derive(Debug, Clone)]
pub struct NestedIndexJoinNode {
    output_schema: Rc<Schema>,
    child: Box<PlanNode>,
}