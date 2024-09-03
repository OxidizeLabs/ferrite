use crate::catalogue::schema::Schema;
use crate::execution::plans::abstract_plan::PlanNode;
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct LimitNode {
    output_schema: Rc<Schema>,
    child: Box<PlanNode>,
}