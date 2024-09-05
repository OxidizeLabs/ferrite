use crate::catalogue::schema::Schema;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::rc::Rc;
use crate::execution::plans::update_plan::UpdateNode;

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteNode {
    output_schema: Rc<Schema>,
    child: Box<PlanNode>,
}

impl AbstractPlanNode for DeleteNode {
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
