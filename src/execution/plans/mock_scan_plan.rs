use crate::catalogue::schema::Schema;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::rc::Rc;

#[derive(Debug, Clone, PartialEq)]
pub struct MockScanNode {
    output_schema: Rc<Schema>,
    child: Option<Rc<PlanNode>>,
}

impl MockScanNode {
    pub fn new(output_schema: Rc<Schema>, child: Option<Rc<PlanNode>>) -> Self {
        Self {
            output_schema,
            child,
        }
    }
}

impl AbstractPlanNode for MockScanNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
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