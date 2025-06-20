use crate::catalog::schema::Schema;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub struct DistinctNode {
    /// The output schema
    schema: Schema,
    /// The child plans
    children: Vec<PlanNode>,
}

impl DistinctNode {
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            children: vec![],
        }
    }

    pub fn with_children(mut self, children: Vec<PlanNode>) -> Self {
        self.children = children;
        self
    }
}

impl AbstractPlanNode for DistinctNode {
    fn get_output_schema(&self) -> &Schema {
        &self.schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::Distinct
    }
}

impl Display for DistinctNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Distinct")
    }
} 