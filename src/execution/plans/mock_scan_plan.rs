use std::fmt::{Display, Formatter};
use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use crate::execution::plans::limit_plan::LimitNode;
use crate::types_db::value::Value;

#[derive(Default, Debug, Clone, PartialEq)]
pub struct MockScanNode {
    output_schema: Schema,
    table: String,
    children: Vec<PlanNode>,
    tuples: Vec<(Vec<Value>, RID)>,
}

impl MockScanNode {
    pub fn new(
        schema: Schema,
        table_name: String,
        tuples: Vec<(Vec<Value>, RID)>,
    ) -> Self {
        Self {
            output_schema: schema,
            table: table_name,
            children: vec![],
            tuples,
        }
    }

    pub fn get_table_name(&self) -> String {
        self.table.clone()
    }

    pub fn get_tuples(&self) -> &Vec<(Vec<Value>, RID)> {
        &self.tuples
    }
}

impl AbstractPlanNode for MockScanNode {
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
        PlanType::MockScan
    }
}

impl Display for MockScanNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

