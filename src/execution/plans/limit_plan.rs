use std::fmt;
use std::fmt::{Display, Formatter};
use crate::catalog::schema::Schema;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use crate::execution::plans::filter_plan::FilterNode;

#[derive(Debug, Clone, PartialEq)]
pub struct LimitNode {
    output_schema: Schema,
    limit: usize,
    children: Vec<PlanNode>,
}

impl LimitNode {
    pub fn new(limit: usize, output_schema: Schema, children: Vec<PlanNode>) -> Self {
        Self {
            output_schema,
            limit,
            children,
        }
    }

    pub fn get_limit(&self) -> usize {
        self.limit
    }
}

impl AbstractPlanNode for LimitNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::Limit
    }
}

impl Display for LimitNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ Limit: {}", self.limit)?;
        
        if f.alternate() {
            write!(f, "\n   Schema: {}", self.output_schema)?;
            
            // Format children with proper indentation
            for (i, child) in self.children.iter().enumerate() {
                writeln!(f)?;
                write!(f, "    Child {}: {:#}", i + 1, child)?;
            }
        }
        
        Ok(())
    }
}
