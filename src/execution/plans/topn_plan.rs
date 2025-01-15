use crate::catalog::schema::Schema;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct TopNNode {
    output_schema: Schema,
    order_bys: Vec<Arc<Expression>>,
    k: usize,
    children: Vec<PlanNode>,
}

impl TopNNode {
    pub fn new(output_schema: Schema,
               order_bys: Vec<Arc<Expression>>,
               k: usize,
               children: Vec<PlanNode>, ) -> Self {
        Self {
            output_schema,
            order_bys,
            k,
            children,
        }
    }
    pub fn get_k(&self) -> usize {
        self.k
    }
    pub fn get_sort_order_by(&self) -> &Vec<Arc<Expression>> {
        &self.order_bys
    }
}

impl AbstractPlanNode for TopNNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::Sort
    }
}

impl Display for TopNNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ TopN: {}", self.k)?;

        if f.alternate() {
            write!(f, "\n   Order By: [")?;
            for (i, expr) in self.order_bys.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", expr)?;
            }
            write!(f, "]")?;
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
