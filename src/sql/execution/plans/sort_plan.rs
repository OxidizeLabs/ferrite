use crate::catalog::schema::Schema;
use crate::sql::execution::expressions::abstract_expression::Expression;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct SortNode {
    output_schema: Schema,
    order_bys: Vec<Arc<Expression>>,
    children: Vec<PlanNode>,
}

impl SortNode {
    pub fn new(output_schema: Schema, order_bys: Vec<Arc<Expression>>, children: Vec<PlanNode>) -> Self {
        Self {
            output_schema,
            order_bys,
            children,
        }
    }

    pub fn get_order_bys(&self) -> &Vec<Arc<Expression>> {
        &self.order_bys
    }
}

impl AbstractPlanNode for SortNode {
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

impl Display for SortNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ Sort")?;

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
