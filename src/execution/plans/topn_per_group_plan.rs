use std::fmt;
use std::fmt::{Display, Formatter};
use crate::binder::bound_order_by::OrderByType;
use crate::catalog::schema::Schema;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::sync::Arc;
use crate::execution::plans::table_scan_plan::TableScanNode;

#[derive(Debug, Clone, PartialEq)]
pub struct TopNPerGroupNode {
    output_schema: Arc<Schema>,
    order_bys: Vec<(OrderByType, Arc<Expression>)>,
    group_bys: Vec<Arc<Expression>>,
    size: usize,
    child: Box<PlanNode>,
}

impl AbstractPlanNode for TopNPerGroupNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }
    fn get_children(&self) -> &Vec<PlanNode> {
        todo!()
    }

    fn get_type(&self) -> PlanType {
        PlanType::TopNPerGroup
    }
}

impl Display for TopNPerGroupNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ TopNPerGroup: {}", self.size)?;
        
        if f.alternate() {
            write!(f, "\n   Order By: [")?;
            for (i, (order_type, expr)) in self.order_bys.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{:?} {}", order_type, expr)?;
            }
            write!(f, "]")?;
            
            write!(f, "\n   Group By: [")?;
            for (i, expr) in self.group_bys.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", expr)?;
            }
            write!(f, "]")?;
            write!(f, "\n   Schema: {}", self.output_schema)?;
            write!(f, "\n   Child: {:#}", self.child)?;
        }
        
        Ok(())
    }
}
