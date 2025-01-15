use std::fmt;
use std::fmt::{Display, Formatter};
use crate::binder::table_ref::bound_join_ref::JoinType;
use crate::catalog::schema::Schema;
use crate::common::config::{IndexOidT, TableOidT};
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::sync::Arc;
use crate::execution::plans::mock_scan_plan::MockScanNode;

#[derive(Debug, Clone, PartialEq)]
pub struct NestedIndexJoinNode {
    output_schema: Arc<Schema>,
    key_predicate: Arc<Expression>,
    index_table_name: String,
    inner_table_id: TableOidT,
    index_name: String,
    index_id: IndexOidT,
    inner_table_schema: Arc<Schema>,
    join_type: JoinType,
    children: Vec<PlanNode>,
}

impl AbstractPlanNode for NestedIndexJoinNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        todo!()
    }

    fn get_type(&self) -> PlanType {
        PlanType::NestedIndexJoin
    }
}

impl Display for NestedIndexJoinNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ NestedIndexJoin: {} using {}", self.index_table_name, self.index_name)?;
        
        if f.alternate() {
            write!(f, "\n   Join Type: {:#?}", self.join_type)?;
            write!(f, "\n   Key Predicate: {}", self.key_predicate)?;
            write!(f, "\n   Inner Schema: {}", self.inner_table_schema)?;
            write!(f, "\n   Output Schema: {}", self.output_schema)?;
            
            // Format children with proper indentation
            for (i, child) in self.children.iter().enumerate() {
                writeln!(f)?;
                write!(f, "    Child {}: {:#}", i + 1, child)?;
            }
        }
        
        Ok(())
    }
}
