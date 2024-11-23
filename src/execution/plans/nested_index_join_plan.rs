use crate::binder::table_ref::bound_join_ref::JoinType;
use crate::catalogue::schema::Schema;
use crate::common::config::{IndexOidT, TableOidT};
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::sync::Arc;

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
    child: Box<PlanNode>,
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

    fn to_string(&self, with_schema: bool) -> String {
        let mut result = String::from("NestedIndexJoin");
        if with_schema {
            result.push_str(&format!(" [{{ type={}, key_predicate={}, index={}, index_table={}, inner_table_schema={} }}]", self.join_type,
                                     self.key_predicate, self.index_name, self.index_table_name, self.inner_table_schema));
        } else {
            result.push_str(&format!(
                " [{{ type={}, key_predicate={}, index={}, index_table={}}}]",
                self.join_type, self.key_predicate, self.index_name, self.index_table_name
            ));
        }
        result
    }
    /// Returns a string representation of this node, including the schema.
    fn plan_node_to_string(&self) -> String {
        self.to_string(true)
    }

    fn children_to_string(&self, indent: usize) -> String {
        todo!()
    }
}
