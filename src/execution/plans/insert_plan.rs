use crate::catalogue::schema::Schema;
use crate::common::config::TableOidT;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use crate::storage::table::tuple::Tuple;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct InsertNode {
    output_schema: Arc<Schema>,
    table_oid: TableOidT,
    table_name: String,
    tuples: Vec<Tuple>,
    child: Box<PlanNode>,
}

impl InsertNode {
    pub fn new(
        output_schema: Schema,
        table_oid: TableOidT,
        table_name: String,
        tuples: Vec<Tuple>,
        child: PlanNode,
    ) -> Self {
        Self {
            output_schema: Arc::new(output_schema),
            table_oid,
            table_name,
            tuples,
            child: Box::new(child),
        }
    }

    pub fn get_child(&self) -> &Box<PlanNode> {
        &self.child
    }

    pub fn get_table_oid(&self) -> TableOidT {
        self.table_oid
    }

    pub fn get_table_name(&self) -> &str {
        &self.table_name
    }

    pub fn get_input_tuples(&self) -> &Vec<Tuple> {
        &self.tuples
    }
}

impl AbstractPlanNode for InsertNode {
    fn get_output_schema(&self) -> &Schema {
        &self.get_output_schema()
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        static CHILDREN: Vec<PlanNode> = Vec::new();
        &CHILDREN
    }

    fn get_type(&self) -> PlanType {
        PlanType::Insert
    }

    fn to_string(&self, with_schema: bool) -> String {
        let mut result = format!("InsertNode [table: {}]", self.table_name);
        if with_schema {
            result.push_str(&format!(", schema: {}", self.output_schema));
        }
        result
    }

    fn plan_node_to_string(&self) -> String {
        self.to_string(true)
    }

    fn children_to_string(&self, indent: usize) -> String {
        let indent_str = " ".repeat(indent);
        format!("{}Child: {}", indent_str, self.child.plan_node_to_string())
    }
}