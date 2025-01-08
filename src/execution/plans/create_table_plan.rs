use crate::catalog::schema::Schema;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTablePlanNode {
    output_schema: Schema,
    table_name: String,
    if_not_exists: bool,
}

impl CreateTablePlanNode {
    pub fn new(output_schema: Schema, table_name: String, if_not_exists: bool) -> Self {
        Self {
            output_schema,
            table_name,
            if_not_exists,
        }
    }

    pub fn get_table_name(&self) -> &str {
        &self.table_name
    }

    pub fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }
}

impl AbstractPlanNode for CreateTablePlanNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        static CHILDREN: Vec<PlanNode> = Vec::new();
        &CHILDREN
    }

    fn get_type(&self) -> PlanType {
        PlanType::CreateTable
    }

    fn to_string(&self, with_schema: bool) -> String {
        let mut result = format!("CreateTable {{ table: {} }}", self.table_name);
        if with_schema {
            result.push_str(&format!("\nSchema: {}", self.output_schema));
        }
        result
    }

    fn plan_node_to_string(&self) -> String {
        self.to_string(true)
    }

    fn children_to_string(&self, _indent: usize) -> String {
        String::new()
    }
}
