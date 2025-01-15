use crate::catalog::schema::Schema;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::fmt;
use std::fmt::{Display, Formatter};

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
}

impl Display for CreateTablePlanNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ CreateTable: {}", self.table_name)?;

        if f.alternate() {
            if self.if_not_exists {
                write!(f, "\n   IF NOT EXISTS")?;
            }
            write!(f, "\n   Schema: {}", self.output_schema)?;
        }

        Ok(())
    }
}
