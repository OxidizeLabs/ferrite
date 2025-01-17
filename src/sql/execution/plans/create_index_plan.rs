use crate::catalog::schema::Schema;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexPlanNode {
    output_schema: Schema,
    table_name: String,
    index_name: String,
    key_attrs: Vec<usize>,
    if_not_exists: bool,
}

impl CreateIndexPlanNode {
    pub fn new(output_schema: Schema, table_name: String, index_name: String, key_attrs: Vec<usize>, if_not_exists: bool) -> Self {
        Self {
            output_schema,
            table_name,
            index_name,
            key_attrs,
            if_not_exists,
        }
    }

    pub fn get_index_name(&self) -> &str {
        &self.index_name
    }

    pub fn get_table_name(&self) -> &str {
        &self.table_name
    }

    pub fn get_key_attrs(&self) -> &[usize] {
        &self.key_attrs
    }

    pub fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }
}

impl AbstractPlanNode for CreateIndexPlanNode {
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

impl Display for CreateIndexPlanNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ CreateIndex: {} on {}", self.index_name, self.table_name)?;

        if f.alternate() {
            write!(f, "\n   Key Columns: {:?}", self.key_attrs)?;
            if self.if_not_exists {
                write!(f, "\n   IF NOT EXISTS")?;
            }
            write!(f, "\n   Schema: {}", self.output_schema)?;
        }

        Ok(())
    }
}

