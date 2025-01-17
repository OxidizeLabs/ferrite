use crate::catalog::schema::Schema;
use crate::common::config::{IndexOidT, TableOidT};
use crate::sql::execution::expressions::abstract_expression::Expression;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct IndexScanNode {
    output_schema: Schema,
    table_name: String,
    table_id: TableOidT,
    index_name: String,
    index_id: IndexOidT,
    predicate_keys: Vec<Arc<Expression>>,
    children: Vec<PlanNode>,
}

impl IndexScanNode {
    pub fn new(
        output_schema: Schema,
        table_name: String,
        table_id: TableOidT,
        index_name: String,
        index_id: IndexOidT,
        predicate_keys: Vec<Arc<Expression>>,
    ) -> Self {
        Self {
            output_schema,
            table_name,
            table_id,
            index_name,
            index_id,
            predicate_keys,
            children: Vec::new(),
        }
    }

    pub fn get_index_name(&self) -> String {
        self.index_name.to_string()
    }

    pub fn get_table_name(&self) -> &str {
        &self.table_name
    }

    pub fn get_index_id(&self) -> IndexOidT {
        self.index_id
    }

    pub fn get_predicate_keys(&self) -> &Vec<Arc<Expression>> {
        &self.predicate_keys
    }
}

impl AbstractPlanNode for IndexScanNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::IndexScan
    }
}

impl Display for IndexScanNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ IndexScan: {} using {}", self.table_name, self.index_name)?;

        if f.alternate() {
            write!(f, "\n   Predicate Keys: [")?;
            for (i, key) in self.predicate_keys.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", key)?;
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
