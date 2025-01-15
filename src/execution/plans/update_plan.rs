use crate::catalog::schema::Schema;
use crate::common::config::TableOidT;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateNode {
    output_schema: Schema,
    table_name: String,
    table_id: TableOidT,
    target_expressions: Vec<Arc<Expression>>,
    children: Vec<PlanNode>,
}

impl UpdateNode {
    pub fn new(output_schema: Schema,
               table_name: String,
               table_id: TableOidT,
               target_expressions: Vec<Arc<Expression>>,
               children: Vec<PlanNode>,
    ) -> Self {
        Self {
            output_schema,
            table_name,
            table_id,
            target_expressions,
            children,
        }
    }

    pub fn get_table_name(&self) -> &str {
        &self.table_name
    }
}

impl AbstractPlanNode for UpdateNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    /// Returns a reference to the child nodes of this node.
    ///
    /// Note: Currently, DeleteNode only has one child, but this method
    /// returns an empty vector for consistency with the trait definition.
    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::Update
    }
}

impl Display for UpdateNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ Update: {}", self.table_name)?;

        if f.alternate() {
            write!(f, "\n   Target Expressions: [")?;
            for (i, expr) in self.target_expressions.iter().enumerate() {
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
