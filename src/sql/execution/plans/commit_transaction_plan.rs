use crate::catalog::schema::Schema;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::fmt;
use std::fmt::{Display, Formatter};

/// Represents a plan node that commits a transaction
#[derive(Debug, Clone, PartialEq)]
pub struct CommitTransactionPlanNode {
    output_schema: Schema,
    chain: bool,
    end: bool,
    children: Vec<PlanNode>,
}

impl CommitTransactionPlanNode {
    pub fn new(chain: bool, end: bool) -> Self {
        // Create an empty schema since transaction operations don't return data
        let output_schema = Schema::new(vec![]);

        Self {
            output_schema,
            chain,
            end,
            children: Vec::new(),
        }
    }

    /// Check if the transaction should chain to a new one
    pub fn is_chain(&self) -> bool {
        self.chain
    }

    /// Check if the session should end after commit
    pub fn is_end(&self) -> bool {
        self.end
    }
}

impl AbstractPlanNode for CommitTransactionPlanNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::Transaction
    }
}

impl Display for CommitTransactionPlanNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ CommitTransaction")?;

        if f.alternate() {
            if self.chain {
                write!(f, "\n   Chain: true")?;
            }
            if self.end {
                write!(f, "\n   End: true")?;
            }
        }

        Ok(())
    }
} 