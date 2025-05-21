use crate::catalog::schema::Schema;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::fmt;
use std::fmt::{Display, Formatter};

/// Represents a plan node that rolls back a transaction
#[derive(Debug, Clone, PartialEq)]
pub struct RollbackTransactionPlanNode {
    output_schema: Schema,
    chain: bool,
    savepoint: Option<String>,
    children: Vec<PlanNode>,
}

impl RollbackTransactionPlanNode {
    pub fn new(chain: bool, savepoint: Option<String>) -> Self {
        // Create an empty schema since transaction operations don't return data
        let output_schema = Schema::new(vec![]);

        Self {
            output_schema,
            chain,
            savepoint,
            children: Vec::new(),
        }
    }

    /// Check if the transaction should chain to a new one
    pub fn is_chain(&self) -> bool {
        self.chain
    }

    /// Get the savepoint name if this is a partial rollback
    pub fn get_savepoint(&self) -> &Option<String> {
        &self.savepoint
    }
}

impl AbstractPlanNode for RollbackTransactionPlanNode {
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

impl Display for RollbackTransactionPlanNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ RollbackTransaction")?;

        if f.alternate() {
            if self.chain {
                write!(f, "\n   Chain: true")?;
            }
            if let Some(savepoint) = &self.savepoint {
                write!(f, "\n   Savepoint: {}", savepoint)?;
            }
        }

        Ok(())
    }
} 