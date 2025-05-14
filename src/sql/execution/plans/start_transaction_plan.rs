use crate::catalog::schema::Schema;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::fmt;
use std::fmt::{Display, Formatter};

/// Represents a plan node that starts a transaction with specified isolation level and read-only status
#[derive(Debug, Clone, PartialEq)]
pub struct StartTransactionPlanNode {
    output_schema: Schema,
    isolation_level: Option<String>,
    read_only: bool,
}

impl StartTransactionPlanNode {
    pub fn new(isolation_level: Option<String>, read_only: bool) -> Self {
        // Create an empty schema since transaction operations don't return data
        let output_schema = Schema::new(vec![]);
        
        Self {
            output_schema,
            isolation_level,
            read_only,
        }
    }

    /// Get the isolation level for the transaction, if specified
    pub fn get_isolation_level(&self) -> &Option<String> {
        &self.isolation_level
    }

    /// Check if the transaction is read-only
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }
}

impl AbstractPlanNode for StartTransactionPlanNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        static CHILDREN: Vec<PlanNode> = Vec::new();
        &CHILDREN
    }

    fn get_type(&self) -> PlanType {
        PlanType::Transaction
    }
}

impl Display for StartTransactionPlanNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ StartTransaction")?;

        if f.alternate() {
            if let Some(level) = &self.isolation_level {
                write!(f, "\n   Isolation Level: {}", level)?;
            }
            
            if self.read_only {
                write!(f, "\n   Read Only: true")?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_start_transaction_creation() {
        // Test with isolation level and read-only flag
        let isolation_level = Some(String::from("SERIALIZABLE"));
        let read_only = true;
        
        let plan_node = StartTransactionPlanNode::new(isolation_level.clone(), read_only);
        
        assert_eq!(plan_node.get_isolation_level(), &isolation_level);
        assert_eq!(plan_node.is_read_only(), read_only);
        
        // Verify empty output schema
        assert_eq!(plan_node.get_output_schema().get_columns().len(), 0);
        
        // Verify no children
        assert!(plan_node.get_children().is_empty());
    }
    
    #[test]
    fn test_start_transaction_display() {
        // Create plan with isolation level and read-only flag
        let plan_node = StartTransactionPlanNode::new(Some(String::from("SERIALIZABLE")), true);
        
        // Basic display
        assert_eq!(plan_node.to_string(), "→ StartTransaction");
        
        // Detailed display
        let detailed = format!("{:#}", plan_node);
        assert!(detailed.contains("→ StartTransaction"));
        assert!(detailed.contains("Isolation Level: SERIALIZABLE"));
        assert!(detailed.contains("Read Only: true"));
        
        // Create plan without isolation level but with read-only flag
        let plan_node_no_isolation = StartTransactionPlanNode::new(None, true);
        let detailed_no_isolation = format!("{:#}", plan_node_no_isolation);
        assert!(!detailed_no_isolation.contains("Isolation Level"));
        assert!(detailed_no_isolation.contains("Read Only: true"));
        
        // Create plan with isolation level but without read-only flag
        let plan_node_not_readonly = StartTransactionPlanNode::new(Some(String::from("READ COMMITTED")), false);
        let detailed_not_readonly = format!("{:#}", plan_node_not_readonly);
        assert!(detailed_not_readonly.contains("Isolation Level: READ COMMITTED"));
        assert!(!detailed_not_readonly.contains("Read Only"));
    }
} 