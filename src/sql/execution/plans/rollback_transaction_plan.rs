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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::execution::plans::abstract_plan::PlanNode;
    use std::fmt::Write;

    #[test]
    fn test_rollback_transaction_creation() {
        // Test with chain flag and savepoint
        let chain = true;
        let savepoint = Some("sp1".to_string());

        let plan_node = RollbackTransactionPlanNode::new(chain, savepoint.clone());

        assert_eq!(plan_node.is_chain(), chain);
        assert_eq!(plan_node.get_savepoint(), &savepoint);

        // Verify empty output schema
        assert_eq!(plan_node.get_output_schema().get_columns().len(), 0);

        // Verify no children
        assert!(plan_node.get_children().is_empty());
    }

    #[test]
    fn test_rollback_transaction_display() {
        // Create plan with chain and savepoint
        let plan_node = RollbackTransactionPlanNode::new(true, Some("sp1".to_string()));

        // Basic display
        assert_eq!(plan_node.to_string(), "→ RollbackTransaction");

        // Detailed display
        let detailed = format!("{:#}", plan_node);
        assert!(detailed.contains("→ RollbackTransaction"));
        assert!(detailed.contains("Chain: true"));
        assert!(detailed.contains("Savepoint: sp1"));

        // Create plan without chain but with savepoint
        let plan_node_no_chain = RollbackTransactionPlanNode::new(false, Some("sp2".to_string()));
        let detailed_no_chain = format!("{:#}", plan_node_no_chain);
        assert!(!detailed_no_chain.contains("Chain: true"));
        assert!(detailed_no_chain.contains("Savepoint: sp2"));

        // Create plan with chain but without savepoint
        let plan_node_no_savepoint = RollbackTransactionPlanNode::new(true, None);
        let detailed_no_savepoint = format!("{:#}", plan_node_no_savepoint);
        assert!(detailed_no_savepoint.contains("Chain: true"));
        assert!(!detailed_no_savepoint.contains("Savepoint:"));
    }

    #[test]
    fn test_plan_type() {
        // Test that the plan node returns the correct plan type
        let plan_node = RollbackTransactionPlanNode::new(false, None);
        assert_eq!(plan_node.get_type(), PlanType::Transaction);
    }

    #[test]
    fn test_different_savepoints() {
        // Test with different savepoint names
        let savepoints = vec![
            Some("savepoint1".to_string()),
            Some("sp_test".to_string()),
            Some("checkpoint_a".to_string()),
            Some("nested_sp".to_string()),
        ];

        for savepoint in savepoints {
            let plan_node = RollbackTransactionPlanNode::new(false, savepoint.clone());
            assert_eq!(plan_node.get_savepoint(), &savepoint);

            // Check the display
            let detailed = format!("{:#}", plan_node);
            if let Some(sp) = &savepoint {
                assert!(detailed.contains(&format!("Savepoint: {}", sp)));
            }
        }
    }

    #[test]
    fn test_empty_savepoint() {
        // Test with no savepoint (full rollback)
        let plan_node = RollbackTransactionPlanNode::new(false, None);
        assert_eq!(plan_node.get_savepoint(), &None);

        // When savepoint is None, it shouldn't be displayed in the output
        let detailed = format!("{:#}", plan_node);
        assert!(!detailed.contains("Savepoint:"));
    }

    #[test]
    fn test_all_parameter_combinations() {
        // Test all combinations of parameters
        let test_cases = vec![
            (false, None),
            (false, Some("sp1".to_string())),
            (true, None),
            (true, Some("sp2".to_string())),
        ];

        for (chain, savepoint) in test_cases {
            let plan_node = RollbackTransactionPlanNode::new(chain, savepoint.clone());

            // Verify parameters were set correctly
            assert_eq!(plan_node.is_chain(), chain);
            assert_eq!(plan_node.get_savepoint(), &savepoint);

            // Verify display
            let detailed = format!("{:#}", plan_node);

            if chain {
                assert!(detailed.contains("Chain: true"));
            } else {
                assert!(!detailed.contains("Chain:"));
            }

            if let Some(sp) = &savepoint {
                assert!(detailed.contains(&format!("Savepoint: {}", sp)));
            } else {
                assert!(!detailed.contains("Savepoint:"));
            }
        }
    }

    #[test]
    fn test_integration_with_plan_system() {
        // Create a rollback transaction plan node
        let plan_node = RollbackTransactionPlanNode::new(true, Some("test_savepoint".to_string()));

        // Create a PlanNode enum variant from it
        let plan_enum = PlanNode::RollbackTransaction(plan_node);

        // Test that we can convert back and forth
        match &plan_enum {
            PlanNode::RollbackTransaction(inner_node) => {
                assert_eq!(inner_node.is_chain(), true);
                assert_eq!(
                    inner_node.get_savepoint(),
                    &Some("test_savepoint".to_string())
                );
                assert_eq!(inner_node.get_type(), PlanType::Transaction);
                assert!(inner_node.get_children().is_empty());
            }
            _ => panic!("Expected RollbackTransaction variant"),
        }

        // Test string representation
        assert!(plan_enum.to_string().contains("RollbackTransaction"));

        // Verify empty output schema through AbstractPlanNode trait
        let schema = plan_enum.get_output_schema();
        assert_eq!(schema.get_columns().len(), 0);

        // Verify no children through AbstractPlanNode trait
        let children = plan_enum.get_children();
        assert!(children.is_empty());
    }

    #[test]
    fn test_explain_output() {
        // Create a rollback transaction plan node
        let plan_node = RollbackTransactionPlanNode::new(true, Some("sp1".to_string()));

        // Create a PlanNode enum variant from it
        let plan_enum = PlanNode::RollbackTransaction(plan_node);

        // Simulate what explain_internal would do
        let indent = "  ".repeat(1); // Indent level 1
        let mut result = String::new();

        match &plan_enum {
            PlanNode::RollbackTransaction(node) => {
                write!(result, "{}→ RollbackTransaction", indent).unwrap();
                if node.is_chain() {
                    write!(result, "\n{}   Chain: true", indent).unwrap();
                }
                if let Some(savepoint) = node.get_savepoint() {
                    write!(result, "\n{}   Savepoint: {}", indent, savepoint).unwrap();
                }
            }
            _ => panic!("Expected RollbackTransaction variant"),
        }

        // Verify the explain output
        assert!(result.contains("→ RollbackTransaction"));
        assert!(result.contains("Chain: true"));
        assert!(result.contains("Savepoint: sp1"));

        // Check indentation
        let lines: Vec<&str> = result.lines().collect();
        assert!(lines[0].starts_with("  →")); // Should have 2 spaces (indent level 1)
        assert!(lines[1].starts_with("     ")); // Should have 5 spaces (2 + 3)
        assert!(lines[2].starts_with("     ")); // Should have 5 spaces (2 + 3)
    }

    #[test]
    fn test_rollback_to_savepoint_vs_full_rollback() {
        // Test rollback to savepoint
        let savepoint_rollback = RollbackTransactionPlanNode::new(false, Some("sp1".to_string()));
        assert_eq!(savepoint_rollback.get_savepoint(), &Some("sp1".to_string()));
        assert!(!savepoint_rollback.is_chain());

        let detailed_savepoint = format!("{:#}", savepoint_rollback);
        assert!(detailed_savepoint.contains("Savepoint: sp1"));
        assert!(!detailed_savepoint.contains("Chain:"));

        // Test full rollback
        let full_rollback = RollbackTransactionPlanNode::new(false, None);
        assert_eq!(full_rollback.get_savepoint(), &None);
        assert!(!full_rollback.is_chain());

        let detailed_full = format!("{:#}", full_rollback);
        assert!(!detailed_full.contains("Savepoint:"));
        assert!(!detailed_full.contains("Chain:"));
    }

    #[test]
    fn test_rollback_and_chain() {
        // Test rollback and chain (start new transaction after rollback)
        let rollback_and_chain = RollbackTransactionPlanNode::new(true, None);
        assert!(rollback_and_chain.is_chain());
        assert_eq!(rollback_and_chain.get_savepoint(), &None);

        let detailed = format!("{:#}", rollback_and_chain);
        assert!(detailed.contains("Chain: true"));
        assert!(!detailed.contains("Savepoint:"));

        // Test rollback to savepoint and chain
        let savepoint_and_chain = RollbackTransactionPlanNode::new(true, Some("sp1".to_string()));
        assert!(savepoint_and_chain.is_chain());
        assert_eq!(
            savepoint_and_chain.get_savepoint(),
            &Some("sp1".to_string())
        );

        let detailed_both = format!("{:#}", savepoint_and_chain);
        assert!(detailed_both.contains("Chain: true"));
        assert!(detailed_both.contains("Savepoint: sp1"));
    }
}
