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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::execution::plans::abstract_plan::PlanNode;
    use std::fmt::Write;

    #[test]
    fn test_commit_transaction_creation() {
        // Test with chain flag and end flag
        let chain = true;
        let end = true;

        let plan_node = CommitTransactionPlanNode::new(chain, end);

        assert_eq!(plan_node.is_chain(), chain);
        assert_eq!(plan_node.is_end(), end);

        // Verify empty output schema
        assert_eq!(plan_node.get_output_schema().get_columns().len(), 0);

        // Verify no children
        assert!(plan_node.get_children().is_empty());
    }

    #[test]
    fn test_commit_transaction_display() {
        // Create plan with chain and end flags
        let plan_node = CommitTransactionPlanNode::new(true, true);

        // Basic display
        assert_eq!(plan_node.to_string(), "→ CommitTransaction");

        // Detailed display
        let detailed = format!("{:#}", plan_node);
        assert!(detailed.contains("→ CommitTransaction"));
        assert!(detailed.contains("Chain: true"));
        assert!(detailed.contains("End: true"));

        // Create plan without chain but with end
        let plan_node_no_chain = CommitTransactionPlanNode::new(false, true);
        let detailed_no_chain = format!("{:#}", plan_node_no_chain);
        assert!(!detailed_no_chain.contains("Chain: true"));
        assert!(detailed_no_chain.contains("End: true"));

        // Create plan with chain but without end
        let plan_node_no_end = CommitTransactionPlanNode::new(true, false);
        let detailed_no_end = format!("{:#}", plan_node_no_end);
        assert!(detailed_no_end.contains("Chain: true"));
        assert!(!detailed_no_end.contains("End: true"));
    }

    #[test]
    fn test_plan_type() {
        // Test that the plan node returns the correct plan type
        let plan_node = CommitTransactionPlanNode::new(false, false);
        assert_eq!(plan_node.get_type(), PlanType::Transaction);
    }

    #[test]
    fn test_all_parameter_combinations() {
        // Test all combinations of parameters
        let test_cases = vec![(false, false), (false, true), (true, false), (true, true)];

        for (chain, end) in test_cases {
            let plan_node = CommitTransactionPlanNode::new(chain, end);

            // Verify parameters were set correctly
            assert_eq!(plan_node.is_chain(), chain);
            assert_eq!(plan_node.is_end(), end);

            // Verify display
            let detailed = format!("{:#}", plan_node);

            if chain {
                assert!(detailed.contains("Chain: true"));
            } else {
                assert!(!detailed.contains("Chain:"));
            }

            if end {
                assert!(detailed.contains("End: true"));
            } else {
                assert!(!detailed.contains("End:"));
            }
        }
    }

    #[test]
    fn test_integration_with_plan_system() {
        // Create a commit transaction plan node
        let plan_node = CommitTransactionPlanNode::new(true, true);

        // Create a PlanNode enum variant from it
        let plan_enum = PlanNode::CommitTransaction(plan_node);

        // Test that we can convert back and forth
        match &plan_enum {
            PlanNode::CommitTransaction(inner_node) => {
                assert!(inner_node.is_chain());
                assert!(inner_node.is_end());
                assert_eq!(inner_node.get_type(), PlanType::Transaction);
                assert!(inner_node.get_children().is_empty());
            },
            _ => panic!("Expected CommitTransaction variant"),
        }

        // Test string representation
        assert!(plan_enum.to_string().contains("CommitTransaction"));

        // Verify empty output schema through AbstractPlanNode trait
        let schema = plan_enum.get_output_schema();
        assert_eq!(schema.get_columns().len(), 0);

        // Verify no children through AbstractPlanNode trait
        let children = plan_enum.get_children();
        assert!(children.is_empty());
    }

    #[test]
    fn test_explain_output() {
        // Create a commit transaction plan node
        let plan_node = CommitTransactionPlanNode::new(true, true);

        // Create a PlanNode enum variant from it
        let plan_enum = PlanNode::CommitTransaction(plan_node);

        // Simulate what explain_internal would do
        let indent = "  ".to_string(); // Indent level 1
        let mut result = String::new();

        match &plan_enum {
            PlanNode::CommitTransaction(node) => {
                write!(result, "{}→ CommitTransaction", indent).unwrap();
                if node.is_chain() {
                    write!(result, "\n{}   Chain: true", indent).unwrap();
                }
                if node.is_end() {
                    write!(result, "\n{}   End: true", indent).unwrap();
                }
            },
            _ => panic!("Expected CommitTransaction variant"),
        }

        // Verify the explain output
        assert!(result.contains("→ CommitTransaction"));
        assert!(result.contains("Chain: true"));
        assert!(result.contains("End: true"));

        // Check indentation
        let lines: Vec<&str> = result.lines().collect();
        assert!(lines[0].starts_with("  →")); // Should have 2 spaces (indent level 1)
        assert!(lines[1].starts_with("     ")); // Should have 5 spaces (2 + 3)
        assert!(lines[2].starts_with("     ")); // Should have 5 spaces (2 + 3)
    }

    #[test]
    fn test_simple_commit() {
        // Test simple commit without chain or end
        let simple_commit = CommitTransactionPlanNode::new(false, false);
        assert!(!simple_commit.is_chain());
        assert!(!simple_commit.is_end());

        let detailed = format!("{:#}", simple_commit);
        assert!(detailed.contains("→ CommitTransaction"));
        assert!(!detailed.contains("Chain:"));
        assert!(!detailed.contains("End:"));
    }

    #[test]
    fn test_commit_and_chain() {
        // Test commit and chain (start new transaction after commit)
        let commit_and_chain = CommitTransactionPlanNode::new(true, false);
        assert!(commit_and_chain.is_chain());
        assert!(!commit_and_chain.is_end());

        let detailed = format!("{:#}", commit_and_chain);
        assert!(detailed.contains("Chain: true"));
        assert!(!detailed.contains("End:"));
    }

    #[test]
    fn test_commit_and_end() {
        // Test commit and end session
        let commit_and_end = CommitTransactionPlanNode::new(false, true);
        assert!(!commit_and_end.is_chain());
        assert!(commit_and_end.is_end());

        let detailed = format!("{:#}", commit_and_end);
        assert!(!detailed.contains("Chain:"));
        assert!(detailed.contains("End: true"));
    }

    #[test]
    fn test_commit_chain_and_end() {
        // Test commit with both chain and end (unusual but valid combination)
        let commit_chain_end = CommitTransactionPlanNode::new(true, true);
        assert!(commit_chain_end.is_chain());
        assert!(commit_chain_end.is_end());

        let detailed = format!("{:#}", commit_chain_end);
        assert!(detailed.contains("Chain: true"));
        assert!(detailed.contains("End: true"));
    }

    #[test]
    fn test_schema_and_children_properties() {
        // Test that schema and children properties are correctly implemented
        let plan_node = CommitTransactionPlanNode::new(true, false);

        // Test schema
        let schema = plan_node.get_output_schema();
        assert_eq!(schema.get_columns().len(), 0);

        // Test children
        let children = plan_node.get_children();
        assert!(children.is_empty());
        assert_eq!(children.len(), 0);

        // Test plan type
        assert_eq!(plan_node.get_type(), PlanType::Transaction);
    }

    #[test]
    fn test_display_formatting_edge_cases() {
        // Test display with no flags set
        let no_flags = CommitTransactionPlanNode::new(false, false);
        let basic_display = no_flags.to_string();
        let detailed_display = format!("{:#}", no_flags);

        assert_eq!(basic_display, "→ CommitTransaction");
        assert_eq!(detailed_display, "→ CommitTransaction");

        // Test display with only chain flag
        let only_chain = CommitTransactionPlanNode::new(true, false);
        let detailed_chain = format!("{:#}", only_chain);
        assert!(detailed_chain.contains("Chain: true"));
        assert!(!detailed_chain.contains("End:"));

        // Test display with only end flag
        let only_end = CommitTransactionPlanNode::new(false, true);
        let detailed_end = format!("{:#}", only_end);
        assert!(!detailed_end.contains("Chain:"));
        assert!(detailed_end.contains("End: true"));
    }
}
