use crate::catalog::schema::Schema;
use crate::concurrency::transaction::IsolationLevel;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::fmt;
use std::fmt::{Display, Formatter};

/// Represents a plan node that starts a transaction with specified isolation level and read-only status
#[derive(Debug, Clone, PartialEq)]
pub struct StartTransactionPlanNode {
    output_schema: Schema,
    isolation_level: Option<IsolationLevel>,
    read_only: bool,
    children: Vec<PlanNode>,
}

impl StartTransactionPlanNode {
    pub fn new(isolation_level: Option<IsolationLevel>, read_only: bool) -> Self {
        // Create an empty schema since transaction operations don't return data
        let output_schema = Schema::new(vec![]);

        Self {
            output_schema,
            isolation_level,
            read_only,
            children: Vec::new(),
        }
    }

    /// Get the isolation level for the transaction, if specified
    pub fn get_isolation_level(&self) -> &Option<IsolationLevel> {
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
        &self.children
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
    use crate::sql::execution::plans::abstract_plan::PlanNode;
    use std::fmt::Write;

    #[test]
    fn test_start_transaction_creation() {
        // Test with isolation level and read-only flag
        let isolation_level = Some(IsolationLevel::Serializable);
        let read_only = true;

        let plan_node = StartTransactionPlanNode::new(isolation_level, read_only);

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
        let plan_node = StartTransactionPlanNode::new(Some(IsolationLevel::Serializable), true);

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
        let plan_node_not_readonly =
            StartTransactionPlanNode::new(Some(IsolationLevel::Serializable), false);
        let detailed_not_readonly = format!("{:#}", plan_node_not_readonly);
        assert!(detailed_not_readonly.contains("Isolation Level: SERIALIZABLE"));
        assert!(!detailed_not_readonly.contains("Read Only"));
    }

    #[test]
    fn test_plan_type() {
        // Test that the plan node returns the correct plan type
        let plan_node = StartTransactionPlanNode::new(None, false);
        assert_eq!(plan_node.get_type(), PlanType::Transaction);
    }

    #[test]
    fn test_different_isolation_levels() {
        // Test with different standard isolation levels
        let levels = vec![
            IsolationLevel::ReadUncommitted,
            IsolationLevel::ReadCommitted,
            IsolationLevel::RepeatableRead,
            IsolationLevel::Serializable,
        ];

        for level in levels {
            let plan_node = StartTransactionPlanNode::new(Some(level), false);
            assert_eq!(plan_node.get_isolation_level(), &Some(level));

            // Check the display
            let detailed = format!("{:#}", plan_node);
            assert!(detailed.contains(&format!("Isolation Level: {}", level)));
        }
    }

    #[test]
    fn test_empty_isolation_level() {
        // Test with empty string for isolation level
        let plan_node = StartTransactionPlanNode::new(None, false);
        assert_eq!(plan_node.get_isolation_level(), &None);

        // When isolation_level is None, it shouldn't be displayed in the output
        let detailed = format!("{:#}", plan_node);
        assert!(!detailed.contains("Isolation Level:"));
    }

    #[test]
    fn test_all_parameter_combinations() {
        // Test all combinations of parameters
        let test_cases = vec![
            (None, false),
            (None, true),
            (Some(IsolationLevel::Serializable), false),
            (Some(IsolationLevel::Serializable), true),
        ];

        for (isolation_level, read_only) in test_cases {
            let plan_node = StartTransactionPlanNode::new(isolation_level, read_only);

            // Verify parameters were set correctly
            assert_eq!(plan_node.get_isolation_level(), &isolation_level);
            assert_eq!(plan_node.is_read_only(), read_only);

            // Verify display
            let detailed = format!("{:#}", plan_node);

            if let Some(level) = &isolation_level {
                assert!(detailed.contains(&format!("Isolation Level: {}", level)));
            } else {
                assert!(!detailed.contains("Isolation Level:"));
            }

            if read_only {
                assert!(detailed.contains("Read Only: true"));
            } else {
                assert!(!detailed.contains("Read Only:"));
            }
        }
    }

    #[test]
    fn test_integration_with_plan_system() {
        // Create a start transaction plan node
        let plan_node = StartTransactionPlanNode::new(Some(IsolationLevel::Serializable), true);

        // Create a PlanNode enum variant from it
        let plan_enum = PlanNode::StartTransaction(plan_node);

        // Test that we can convert back and forth
        match &plan_enum {
            PlanNode::StartTransaction(inner_node) => {
                assert_eq!(
                    inner_node.get_isolation_level(),
                    &Some(IsolationLevel::Serializable)
                );
                assert!(inner_node.is_read_only());
                assert_eq!(inner_node.get_type(), PlanType::Transaction);
                assert!(inner_node.get_children().is_empty());
            },
            _ => panic!("Expected StartTransaction variant"),
        }

        // Test string representation
        assert!(plan_enum.to_string().contains("StartTransaction"));

        // Verify empty output schema through AbstractPlanNode trait
        let schema = plan_enum.get_output_schema();
        assert_eq!(schema.get_columns().len(), 0);

        // Verify no children through AbstractPlanNode trait
        let children = plan_enum.get_children();
        assert!(children.is_empty());
    }

    #[test]
    fn test_explain_output() {
        // Create a start transaction plan node
        let plan_node = StartTransactionPlanNode::new(Some(IsolationLevel::Serializable), true);

        // Create a PlanNode enum variant from it
        let plan_enum = PlanNode::StartTransaction(plan_node);

        // Simulate what explain_internal would do
        let indent = "  ".to_string(); // Indent level 1
        let mut result = String::new();

        match &plan_enum {
            PlanNode::StartTransaction(node) => {
                write!(result, "{}→ StartTransaction", indent).unwrap();
                if let Some(level) = node.get_isolation_level() {
                    write!(result, "\n{}   Isolation Level: {}", indent, level).unwrap();
                }
                if node.is_read_only() {
                    write!(result, "\n{}   Read Only: true", indent).unwrap();
                }
            },
            _ => panic!("Expected StartTransaction variant"),
        }

        // Verify the explain output
        assert!(result.contains("→ StartTransaction"));
        assert!(result.contains("Isolation Level: SERIALIZABLE"));
        assert!(result.contains("Read Only: true"));

        // Check indentation
        let lines: Vec<&str> = result.lines().collect();
        assert!(lines[0].starts_with("  →")); // Should have 2 spaces (indent level 1)
        assert!(lines[1].starts_with("     ")); // Should have 5 spaces (2 + 3)
        assert!(lines[2].starts_with("     ")); // Should have 5 spaces (2 + 3)
    }
}
