use std::fmt::Display;
use crate::catalog::schema::Schema;
use crate::common::config::TableOidT;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};

/// Represents a delete operation in the query execution plan.
///
/// DeleteNode is responsible for deleting rows from a table based on
/// the conditions specified in its child node.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteNode {
    output_schema: Schema,
    table_name: String,
    table_id: TableOidT,
    children: Vec<PlanNode>,
}

impl DeleteNode {
    /// Creates a new DeleteNode with the given output schema and child node.
    ///
    /// # Arguments
    ///
    /// * `output_schema` - The schema of the output after the delete operation.
    /// * `table_id` - The TableID of the table to delete from
    /// * `child` - The child node that produces the rows to be deleted.
    ///
    /// # Returns
    ///
    /// A new instance of DeleteNode.
    pub fn new(output_schema: Schema, table_name: String, table_id: TableOidT, children: Vec<PlanNode>) -> Self {
        DeleteNode {
            output_schema,
            table_name,
            table_id,
            children,
        }
    }

    pub fn get_table_name(&self) -> &str {
        &self.table_name
    }
}

impl AbstractPlanNode for DeleteNode {
    /// Returns a reference to the output schema of this node.
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

    /// Returns the type of this plan node.
    fn get_type(&self) -> PlanType {
        todo!()
    }
}

impl Display for DeleteNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "→ Delete from {}", self.table_name)?;
        
        if f.alternate() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::schema::Schema;
    use crate::execution::plans::mock_scan_plan::MockScanNode;

    fn create_mock_child(name: &str) -> PlanNode {
        let schema = Schema::new(vec![]);
        let table = name.to_string();
        PlanNode::MockScan(MockScanNode::new(schema, table, vec![]))
    }

    #[test]
    fn test_delete_node_creation() {
        let table_name = "test_table".to_string();
        let schema = Schema::new(vec![]);
        let child = create_mock_child("child1");
        let delete_node = DeleteNode::new(schema.clone(), table_name, 0, vec![child]);

        assert_eq!(delete_node.get_output_schema(), &schema);
        assert_eq!(delete_node.get_type(), PlanType::Delete);
    }

    #[test]
    fn test_delete_node_to_string() {
        let table_name = "test_table".to_string();
        let schema = Schema::new(vec![]);
        let child = create_mock_child("child1");
        let delete_node = DeleteNode::new(schema.clone(), table_name, 0, vec![child]);

        assert_eq!(delete_node.to_string(), "DeleteNode");
        assert!(delete_node.to_string().starts_with("DeleteNode ["));
    }

    #[test]
    fn test_delete_node_no_children() {
        let table_name = "test_table".to_string();
        let schema = Schema::new(vec![]);
        let delete_node = DeleteNode::new(schema.clone(), table_name, 0, vec![]);

        // Verify empty children list
        assert!(delete_node.get_children().is_empty());

        // Verify children string is empty
        let children_string = delete_node.children.get(0).unwrap().to_string();
        assert_eq!(children_string, "");
    }

    #[test]
    fn test_delete_node_single_child() {
        let table_name = "test_table".to_string();
        let schema = Schema::new(vec![]);
        let child = create_mock_child("child1");
        let delete_node = DeleteNode::new(schema.clone(), table_name, 0, vec![child]);

        let children_string0 = delete_node.children.get(0).unwrap().to_string();
        let children_string1 = delete_node.children.get(1).unwrap().to_string();

        assert!(children_string0.contains("Child 0:"));
        assert!(children_string0.contains("MockScanNode"));
        assert!(!children_string1.contains("Child 1:"));
    }

    #[test]
    fn test_delete_node_multiple_children() {
        let table_name = "test_table".to_string();
        let schema = Schema::new(vec![]);
        let child1 = create_mock_child("child1");
        let child2 = create_mock_child("child2");
        let child3 = create_mock_child("child3");
        let delete_node = DeleteNode::new(
            schema.clone(),
            table_name,
            0,
            vec![child1, child2, child3],
        );

        let children_string = delete_node.children.iter().map(|child| child.to_string()).collect::<Vec<String>>().join("\n");

        // Verify all children are present and properly numbered
        assert!(children_string.contains("Child 0:"));
        assert!(children_string.contains("Child 1:"));
        assert!(children_string.contains("Child 2:"));
        assert!(!children_string.contains("Child 3:"));

        // Verify proper indentation (2 spaces)
        assert!(children_string.lines().all(|line| line.starts_with("  ")));

        // Count number of child nodes
        let child_count = children_string.matches("Child").count();
        assert_eq!(child_count, 3);
    }

    #[test]
    fn test_delete_node_table_properties() {
        let table_name = "test_table".to_string();
        let table_id: TableOidT = 42;
        let schema = Schema::new(vec![]);
        let child = create_mock_child("child1");
        let delete_node = DeleteNode::new(schema.clone(), table_name.clone(), table_id, vec![child]);

        // Test table name and ID are stored correctly
        assert_eq!(delete_node.table_name, table_name);
        assert_eq!(delete_node.table_id, table_id);
    }
}
