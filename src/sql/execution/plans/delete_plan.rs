use crate::catalog::schema::Schema;
use crate::common::config::TableOidT;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::fmt::{Display, Formatter};

/// Represents a delete operation in the query execution plan.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteNode {
    output_schema: Schema,
    table_name: String,
    table_id: TableOidT,
    children: Vec<PlanNode>,
}

impl DeleteNode {
    /// Creates a new DeleteNode with the given output schema and child node.
    pub fn new(
        output_schema: Schema,
        table_name: String,
        table_id: TableOidT,
        children: Vec<PlanNode>,
    ) -> Self {
        Self {
            output_schema,
            table_name,
            table_id,
            children,
        }
    }

    pub fn get_table_name(&self) -> &str {
        &self.table_name
    }

    pub fn get_table_id(&self) -> TableOidT {
        self.table_id
    }
}

impl AbstractPlanNode for DeleteNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::Delete
    }
}

impl Display for DeleteNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            writeln!(f, "Delete [")?;
            writeln!(f, "  Table: {} (OID: {})", self.table_name, self.table_id)?;
            write!(f, "  Schema: {}", self.output_schema)?;
            for child in &self.children {
                write!(f, "\n  {}", child)?;
            }
            write!(f, "\n]")
        } else {
            write!(f, "Delete({})", self.table_name)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::execution::plans::mock_scan_plan::MockScanNode;

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

        // Test basic format
        assert_eq!(delete_node.to_string(), "Delete(test_table)");

        // Test detailed format
        let detailed = format!("{:#}", delete_node);
        assert!(detailed.starts_with("Delete ["));
        assert!(detailed.contains("Table: test_table"));
    }

    #[test]
    fn test_delete_node_no_children() {
        let table_name = "test_table".to_string();
        let schema = Schema::new(vec![]);
        let delete_node = DeleteNode::new(schema.clone(), table_name, 0, vec![]);

        // Verify empty children list
        assert!(delete_node.get_children().is_empty());
    }
}
