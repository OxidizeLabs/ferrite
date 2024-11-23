use crate::catalogue::schema::Schema;
use crate::common::config::TableOidT;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::sync::Arc;

/// Represents a delete operation in the query execution plan.
///
/// DeleteNode is responsible for deleting rows from a table based on
/// the conditions specified in its child node.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteNode {
    output_schema: Arc<Schema>,
    table_id: TableOidT,
    child: Box<PlanNode>,
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
    pub fn new(output_schema: Arc<Schema>, table_id: TableOidT, child: Box<PlanNode>) -> Self {
        DeleteNode {
            output_schema,
            table_id,
            child,
        }
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
        static CHILDREN: Vec<PlanNode> = Vec::new();
        &CHILDREN
    }

    /// Returns the type of this plan node.
    fn get_type(&self) -> PlanType {
        PlanType::Delete
    }

    /// Returns a string representation of this node.
    ///
    /// # Arguments
    ///
    /// * `with_schema` - If true, includes the schema in the string representation.
    fn to_string(&self, with_schema: bool) -> String {
        let mut result = String::from("DeleteNode");
        if with_schema {
            result.push_str(&format!(" [{}]", self.output_schema));
        }
        result
    }

    /// Returns a string representation of this node, including the schema.
    fn plan_node_to_string(&self) -> String {
        self.to_string(true)
    }

    /// Returns a string representation of this node's children.
    ///
    /// # Arguments
    ///
    /// * `indent` - The number of spaces to indent the output.
    fn children_to_string(&self, indent: usize) -> String {
        let indent_str = " ".repeat(indent);
        format!("{}Child: {}", indent_str, self.child.plan_node_to_string())
    }
}

impl DeleteNode {
    /// Returns a reference to the child node.
    pub fn get_child(&self) -> &PlanNode {
        &self.child
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalogue::schema::Schema;
    use crate::execution::plans::mock_scan_plan::MockScanNode;

    fn create_mock_child() -> Box<PlanNode> {
        let schema = Arc::new(Schema::new(vec![]));
        let table = "mock_table".to_string();
        Box::new(PlanNode::MockScan(MockScanNode::new(schema, table, vec![])))
    }

    #[test]
    fn test_delete_node_creation() {
        let schema = Arc::new(Schema::new(vec![]));
        let child = create_mock_child();
        let delete_node = DeleteNode::new(schema.clone(), 0, child);

        assert_eq!(delete_node.get_output_schema(), &*schema);
        assert_eq!(delete_node.get_type(), PlanType::Delete);
    }

    #[test]
    fn test_delete_node_to_string() {
        let schema = Arc::new(Schema::new(vec![]));
        let child = create_mock_child();
        let delete_node = DeleteNode::new(schema, 0, child);

        assert_eq!(delete_node.to_string(false), "DeleteNode");
        assert!(delete_node.to_string(true).starts_with("DeleteNode ["));
    }

    #[test]
    fn test_delete_node_children_to_string() {
        let schema = Arc::new(Schema::new(vec![]));
        let child = create_mock_child();
        let delete_node = DeleteNode::new(schema, 0, child);

        let children_string = delete_node.children_to_string(2);
        assert!(children_string.starts_with("  Child: MockScanNode"));
    }

    #[test]
    fn test_delete_node_get_child() {
        let schema = Arc::new(Schema::new(vec![]));
        let child = create_mock_child();
        let delete_node = DeleteNode::new(schema, 0, child);

        match delete_node.get_child() {
            PlanNode::MockScan(_) => {}
            _ => panic!("Expected child to be MockScanNode"),
        }
    }
}
