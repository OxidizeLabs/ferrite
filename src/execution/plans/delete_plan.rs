use crate::catalogue::schema::Schema;
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
    children: Vec<PlanNode>
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
        let mut result = String::new();

        // Add left child
        result.push_str(&format!("{}Left Child: {}\n", indent_str, self.get_children()[0].plan_node_to_string()));
        result.push_str(&self.get_children()[0].children_to_string(indent + 2));

        // Add right child
        result.push_str(&format!("{}Right Child: {}\n", indent_str, self.get_children()[1].plan_node_to_string()));
        result.push_str(&self.get_children()[1].children_to_string(indent + 2));

        result
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalogue::schema::Schema;
    use crate::execution::plans::mock_scan_plan::MockScanNode;

    fn create_mock_child() -> PlanNode {
        let schema = Schema::new(vec![]);
        let table = "mock_table".to_string();
        PlanNode::MockScan(MockScanNode::new(schema, table, vec![]))
    }

    #[test]
    fn test_delete_node_creation() {
        let table_name = "mock_table".to_string();
        let schema = Schema::new(vec![]);
        let child = create_mock_child();
        let delete_node = DeleteNode::new(schema.clone(), table_name, 0, vec![child]);

        assert_eq!(delete_node.get_output_schema(), &schema);
        assert_eq!(delete_node.get_type(), PlanType::Delete);
    }

    #[test]
    fn test_delete_node_to_string() {
        let table_name = "mock_table".to_string();
        let schema = Schema::new(vec![]);
        let child = create_mock_child();
        let delete_node = DeleteNode::new(schema.clone(), table_name, 0, vec![child]);

        assert_eq!(delete_node.to_string(false), "DeleteNode");
        assert!(delete_node.to_string(true).starts_with("DeleteNode ["));
    }

    #[test]
    fn test_delete_node_children_to_string() {
        let table_name = "mock_table".to_string();
        let schema = Schema::new(vec![]);
        let child = create_mock_child();
        let delete_node = DeleteNode::new(schema.clone(), table_name, 0, vec![child]);

        let children_string = delete_node.children_to_string(2);
        assert!(children_string.starts_with("  Child: MockScanNode"));
    }

    #[test]
    fn test_delete_node_get_child() {
        let table_name = "mock_table".to_string();
        let schema = Schema::new(vec![]);
        let child = create_mock_child();
        let delete_node = DeleteNode::new(schema.clone(), table_name, 0, vec![child]);

        match delete_node.get_children()[0] {
            PlanNode::MockScan(_) => {}
            _ => panic!("Expected child to be MockScanNode"),
        }
    }
}
