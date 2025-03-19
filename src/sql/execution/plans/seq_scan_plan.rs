use crate::catalog::schema::Schema;
use crate::common::config::TableOidT;
use crate::sql::binder::table_ref::bound_base_table_ref::BoundBaseTableRef;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub struct SeqScanPlanNode {
    output_schema: Schema,
    table_oid: TableOidT,
    table_name: String,
    children: Vec<PlanNode>,
}

impl SeqScanPlanNode {
    pub fn new(output_schema: Schema, table_oid: TableOidT, table_name: String) -> Self {
        Self {
            output_schema,
            table_oid,
            table_name,
            children: Vec::new(),
        }
    }

    pub fn get_table_oid(&self) -> TableOidT {
        self.table_oid
    }

    pub fn get_table_name(&self) -> &str {
        &self.table_name
    }

    pub fn infer_scan_schema(table_ref: &BoundBaseTableRef) -> Result<Schema, String> {
        // This is a placeholder implementation. You should replace this with actual schema inference logic.
        let schema = table_ref.get_schema();
        Ok(schema.clone())
    }
}

impl AbstractPlanNode for SeqScanPlanNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::SeqScan
    }
}

impl Display for SeqScanPlanNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ SeqScan: {}", self.table_name)?;

        if f.alternate() {
            write!(f, "\n   Table ID: {}", self.table_oid)?;
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

impl From<SeqScanPlanNode> for PlanNode {
    fn from(node: SeqScanPlanNode) -> Self {
        PlanNode::SeqScan(node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::types_db::type_id::TypeId;

    // Test fixtures
    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ])
    }

    fn create_test_node() -> (SeqScanPlanNode, Schema, TableOidT, String) {
        let schema = create_test_schema();
        let table_oid = 42;
        let table_name = "test_table".to_string();
        let node = SeqScanPlanNode::new(schema.clone(), table_oid, table_name.clone());
        (node, schema, table_oid, table_name)
    }

    mod basic_functionality {
        use super::*;

        #[test]
        fn test_node_creation() {
            let (node, schema, table_oid, table_name) = create_test_node();

            assert_eq!(node.get_output_schema(), &schema);
            assert_eq!(node.get_table_oid(), table_oid);
            assert_eq!(node.get_table_name(), &table_name);
            assert!(node.get_children().is_empty());
            assert_eq!(node.get_type(), PlanType::SeqScan);
        }

        #[test]
        fn test_plan_node_conversion() {
            let (node, _, _, _) = create_test_node();
            let plan_node: PlanNode = node.clone().into();

            match plan_node {
                PlanNode::SeqScan(converted_node) => {
                    assert_eq!(converted_node, node);
                }
                _ => panic!("Expected PlanNode::SeqScan"),
            }
        }
    }

    mod display_formatting {
        use super::*;

        #[test]
        fn test_basic_display() {
            let (node, _, _, table_name) = create_test_node();
            let basic_str = format!("{}", node);

            assert!(basic_str.contains(&table_name));
            assert!(!basic_str.contains("Schema"));
            assert!(!basic_str.contains("Table ID"));
        }

        #[test]
        fn test_alternate_display() {
            let (node, _, table_oid, table_name) = create_test_node();
            let detailed_str = format!("{:#}", node);

            println!("Detailed string: {}", detailed_str);
            assert!(detailed_str.contains(&table_name));
            assert!(detailed_str.contains("Schema"));
            assert!(detailed_str.contains(&format!("Table ID: {}", table_oid)));
        }

        #[test]
        fn test_display_with_children() {
            let schema = create_test_schema();
            let mut parent_node = SeqScanPlanNode::new(schema.clone(), 1, "parent".to_string());

            // Add a child node
            let child_node =
                PlanNode::SeqScan(SeqScanPlanNode::new(schema, 2, "child".to_string()));
            parent_node.children = vec![child_node];

            let detailed_str = format!("{:#}", parent_node);
            println!("Node with children: {}", detailed_str);
            assert!(detailed_str.contains("Child 1:"));
            assert!(detailed_str.contains("child"));
        }
    }

    mod schema_handling {
        use super::*;

        #[test]
        fn test_schema_inference() {
            let schema = create_test_schema();
            let table_ref =
                BoundBaseTableRef::new("test_table".to_string(), 1, None, schema.clone());

            let inferred_schema = SeqScanPlanNode::infer_scan_schema(&table_ref).unwrap();
            assert_eq!(inferred_schema, schema);
        }

        #[test]
        fn test_schema_columns() {
            let (node, _, _, _) = create_test_node();
            let schema = node.get_output_schema();

            assert_eq!(schema.get_column_count(), 3);
            assert_eq!(schema.get_column(0).unwrap().get_name(), "id");
            assert_eq!(schema.get_column(1).unwrap().get_name(), "name");
            assert_eq!(schema.get_column(2).unwrap().get_name(), "age");
        }
    }
}
