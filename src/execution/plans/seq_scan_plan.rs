use std::fmt;
use std::fmt::{Display, Formatter};
use crate::binder::table_ref::bound_base_table_ref::BoundBaseTableRef;
use crate::catalog::schema::Schema;
use crate::common::config::TableOidT;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use crate::execution::plans::nested_loop_join_plan::NestedLoopJoinNode;

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
mod unit_tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::types_db::type_id::TypeId;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ])
    }

    #[test]
    fn seq_scan_plan_node() {
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let table_oid = 1;
        let table_name = "test_table".to_string();

        let seq_scan_node = SeqScanPlanNode::new(schema, table_oid, table_name.clone());

        assert_eq!(seq_scan_node.get_type(), PlanType::SeqScan);
        assert_eq!(seq_scan_node.get_table_oid(), table_oid);
        assert_eq!(seq_scan_node.get_table_name(), table_name);

        let plan_node_str = seq_scan_node.to_string();
        assert!(plan_node_str.contains(&table_name));

        let full_str = seq_scan_node.to_string();
        assert!(full_str.contains(&table_name));
        assert!(full_str.contains("Schema"));
    }

    #[test]
    fn seq_scan_plan_node_no_filter() {
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let table_oid = 1;
        let table_name = "test_table".to_string();

        let seq_scan_node = SeqScanPlanNode::new(schema, table_oid, table_name.clone());

        assert_eq!(seq_scan_node.get_type(), PlanType::SeqScan);
        assert_eq!(seq_scan_node.get_table_oid(), table_oid);
        assert_eq!(seq_scan_node.get_table_name(), table_name);

        let plan_node_str = seq_scan_node.to_string();
        assert!(plan_node_str.contains(&table_name));
        assert!(!plan_node_str.contains("filter"));
    }

    #[test]
    fn seq_scan_node_creation() {
        let schema = create_test_schema();
        let table_oid = 42;
        let table_name = "employees".to_string();

        let seq_scan_node = SeqScanPlanNode::new(schema.clone(), table_oid, table_name.clone());

        assert_eq!(seq_scan_node.get_output_schema(), &schema);
        assert_eq!(seq_scan_node.get_table_oid(), table_oid);
        assert_eq!(seq_scan_node.get_table_name(), &table_name);
        assert!(seq_scan_node.get_children().is_empty());
    }

    #[test]
    fn seq_scan_node_to_plan_node_conversion() {
        let schema = create_test_schema();
        let table_oid = 200;
        let table_name = "products".to_string();

        let seq_scan_node = SeqScanPlanNode::new(schema, table_oid, table_name);

        let plan_node: PlanNode = seq_scan_node.clone().into();

        match plan_node {
            PlanNode::SeqScan(node) => {
                assert_eq!(node, seq_scan_node);
            }
            _ => panic!("Expected PlanNode::SeqScan"),
        }
    }

    #[test]
    fn seq_scan_node_string_representation() {
        let schema = create_test_schema();
        let table_oid = 300;
        let table_name = "orders".to_string();

        let seq_scan_node = SeqScanPlanNode::new(schema, table_oid, table_name.clone());

        let with_schema = seq_scan_node.to_string();
        let without_schema = seq_scan_node.to_string();

        assert!(with_schema.contains("Schema"));
        assert!(with_schema.contains(&table_name));

        assert!(!without_schema.contains("Schema"));
        assert!(without_schema.contains(&table_name));
    }
}

#[cfg(test)]
mod seq_scan_string_tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::types_db::type_id::TypeId;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ])
    }

    fn create_complex_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
            Column::new("salary", TypeId::Decimal),
            Column::new("is_active", TypeId::Boolean),
        ])
    }

    #[test]
    fn seq_scan_simple_no_filter() {
        let schema = create_test_schema();
        let table_oid = 1;
        let table_name = "employees".to_string();

        let seq_scan_node = SeqScanPlanNode::new(schema, table_oid, table_name.clone());

        let with_schema = seq_scan_node.to_string();
        let without_schema = seq_scan_node.to_string();

        assert_eq!(
            without_schema,
            format!("SeqScan {{ table: {} }}", table_name)
        );
        assert_eq!(
            with_schema,
            format!(
                "SeqScan {{ table: {} }}\nSchema: Schema (id, name, age)",
                table_name
            )
        );
    }

    #[test]
    fn seq_scan_complex_schema_no_filter() {
        let schema = create_complex_schema();
        let table_oid = 2;
        let table_name = "complex_table".to_string();

        let seq_scan_node = SeqScanPlanNode::new(schema, table_oid, table_name.clone());

        let with_schema = seq_scan_node.to_string();
        let without_schema = seq_scan_node.to_string();

        assert_eq!(
            without_schema,
            format!("SeqScan {{ table: {} }}", table_name)
        );
        assert_eq!(
            with_schema,
            format!(
                "SeqScan {{ table: {} }}\nSchema: Schema (id, name, age, salary, is_active)",
                table_name
            )
        );
    }
}
