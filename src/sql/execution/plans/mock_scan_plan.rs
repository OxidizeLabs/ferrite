use std::fmt::{Display, Formatter};

use crate::catalog::schema::Schema;
use crate::common::rid::RID;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use crate::types_db::value::Value;

#[derive(Default, Debug, Clone, PartialEq)]
pub struct MockScanNode {
    output_schema: Schema,
    table: String,
    children: Vec<PlanNode>,
    tuples: Vec<(Vec<Value>, RID)>,
}

impl MockScanNode {
    pub fn new(schema: Schema, table_name: String, children: Vec<PlanNode>) -> Self {
        Self {
            output_schema: schema,
            table: table_name,
            children,
            tuples: vec![],
        }
    }

    pub fn with_tuples(mut self, tuples: Vec<(Vec<Value>, RID)>) -> Self {
        self.tuples = tuples;
        self
    }

    pub fn get_table_name(&self) -> String {
        self.table.clone()
    }

    pub fn get_tuples(&self) -> &Vec<(Vec<Value>, RID)> {
        &self.tuples
    }
}

impl AbstractPlanNode for MockScanNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::MockScan
    }
}

impl Display for MockScanNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            writeln!(f, "MockScan [")?;
            writeln!(f, "  Table: {}", self.get_table_name())?;
            write!(f, "  Schema: {}", self.output_schema)?;
            if !self.children.is_empty() {
                for child in &self.children {
                    write!(f, "\n  {}", child)?;
                }
            }
            write!(f, "\n]")
        } else {
            write!(f, "MockScan({})", self.get_table_name())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::types_db::type_id::TypeId;

    #[test]
    fn test_mock_scan_display() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let table_name = "test_table".to_string();
        let node = MockScanNode::new(schema, table_name, vec![]);

        // Test basic format
        assert_eq!(node.to_string(), "MockScan(test_table)");

        // Test detailed format
        let detailed = format!("{:#}", node);
        println!("Detailed output: {}", detailed);
        assert!(detailed.starts_with("MockScan ["));
        assert!(detailed.contains("Table: test_table"));
        assert!(detailed.contains("id") && detailed.contains("Integer"));
        assert!(detailed.contains("name") && detailed.contains("VarChar"));
    }

    #[test]
    fn test_mock_scan_with_children() {
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let child_schema = Schema::new(vec![Column::new("value", TypeId::Integer)]);

        let child = MockScanNode::new(child_schema, "child_table".to_string(), vec![]);

        let parent = MockScanNode::new(
            schema,
            "parent_table".to_string(),
            vec![PlanNode::MockScan(child)],
        );

        // Test basic format
        assert_eq!(parent.to_string(), "MockScan(parent_table)");

        // Test detailed format
        let detailed = format!("{:#}", parent);
        println!("Detailed output with children: {}", detailed);
        assert!(detailed.starts_with("MockScan ["));
        assert!(detailed.contains("Table: parent_table"));
        assert!(detailed.contains("MockScan(child_table)"));
    }

    #[test]
    fn test_mock_scan_with_tuples() {
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let table_name = "test_table".to_string();
        let tuples = vec![
            (vec![Value::new(1)], RID::new(1, 1)),
            (vec![Value::new(2)], RID::new(1, 2)),
        ];

        let node = MockScanNode::new(schema, table_name, vec![]).with_tuples(tuples.clone());

        assert_eq!(node.get_tuples(), &tuples);
    }
}
