use crate::catalog::schema::Schema;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTablePlanNode {
    output_schema: Schema,
    table_name: String,
    if_not_exists: bool,
}

impl CreateTablePlanNode {
    pub fn new(output_schema: Schema, table_name: String, if_not_exists: bool) -> Self {
        Self {
            output_schema,
            table_name,
            if_not_exists,
        }
    }

    pub fn get_table_name(&self) -> &str {
        &self.table_name
    }

    pub fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }
}

impl AbstractPlanNode for CreateTablePlanNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        static CHILDREN: Vec<PlanNode> = Vec::new();
        &CHILDREN
    }

    fn get_type(&self) -> PlanType {
        PlanType::CreateTable
    }
}

impl Display for CreateTablePlanNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ CreateTable: {}", self.table_name)?;

        if f.alternate() {
            if self.if_not_exists {
                write!(f, "\n   IF NOT EXISTS")?;
            }
            write!(f, "\n   Schema: {}", self.output_schema)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::types_db::type_id::TypeId;

    fn create_test_schema() -> Schema {
        let columns = vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
            Column::new("salary", TypeId::Decimal),
            Column::new("date", TypeId::Date),
        ];
        Schema::new(columns)
    }

    #[test]
    fn test_create_table_creation() {
        let schema = create_test_schema();
        let table_name = "employees";
        let if_not_exists = false;

        let plan_node =
            CreateTablePlanNode::new(schema.clone(), table_name.to_string(), if_not_exists);

        // Verify basic properties
        assert_eq!(plan_node.get_table_name(), table_name);
        assert_eq!(plan_node.if_not_exists(), if_not_exists);

        // Verify output schema
        let output_schema = plan_node.get_output_schema();
        assert_eq!(output_schema.get_columns().len(), 5);
        assert_eq!(output_schema.get_columns()[0].get_name(), "id");
        assert_eq!(output_schema.get_columns()[1].get_name(), "name");
        assert_eq!(output_schema.get_columns()[2].get_name(), "age");
        assert_eq!(output_schema.get_columns()[3].get_name(), "salary");
        assert_eq!(output_schema.get_columns()[4].get_name(), "date");

        // Verify column types
        assert_eq!(output_schema.get_columns()[0].get_type(), TypeId::Integer);
        assert_eq!(output_schema.get_columns()[1].get_type(), TypeId::VarChar);
        assert_eq!(output_schema.get_columns()[2].get_type(), TypeId::Integer);
        assert_eq!(output_schema.get_columns()[3].get_type(), TypeId::Decimal);
        assert_eq!(output_schema.get_columns()[4].get_type(), TypeId::Date);
    }

    #[test]
    fn test_create_table_if_not_exists() {
        let schema = create_test_schema();

        // Test with if_not_exists = true
        let plan_node_with_flag =
            CreateTablePlanNode::new(schema.clone(), "employees".to_string(), true);
        assert!(plan_node_with_flag.if_not_exists());

        // Test with if_not_exists = false
        let plan_node_without_flag =
            CreateTablePlanNode::new(schema.clone(), "employees".to_string(), false);
        assert!(!plan_node_without_flag.if_not_exists());
    }

    #[test]
    fn test_create_table_display() {
        let schema = create_test_schema();
        let table_name = "employees";

        // Test without IF NOT EXISTS
        let plan_node = CreateTablePlanNode::new(schema.clone(), table_name.to_string(), false);

        // Test basic display
        assert_eq!(plan_node.to_string(), "→ CreateTable: employees");

        // Test detailed display without IF NOT EXISTS
        let detailed = format!("{:#}", plan_node);
        assert!(detailed.contains("Schema:"));
        assert!(!detailed.contains("IF NOT EXISTS"));

        // Test with IF NOT EXISTS
        let plan_node_if_not_exists =
            CreateTablePlanNode::new(schema.clone(), table_name.to_string(), true);

        // Test detailed display with IF NOT EXISTS
        let detailed_if_not_exists = format!("{:#}", plan_node_if_not_exists);
        assert!(detailed_if_not_exists.contains("IF NOT EXISTS"));
        assert!(detailed_if_not_exists.contains("Schema:"));
    }

    #[test]
    fn test_create_table_children() {
        let schema = create_test_schema();
        let plan_node = CreateTablePlanNode::new(schema, "employees".to_string(), false);

        // CreateTable nodes should have no children
        assert!(plan_node.get_children().is_empty());
    }

    #[test]
    fn test_plan_type() {
        let schema = create_test_schema();
        let plan_node = CreateTablePlanNode::new(schema, "employees".to_string(), false);

        assert_eq!(plan_node.get_type(), PlanType::CreateTable);
    }

    #[test]
    fn test_create_table_empty_schema() {
        let empty_schema = Schema::new(vec![]);
        let plan_node = CreateTablePlanNode::new(empty_schema, "empty_table".to_string(), false);

        // Verify empty schema handling
        assert_eq!(plan_node.get_output_schema().get_columns().len(), 0);

        // Verify display still works
        let detailed = format!("{:#}", plan_node);
        assert!(detailed.contains("Schema:"));
    }
}
