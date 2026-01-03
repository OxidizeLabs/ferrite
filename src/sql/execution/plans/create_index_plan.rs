use std::fmt;
use std::fmt::{Display, Formatter};

use crate::catalog::schema::Schema;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};

#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexPlanNode {
    output_schema: Schema,
    table_name: String,
    index_name: String,
    key_attrs: Vec<usize>,
    if_not_exists: bool,
}

impl CreateIndexPlanNode {
    pub fn new(
        output_schema: Schema,
        table_name: String,
        index_name: String,
        key_attrs: Vec<usize>,
        if_not_exists: bool,
    ) -> Self {
        Self {
            output_schema,
            table_name,
            index_name,
            key_attrs,
            if_not_exists,
        }
    }

    pub fn get_index_name(&self) -> &str {
        &self.index_name
    }

    pub fn get_table_name(&self) -> &str {
        &self.table_name
    }

    pub fn get_key_attrs(&self) -> &[usize] {
        &self.key_attrs
    }

    pub fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }
}

impl AbstractPlanNode for CreateIndexPlanNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        static CHILDREN: Vec<PlanNode> = Vec::new();
        &CHILDREN
    }

    fn get_type(&self) -> PlanType {
        PlanType::CreateIndex
    }
}

impl Display for CreateIndexPlanNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "→ CreateIndex: {} on {}",
            self.index_name, self.table_name
        )?;

        if f.alternate() {
            write!(f, "\n   Key Columns: {:?}", self.key_attrs)?;
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
            Column::new("email", TypeId::VarChar),
        ];
        Schema::new(columns)
    }

    #[test]
    fn test_create_index_creation() {
        let schema = create_test_schema();
        let table_name = "employees";
        let index_name = "emp_id_idx";
        let key_attrs = vec![0]; // Index on id column
        let if_not_exists = false;

        let plan_node = CreateIndexPlanNode::new(
            schema.clone(),
            table_name.to_string(),
            index_name.to_string(),
            key_attrs.clone(),
            if_not_exists,
        );

        // Verify basic properties
        assert_eq!(plan_node.get_table_name(), table_name);
        assert_eq!(plan_node.get_index_name(), index_name);
        assert_eq!(plan_node.get_key_attrs(), &key_attrs);
        assert_eq!(plan_node.if_not_exists(), if_not_exists);

        // Verify output schema
        let output_schema = plan_node.get_output_schema();
        assert_eq!(output_schema.get_columns().len(), 4);
        assert_eq!(output_schema.get_columns()[0].get_name(), "id");
        assert_eq!(output_schema.get_columns()[1].get_name(), "name");
        assert_eq!(output_schema.get_columns()[2].get_name(), "age");
        assert_eq!(output_schema.get_columns()[3].get_name(), "email");
    }

    #[test]
    fn test_create_index_multiple_keys() {
        let schema = create_test_schema();
        let key_attrs = vec![0, 2]; // Composite index on id and age

        let plan_node = CreateIndexPlanNode::new(
            schema,
            "employees".to_string(),
            "emp_id_age_idx".to_string(),
            key_attrs.clone(),
            false,
        );

        // Verify key attributes
        assert_eq!(plan_node.get_key_attrs(), &key_attrs);
        assert_eq!(plan_node.get_key_attrs().len(), 2);
    }

    #[test]
    fn test_create_index_if_not_exists() {
        let schema = create_test_schema();

        // Test with if_not_exists = true
        let plan_node_with_flag = CreateIndexPlanNode::new(
            schema.clone(),
            "employees".to_string(),
            "emp_idx".to_string(),
            vec![0],
            true,
        );
        assert!(plan_node_with_flag.if_not_exists());

        // Test with if_not_exists = false
        let plan_node_without_flag = CreateIndexPlanNode::new(
            schema.clone(),
            "employees".to_string(),
            "emp_idx".to_string(),
            vec![0],
            false,
        );
        assert!(!plan_node_without_flag.if_not_exists());
    }

    #[test]
    fn test_create_index_display() {
        let schema = create_test_schema();
        let table_name = "employees";
        let index_name = "emp_id_idx";
        let key_attrs = vec![0, 2]; // Composite index on id and age

        // Test without IF NOT EXISTS
        let plan_node = CreateIndexPlanNode::new(
            schema.clone(),
            table_name.to_string(),
            index_name.to_string(),
            key_attrs,
            false,
        );

        // Test basic display
        assert_eq!(
            plan_node.to_string(),
            "→ CreateIndex: emp_id_idx on employees"
        );

        // Test detailed display without IF NOT EXISTS
        let detailed = format!("{:#}", plan_node);
        assert!(detailed.contains("Key Columns: [0, 2]"));
        assert!(detailed.contains("Schema:"));
        assert!(!detailed.contains("IF NOT EXISTS"));

        // Test with IF NOT EXISTS
        let plan_node_if_not_exists = CreateIndexPlanNode::new(
            schema.clone(),
            table_name.to_string(),
            index_name.to_string(),
            vec![0],
            true,
        );

        // Test detailed display with IF NOT EXISTS
        let detailed_if_not_exists = format!("{:#}", plan_node_if_not_exists);
        assert!(detailed_if_not_exists.contains("IF NOT EXISTS"));
        assert!(detailed_if_not_exists.contains("Key Columns: [0]"));
        assert!(detailed_if_not_exists.contains("Schema:"));
    }

    #[test]
    fn test_create_index_children() {
        let schema = create_test_schema();
        let plan_node = CreateIndexPlanNode::new(
            schema,
            "employees".to_string(),
            "emp_idx".to_string(),
            vec![0],
            false,
        );

        // CreateIndex nodes should have no children
        assert!(plan_node.get_children().is_empty());
    }

    #[test]
    fn test_plan_type() {
        let schema = create_test_schema();
        let plan_node = CreateIndexPlanNode::new(
            schema,
            "employees".to_string(),
            "emp_idx".to_string(),
            vec![0],
            false,
        );

        assert_eq!(plan_node.get_type(), PlanType::CreateIndex);
    }

    #[test]
    fn test_create_index_empty_keys() {
        let schema = create_test_schema();
        let plan_node = CreateIndexPlanNode::new(
            schema,
            "employees".to_string(),
            "emp_idx".to_string(),
            vec![], // Empty key attributes
            false,
        );

        // Verify empty key attributes handling
        assert!(plan_node.get_key_attrs().is_empty());

        // Verify display still works
        let detailed = format!("{:#}", plan_node);
        assert!(detailed.contains("Key Columns: []"));
    }
}
