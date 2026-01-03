use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use crate::catalog::schema::Schema;
use crate::common::config::TableOidT;
use crate::sql::execution::expressions::abstract_expression::Expression;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateNode {
    output_schema: Schema,
    table_name: String,
    table_id: TableOidT,
    target_expressions: Vec<Arc<Expression>>,
    children: Vec<PlanNode>,
}

impl UpdateNode {
    pub fn new(
        output_schema: Schema,
        table_name: String,
        table_id: TableOidT,
        target_expressions: Vec<Arc<Expression>>,
        children: Vec<PlanNode>,
    ) -> Self {
        Self {
            output_schema,
            table_name,
            table_id,
            target_expressions,
            children,
        }
    }

    pub fn get_table_name(&self) -> &str {
        &self.table_name
    }

    pub fn get_table_id(&self) -> TableOidT {
        self.table_id
    }

    pub fn get_target_expressions(&self) -> &Vec<Arc<Expression>> {
        &self.target_expressions
    }
}

impl AbstractPlanNode for UpdateNode {
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

    fn get_type(&self) -> PlanType {
        PlanType::Update
    }
}

impl Display for UpdateNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ Update: {}", self.table_name)?;

        if f.alternate() {
            write!(f, "\n   Target Expressions: [")?;
            for (i, expr) in self.target_expressions.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", expr)?;
            }
            write!(f, "]")?;
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
    use crate::catalog::column::Column;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::sql::execution::plans::seq_scan_plan::SeqScanPlanNode;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::Integer),
        ])
    }

    fn create_test_expressions() -> Vec<Arc<Expression>> {
        vec![
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(42),
                Column::new("const1", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(100),
                Column::new("const2", TypeId::Integer),
                vec![],
            ))),
        ]
    }

    #[test]
    fn test_update_node_creation() {
        let schema = create_test_schema();
        let expressions = create_test_expressions();
        let child = PlanNode::SeqScan(SeqScanPlanNode::new(
            schema.clone(),
            1,
            "test_table".to_string(),
        ));

        let update_node = UpdateNode::new(
            schema.clone(),
            "test_table".to_string(),
            1,
            expressions.clone(),
            vec![child],
        );

        assert_eq!(update_node.get_table_name(), "test_table");
        assert_eq!(update_node.get_table_id(), 1);
        assert_eq!(update_node.get_target_expressions(), &expressions);
        assert_eq!(update_node.get_output_schema(), &schema);
        assert_eq!(update_node.get_children().len(), 1);
        assert_eq!(update_node.get_type(), PlanType::Update);
    }

    #[test]
    fn test_update_node_display() {
        let schema = create_test_schema();
        let expressions = create_test_expressions();
        let child = PlanNode::SeqScan(SeqScanPlanNode::new(
            schema.clone(),
            1,
            "test_table".to_string(),
        ));

        let update_node = UpdateNode::new(
            schema,
            "test_table".to_string(),
            1,
            expressions,
            vec![child],
        );

        // Test default display format
        let default_display = format!("{}", update_node);
        assert_eq!(default_display, "→ Update: test_table");

        // Test alternate display format
        let alternate_display = format!("{:#}", update_node);
        assert!(alternate_display.contains("→ Update: test_table"));
        assert!(alternate_display.contains("Target Expressions:"));
        assert!(alternate_display.contains("Schema:"));
        assert!(alternate_display.contains("Child 1:"));
    }

    #[test]
    fn test_update_node_clone() {
        let schema = create_test_schema();
        let expressions = create_test_expressions();
        let child = PlanNode::SeqScan(SeqScanPlanNode::new(
            schema.clone(),
            1,
            "test_table".to_string(),
        ));

        let original = UpdateNode::new(
            schema,
            "test_table".to_string(),
            1,
            expressions,
            vec![child],
        );

        let cloned = original.clone();

        assert_eq!(cloned.get_table_name(), original.get_table_name());
        assert_eq!(cloned.get_table_id(), original.get_table_id());
        assert_eq!(
            cloned.get_target_expressions(),
            original.get_target_expressions()
        );
        assert_eq!(cloned.get_output_schema(), original.get_output_schema());
        assert_eq!(cloned.get_children(), original.get_children());
    }
}
