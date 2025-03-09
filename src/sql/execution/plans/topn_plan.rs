use crate::catalog::schema::Schema;
use crate::sql::execution::expressions::abstract_expression::Expression;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct TopNNode {
    output_schema: Schema,
    order_bys: Vec<Arc<Expression>>,
    k: usize,
    children: Vec<PlanNode>,
}

impl TopNNode {
    pub fn new(
        output_schema: Schema,
        order_bys: Vec<Arc<Expression>>,
        k: usize,
        children: Vec<PlanNode>,
    ) -> Self {
        Self {
            output_schema,
            order_bys,
            k,
            children,
        }
    }
    pub fn get_k(&self) -> usize {
        self.k
    }
    pub fn get_sort_order_by(&self) -> &Vec<Arc<Expression>> {
        &self.order_bys
    }
}

impl AbstractPlanNode for TopNNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::Sort
    }
}

impl Display for TopNNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ TopN: {}", self.k)?;

        if f.alternate() {
            write!(f, "\n   Order By: [")?;
            for (i, expr) in self.order_bys.iter().enumerate() {
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
    use crate::sql::execution::expressions::mock_expression::MockExpression;
    use crate::sql::execution::plans::seq_scan_plan::SeqScanPlanNode;
    use crate::types_db::type_id::TypeId;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
            Column::new("salary", TypeId::Decimal),
        ])
    }

    fn create_mock_child_plan(schema: Schema) -> PlanNode {
        PlanNode::SeqScan(SeqScanPlanNode::new(schema, 0, "test_table".to_string()))
    }

    fn create_mock_order_by(name: &str, type_id: TypeId) -> Arc<Expression> {
        Arc::new(Expression::Mock(MockExpression::new(
            name.to_string(),
            type_id,
        )))
    }

    #[test]
    fn test_topn_node_creation() {
        let schema = create_test_schema();
        let order_bys = vec![
            create_mock_order_by("age", TypeId::Integer),
            create_mock_order_by("salary", TypeId::Decimal),
        ];
        let k = 5;
        let children = vec![create_mock_child_plan(schema.clone())];

        let topn = TopNNode::new(schema.clone(), order_bys.clone(), k, children.clone());

        // Test getters
        assert_eq!(topn.get_k(), k);
        assert_eq!(topn.get_sort_order_by(), &order_bys);
        assert_eq!(topn.get_output_schema(), &schema);
        assert_eq!(topn.get_children(), &children);
        assert_eq!(topn.get_type(), PlanType::Sort);
    }

    #[test]
    fn test_topn_node_display() {
        let schema = create_test_schema();
        let order_bys = vec![create_mock_order_by("age", TypeId::Integer)];
        let k = 10;
        let children = vec![create_mock_child_plan(schema.clone())];

        let topn = TopNNode::new(schema, order_bys, k, children);

        // Test basic display
        assert_eq!(format!("{}", topn), "→ TopN: 10");

        // Test alternate display with more details
        let alt_display = format!("{:#}", topn);
        assert!(alt_display.contains("→ TopN: 10"));
        assert!(alt_display.contains("Order By: [age]")); // MockExpression name is "test"
        assert!(alt_display.contains("Schema:"));
        assert!(alt_display.contains("Child 1:"));
    }

    #[test]
    fn test_topn_node_equality() {
        let schema = create_test_schema();
        let order_bys = vec![create_mock_order_by("age", TypeId::Integer)];
        let k = 5;
        let children = vec![create_mock_child_plan(schema.clone())];

        let topn1 = TopNNode::new(schema.clone(), order_bys.clone(), k, children.clone());
        let topn2 = TopNNode::new(schema, order_bys, k, children);

        assert_eq!(topn1, topn2);
    }
}
