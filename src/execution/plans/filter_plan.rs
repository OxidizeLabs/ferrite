use crate::catalogue::schema::Schema;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct FilterNode {
    output_schema: Schema,
    predicate: Arc<Expression>,
    children: Vec<PlanNode>,
}

impl FilterNode {
    pub fn new(output_schema: Schema, predicate: Arc<Expression>, child: PlanNode) -> Self {
        Self {
            output_schema,
            predicate,
            children: vec![child],
        }
    }

    pub fn get_predicate(&self) -> &Arc<Expression> {
        &self.predicate
    }

    pub fn get_child_plan(&self) -> &PlanNode {
        &self.children[0]
    }
}

impl AbstractPlanNode for FilterNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::Filter
    }

    fn to_string(&self, with_schema: bool) -> String {
        let mut result = self.plan_node_to_string();
        if with_schema {
            result.push_str(&format!("\nSchema: {}", self.output_schema));
        }
        result.push_str(&self.children_to_string(2));
        result
    }

    fn plan_node_to_string(&self) -> String {
        format!("Filter {{ predicate={:?} }}", self.predicate)
    }

    fn children_to_string(&self, indent: usize) -> String {
        self.children
            .iter()
            .enumerate()
            .map(|(i, child)| {
                format!(
                    "\n{:indent$}Child {}: {}",
                    "",
                    i + 1,
                    AbstractPlanNode::to_string(child, false),
                    indent = indent
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalogue::column::Column;
    use crate::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::execution::expressions::comparison_expression::{
        ComparisonExpression, ComparisonType,
    };
    use crate::execution::plans::seq_scan_plan::SeqScanPlanNode;
    use crate::types_db::type_id::TypeId;

    #[test]
    fn filter_plan_node() {
        let schema = Schema::new(vec![
            Column::new("col1", TypeId::Integer),
            Column::new("col2", TypeId::Integer),
        ]);

        let col1 = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            schema.get_column(0).unwrap().clone(),
            vec![],
        )));
        let col2 = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            schema.get_column(1).unwrap().clone(),
            vec![],
        )));

        let less_than_expr = Expression::Comparison(ComparisonExpression::new(
            col1.clone(),
            col2.clone(),
            ComparisonType::LessThan,
            vec![],
        ));

        let predicate = Arc::new(less_than_expr);
        let child = PlanNode::SeqScan(SeqScanPlanNode::new(
            schema.clone(),
            0,
            "test_table".to_string(),
            None,
        ));

        let filter_node = FilterNode::new(schema.clone(), predicate.clone(), child);

        assert_eq!(filter_node.get_type(), PlanType::Filter);
        assert_eq!(filter_node.get_predicate(), &predicate);
        assert!(matches!(filter_node.get_child_plan(), PlanNode::SeqScan(_)));
        assert_eq!(filter_node.get_children().len(), 1);

        let expected_string = filter_node.to_string(true);

        assert!(expected_string.contains("Filter { predicate="));
        assert!(expected_string.contains("Schema: Schema (col1, col2)"));
        assert!(expected_string.contains("Child 1: SeqScan { table: test_table }"));

        // Check that the schema is not repeated for the child node
        assert!(!expected_string
            .contains("Child 1: SeqScan { table: test_table }\nSchema: Schema (col1, col2)"));
    }
}
