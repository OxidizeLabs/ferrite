use std::fmt;
use std::fmt::{Display, Formatter};

use crate::catalog::schema::Schema;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};

#[derive(Debug, Clone, PartialEq)]
pub struct LimitNode {
    output_schema: Schema,
    limit: usize,
    children: Vec<PlanNode>,
}

impl LimitNode {
    pub fn new(limit: usize, output_schema: Schema, children: Vec<PlanNode>) -> Self {
        Self {
            output_schema,
            limit,
            children,
        }
    }

    pub fn get_limit(&self) -> usize {
        self.limit
    }
}

impl AbstractPlanNode for LimitNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::Limit
    }
}

impl Display for LimitNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ Limit: {}", self.limit)?;

        if f.alternate() {
            write!(f, "\n   Schema: {}", self.output_schema)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::sql::execution::expressions::abstract_expression::Expression;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::sql::execution::plans::values_plan::ValuesNode;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ])
    }

    fn create_test_values_node() -> PlanNode {
        let schema = create_test_schema();
        let row = vec![
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(1),
                Column::new("id", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new("test".to_string()),
                Column::new("name", TypeId::VarChar),
                vec![],
            ))),
        ];
        PlanNode::Values(ValuesNode::new(schema, vec![row], vec![]))
    }

    #[test]
    fn test_limit_node_creation() {
        let values_node = create_test_values_node();
        let schema = create_test_schema();
        let limit_node = LimitNode::new(5, schema, vec![values_node]);

        assert_eq!(limit_node.get_limit(), 5);
        assert_eq!(limit_node.get_type(), PlanType::Limit);
        assert_eq!(limit_node.get_children().len(), 1);
    }

    #[test]
    fn test_limit_node_zero_limit() {
        let values_node = create_test_values_node();
        let schema = create_test_schema();
        let limit_node = LimitNode::new(0, schema, vec![values_node]);

        assert_eq!(limit_node.get_limit(), 0);
    }

    #[test]
    fn test_limit_node_display() {
        let values_node = create_test_values_node();
        let schema = create_test_schema();
        let limit_node = LimitNode::new(10, schema, vec![values_node]);

        assert!(format!("{}", limit_node).contains("Limit"));
        assert!(format!("{:#}", limit_node).contains("Limit"));
    }

    #[test]
    fn test_limit_node_as_plan_node() {
        let values_node = create_test_values_node();
        let schema = create_test_schema();
        let limit_node = LimitNode::new(5, schema, vec![values_node]);
        let plan_node = PlanNode::Limit(limit_node);

        assert!(matches!(plan_node, PlanNode::Limit(_)));
    }
}
