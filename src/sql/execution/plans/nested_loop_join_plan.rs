use crate::catalog::schema::Schema;
use crate::sql::execution::expressions::abstract_expression::Expression;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use sqlparser::ast::JoinOperator;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct NestedLoopJoinNode {
    left_schema: Schema,
    right_schema: Schema,
    output_schema: Schema,
    predicate: Arc<Expression>,
    join_type: JoinOperator,
    left_key_expressions: Vec<Arc<Expression>>,
    right_key_expressions: Vec<Arc<Expression>>,
    children: Vec<PlanNode>,
}

impl NestedLoopJoinNode {
    pub fn new(
        left_schema: Schema,
        right_schema: Schema,
        predicate: Arc<Expression>,
        join_type: JoinOperator,
        left_key_expressions: Vec<Arc<Expression>>,
        right_key_expressions: Vec<Arc<Expression>>,
        children: Vec<PlanNode>,
    ) -> Self {
        // Create output schema by combining columns from both input schemas
        let mut output_columns = Vec::new();

        // Add columns from left schema
        output_columns.extend(left_schema.get_columns().iter().cloned());

        // Add columns from right schema
        output_columns.extend(right_schema.get_columns().iter().cloned());

        let output_schema = Schema::new(output_columns);

        Self {
            left_schema,
            right_schema,
            output_schema,
            predicate,
            join_type,
            left_key_expressions,
            right_key_expressions,
            children,
        }
    }

    pub fn get_left_schema(&self) -> &Schema {
        &self.left_schema
    }

    pub fn get_right_schema(&self) -> &Schema {
        &self.right_schema
    }

    pub fn get_predicate(&self) -> &Arc<Expression> {
        &self.predicate
    }

    pub fn get_join_type(&self) -> &JoinOperator {
        &self.join_type
    }

    pub fn get_left_key_expressions(&self) -> &Vec<Arc<Expression>> {
        &self.left_key_expressions
    }

    pub fn get_right_key_expressions(&self) -> &Vec<Arc<Expression>> {
        &self.right_key_expressions
    }

    pub fn get_left_child(&self) -> &PlanNode {
        &self.children[0]
    }

    pub fn get_right_child(&self) -> &PlanNode {
        &self.children[1]
    }
}

impl AbstractPlanNode for NestedLoopJoinNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::NestedLoopJoin
    }
}

impl Display for NestedLoopJoinNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "→ NestedLoopJoin")?;

        if f.alternate() {
            write!(f, "\n   Join Type: {:?}", self.join_type)?;
            write!(f, "\n   Predicate: {}", self.predicate)?;
            write!(f, "\n   Left Schema: {}", self.left_schema)?;
            write!(f, "\n   Right Schema: {}", self.right_schema)?;

            if !self.left_key_expressions.is_empty() {
                write!(f, "\n   Join Keys:")?;
                write!(f, "\n      Left:  [")?;
                for (i, expr) in self.left_key_expressions.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", expr)?;
                }
                write!(f, "]")?;
            }

            if !self.right_key_expressions.is_empty() {
                if self.left_key_expressions.is_empty() {
                    write!(f, "\n   Join Keys:")?;
                }
                write!(f, "\n      Right: [")?;
                for (i, expr) in self.right_key_expressions.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", expr)?;
                }
                write!(f, "]")?;
            }

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
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::plans::mock_scan_plan::MockScanNode;
    use crate::types_db::type_id::TypeId;

    fn create_test_schema(prefix: &str) -> Schema {
        let columns = vec![
            Column::new(&format!("{}_id", prefix), TypeId::Integer),
            Column::new(&format!("{}_name", prefix), TypeId::VarChar),
        ];
        Schema::new(columns)
    }

    #[test]
    fn test_nested_loop_join_creation() {
        let left_schema = create_test_schema("dept");
        let right_schema = create_test_schema("emp");

        // Create join predicate (dept_id = emp_id)
        let left_col = Column::new("dept_id", TypeId::Integer);
        let right_col = Column::new("emp_id", TypeId::Integer);

        let left_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            left_col,
            vec![],
        )));
        let right_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1,
            0,
            right_col,
            vec![],
        )));

        let children = vec![
            PlanNode::MockScan(MockScanNode::new(
                left_schema.clone(),
                "departments".to_string(),
                vec![],
            )),
            PlanNode::MockScan(MockScanNode::new(
                right_schema.clone(),
                "employees".to_string(),
                vec![],
            )),
        ];

        let join_node = NestedLoopJoinNode::new(
            left_schema.clone(),
            right_schema.clone(),
            left_expr.clone(),
            JoinOperator::Inner(JoinConstraint::None),
            vec![left_expr],
            vec![right_expr],
            children,
        );

        // Verify output schema contains all columns
        let output_schema = join_node.get_output_schema();
        assert_eq!(output_schema.get_columns().len(), 4);
        assert_eq!(output_schema.get_columns()[0].get_name(), "dept_id");
        assert_eq!(output_schema.get_columns()[1].get_name(), "dept_name");
        assert_eq!(output_schema.get_columns()[2].get_name(), "emp_id");
        assert_eq!(output_schema.get_columns()[3].get_name(), "emp_name");
    }

    #[test]
    fn test_nested_loop_join_getters() {
        let left_schema = create_test_schema("dept");
        let right_schema = create_test_schema("emp");

        let predicate = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("test", TypeId::Integer),
            vec![],
        )));

        let left_key = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("left_key", TypeId::Integer),
            vec![],
        )));

        let right_key = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            1,
            0,
            Column::new("right_key", TypeId::Integer),
            vec![],
        )));

        let children = vec![
            PlanNode::MockScan(MockScanNode::new(
                left_schema.clone(),
                "left".to_string(),
                vec![],
            )),
            PlanNode::MockScan(MockScanNode::new(
                right_schema.clone(),
                "right".to_string(),
                vec![],
            )),
        ];

        let join_node = NestedLoopJoinNode::new(
            left_schema.clone(),
            right_schema.clone(),
            predicate.clone(),
            JoinOperator::Inner(JoinConstraint::None),
            vec![left_key.clone()],
            vec![right_key.clone()],
            children.clone(),
        );

        assert_eq!(join_node.get_left_schema(), &left_schema);
        assert_eq!(join_node.get_right_schema(), &right_schema);
        assert_eq!(join_node.get_predicate(), &predicate);
        assert_eq!(
            join_node.get_join_type(),
            &JoinOperator::Inner(JoinConstraint::None)
        );
        assert_eq!(join_node.get_left_key_expressions(), &vec![left_key]);
        assert_eq!(join_node.get_right_key_expressions(), &vec![right_key]);
        assert_eq!(join_node.get_left_child(), &children[0]);
        assert_eq!(join_node.get_right_child(), &children[1]);
    }

    #[test]
    fn test_nested_loop_join_display() {
        let left_schema = create_test_schema("dept");
        let right_schema = create_test_schema("emp");

        let predicate = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("test", TypeId::Integer),
            vec![],
        )));

        let children = vec![
            PlanNode::MockScan(MockScanNode::new(
                left_schema.clone(),
                "departments".to_string(),
                vec![],
            )),
            PlanNode::MockScan(MockScanNode::new(
                right_schema.clone(),
                "employees".to_string(),
                vec![],
            )),
        ];

        let join_node = NestedLoopJoinNode::new(
            left_schema,
            right_schema,
            predicate,
            JoinOperator::Inner(JoinConstraint::None),
            vec![],
            vec![],
            children,
        );

        // Test basic display
        assert_eq!(join_node.to_string(), "→ NestedLoopJoin");

        // Test detailed display
        let detailed = format!("{:#}", join_node);
        assert!(detailed.contains("Join Type: Inner"));
        assert!(detailed.contains("Left Schema:"));
        assert!(detailed.contains("Right Schema:"));
    }

    #[test]
    fn test_plan_type() {
        let left_schema = create_test_schema("left");
        let right_schema = create_test_schema("right");

        let predicate = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("test", TypeId::Integer),
            vec![],
        )));

        let children = vec![
            PlanNode::MockScan(MockScanNode::new(
                left_schema.clone(),
                "left".to_string(),
                vec![],
            )),
            PlanNode::MockScan(MockScanNode::new(
                right_schema.clone(),
                "right".to_string(),
                vec![],
            )),
        ];

        let join_node = NestedLoopJoinNode::new(
            left_schema,
            right_schema,
            predicate,
            JoinOperator::Inner(JoinConstraint::None),
            vec![],
            vec![],
            children,
        );

        assert_eq!(join_node.get_type(), PlanType::NestedLoopJoin);
    }
}
