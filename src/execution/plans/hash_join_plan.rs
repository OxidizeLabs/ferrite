use crate::catalog::schema::Schema;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use sqlparser::ast::JoinOperator;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct HashJoinNode {
    left_schema: Schema,
    right_schema: Schema,
    output_schema: Schema,
    predicate: Arc<Expression>,
    join_type: JoinOperator,
    left_key_expressions: Vec<Arc<Expression>>,
    right_key_expressions: Vec<Arc<Expression>>,
    children: Vec<PlanNode>,
}

impl HashJoinNode {
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

impl AbstractPlanNode for HashJoinNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::HashJoin
    }
}

impl Display for HashJoinNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
