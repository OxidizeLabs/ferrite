use crate::catalog::schema::Schema;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// Represents a plan node that returns the top N rows from each group
/// For example: SELECT * FROM employees GROUP BY department ORDER BY salary DESC LIMIT 5
/// would return the top 5 highest paid employees from each department
#[derive(Debug, Clone, PartialEq)]
pub struct TopNPerGroupNode {
    output_schema: Schema,
    n: usize,  // number of rows to return per group
    sort_expressions: Vec<Arc<Expression>>,  // expressions to sort by
    group_expressions: Vec<Arc<Expression>>, // expressions to group by
    children: Vec<PlanNode>,
}

impl TopNPerGroupNode {
    pub fn new(
        n: usize,
        sort_expressions: Vec<Arc<Expression>>,
        group_expressions: Vec<Arc<Expression>>,
        output_schema: Schema,
        children: Vec<PlanNode>,
    ) -> Self {
        Self {
            output_schema,
            n,
            sort_expressions,
            group_expressions,
            children,
        }
    }

    /// Get the number of rows to return per group
    pub fn get_n(&self) -> usize {
        self.n
    }

    /// Get the expressions used for sorting within each group
    pub fn get_sort_expressions(&self) -> &Vec<Arc<Expression>> {
        &self.sort_expressions
    }

    /// Get the expressions used for grouping
    pub fn get_group_expressions(&self) -> &Vec<Arc<Expression>> {
        &self.group_expressions
    }

    /// Get the child plan node
    pub fn get_child(&self) -> Option<&PlanNode> {
        self.children.first()
    }
}

impl AbstractPlanNode for TopNPerGroupNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::TopNPerGroup
    }
}

impl Display for TopNPerGroupNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ TopNPerGroup: {}", self.n)?;

        if f.alternate() {
            // Display sort expressions
            write!(f, "\n   Sort By: [")?;
            for (i, expr) in self.sort_expressions.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", expr)?;
            }
            write!(f, "]")?;

            // Display group expressions
            write!(f, "\n   Group By: [")?;
            for (i, expr) in self.group_expressions.iter().enumerate() {
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
    use crate::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::types_db::type_id::TypeId;

    #[test]
    fn test_topn_per_group_creation() {
        let schema = Schema::new(vec![
            Column::new("department", TypeId::VarChar),
            Column::new("salary", TypeId::Integer),
        ]);

        // Create group by expression (department)
        let group_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, // tuple index
            0, // column index
            Column::new("department", TypeId::VarChar),
            vec![],
        )));

        // Create sort expression (salary)
        let sort_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, // tuple index
            1, // column index
            Column::new("salary", TypeId::Integer),
            vec![],
        )));

        let node = TopNPerGroupNode::new(
            5, // top 5 per group
            vec![sort_expr],
            vec![group_expr],
            schema.clone(),
            vec![],
        );

        assert_eq!(node.get_n(), 5);
        assert_eq!(node.get_output_schema(), &schema);
        assert_eq!(node.get_sort_expressions().len(), 1);
        assert_eq!(node.get_group_expressions().len(), 1);
    }
}
