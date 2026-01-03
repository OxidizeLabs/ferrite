use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use crate::catalog::schema::Schema;
use crate::sql::execution::expressions::abstract_expression::Expression;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};

/// Represents the sort order direction for ORDER BY clauses
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OrderDirection {
    #[default]
    Asc,
    Desc,
}

impl OrderDirection {
    /// Returns true if this is ascending order
    pub fn is_ascending(&self) -> bool {
        matches!(self, OrderDirection::Asc)
    }

    /// Returns true if this is descending order
    pub fn is_descending(&self) -> bool {
        matches!(self, OrderDirection::Desc)
    }
}

impl Display for OrderDirection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            OrderDirection::Asc => write!(f, "ASC"),
            OrderDirection::Desc => write!(f, "DESC"),
        }
    }
}

/// Represents a single order by specification with expression and direction
#[derive(Debug, Clone, PartialEq)]
pub struct OrderBySpec {
    expression: Arc<Expression>,
    direction: OrderDirection,
}

impl OrderBySpec {
    pub fn new(expression: Arc<Expression>, direction: OrderDirection) -> Self {
        Self {
            expression,
            direction,
        }
    }

    pub fn get_expression(&self) -> &Arc<Expression> {
        &self.expression
    }

    pub fn get_direction(&self) -> OrderDirection {
        self.direction
    }
}

impl Display for OrderBySpec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.expression, self.direction)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortNode {
    output_schema: Schema,
    order_bys: Vec<OrderBySpec>,
    children: Vec<PlanNode>,
}

impl SortNode {
    pub fn new(
        output_schema: Schema,
        order_bys: Vec<OrderBySpec>,
        children: Vec<PlanNode>,
    ) -> Self {
        Self {
            output_schema,
            order_bys,
            children,
        }
    }

    /// Create a SortNode with expressions and default ASC order
    pub fn new_with_expressions(
        output_schema: Schema,
        order_bys: Vec<Arc<Expression>>,
        children: Vec<PlanNode>,
    ) -> Self {
        let order_specs = order_bys
            .into_iter()
            .map(|expr| OrderBySpec::new(expr, OrderDirection::Asc))
            .collect();

        Self {
            output_schema,
            order_bys: order_specs,
            children,
        }
    }

    pub fn get_order_bys(&self) -> &Vec<OrderBySpec> {
        &self.order_bys
    }

    /// Get just the expressions for backward compatibility
    pub fn get_order_by_expressions(&self) -> Vec<Arc<Expression>> {
        self.order_bys
            .iter()
            .map(|spec| spec.get_expression().clone())
            .collect()
    }
}

impl AbstractPlanNode for SortNode {
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

impl Display for SortNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ Sort")?;

        if f.alternate() {
            write!(f, "\n   Order By: [")?;
            for (i, spec) in self.order_bys.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", spec)?;
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
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::types_db::type_id::TypeId;

    #[test]
    fn test_sort_node_creation() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let order_by_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));
        let order_bys = vec![OrderBySpec::new(order_by_expr.clone(), OrderDirection::Asc)];
        let children = vec![];

        let sort_node = SortNode::new(schema.clone(), order_bys.clone(), children);

        assert_eq!(sort_node.get_output_schema(), &schema);
        assert_eq!(sort_node.get_order_bys(), &order_bys);
        assert_eq!(sort_node.get_children().len(), 0);
        assert_eq!(sort_node.get_type(), PlanType::Sort);
    }

    #[test]
    fn test_sort_node_with_expressions() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let order_by_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));
        let order_bys = vec![order_by_expr.clone()];
        let children = vec![];

        let sort_node = SortNode::new_with_expressions(schema.clone(), order_bys, children);

        assert_eq!(sort_node.get_output_schema(), &schema);
        assert_eq!(sort_node.get_order_bys().len(), 1);
        assert_eq!(
            sort_node.get_order_bys()[0].get_direction(),
            OrderDirection::Asc
        );
        assert_eq!(sort_node.get_children().len(), 0);
        assert_eq!(sort_node.get_type(), PlanType::Sort);
    }

    #[test]
    fn test_sort_node_display() {
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let order_by_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));
        let order_bys = vec![OrderBySpec::new(order_by_expr, OrderDirection::Desc)];
        let children = vec![];

        let sort_node = SortNode::new(schema, order_bys, children);

        // Test default format
        let default_format = format!("{}", sort_node);
        assert_eq!(default_format, "→ Sort");

        // Test alternate format
        let alternate_format = format!("{:#}", sort_node);
        assert!(alternate_format.contains("→ Sort"));
        assert!(alternate_format.contains("Order By:"));
        assert!(alternate_format.contains("DESC"));
        assert!(alternate_format.contains("Schema:"));
    }

    #[test]
    fn test_order_direction() {
        assert!(OrderDirection::Asc.is_ascending());
        assert!(!OrderDirection::Asc.is_descending());
        assert!(!OrderDirection::Desc.is_ascending());
        assert!(OrderDirection::Desc.is_descending());

        assert_eq!(OrderDirection::default(), OrderDirection::Asc);

        assert_eq!(format!("{}", OrderDirection::Asc), "ASC");
        assert_eq!(format!("{}", OrderDirection::Desc), "DESC");
    }

    #[test]
    fn test_order_by_spec() {
        let expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let spec = OrderBySpec::new(expr.clone(), OrderDirection::Desc);

        assert_eq!(spec.get_expression(), &expr);
        assert_eq!(spec.get_direction(), OrderDirection::Desc);

        let display_str = format!("{}", spec);
        assert!(display_str.contains("DESC"));
    }

    #[test]
    fn test_sort_node_with_children() {
        let child_schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let child_order_bys: Vec<OrderBySpec> = vec![];
        let child_children = vec![];
        let child_node = PlanNode::Sort(SortNode::new(
            child_schema.clone(),
            child_order_bys,
            child_children,
        ));

        let parent_schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let parent_order_by_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));
        let parent_order_bys = vec![OrderBySpec::new(parent_order_by_expr, OrderDirection::Asc)];
        let parent_children = vec![child_node];

        let parent_sort_node = SortNode::new(parent_schema, parent_order_bys, parent_children);

        assert_eq!(parent_sort_node.get_children().len(), 1);
    }
}
