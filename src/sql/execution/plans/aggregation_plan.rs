use crate::catalog::schema::Schema;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::aggregate_expression::AggregationType;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

impl Display for AggregationType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            AggregationType::Sum => write!(f, "SUM"),
            AggregationType::Count => write!(f, "COUNT"),
            AggregationType::CountStar => write!(f, "COUNT"),
            AggregationType::Min => write!(f, "MIN"),
            AggregationType::Max => write!(f, "MAX"),
            AggregationType::Avg => write!(f, "AVG"),
            AggregationType::StdDev => write!(f, "STDDEV"),
            AggregationType::Variance => write!(f, "VARIANCE"),
        }
    }
}

/// Plan node for aggregation operations
#[derive(Debug, Clone, PartialEq)]
pub struct AggregationPlanNode {
    children: Vec<PlanNode>,
    group_by_exprs: Vec<Arc<Expression>>,
    aggregate_exprs: Vec<Arc<Expression>>,
    output_schema: Schema,
}

impl AggregationPlanNode {
    /// Constructs a new AggregationPlanNode.
    pub fn new(
        children: Vec<PlanNode>,
        group_by_exprs: Vec<Arc<Expression>>,
        aggregate_exprs: Vec<Arc<Expression>>,
    ) -> Self {
        let mut columns = Vec::new();

        // Add group by columns with their original names
        for expr in &group_by_exprs {
            if let Expression::ColumnRef(col_ref) = expr.as_ref() {
                // Keep the original column name for group by columns
                columns.push(col_ref.get_return_type().clone());
            }
        }

        // Add aggregate columns with properly formatted names
        for expr in &aggregate_exprs {
            if let Expression::Aggregate(agg) = expr.as_ref() {
                let col = agg.get_return_type().clone();
                // Get the actual column name from the aggregate argument
                let arg_name = match agg.get_arg().as_ref() {
                    Expression::ColumnRef(col_ref) => col_ref.get_return_type().get_name().to_string(),
                    _ => "*".to_string(), // Use * for COUNT(*) etc.
                };

                // Format name as FUNCTION(column)
                let new_name = if *agg.get_agg_type() == AggregationType::CountStar {
                    "COUNT(*)".to_string()
                } else {
                    format!("{}({})",
                            agg.get_agg_type().to_string().to_uppercase(),
                            arg_name)
                };

                columns.push(col.with_name(&new_name));
            }
        }

        Self {
            children,
            group_by_exprs,
            aggregate_exprs,
            output_schema: Schema::new(columns),
        }
    }

    /// Returns the child plan of this aggregation node.
    pub fn get_child_plan(&self) -> &PlanNode {
        &self.children[0]
    }

    /// Returns all group by expressions.
    pub fn get_group_bys(&self) -> &[Arc<Expression>] {
        &self.group_by_exprs
    }

    /// Returns all aggregate expressions.
    pub fn get_aggregates(&self) -> &[Arc<Expression>] {
        &self.aggregate_exprs
    }

    /// Returns all aggregate types.
    pub fn get_aggregate_types(&self) -> Vec<AggregationType> {
        self.aggregate_exprs.iter().map(|expr| {
            match expr.as_ref() {
                Expression::Aggregate(agg) => agg.get_agg_type().clone(),
                Expression::ColumnRef(_) => AggregationType::Sum, // Default for column refs
                _ => AggregationType::Sum, // Default for other expressions
            }
        }).collect()
    }

    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.output_schema = schema;
        self
    }

    pub fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }
}

impl AbstractPlanNode for AggregationPlanNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::Aggregation
    }
}

impl Display for AggregationPlanNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ Aggregate")?;

        // Add aggregate expressions with their types
        for expr in self.aggregate_exprs.iter() {
            match expr.as_ref() {
                Expression::Aggregate(agg_expr) => {
                    match agg_expr.get_agg_type() {
                        AggregationType::CountStar => {
                            write!(f, "\n   COUNT(*)")?;
                        }
                        _ => {
                            // Get the column name from the child expression
                            let col_name = if let Some(child) = agg_expr.get_children().first() {
                                if let Expression::ColumnRef(col_ref) = child.as_ref() {
                                    col_ref.get_return_type().get_name()
                                } else {
                                    "expr"
                                }
                            } else {
                                "expr"
                            };

                            write!(f, "\n   {}({})", agg_expr.get_agg_type(), col_name)?;
                        }
                    }
                }
                _ => write!(f, "\n   {}", expr)?,
            }
        }

        // Add group by expressions if any
        if !self.group_by_exprs.is_empty() {
            write!(f, "\n   Group By: [")?;
            for (i, expr) in self.group_by_exprs.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                match expr.as_ref() {
                    Expression::ColumnRef(col_ref) => {
                        write!(f, "{}", col_ref.get_return_type().get_name())?;
                    }
                    _ => write!(f, "{}", expr)?,
                }
            }
            write!(f, "]")?;
        }

        // Add schema and children if alternate flag is set
        if f.alternate() {
            writeln!(f, "\n   Schema: {}", self.output_schema)?;

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
    use crate::sql::execution::expressions::aggregate_expression::AggregateExpression;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::sql::execution::plans::mock_scan_plan::MockScanNode;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    #[test]
    fn test_aggregation_plan_node_creation() {
        let schema = Schema::new(vec![]);
        let table = "mock_table".to_string();
        let children = vec![PlanNode::MockScan(MockScanNode::new(
            schema.clone(),
            table,
            vec![],
        ))];

        // Create a COUNT(*) aggregate
        let count_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::CountStar,
            vec![Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(1),
                Column::new("count", TypeId::BigInt),
                vec![]
            )))],
            Column::new("count", TypeId::BigInt),
        )));

        let agg_node = AggregationPlanNode::new(
            children,
            vec![], // no group bys
            vec![count_expr], // one COUNT(*) aggregate
        );

        assert_eq!(agg_node.get_group_bys().len(), 0);
        assert_eq!(agg_node.get_aggregates().len(), 1);
        assert_eq!(agg_node.get_aggregate_types()[0], AggregationType::CountStar);
    }

    #[test]
    fn test_aggregation_plan_node_to_string() {
        let schema = Schema::new(vec![]);
        let table = "mock_table".to_string();
        let children = vec![PlanNode::MockScan(MockScanNode::new(
            schema.clone(),
            table,
            vec![],
        ))];

        let col1 = Column::new("col1", TypeId::Integer);
        let col2 = Column::new("col2", TypeId::Integer);

        let group_by = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            col1,
            vec![],
        )));

        let col2_ref = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            col2.clone(),
            vec![],
        )));

        // Create proper aggregate expressions
        let sum_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            vec![col2_ref.clone()],
            Column::new("SUM(col2)", TypeId::Integer),
        )));

        let count_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Count,
            vec![col2_ref.clone()],
            Column::new("COUNT(col2)", TypeId::BigInt),
        )));

        let agg_node = AggregationPlanNode::new(
            children,
            vec![group_by],
            vec![sum_expr, count_expr],
        );

        let result = agg_node.to_string();
        assert_eq!(
            result,
            "→ Aggregate\n   SUM(col2)\n   COUNT(col2)\n   Group By: [col1]"
        );
    }

    #[test]
    fn test_output_schema_with_group_by() {
        // Create test expressions
        let name_col = Column::new("name", TypeId::VarChar);
        let age_col = Column::new("age", TypeId::Integer);

        let group_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, name_col, vec![],
        )));

        let age_ref = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 1, age_col.clone(), vec![],
        )));

        let sum_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            vec![age_ref.clone()],
            Column::new("SUM(age)", TypeId::Integer),
        )));

        let count_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Count,
            vec![age_ref.clone()],
            Column::new("COUNT(age)", TypeId::BigInt),
        )));

        // Create plan node
        let agg_plan = AggregationPlanNode::new(
            vec![],
            vec![group_expr],
            vec![sum_expr, count_expr],
        );

        // Verify output schema
        let schema = agg_plan.get_output_schema();
        let columns = schema.get_columns();

        assert_eq!(columns.len(), 3, "Schema should have 3 columns");

        // Check group by column
        assert_eq!(columns[0].get_name(), "name");
        assert_eq!(columns[0].get_type(), TypeId::VarChar);

        // Check aggregate columns
        assert_eq!(columns[1].get_name(), "SUM(age)");
        assert_eq!(columns[1].get_type(), TypeId::Integer);

        assert_eq!(columns[2].get_name(), "COUNT(age)");
        assert_eq!(columns[2].get_type(), TypeId::BigInt);
    }

    #[test]
    fn test_output_schema_no_group_by() {
        let value_col = Column::new("value", TypeId::Integer);
        let value_ref = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, value_col.clone(), vec![],
        )));

        // Create MIN aggregate
        let min_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Min,
            vec![value_ref.clone()],
            Column::new("MIN(value)", TypeId::Integer),
        )));

        // Create MAX aggregate
        let max_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Max,
            vec![value_ref.clone()],
            Column::new("MAX(value)", TypeId::Integer),
        )));

        let agg_plan = AggregationPlanNode::new(
            vec![],
            vec![], // No group by
            vec![min_expr, max_expr],
        );

        let schema = agg_plan.get_output_schema();
        let columns = schema.get_columns();

        assert_eq!(columns.len(), 2, "Schema should have 2 columns");
        assert_eq!(columns[0].get_name(), "MIN(value)");
        assert_eq!(columns[0].get_type(), TypeId::Integer);
        assert_eq!(columns[1].get_name(), "MAX(value)");
        assert_eq!(columns[1].get_type(), TypeId::Integer);
    }

    #[test]
    fn test_output_schema_count_star() {
        let count_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::CountStar,
            vec![Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(1),
                Column::new("count", TypeId::Integer),
                vec![],
            )))],
            Column::new("count", TypeId::BigInt),
        )));

        let agg_plan = AggregationPlanNode::new(
            vec![],
            vec![],
            vec![count_expr],
        );

        let schema = agg_plan.get_output_schema();
        let columns = schema.get_columns();

        assert_eq!(columns.len(), 1, "Schema should have 1 column");
        assert_eq!(columns[0].get_name(), "COUNT(*)");
        assert_eq!(columns[0].get_type(), TypeId::BigInt);
    }

    #[test]
    fn test_output_schema_multiple_group_by() {
        let name_col = Column::new("name", TypeId::VarChar);
        let dept_col = Column::new("dept", TypeId::VarChar);
        let salary_col = Column::new("salary", TypeId::Integer);

        let name_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 0, name_col, vec![],
        )));
        let dept_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 1, dept_col, vec![],
        )));
        let salary_ref = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0, 2, salary_col.clone(), vec![],
        )));

        let sum_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            vec![salary_ref.clone()],
            Column::new("SUM(salary)", TypeId::Integer),
        )));

        let agg_plan = AggregationPlanNode::new(
            vec![],
            vec![name_expr, dept_expr], // Multiple group by columns
            vec![sum_expr],
        );

        let schema = agg_plan.get_output_schema();
        let columns = schema.get_columns();

        assert_eq!(columns.len(), 3, "Schema should have 3 columns");
        assert_eq!(columns[0].get_name(), "name");
        assert_eq!(columns[0].get_type(), TypeId::VarChar);
        assert_eq!(columns[1].get_name(), "dept");
        assert_eq!(columns[1].get_type(), TypeId::VarChar);
        assert_eq!(columns[2].get_name(), "SUM(salary)");
        assert_eq!(columns[2].get_type(), TypeId::Integer);
    }
}
