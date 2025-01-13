use crate::catalog::schema::Schema;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::expressions::aggregate_expression::AggregationType;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use crate::types_db::value::Value;
use std::fmt::Write;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Plan node for aggregation operations
#[derive(Debug, Clone, PartialEq)]
pub struct AggregationPlanNode {
    output_schema: Schema,
    children: Vec<PlanNode>,
    group_bys: Vec<Arc<Expression>>,
    aggregates: Vec<Arc<Expression>>,
    agg_types: Vec<AggregationType>,
}

/// Represents a key in an aggregation operation.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct AggregateKey {
    pub group_bys: Vec<Value>,
}

/// Represents a value for each of the running aggregates.
#[derive(Debug, Clone)]
pub struct AggregateValue {
    pub aggregates: Vec<Value>,
}

impl AggregationPlanNode {
    /// Constructs a new AggregationPlanNode.
    pub fn new(
        output_schema: Schema,
        children: Vec<PlanNode>,
        group_bys: Vec<Arc<Expression>>,
        aggregates: Vec<Arc<Expression>>,
        agg_types: Vec<AggregationType>,
    ) -> Self {
        Self {
            output_schema,
            children,
            group_bys,
            aggregates,
            agg_types,
        }
    }

    /// Returns the child plan of this aggregation node.
    pub fn get_child_plan(&self) -> &PlanNode {
        &self.children[0]
    }

    /// Returns all group by expressions.
    pub fn get_group_bys(&self) -> &[Arc<Expression>] {
        &self.group_bys
    }

    /// Returns all aggregate expressions.
    pub fn get_aggregates(&self) -> &[Arc<Expression>] {
        &self.aggregates
    }

    /// Returns all aggregate types.
    pub fn get_aggregate_types(&self) -> &[AggregationType] {
        &self.agg_types
    }
}

impl AggregateKey {
    pub fn new(group_bys: Vec<Value>) -> Self {
        Self { group_bys }
    }
}

impl AggregateValue {
    pub fn new(aggregates: Vec<Value>) -> Self {
        Self { aggregates }
    }

    pub fn get_aggregate(&self, index: usize) -> &Value {
        &self.aggregates[index]
    }

    pub fn set_aggregate(&mut self, index: usize, value: Value) {
        self.aggregates[index] = value;
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

    fn to_string(&self, with_schema: bool) -> String {
        let mut result = String::new();
        write!(&mut result, "→ Aggregate").unwrap();

        // Add aggregate expressions with their types
        for (expr, agg_type) in self.aggregates.iter().zip(self.agg_types.iter()) {
            match agg_type {
                AggregationType::CountStar => {
                    write!(&mut result, "\n   COUNT(*)").unwrap();
                }
                _ => {
                    write!(&mut result, "\n   {}({})", agg_type, expr).unwrap();
                }
            }
        }

        // Add group by expressions if any
        if !self.group_bys.is_empty() {
            write!(&mut result, "\n   Group By: [").unwrap();
            for (i, expr) in self.group_bys.iter().enumerate() {
                if i > 0 {
                    write!(&mut result, ", ").unwrap();
                }
                write!(&mut result, "{}", expr).unwrap();
            }
            write!(&mut result, "]").unwrap();
        }

        // Add schema if requested
        if with_schema {
            write!(&mut result, "\n   Schema: {}", self.output_schema).unwrap();
        }

        // Add children
        for child in &self.children {
            let child_str = AbstractPlanNode::to_string(child, with_schema);
            for line in child_str.lines() {
                write!(&mut result, "\n   {}", line).unwrap();
            }
        }

        result
    }

    fn plan_node_to_string(&self) -> String {
        let mut result = String::new();
        write!(&mut result, "Aggregate ").unwrap();

        // Add group by expressions if any
        if !self.group_bys.is_empty() {
            write!(&mut result, "[GROUP BY: ").unwrap();
            for (i, expr) in self.group_bys.iter().enumerate() {
                if i > 0 {
                    write!(&mut result, ", ").unwrap();
                }
                write!(&mut result, "{}", expr).unwrap();
            }
            write!(&mut result, "] ").unwrap();
        }

        // Add aggregate expressions
        write!(&mut result, "[").unwrap();
        for (i, (expr, agg_type)) in self.aggregates.iter().zip(self.agg_types.iter()).enumerate() {
            if i > 0 {
                write!(&mut result, ", ").unwrap();
            }
            write!(&mut result, "{}({})", agg_type, expr).unwrap();
        }
        write!(&mut result, "]").unwrap();

        result
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
    use crate::catalog::column::Column;
    use crate::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::execution::plans::mock_scan_plan::MockScanNode;
    use crate::types_db::type_id::TypeId;

    #[test]
    fn test_aggregation_plan_node_creation() {
        let schema = Schema::new(vec![]);
        let table = "mock_table".to_string();
        let children = vec![PlanNode::MockScan(MockScanNode::new(
            schema.clone(),
            table,
            vec![],
        ))];
        let group_bys = vec![];
        let aggregates = vec![];
        let agg_types = vec![AggregationType::CountStar];

        let agg_node = AggregationPlanNode::new(
            schema,
            children,
            group_bys,
            aggregates,
            agg_types.clone(),
        );

        assert_eq!(agg_node.get_group_bys().len(), 0);
        assert_eq!(agg_node.get_aggregates().len(), 0);
        assert_eq!(agg_node.get_aggregate_types().len(), 1);
        assert_eq!(agg_node.get_aggregate_types()[0], AggregationType::CountStar);
    }

    #[test]
    fn test_aggregate_key_equality() {
        let key1 = AggregateKey::new(vec![Value::from(1), Value::from("test")]);
        let key2 = AggregateKey::new(vec![Value::from(1), Value::from("test")]);
        let key3 = AggregateKey::new(vec![Value::from(2), Value::from("test")]);

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
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
        let aggregate = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            col2,
            vec![],
        )));

        let agg_node = AggregationPlanNode::new(
            schema,
            children,
            vec![group_by],
            vec![aggregate.clone(), aggregate.clone()],
            vec![AggregationType::Sum, AggregationType::Count],
        );

        let result = agg_node.plan_node_to_string();
        assert_eq!(
            result,
            "Aggregate [GROUP BY: #0.0] [SUM(#0.1), COUNT(#0.1)]"
        );
    }
}
