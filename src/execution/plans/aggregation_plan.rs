use crate::catalogue::schema::Schema;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::Value;
use std::fmt;
use std::fmt::Write;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Represents the various SQL aggregation functions.
#[derive(Debug, Clone, PartialEq)]
pub enum AggregationType {
    CountStar,
    Count,
    Sum,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregationPlanNode {
    output_schema: Arc<Schema>,
    children: Vec<PlanNode>,
    group_bys: Vec<Arc<Expression>>,
    aggregates: Vec<Arc<Expression>>,
    agg_types: Vec<AggregationType>,
}

/// Represents a key in an aggregation operation.
#[derive(Clone, Debug)]
pub struct AggregateKey {
    group_bys: Vec<Value>,
}

/// Represents a value for each of the running aggregates.
#[derive(Debug, Clone)]
pub struct AggregateValue {
    aggregates: Vec<Value>,
}

impl AggregationPlanNode {
    /// Constructs a new AggregationPlanNode.
    pub fn new(
        output_schema: Arc<Schema>,
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

    /// Returns the group by expression at the given index.
    pub fn get_group_by_at(&self, idx: usize) -> Option<&Arc<Expression>> {
        self.group_bys.get(idx)
    }

    /// Returns all group by expressions.
    pub fn get_group_bys(&self) -> &[Arc<Expression>] {
        &self.group_bys
    }

    /// Returns the aggregate expression at the given index.
    pub fn get_aggregate_at(&self, idx: usize) -> Option<&Arc<Expression>> {
        self.aggregates.get(idx)
    }

    /// Returns all aggregate expressions.
    pub fn get_aggregates(&self) -> &[Arc<Expression>] {
        &self.aggregates
    }

    /// Returns all aggregate types.
    pub fn get_aggregate_types(&self) -> &[AggregationType] {
        &self.agg_types
    }

    /// Infers the schema for the aggregation.
    pub fn infer_agg_schema(
        group_bys: &[Arc<Expression>],
        aggregates: &[Arc<Expression>],
        agg_types: &[AggregationType],
    ) -> Schema {
        // Implementation of schema inference goes here
        unimplemented!("Schema inference not yet implemented")
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
        format!("AggregationPlanNode: {:?}", self)
    }

    fn plan_node_to_string(&self) -> String {
        let mut result = String::new();

        write!(&mut result, "Aggregation {{ ").unwrap();

        if !self.group_bys.is_empty() {
            write!(&mut result, "group_by=[").unwrap();
            for (i, expr) in self.group_bys.iter().enumerate() {
                if i > 0 {
                    write!(&mut result, ", ").unwrap();
                }
                write!(&mut result, "{}", expr).unwrap(); // Use {} instead of {:?}
            }
            write!(&mut result, "], ").unwrap();
        }

        write!(&mut result, "aggregates=[").unwrap();
        for (i, (expr, agg_type)) in self
            .aggregates
            .iter()
            .zip(self.agg_types.iter())
            .enumerate()
        {
            if i > 0 {
                write!(&mut result, ", ").unwrap();
            }
            write!(&mut result, "{}({})", agg_type, expr).unwrap(); // Use {} instead of {:?}
        }
        write!(&mut result, "] }}").unwrap();

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

impl Hash for AggregateKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for value in &self.group_bys {
            value.hash(state);
        }
    }
}

impl PartialEq for AggregateKey {
    fn eq(&self, other: &Self) -> bool {
        self.group_bys.len() == other.group_bys.len()
            && self
                .group_bys
                .iter()
                .zip(&other.group_bys)
                .all(|(a, b)| a.compare_equals(b) == CmpBool::CmpTrue)
    }
}

impl Display for AggregationType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            AggregationType::CountStar => write!(f, "COUNT(*)"),
            AggregationType::Count => write!(f, "COUNT"),
            AggregationType::Sum => write!(f, "SUM"),
            AggregationType::Min => write!(f, "MIN"),
            AggregationType::Max => write!(f, "MAX"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalogue::column::Column;
    use crate::container::hash_function::HashFunction;
    use crate::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::execution::plans::mock_scan_plan::MockScanNode;
    use crate::types_db::type_id::TypeId;

    #[test]
    fn test_aggregation_plan_node_creation() {
        let schema = Arc::new(Schema::new(vec![]));
        let table = "mock_table".to_string();
        let children = vec![PlanNode::MockScan(MockScanNode::new(
            schema.clone(),
            table,
            vec![],
        ))];
        let group_bys = vec![];
        let aggregates = vec![];
        let agg_types = vec![AggregationType::CountStar];

        let agg_node =
            AggregationPlanNode::new(schema, children, group_bys, aggregates, agg_types.clone());

        assert_eq!(agg_node.get_group_bys().len(), 0);
        assert_eq!(agg_node.get_aggregates().len(), 0);
        assert_eq!(agg_node.get_aggregate_types().len(), 1);
        assert_eq!(
            agg_node.get_aggregate_types()[0],
            AggregationType::CountStar
        );
    }

    #[test]
    fn test_aggregate_key_equality() {
        let key1 = AggregateKey {
            group_bys: vec![Value::from(1), Value::from("tests")],
        };
        let key2 = AggregateKey {
            group_bys: vec![Value::from(1), Value::from("tests")],
        };
        let key3 = AggregateKey {
            group_bys: vec![Value::from(2), Value::from("tests")],
        };

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_aggregate_key_hash() {
        let key1 = AggregateKey {
            group_bys: vec![Value::from(1), Value::from("tests")],
        };
        let key2 = AggregateKey {
            group_bys: vec![Value::from(1), Value::from("tests")],
        };

        let hasher = HashFunction::<AggregateKey>::new();

        let hasher1 = hasher.get_hash(&key1);
        let hasher2 = hasher.get_hash(&key2);

        assert_eq!(hasher1, hasher2);
    }

    fn test_aggregation_plan_node_to_string() {
        let schema = Arc::new(Schema::new(vec![]));
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
            "Aggregation { group_by=[ColumnValue(0)], aggregates=[SUM(ColumnValue(1)), COUNT(ColumnValue(1))] }"
        );
    }

    #[test]
    fn test_aggregation_plan_node_to_string_no_group_by() {
        let schema = Arc::new(Schema::new(vec![]));
        let table = "mock_table".to_string();
        let children = vec![PlanNode::MockScan(MockScanNode::new(
            schema.clone(),
            table,
            vec![],
        ))];

        let col1 = Column::new("col1", TypeId::Integer);

        let aggregate = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            col1,
            vec![],
        )));

        let agg_node = AggregationPlanNode::new(
            schema,
            children,
            vec![],
            vec![aggregate.clone()],
            vec![AggregationType::CountStar],
        );

        let result = agg_node.plan_node_to_string();
        assert_eq!(result, "Aggregation { aggregates=[COUNT(*)(#0.0)] }");
    }
}
