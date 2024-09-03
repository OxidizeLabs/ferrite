use crate::catalogue::schema::Schema;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use crate::execution::plans::mock_scan_plan::MockScanNode;
use crate::types_db::value::Value;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::rc::Rc;

#[derive(Debug, Clone, PartialEq)]
pub enum AggregationType {
    CountStar,
    Count,
    Sum,
    Min,
    Max,
}

#[derive(Debug, Clone)]
pub struct AggregationPlanNode {
    output_schema: Rc<Schema>,
    child: Rc<PlanNode>,
    group_bys: Vec<Rc<Expression>>,
    aggregates: Vec<Rc<Expression>>,
    agg_types: Vec<AggregationType>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregateKey {
    group_bys: Vec<Value>,
}

#[derive(Debug, Clone)]
pub struct AggregateValue {
    aggregates: Vec<Value>,
}

impl AggregationPlanNode {
    pub fn new(
        output_schema: Rc<Schema>,
        child: Rc<PlanNode>,
        group_bys: Vec<Rc<Expression>>,
        aggregates: Vec<Rc<Expression>>,
        agg_types: Vec<AggregationType>,
    ) -> Self {
        Self {
            output_schema,
            child,
            group_bys,
            aggregates,
            agg_types,
        }
    }

    pub fn get_child_plan(&self) -> &PlanNode {
        &self.child
    }

    pub fn get_group_by_at(&self, idx: usize) -> Option<&Rc<Expression>> {
        self.group_bys.get(idx)
    }

    pub fn get_group_bys(&self) -> &[Rc<Expression>] {
        &self.group_bys
    }

    pub fn get_aggregate_at(&self, idx: usize) -> Option<&Rc<Expression>> {
        self.aggregates.get(idx)
    }

    pub fn get_aggregates(&self) -> &[Rc<Expression>] {
        &self.aggregates
    }

    pub fn get_aggregate_types(&self) -> &[AggregationType] {
        &self.agg_types
    }

    pub fn infer_agg_schema(
        group_bys: &[Rc<Expression>],
        aggregates: &[Rc<Expression>],
        agg_types: &[AggregationType],
    ) -> Schema {
        // Implementation of schema inference goes here
        unimplemented!()
    }
}

impl AbstractPlanNode for AggregationPlanNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        todo!()
    }

    fn get_type(&self) -> PlanType {
        self.get_type()
    }

    fn to_string(&self, with_schema: bool) -> String {
        todo!()
    }

    fn plan_node_to_string(&self) -> String {
        todo!()
    }

    fn children_to_string(&self, indent: usize) -> String {
        todo!()
    }
}

impl Display for AggregationType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            AggregationType::CountStar => write!(f, "count_star"),
            AggregationType::Count => write!(f, "count"),
            AggregationType::Sum => write!(f, "sum"),
            AggregationType::Min => write!(f, "min"),
            AggregationType::Max => write!(f, "max"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregation_plan_node() {
        let schema = Rc::new(Schema::new(vec![])); // Dummy schema
        let child = Rc::new(PlanNode::MockScan(MockScanNode::new(schema.clone(), None)));
        let group_bys = vec![];
        let aggregates = vec![];
        let agg_types = vec![AggregationType::CountStar];

        let agg_node = PlanNode::Aggregation(AggregationPlanNode::new(
            schema,
            child,
            group_bys,
            aggregates,
            agg_types.clone(),
        ));

        if let PlanNode::Aggregation(node) = agg_node {
            assert_eq!(node.get_group_bys().len(), 0);
            assert_eq!(node.get_aggregates().len(), 0);
            assert_eq!(node.get_aggregate_types().len(), 1);
            assert_eq!(node.get_aggregate_types()[0], AggregationType::CountStar);
        } else {
            panic!("Expected Aggregation node");
        }
    }

    #[test]
    fn aggregate_key() {
        let key1 = AggregateKey {
            group_bys: vec![Value::from(1), Value::from(2)],
        };
        let key2 = AggregateKey {
            group_bys: vec![Value::from(1), Value::from(2)],
        };
        let key3 = AggregateKey {
            group_bys: vec![Value::from(1), Value::from(3)],
        };

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }
}