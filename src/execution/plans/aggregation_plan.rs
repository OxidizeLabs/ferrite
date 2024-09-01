// use crate::catalogue::schema::Schema;
// use crate::container::hash_function::HashFunction;
// use crate::types_db::value::Value;
// use std::fmt;
// use std::fmt::{Display, Formatter};
// use std::rc::Rc;
// use crate::execution::plans::abstract_plan::PlanNode;
//
// #[derive(Debug, Clone, PartialEq)]
// pub enum AggregationType {
//     CountStar,
//     Count,
//     Sum,
//     Min,
//     Max,
// }
//
// #[derive(Debug, Clone)]
// pub struct AggregationPlanNode {
//     output_schema: Rc<Schema>,
//     child: Box<PlanNode>,
//     group_bys: Vec<Rc<Expression>>,
//     aggregates: Vec<Rc<Expression>>,
//     agg_types: Vec<AggregationType>,
// }
//
// #[derive(Clone, Debug, Eq, PartialEq)]
// pub struct AggregateKey {
//     group_bys: Vec<Value>,
// }
//
// #[derive(Debug, Clone)]
// pub struct AggregateValue {
//     aggregates: Vec<Value>,
// }
//
// // Assuming we've defined an Expression enum to replace AbstractExpression trait
// #[derive(Debug, Clone, PartialEq)]
// pub enum Expression {
//     // Add variants as needed
// }
//
// impl AggregationPlanNode {
//     pub fn new(
//         output_schema: Rc<Schema>,
//         child: Box<PlanNode>,
//         group_bys: Vec<Rc<Expression>>,
//         aggregates: Vec<Rc<Expression>>,
//         agg_types: Vec<AggregationType>,
//     ) -> Self {
//         Self {
//             output_schema,
//             child,
//             group_bys,
//             aggregates,
//             agg_types,
//         }
//     }
//
//     pub fn get_child_plan(&self) -> &PlanNode {
//         &self.child
//     }
//
//     pub fn get_group_by_at(&self, idx: usize) -> Option<&Rc<Expression>> {
//         self.group_bys.get(idx)
//     }
//
//     pub fn get_group_bys(&self) -> &[Rc<Expression>] {
//         &self.group_bys
//     }
//
//     pub fn get_aggregate_at(&self, idx: usize) -> Option<&Rc<Expression>> {
//         self.aggregates.get(idx)
//     }
//
//     pub fn get_aggregates(&self) -> &[Rc<Expression>] {
//         &self.aggregates
//     }
//
//     pub fn get_aggregate_types(&self) -> &[AggregationType] {
//         &self.agg_types
//     }
//
//     pub fn infer_agg_schema(
//         group_bys: &[Rc<Expression>],
//         aggregates: &[Rc<Expression>],
//         agg_types: &[AggregationType],
//     ) -> Schema {
//         // Implementation of schema inference goes here
//         unimplemented!()
//     }
// }
//
// impl PlanNode {
//     pub fn as_aggregation(&self) -> Option<&AggregationPlanNode> {
//         if let PlanNode::Aggregation(node) = self {
//             Some(node)
//         } else {
//             None
//         }
//     }
// }
//
// impl Display for AggregationType {
//     fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
//         match self {
//             AggregationType::CountStar => write!(f, "count_star"),
//             AggregationType::Count => write!(f, "count"),
//             AggregationType::Sum => write!(f, "sum"),
//             AggregationType::Min => write!(f, "min"),
//             AggregationType::Max => write!(f, "max"),
//         }
//     }
// }
//
// // Add AggregationPlanNode as a variant to the PlanNode enum
// impl PlanNode {
//     pub fn new_aggregation(
//         output_schema: Rc<Schema>,
//         child: Box<PlanNode>,
//         group_bys: Vec<Rc<Expression>>,
//         aggregates: Vec<Rc<Expression>>,
//         agg_types: Vec<AggregationType>,
//     ) -> Self {
//         PlanNode::Aggregation(AggregationPlanNode::new(
//             output_schema,
//             child,
//             group_bys,
//             aggregates,
//             agg_types,
//         ))
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use crate::execution::plans::abstract_plan::PlanNode;
//     use super::*;
//
//     #[test]
//     fn aggregation_plan_node() {
//         let schema = Rc::new(Schema::new(vec![])); // Dummy schema
//         let child = Box::new(PlanNode::MockScan(/* Add necessary fields */));
//         let group_bys = vec![];
//         let aggregates = vec![];
//         let agg_types = vec![AggregationType::CountStar];
//
//         let agg_node = PlanNode::new_aggregation(
//             schema.clone(),
//             child,
//             group_bys,
//             aggregates,
//             agg_types.clone(),
//         );
//
//         if let PlanNode::Aggregation(node) = agg_node {
//             assert_eq!(node.get_group_bys().len(), 0);
//             assert_eq!(node.get_aggregates().len(), 0);
//             assert_eq!(node.get_aggregate_types().len(), 1);
//             assert_eq!(node.get_aggregate_types()[0], AggregationType::CountStar);
//         } else {
//             panic!("Expected Aggregation node");
//         }
//     }
//
//     #[test]
//     fn aggregate_key() {
//         let key1 = AggregateKey {
//             group_bys: vec![Value::from(1), Value::from(2)],
//         };
//         let key2 = AggregateKey {
//             group_bys: vec![Value::from(1), Value::from(2)],
//         };
//         let key3 = AggregateKey {
//             group_bys: vec![Value::from(1), Value::from(3)],
//         };
//
//         assert_eq!(key1, key2);
//         assert_ne!(key1, key3);
//
//         let hash_function = HashFunction::<AggregateKey>::new();
//         let hash_key1 = hash_function.get_hash(&key1);
//         let hash_key2 = hash_function.get_hash(&key2);
//
//         assert_eq!(hash_key1, hash_key2);
//     }
// }