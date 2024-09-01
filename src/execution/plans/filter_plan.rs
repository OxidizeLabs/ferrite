// use std::rc::Rc;
// use crate::catalogue::schema::Schema;
//
// // Assuming we've defined these types elsewhere
// type SchemaRef = Rc<Schema>;
//
// // Assuming we've defined an Expression enum to replace AbstractExpression
// #[derive(Debug, Clone, PartialEq)]
// pub enum Expression {
//     // Add variants as needed
// }
//
// #[derive(Debug, Clone)]
// pub struct FilterNode {
//     output: SchemaRef,
//     predicate: Rc<Expression>,
//     child: Box<PlanNode>,
// }
//
// impl FilterNode {
//     pub fn new(
//         output: SchemaRef,
//         predicate: Rc<Expression>,
//         child: Box<PlanNode>,
//     ) -> Self {
//         Self {
//             output,
//             predicate,
//             child,
//         }
//     }
//
//     pub fn get_predicate(&self) -> &Rc<Expression> {
//         &self.predicate
//     }
//
//     pub fn get_child_plan(&self) -> &PlanNode {
//         &self.child
//     }
// }
//
// // Add FilterNode as a variant to the PlanNode enum
// #[derive(Debug, Clone)]
// pub enum PlanNode {
//     Filter(FilterNode),
//     // ... other variants (like SeqScan)
// }
//
// impl PlanNode {
//     pub fn new_filter(
//         output: SchemaRef,
//         predicate: Rc<Expression>,
//         child: Box<PlanNode>,
//     ) -> Self {
//         PlanNode::Filter(FilterNode::new(output, predicate, child))
//     }
//
//     pub fn get_type(&self) -> PlanType {
//         match self {
//             PlanNode::Filter(_) => PlanType::Filter,
//             // ... handle other variants
//         }
//     }
//
//     pub fn as_filter(&self) -> Option<&FilterNode> {
//         if let PlanNode::Filter(node) = self {
//             Some(node)
//         } else {
//             None
//         }
//     }
//
//     pub fn plan_node_to_string(&self) -> string {
//         match self {
//             PlanNode::Filter(node) => {
//                 format!("Filter {{ predicate={:?} }}", node.predicate)
//             }
//             // ... handle other variants
//         }
//     }
// }
//
// #[derive(Debug, Clone, PartialEq)]
// pub enum PlanType {
//     Filter,
//     // ... other plan types
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn filter_plan_node() {
//         let schema = Rc::new(Schema::new(vec![])); // Dummy schema
//         let predicate = Rc::new(Expression::/* some expression */);
//         let child = Box::new(PlanNode::SeqScan(/* SeqScanNode details */));
//
//         let filter_node = PlanNode::new_filter(
//             schema.clone(),
//             predicate.clone(),
//             child,
//         );
//
//         assert_eq!(filter_node.get_type(), PlanType::Filter);
//
//         if let Some(node) = filter_node.as_filter() {
//             assert_eq!(node.get_predicate(), &predicate);
//             assert!(matches!(node.get_child_plan(), PlanNode::SeqScan(_)));
//         } else {
//             panic!("Expected Filter node");
//         }
//
//         assert_eq!(
//             filter_node.plan_node_to_string(),
//             format!("Filter {{ predicate={:?} }}", predicate)
//         );
//     }
// }