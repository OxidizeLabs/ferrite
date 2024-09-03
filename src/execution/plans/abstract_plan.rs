use crate::catalogue::schema::Schema;
use crate::execution::plans::aggregation_plan::AggregationPlanNode;
use crate::execution::plans::mock_scan_plan::MockScanNode;
use crate::execution::plans::seq_scan_plan::SeqScanNode;
use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub enum PlanType {
    SeqScan,
    IndexScan,
    Insert,
    Update,
    Delete,
    Aggregation,
    Limit,
    NestedLoopJoin,
    NestedIndexJoin,
    HashJoin,
    Filter,
    Values,
    Projection,
    Sort,
    TopN,
    TopNPerGroup,
    MockScan,
    // InitCheck,
    Window,
}

#[derive(Debug, Clone)]
pub enum PlanNode {
    SeqScan(SeqScanNode),
    // IndexScan(IndexScanNode),
    // Insert(InsertNode),
    // Update(UpdateNode),
    // Delete(DeleteNode),
    Aggregation(AggregationPlanNode),
    // Limit(LimitNode),
    // NestedLoopJoin(NestedLoopJoinNode),
    // NestedIndexJoin(NestedIndexJoinNode),
    // HashJoin(HashJoinNode),
    // Filter(FilterNode),
    // Values(ValuesNode),
    // Projection(ProjectionNode),
    // Sort(SortNode),
    // TopN(TopNNode),
    // TopNPerGroup(TopNPerGroupNode),
    MockScan(MockScanNode),
    // Window(WindowNode)
}

pub trait AbstractPlanNode {
    fn get_output_schema(&self) -> &Schema;
    fn get_children(&self) -> &Vec<PlanNode>;
    fn get_type(&self) -> PlanType;
    fn to_string(&self, with_schema: bool) -> String;
    fn plan_node_to_string(&self) -> String;
    fn children_to_string(&self, indent: usize) -> String;
}

impl AbstractPlanNode for PlanNode {
    fn get_output_schema(&self) -> &Schema {
        match self {
            PlanNode::SeqScan(node) => &node.get_output_schema(),
            // PlanNode::IndexScan(node) => &node.get_output_schema(),
            // PlanNode::NestedLoopJoin(node) => &node.get_output_schema(),
            // PlanNode::Filter(node) => &node.get_output_schema(),
            PlanNode::Aggregation(node) => &node.get_output_schema(),
            // PlanNode::Insert(node) => &node.get_output_schema(),
            // PlanNode::Update(node) => &node.get_output_schema(),
            // PlanNode::Delete(node) => &node.get_output_schema(),
            // PlanNode::Limit(node) => &node.get_output_schema(),
            // PlanNode::NestedIndexJoin(node) => &node.get_output_schema(),
            // PlanNode::HashJoin(node) => &node.get_output_schema(),
            // PlanNode::Values(node) => &node.get_output_schema(),
            // PlanNode::Projection(node) => &node.get_output_schema(),
            // PlanNode::Sort(node) => &node.get_output_schema(),
            // PlanNode::TopN(node) => &node.get_output_schema(),
            // PlanNode::TopNPerGroup(node) => &node.get_output_schema(),
            PlanNode::MockScan(node) => &node.get_output_schema(),
            // PlanNode::Window(node) => &node.get_output_schema(),
        }
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        match self {
            PlanNode::SeqScan(node) => node.get_children(),
            // PlanNode::IndexScan(node) => &node.get_children(),
            // PlanNode::NestedLoopJoin(node) => &node.get_children(),
            // PlanNode::Filter(node) => &node.get_children(),
            // PlanNode::Insert(node) => &node.get_children(),
            // PlanNode::Update(node) => &node.get_children(),
            // PlanNode::Delete(node) => &node.get_children(),
            PlanNode::Aggregation(node) => &node.get_children(),
            // PlanNode::Limit(node) => &node.get_children(),
            // PlanNode::NestedIndexJoin(node) => &node.get_children(),
            // PlanNode::HashJoin(node) => &node.get_children(),
            // PlanNode::Values(node) => &node.get_children(),
            // PlanNode::Projection(node) => &node.get_children(),
            // PlanNode::Sort(node) => &node.get_children(),
            // PlanNode::TopN(node) => &node.get_children(),
            // PlanNode::TopNPerGroup(node) => &node.get_children(),
            PlanNode::MockScan(node) => &node.get_children(),
            // PlanNode::Window(node) => &node.get_children(),
        }
    }

    fn get_type(&self) -> PlanType {
        match self {
            PlanNode::SeqScan(_) => PlanType::SeqScan,
            // PlanNode::IndexScan(_) => PlanType::IndexScan,
            // PlanNode::NestedLoopJoin(_) => PlanType::NestedLoopJoin,
            // PlanNode::Filter(_) => PlanType::Filter,
            PlanNode::Aggregation(_) => PlanType::Aggregation,
            // PlanNode::Insert(_) => PlanType::Insert,
            // PlanNode::Update(_) => PlanType::Update,
            // PlanNode::Delete(_) => PlanType::Delete,
            // PlanNode::Limit(_) => PlanType::Limit,
            // PlanNode::NestedIndexJoin(_) => PlanType::NestedIndexJoin,
            // PlanNode::HashJoin(_) => PlanType::HashJoin,
            // PlanNode::Values(_) => PlanType::Values,
            // PlanNode::Projection(_) => PlanType::Projection,
            // PlanNode::Sort(_) => PlanType::Sort,
            // PlanNode::TopN(_) => PlanType::TopN,
            // PlanNode::TopNPerGroup(_) => PlanType::TopNPerGroup,
            PlanNode::MockScan(_) => PlanType::MockScan,
            // PlanNode::Window(_) => PlanType::Window,
        }
    }

    fn to_string(&self, with_schema: bool) -> String {
        let node_str = self.plan_node_to_string();
        let children_str = self.children_to_string(2);

        if with_schema {
            format!("{} | {}{}", node_str, self.get_output_schema(), children_str)
        } else {
            format!("{}{}", node_str, children_str)
        }
    }

    fn plan_node_to_string(&self) -> String {
        format!("{:?}", self.get_type())
    }

    fn children_to_string(&self, indent: usize) -> String {
        let mut result = String::new();
        for child in self.get_children() {
            result.push_str(&format!("\n{:indent$}", "", indent = indent));
            let child_str = ToString::to_string(&child);
            result.push_str(&child_str.as_str());
        }
        result
    }
}

// impl PartialEq for PlanNode {
//     fn eq(&self, other: &Self) -> bool {
//         if std::mem::discriminant(self) != std::mem::discriminant(other) {
//             return false;
//         }
//
//         match (self, other) {
//             (PlanNode::SeqScan(a), PlanNode::SeqScan(b)) => a.get_output_schema() == b.get_output_schema(),
//             (PlanNode::IndexScan(a), PlanNode::IndexScan(b)) => a.get_output_schema() == b.get_output_schema(),
//             (PlanNode::NestedLoopJoin(a), PlanNode::NestedLoopJoin(b)) => {
//                 a.get_output_schema() == b.get_output_schema() && a.left == b.left && a.right == b.right
//             }
//             (PlanNode::Filter(a), PlanNode::Filter(b)) => {
//                 a.get_output_schema() == b.get_output_schema() && a.child == b.child
//             }
//
//             (PlanNode::Insert(_), _) => {}
//             (PlanNode::Update(_), _) => {}
//             (PlanNode::Delete(_), _) => {}
//             (PlanNode::Aggregation(_), _) => {}
//             (PlanNode::Limit(_), _) => {}
//             (PlanNode::NestedIndexJoin(_), _) => {}
//             (PlanNode::HashJoin(_), _) => {}
//             (PlanNode::Values(_), _) => {}
//             (PlanNode::Projection(_), _) => {}
//             (PlanNode::Sort(_), _) => {}
//             (PlanNode::TopN(_), _) => {}
//             (PlanNode::TopNPerGroup(_), _) => {}
//             (PlanNode::MockScan(_), _) => {}
//             (PlanNode::Window(_), _) => {}
//         }
//     }
// }

impl Display for PlanNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", AbstractPlanNode::to_string(self, true))
    }
}

// #[cfg(test)]
// mod unit_tests {
//     use super::*;
//
//     #[test]
//     fn plan_node() {
//         let schema = Rc::new(Schema::new(vec![]));
//         let seq_scan = PlanNode::SeqScan(SeqScanNode {
//             output_schema: schema.clone(),
//         });
//         let index_scan = PlanNode::IndexScan(IndexScanNode {
//             output_schema: schema.clone(),
//         });
//         let nested_loop_join = PlanNode::NestedLoopJoin(NestedLoopJoinNode {
//             output_schema: schema.clone(),
//             left: Box::new(seq_scan),
//             right: Box::new(index_scan),
//         });
//
//         assert_eq!(nested_loop_join.get_type(), PlanType::NestedLoopJoin);
//         assert_eq!(nested_loop_join.get_children().len(), 2);
//         assert_eq!(nested_loop_join.get_children()[0].get_type(), PlanType::SeqScan);
//         assert_eq!(nested_loop_join.get_children()[1].get_type(), PlanType::IndexScan);
//     }
//
//     #[test]
//     fn to_string() {
//         let schema = Rc::new(Schema::new(vec![]));
//         let child = SeqScanNode::new((), 0, "".to_string(), None, vec![]);
//         let parent = FilterNode::new(Rc::new(()));
//
//
//         let expected = "Filter | Schema ()\n  SeqScan | Schema ()";
//         assert_eq!(parent.to_string(), expected);
//     }
// }