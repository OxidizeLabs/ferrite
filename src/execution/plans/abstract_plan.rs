use crate::catalogue::schema::Schema;
use crate::execution::plans::aggregation_plan::AggregationPlanNode;
use crate::execution::plans::mock_scan_plan::MockScanNode;
use crate::execution::plans::seq_scan_plan::SeqScanNode;
use std::fmt;
use std::fmt::{Display, Formatter};
use crate::execution::plans::abstract_plan::PlanType::Aggregation;
use crate::execution::plans::delete_plan::DeleteNode;
use crate::execution::plans::filter_plan::FilterNode;
use crate::execution::plans::hash_join_plan::HashJoinNode;
use crate::execution::plans::index_scan_plan::IndexScanNode;
use crate::execution::plans::insert_plan::InsertNode;
use crate::execution::plans::limit_plan::LimitNode;
use crate::execution::plans::nested_index_join_plan::NestedIndexJoinNode;
use crate::execution::plans::nested_loop_join_plan::NestedLoopJoinNode;
use crate::execution::plans::projection_plan::ProjectionNode;
use crate::execution::plans::sort_plan::SortNode;
use crate::execution::plans::topn_per_group_plan::TopNPerGroupNode;
use crate::execution::plans::topn_plan::TopNNode;
use crate::execution::plans::update_plan::UpdateNode;
use crate::execution::plans::values_plan::ValuesNode;
use crate::execution::plans::window_plan::WindowNode;

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

#[derive(Debug, Clone, PartialEq)]
pub enum PlanNode {
    SeqScan(SeqScanNode),
    IndexScan(IndexScanNode),
    Insert(InsertNode),
    Update(UpdateNode),
    Delete(DeleteNode),
    Aggregation(AggregationPlanNode),
    Limit(LimitNode),
    NestedLoopJoin(NestedLoopJoinNode),
    NestedIndexJoin(NestedIndexJoinNode),
    HashJoin(HashJoinNode),
    Filter(FilterNode),
    Values(ValuesNode),
    Projection(ProjectionNode),
    Sort(SortNode),
    TopN(TopNNode),
    TopNPerGroup(TopNPerGroupNode),
    MockScan(MockScanNode),
    Window(WindowNode)
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
            _ => self.as_abstract_plan_node().get_output_schema()
        }
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        match self {
            _ => self.as_abstract_plan_node().get_children()
        }
    }

    fn get_type(&self) -> PlanType {
        match self {
            _ => self.as_abstract_plan_node().get_type()
        }
    }

    fn to_string(&self, with_schema: bool) -> String {
        match self {
            _ => self.as_abstract_plan_node().to_string(with_schema)
        }
    }

    fn plan_node_to_string(&self) -> String {
        match self {
            _ => self.as_abstract_plan_node().plan_node_to_string()
        }
    }

    fn children_to_string(&self, indent: usize) -> String {
        match self {
            _ => self.as_abstract_plan_node().children_to_string(indent)
        }
    }

}

impl PlanNode {

    // Helper method to get a reference to the AbstractPlanNode
    fn as_abstract_plan_node(&self) -> &dyn AbstractPlanNode {
        match self {
            PlanNode::SeqScan(node) => node,
            PlanNode::IndexScan(node) => node,
            PlanNode::NestedLoopJoin(node) => node,
            PlanNode::Filter(node) => node,
            PlanNode::Aggregation(node) => node,
            PlanNode::Insert(node) => node,
            PlanNode::Update(node) => node,
            PlanNode::Delete(node) => node,
            PlanNode::Limit(node) => node,
            PlanNode::NestedIndexJoin(node) => node,
            PlanNode::HashJoin(node) => node,
            PlanNode::Values(node) => node,
            PlanNode::Projection(node) => node,
            PlanNode::Sort(node) => node,
            PlanNode::TopN(node) => node,
            PlanNode::TopNPerGroup(node) => node,
            PlanNode::MockScan(node) => node,
            PlanNode::Window(node) => node,
        }
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
//                 a.get_output_schema() == b.get_output_schema() && a.get_left() == b.get_left() && a.get_right() == b.get_right()
//             }
//             (PlanNode::Filter(a), PlanNode::Filter(b)) => {
//                 a == b
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
//             (PlanNode::SeqScan(_), PlanNode::IndexScan(_)) => {}
//             (PlanNode::SeqScan(_), PlanNode::Insert(_)) => {}
//             (PlanNode::SeqScan(_), PlanNode::Update(_)) => {}
//             (PlanNode::SeqScan(_), PlanNode::Delete(_)) => {}
//             (PlanNode::SeqScan(_), PlanNode::Aggregation(_)) => {}
//             (PlanNode::SeqScan(_), PlanNode::Limit(_)) => {}
//             (PlanNode::SeqScan(_), PlanNode::NestedLoopJoin(_)) => {}
//             (PlanNode::SeqScan(_), PlanNode::NestedIndexJoin(_)) => {}
//             (PlanNode::SeqScan(_), PlanNode::HashJoin(_)) => {}
//             (PlanNode::SeqScan(_), PlanNode::Filter(_)) => {}
//             (PlanNode::SeqScan(_), PlanNode::Values(_)) => {}
//             (PlanNode::SeqScan(_), PlanNode::Projection(_)) => {}
//             (PlanNode::SeqScan(_), PlanNode::Sort(_)) => {}
//             (PlanNode::SeqScan(_), PlanNode::TopN(_)) => {}
//             (PlanNode::SeqScan(_), PlanNode::TopNPerGroup(_)) => {}
//             (PlanNode::SeqScan(_), PlanNode::MockScan(_)) => {}
//             (PlanNode::SeqScan(_), PlanNode::Window(_)) => {}
//         }
//     }
// }

impl Display for PlanNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", AbstractPlanNode::to_string(self, true))
    }
}
