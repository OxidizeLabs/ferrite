use std::fmt;
use std::rc::Rc;

use crate::catalogue::schema::Schema;

#[derive(Debug, Clone, Copy)]
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
    InitCheck,
    Window,
}

pub type AbstractPlanNodeRef = Rc<dyn AbstractPlanNode>;

pub trait AbstractPlanNode: fmt::Debug {
    fn output_schema(&self) -> &Schema;
    fn children(&self) -> &[AbstractPlanNodeRef];
    fn get_type(&self) -> PlanType;
    fn to_string(&self, with_schema: bool) -> String;
    fn clone_with_children(&self, children: Vec<AbstractPlanNodeRef>) -> Box<dyn AbstractPlanNode>;

    fn plan_node_to_string(&self) -> String {
        "<unknown>".to_string()
    }

    fn children_to_string(&self, indent: usize, _with_schema: bool) -> String {
        let mut result = String::new();
        for child in self.children() {
            result.push_str(&format!("\n{:indent$}", "", indent = indent));
            result.push_str(&child.to_string());
        }
        result
    }
}

pub struct AbstractPlanNodeBase {
    output_schema: Rc<Schema>,
    children: Vec<AbstractPlanNodeRef>,
}

impl AbstractPlanNodeBase {
    pub fn new(output_schema: Rc<Schema>, children: Vec<AbstractPlanNodeRef>) -> Self {
        Self {
            output_schema,
            children,
        }
    }
}

impl fmt::Display for dyn AbstractPlanNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string(true))
    }
}

// Implement for Box<dyn AbstractPlanNode> to allow formatting of boxed nodes
impl fmt::Display for Box<dyn AbstractPlanNode> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

// Macro to simplify implementation of clone_with_children for concrete plan nodes
#[macro_export]
macro_rules! impl_clone_with_children {
    ($type:ident) => {
        fn clone_with_children(&self, children: Vec<AbstractPlanNodeRef>) -> Box<dyn AbstractPlanNode> {
            let mut new_node = self.clone();
            new_node.base.children = children;
            Box::new(new_node)
        }
    };
}

// Example of how to implement a concrete plan node:
//
// #[derive(Clone)]
// pub struct SeqScanPlanNode {
//     base: AbstractPlanNodeBase,
//     table_oid: u64,
//     // other fields...
// }
//
// impl AbstractPlanNode for SeqScanPlanNode {
//     fn output_schema(&self) -> &Schema { &self.base.output_schema }
//     fn children(&self) -> &[AbstractPlanNodeRef] { &self.base.children }
//     fn get_type(&self) -> PlanType { PlanType::SeqScan }
//     fn to_string(&self, with_schema: bool) -> String {
//         // Implementation...
//     }
//     impl_clone_with_children!(SeqScanPlanNode);
// }