use crate::catalog::schema::Schema;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub struct OffsetNode {
    output_schema: Schema,
    offset: usize,
    children: Vec<PlanNode>,
}

impl OffsetNode {
    pub fn new(offset: usize, output_schema: Schema, children: Vec<PlanNode>) -> Self {
        Self {
            output_schema,
            offset,
            children,
        }
    }

    pub fn get_offset(&self) -> usize {
        self.offset
    }
}

impl AbstractPlanNode for OffsetNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::Offset
    }
}

impl Display for OffsetNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ Offset: {}", self.offset)?;

        if f.alternate() {
            write!(f, "\n   Schema: {}", self.output_schema)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::types_db::type_id::TypeId;

    #[test]
    fn test_offset_node_creation() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let offset_node = OffsetNode::new(5, schema.clone(), vec![]);

        assert_eq!(offset_node.get_offset(), 5);
        assert_eq!(offset_node.get_output_schema(), &schema);
        assert_eq!(offset_node.get_type(), PlanType::Offset);
    }

    #[test]
    fn test_offset_node_display() {
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let offset_node = OffsetNode::new(10, schema, vec![]);

        let display_str = format!("{}", offset_node);
        assert!(display_str.contains("Offset: 10"));
    }

    #[test]
    fn test_offset_node_display_alternate() {
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let offset_node = OffsetNode::new(3, schema, vec![]);

        let display_str = format!("{:#}", offset_node);
        assert!(display_str.contains("Offset: 3"));
        assert!(display_str.contains("Schema:"));
    }
} 