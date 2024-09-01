use crate::binder::table_ref::bound_base_table_ref::BoundBaseTableRef;
use crate::catalogue::column::Column;
use crate::catalogue::schema::Schema;
use crate::common::config::TableOidT;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::expressions::constant_value_expression::ConstantExpression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct SeqScanNode {
    output_schema: Schema,
    table_oid: TableOidT,
    table_name: String,
    filter_predicate: Option<Rc<Expression>>,
    children: Vec<PlanNode>,
}

impl SeqScanNode {
    pub fn new(
        output_schema: Schema,
        table_oid: TableOidT,
        table_name: String,
        filter_predicate: Option<Rc<Expression>>,
    ) -> Self {
        Self {
            output_schema,
            table_oid,
            table_name,
            filter_predicate,
            children: Vec::new(),
        }
    }

    pub fn get_table_oid(&self) -> TableOidT {
        self.table_oid
    }

    pub fn get_table_name(&self) -> &str {
        &self.table_name
    }

    pub fn get_filter_predicate(&self) -> Option<&Rc<Expression>> {
        self.filter_predicate.as_ref()
    }

    pub fn infer_scan_schema(table_ref: &BoundBaseTableRef) -> Result<Schema, String> {
        // This is a placeholder implementation. You should replace this with actual schema inference logic.
        let schema = table_ref.get_schema();
        Ok(schema.clone())
    }
}

impl AbstractPlanNode for SeqScanNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::SeqScan
    }

    fn to_string(&self, with_schema: bool) -> String {
        let mut result = self.plan_node_to_string();
        if with_schema {
            result.push_str(&format!("\nSchema: {}", self.output_schema));
        }
        if !self.children.is_empty() {
            result.push_str(&self.children_to_string(2));
        }
        result
    }

    fn plan_node_to_string(&self) -> String {
        if let Some(filter) = &self.filter_predicate {
            format!("SeqScan {{ table: {}, filter: {} }}", self.table_name, filter)
        } else {
            format!("SeqScan {{ table: {} }}", self.table_name)
        }
    }

    fn children_to_string(&self, indent: usize) -> String {
        self.children
            .iter()
            .enumerate()
            .map(|(i, child)| format!("\n{:indent$}Child {}: {}", "", i + 1, AbstractPlanNode::to_string(child, true), indent = indent))
            .collect()
    }
}

impl From<SeqScanNode> for PlanNode {
    fn from(node: SeqScanNode) -> Self {
        PlanNode::SeqScan(node)
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn seq_scan_plan_node() {
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let table_oid = 1;
        let table_name = "test_table".to_string();
        let filter_predicate = Some(Rc::new(Expression::Constant(ConstantExpression::new(
            Value::from(true),
            Column::new("test_column", TypeId::Integer),
        ))));

        let seq_scan_node = SeqScanNode::new(
            schema,
            table_oid,
            table_name.clone(),
            filter_predicate,
        );

        assert_eq!(seq_scan_node.get_type(), PlanType::SeqScan);
        assert_eq!(seq_scan_node.get_table_oid(), table_oid);
        assert_eq!(seq_scan_node.get_table_name(), table_name);
        assert!(seq_scan_node.get_filter_predicate().is_some());

        let plan_node_str = seq_scan_node.plan_node_to_string();
        assert!(plan_node_str.contains(&table_name));
        assert!(plan_node_str.contains("filter"));

        let full_str = seq_scan_node.to_string(true);
        assert!(full_str.contains(&table_name));
        assert!(full_str.contains("filter"));
        assert!(full_str.contains("Schema"));
    }

    #[test]
    fn seq_scan_plan_node_no_filter() {
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let table_oid = 1;
        let table_name = "test_table".to_string();

        let seq_scan_node = SeqScanNode::new(
            schema,
            table_oid,
            table_name.clone(),
            None,
        );

        assert_eq!(seq_scan_node.get_type(), PlanType::SeqScan);
        assert_eq!(seq_scan_node.get_table_oid(), table_oid);
        assert_eq!(seq_scan_node.get_table_name(), table_name);
        assert!(seq_scan_node.get_filter_predicate().is_none());

        let plan_node_str = seq_scan_node.plan_node_to_string();
        assert!(plan_node_str.contains(&table_name));
        assert!(!plan_node_str.contains("filter"));
    }
}