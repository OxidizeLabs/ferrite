use crate::binder::table_ref::bound_base_table_ref::BoundBaseTableRef;
use crate::catalogue::column::Column;
use crate::catalogue::schema::Schema;
use crate::common::config::TableOidT;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::execution::expressions::comparison_expression::{ComparisonExpression, ComparisonType};
use crate::execution::expressions::constant_value_expression::ConstantExpression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct SeqScanNode {
    output_schema: Schema,
    table_oid: TableOidT,
    table_name: String,
    filter_predicate: Option<Arc<Expression>>,
    children: Vec<PlanNode>,
}

impl SeqScanNode {
    pub fn new(
        output_schema: Schema,
        table_oid: TableOidT,
        table_name: String,
        filter_predicate: Option<Arc<Expression>>,
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

    pub fn get_filter_predicate(&self) -> Option<&Arc<Expression>> {
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
        String::new()
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

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ])
    }

    #[test]
    fn seq_scan_plan_node() {
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let table_oid = 1;
        let table_name = "test_table".to_string();
        let filter_predicate = Some(Arc::new(Expression::Constant(ConstantExpression::new(Value::from(true), Column::new("test_column", TypeId::Integer), vec![]))));

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

    #[test]
    fn seq_scan_node_creation() {
        let schema = create_test_schema();
        let table_oid = 42;
        let table_name = "employees".to_string();

        let seq_scan_node = SeqScanNode::new(
            schema.clone(),
            table_oid,
            table_name.clone(),
            None,
        );

        assert_eq!(seq_scan_node.get_output_schema(), &schema);
        assert_eq!(seq_scan_node.get_table_oid(), table_oid);
        assert_eq!(seq_scan_node.get_table_name(), &table_name);
        assert!(seq_scan_node.get_filter_predicate().is_none());
        assert!(seq_scan_node.get_children().is_empty());
    }

    #[test]
    fn seq_scan_node_to_plan_node_conversion() {
        let schema = create_test_schema();
        let table_oid = 200;
        let table_name = "products".to_string();

        let seq_scan_node = SeqScanNode::new(
            schema,
            table_oid,
            table_name,
            None,
        );

        let plan_node: PlanNode = seq_scan_node.clone().into();

        match plan_node {
            PlanNode::SeqScan(node) => {
                assert_eq!(node, seq_scan_node);
            }
            _ => panic!("Expected PlanNode::SeqScan"),
        }
    }

    #[test]
    fn seq_scan_node_string_representation() {
        let schema = create_test_schema();
        let table_oid = 300;
        let table_name = "orders".to_string();

        let filter = Expression::Constant(ConstantExpression::new(
            Value::from(1000),
            Column::new("total", TypeId::Integer),
            vec![],
        ));

        let seq_scan_node = SeqScanNode::new(
            schema,
            table_oid,
            table_name.clone(),
            Some(Arc::new(filter)),
        );

        let with_schema = seq_scan_node.to_string(true);
        let without_schema = seq_scan_node.to_string(false);

        assert!(with_schema.contains("Schema"));
        assert!(with_schema.contains(&table_name));
        assert!(with_schema.contains("filter"));

        assert!(!without_schema.contains("Schema"));
        assert!(without_schema.contains(&table_name));
        assert!(without_schema.contains("filter"));
    }
}

#[cfg(test)]
mod seq_scan_string_tests {
    use super::*;


    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ])
    }

    fn create_complex_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
            Column::new("salary", TypeId::Decimal),
            Column::new("is_active", TypeId::Boolean),
        ])
    }

    #[test]
    fn seq_scan_simple_no_filter() {
        let schema = create_test_schema();
        let table_oid = 1;
        let table_name = "employees".to_string();

        let seq_scan_node = SeqScanNode::new(schema, table_oid, table_name.clone(), None);

        let with_schema = seq_scan_node.to_string(true);
        let without_schema = seq_scan_node.to_string(false);

        assert_eq!(without_schema, format!("SeqScan {{ table: {} }}", table_name));
        assert_eq!(with_schema, format!("SeqScan {{ table: {} }}\nSchema: Schema (id, name, age)", table_name));
    }

    #[test]
    fn seq_scan_complex_schema_no_filter() {
        let schema = create_complex_schema();
        let table_oid = 2;
        let table_name = "complex_table".to_string();

        let seq_scan_node = SeqScanNode::new(schema, table_oid, table_name.clone(), None);

        let with_schema = seq_scan_node.to_string(true);
        let without_schema = seq_scan_node.to_string(false);

        assert_eq!(without_schema, format!("SeqScan {{ table: {} }}", table_name));
        assert_eq!(with_schema, format!("SeqScan {{ table: {} }}\nSchema: Schema (id, name, age, salary, is_active)", table_name));
    }

    #[test]
    fn seq_scan_with_simple_filter() {
        let schema = create_test_schema();
        let table_oid = 3;
        let table_name = "filtered_employees".to_string();

        let age_column = schema.get_column(1).unwrap().clone();
        let age_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(0, 2, age_column, vec![])));
        let const_expr = Arc::new(Expression::Constant(ConstantExpression::new(Value::from(30), Column::new("const", TypeId::Integer), vec![])));
        let filter = Arc::new(Expression::Comparison(ComparisonExpression::new(
            age_expr,
            const_expr,
            ComparisonType::GreaterThan,
            vec![],
        )));

        let seq_scan_node = SeqScanNode::new(schema, table_oid, table_name.clone(), Some(filter));

        let with_schema = seq_scan_node.to_string(true);
        let without_schema = seq_scan_node.to_string(false);

        assert!(without_schema.contains(&format!("SeqScan {{ table: {}, filter: ", table_name)));
        assert!(without_schema.contains(">"));
        assert!(with_schema.contains(&format!("SeqScan {{ table: {}, filter: ", table_name)));
        assert!(with_schema.contains(">"));
        assert!(with_schema.contains("Schema: Schema (id, name, age)"));
    }
}