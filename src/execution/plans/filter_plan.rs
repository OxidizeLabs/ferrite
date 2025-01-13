use crate::catalog::schema::Schema;
use crate::common::config::TableOidT;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};

#[derive(Debug, Clone, PartialEq)]
pub struct FilterNode {
    output_schema: Schema,
    table_oid_t: TableOidT,
    table_name: String,
    predicate: Expression,
    children: Vec<PlanNode>,
}

impl FilterNode {
    pub fn new(
        output_schema: Schema,
        table_oid_t: TableOidT,
        table_name: String,
        predicate: Expression,
        child: PlanNode,
    ) -> Self {
        Self {
            output_schema,
            table_oid_t,
            table_name,
            predicate,
            children: vec![child],
        }
    }

    pub fn get_table_oid(&self) -> TableOidT {
        self.table_oid_t
    }

    pub fn get_table_name(&self) -> &String {
        &self.table_name
    }

    pub fn get_filter_predicate(&self) -> &Expression {
        &self.predicate
    }

    pub fn get_child_plan(&self) -> &PlanNode {
        &self.children[0]
    }
}

impl AbstractPlanNode for FilterNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::Filter
    }

    fn to_string(&self, with_schema: bool) -> String {
        let mut result = self.plan_node_to_string();
        if with_schema {
            result.push_str(&format!("\nSchema: {}", self.output_schema));
        }
        result.push_str(&self.children_to_string(2));
        result
    }

    fn plan_node_to_string(&self) -> String {
        format!("Filter {{ predicate={:?} }}", self.predicate)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::execution::expressions::abstract_expression::Expression;
    use crate::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::execution::expressions::comparison_expression::{
        ComparisonExpression, ComparisonType,
    };
    use crate::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
    use crate::execution::plans::seq_scan_plan::SeqScanPlanNode;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use std::sync::Arc;

    // =============== Test Helpers ===============
    mod helpers {
        use super::*;

        pub fn create_test_schema() -> Schema {
            Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("value", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("flag", TypeId::Boolean),
            ])
        }

        pub fn create_column_ref(schema: &Schema, col_idx: usize) -> Arc<Expression> {
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                col_idx,
                schema.get_column(col_idx).unwrap().clone(),
                vec![],
            )))
        }

        pub fn create_constant_int(value: i32, column: Column) -> Arc<Expression> {
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(value),
                column,
                vec![],
            )))
        }

        pub fn create_constant_bool(value: bool, column: Column) -> Arc<Expression> {
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(value),
                column,
                vec![],
            )))
        }

        pub fn create_constant_string(value: &str, column: Column) -> Arc<Expression> {
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(value.to_string()),
                column,
                vec![],
            )))
        }

        pub fn create_seq_scan(schema: Schema, table_name: &str) -> PlanNode {
            PlanNode::SeqScan(SeqScanPlanNode::new(
                schema,
                0,
                table_name.to_string(),
            ))
        }
    }

    // =============== Construction Tests ===============
    mod construction {
        use super::helpers::*;
        use super::*;

        #[test]
        fn test_basic_construction() {
            let schema = create_test_schema();
            let predicate = Expression::Comparison(ComparisonExpression::new(
                create_column_ref(&schema, 0),
                create_column_ref(&schema, 1),
                ComparisonType::LessThan,
                vec![],
            ));

            let filter_node = FilterNode::new(
                schema.clone(),
                42,
                "test_table".to_string(),
                predicate.clone(),
                create_seq_scan(schema.clone(), "test_table"),
            );

            assert_eq!(filter_node.get_type(), PlanType::Filter);
            assert_eq!(filter_node.get_table_oid(), 42);
            assert_eq!(filter_node.get_table_name(), "test_table");
            assert_eq!(filter_node.get_output_schema(), &schema);
        }

        #[test]
        fn test_construction_with_empty_schema() {
            let schema = Schema::new(vec![]);
            let predicate = Expression::Comparison(ComparisonExpression::new(
                create_constant_int(1, Column::new("const", TypeId::Integer)),
                create_constant_int(2, Column::new("const", TypeId::Integer)),
                ComparisonType::Equal,
                vec![],
            ));

            let filter_node = FilterNode::new(
                schema.clone(),
                0,
                "empty_table".to_string(),
                predicate,
                create_seq_scan(schema.clone(), "empty_table"),
            );

            assert_eq!(filter_node.get_output_schema().get_column_count(), 0);
            assert_eq!(filter_node.get_children().len(), 1);
        }
    }

    // =============== Predicate Tests ===============
    mod predicates {
        use super::helpers::*;
        use super::*;
        use crate::execution::expressions::abstract_expression::ExpressionOps;

        // =============== Basic Comparison Tests ===============
        #[test]
        fn test_all_comparison_types() {
            let schema = create_test_schema();
            let col_ref = create_column_ref(&schema, 0); // id column
            let const_val = create_constant_int(10, schema.get_column(0).unwrap().clone());

            let comparison_types = vec![
                ComparisonType::Equal,
                ComparisonType::NotEqual,
                ComparisonType::LessThan,
                ComparisonType::LessThanOrEqual,
                ComparisonType::GreaterThan,
                ComparisonType::GreaterThanOrEqual,
            ];

            for comp_type in comparison_types {
                let predicate = Expression::Comparison(ComparisonExpression::new(
                    col_ref.clone(),
                    const_val.clone(),
                    comp_type,
                    vec![],
                ));

                let filter_node = FilterNode::new(
                    schema.clone(),
                    0,
                    "test_table".to_string(),
                    predicate.clone(),
                    create_seq_scan(schema.clone(), "test_table"),
                );

                assert_eq!(filter_node.get_filter_predicate(), &predicate);
            }
        }

        // =============== Type-Specific Tests ===============
        #[test]
        fn test_integer_predicates() {
            let schema = create_test_schema();
            let test_cases = vec![
                (0, ComparisonType::Equal),              // id = 0
                (-1, ComparisonType::LessThan),          // id < -1
                (i32::MAX, ComparisonType::LessThan),    // id < MAX
                (i32::MIN, ComparisonType::GreaterThan), // id > MIN
            ];

            for (value, comp_type) in test_cases {
                let predicate = Expression::Comparison(ComparisonExpression::new(
                    create_column_ref(&schema, 0),
                    create_constant_int(value, schema.get_column(0).unwrap().clone()),
                    comp_type,
                    vec![],
                ));

                let filter_node = FilterNode::new(
                    schema.clone(),
                    0,
                    "test_table".to_string(),
                    predicate.clone(),
                    create_seq_scan(schema.clone(), "test_table"),
                );

                assert_eq!(filter_node.get_filter_predicate(), &predicate);
            }
        }

        #[test]
        fn test_string_predicates() {
            let schema = create_test_schema();
            let test_cases = vec![
                ("", ComparisonType::Equal),        // empty string
                ("test", ComparisonType::NotEqual), // normal string
                (
                    "very_long_string_test_case_with_underscores_and_multiple_words",
                    ComparisonType::LessThan,
                ), // long string
                ("特殊文字", ComparisonType::Equal), // non-ASCII characters
            ];

            for (value, comp_type) in test_cases {
                let predicate = Expression::Comparison(ComparisonExpression::new(
                    create_column_ref(&schema, 2), // name column
                    create_constant_string(value, schema.get_column(2).unwrap().clone()),
                    comp_type,
                    vec![],
                ));

                let filter_node = FilterNode::new(
                    schema.clone(),
                    0,
                    "test_table".to_string(),
                    predicate.clone(),
                    create_seq_scan(schema.clone(), "test_table"),
                );

                assert_eq!(filter_node.get_filter_predicate(), &predicate);
            }
        }

        #[test]
        fn test_boolean_predicates() {
            let schema = create_test_schema();
            let test_cases = vec![
                (true, ComparisonType::Equal),
                (false, ComparisonType::Equal),
                (true, ComparisonType::NotEqual),
                (false, ComparisonType::NotEqual),
            ];

            for (value, comp_type) in test_cases {
                let predicate = Expression::Comparison(ComparisonExpression::new(
                    create_column_ref(&schema, 3), // flag column
                    create_constant_bool(value, schema.get_column(3).unwrap().clone()),
                    comp_type,
                    vec![],
                ));

                let filter_node = FilterNode::new(
                    schema.clone(),
                    0,
                    "test_table".to_string(),
                    predicate.clone(),
                    create_seq_scan(schema.clone(), "test_table"),
                );

                assert_eq!(filter_node.get_filter_predicate(), &predicate);
            }
        }

        // =============== Column Comparison Tests ===============
        #[test]
        fn test_column_to_column_comparisons() {
            let schema = create_test_schema();
            let comparison_types = vec![
                ComparisonType::Equal,
                ComparisonType::NotEqual,
                ComparisonType::LessThan,
                ComparisonType::GreaterThan,
            ];

            // Compare different combinations of columns
            let column_pairs = vec![
                (0, 1), // id vs value
                (1, 0), // value vs id
                (0, 0), // id vs id (self-comparison)
            ];

            for (col1_idx, col2_idx) in column_pairs {
                for comp_type in &comparison_types {
                    let predicate = Expression::Comparison(ComparisonExpression::new(
                        create_column_ref(&schema, col1_idx),
                        create_column_ref(&schema, col2_idx),
                        comp_type.clone(),
                        vec![],
                    ));

                    let filter_node = FilterNode::new(
                        schema.clone(),
                        0,
                        "test_table".to_string(),
                        predicate.clone(),
                        create_seq_scan(schema.clone(), "test_table"),
                    );

                    assert_eq!(filter_node.get_filter_predicate(), &predicate);
                }
            }
        }

        // =============== Edge Cases ===============
        #[test]
        fn test_empty_children_vector() {
            let schema = create_test_schema();
            let predicate = Expression::Comparison(ComparisonExpression::new(
                create_column_ref(&schema, 0),
                create_constant_int(5, schema.get_column(0).unwrap().clone()),
                ComparisonType::Equal,
                vec![], // empty children vector
            ));

            let filter_node = FilterNode::new(
                schema.clone(),
                0,
                "test_table".to_string(),
                predicate.clone(),
                create_seq_scan(schema, "test_table"),
            );

            assert_eq!(filter_node.get_filter_predicate().get_children().len(), 0);
        }

        #[test]
        fn test_predicate_type_consistency() {
            let schema = create_test_schema();
            let test_cases = vec![
                // Column refs with matching types
                (create_column_ref(&schema, 0), create_column_ref(&schema, 1)), // both INTEGER
                // Constants with matching types
                (
                    create_constant_int(5, schema.get_column(0).unwrap().clone()),
                    create_constant_int(10, schema.get_column(0).unwrap().clone()),
                ),
                // Mixed column ref and constant
                (
                    create_column_ref(&schema, 0),
                    create_constant_int(5, schema.get_column(0).unwrap().clone()),
                ),
            ];

            for (left_expr, right_expr) in test_cases {
                let predicate = Expression::Comparison(ComparisonExpression::new(
                    left_expr,
                    right_expr,
                    ComparisonType::Equal,
                    vec![],
                ));

                let filter_node = FilterNode::new(
                    schema.clone(),
                    0,
                    "test_table".to_string(),
                    predicate.clone(),
                    create_seq_scan(schema.clone(), "test_table"),
                );

                assert_eq!(filter_node.get_filter_predicate(), &predicate);
            }
        }

        // =============== Complex Predicate Tests ===============
        #[test]
        fn test_nested_predicate_structure() {
            let schema = create_test_schema();

            // Create a more complex predicate structure
            // Example: (id > 0) AND (value < 100)
            let id_pred = Expression::Comparison(ComparisonExpression::new(
                create_column_ref(&schema, 0),
                create_constant_int(0, schema.get_column(0).unwrap().clone()),
                ComparisonType::GreaterThan,
                vec![],
            ));

            let value_pred = Expression::Comparison(ComparisonExpression::new(
                create_column_ref(&schema, 1),
                create_constant_int(100, schema.get_column(1).unwrap().clone()),
                ComparisonType::LessThan,
                vec![],
            ));

            let filter_node = FilterNode::new(
                schema.clone(),
                0,
                "test_table".to_string(),
                id_pred.clone(),
                create_seq_scan(schema.clone(), "test_table"),
            );

            assert_eq!(filter_node.get_filter_predicate(), &id_pred);

            // Test with the second predicate
            let filter_node2 = FilterNode::new(
                schema.clone(),
                0,
                "test_table".to_string(),
                value_pred.clone(),
                create_seq_scan(schema, "test_table"),
            );

            assert_eq!(filter_node2.get_filter_predicate(), &value_pred);
        }
    }

    // =============== Child Plan Tests ===============
    mod child_plans {
        use super::helpers::*;
        use super::*;

        #[test]
        fn test_child_plan_access() {
            let schema = create_test_schema();
            let child = create_seq_scan(schema.clone(), "test_table");
            let predicate = Expression::Comparison(ComparisonExpression::new(
                create_column_ref(&schema, 0),
                create_constant_int(1, schema.get_column(0).unwrap().clone()),
                ComparisonType::Equal,
                vec![],
            ));

            let filter_node = FilterNode::new(
                schema,
                0,
                "test_table".to_string(),
                predicate,
                child.clone(),
            );

            assert_eq!(filter_node.get_child_plan(), &child);
            assert_eq!(filter_node.get_children().len(), 1);
            assert_eq!(filter_node.get_children()[0], child);
        }

        #[test]
        fn test_multiple_predicates_same_child() {
            let schema = create_test_schema();
            let child = create_seq_scan(schema.clone(), "test_table");

            // Create two different predicates
            let predicate1 = Expression::Comparison(ComparisonExpression::new(
                create_column_ref(&schema, 0),
                create_constant_int(10, schema.get_column(0).unwrap().clone()),
                ComparisonType::GreaterThan,
                vec![],
            ));

            let predicate2 = Expression::Comparison(ComparisonExpression::new(
                create_column_ref(&schema, 1),
                create_constant_int(20, schema.get_column(1).unwrap().clone()),
                ComparisonType::LessThan,
                vec![],
            ));

            let filter_node1 = FilterNode::new(
                schema.clone(),
                0,
                "test_table".to_string(),
                predicate1,
                child.clone(),
            );

            let filter_node2 = FilterNode::new(
                schema,
                0,
                "test_table".to_string(),
                predicate2,
                child.clone(),
            );

            assert_eq!(filter_node1.get_child_plan(), &child);
            assert_eq!(filter_node2.get_child_plan(), &child);
        }
    }

    // =============== String Representation Tests ===============
    mod string_representation {
        use super::helpers::*;
        use super::*;

        #[test]
        fn test_to_string_with_schema() {
            let schema = create_test_schema();
            let predicate = Expression::Comparison(ComparisonExpression::new(
                create_column_ref(&schema, 0),
                create_constant_int(5, schema.get_column(0).unwrap().clone()),
                ComparisonType::Equal,
                vec![],
            ));

            let filter_node = FilterNode::new(
                schema.clone(),
                0,
                "test_table".to_string(),
                predicate,
                create_seq_scan(schema, "test_table"),
            );

            let string_repr = filter_node.to_string(true);
            assert!(string_repr.contains("Filter { predicate="));
            assert!(string_repr.contains("Schema: Schema (id, value, name, flag)"));
            assert!(string_repr.contains("Child 1: SeqScan { table: test_table }"));
        }

        #[test]
        fn test_to_string_without_schema() {
            let schema = create_test_schema();
            let predicate = Expression::Comparison(ComparisonExpression::new(
                create_column_ref(&schema, 0),
                create_constant_int(5, schema.get_column(0).unwrap().clone()),
                ComparisonType::Equal,
                vec![],
            ));

            let filter_node = FilterNode::new(
                schema.clone(),
                0,
                "test_table".to_string(),
                predicate,
                create_seq_scan(schema, "test_table"),
            );

            let string_repr = filter_node.to_string(false);
            assert!(string_repr.contains("Filter { predicate="));
            assert!(!string_repr.contains("Schema:"));
            assert!(string_repr.contains("Child 1: SeqScan { table: test_table }"));
        }

        #[test]
        fn test_plan_node_to_string() {
            let schema = create_test_schema();
            let predicate = Expression::Comparison(ComparisonExpression::new(
                create_column_ref(&schema, 0),
                create_constant_int(5, schema.get_column(0).unwrap().clone()),
                ComparisonType::Equal,
                vec![],
            ));

            let filter_node = FilterNode::new(
                schema.clone(),
                0,
                "test_table".to_string(),
                predicate,
                create_seq_scan(schema, "test_table"),
            );

            let node_string = filter_node.plan_node_to_string();
            assert!(node_string.contains("Filter { predicate="));
            assert!(!node_string.contains("Child"));
            assert!(!node_string.contains("Schema:"));
        }
    }
}
