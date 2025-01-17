use crate::catalog::schema::Schema;
use crate::common::config::TableOidT;
use crate::sql::execution::expressions::abstract_expression::Expression;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct FilterNode {
    output_schema: Schema,
    table_oid_t: TableOidT,
    table_name: String,
    predicate: Arc<Expression>,
    children: Vec<PlanNode>,
}

impl FilterNode {
    pub fn new(
        output_schema: Schema,
        table_oid_t: TableOidT,
        table_name: String,
        predicate: Arc<Expression>,
        children: Vec<PlanNode>,
    ) -> Self {
        Self {
            output_schema,
            table_oid_t,
            table_name,
            predicate,
            children,
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
}

impl Display for FilterNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            // Detailed format
            write!(f, "→ Filter")?;
            write!(f, "\n   Predicate: {:#}", self.predicate)?;
            write!(f, "\n   Table: {}", self.table_name)?;
            write!(f, "\n   Schema: {}", self.output_schema)?;

            for (i, child) in self.children.iter().enumerate() {
                writeln!(f)?;
                write!(f, "   Child {}: {:#}", i + 1, child)?;
            }
        } else {
            // Basic format
            write!(f, "Filter {}", self.predicate)?
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::sql::execution::expressions::abstract_expression::Expression;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::comparison_expression::{
        ComparisonExpression, ComparisonType,
    };
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
    use crate::sql::execution::plans::seq_scan_plan::SeqScanPlanNode;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use std::sync::Arc;

    mod helpers {
        use super::*;

        // Schema creation helpers
        pub fn create_test_schema() -> Schema {
            Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("value", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("flag", TypeId::Boolean),
            ])
        }

        // Expression creation helpers
        pub fn create_column_ref(schema: &Schema, col_idx: usize) -> Arc<Expression> {
            Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                col_idx,
                schema.get_column(col_idx).unwrap().clone(),
                vec![],
            )))
        }

        pub fn create_constant_value(value: impl Into<Value>, column: Column) -> Arc<Expression> {
            Arc::new(Expression::Constant(ConstantExpression::new(
                value.into(),
                column,
                vec![],
            )))
        }

        pub fn create_comparison(
            left: Arc<Expression>,
            right: Arc<Expression>,
            comp_type: ComparisonType,
        ) -> Arc<Expression> {
            Arc::new(Expression::Comparison(ComparisonExpression::new(left, right, comp_type, vec![])))
        }

        // Plan node creation helpers
        pub fn create_seq_scan(schema: Schema, table_name: &str) -> PlanNode {
            PlanNode::SeqScan(SeqScanPlanNode::new(
                schema,
                0,
                table_name.to_string(),
            ))
        }

        pub fn create_test_filter_node() -> (FilterNode, Arc<Expression>) {
            let schema = create_test_schema();
            let col_ref = create_column_ref(&schema, 1); // value column
            let constant = create_constant_value(25, schema.get_column(1).unwrap().clone());
            let predicate = create_comparison(col_ref, constant, ComparisonType::GreaterThan);

            let scan_node = create_seq_scan(schema.clone(), "test_table");
            let filter_node = FilterNode::new(
                schema,
                1,
                "test_table".to_string(),
                predicate.clone(),
                vec![scan_node],
            );

            (filter_node, predicate)
        }

        // Predicate creation helpers
        pub fn create_test_predicate(expr_str: &str) -> Arc<Expression> {
            let schema = create_test_schema();

            match expr_str {
                "value > 25" | "age > 25" => {
                    let col_ref = create_column_ref(&schema, 1); // value/age column
                    let constant = create_constant_value(25, schema.get_column(1).unwrap().clone());
                    create_comparison(col_ref, constant, ComparisonType::GreaterThan)
                }
                "id < 100" => {
                    let col_ref = create_column_ref(&schema, 0); // id column
                    let constant = create_constant_value(100, schema.get_column(0).unwrap().clone());
                    create_comparison(col_ref, constant, ComparisonType::LessThan)
                }
                "flag = true" => {
                    let col_ref = create_column_ref(&schema, 3); // flag column
                    let constant = create_constant_value(true, schema.get_column(3).unwrap().clone());
                    create_comparison(col_ref, constant, ComparisonType::Equal)
                }
                expr => {
                    // Parse expression in format: "<column> <op> <value>"
                    let parts: Vec<&str> = expr.split_whitespace().collect();
                    if parts.len() != 3 {
                        panic!("Invalid predicate format: {}", expr);
                    }

                    let (col_name, op, value_str) = (parts[0], parts[1], parts[2]);

                    // Find column index
                    let col_idx = match col_name {
                        "id" => 0,
                        "value" | "age" => 1,
                        "name" => 2,
                        "flag" => 3,
                        _ => panic!("Unknown column: {}", col_name),
                    };

                    let col_ref = create_column_ref(&schema, col_idx);
                    let column = schema.get_column(col_idx).unwrap().clone();

                    // Parse value based on column type
                    let value = match column.get_type() {
                        TypeId::Integer => create_constant_value(value_str.parse::<i32>().unwrap(), column),
                        TypeId::VarChar => create_constant_value(value_str.to_string(), column),
                        TypeId::Boolean => create_constant_value(value_str.parse::<bool>().unwrap(), column),
                        _ => panic!("Unsupported type for column: {}", col_name),
                    };

                    // Parse operator
                    let comp_type = match op {
                        "=" | "==" => ComparisonType::Equal,
                        "!=" | "<>" => ComparisonType::NotEqual,
                        "<" => ComparisonType::LessThan,
                        "<=" => ComparisonType::LessThanOrEqual,
                        ">" => ComparisonType::GreaterThan,
                        ">=" => ComparisonType::GreaterThanOrEqual,
                        _ => panic!("Unknown operator: {}", op),
                    };

                    create_comparison(col_ref, value, comp_type)
                }
            }
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
                Arc::from(predicate.clone()),
                vec![create_seq_scan(schema.clone(), "test_table")],
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
                create_constant_value(1, Column::new("const", TypeId::Integer)),
                create_constant_value(2, Column::new("const", TypeId::Integer)),
                ComparisonType::Equal,
                vec![],
            ));

            let filter_node = FilterNode::new(
                schema.clone(),
                0,
                "empty_table".to_string(),
                Arc::from(predicate),
                vec![create_seq_scan(schema.clone(), "empty_table")],
            );

            assert_eq!(filter_node.get_output_schema().get_column_count(), 0);
            assert_eq!(filter_node.get_children().len(), 1);
        }
    }

    // =============== Predicate Tests ===============
    mod predicates {
        use super::helpers::*;
        use super::*;
        use crate::sql::execution::expressions::abstract_expression::ExpressionOps;

        // =============== Basic Comparison Tests ===============
        #[test]
        fn test_all_comparison_types() {
            let schema = create_test_schema();
            let col_ref = create_column_ref(&schema, 0); // id column
            let const_val = create_constant_value(10, schema.get_column(0).unwrap().clone());

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
                    Arc::from(predicate.clone()),
                    vec![create_seq_scan(schema.clone(), "test_table")],
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
                    create_constant_value(value, schema.get_column(0).unwrap().clone()),
                    comp_type,
                    vec![],
                ));

                let filter_node = FilterNode::new(
                    schema.clone(),
                    0,
                    "test_table".to_string(),
                    Arc::from(predicate.clone()),
                    vec![create_seq_scan(schema.clone(), "test_table")],
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
                    create_constant_value(value, schema.get_column(2).unwrap().clone()),
                    comp_type,
                    vec![],
                ));

                let filter_node = FilterNode::new(
                    schema.clone(),
                    0,
                    "test_table".to_string(),
                    Arc::from(predicate.clone()),
                    vec![create_seq_scan(schema.clone(), "test_table")],
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
                    create_constant_value(value, schema.get_column(3).unwrap().clone()),
                    comp_type,
                    vec![],
                ));

                let filter_node = FilterNode::new(
                    schema.clone(),
                    0,
                    "test_table".to_string(),
                    Arc::from(predicate.clone()),
                    vec![create_seq_scan(schema.clone(), "test_table")],
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
                        Arc::from(predicate.clone()),
                        vec![create_seq_scan(schema.clone(), "test_table")],
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
                create_constant_value(5, schema.get_column(0).unwrap().clone()),
                ComparisonType::Equal,
                vec![], // empty children vector
            ));

            let filter_node = FilterNode::new(
                schema.clone(),
                0,
                "test_table".to_string(),
                Arc::from(predicate),
                vec![create_seq_scan(schema.clone(), "test_table")],
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
                    create_constant_value(5, schema.get_column(0).unwrap().clone()),
                    create_constant_value(10, schema.get_column(0).unwrap().clone()),
                ),
                // Mixed column ref and constant
                (
                    create_column_ref(&schema, 0),
                    create_constant_value(5, schema.get_column(0).unwrap().clone()),
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
                    Arc::from(predicate.clone()),
                    vec![create_seq_scan(schema.clone(), "test_table")],
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
                create_constant_value(0, schema.get_column(0).unwrap().clone()),
                ComparisonType::GreaterThan,
                vec![],
            ));

            let value_pred = Expression::Comparison(ComparisonExpression::new(
                create_column_ref(&schema, 1),
                create_constant_value(100, schema.get_column(1).unwrap().clone()),
                ComparisonType::LessThan,
                vec![],
            ));

            let filter_node = FilterNode::new(
                schema.clone(),
                0,
                "test_table".to_string(),
                Arc::from(id_pred.clone()),
                vec![create_seq_scan(schema.clone(), "test_table")],
            );

            assert_eq!(filter_node.get_filter_predicate(), &id_pred);

            // Test with the second predicate
            let filter_node2 = FilterNode::new(
                schema.clone(),
                0,
                "test_table".to_string(),
                Arc::from(value_pred.clone()),
                vec![create_seq_scan(schema.clone(), "test_table")],
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
                create_constant_value(1, schema.get_column(0).unwrap().clone()),
                ComparisonType::Equal,
                vec![],
            ));

            let filter_node = FilterNode::new(
                schema,
                0,
                "test_table".to_string(),
                Arc::from(predicate),
                vec![child.clone()],
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
                create_constant_value(10, schema.get_column(0).unwrap().clone()),
                ComparisonType::GreaterThan,
                vec![],
            ));

            let predicate2 = Expression::Comparison(ComparisonExpression::new(
                create_column_ref(&schema, 1),
                create_constant_value(20, schema.get_column(1).unwrap().clone()),
                ComparisonType::LessThan,
                vec![],
            ));

            let filter_node1 = FilterNode::new(
                schema.clone(),
                0,
                "test_table".to_string(),
                Arc::from(predicate1),
                vec![child.clone()],
            );

            let filter_node2 = FilterNode::new(
                schema,
                0,
                "test_table".to_string(),
                Arc::from(predicate2),
                vec![child.clone()],
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
        fn test_basic_display() {
            let (filter_node, _) = create_test_filter_node();
            let basic_str = format!("{}", filter_node);

            println!("Basic display: {}", basic_str);
            assert!(basic_str.contains("Filter (value > 25)"));
        }

        #[test]
        fn test_detailed_display() {
            let (filter_node, _) = create_test_filter_node();
            let detailed_str = format!("{:#}", filter_node);

            println!("Detailed display: {}", detailed_str);
            assert!(detailed_str.contains("→ Filter"));
            assert!(detailed_str.contains("(Col#0.1 > Constant(25))"));
            assert!(detailed_str.contains("Schema:"));
            assert!(detailed_str.contains("Child 1:"));
        }

        #[test]
        fn test_plan_node_to_string() {
            let (filter_node, _) = create_test_filter_node();
            let plan_node = PlanNode::Filter(filter_node);
            let node_string = plan_node.to_string();

            println!("Plan node string: {}", node_string);
            assert!(node_string.contains("Filter (value > 25)"));
        }

        #[test]
        fn test_nested_filters_display() {
            let schema = create_test_schema();
            let inner_predicate = create_test_predicate("value > 25");
            let outer_predicate = create_test_predicate("id < 100");

            let scan_node = create_seq_scan(schema.clone(), "test_table");
            let inner_filter = FilterNode::new(
                schema.clone(),
                1,
                "test_table".to_string(),
                inner_predicate,
                vec![scan_node],
            );

            let outer_filter = FilterNode::new(
                schema,
                1,
                "test_table".to_string(),
                outer_predicate,
                vec![PlanNode::Filter(inner_filter)],
            );

            // Test basic format
            let basic_str = format!("{}", outer_filter);
            assert!(basic_str.contains("Filter (id < 100)"));

            // Test detailed format
            let detailed_str = format!("{:#}", outer_filter);
            assert!(detailed_str.contains("(Col#0.0 < Constant(100))"));
            assert!(detailed_str.contains("(Col#0.1 > Constant(25))"));
        }
    }
}
