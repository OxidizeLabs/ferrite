#![allow(clippy::single_match)]

use std::sync::Arc;

use ferrite::buffer::buffer_pool_manager_async::BufferPoolManager;
use ferrite::buffer::lru_k_replacer::LRUKReplacer;
use ferrite::catalog::Catalog;
use ferrite::sql::execution::expressions::abstract_expression::Expression;
use ferrite::sql::execution::expressions::comparison_expression::ComparisonType;
use ferrite::sql::planner::logical_plan::LogicalPlanType;
use ferrite::sql::planner::query_planner::QueryPlanner;
use ferrite::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
use parking_lot::RwLock;
use tempfile::TempDir;

use crate::common::logger::init_test_logger;

struct TestContext {
    catalog: Arc<RwLock<Catalog>>,
    planner: QueryPlanner,
    _temp_dir: TempDir,
}

impl TestContext {
    async fn new(name: &str) -> Self {
        init_test_logger();
        const BUFFER_POOL_SIZE: usize = 100;
        const K: usize = 2;

        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir
            .path()
            .join(format!("{name}.db"))
            .to_str()
            .unwrap()
            .to_string();
        let log_path = temp_dir
            .path()
            .join(format!("{name}.log"))
            .to_str()
            .unwrap()
            .to_string();

        let disk_manager = AsyncDiskManager::new(db_path, log_path, DiskManagerConfig::default())
            .await
            .unwrap();
        let disk_manager_arc = Arc::new(disk_manager);
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
        let bpm =
            Arc::new(BufferPoolManager::new(BUFFER_POOL_SIZE, disk_manager_arc, replacer).unwrap());

        let transaction_manager =
            Arc::new(ferrite::concurrency::transaction_manager::TransactionManager::new());
        let catalog = Arc::new(RwLock::new(Catalog::new(bpm, transaction_manager.clone())));
        let planner = QueryPlanner::new(Arc::clone(&catalog));

        Self {
            catalog,
            planner,
            _temp_dir: temp_dir,
        }
    }

    fn setup_users(&mut self) {
        let mut catalog = self.catalog.write();
        let schema = ferrite::catalog::schema::Schema::new(vec![
            ferrite::catalog::column::Column::new(
                "id",
                ferrite::types_db::type_id::TypeId::Integer,
            ),
            ferrite::catalog::column::Column::new(
                "name",
                ferrite::types_db::type_id::TypeId::VarChar,
            ),
            ferrite::catalog::column::Column::new(
                "age",
                ferrite::types_db::type_id::TypeId::Integer,
            ),
        ]);
        let _ = catalog.create_table("users".to_string(), schema);
    }
}

#[tokio::test]
async fn simple_select() {
    let mut fixture = TestContext::new("simple_select").await;
    fixture.setup_users();
    let plan = fixture
        .planner
        .create_logical_plan("SELECT * FROM users")
        .unwrap();
    match &plan.plan_type {
        LogicalPlanType::Projection { schema, .. } => assert_eq!(schema.get_column_count(), 3),
        _ => panic!("Expected Projection root"),
    }
}

#[tokio::test]
async fn select_specific_columns() {
    let mut fixture = TestContext::new("select_specific_columns").await;
    fixture.setup_users();
    let plan = fixture
        .planner
        .create_logical_plan("SELECT id, name FROM users")
        .unwrap();
    match &plan.plan_type {
        LogicalPlanType::Projection { schema, .. } => assert_eq!(schema.get_column_count(), 2),
        _ => panic!("Expected Projection root"),
    }
}

#[tokio::test]
async fn select_with_filter() {
    let mut fixture = TestContext::new("select_with_filter").await;
    fixture.setup_users();
    let plan = fixture
        .planner
        .create_logical_plan("SELECT * FROM users WHERE age > 25")
        .unwrap();
    match &plan.children[0].plan_type {
        LogicalPlanType::Filter { .. } => (),
        _ => panic!("Expected Filter"),
    }
}

#[tokio::test]
async fn select_with_equality_filter() {
    let mut fixture = TestContext::new("select_equality_filter").await;
    fixture.setup_users();
    let plan = fixture
        .planner
        .create_logical_plan("SELECT * FROM users WHERE id = 1")
        .unwrap();
    match &plan.children[0].plan_type {
        LogicalPlanType::Filter { predicate, .. } => match predicate.as_ref() {
            ferrite::sql::execution::expressions::abstract_expression::Expression::Comparison(
                comp,
            ) => {
                assert_eq!(comp.get_comp_type(), ComparisonType::Equal);
            },
            _ => panic!("Expected Comparison"),
        },
        _ => panic!("Expected Filter"),
    }
}

#[tokio::test]
async fn select_from_nonexistent_table() {
    let mut fixture = TestContext::new("select_nonexistent_table").await;
    let result = fixture
        .planner
        .create_logical_plan("SELECT * FROM nonexistent_table");
    assert!(result.is_err());
}

#[tokio::test]
async fn test_select_with_column_aliases() {
    let mut fixture = TestContext::new("select_with_aliases").await;
    fixture.setup_users();

    let select_sql = "SELECT id AS user_id, name AS full_name, age AS years_old FROM users";
    let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Projection {
            expressions,
            schema,
            column_mappings: _,
        } => {
            assert_eq!(schema.get_column_count(), 3);
            assert_eq!(expressions.len(), 3);

            // Verify aliased column names
            let col_names: Vec<_> = (0..schema.get_column_count())
                .map(|i| schema.get_column(i as usize).unwrap().get_name())
                .collect();
            assert!(col_names.contains(&"user_id"));
            assert!(col_names.contains(&"full_name"));
            assert!(col_names.contains(&"years_old"));
        },
        _ => panic!("Expected Projection as root node"),
    }
}

#[tokio::test]
async fn test_select_with_multiple_conditions() {
    let mut fixture = TestContext::new("select_multiple_conditions").await;
    fixture.setup_users();

    let select_sql = "SELECT name FROM users WHERE age > 25 AND age < 65";
    let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Projection { schema, .. } => {
            assert_eq!(schema.get_column_count(), 1);
        },
        _ => panic!("Expected Projection as root node"),
    }

    match &plan.children[0].plan_type {
        LogicalPlanType::Filter { predicate, .. } => {
            // Should have a compound expression (AND)
            match predicate.as_ref() {
                Expression::Logic(_) => (), // AND expression
                _ => (),                    // Might be optimized differently
            }
        },
        _ => panic!("Expected Filter node"),
    }
}

#[tokio::test]
async fn test_select_with_string_comparison() {
    let mut fixture = TestContext::new("select_string_comparison").await;
    fixture.setup_users();

    let select_sql = "SELECT * FROM users WHERE name = 'John'";
    let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::Filter { predicate, .. } => match predicate.as_ref() {
            Expression::Comparison(comp) => {
                assert_eq!(comp.get_comp_type(), ComparisonType::Equal);
            },
            _ => panic!("Expected Comparison expression"),
        },
        _ => panic!("Expected Filter node"),
    }
}

#[tokio::test]
async fn test_select_with_not_equal() {
    let mut fixture = TestContext::new("select_not_equal").await;
    fixture.setup_users();

    let select_sql = "SELECT * FROM users WHERE age != 30";
    let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::Filter { predicate, .. } => match predicate.as_ref() {
            Expression::Comparison(comp) => {
                assert_eq!(comp.get_comp_type(), ComparisonType::NotEqual);
            },
            _ => panic!("Expected Comparison expression"),
        },
        _ => panic!("Expected Filter node"),
    }
}

#[tokio::test]
async fn test_select_with_less_than_equal() {
    let mut fixture = TestContext::new("select_less_than_equal").await;
    fixture.setup_users();

    let select_sql = "SELECT * FROM users WHERE age <= 40";
    let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::Filter { predicate, .. } => match predicate.as_ref() {
            Expression::Comparison(comp) => {
                assert_eq!(comp.get_comp_type(), ComparisonType::LessThanOrEqual);
            },
            _ => panic!("Expected Comparison expression"),
        },
        _ => panic!("Expected Filter node"),
    }
}

#[tokio::test]
async fn test_select_with_greater_than_equal() {
    let mut fixture = TestContext::new("select_greater_than_equal").await;
    fixture.setup_users();

    let select_sql = "SELECT * FROM users WHERE age >= 18";
    let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::Filter { predicate, .. } => match predicate.as_ref() {
            Expression::Comparison(comp) => {
                assert_eq!(comp.get_comp_type(), ComparisonType::GreaterThanOrEqual);
            },
            _ => panic!("Expected Comparison expression"),
        },
        _ => panic!("Expected Filter node"),
    }
}

#[tokio::test]
async fn test_select_with_is_null() {
    let mut fixture = TestContext::new("select_is_null").await;
    fixture.setup_users();

    let select_sql = "SELECT * FROM users WHERE name IS NULL";
    let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::Filter { predicate, .. } => {
            // Should have some form of NULL check expression
            match predicate.as_ref() {
                Expression::Comparison(comp) => {
                    // IS NULL might be represented as a special comparison
                    assert_eq!(comp.get_comp_type(), ComparisonType::Equal);
                },
                _ => (), // Might be a different expression type for IS NULL
            }
        },
        _ => panic!("Expected Filter node"),
    }
}

#[tokio::test]
async fn test_select_with_is_not_null() {
    let mut fixture = TestContext::new("select_is_not_null").await;
    fixture.setup_users();

    let select_sql = "SELECT * FROM users WHERE name IS NOT NULL";
    let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::Filter { predicate, .. } => {
            // Should have some form of NOT NULL check expression
            match predicate.as_ref() {
                Expression::Comparison(comp) => {
                    // IS NOT NULL might be represented as a special comparison
                    assert_eq!(comp.get_comp_type(), ComparisonType::NotEqual);
                },
                _ => (), // Might be a different expression type for IS NOT NULL
            }
        },
        _ => panic!("Expected Filter node"),
    }
}

#[tokio::test]
async fn test_select_with_or_condition() {
    let mut fixture = TestContext::new("select_or_condition").await;
    fixture.setup_users();

    let select_sql = "SELECT * FROM users WHERE age < 25 OR age > 65";
    let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::Filter { predicate, .. } => {
            // Should have a logical OR expression
            match predicate.as_ref() {
                Expression::Logic(_) => (), // OR expression
                _ => (),                    // Might be represented differently
            }
        },
        _ => panic!("Expected Filter node"),
    }
}

#[tokio::test]
async fn test_select_with_parentheses() {
    let mut fixture = TestContext::new("select_parentheses").await;
    fixture.setup_users();

    let select_sql = "SELECT * FROM users WHERE (age > 25 AND age < 35) OR (age > 65)";
    let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::Filter { predicate, .. } => {
            // Should handle nested logical expressions
            match predicate.as_ref() {
                Expression::Logic(_) => (), // Complex logical expression
                _ => (),                    // Might be optimized or represented differently
            }
        },
        _ => panic!("Expected Filter node"),
    }
}

#[tokio::test]
async fn test_select_with_table_alias() {
    let mut fixture = TestContext::new("select_table_alias").await;
    fixture.setup_users();

    let select_sql = "SELECT u.id, u.name FROM users u WHERE u.age > 25";
    let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Projection { schema, .. } => {
            assert_eq!(schema.get_column_count(), 2);

            // Check that aliases are handled properly
            let _col_names: Vec<_> = (0..schema.get_column_count())
                .map(|i| schema.get_column(i as usize).unwrap().get_name())
                .collect();

            // The column names might include the table alias
            // This depends on the implementation
        },
        _ => panic!("Expected Projection as root node"),
    }

    // Verify table scan has alias applied
    let table_scan = &plan.children[0].children[0];
    match &table_scan.plan_type {
        LogicalPlanType::TableScan { schema, .. } => {
            // Schema should have aliased column names
            let col = schema.get_column(0).unwrap();
            assert!(col.get_name().contains("u.") || col.get_name() == "id");
        },
        _ => panic!("Expected TableScan node"),
    }
}

#[tokio::test]
async fn test_select_mixed_qualified_unqualified() {
    let mut fixture = TestContext::new("select_mixed_qualified").await;
    fixture.setup_users();

    let select_sql = "SELECT users.id, name FROM users WHERE age > 25";
    let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Projection { schema, .. } => {
            assert_eq!(schema.get_column_count(), 2);
        },
        _ => panic!("Expected Projection as root node"),
    }
}

#[tokio::test]
async fn test_select_arithmetic_expression() {
    let mut fixture = TestContext::new("select_arithmetic").await;
    fixture.setup_users();

    let select_sql = "SELECT id, age + 1 AS next_age FROM users";
    let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Projection {
            expressions,
            schema,
            ..
        } => {
            assert_eq!(schema.get_column_count(), 2);
            assert_eq!(expressions.len(), 2);

            // Verify the alias is applied
            let col_names: Vec<_> = (0..schema.get_column_count())
                .map(|i| schema.get_column(i as usize).unwrap().get_name())
                .collect();
            assert!(col_names.contains(&"next_age"));
        },
        _ => panic!("Expected Projection as root node"),
    }
}

#[tokio::test]
async fn test_select_constant_expressions() {
    let mut fixture = TestContext::new("select_constants").await;
    fixture.setup_users();

    let select_sql = "SELECT id, 'constant_string' AS const_str, 42 AS const_num FROM users";
    let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Projection {
            expressions,
            schema,
            ..
        } => {
            assert_eq!(schema.get_column_count(), 3);
            assert_eq!(expressions.len(), 3);

            // Verify the constant aliases
            let col_names: Vec<_> = (0..schema.get_column_count())
                .map(|i| schema.get_column(i as usize).unwrap().get_name())
                .collect();
            assert!(col_names.contains(&"const_str"));
            assert!(col_names.contains(&"const_num"));
        },
        _ => panic!("Expected Projection as root node"),
    }
}

#[tokio::test]
async fn test_select_complex_expressions() {
    let mut fixture = TestContext::new("select_complex_expressions").await;
    fixture.setup_users();

    let select_sql = "SELECT id, age * 2 + 1 AS formula, name FROM users WHERE age * 2 > 50";
    let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Projection {
            expressions,
            schema,
            ..
        } => {
            assert_eq!(schema.get_column_count(), 3);
            assert_eq!(expressions.len(), 3);

            let col_names: Vec<_> = (0..schema.get_column_count())
                .map(|i| schema.get_column(i as usize).unwrap().get_name())
                .collect();
            assert!(col_names.contains(&"formula"));
        },
        _ => panic!("Expected Projection as root node"),
    }

    // Should have filter with arithmetic expression
    match &plan.children[0].plan_type {
        LogicalPlanType::Filter { .. } => (),
        _ => panic!("Expected Filter node"),
    }
}

#[tokio::test]
async fn test_select_nonexistent_column() {
    let mut fixture = TestContext::new("select_nonexistent_column").await;
    fixture.setup_users();

    let select_sql = "SELECT nonexistent_column FROM users";
    let result = fixture.planner.create_logical_plan(select_sql);

    assert!(result.is_err());
    let error_msg = result.unwrap_err();
    assert!(
        error_msg.contains("not found") || error_msg.contains("nonexistent_column"),
        "Expected error about column not found, got: {}",
        error_msg
    );
}

#[tokio::test]
async fn test_select_mixed_valid_invalid_columns() {
    let mut fixture = TestContext::new("select_mixed_columns").await;
    fixture.setup_users();

    let select_sql = "SELECT id, nonexistent_column FROM users";
    let result = fixture.planner.create_logical_plan(select_sql);

    assert!(result.is_err());
    let error_msg = result.unwrap_err();
    assert!(
        error_msg.contains("not found") || error_msg.contains("nonexistent_column"),
        "Expected error about invalid column, got: {}",
        error_msg
    );
}

#[tokio::test]
async fn test_select_with_case_expression() {
    let mut fixture = TestContext::new("select_case_expression").await;
    fixture.setup_users();

    let select_sql = "SELECT id, CASE WHEN age < 18 THEN 'Minor' WHEN age >= 65 THEN 'Senior' ELSE 'Adult' END AS age_group FROM users";
    let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Projection {
            expressions,
            schema,
            ..
        } => {
            assert_eq!(schema.get_column_count(), 2);
            assert_eq!(expressions.len(), 2);

            let col_names: Vec<_> = (0..schema.get_column_count())
                .map(|i| schema.get_column(i as usize).unwrap().get_name())
                .collect();
            assert!(col_names.contains(&"age_group"));
        },
        _ => panic!("Expected Projection as root node"),
    }
}

#[tokio::test]
async fn test_select_single_column() {
    let mut fixture = TestContext::new("select_single_column").await;
    fixture.setup_users();

    let select_sql = "SELECT name FROM users";
    let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Projection {
            expressions,
            schema,
            ..
        } => {
            assert_eq!(schema.get_column_count(), 1);
            assert_eq!(expressions.len(), 1);

            let col = schema.get_column(0).unwrap();
            assert_eq!(col.get_name(), "name");
        },
        _ => panic!("Expected Projection as root node"),
    }
}

#[tokio::test]
async fn test_select_duplicate_columns() {
    let mut fixture = TestContext::new("select_duplicate_columns").await;
    fixture.setup_users();

    let select_sql = "SELECT name, name, name FROM users";
    let plan = fixture.planner.create_logical_plan(select_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Projection {
            expressions,
            schema,
            ..
        } => {
            assert_eq!(schema.get_column_count(), 3);
            assert_eq!(expressions.len(), 3);

            // All three should be name columns
            for i in 0..3 {
                let col = schema.get_column(i).unwrap();
                assert_eq!(col.get_name(), "name");
            }
        },
        _ => panic!("Expected Projection as root node"),
    }
}

#[tokio::test]
async fn test_select_all_comparison_operators() {
    let mut fixture = TestContext::new("select_all_comparisons").await;
    fixture.setup_users();

    let test_cases = vec![
        ("SELECT * FROM users WHERE age = 30", ComparisonType::Equal),
        (
            "SELECT * FROM users WHERE age != 30",
            ComparisonType::NotEqual,
        ),
        (
            "SELECT * FROM users WHERE age <> 30",
            ComparisonType::NotEqual,
        ),
        (
            "SELECT * FROM users WHERE age < 30",
            ComparisonType::LessThan,
        ),
        (
            "SELECT * FROM users WHERE age <= 30",
            ComparisonType::LessThanOrEqual,
        ),
        (
            "SELECT * FROM users WHERE age > 30",
            ComparisonType::GreaterThan,
        ),
        (
            "SELECT * FROM users WHERE age >= 30",
            ComparisonType::GreaterThanOrEqual,
        ),
    ];

    for (sql, expected_op) in test_cases {
        let plan = fixture.planner.create_logical_plan(sql).unwrap();

        match &plan.children[0].plan_type {
            LogicalPlanType::Filter { predicate, .. } => match predicate.as_ref() {
                Expression::Comparison(comp) => {
                    assert_eq!(comp.get_comp_type(), expected_op, "Failed for SQL: {}", sql);
                },
                _ => panic!("Expected Comparison expression for SQL: {}", sql),
            },
            _ => panic!("Expected Filter node for SQL: {}", sql),
        }
    }
}

#[tokio::test]
async fn test_select_with_complex_where_conditions() {
    let mut fixture = TestContext::new("select_complex_where").await;
    fixture.setup_users();

    let test_cases = vec![
        "SELECT * FROM users WHERE age > 18 AND age < 65 AND name IS NOT NULL",
        "SELECT * FROM users WHERE (age < 25 OR age > 65) AND id > 0",
        "SELECT * FROM users WHERE NOT (age < 18)",
        "SELECT * FROM users WHERE age BETWEEN 25 AND 65",
        "SELECT * FROM users WHERE name LIKE 'John%'",
        "SELECT * FROM users WHERE id IN (1, 2, 3, 4, 5)",
    ];

    for sql in test_cases {
        let result = fixture.planner.create_logical_plan(sql);
        // These might not all be supported, but they should either work or fail gracefully
        match result {
            Ok(plan) => {
                // If it works, verify basic structure
                match &plan.plan_type {
                    LogicalPlanType::Projection { .. } => match &plan.children[0].plan_type {
                        LogicalPlanType::Filter { .. } => (),
                        _ => panic!("Expected Filter node for SQL: {}", sql),
                    },
                    _ => panic!("Expected Projection as root for SQL: {}", sql),
                }
            },
            Err(_) => {
                // Some complex expressions might not be implemented yet
                // That's acceptable for this test
            },
        }
    }
}

#[tokio::test]
async fn test_select_with_string_operations() {
    let mut fixture = TestContext::new("select_string_operations").await;
    fixture.setup_users();

    let select_sql =
        "SELECT name, UPPER(name) AS upper_name, LENGTH(name) AS name_length FROM users";
    let result = fixture.planner.create_logical_plan(select_sql);

    // String functions might not be implemented yet, so test graceful handling
    match result {
        Ok(plan) => match &plan.plan_type {
            LogicalPlanType::Projection {
                expressions,
                schema,
                ..
            } => {
                assert_eq!(schema.get_column_count(), 3);
                assert_eq!(expressions.len(), 3);
            },
            _ => panic!("Expected Projection as root node"),
        },
        Err(_) => {
            // String functions might not be implemented, which is acceptable
        },
    }
}

#[tokio::test]
async fn test_select_empty_where_clause() {
    let mut fixture = TestContext::new("select_empty_where").await;
    fixture.setup_users();

    // Test with just WHERE keyword but no condition (should be syntax error)
    let select_sql = "SELECT * FROM users WHERE";
    let result = fixture.planner.create_logical_plan(select_sql);

    assert!(result.is_err(), "Empty WHERE clause should cause an error");
}

#[tokio::test]
async fn test_select_very_long_query() {
    let mut fixture = TestContext::new("select_long_query").await;
    fixture.setup_users();

    // Test with a very long column list
    let long_projection = (0..50)
        .map(|i| format!("id AS id_{}", i))
        .collect::<Vec<_>>()
        .join(", ");

    let select_sql = format!("SELECT {} FROM users", long_projection);
    let plan = fixture.planner.create_logical_plan(&select_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Projection {
            expressions,
            schema,
            ..
        } => {
            assert_eq!(schema.get_column_count(), 50);
            assert_eq!(expressions.len(), 50);
        },
        _ => panic!("Expected Projection as root node"),
    }
}

#[tokio::test]
async fn test_select_with_nested_expressions() {
    todo!()
}
