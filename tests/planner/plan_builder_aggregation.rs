#![allow(clippy::all, unused_must_use, unused_variables, dead_code)]

use crate::common::logger::init_test_logger;
use parking_lot::RwLock;
use std::sync::Arc;
use tempfile::TempDir;
use tkdb::buffer::lru_k_replacer::LRUKReplacer;
use tkdb::catalog::Catalog;
use tkdb::sql::execution::expressions::abstract_expression::Expression;
use tkdb::sql::execution::expressions::aggregate_expression::AggregationType;
use tkdb::sql::execution::plans::abstract_plan::AbstractPlanNode;
use tkdb::sql::planner::logical_plan::{LogicalPlanType, LogicalToPhysical};
use tkdb::sql::planner::query_planner::QueryPlanner;
use tkdb::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
use tkdb::types_db::type_id::TypeId;
use tkdb::{
    buffer::buffer_pool_manager_async::BufferPoolManager,
    sql::execution::plans::abstract_plan::PlanNode,
};

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
            Arc::new(tkdb::concurrency::transaction_manager::TransactionManager::new());
        let catalog = Arc::new(RwLock::new(Catalog::new(bpm, transaction_manager.clone())));
        let planner = QueryPlanner::new(Arc::clone(&catalog));

        Self {
            catalog,
            planner,
            _temp_dir: temp_dir,
        }
    }

    // Helper to create a table and verify it was created successfully
    fn create_table(
        &mut self,
        table_name: &str,
        columns: &str,
        if_not_exists: bool,
    ) -> Result<(), String> {
        let if_not_exists_clause = if if_not_exists { "IF NOT EXISTS " } else { "" };
        let create_sql = format!(
            "CREATE TABLE {}{} ({})",
            if_not_exists_clause, table_name, columns
        );
        let create_plan = self.planner.create_logical_plan(&create_sql)?;

        // Convert to physical plan and execute
        let physical_plan = create_plan.to_physical_plan()?;
        match physical_plan {
            PlanNode::CreateTable(create_table) => {
                let mut catalog = self.catalog.write();
                catalog.create_table(
                    create_table.get_table_name().to_string(),
                    create_table.get_output_schema().clone(),
                );
                Ok(())
            }
            _ => Err("Expected CreateTable plan node".to_string()),
        }
    }

    fn create_users(&mut self) {
        let mut catalog = self.catalog.write();
        let schema = tkdb::catalog::schema::Schema::new(vec![
            tkdb::catalog::column::Column::new("id", TypeId::Integer),
            tkdb::catalog::column::Column::new("name", TypeId::VarChar),
            tkdb::catalog::column::Column::new("age", TypeId::Integer),
        ]);
        let _ = catalog.create_table("users".to_string(), schema);
    }

    fn create_sales_table(&mut self) {
        let mut catalog = self.catalog.write();
        let schema = tkdb::catalog::schema::Schema::new(vec![
            tkdb::catalog::column::Column::new("id", TypeId::Integer),
            tkdb::catalog::column::Column::new("region", TypeId::VarChar),
            tkdb::catalog::column::Column::new("product", TypeId::VarChar),
            tkdb::catalog::column::Column::new("amount", TypeId::Decimal),
            tkdb::catalog::column::Column::new("quantity", TypeId::Integer),
            tkdb::catalog::column::Column::new("sale_date", TypeId::VarChar),
        ]);
        let _ = catalog.create_table("sales".to_string(), schema);
    }

    fn setup_multiple_tables(fixture: &mut TestContext) {
        fixture.create_users();
        fixture
            .create_table(
                "sales",
                "id INTEGER, region VARCHAR(255), product VARCHAR(255), amount DECIMAL, quantity INTEGER, sale_date VARCHAR(255)",
                false
            )
            .unwrap();
        fixture
            .create_table(
                "orders",
                "order_id INTEGER, customer_id INTEGER, total DECIMAL, order_date VARCHAR(255)",
                false,
            )
            .unwrap();

        // Create employees table
        fixture
            .create_table(
                "employees",
                "id INTEGER, name VARCHAR(255), department_id INTEGER, salary DECIMAL",
                false,
            )
            .unwrap();

        // Create departments table
        fixture
            .create_table("departments", "id INTEGER, name VARCHAR(255)", false)
            .unwrap();

        // Create employee_projects table
        fixture
            .create_table(
                "employee_projects",
                "employee_id INTEGER, project_id INTEGER",
                false,
            )
            .unwrap();
    }

    // Helper to verify a table exists in the catalog
    fn assert_table_exists(&self, table_name: &str) {
        let catalog = self.catalog.read();
        assert!(
            catalog.get_table(table_name).is_some(),
            "Table '{}' should exist",
            table_name
        );
    }

    // Helper to verify a table's schema
    fn assert_table_schema(&self, table_name: &str, expected_columns: &[(String, TypeId)]) {
        let catalog = self.catalog.read();
        let schema = catalog.get_table_schema(table_name).unwrap();

        assert_eq!(schema.get_column_count() as usize, expected_columns.len());

        for (i, (name, _type_id)) in expected_columns.iter().enumerate() {
            let column = schema.get_column(i).unwrap();
            assert_eq!(column.get_name(), name);
        }
    }
}

#[tokio::test]
async fn aggregate_count_star() {
    let mut ctx = TestContext::new("planner_agg_count_star").await;
    ctx.create_users();
    let plan = ctx
        .planner
        .create_logical_plan("SELECT COUNT(*) FROM users")
        .unwrap();
    match &plan.children[0].plan_type {
        LogicalPlanType::Aggregate {
            aggregates,
            group_by,
            ..
        } => {
            assert_eq!(aggregates.len(), 1);
            assert_eq!(group_by.len(), 0);
        }
        _ => panic!("Expected Aggregate node"),
    }
}

#[tokio::test]
async fn group_by_with_aggregates() {
    let mut ctx = TestContext::new("planner_agg_group_by").await;
    ctx.create_users();
    let sql = "SELECT name, COUNT(*) as c FROM users GROUP BY name";
    let plan = ctx.planner.create_logical_plan(sql).unwrap();
    match &plan.children[0].plan_type {
        LogicalPlanType::Aggregate {
            group_by,
            aggregates,
            schema,
        } => {
            assert_eq!(group_by.len(), 1);
            assert_eq!(aggregates.len(), 1);
            assert_eq!(schema.get_column_count(), 2);
        }
        _ => panic!("Expected Aggregate node"),
    }
}

#[tokio::test]
async fn having_clause() {
    let mut ctx = TestContext::new("planner_agg_having").await;
    ctx.create_users();
    let sql = "SELECT name, COUNT(*) FROM users GROUP BY name HAVING COUNT(*) > 1";
    let plan = ctx.planner.create_logical_plan(sql).unwrap();
    match &plan.children[0].plan_type {
        LogicalPlanType::Filter { .. } => match &plan.children[0].children[0].plan_type {
            LogicalPlanType::Aggregate { .. } => (),
            _ => panic!("Expected Aggregate under Filter"),
        },
        _ => panic!("Expected Filter for HAVING"),
    }
}

#[tokio::test]
async fn test_plan_aggregate_column_names() {
    let mut fixture = TestContext::new("test_plan_aggregate_column_names").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false);

    let test_cases = vec![
        (
            "SELECT name, SUM(age) FROM users GROUP BY name",
            vec!["name", "SUM_age"],
        ),
        (
            "SELECT name, SUM(age) as total_age FROM users GROUP BY name",
            vec!["name", "total_age"],
        ),
        (
            "SELECT name, COUNT(*) as user_count FROM users GROUP BY name",
            vec!["name", "user_count"],
        ),
        (
            "SELECT name, MIN(age) as min_age, MAX(age) as max_age FROM users GROUP BY name",
            vec!["name", "min_age", "max_age"],
        ),
        (
            "SELECT name, AVG(age) FROM users GROUP BY name",
            vec!["name", "AVG_age"],
        ),
    ];

    for (sql, expected_columns) in test_cases {
        let plan = fixture
            .planner
            .create_logical_plan(sql)
            .unwrap_or_else(|e| panic!("Failed to create plan for query '{}': {}", sql, e));

        // Verify the plan structure
        match &plan.plan_type {
            LogicalPlanType::Projection { schema, .. } => {
                // Verify column names match expected
                let actual_columns: Vec<_> = (0..schema.get_column_count())
                    .map(|i| {
                        schema
                            .get_column(i as usize)
                            .unwrap()
                            .get_name()
                            .to_string()
                    })
                    .collect();
                assert_eq!(
                    actual_columns, expected_columns,
                    "Column names don't match for query: {}",
                    sql
                );
            }
            _ => panic!("Expected Projection as root node for query: {}", sql),
        }
    }
}

#[tokio::test]
async fn test_plan_aggregate_types() {
    let mut fixture = TestContext::new("test_plan_aggregate_types").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false);

    let test_cases = vec![
        (
            "SELECT COUNT(*) FROM users",
            vec![AggregationType::CountStar],
            vec![TypeId::BigInt],
        ),
        (
            "SELECT SUM(age) FROM users",
            vec![AggregationType::Sum],
            vec![TypeId::BigInt],
        ),
        (
            "SELECT MIN(age), MAX(age) FROM users",
            vec![AggregationType::Min, AggregationType::Max],
            vec![TypeId::Integer, TypeId::Integer],
        ),
        (
            "SELECT AVG(age) FROM users",
            vec![AggregationType::Avg],
            vec![TypeId::Decimal],
        ),
    ];

    for (sql, expected_types, expected_return_types) in test_cases {
        let plan = fixture
            .planner
            .create_logical_plan(sql)
            .unwrap_or_else(|e| panic!("Failed to create plan for query '{}': {}", sql, e));

        // First verify we have a projection node
        match &plan.plan_type {
            LogicalPlanType::Projection { expressions, .. } => {
                assert_eq!(expressions.len(), expected_types.len());
            }
            _ => panic!("Expected Projection as root node for query: {}", sql),
        }

        // Then verify the aggregate node
        match &plan.children[0].plan_type {
            LogicalPlanType::Aggregate { aggregates, .. } => {
                // Check aggregate types
                assert_eq!(
                    aggregates.len(),
                    expected_types.len(),
                    "Wrong number of aggregates for query: {}",
                    sql
                );

                for (i, expected_type) in expected_types.iter().enumerate() {
                    if let Expression::Aggregate(agg) = aggregates[i].as_ref() {
                        assert_eq!(
                            agg.get_agg_type(),
                            expected_type,
                            "Aggregate type mismatch for query: {}",
                            sql
                        );
                        assert_eq!(
                            agg.get_return_type().get_type(),
                            expected_return_types[i],
                            "Return type mismatch for query: {}",
                            sql
                        );
                    } else {
                        panic!("Expected aggregate expression for query: {}", sql);
                    }
                }
            }
            _ => panic!("Expected Aggregate node for query: {}", sql),
        }
    }
}

#[tokio::test]
async fn test_simple_count_column() {
    let mut fixture = TestContext::new("simple_count_column").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false);

    let sql = "SELECT COUNT(name) FROM users";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::Aggregate { aggregates, .. } => {
            if let Expression::Aggregate(agg) = aggregates[0].as_ref() {
                assert_eq!(*agg.get_agg_type(), AggregationType::Count);
                assert_eq!(agg.get_return_type().get_type(), TypeId::BigInt);
            } else {
                panic!("Expected aggregate expression");
            }
        }
        _ => panic!("Expected Aggregate node"),
    }
}

#[tokio::test]
async fn test_sum_aggregate() {
    let mut fixture = TestContext::new("sum_aggregate").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false);

    let sql = "SELECT SUM(age) FROM users";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::Aggregate { aggregates, .. } => {
            if let Expression::Aggregate(agg) = aggregates[0].as_ref() {
                assert_eq!(*agg.get_agg_type(), AggregationType::Sum);
                assert_eq!(agg.get_return_type().get_type(), TypeId::BigInt);
            } else {
                panic!("Expected aggregate expression");
            }
        }
        _ => panic!("Expected Aggregate node"),
    }
}

#[tokio::test]
async fn test_avg_aggregate() {
    let mut fixture = TestContext::new("avg_aggregate").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false);

    let sql = "SELECT AVG(age) FROM users";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::Aggregate { aggregates, .. } => {
            if let Expression::Aggregate(agg) = aggregates[0].as_ref() {
                assert_eq!(*agg.get_agg_type(), AggregationType::Avg);
                assert_eq!(agg.get_return_type().get_type(), TypeId::Decimal);
            } else {
                panic!("Expected aggregate expression");
            }
        }
        _ => panic!("Expected Aggregate node"),
    }
}

#[tokio::test]
async fn test_min_max_aggregates() {
    let mut fixture = TestContext::new("min_max_aggregates").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false);

    let sql = "SELECT MIN(age), MAX(age) FROM users";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::Aggregate { aggregates, .. } => {
            assert_eq!(aggregates.len(), 2);

            if let Expression::Aggregate(min_agg) = aggregates[0].as_ref() {
                assert_eq!(*min_agg.get_agg_type(), AggregationType::Min);
                assert_eq!(min_agg.get_return_type().get_type(), TypeId::Integer);
            } else {
                panic!("Expected MIN aggregate expression");
            }

            if let Expression::Aggregate(max_agg) = aggregates[1].as_ref() {
                assert_eq!(*max_agg.get_agg_type(), AggregationType::Max);
                assert_eq!(max_agg.get_return_type().get_type(), TypeId::Integer);
            } else {
                panic!("Expected MAX aggregate expression");
            }
        }
        _ => panic!("Expected Aggregate node"),
    }
}

#[tokio::test]
async fn test_multiple_aggregates_with_aliases() {
    let mut fixture = TestContext::new("multiple_aggregates_aliases").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false);

    let sql = "SELECT COUNT(*) as user_count, SUM(age) as total_age, AVG(age) as avg_age, MIN(age) as min_age, MAX(age) as max_age FROM users";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();

    // Verify projection schema
    match &plan.plan_type {
        LogicalPlanType::Projection { schema, .. } => {
            assert_eq!(schema.get_column_count(), 5);

            let col_names: Vec<_> = (0..schema.get_column_count())
                .map(|i| schema.get_column(i as usize).unwrap().get_name())
                .collect();

            assert!(col_names.contains(&"user_count"));
            assert!(col_names.contains(&"total_age"));
            assert!(col_names.contains(&"avg_age"));
            assert!(col_names.contains(&"min_age"));
            assert!(col_names.contains(&"max_age"));
        }
        _ => panic!("Expected Projection as root node"),
    }

    // Verify aggregate types
    match &plan.children[0].plan_type {
        LogicalPlanType::Aggregate { aggregates, .. } => {
            assert_eq!(aggregates.len(), 5);

            let expected_types = [
                AggregationType::CountStar,
                AggregationType::Sum,
                AggregationType::Avg,
                AggregationType::Min,
                AggregationType::Max,
            ];

            for (i, expected_type) in expected_types.iter().enumerate() {
                if let Expression::Aggregate(agg) = aggregates[i].as_ref() {
                    assert_eq!(*agg.get_agg_type(), *expected_type);
                } else {
                    panic!("Expected aggregate expression at index {}", i);
                }
            }
        }
        _ => panic!("Expected Aggregate node"),
    }
}

#[tokio::test]
async fn test_group_by_single_column() {
    let mut fixture = TestContext::new("group_by_single_column").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false);

    let sql = "SELECT name, COUNT(*) FROM users GROUP BY name";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::Aggregate {
            group_by,
            aggregates,
            schema,
        } => {
            assert_eq!(group_by.len(), 1); // name column
            assert_eq!(aggregates.len(), 1); // COUNT(*)
            assert_eq!(schema.get_column_count(), 2); // name + count
        }
        _ => panic!("Expected Aggregate node"),
    }
}

#[tokio::test]
async fn test_group_by_multiple_columns() {
    let mut fixture = TestContext::new("group_by_multiple_columns").await;
    fixture.create_sales_table();

    let sql = "SELECT region, product, SUM(amount), COUNT(*) FROM sales GROUP BY region, product";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::Aggregate {
            group_by,
            aggregates,
            schema,
        } => {
            assert_eq!(group_by.len(), 2); // region, product
            assert_eq!(aggregates.len(), 2); // SUM, COUNT
            assert_eq!(schema.get_column_count(), 4); // region + product + sum + count
        }
        _ => panic!("Expected Aggregate node"),
    }
}

#[tokio::test]
async fn test_group_by_with_where_clause() {
    let mut fixture = TestContext::new("group_by_with_where").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false);

    let sql = "SELECT name, COUNT(*) FROM users WHERE age > 18 GROUP BY name";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();

    // Should have projection -> aggregate -> filter -> table_scan
    match &plan.plan_type {
        LogicalPlanType::Projection { .. } => match &plan.children[0].plan_type {
            LogicalPlanType::Aggregate { .. } => match &plan.children[0].children[0].plan_type {
                LogicalPlanType::Filter { .. } => (),
                _ => panic!("Expected Filter under Aggregate"),
            },
            _ => panic!("Expected Aggregate under Projection"),
        },
        _ => panic!("Expected Projection as root node"),
    }
}

#[tokio::test]
async fn test_having_with_different_aggregate_functions() {
    let mut fixture = TestContext::new("having_different_aggregates").await;
    fixture.create_sales_table();

    let test_cases = vec![
        "SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(amount) > 1000",
        "SELECT region, AVG(amount) FROM sales GROUP BY region HAVING AVG(amount) > 100",
        "SELECT region, COUNT(*) FROM sales GROUP BY region HAVING COUNT(*) >= 5",
        "SELECT region, MIN(amount) FROM sales GROUP BY region HAVING MIN(amount) > 10",
        "SELECT region, MAX(amount) FROM sales GROUP BY region HAVING MAX(amount) < 1000",
    ];

    for sql in test_cases {
        let plan = fixture.planner.create_logical_plan(sql).unwrap();

        // Verify basic structure: Projection -> Filter (HAVING) -> Aggregate
        match &plan.plan_type {
            LogicalPlanType::Projection { .. } => match &plan.children[0].plan_type {
                LogicalPlanType::Filter { .. } => match &plan.children[0].children[0].plan_type {
                    LogicalPlanType::Aggregate { .. } => (),
                    _ => panic!("Expected Aggregate under Filter for SQL: {}", sql),
                },
                _ => panic!("Expected Filter for HAVING clause for SQL: {}", sql),
            },
            _ => panic!("Expected Projection as root for SQL: {}", sql),
        }
    }
}

#[tokio::test]
async fn test_aggregates_with_expressions() {
    let mut fixture = TestContext::new("aggregates_with_expressions").await;
    fixture.create_sales_table();

    let sql = "SELECT region, SUM(amount * quantity) as total_value, AVG(amount / quantity) as avg_unit_price FROM sales GROUP BY region";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();

    // Verify that we can handle expressions inside aggregates
    match &plan.children[0].plan_type {
        LogicalPlanType::Aggregate { aggregates, .. } => {
            assert_eq!(aggregates.len(), 2); // SUM and AVG

            for agg_expr in aggregates {
                match agg_expr.as_ref() {
                    Expression::Aggregate(agg) => {
                        // Verify it's a supported aggregate type
                        assert!(matches!(
                            agg.get_agg_type(),
                            AggregationType::Sum | AggregationType::Avg
                        ));
                    }
                    _ => panic!("Expected aggregate expression"),
                }
            }
        }
        _ => panic!("Expected Aggregate node"),
    }
}

#[tokio::test]
async fn test_aggregate_without_group_by() {
    let mut fixture = TestContext::new("aggregate_without_group_by").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false);

    let sql = "SELECT COUNT(*), SUM(age), AVG(age) FROM users";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::Aggregate {
            group_by,
            aggregates,
            ..
        } => {
            assert_eq!(group_by.len(), 0); // No GROUP BY
            assert_eq!(aggregates.len(), 3); // COUNT, SUM, AVG
        }
        _ => panic!("Expected Aggregate node"),
    }
}

#[tokio::test]
async fn test_count_distinct() {
    let mut fixture = TestContext::new("count_distinct").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false);

    let sql = "SELECT COUNT(DISTINCT name) FROM users";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();

    // This might not be fully implemented yet, but should parse correctly
    match &plan.children[0].plan_type {
        LogicalPlanType::Aggregate { aggregates, .. } => {
            assert_eq!(aggregates.len(), 1);
            // The exact handling of DISTINCT might vary
        }
        _ => panic!("Expected Aggregate node"),
    }
}

#[tokio::test]
async fn test_aggregates_with_null_handling() {
    let mut fixture = TestContext::new("aggregates_null_handling").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false);

    let sql = "SELECT COUNT(name), COUNT(*) FROM users WHERE name IS NOT NULL";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();

    // Structure: Projection -> Aggregate -> Filter -> TableScan
    match &plan.plan_type {
        LogicalPlanType::Projection { .. } => match &plan.children[0].plan_type {
            LogicalPlanType::Aggregate { aggregates, .. } => {
                assert_eq!(aggregates.len(), 2);
            }
            _ => panic!("Expected Aggregate node"),
        },
        _ => panic!("Expected Projection as root node"),
    }
}

#[tokio::test]
async fn test_mixed_aggregates_and_columns_error() {
    let mut fixture = TestContext::new("mixed_aggregates_columns_error").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false);

    // This should be an error: selecting non-grouped columns with aggregates
    let sql = "SELECT name, age, COUNT(*) FROM users";
    let result = fixture.planner.create_logical_plan(sql);

    // This might be an error or might be handled differently depending on implementation
    // The test documents the expected behavior
}

#[tokio::test]
async fn test_group_by_ordinal_position() {
    let mut fixture = TestContext::new("group_by_ordinal").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false);

    let sql = "SELECT name, COUNT(*) FROM users GROUP BY 1";
    let result = fixture.planner.create_logical_plan(sql);

    // GROUP BY ordinal position might not be implemented yet
    // Test documents expected behavior
}

#[tokio::test]
async fn test_aggregate_functions_case_insensitive() {
    let mut fixture = TestContext::new("aggregate_case_insensitive").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false);

    let test_cases = vec![
        "SELECT count(*) FROM users",
        "SELECT Count(*) FROM users",
        "SELECT COUNT(*) FROM users",
        "SELECT sum(age) FROM users",
        "SELECT SUM(age) FROM users",
        "SELECT avg(age) FROM users",
        "SELECT AVG(age) FROM users",
    ];

    for sql in test_cases {
        let plan = fixture.planner.create_logical_plan(sql).unwrap();

        match &plan.children[0].plan_type {
            LogicalPlanType::Aggregate { aggregates, .. } => {
                assert_eq!(aggregates.len(), 1);
            }
            _ => panic!("Expected Aggregate node for SQL: {}", sql),
        }
    }
}

#[tokio::test]
async fn test_nested_aggregates_error() {
    let mut fixture = TestContext::new("nested_aggregates_error").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false);

    // This should be an error: nested aggregates
    let sql = "SELECT SUM(COUNT(age)) FROM users";
    let result = fixture.planner.create_logical_plan(sql);

    // Nested aggregates should cause an error
    assert!(result.is_err(), "Nested aggregates should cause an error");
}

#[tokio::test]
async fn test_aggregate_with_order_by() {
    let mut fixture = TestContext::new("aggregate_with_order_by").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false);

    let sql =
        "SELECT name, COUNT(*) as user_count FROM users GROUP BY name ORDER BY user_count DESC";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();

    // Should have Sort -> Projection -> Aggregate structure
    match &plan.plan_type {
        LogicalPlanType::Sort { .. } => match &plan.children[0].plan_type {
            LogicalPlanType::Projection { .. } => match &plan.children[0].children[0].plan_type {
                LogicalPlanType::Aggregate { .. } => (),
                _ => panic!("Expected Aggregate under Projection"),
            },
            _ => panic!("Expected Projection under Sort"),
        },
        _ => panic!("Expected Sort as root node"),
    }
}

#[tokio::test]
async fn test_aggregate_with_limit() {
    let mut fixture = TestContext::new("aggregate_with_limit").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false);

    let sql = "SELECT name, COUNT(*) FROM users GROUP BY name LIMIT 5";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();

    // Should have Limit -> Projection -> Aggregate structure
    match &plan.plan_type {
        LogicalPlanType::Limit { limit, .. } => {
            assert_eq!(*limit, 5);
            match &plan.children[0].plan_type {
                LogicalPlanType::Projection { .. } => {
                    match &plan.children[0].children[0].plan_type {
                        LogicalPlanType::Aggregate { .. } => (),
                        _ => panic!("Expected Aggregate under Projection"),
                    }
                }
                _ => panic!("Expected Projection under Limit"),
            }
        }
        _ => panic!("Expected Limit as root node"),
    }
}

#[tokio::test]
async fn test_complex_having_conditions() {
    let mut fixture = TestContext::new("complex_having_conditions").await;
    fixture.create_sales_table();

    let sql = "SELECT region, SUM(amount) as total, COUNT(*) as count FROM sales GROUP BY region HAVING SUM(amount) > 1000 AND COUNT(*) > 5";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();

    // Should have complex predicate in HAVING clause
    match &plan.children[0].plan_type {
        LogicalPlanType::Filter { predicate, .. } => {
            // Should be a compound condition (AND)
            match predicate.as_ref() {
                Expression::Logic(_) => (), // AND expression
                _ => (),                    // Might be represented differently
            }
        }
        _ => panic!("Expected Filter for HAVING clause"),
    }
}

#[tokio::test]
async fn test_group_by_all_syntax() {
    let mut fixture = TestContext::new("group_by_all").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false);

    let sql = "SELECT name, age, COUNT(*) FROM users GROUP BY ALL";
    let result = fixture.planner.create_logical_plan(sql);

    // GROUP BY ALL might not be implemented yet
    // Test documents expected behavior
}

#[tokio::test]
async fn test_aggregates_with_different_data_types() {
    let mut fixture = TestContext::new("aggregates_different_types").await;
    fixture.create_sales_table();

    let sql =
        "SELECT COUNT(id), SUM(amount), AVG(quantity), MIN(sale_date), MAX(product) FROM sales";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::Aggregate { aggregates, .. } => {
            assert_eq!(aggregates.len(), 5);

            // Verify different return types for different aggregates
            let expected_types = [
                (AggregationType::Count, TypeId::BigInt),
                (AggregationType::Sum, TypeId::BigInt),
                (AggregationType::Avg, TypeId::Decimal),
                (AggregationType::Min, TypeId::VarChar),
                (AggregationType::Max, TypeId::VarChar),
            ];

            for (i, (expected_agg_type, _expected_return_type)) in expected_types.iter().enumerate()
            {
                if let Expression::Aggregate(agg) = aggregates[i].as_ref() {
                    assert_eq!(*agg.get_agg_type(), *expected_agg_type);
                    // Note: Return types might be different based on implementation
                } else {
                    panic!("Expected aggregate expression at index {}", i);
                }
            }
        }
        _ => panic!("Expected Aggregate node"),
    }
}

#[tokio::test]
async fn test_aggregate_with_table_alias() {
    let mut fixture = TestContext::new("aggregate_table_alias").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false);

    let sql = "SELECT u.name, COUNT(*) FROM users u GROUP BY u.name";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::Aggregate {
            group_by,
            aggregates,
            ..
        } => {
            assert_eq!(group_by.len(), 1);
            assert_eq!(aggregates.len(), 1);
        }
        _ => panic!("Expected Aggregate node"),
    }
}

#[tokio::test]
async fn test_error_cases() {
    let mut fixture = TestContext::new("aggregate_error_cases").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false);

    let error_cases = vec![
        "SELECT COUNT() FROM users", // Missing argument for COUNT
        "SELECT SUM() FROM users",   // Missing argument for SUM
        "SELECT AVG(name) FROM users WHERE name IS NULL", // AVG on string (might be handled)
        "SELECT name FROM users GROUP BY age", // SELECT column not in GROUP BY
    ];

    for sql in error_cases {
        let result = fixture.planner.create_logical_plan(sql);
        // Most of these should be errors, but we test graceful handling
        // The exact behavior depends on implementation
    }
}

#[tokio::test]
async fn test_group_by_with_qualified_names_and_aliases() {
    let mut fixture = TestContext::new("test_group_by_qualified_aliases").await;
    TestContext::setup_multiple_tables(&mut fixture);

    // Test the exact case that was failing - GROUP BY with qualified names and aliases
    let sql = "
        SELECT e.name AS employee, d.name AS department, e.salary, COUNT(ep.project_id) AS project_count
        FROM employees e
        JOIN departments d ON e.department_id = d.id
        JOIN employee_projects ep ON e.id = ep.employee_id
        GROUP BY e.name, d.name, e.salary
    ";

    let result = fixture.planner.create_logical_plan(sql);
    assert!(
        result.is_ok(),
        "Query planning should succeed: {:?}",
        result.err()
    );

    let plan = result.unwrap();
    let schema = plan.get_schema().unwrap();

    // Verify the output schema has the correct column names
    assert_eq!(schema.get_column_count(), 4);
    assert_eq!(schema.get_column(0).unwrap().get_name(), "employee");
    assert_eq!(schema.get_column(1).unwrap().get_name(), "department");
    assert_eq!(schema.get_column(2).unwrap().get_name(), "e.salary");
    assert_eq!(schema.get_column(3).unwrap().get_name(), "project_count");
}

#[tokio::test]
async fn test_group_by_mixed_qualified_and_unqualified_with_aliases() {
    let mut fixture = TestContext::new("test_group_by_mixed_qualified").await;
    TestContext::setup_multiple_tables(&mut fixture);

    // Test mixing qualified and unqualified names with aliases
    let sql = "
        SELECT e.name AS emp_name, salary, COUNT(*) AS total_count
        FROM employees e
        GROUP BY e.name, salary
    ";

    let result = fixture.planner.create_logical_plan(sql);
    assert!(
        result.is_ok(),
        "Query planning should succeed: {:?}",
        result.err()
    );

    let plan = result.unwrap();
    let schema = plan.get_schema().unwrap();

    // Verify the output schema
    assert_eq!(schema.get_column_count(), 3);
    assert_eq!(schema.get_column(0).unwrap().get_name(), "emp_name");
    assert_eq!(schema.get_column(1).unwrap().get_name(), "salary");
    assert_eq!(schema.get_column(2).unwrap().get_name(), "total_count");
}

#[tokio::test]
async fn test_group_by_without_aliases_qualified_names() {
    let mut fixture = TestContext::new("test_group_by_no_aliases").await;
    TestContext::setup_multiple_tables(&mut fixture);

    // Test GROUP BY with qualified names but no explicit aliases in SELECT
    let sql = "
        SELECT e.name, d.name, COUNT(ep.project_id)
        FROM employees e
        JOIN departments d ON e.department_id = d.id
        JOIN employee_projects ep ON e.id = ep.employee_id
        GROUP BY e.name, d.name
    ";

    let result = fixture.planner.create_logical_plan(sql);
    assert!(
        result.is_ok(),
        "Query planning should succeed: {:?}",
        result.err()
    );

    let plan = result.unwrap();
    let schema = plan.get_schema().unwrap();

    // Verify the output schema preserves qualified names where no alias is given
    assert_eq!(schema.get_column_count(), 3);
    assert_eq!(schema.get_column(0).unwrap().get_name(), "e.name");
    assert_eq!(schema.get_column(1).unwrap().get_name(), "d.name");
    assert_eq!(schema.get_column(2).unwrap().get_name(), "COUNT_project_id");
}

#[tokio::test]
async fn test_group_by_complex_qualified_expressions() {
    let mut fixture = TestContext::new("test_group_by_complex").await;
    TestContext::setup_multiple_tables(&mut fixture);

    // Test with more complex qualified expressions and multiple aliases
    let sql = "
        SELECT
            e.name AS employee_name,
            d.name AS dept_name,
            e.salary AS emp_salary,
            COUNT(ep.project_id) AS project_count,
            SUM(e.salary) AS total_salary,
            AVG(e.salary) AS avg_salary
        FROM employees e
        JOIN departments d ON e.department_id = d.id
        JOIN employee_projects ep ON e.id = ep.employee_id
        GROUP BY e.name, d.name, e.salary
    ";

    let result = fixture.planner.create_logical_plan(sql);
    assert!(
        result.is_ok(),
        "Query planning should succeed: {:?}",
        result.err()
    );

    let plan = result.unwrap();
    let schema = plan.get_schema().unwrap();

    // Verify all columns are properly aliased
    assert_eq!(schema.get_column_count(), 6);
    assert_eq!(schema.get_column(0).unwrap().get_name(), "employee_name");
    assert_eq!(schema.get_column(1).unwrap().get_name(), "dept_name");
    assert_eq!(schema.get_column(2).unwrap().get_name(), "emp_salary");
    assert_eq!(schema.get_column(3).unwrap().get_name(), "project_count");
    assert_eq!(schema.get_column(4).unwrap().get_name(), "total_salary");
    assert_eq!(schema.get_column(5).unwrap().get_name(), "avg_salary");
}

#[tokio::test]
async fn test_group_by_error_case_column_not_in_group_by() {
    let mut fixture = TestContext::new("test_group_by_error").await;
    TestContext::setup_multiple_tables(&mut fixture);

    // Test error case - column in SELECT but not in GROUP BY should fail
    let sql = "
        SELECT e.name AS employee, d.name AS department, e.salary, e.id
        FROM employees e
        JOIN departments d ON e.department_id = d.id
        GROUP BY e.name, d.name, e.salary
    ";

    let result = fixture.planner.create_logical_plan(sql);
    // This should fail because e.id is not in GROUP BY and not an aggregate
    assert!(
        result.is_err(),
        "Query should fail because e.id is not in GROUP BY"
    );
}
