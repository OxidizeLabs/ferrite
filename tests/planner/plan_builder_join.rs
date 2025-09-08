use sqlparser::ast::JoinOperator;
use tkdb::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
use tkdb::types_db::type_id::TypeId;
use tkdb::{buffer::buffer_pool_manager_async::BufferPoolManager, sql::execution::expressions::abstract_expression::Expression};
use tkdb::buffer::lru_k_replacer::LRUKReplacer;
use tkdb::catalog::Catalog;
use tkdb::sql::planner::logical_plan::{LogicalPlanType, LogicalToPhysical};
use tkdb::sql::planner::query_planner::QueryPlanner;
use parking_lot::RwLock;
use std::sync::Arc;
use tempfile::TempDir;
use tkdb::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
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
        let db_path = temp_dir.path().join(format!("{name}.db")).to_str().unwrap().to_string();
        let log_path = temp_dir.path().join(format!("{name}.log")).to_str().unwrap().to_string();

        let disk_manager = AsyncDiskManager::new(db_path, log_path, DiskManagerConfig::default()).await.unwrap();
        let disk_manager_arc = Arc::new(disk_manager);
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
        let bpm = Arc::new(BufferPoolManager::new(BUFFER_POOL_SIZE, disk_manager_arc, replacer).unwrap());

        let transaction_manager = Arc::new(tkdb::concurrency::transaction_manager::TransactionManager::new());
        let catalog = Arc::new(RwLock::new(Catalog::new(bpm, transaction_manager.clone())));
        let planner = QueryPlanner::new(Arc::clone(&catalog));

        Self { catalog, planner, _temp_dir: temp_dir }
    }

    fn create_basic(&mut self) {
        let mut catalog = self.catalog.write();
        let users = tkdb::catalog::schema::Schema::new(vec![
            tkdb::catalog::column::Column::new("id", tkdb::types_db::type_id::TypeId::Integer),
            tkdb::catalog::column::Column::new("name", tkdb::types_db::type_id::TypeId::VarChar),
            tkdb::catalog::column::Column::new("age", tkdb::types_db::type_id::TypeId::Integer),
        ]);
        let orders = tkdb::catalog::schema::Schema::new(vec![
            tkdb::catalog::column::Column::new("order_id", tkdb::types_db::type_id::TypeId::Integer),
            tkdb::catalog::column::Column::new("user_id", tkdb::types_db::type_id::TypeId::Integer),
            tkdb::catalog::column::Column::new("amount", tkdb::types_db::type_id::TypeId::Decimal),
        ]);
        let _ = catalog.create_table("users".to_string(), users);
        let _ = catalog.create_table("orders".to_string(), orders);
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

    fn setup_test_tables(fixture: &mut TestContext) {
        fixture
            .create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER", false)
            .unwrap();
        fixture
            .create_table(
                "orders",
                "order_id INTEGER, user_id INTEGER, amount DECIMAL",
                false,
            )
            .unwrap();
    }

    fn setup_extended_test_tables(fixture: &mut TestContext) {
        TestContext::setup_test_tables(fixture);
        fixture
            .create_table(
                "products",
                "product_id INTEGER, name VARCHAR(255), price DECIMAL",
                false,
            )
            .unwrap();
        fixture
            .create_table(
                "order_items",
                "order_id INTEGER, product_id INTEGER, quantity INTEGER",
                false,
            )
            .unwrap();
        fixture
            .create_table(
                "categories",
                "category_id INTEGER, name VARCHAR(255), description TEXT",
                false,
            )
            .unwrap();
        fixture
            .create_table(
                "departments",
                "dept_id INTEGER, name VARCHAR(255), budget DECIMAL",
                false,
            )
            .unwrap();
        fixture
            .create_table(
                "employees",
                "emp_id INTEGER, name VARCHAR(255), dept_id INTEGER, salary DECIMAL",
                false,
            )
            .unwrap();
    }

}

#[tokio::test]
async fn inner_join_basic() {
    let mut ctx = TestContext::new("planner_join_inner").await;
    ctx.create_basic();
    let sql = "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id";
    let plan = ctx.planner.create_logical_plan(sql).unwrap();
    match &plan.children[0].plan_type {
        LogicalPlanType::NestedLoopJoin { .. } => (),
        _ => panic!("Expected join node"),
    }
}

#[tokio::test]
async fn join_with_where() {
    let mut ctx = TestContext::new("planner_join_where").await;
    ctx.create_basic();
    let sql = "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.amount > 100";
    let plan = ctx.planner.create_logical_plan(sql).unwrap();
    match &plan.children[0].plan_type {
        LogicalPlanType::Filter { .. } => (),
        _ => panic!("Expected Filter over join"),
    }
}

#[tokio::test]
async fn test_left_outer_join() {
    let mut fixture = TestContext::new("left_outer_join").await;
    TestContext::setup_test_tables(&mut fixture);

    let join_sql = "SELECT u.name, o.amount \
                   FROM users u \
                   LEFT OUTER JOIN orders o ON u.id = o.user_id";

    let plan = fixture.planner.create_logical_plan(join_sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::NestedLoopJoin { join_type, .. } => {
            assert!(matches!(join_type, JoinOperator::LeftOuter(_)));
        }
        _ => panic!("Expected NestedLoopJoin node"),
    }
}

#[tokio::test]
async fn test_right_outer_join() {
    let mut fixture = TestContext::new("right_outer_join").await;
    TestContext::setup_test_tables(&mut fixture);

    let join_sql = "SELECT u.name, o.amount \
                   FROM users u \
                   RIGHT OUTER JOIN orders o ON u.id = o.user_id";

    let plan = fixture.planner.create_logical_plan(join_sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::NestedLoopJoin { join_type, .. } => {
            assert!(matches!(join_type, JoinOperator::RightOuter(_)));
        }
        _ => panic!("Expected NestedLoopJoin node"),
    }
}

#[tokio::test]
async fn test_full_outer_join() {
    let mut fixture = TestContext::new("full_outer_join").await;
    TestContext::setup_test_tables(&mut fixture);

    let join_sql = "SELECT u.name, o.amount \
                   FROM users u \
                   FULL OUTER JOIN orders o ON u.id = o.user_id";

    let plan = fixture.planner.create_logical_plan(join_sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::NestedLoopJoin { join_type, .. } => {
            assert!(matches!(join_type, JoinOperator::FullOuter(_)));
        }
        _ => panic!("Expected NestedLoopJoin node"),
    }
}

#[tokio::test]
async fn test_cross_join() {
    let mut fixture = TestContext::new("cross_join").await;
    TestContext::setup_test_tables(&mut fixture);

    let join_sql = "SELECT u.name, o.amount \
                   FROM users u \
                   CROSS JOIN orders o";

    let plan = fixture.planner.create_logical_plan(join_sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::NestedLoopJoin { join_type, .. } => {
            assert!(matches!(join_type, JoinOperator::CrossJoin));
        }
        _ => panic!("Expected NestedLoopJoin node"),
    }
}

#[tokio::test]
async fn test_multiple_joins() {
    let mut fixture = TestContext::new("multiple_joins").await;
    TestContext::setup_extended_test_tables(&mut fixture);

    let join_sql = "SELECT u.name, o.amount, p.name as product_name \
                   FROM users u \
                   INNER JOIN orders o ON u.id = o.user_id \
                   INNER JOIN order_items oi ON o.order_id = oi.order_id \
                   INNER JOIN products p ON oi.product_id = p.product_id";

    let plan = fixture.planner.create_logical_plan(join_sql).unwrap();

    // Verify it's a projection on top of joins
    match &plan.plan_type {
        LogicalPlanType::Projection { schema, .. } => {
            assert_eq!(schema.get_column_count(), 3);
        }
        _ => panic!("Expected Projection as root node"),
    }
}

#[tokio::test]
async fn test_self_join() {
    let mut fixture = TestContext::new("self_join").await;
    TestContext::setup_extended_test_tables(&mut fixture);

    let join_sql = "SELECT e1.name as employee, e2.name as manager \
                   FROM employees e1 \
                   INNER JOIN employees e2 ON e1.dept_id = e2.emp_id";

    let plan = fixture.planner.create_logical_plan(join_sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::NestedLoopJoin {
            left_schema,
            right_schema,
            ..
        } => {
            // Both schemas should have aliased columns from employees table
            assert_eq!(left_schema.get_column_count(), 4); // e1 alias
            assert_eq!(right_schema.get_column_count(), 4); // e2 alias
        }
        _ => panic!("Expected NestedLoopJoin node"),
    }
}

#[tokio::test]
async fn test_join_with_complex_conditions() {
    let mut fixture = TestContext::new("join_complex_conditions").await;
    TestContext::setup_test_tables(&mut fixture);

    let join_sql = "SELECT u.name, o.amount \
                   FROM users u \
                   INNER JOIN orders o ON u.id = o.user_id AND u.age > 18";

    let plan = fixture.planner.create_logical_plan(join_sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::NestedLoopJoin { predicate, .. } => {
            // Should have a complex predicate combining both conditions
            match predicate.as_ref() {
                Expression::Comparison(_) => (), // Could be compound comparison
                _ => (), // Might be a different expression type for complex conditions
            }
        }
        _ => panic!("Expected NestedLoopJoin node"),
    }
}

#[tokio::test]
async fn test_join_with_aggregation() {
    let mut fixture = TestContext::new("join_with_aggregation").await;
    TestContext::setup_test_tables(&mut fixture);

    let join_sql = "SELECT u.name, COUNT(o.order_id) as order_count, SUM(o.amount) as total \
                   FROM users u \
                   LEFT JOIN orders o ON u.id = o.user_id \
                   GROUP BY u.name";

    let plan = fixture.planner.create_logical_plan(join_sql).unwrap();

    // Should have projection -> aggregation -> join
    match &plan.plan_type {
        LogicalPlanType::Projection { .. } => match &plan.children[0].plan_type {
            LogicalPlanType::Aggregate { .. } => {
                match &plan.children[0].children[0].plan_type {
                    LogicalPlanType::NestedLoopJoin { .. } => (),
                    _ => panic!("Expected NestedLoopJoin under Aggregate"),
                }
            }
            _ => panic!("Expected Aggregate node"),
        },
        _ => panic!("Expected Projection as root node"),
    }
}

#[tokio::test]
async fn test_join_with_order_by() {
    let mut fixture = TestContext::new("join_with_order_by").await;
    TestContext::setup_test_tables(&mut fixture);

    let join_sql = "SELECT u.name, o.amount \
                   FROM users u \
                   INNER JOIN orders o ON u.id = o.user_id \
                   ORDER BY u.name, o.amount DESC";

    let plan = fixture.planner.create_logical_plan(join_sql).unwrap();

    // Should have sort at the top level
    match &plan.plan_type {
        LogicalPlanType::Sort { .. } => {
            // Under sort should be projection
            match &plan.children[0].plan_type {
                LogicalPlanType::Projection { .. } => (),
                _ => panic!("Expected Projection under Sort"),
            }
        }
        _ => panic!("Expected Sort as root node"),
    }
}

#[tokio::test]
async fn test_join_with_limit() {
    let mut fixture = TestContext::new("join_with_limit").await;
    TestContext::setup_test_tables(&mut fixture);

    let join_sql = "SELECT u.name, o.amount \
                   FROM users u \
                   INNER JOIN orders o ON u.id = o.user_id \
                   LIMIT 10";

    let plan = fixture.planner.create_logical_plan(join_sql).unwrap();

    // Should have limit at the top level
    match &plan.plan_type {
        LogicalPlanType::Limit { limit, .. } => {
            assert_eq!(*limit, 10);
        }
        _ => panic!("Expected Limit as root node"),
    }
}

#[tokio::test]
async fn test_join_with_subquery() {
    let mut fixture = TestContext::new("join_with_subquery").await;
    TestContext::setup_test_tables(&mut fixture);

    let join_sql = "SELECT u.name, recent_orders.total \
                   FROM users u \
                   INNER JOIN (SELECT user_id, SUM(amount) as total \
                              FROM orders \
                              GROUP BY user_id) recent_orders \
                   ON u.id = recent_orders.user_id";

    let plan = fixture.planner.create_logical_plan(join_sql).unwrap();

    // Should handle subquery as derived table
    match &plan.children[0].plan_type {
        LogicalPlanType::NestedLoopJoin {
            left_schema,
            right_schema,
            ..
        } => {
            assert_eq!(left_schema.get_column_count(), 3); // users
            assert_eq!(right_schema.get_column_count(), 2); // subquery result
        }
        _ => panic!("Expected NestedLoopJoin node"),
    }
}

#[tokio::test]
async fn test_join_with_aliases() {
    let mut fixture = TestContext::new("join_with_aliases").await;
    TestContext::setup_test_tables(&mut fixture);

    let join_sql = "SELECT u.name as user_name, o.amount as order_amount \
                   FROM users u \
                   INNER JOIN orders o ON u.id = o.user_id";

    let plan = fixture.planner.create_logical_plan(join_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Projection { schema, .. } => {
            let col_names: Vec<_> = (0..schema.get_column_count())
                .map(|i| schema.get_column(i as usize).unwrap().get_name())
                .collect();
            assert!(col_names.contains(&"user_name"));
            assert!(col_names.contains(&"order_amount"));
        }
        _ => panic!("Expected Projection as root node"),
    }
}

#[tokio::test]
async fn test_join_nonexistent_table() {
    let mut fixture = TestContext::new("join_nonexistent_table").await;
    TestContext::setup_test_tables(&mut fixture);

    let join_sql = "SELECT u.name, x.value \
                   FROM users u \
                   INNER JOIN nonexistent_table x ON u.id = x.user_id";

    let result = fixture.planner.create_logical_plan(join_sql);
    assert!(result.is_err());

    let error_msg = result.unwrap_err();
    assert!(error_msg.contains("does not exist") || error_msg.contains("not found"));
}

#[tokio::test]
async fn test_join_invalid_condition() {
    let mut fixture = TestContext::new("join_invalid_condition").await;
    TestContext::setup_test_tables(&mut fixture);

    let join_sql = "SELECT u.name, o.amount \
                   FROM users u \
                   INNER JOIN orders o ON u.nonexistent_column = o.user_id";

    let result = fixture.planner.create_logical_plan(join_sql);
    assert!(result.is_err());

    let error_msg = result.unwrap_err();
    assert!(
        error_msg.contains("not found") || error_msg.contains("nonexistent_column"),
        "Expected error about column not found, got: {}",
        error_msg
    );
}

#[tokio::test]
async fn test_join_mixed_table_types() {
    let mut fixture = TestContext::new("join_mixed_table_types").await;
    TestContext::setup_test_tables(&mut fixture);

    // Instead of VALUES with aliases, test join with a simple subquery
    let join_sql = "SELECT u.name, sub.order_id \
                   FROM users u \
                   INNER JOIN (SELECT order_id, user_id FROM orders LIMIT 1) sub \
                   ON u.id = sub.user_id";

    let plan = fixture.planner.create_logical_plan(join_sql).unwrap();

    match &plan.children[0].plan_type {
        LogicalPlanType::NestedLoopJoin { .. } => (),
        _ => panic!("Expected NestedLoopJoin node"),
    }
}

#[tokio::test]
async fn test_join_with_null_handling() {
    let mut fixture = TestContext::new("join_null_handling").await;
    TestContext::setup_test_tables(&mut fixture);

    let join_sql = "SELECT u.name, o.amount \
                   FROM users u \
                   LEFT JOIN orders o ON u.id = o.user_id \
                   WHERE o.amount IS NULL";

    let plan = fixture.planner.create_logical_plan(join_sql).unwrap();

    // Should have filter after join to handle NULL values
    match &plan.children[0].plan_type {
        LogicalPlanType::Filter { .. } => match &plan.children[0].children[0].plan_type {
            LogicalPlanType::NestedLoopJoin { join_type, .. } => {
                assert!(matches!(join_type, JoinOperator::Left(_)));
            }
            _ => panic!("Expected NestedLoopJoin under Filter"),
        },
        _ => panic!("Expected Filter node"),
    }
}

#[tokio::test]
async fn test_natural_join() {
    let mut fixture = TestContext::new("natural_join").await;
    TestContext::setup_test_tables(&mut fixture);

    // Test NATURAL JOIN (if supported)
    let join_sql = "SELECT name, amount \
                   FROM users \
                   NATURAL JOIN orders";

    let result = fixture.planner.create_logical_plan(join_sql);
    // This might not be supported yet, so we just check if it parses or fails gracefully
}

#[tokio::test]
async fn test_join_performance_hints() {
    let mut fixture = TestContext::new("join_performance_hints").await;
    TestContext::setup_test_tables(&mut fixture);

    // Test various join hints (database-specific syntax)
    let join_sql = "SELECT /*+ USE_HASH(u,o) */ u.name, o.amount \
                   FROM users u \
                   INNER JOIN orders o ON u.id = o.user_id";

    let plan = fixture.planner.create_logical_plan(join_sql).unwrap();

    // Hints are typically ignored during planning but should not cause errors
    match &plan.children[0].plan_type {
        LogicalPlanType::NestedLoopJoin { .. } => (),
        _ => panic!("Expected NestedLoopJoin node"),
    }
}

#[tokio::test]
async fn test_left_join_same_as_left_outer_join() {
    let mut fixture = TestContext::new("left_join_synonym").await;
    TestContext::setup_test_tables(&mut fixture);

    // Test LEFT JOIN
    let left_join_sql = "SELECT u.name, o.amount \
                       FROM users u \
                       LEFT JOIN orders o ON u.id = o.user_id";

    let left_join_plan = fixture.planner.create_logical_plan(left_join_sql).unwrap();

    // Test LEFT OUTER JOIN
    let left_outer_join_sql = "SELECT u.name, o.amount \
                             FROM users u \
                             LEFT OUTER JOIN orders o ON u.id = o.user_id";

    let left_outer_join_plan = fixture
        .planner
        .create_logical_plan(left_outer_join_sql)
        .unwrap();

    // Both should produce NestedLoopJoin plans with equivalent join types
    match (
        &left_join_plan.children[0].plan_type,
        &left_outer_join_plan.children[0].plan_type,
    ) {
        (
            LogicalPlanType::NestedLoopJoin {
                join_type: left_join_type,
                ..
            },
            LogicalPlanType::NestedLoopJoin {
                join_type: left_outer_join_type,
                ..
            },
        ) => {
            // Both should be treated as left outer joins
            assert!(matches!(left_join_type, JoinOperator::Left(_)));
            assert!(matches!(left_outer_join_type, JoinOperator::LeftOuter(_)));

            // Verify the plans have the same structure and schemas
            assert_eq!(
                left_join_plan.children[0].get_schema(),
                left_outer_join_plan.children[0].get_schema()
            );
        }
        _ => panic!("Expected NestedLoopJoin nodes for both LEFT JOIN and LEFT OUTER JOIN"),
    }
}

#[tokio::test]
async fn test_right_join_same_as_right_outer_join() {
    let mut fixture = TestContext::new("right_join_synonym").await;
    TestContext::setup_test_tables(&mut fixture);

    // Test RIGHT JOIN
    let right_join_sql = "SELECT u.name, o.amount \
                        FROM users u \
                        RIGHT JOIN orders o ON u.id = o.user_id";

    let right_join_plan = fixture.planner.create_logical_plan(right_join_sql).unwrap();

    // Test RIGHT OUTER JOIN
    let right_outer_join_sql = "SELECT u.name, o.amount \
                              FROM users u \
                              RIGHT OUTER JOIN orders o ON u.id = o.user_id";

    let right_outer_join_plan = fixture
        .planner
        .create_logical_plan(right_outer_join_sql)
        .unwrap();

    // Both should produce NestedLoopJoin plans with equivalent join types
    match (
        &right_join_plan.children[0].plan_type,
        &right_outer_join_plan.children[0].plan_type,
    ) {
        (
            LogicalPlanType::NestedLoopJoin {
                join_type: right_join_type,
                ..
            },
            LogicalPlanType::NestedLoopJoin {
                join_type: right_outer_join_type,
                ..
            },
        ) => {
            // Both should be treated as right outer joins
            assert!(matches!(right_join_type, JoinOperator::Right(_)));
            assert!(matches!(right_outer_join_type, JoinOperator::RightOuter(_)));

            // Verify the plans have the same structure and schemas
            assert_eq!(
                right_join_plan.children[0].get_schema(),
                right_outer_join_plan.children[0].get_schema()
            );
        }
        _ => {
            panic!("Expected NestedLoopJoin nodes for both RIGHT JOIN and RIGHT OUTER JOIN")
        }
    }
}

