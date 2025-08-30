use tkdb::buffer::buffer_pool_manager_async::BufferPoolManager;
use tkdb::buffer::lru_k_replacer::LRUKReplacer;
use tkdb::catalog::catalog::Catalog;
use tkdb::sql::planner::logical_plan::LogicalPlanType;
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

        let disk_manager = AsyncDiskManager::new(db_path, log_path, DiskManagerConfig::default()).await.unwrap();
        let disk_manager_arc = Arc::new(disk_manager);
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
        let bpm = Arc::new(BufferPoolManager::new(BUFFER_POOL_SIZE, disk_manager_arc, replacer).unwrap());

        let transaction_manager = Arc::new(tkdb::concurrency::transaction_manager::TransactionManager::new());
        let catalog = Arc::new(RwLock::new(Catalog::new(bpm, transaction_manager.clone())));
        let planner = QueryPlanner::new(Arc::clone(&catalog));

        Self { catalog, planner, _temp_dir: temp_dir }
    }

    fn create_table(&mut self, name: &str, columns: &str) {
        let mut catalog = self.catalog.write();
        let schema = tkdb::catalog::schema::Schema::new(
            columns.split(',').map(|c| c.trim()).filter(|c| !c.is_empty()).map(|c| {
                let parts: Vec<&str> = c.split_whitespace().collect();
                let (col_name, ty) = (parts[0], parts[1].to_uppercase());
                let type_id = match ty.as_str() {
                    "INTEGER" => tkdb::types_db::type_id::TypeId::Integer,
                    "VARCHAR(255)" | "VARCHAR(50)" | "TEXT" | "VARCHAR" => tkdb::types_db::type_id::TypeId::VarChar,
                    _ => tkdb::types_db::type_id::TypeId::VarChar,
                };
                tkdb::catalog::column::Column::new(col_name, type_id)
            }).collect()
        );
        let _ = catalog.create_table(name.to_string(), schema);
    }
}

#[tokio::test]
async fn update_simple_set() {
    let mut ctx = TestContext::new("planner_update_simple").await;
    ctx.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER");
    let sql = "UPDATE users SET age = 30";
    let plan = ctx.planner.create_logical_plan(sql).unwrap();
    match &plan.plan_type {
        LogicalPlanType::Update { table_name, update_expressions, .. } => {
            assert_eq!(table_name, "users");
            assert_eq!(update_expressions.len(), 1);
        }
        _ => panic!("Expected Update plan"),
    }
}

#[tokio::test]
async fn update_with_where_clause() {
    let mut ctx = TestContext::new("planner_update_where").await;
    ctx.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER");
    let sql = "UPDATE users SET age = 25 WHERE id = 1";
    let plan = ctx.planner.create_logical_plan(sql).unwrap();
    match &plan.plan_type {
        LogicalPlanType::Update { table_name, update_expressions, .. } => {
            assert_eq!(table_name, "users");
            assert_eq!(update_expressions.len(), 1);
            match &plan.children[0].plan_type {
                LogicalPlanType::Filter { .. } => (),
                _ => panic!("Expected Filter child for Update"),
            }
        }
        _ => panic!("Expected Update plan"),
    }
}

#[tokio::test]
async fn update_multiple_columns() {
    let mut ctx = TestContext::new("planner_update_multi").await;
    ctx.create_table("users", "id INTEGER, name VARCHAR(255), dept VARCHAR(255), salary INTEGER");
    let sql = "UPDATE users SET salary = 60000, name = 'John'";
    let plan = ctx.planner.create_logical_plan(sql).unwrap();
    match &plan.plan_type {
        LogicalPlanType::Update { update_expressions, .. } => {
            assert_eq!(update_expressions.len(), 2);
        }
        _ => panic!("Expected Update plan"),
    }
}

#[tokio::test]
async fn test_update_with_arithmetic_expression() {
    let mut fixture = TestContext::new("update_arithmetic").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), salary INTEGER");

    let update_sql = "UPDATE users SET salary = salary + 1000";
    let plan = fixture.planner.create_logical_plan(update_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Update {
            table_name,
            schema,
            table_oid,
            update_expressions,
            ..
        } => {
            assert_eq!(table_name, "users");
            assert_eq!(schema.get_column_count(), 5);
            assert_eq!(*table_oid, 0);
            assert_eq!(update_expressions.len(), 1);

            match &plan.children[0].plan_type {
                LogicalPlanType::TableScan { table_name, .. } => {
                    assert_eq!(table_name, "users");
                }
                _ => panic!("Expected TableScan node as child of Update"),
            }
        }
        _ => panic!("Expected Update plan node"),
    }
}

#[tokio::test]
async fn test_update_with_complex_where() {
    let mut fixture = TestContext::new("update_complex_where").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER, department VARCHAR(255), salary INTEGER");

    let update_sql =
        "UPDATE users SET salary = 75000 WHERE age > 25 AND department = 'Engineering'";
    let plan = fixture.planner.create_logical_plan(update_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Update {
            table_name,
            schema,
            table_oid,
            update_expressions,
            ..
        } => {
            assert_eq!(table_name, "users");
            assert_eq!(schema.get_column_count(), 5);
            assert_eq!(*table_oid, 0);
            assert_eq!(update_expressions.len(), 1);

            match &plan.children[0].plan_type {
                LogicalPlanType::Filter { table_name, .. } => {
                    assert_eq!(table_name, "users");
                }
                _ => panic!("Expected Filter node as child of Update"),
            }
        }
        _ => panic!("Expected Update plan node"),
    }
}

#[tokio::test]
async fn test_update_nonexistent_table() {
    let mut fixture = TestContext::new("update_nonexistent").await;

    let update_sql = "UPDATE nonexistent_table SET age = 30";
    let result = fixture.planner.create_logical_plan(update_sql);

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("not found"));
}

#[tokio::test]
async fn test_update_nonexistent_column() {
    let mut fixture = TestContext::new("update_nonexistent_column").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), age INTEGER");

    let update_sql = "UPDATE users SET nonexistent_column = 30";
    let result = fixture.planner.create_logical_plan(update_sql);

    assert!(result.is_err());
}

#[tokio::test]
async fn test_update_with_null() {
    let mut fixture = TestContext::new("update_with_null").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), salary INTEGER, age INTEGER, department VARCHAR(255)");

    let update_sql = "UPDATE users SET name = NULL WHERE id = 1";
    let plan = fixture.planner.create_logical_plan(update_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Update {
            table_name,
            schema,
            table_oid,
            update_expressions,
            ..
        } => {
            assert_eq!(table_name, "users");
            assert_eq!(schema.get_column_count(), 5);
            assert_eq!(*table_oid, 0);
            assert_eq!(update_expressions.len(), 1);

            match &plan.children[0].plan_type {
                LogicalPlanType::Filter { table_name, .. } => {
                    assert_eq!(table_name, "users");
                }
                _ => panic!("Expected Filter node as child of Update"),
            }
        }
        _ => panic!("Expected Update plan node"),
    }
}

#[tokio::test]
async fn test_update_with_string_literal() {
    let mut fixture = TestContext::new("update_string_literal").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), salary INTEGER, age INTEGER, department VARCHAR(255)");

    let update_sql = "UPDATE users SET name = 'John Doe', department = 'Marketing'";
    let plan = fixture.planner.create_logical_plan(update_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Update {
            table_name,
            schema,
            table_oid,
            update_expressions,
            ..
        } => {
            assert_eq!(table_name, "users");
            assert_eq!(schema.get_column_count(), 5);
            assert_eq!(*table_oid, 0);
            assert_eq!(update_expressions.len(), 2);

            match &plan.children[0].plan_type {
                LogicalPlanType::TableScan { table_name, .. } => {
                    assert_eq!(table_name, "users");
                }
                _ => panic!("Expected TableScan node as child of Update"),
            }
        }
        _ => panic!("Expected Update plan node"),
    }
}

#[tokio::test]
async fn test_update_with_column_reference() {
    let mut fixture = TestContext::new("update_column_reference").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), salary INTEGER, age INTEGER, department VARCHAR(255)");

    let update_sql = "UPDATE users SET salary = age * 1000";
    let plan = fixture.planner.create_logical_plan(update_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Update {
            table_name,
            schema,
            table_oid,
            update_expressions,
            ..
        } => {
            assert_eq!(table_name, "users");
            assert_eq!(schema.get_column_count(), 5);
            assert_eq!(*table_oid, 0);
            assert_eq!(update_expressions.len(), 1);

            match &plan.children[0].plan_type {
                LogicalPlanType::TableScan { table_name, .. } => {
                    assert_eq!(table_name, "users");
                }
                _ => panic!("Expected TableScan node as child of Update"),
            }
        }
        _ => panic!("Expected Update plan node"),
    }
}

#[tokio::test]
async fn test_update_different_data_types() {
    let mut fixture = TestContext::new("update_different_types").await;

    // Create a table with various data types
    fixture
        .create_table(
            "mixed_types",
            "id INTEGER, name VARCHAR(255), age INTEGER, salary DECIMAL, active BOOLEAN",
        );

    let update_sql =
        "UPDATE mixed_types SET name = 'Alice', age = 25, salary = 50000.50, active = true";
    let plan = fixture.planner.create_logical_plan(update_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Update {
            table_name,
            schema,
            table_oid,
            update_expressions,
            ..
        } => {
            assert_eq!(table_name, "mixed_types");
            assert_eq!(schema.get_column_count(), 5);
            assert_eq!(*table_oid, 0);
            assert_eq!(update_expressions.len(), 4);

            match &plan.children[0].plan_type {
                LogicalPlanType::TableScan { table_name, .. } => {
                    assert_eq!(table_name, "mixed_types");
                }
                _ => panic!("Expected TableScan node as child of Update"),
            }
        }
        _ => panic!("Expected Update plan node"),
    }
}

#[tokio::test]
async fn test_update_with_subexpression() {
    let mut fixture = TestContext::new("update_subexpression").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), salary INTEGER, age INTEGER, department VARCHAR(255)");

    let update_sql = "UPDATE users SET salary = (age + 5) * 1000 WHERE id = 1";
    let plan = fixture.planner.create_logical_plan(update_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Update {
            table_name,
            schema,
            table_oid,
            update_expressions,
            ..
        } => {
            assert_eq!(table_name, "users");
            assert_eq!(schema.get_column_count(), 5);
            assert_eq!(*table_oid, 0);
            assert_eq!(update_expressions.len(), 1);

            match &plan.children[0].plan_type {
                LogicalPlanType::Filter { table_name, .. } => {
                    assert_eq!(table_name, "users");
                }
                _ => panic!("Expected Filter node as child of Update"),
            }
        }
        _ => panic!("Expected Update plan node"),
    }
}

#[tokio::test]
async fn test_update_single_column_table() {
    let mut fixture = TestContext::new("update_single_column").await;

    fixture
        .create_table("single_col", "value INTEGER");

    let update_sql = "UPDATE single_col SET value = 42";
    let plan = fixture.planner.create_logical_plan(update_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Update {
            table_name,
            schema,
            table_oid,
            update_expressions,
            ..
        } => {
            assert_eq!(table_name, "single_col");
            assert_eq!(schema.get_column_count(), 1);
            assert_eq!(*table_oid, 0);
            assert_eq!(update_expressions.len(), 1);

            match &plan.children[0].plan_type {
                LogicalPlanType::TableScan { table_name, .. } => {
                    assert_eq!(table_name, "single_col");
                }
                _ => panic!("Expected TableScan node as child of Update"),
            }
        }
        _ => panic!("Expected Update plan node"),
    }
}

#[tokio::test]
async fn test_update_with_comparison_in_where() {
    let mut fixture = TestContext::new("update_comparison_where").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), salary INTEGER, age INTEGER, department VARCHAR(255)");

    let test_cases = vec![
        "UPDATE users SET age = 30 WHERE id > 5",
        "UPDATE users SET age = 30 WHERE id < 10",
        "UPDATE users SET age = 30 WHERE id >= 5",
        "UPDATE users SET age = 30 WHERE id <= 10",
        "UPDATE users SET age = 30 WHERE id != 5",
        "UPDATE users SET age = 30 WHERE name = 'John'",
    ];

    for update_sql in test_cases {
        let plan = fixture
            .planner
            .create_logical_plan(update_sql)
            .unwrap_or_else(|e| {
                panic!("Failed to create plan for: {}, error: {}", update_sql, e)
            });

        match &plan.plan_type {
            LogicalPlanType::Update {
                table_name,
                schema,
                table_oid,
                update_expressions,
                ..
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(schema.get_column_count(), 5);
                assert_eq!(*table_oid, 0);
                assert_eq!(update_expressions.len(), 1);

                match &plan.children[0].plan_type {
                    LogicalPlanType::Filter { table_name, .. } => {
                        assert_eq!(table_name, "users");
                    }
                    _ => panic!(
                        "Expected Filter node as child of Update for: {}",
                        update_sql
                    ),
                }
            }
            _ => panic!("Expected Update plan node for: {}", update_sql),
        }
    }
}

#[tokio::test]
async fn test_update_with_logical_operators() {
    let mut fixture = TestContext::new("update_logical_operators").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), salary INTEGER, age INTEGER, department VARCHAR(255)");

    let test_cases = vec![
        "UPDATE users SET salary = 60000 WHERE age > 25 AND department = 'Engineering'",
        "UPDATE users SET salary = 60000 WHERE age < 30 OR department = 'Sales'",
        "UPDATE users SET salary = 60000 WHERE NOT (age < 25)",
    ];

    for update_sql in test_cases {
        let plan = fixture
            .planner
            .create_logical_plan(update_sql)
            .unwrap_or_else(|e| {
                panic!("Failed to create plan for: {}, error: {}", update_sql, e)
            });

        match &plan.plan_type {
            LogicalPlanType::Update {
                table_name,
                schema,
                table_oid,
                update_expressions,
                ..
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(schema.get_column_count(), 5);
                assert_eq!(*table_oid, 0);
                assert_eq!(update_expressions.len(), 1);

                match &plan.children[0].plan_type {
                    LogicalPlanType::Filter { table_name, .. } => {
                        assert_eq!(table_name, "users");
                    }
                    _ => panic!(
                        "Expected Filter node as child of Update for: {}",
                        update_sql
                    ),
                }
            }
            _ => panic!("Expected Update plan node for: {}", update_sql),
        }
    }
}

#[tokio::test]
async fn test_update_with_parentheses_in_expression() {
    let mut fixture = TestContext::new("update_parentheses").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), salary INTEGER, age INTEGER, department VARCHAR(255)");

    let update_sql =
        "UPDATE users SET salary = (age * 1000) + 5000 WHERE (id > 1 AND id < 10)";
    let plan = fixture.planner.create_logical_plan(update_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Update {
            table_name,
            schema,
            table_oid,
            update_expressions,
            ..
        } => {
            assert_eq!(table_name, "users");
            assert_eq!(schema.get_column_count(), 5);
            assert_eq!(*table_oid, 0);
            assert_eq!(update_expressions.len(), 1);

            match &plan.children[0].plan_type {
                LogicalPlanType::Filter { table_name, .. } => {
                    assert_eq!(table_name, "users");
                }
                _ => panic!("Expected Filter node as child of Update"),
            }
        }
        _ => panic!("Expected Update plan node"),
    }
}

#[tokio::test]
async fn test_update_all_columns() {
    let mut fixture = TestContext::new("update_all_columns").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), salary INTEGER, age INTEGER, department VARCHAR(255)");

    let update_sql = "UPDATE users SET id = 1, name = 'Updated', age = 35, department = 'IT', salary = 80000";
    let plan = fixture.planner.create_logical_plan(update_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Update {
            table_name,
            schema,
            table_oid,
            update_expressions,
            ..
        } => {
            assert_eq!(table_name, "users");
            assert_eq!(schema.get_column_count(), 5);
            assert_eq!(*table_oid, 0);
            assert_eq!(update_expressions.len(), 5); // All columns being updated

            match &plan.children[0].plan_type {
                LogicalPlanType::TableScan { table_name, .. } => {
                    assert_eq!(table_name, "users");
                }
                _ => panic!("Expected TableScan node as child of Update"),
            }
        }
        _ => panic!("Expected Update plan node"),
    }
}

#[tokio::test]
async fn test_update_same_column_multiple_times() {
    let mut fixture = TestContext::new("update_same_column").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255), salary INTEGER, age INTEGER, department VARCHAR(255)");

    // SQL that tries to update the same column multiple times
    // This should be parsed successfully (whether it's logically valid is a different matter)
    let update_sql = "UPDATE users SET age = 25, age = 30";
    let plan = fixture.planner.create_logical_plan(update_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Update {
            table_name,
            schema,
            table_oid,
            update_expressions,
            ..
        } => {
            assert_eq!(table_name, "users");
            assert_eq!(schema.get_column_count(), 5);
            assert_eq!(*table_oid, 0);
            assert_eq!(update_expressions.len(), 2); // Two assignments

            match &plan.children[0].plan_type {
                LogicalPlanType::TableScan { table_name, .. } => {
                    assert_eq!(table_name, "users");
                }
                _ => panic!("Expected TableScan node as child of Update"),
            }
        }
        _ => panic!("Expected Update plan node"),
    }
}
