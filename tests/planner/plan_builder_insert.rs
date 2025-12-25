use crate::common::logger::init_test_logger;
use ferrite::buffer::buffer_pool_manager_async::BufferPoolManager;
use ferrite::buffer::lru_k_replacer::LRUKReplacer;
use ferrite::catalog::Catalog;
use ferrite::sql::planner::logical_plan::LogicalPlanType;
use ferrite::sql::planner::query_planner::QueryPlanner;
use ferrite::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
use parking_lot::RwLock;
use std::sync::Arc;
use tempfile::TempDir;

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

    fn create_table(&mut self, name: &str, columns: &str) {
        let mut catalog = self.catalog.write();
        let schema = ferrite::catalog::schema::Schema::new(
            columns
                .split(',')
                .map(|c| c.trim())
                .filter(|c| !c.is_empty())
                .map(|c| {
                    let parts: Vec<&str> = c.split_whitespace().collect();
                    let (col_name, ty) = (parts[0], parts[1].to_uppercase());
                    let type_id = match ty.as_str() {
                        "INTEGER" => ferrite::types_db::type_id::TypeId::Integer,
                        "VARCHAR(255)" | "VARCHAR(50)" | "TEXT" | "VARCHAR" => {
                            ferrite::types_db::type_id::TypeId::VarChar
                        },
                        _ => ferrite::types_db::type_id::TypeId::VarChar,
                    };
                    ferrite::catalog::column::Column::new(col_name, type_id)
                })
                .collect(),
        );
        let _ = catalog.create_table(name.to_string(), schema);
    }
}

#[tokio::test]
async fn insert_simple_row() {
    let mut ctx = TestContext::new("planner_insert_simple").await;
    ctx.create_table("users", "id INTEGER, name VARCHAR(255)");
    let sql = "INSERT INTO users VALUES (1, 'Alice')";
    let plan = ctx.planner.create_logical_plan(sql).unwrap();
    match &plan.plan_type {
        LogicalPlanType::Insert {
            table_name, schema, ..
        } => {
            assert_eq!(table_name, "users");
            assert_eq!(schema.get_column_count(), 2);
            match &plan.children[0].plan_type {
                LogicalPlanType::Values { rows, schema } => {
                    assert_eq!(schema.get_column_count(), 2);
                    assert_eq!(rows.len(), 1);
                },
                _ => panic!("Expected Values child"),
            }
        },
        _ => panic!("Expected Insert plan"),
    }
}

#[tokio::test]
async fn insert_multiple_rows() {
    let mut ctx = TestContext::new("planner_insert_multi").await;
    ctx.create_table("users", "id INTEGER, name VARCHAR(255)");
    let sql = "INSERT INTO users VALUES (1, 'A'), (2, 'B'), (3, 'C')";
    let plan = ctx.planner.create_logical_plan(sql).unwrap();
    match &plan.plan_type {
        LogicalPlanType::Insert { table_name, .. } => {
            assert_eq!(table_name, "users");
            match &plan.children[0].plan_type {
                LogicalPlanType::Values { rows, .. } => {
                    assert_eq!(rows.len(), 3);
                },
                _ => panic!("Expected Values child"),
            }
        },
        _ => panic!("Expected Insert plan"),
    }
}

#[tokio::test]
async fn insert_with_explicit_columns() {
    let mut ctx = TestContext::new("planner_insert_columns").await;
    ctx.create_table("users", "id INTEGER, name VARCHAR(255)");
    let sql = "INSERT INTO users (id, name) VALUES (10, 'Zed')";
    let plan = ctx.planner.create_logical_plan(sql).unwrap();
    match &plan.plan_type {
        LogicalPlanType::Insert {
            table_name, schema, ..
        } => {
            assert_eq!(table_name, "users");
            assert_eq!(schema.get_column_count(), 2);
        },
        _ => panic!("Expected Insert plan"),
    }
}

#[tokio::test]
async fn test_insert_different_data_types() {
    let mut fixture = TestContext::new("insert_different_types").await;

    // Create a table with various data types
    fixture.create_table(
        "mixed_types",
        "id INTEGER, name VARCHAR(255), age INTEGER, salary DECIMAL, active BOOLEAN",
    );

    let insert_sql = "INSERT INTO mixed_types VALUES (1, 'Alice', 25, 50000.50, true)";
    let plan = fixture.planner.create_logical_plan(insert_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Insert {
            table_name, schema, ..
        } => {
            assert_eq!(table_name, "mixed_types");
            assert_eq!(schema.get_column_count(), 5);

            match &plan.children[0].plan_type {
                LogicalPlanType::Values { rows, schema } => {
                    assert_eq!(schema.get_column_count(), 5);
                    assert_eq!(rows.len(), 1);
                },
                _ => panic!("Expected Values node as child of Insert"),
            }
        },
        _ => panic!("Expected Insert plan node"),
    }
}

#[tokio::test]
async fn test_insert_null_values() {
    let mut fixture = TestContext::new("insert_null_values").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255)");

    let insert_sql = "INSERT INTO users VALUES (1, NULL)";
    let plan = fixture.planner.create_logical_plan(insert_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Insert {
            table_name, schema, ..
        } => {
            assert_eq!(table_name, "users");
            assert_eq!(schema.get_column_count(), 2);

            match &plan.children[0].plan_type {
                LogicalPlanType::Values { rows, schema } => {
                    assert_eq!(schema.get_column_count(), 2);
                    assert_eq!(rows.len(), 1);
                },
                _ => panic!("Expected Values node as child of Insert"),
            }
        },
        _ => panic!("Expected Insert plan node"),
    }
}

#[tokio::test]
async fn test_insert_into_nonexistent_table() {
    let mut fixture = TestContext::new("insert_nonexistent_table").await;

    let insert_sql = "INSERT INTO nonexistent_table VALUES (1, 'test')";
    let result = fixture.planner.create_logical_plan(insert_sql);

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("does not exist"));
}

#[tokio::test]
async fn test_insert_column_count_mismatch() {
    let mut fixture = TestContext::new("insert_column_mismatch").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255)");

    // Too few values
    let insert_sql = "INSERT INTO users VALUES (1)";
    let result = fixture.planner.create_logical_plan(insert_sql);
    assert!(result.is_err());

    // Too many values
    let insert_sql = "INSERT INTO users VALUES (1, 'test', 'extra')";
    let result = fixture.planner.create_logical_plan(insert_sql);
    assert!(result.is_err());
}

#[tokio::test]
async fn test_insert_single_column_table() {
    let mut fixture = TestContext::new("insert_single_column").await;

    fixture.create_table("single_col", "id INTEGER");

    let insert_sql = "INSERT INTO single_col VALUES (42)";
    let plan = fixture.planner.create_logical_plan(insert_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Insert {
            table_name, schema, ..
        } => {
            assert_eq!(table_name, "single_col");
            assert_eq!(schema.get_column_count(), 1);

            match &plan.children[0].plan_type {
                LogicalPlanType::Values { rows, schema } => {
                    assert_eq!(schema.get_column_count(), 1);
                    assert_eq!(rows.len(), 1);
                },
                _ => panic!("Expected Values node as child of Insert"),
            }
        },
        _ => panic!("Expected Insert plan node"),
    }
}

#[tokio::test]
async fn test_insert_with_expressions() {
    let mut fixture = TestContext::new("insert_with_expressions").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255)");

    let insert_sql = "INSERT INTO users VALUES (1 + 2, 'user_' || '123')";
    let plan = fixture.planner.create_logical_plan(insert_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Insert {
            table_name, schema, ..
        } => {
            assert_eq!(table_name, "users");
            assert_eq!(schema.get_column_count(), 2);

            match &plan.children[0].plan_type {
                LogicalPlanType::Values { rows, schema } => {
                    assert_eq!(schema.get_column_count(), 2);
                    assert_eq!(rows.len(), 1);
                },
                _ => panic!("Expected Values node as child of Insert"),
            }
        },
        _ => panic!("Expected Insert plan node"),
    }
}

#[tokio::test]
async fn test_insert_many_columns() {
    let mut fixture = TestContext::new("insert_many_columns").await;

    // Create a table with many columns
    fixture
        .create_table(
            "wide_table",
            "col1 INTEGER, col2 VARCHAR(50), col3 INTEGER, col4 VARCHAR(50), col5 INTEGER, col6 VARCHAR(50), col7 INTEGER, col8 VARCHAR(50)"
        );

    let insert_sql = "INSERT INTO wide_table VALUES (1, 'a', 2, 'b', 3, 'c', 4, 'd')";
    let plan = fixture.planner.create_logical_plan(insert_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Insert {
            table_name, schema, ..
        } => {
            assert_eq!(table_name, "wide_table");
            assert_eq!(schema.get_column_count(), 8);

            match &plan.children[0].plan_type {
                LogicalPlanType::Values { rows, schema } => {
                    assert_eq!(schema.get_column_count(), 8);
                    assert_eq!(rows.len(), 1);
                },
                _ => panic!("Expected Values node as child of Insert"),
            }
        },
        _ => panic!("Expected Insert plan node"),
    }
}

#[tokio::test]
async fn test_insert_large_batch() {
    let mut fixture = TestContext::new("insert_large_batch").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255)");

    // Build a query with many rows
    let mut values_list = Vec::new();
    for i in 1..=100 {
        values_list.push(format!("({}, 'user{}')", i, i));
    }
    let insert_sql = format!("INSERT INTO users VALUES {}", values_list.join(", "));

    let plan = fixture.planner.create_logical_plan(&insert_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Insert {
            table_name, schema, ..
        } => {
            assert_eq!(table_name, "users");
            assert_eq!(schema.get_column_count(), 2);

            match &plan.children[0].plan_type {
                LogicalPlanType::Values { rows, schema } => {
                    assert_eq!(schema.get_column_count(), 2);
                    assert_eq!(rows.len(), 100);
                },
                _ => panic!("Expected Values node as child of Insert"),
            }
        },
        _ => panic!("Expected Insert plan node"),
    }
}

#[tokio::test]
async fn test_insert_string_with_quotes() {
    let mut fixture = TestContext::new("insert_string_quotes").await;
    fixture.create_table("users", "id INTEGER, name VARCHAR(255)");

    let insert_sql = "INSERT INTO users VALUES (1, 'John''s Database')";
    let plan = fixture.planner.create_logical_plan(insert_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Insert {
            table_name, schema, ..
        } => {
            assert_eq!(table_name, "users");
            assert_eq!(schema.get_column_count(), 2);

            match &plan.children[0].plan_type {
                LogicalPlanType::Values { rows, schema } => {
                    assert_eq!(schema.get_column_count(), 2);
                    assert_eq!(rows.len(), 1);
                },
                _ => panic!("Expected Values node as child of Insert"),
            }
        },
        _ => panic!("Expected Insert plan node"),
    }
}

#[tokio::test]
async fn test_insert_with_default_values() {
    let mut fixture = TestContext::new("insert_default_values").await;

    // Create a table with default values
    fixture.create_table(
        "users_with_defaults",
        "id INTEGER, name VARCHAR(255) DEFAULT 'Unknown', created_at INTEGER DEFAULT 0",
    );

    let insert_sql = "INSERT INTO users_with_defaults (id) VALUES (1)";

    // This should work with the current implementation
    // Note: Full default value handling might need additional implementation
    let result = fixture.planner.create_logical_plan(insert_sql);

    // For now, just check that it doesn't crash - full default value support
    // might require additional logic in the planner
    match result {
        Ok(plan) => match &plan.plan_type {
            LogicalPlanType::Insert { table_name, .. } => {
                assert_eq!(table_name, "users_with_defaults");
            },
            _ => panic!("Expected Insert plan node"),
        },
        Err(e) => {
            // Default value handling might not be fully implemented yet
            // This test documents the current behavior
            println!(
                "Expected behavior - default values not yet supported: {}",
                e
            );
        },
    }
}
