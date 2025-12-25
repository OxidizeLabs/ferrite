use crate::common::logger::init_test_logger;
use ferrite::buffer::buffer_pool_manager_async::BufferPoolManager;
use ferrite::buffer::lru_k_replacer::LRUKReplacer;
use ferrite::catalog::Catalog;
use ferrite::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
use ferrite::sql::planner::logical_plan::{LogicalPlanType, LogicalToPhysical};
use ferrite::sql::planner::query_planner::QueryPlanner;
use ferrite::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
use ferrite::types_db::type_id::TypeId;
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
        let physical_plan = create_plan.to_physical_plan()?;
        match physical_plan {
            PlanNode::CreateTable(create_table) => {
                let mut catalog = self.catalog.write();
                catalog.create_table(
                    create_table.get_table_name().to_string(),
                    create_table.get_output_schema().clone(),
                );
                Ok(())
            },
            _ => Err("Expected CreateTable plan node".to_string()),
        }
    }

    fn assert_table_exists(&self, table_name: &str) {
        let catalog = self.catalog.read();
        assert!(
            catalog.get_table(table_name).is_some(),
            "Table '{}' should exist",
            table_name
        );
    }

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
async fn create_simple_table() {
    let mut fixture = TestContext::new("create_simple_table").await;
    fixture
        .create_table("users", "id INTEGER, name VARCHAR(255)", false)
        .unwrap();
    fixture.assert_table_exists("users");
    fixture.assert_table_schema(
        "users",
        &[
            ("id".to_string(), TypeId::Integer),
            ("name".to_string(), TypeId::VarChar),
        ],
    );
}

#[tokio::test]
async fn create_table_if_not_exists() {
    let mut fixture = TestContext::new("create_table_if_not_exists").await;
    fixture.create_table("users", "id INTEGER", false).unwrap();
    assert!(fixture.create_table("users", "id INTEGER", false).is_err());
    assert!(fixture.create_table("users", "id INTEGER", true).is_ok());
}

#[tokio::test]
async fn create_table_various_data_types() {
    let mut fixture = TestContext::new("create_table_various_types").await;
    fixture
        .create_table(
            "test_types",
            "col_int INTEGER, col_bigint BIGINT, col_smallint SMALLINT, col_tinyint TINYINT, \
             col_varchar VARCHAR(255), col_char CHAR(10), col_text TEXT, \
             col_decimal DECIMAL(10,2), col_float FLOAT, col_double DOUBLE, \
             col_bool BOOLEAN, col_timestamp TIMESTAMP",
            false,
        )
        .unwrap();
    fixture.assert_table_exists("test_types");
}

#[tokio::test]
async fn create_table_with_primary_key() {
    let mut fixture = TestContext::new("create_table_with_primary_key").await;
    fixture
        .create_table(
            "products",
            "id INTEGER PRIMARY KEY, name VARCHAR(255), price DECIMAL(10,2)",
            false,
        )
        .unwrap();
    fixture.assert_table_exists("products");
}

#[tokio::test]
async fn create_table_with_not_null_constraints() {
    let mut fixture = TestContext::new("create_table_with_not_null").await;
    fixture
        .create_table(
            "customers",
            "id INTEGER NOT NULL, name VARCHAR(255) NOT NULL, email VARCHAR(255)",
            false,
        )
        .unwrap();
    fixture.assert_table_exists("customers");
}

#[tokio::test]
async fn create_table_with_default_values() {
    let mut fixture = TestContext::new("create_table_with_defaults").await;
    fixture
        .create_table(
            "settings",
            "id INTEGER, name VARCHAR(255) DEFAULT 'unknown', active BOOLEAN DEFAULT true, count INTEGER DEFAULT 0",
            false,
        )
        .unwrap();
    fixture.assert_table_exists("settings");
}

#[tokio::test]
async fn create_table_single_column() {
    let mut fixture = TestContext::new("create_table_single_column").await;
    fixture
        .create_table("simple", "value INTEGER", false)
        .unwrap();
    fixture.assert_table_exists("simple");
}

#[tokio::test]
async fn create_table_many_columns() {
    let mut fixture = TestContext::new("create_table_many_columns").await;
    let columns = (1..=20)
        .map(|i| format!("col{} INTEGER", i))
        .collect::<Vec<_>>()
        .join(", ");
    fixture.create_table("wide_table", &columns, false).unwrap();
    fixture.assert_table_exists("wide_table");
}

#[tokio::test]
async fn create_table_quoted_identifiers() {
    let mut fixture = TestContext::new("create_table_quoted").await;
    fixture
        .create_table(
            "\"Special Table\"",
            "\"column with spaces\" INTEGER, \"ORDER\" VARCHAR(255), \"select\" INTEGER",
            false,
        )
        .unwrap();
    fixture.assert_table_exists("\"Special Table\"");
}

#[tokio::test]
async fn create_table_case_sensitivity() {
    let mut fixture = TestContext::new("create_table_case").await;
    fixture
        .create_table(
            "MyTable",
            "ID INTEGER, Name VARCHAR(255), Age INTEGER",
            false,
        )
        .unwrap();
    fixture.assert_table_exists("MyTable");
}

#[tokio::test]
async fn create_table_long_names() {
    let mut fixture = TestContext::new("create_table_long_names").await;
    let long_table_name = "a".repeat(50);
    let long_column_name = "b".repeat(50);
    fixture
        .create_table(
            &long_table_name,
            &format!("{} INTEGER, normal_col VARCHAR(255)", long_column_name),
            false,
        )
        .unwrap();
    fixture.assert_table_exists(&long_table_name);
}

#[tokio::test]
async fn create_table_with_check_constraint() {
    let mut fixture = TestContext::new("create_table_with_check").await;
    fixture
        .create_table(
            "products",
            "id INTEGER, price DECIMAL(10,2) CHECK (price > 0), discount INTEGER CHECK (discount >= 0 AND discount <= 100)",
            false,
        )
        .unwrap();
    fixture.assert_table_exists("products");
}

#[tokio::test]
async fn create_table_with_foreign_key() {
    let mut fixture = TestContext::new("create_table_with_fk").await;
    fixture
        .create_table(
            "categories",
            "id INTEGER PRIMARY KEY, name VARCHAR(255)",
            false,
        )
        .unwrap();
    fixture
        .create_table(
            "products",
            "id INTEGER PRIMARY KEY, name VARCHAR(255), category_id INTEGER REFERENCES categories(id)",
            false,
        )
        .unwrap();
    fixture.assert_table_exists("products");
}

#[tokio::test]
async fn create_table_with_unique_constraint() {
    let mut fixture = TestContext::new("create_table_with_unique").await;
    fixture
        .create_table(
            "users",
            "id INTEGER PRIMARY KEY, username VARCHAR(255) UNIQUE, email VARCHAR(255) UNIQUE, age INTEGER",
            false,
        )
        .unwrap();
    fixture.assert_table_exists("users");
}

#[tokio::test]
async fn create_table_with_auto_increment() {
    let mut fixture = TestContext::new("create_table_with_auto_increment").await;
    fixture
        .create_table(
            "logs",
            "id INTEGER AUTO_INCREMENT PRIMARY KEY, message TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            false,
        )
        .unwrap();
    fixture.assert_table_exists("logs");
}

#[tokio::test]
async fn create_temporary_table() {
    let mut fixture = TestContext::new("create_temporary_table").await;
    let create_sql = "CREATE TEMPORARY TABLE temp_data (id INTEGER, value VARCHAR(255))";
    let plan = fixture.planner.create_logical_plan(create_sql).unwrap();
    match &plan.plan_type {
        LogicalPlanType::CreateTable {
            schema, table_name, ..
        } => {
            assert_eq!(table_name, "temp_data");
            assert_eq!(schema.get_column_count(), 2);
        },
        _ => panic!("Expected CreateTable plan node"),
    }
}

#[tokio::test]
async fn create_table_duplicate_already_exists() {
    let mut fixture = TestContext::new("create_table_duplicate").await;
    fixture.create_table("users", "id INTEGER", false).unwrap();
    let result = fixture.create_table("users", "id INTEGER", false);
    assert!(result.is_err());
    let error_msg = result.unwrap_err();
    assert!(error_msg.contains("already exists"));
}

#[tokio::test]
async fn create_table_empty_name() {
    let mut fixture = TestContext::new("create_table_empty_name").await;
    let create_sql = "CREATE TABLE (id INTEGER)";
    let result = fixture.planner.create_logical_plan(create_sql);
    assert!(result.is_err());
}

#[tokio::test]
async fn create_table_no_columns() {
    let mut fixture = TestContext::new("create_table_no_columns").await;
    let create_sql = "CREATE TABLE empty_table ()";
    let result = fixture.planner.create_logical_plan(create_sql);
    assert!(result.is_err());
}

#[tokio::test]
async fn test_create_table_with_complex_constraints() {
    let mut fixture = TestContext::new("create_table_complex_constraints").await;

    fixture
        .create_table(
            "orders",
            "id INTEGER PRIMARY KEY AUTO_INCREMENT, \
             customer_id INTEGER NOT NULL, \
             order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
             total DECIMAL(10,2) NOT NULL CHECK (total > 0), \
             status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'completed', 'cancelled')), \
             UNIQUE (customer_id, order_date)",
            false,
        )
        .unwrap();

    fixture.assert_table_exists("orders");
}

#[tokio::test]
async fn test_create_table_with_index_hints() {
    let mut fixture = TestContext::new("create_table_with_index_hints").await;

    fixture
        .create_table(
            "indexed_table",
            "id INTEGER PRIMARY KEY, \
             name VARCHAR(255), \
             email VARCHAR(255), \
             INDEX idx_name (name), \
             INDEX idx_email (email)",
            false,
        )
        .unwrap();

    fixture.assert_table_exists("indexed_table");
}

#[tokio::test]
async fn test_create_table_with_computed_columns() {
    let mut fixture = TestContext::new("create_table_computed").await;

    // Test computed/generated columns
    fixture
        .create_table(
            "rectangle",
            "width DECIMAL(10,2), height DECIMAL(10,2), \
             area DECIMAL(10,2) AS (width * height) STORED",
            false,
        )
        .unwrap();

    fixture.assert_table_exists("rectangle");
}

#[tokio::test]
async fn test_create_table_with_collation() {
    let mut fixture = TestContext::new("create_table_collation").await;

    fixture
        .create_table(
            "text_table",
            "id INTEGER, \
             name VARCHAR(255) COLLATE utf8_general_ci, \
             description TEXT COLLATE utf8_unicode_ci",
            false,
        )
        .unwrap();

    fixture.assert_table_exists("text_table");
}

#[tokio::test]
async fn test_create_table_with_engine_options() {
    let mut fixture = TestContext::new("create_table_engine").await;

    // Test MySQL-style engine options
    let create_sql = "CREATE TABLE test_table (id INTEGER) ENGINE=InnoDB DEFAULT CHARSET=utf8";
    let plan = fixture.planner.create_logical_plan(create_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::CreateTable { table_name, .. } => {
            assert_eq!(table_name, "test_table");
        },
        _ => panic!("Expected CreateTable plan node"),
    }
}
