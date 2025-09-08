use tkdb::buffer::buffer_pool_manager_async::BufferPoolManager;
use tkdb::buffer::lru_k_replacer::LRUKReplacer;
use tkdb::catalog::Catalog;
use tkdb::sql::planner::logical_plan::LogicalPlanType;
use tkdb::sql::planner::query_planner::QueryPlanner;
use tkdb::types_db::type_id::TypeId;
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

    fn create_users(&mut self) {
        let mut catalog = self.catalog.write();
        let schema = tkdb::catalog::schema::Schema::new(vec![
            tkdb::catalog::column::Column::new("id", TypeId::Integer),
            tkdb::catalog::column::Column::new("name", TypeId::VarChar),
            tkdb::catalog::column::Column::new("age", TypeId::Integer),
            tkdb::catalog::column::Column::new("email", TypeId::VarChar),
        ]);
        let _ = catalog.create_table("users".to_string(), schema);
    }
}

#[tokio::test]
async fn create_simple_index() {
    let mut fixture = TestContext::new("create_simple_index").await;
    fixture.create_users();

    let create_index_sql = "CREATE INDEX idx_name ON users (name)";
    let plan = fixture.planner.create_logical_plan(create_index_sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::CreateIndex { table_name, index_name, key_attrs, schema, if_not_exists } => {
            assert_eq!(table_name, "users");
            assert_eq!(index_name, "idx_name");
            assert_eq!(key_attrs.len(), 1);
            assert_eq!(key_attrs[0], 1);
            assert_eq!(schema.get_column_count(), 1);
            assert!(!if_not_exists);
            let col = schema.get_column(0).unwrap();
            assert_eq!(col.get_name(), "name");
            assert_eq!(col.get_type(), TypeId::VarChar);
        }
        _ => panic!("Expected CreateIndex plan node"),
    }
}

#[tokio::test]
async fn create_composite_index() {
    let mut fixture = TestContext::new("create_composite_index").await;
    fixture.create_users();
    let sql = "CREATE INDEX idx_name_age ON users (name, age)";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();
    match &plan.plan_type {
        LogicalPlanType::CreateIndex { table_name, index_name, key_attrs, schema, if_not_exists } => {
            assert_eq!(table_name, "users");
            assert_eq!(index_name, "idx_name_age");
            assert_eq!(key_attrs.as_slice(), [1, 2]);
            assert_eq!(schema.get_column_count(), 2);
            assert!(!if_not_exists);
        }
        _ => panic!("Expected CreateIndex plan node"),
    }
}

#[tokio::test]
async fn create_index_if_not_exists() {
    let mut fixture = TestContext::new("create_index_if_not_exists").await;
    fixture.create_users();
    let sql = "CREATE INDEX IF NOT EXISTS idx_email ON users (email)";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();
    match &plan.plan_type {
        LogicalPlanType::CreateIndex { table_name, index_name, key_attrs, schema, if_not_exists } => {
            assert_eq!(table_name, "users");
            assert_eq!(index_name, "idx_email");
            assert_eq!(key_attrs.as_slice(), [3]);
            assert_eq!(schema.get_column_count(), 1);
            assert!(if_not_exists);
        }
        _ => panic!("Expected CreateIndex plan node"),
    }
}

#[tokio::test]
async fn create_index_all_columns() {
    let mut fixture = TestContext::new("create_index_all_columns").await;
    fixture.create_users();
    let sql = "CREATE INDEX idx_all ON users (id, name, age, email)";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();
    match &plan.plan_type {
        LogicalPlanType::CreateIndex { table_name, index_name, key_attrs, schema, if_not_exists } => {
            assert_eq!(table_name, "users");
            assert_eq!(index_name, "idx_all");
            assert_eq!(key_attrs.as_slice(), [0, 1, 2, 3]);
            assert_eq!(schema.get_column_count(), 4);
            assert!(!if_not_exists);
        }
        _ => panic!("Expected CreateIndex plan node"),
    }
}

#[tokio::test]
async fn create_index_on_nonexistent_table_errors() {
    let mut fixture = TestContext::new("create_index_nonexistent_table").await;
    let sql = "CREATE INDEX idx_name ON nonexistent_table (some_column)";
    let res = fixture.planner.create_logical_plan(sql);
    assert!(res.is_err());
}

#[tokio::test]
async fn create_index_on_nonexistent_column_errors() {
    let mut fixture = TestContext::new("create_index_nonexistent_column").await;
    fixture.create_users();
    let sql = "CREATE INDEX idx_bad ON users (nonexistent_column)";
    let res = fixture.planner.create_logical_plan(sql);
    assert!(res.is_err());
}

#[tokio::test]
async fn create_index_mixed_valid_invalid_columns_errors() {
    let mut fixture = TestContext::new("create_index_mixed_columns").await;
    fixture.create_users();

    // Mix valid and invalid columns
    let sql = "CREATE INDEX idx_mixed ON users (name, invalid_column)";
    let res = fixture.planner.create_logical_plan(sql);
    assert!(res.is_err());
}

#[tokio::test]
async fn create_index_on_primary_key() {
    let mut fixture = TestContext::new("create_index_on_primary_key").await;
    fixture.create_users();

    let sql = "CREATE INDEX idx_id ON users (id)";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::CreateIndex { table_name, index_name, key_attrs, schema, if_not_exists } => {
            assert_eq!(table_name, "users");
            assert_eq!(index_name, "idx_id");
            assert_eq!(key_attrs.as_slice(), [0]);
            assert_eq!(schema.get_column_count(), 1);

            let col = schema.get_column(0).unwrap();
            assert_eq!(col.get_name(), "id");
            assert_eq!(col.get_type(), TypeId::Integer);
            assert!(!if_not_exists);
        }
        _ => panic!("Expected CreateIndex plan node"),
    }
}

#[tokio::test]
async fn create_unique_index() {
    let mut fixture = TestContext::new("create_unique_index").await;
    fixture.create_users();

    // Parser should accept UNIQUE INDEX syntax; enforcement is handled at execution-time
    let sql = "CREATE UNIQUE INDEX idx_unique_email ON users (email)";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::CreateIndex { table_name, index_name, key_attrs, schema, if_not_exists } => {
            assert_eq!(table_name, "users");
            assert_eq!(index_name, "idx_unique_email");
            assert_eq!(key_attrs.as_slice(), [3]);
            assert_eq!(schema.get_column_count(), 1);
            assert!(!if_not_exists);
        }
        _ => panic!("Expected CreateIndex plan node"),
    }
}


