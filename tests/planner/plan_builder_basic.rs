use tkdb::buffer::buffer_pool_manager_async::BufferPoolManager;
use tkdb::buffer::lru_k_replacer::LRUKReplacer;
use tkdb::catalog::Catalog;
use tkdb::catalog::column::Column;
use tkdb::catalog::schema::Schema;
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
        let catalog = Arc::new(RwLock::new(Catalog::new(bpm.clone(), transaction_manager.clone())));
        let planner = QueryPlanner::new(Arc::clone(&catalog));

        Self { catalog, planner, _temp_dir: temp_dir }
    }

    fn setup_tables(&mut self) {
        let mut catalog = self.catalog.write();
        let users_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
            Column::new("email", TypeId::VarChar),
        ]);
        let _ = catalog.create_table("users".to_string(), users_schema);
    }
}

#[tokio::test]
async fn plan_builder_basic_selects() {
    let mut ctx = TestContext::new("plan_builder_basic_selects").await;
    ctx.setup_tables();
    for sql in [
        "SELECT id FROM users",
        "SELECT id, name FROM users",
        "SELECT * FROM users",
        "SELECT id as user_id FROM users",
    ] {
        let plan = ctx.planner.explain(sql).unwrap();
        assert!(plan.contains("→"));
        assert!(plan.contains("users"));
    }
}

#[tokio::test]
async fn plan_builder_basic_filters() {
    let mut ctx = TestContext::new("plan_builder_basic_filters").await;
    ctx.setup_tables();
    for sql in [
        "SELECT id FROM users WHERE age > 25",
        "SELECT id FROM users WHERE age >= 20 AND age <= 30",
        "SELECT id FROM users WHERE age > 25 AND name = 'John'",
    ] {
        let plan = ctx.planner.explain(sql).unwrap();
        assert!(plan.contains("→"));
        assert!(plan.contains("users"));
    }
}

#[tokio::test]
async fn plan_builder_basic_aggregations() {
    let mut ctx = TestContext::new("plan_builder_basic_aggs").await;
    ctx.setup_tables();
    for sql in [
        "SELECT COUNT(*) FROM users",
        "SELECT age, COUNT(*) FROM users GROUP BY age",
    ] {
        let plan = ctx.planner.explain(sql).unwrap();
        assert!(plan.contains("→"));
        assert!(plan.contains("users"));
    }
}

#[tokio::test]
async fn plan_builder_error_handling() {
    let mut ctx = TestContext::new("plan_builder_error_handling").await;
    ctx.setup_tables();
    for (sql, expected) in [
        ("SELECT * FROM nonexistent_table", "not found in catalog"),
        ("SELECT nonexistent FROM users", "not found in schema"),
        ("SELECT * FROM users WHERE nonexistent > 0", "not found in schema"),
    ] {
        let err = ctx.planner.explain(sql).unwrap_err();
        assert!(err.to_string().contains(expected));
    }
}


