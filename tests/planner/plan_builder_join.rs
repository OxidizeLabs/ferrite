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

