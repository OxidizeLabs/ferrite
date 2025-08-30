use tkdb::buffer::buffer_pool_manager_async::BufferPoolManager;
use tkdb::buffer::lru_k_replacer::LRUKReplacer;
use tkdb::catalog::catalog::Catalog;
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
        ]);
        let _ = catalog.create_table("users".to_string(), schema);
    }
}

#[tokio::test]
async fn aggregate_count_star() {
    let mut ctx = TestContext::new("planner_agg_count_star").await;
    ctx.create_users();
    let plan = ctx.planner.create_logical_plan("SELECT COUNT(*) FROM users").unwrap();
    match &plan.children[0].plan_type {
        LogicalPlanType::Aggregate { aggregates, group_by, .. } => {
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
        LogicalPlanType::Aggregate { group_by, aggregates, schema } => {
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

