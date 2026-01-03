use std::sync::Arc;

use ferrite::buffer::buffer_pool_manager_async::BufferPoolManager;
use ferrite::buffer::lru_k_replacer::LRUKReplacer;
use ferrite::catalog::Catalog;
use ferrite::sql::planner::logical_plan::LogicalPlanType;
use ferrite::sql::planner::plan_builder::LogicalPlanBuilder;
use ferrite::sql::planner::query_planner::QueryPlanner;
use ferrite::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
use parking_lot::RwLock;
use tempfile::TempDir;

use crate::common::logger::init_test_logger;

struct TestContext {
    catalog: Arc<RwLock<Catalog>>,
    _planner: QueryPlanner,
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
            _planner: planner,
            _temp_dir: temp_dir,
        }
    }
}

#[tokio::test]
async fn savepoint_and_release() {
    use sqlparser::ast::Ident;
    let fixture = TestContext::new("savepoint_release").await;
    let builder = LogicalPlanBuilder::new(fixture.catalog.clone());

    let name = Ident::new("my_savepoint");
    let savepoint = builder.build_savepoint_plan(&name).unwrap();
    match &savepoint.plan_type {
        LogicalPlanType::Savepoint { name } => assert_eq!(name, "my_savepoint"),
        _ => panic!("Expected Savepoint plan"),
    }

    let release = builder.build_release_savepoint_plan(&name).unwrap();
    match &release.plan_type {
        LogicalPlanType::ReleaseSavepoint { name } => assert_eq!(name, "my_savepoint"),
        _ => panic!("Expected ReleaseSavepoint plan"),
    }
}

#[tokio::test]
async fn drop_plan_building() {
    let fixture = TestContext::new("drop_plan").await;
    let builder = LogicalPlanBuilder::new(fixture.catalog.clone());

    let object_type = "TABLE".to_string();
    let if_exists = true;
    let names = vec!["users".to_string(), "orders".to_string()];
    let cascade = true;

    let plan = builder
        .build_drop_plan(object_type.clone(), if_exists, names.clone(), cascade)
        .unwrap();

    match &plan.plan_type {
        LogicalPlanType::Drop {
            object_type: ot,
            if_exists: ie,
            names: n,
            cascade: c,
        } => {
            assert_eq!(object_type, ot.clone());
            assert_eq!(if_exists, *ie);
            assert_eq!(names, n.clone());
            assert_eq!(cascade, *c);
        },
        _ => panic!("Expected Drop plan"),
    }
}
