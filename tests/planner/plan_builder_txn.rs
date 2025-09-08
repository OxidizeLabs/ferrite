use tkdb::buffer::buffer_pool_manager_async::BufferPoolManager;
use tkdb::buffer::lru_k_replacer::LRUKReplacer;
use tkdb::catalog::Catalog;
use tkdb::sql::planner::logical_plan::LogicalPlanType;
use tkdb::sql::planner::plan_builder::LogicalPlanBuilder;
use tkdb::sql::planner::query_planner::QueryPlanner;
use tkdb::concurrency::transaction::IsolationLevel;
use parking_lot::RwLock;
use std::sync::Arc;
use tempfile::TempDir;
use tkdb::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
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
        let db_path = temp_dir.path().join(format!("{name}.db")).to_str().unwrap().to_string();
        let log_path = temp_dir.path().join(format!("{name}.log")).to_str().unwrap().to_string();

        let disk_manager = AsyncDiskManager::new(db_path, log_path, DiskManagerConfig::default()).await.unwrap();
        let disk_manager_arc = Arc::new(disk_manager);
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
        let bpm = Arc::new(BufferPoolManager::new(BUFFER_POOL_SIZE, disk_manager_arc, replacer).unwrap());

        let transaction_manager = Arc::new(tkdb::concurrency::transaction_manager::TransactionManager::new());
        let catalog = Arc::new(RwLock::new(Catalog::new(bpm, transaction_manager.clone())));
        let planner = QueryPlanner::new(Arc::clone(&catalog));

        Self { catalog, _planner: planner, _temp_dir: temp_dir }
    }
}

#[tokio::test]
async fn start_transaction_with_access_mode() {
    use sqlparser::ast::{TransactionAccessMode, TransactionMode};
    let context = TestContext::new("test_access_mode").await;
    let builder = LogicalPlanBuilder::new(context.catalog.clone());
    let modes = vec![TransactionMode::AccessMode(TransactionAccessMode::ReadOnly)];
    let plan = builder
        .build_start_transaction_plan(&modes, &true, &None, &None, &vec![], &None, &false)
        .unwrap();
    match plan.plan_type {
        LogicalPlanType::StartTransaction { read_only, .. } => assert!(read_only),
        _ => panic!("Expected StartTransaction plan"),
    }
}

#[tokio::test]
async fn start_transaction_with_isolation_level() {
    use sqlparser::ast::{TransactionIsolationLevel, TransactionMode};
    let context = TestContext::new("test_isolation_level").await;
    let builder = LogicalPlanBuilder::new(context.catalog.clone());
    let modes = vec![TransactionMode::IsolationLevel(TransactionIsolationLevel::Serializable)];
    let plan = builder
        .build_start_transaction_plan(&modes, &true, &None, &None, &vec![], &None, &false)
        .unwrap();
    match plan.plan_type {
        LogicalPlanType::StartTransaction { isolation_level, .. } => {
            assert_eq!(isolation_level, Some(IsolationLevel::Serializable));
        }
        _ => panic!("Expected StartTransaction plan"),
    }
}


