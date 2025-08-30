use tkdb::buffer::buffer_pool_manager_async::BufferPoolManager;
use tkdb::buffer::lru_k_replacer::LRUKReplacer;
use tkdb::catalog::catalog::Catalog;
use tkdb::sql::planner::logical_plan::LogicalPlanType;
use tkdb::sql::planner::query_planner::QueryPlanner;
use tkdb::sql::execution::expressions::comparison_expression::ComparisonType;
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

    fn setup_users(&mut self) {
        let mut catalog = self.catalog.write();
        let schema = tkdb::catalog::schema::Schema::new(vec![
            tkdb::catalog::column::Column::new("id", tkdb::types_db::type_id::TypeId::Integer),
            tkdb::catalog::column::Column::new("name", tkdb::types_db::type_id::TypeId::VarChar),
            tkdb::catalog::column::Column::new("age", tkdb::types_db::type_id::TypeId::Integer),
        ]);
        let _ = catalog.create_table("users".to_string(), schema);
    }
}

#[tokio::test]
async fn simple_select() {
    let mut fixture = TestContext::new("simple_select").await;
    fixture.setup_users();
    let plan = fixture.planner.create_logical_plan("SELECT * FROM users").unwrap();
    match &plan.plan_type {
        LogicalPlanType::Projection { schema, .. } => assert_eq!(schema.get_column_count(), 3),
        _ => panic!("Expected Projection root"),
    }
}

#[tokio::test]
async fn select_specific_columns() {
    let mut fixture = TestContext::new("select_specific_columns").await;
    fixture.setup_users();
    let plan = fixture.planner.create_logical_plan("SELECT id, name FROM users").unwrap();
    match &plan.plan_type {
        LogicalPlanType::Projection { schema, .. } => assert_eq!(schema.get_column_count(), 2),
        _ => panic!("Expected Projection root"),
    }
}

#[tokio::test]
async fn select_with_filter() {
    let mut fixture = TestContext::new("select_with_filter").await;
    fixture.setup_users();
    let plan = fixture.planner.create_logical_plan("SELECT * FROM users WHERE age > 25").unwrap();
    match &plan.children[0].plan_type {
        LogicalPlanType::Filter { .. } => (),
        _ => panic!("Expected Filter"),
    }
}

#[tokio::test]
async fn select_with_equality_filter() {
    let mut fixture = TestContext::new("select_equality_filter").await;
    fixture.setup_users();
    let plan = fixture.planner.create_logical_plan("SELECT * FROM users WHERE id = 1").unwrap();
    match &plan.children[0].plan_type {
        LogicalPlanType::Filter { predicate, .. } => match predicate.as_ref() {
            tkdb::sql::execution::expressions::abstract_expression::Expression::Comparison(comp) => {
                assert_eq!(comp.get_comp_type(), ComparisonType::Equal);
            }
            _ => panic!("Expected Comparison"),
        },
        _ => panic!("Expected Filter"),
    }
}

#[tokio::test]
async fn select_from_nonexistent_table() {
    let mut fixture = TestContext::new("select_nonexistent_table").await;
    let result = fixture.planner.create_logical_plan("SELECT * FROM nonexistent_table");
    assert!(result.is_err());
}


