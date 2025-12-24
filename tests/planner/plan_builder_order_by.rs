use crate::common::logger::init_test_logger;
use parking_lot::RwLock;
use std::sync::Arc;
use tempfile::TempDir;
use ferrite::buffer::buffer_pool_manager_async::BufferPoolManager;
use ferrite::buffer::lru_k_replacer::LRUKReplacer;
use ferrite::catalog::Catalog;
use ferrite::sql::planner::logical_plan::LogicalPlanType;
use ferrite::sql::planner::query_planner::QueryPlanner;
use ferrite::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};

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

    fn create_employees(&mut self) {
        let mut catalog = self.catalog.write();
        let schema = ferrite::catalog::schema::Schema::new(vec![
            ferrite::catalog::column::Column::new("id", ferrite::types_db::type_id::TypeId::Integer),
            ferrite::catalog::column::Column::new("name", ferrite::types_db::type_id::TypeId::VarChar),
            ferrite::catalog::column::Column::new("salary", ferrite::types_db::type_id::TypeId::Decimal),
            ferrite::catalog::column::Column::new("dept", ferrite::types_db::type_id::TypeId::VarChar),
        ]);
        let _ = catalog.create_table("employees".to_string(), schema);
    }
}

#[tokio::test]
async fn test_order_by_with_limit() {
    let mut fixture = TestContext::new("order_by_limit").await;
    fixture.create_employees();

    let sql = "SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 10";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Limit { limit, schema } => {
            assert_eq!(*limit, 10);
            assert_eq!(schema.get_column_count(), 2);
        }
        _ => panic!("Expected Limit as root node"),
    }

    match &plan.children[0].plan_type {
        LogicalPlanType::Sort {
            sort_specifications,
            schema,
        } => {
            assert_eq!(sort_specifications.len(), 1);
            assert_eq!(schema.get_column_count(), 2);
        }
        _ => panic!("Expected Sort node"),
    }

    match &plan.children[0].children[0].plan_type {
        LogicalPlanType::Projection {
            expressions,
            schema,
            column_mappings: _,
        } => {
            assert_eq!(expressions.len(), 2);
            assert_eq!(schema.get_column_count(), 2);
        }
        _ => panic!("Expected Projection node"),
    }
}
