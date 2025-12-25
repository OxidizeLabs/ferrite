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

    fn create_employees_departments(&mut self) {
        let mut catalog = self.catalog.write();
        let employees = ferrite::catalog::schema::Schema::new(vec![
            ferrite::catalog::column::Column::new(
                "id",
                ferrite::types_db::type_id::TypeId::Integer,
            ),
            ferrite::catalog::column::Column::new(
                "name",
                ferrite::types_db::type_id::TypeId::VarChar,
            ),
            ferrite::catalog::column::Column::new(
                "salary",
                ferrite::types_db::type_id::TypeId::Decimal,
            ),
            ferrite::catalog::column::Column::new(
                "dept_id",
                ferrite::types_db::type_id::TypeId::Integer,
            ),
        ]);
        let departments = ferrite::catalog::schema::Schema::new(vec![
            ferrite::catalog::column::Column::new(
                "dept_id",
                ferrite::types_db::type_id::TypeId::Integer,
            ),
            ferrite::catalog::column::Column::new(
                "name",
                ferrite::types_db::type_id::TypeId::VarChar,
            ),
            ferrite::catalog::column::Column::new(
                "budget",
                ferrite::types_db::type_id::TypeId::Decimal,
            ),
        ]);
        let _ = catalog.create_table("employees".to_string(), employees);
        let _ = catalog.create_table("departments".to_string(), departments);
    }
}

#[tokio::test]
async fn test_subquery_in_where() {
    let mut fixture = TestContext::new("subquery_where").await;
    fixture.create_employees_departments();

    let sql = "SELECT e.name, e.salary FROM employees e WHERE e.salary > (SELECT AVG(salary) FROM employees)";
    let plan = fixture.planner.create_logical_plan(sql).unwrap();

    match &plan.plan_type {
        LogicalPlanType::Projection { .. } => match &plan.children[0].plan_type {
            LogicalPlanType::Filter { .. } => (),
            _ => panic!("Expected Filter under Projection for subquery WHERE"),
        },
        _ => panic!("Expected Projection root"),
    }
}
