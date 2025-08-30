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

    fn create_table(&mut self, name: &str, columns: &str) {
        let mut catalog = self.catalog.write();
        let schema = tkdb::catalog::schema::Schema::new(
            columns.split(',').map(|c| c.trim()).filter(|c| !c.is_empty()).map(|c| {
                let parts: Vec<&str> = c.split_whitespace().collect();
                let (col_name, ty) = (parts[0], parts[1].to_uppercase());
                let type_id = match ty.as_str() {
                    "INTEGER" => tkdb::types_db::type_id::TypeId::Integer,
                    "VARCHAR(255)" | "VARCHAR(50)" | "TEXT" | "VARCHAR" => tkdb::types_db::type_id::TypeId::VarChar,
                    _ => tkdb::types_db::type_id::TypeId::VarChar,
                };
                tkdb::catalog::column::Column::new(col_name, type_id)
            }).collect()
        );
        let _ = catalog.create_table(name.to_string(), schema);
    }
}

#[tokio::test]
async fn insert_simple_row() {
    let mut ctx = TestContext::new("planner_insert_simple").await;
    ctx.create_table("users", "id INTEGER, name VARCHAR(255)");
    let sql = "INSERT INTO users VALUES (1, 'Alice')";
    let plan = ctx.planner.create_logical_plan(sql).unwrap();
    match &plan.plan_type {
        LogicalPlanType::Insert { table_name, schema, .. } => {
            assert_eq!(table_name, "users");
            assert_eq!(schema.get_column_count(), 2);
            match &plan.children[0].plan_type {
                LogicalPlanType::Values { rows, schema } => {
                    assert_eq!(schema.get_column_count(), 2);
                    assert_eq!(rows.len(), 1);
                }
                _ => panic!("Expected Values child"),
            }
        }
        _ => panic!("Expected Insert plan"),
    }
}

#[tokio::test]
async fn insert_multiple_rows() {
    let mut ctx = TestContext::new("planner_insert_multi").await;
    ctx.create_table("users", "id INTEGER, name VARCHAR(255)");
    let sql = "INSERT INTO users VALUES (1, 'A'), (2, 'B'), (3, 'C')";
    let plan = ctx.planner.create_logical_plan(sql).unwrap();
    match &plan.plan_type {
        LogicalPlanType::Insert { table_name, .. } => {
            assert_eq!(table_name, "users");
            match &plan.children[0].plan_type {
                LogicalPlanType::Values { rows, .. } => {
                    assert_eq!(rows.len(), 3);
                }
                _ => panic!("Expected Values child"),
            }
        }
        _ => panic!("Expected Insert plan"),
    }
}

#[tokio::test]
async fn insert_with_explicit_columns() {
    let mut ctx = TestContext::new("planner_insert_columns").await;
    ctx.create_table("users", "id INTEGER, name VARCHAR(255)");
    let sql = "INSERT INTO users (id, name) VALUES (10, 'Zed')";
    let plan = ctx.planner.create_logical_plan(sql).unwrap();
    match &plan.plan_type {
        LogicalPlanType::Insert { table_name, schema, .. } => {
            assert_eq!(table_name, "users");
            assert_eq!(schema.get_column_count(), 2);
        }
        _ => panic!("Expected Insert plan"),
    }
}
