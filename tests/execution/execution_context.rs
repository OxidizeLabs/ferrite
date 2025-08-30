use tkdb::sql::execution::execution_context::{ExecutionContext, ExecutorType};
use tkdb::sql::execution::executors::create_table_executor::CreateTableExecutor;
use tkdb::sql::execution::plans::create_table_plan::CreateTablePlanNode;
use tkdb::catalog::column::Column;
use tkdb::catalog::schema::Schema;
use tkdb::types_db::type_id::TypeId;
use std::sync::Arc;
use parking_lot::RwLock;
use tkdb::sql::planner::query_planner::QueryPlanner;
use tkdb::catalog::catalog::Catalog;
use tkdb::buffer::buffer_pool_manager_async::BufferPoolManager;
use tkdb::buffer::lru_k_replacer::LRUKReplacer;
use tkdb::concurrency::transaction::{IsolationLevel, Transaction};
use tkdb::concurrency::transaction_manager::TransactionManager;
use tkdb::concurrency::lock_manager::LockManager;
use tkdb::sql::execution::transaction_context::TransactionContext;
use tkdb::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
use crate::common::logger::init_test_logger;
use tempfile::TempDir;

struct TestContext {
    bpm: Arc<BufferPoolManager>,
    catalog: Arc<RwLock<Catalog>>,
    execution_context: Arc<RwLock<ExecutionContext>>,
    _temp_dir: TempDir,
}

impl TestContext {
    async fn new(name: &str) -> Self {
        init_test_logger();
        const BUFFER_POOL_SIZE: usize = 10;
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
        let bpm = Arc::new(BufferPoolManager::new(BUFFER_POOL_SIZE, disk_manager_arc.clone(), replacer).unwrap());

        let transaction_manager = Arc::new(TransactionManager::new());
        let lock_manager = Arc::new(LockManager::new());
        let transaction = Arc::new(Transaction::new(0, IsolationLevel::ReadUncommitted));
        let transaction_context = Arc::new(TransactionContext::new(
            transaction,
            lock_manager,
            transaction_manager.clone(),
        ));

        let catalog = Arc::new(RwLock::new(Catalog::new(bpm.clone(), transaction_manager)));
        let execution_context = Arc::new(RwLock::new(ExecutionContext::new(
            bpm.clone(),
            catalog.clone(),
            transaction_context,
        )));

        Self { bpm, catalog, execution_context, _temp_dir: temp_dir }
    }
}

fn create_test_schema() -> Schema {
    Schema::new(vec![Column::new("id", TypeId::Integer), Column::new("name", TypeId::VarChar)])
}

#[tokio::test]
async fn executor_type_create_table_construction() {
    let test_ctx = TestContext::new("executor_type_create_table_construction").await;
    let schema = create_test_schema();
    let plan = Arc::new(CreateTablePlanNode::new(schema, "test_table".to_string(), false));
    let executor = CreateTableExecutor::new(test_ctx.execution_context.clone(), plan, false);
    let executor_type = ExecutorType::CreateTable(executor);
    assert_eq!(ExecutionContext::get_executor_type_name(&executor_type), "CreateTableExecutor");
}

#[tokio::test]
async fn execution_context_getters_setters() {
    let test_ctx = TestContext::new("execution_context_getters_setters").await;
    {
        let mut exec_ctx = test_ctx.execution_context.write();
        assert!(!exec_ctx.is_delete());
        exec_ctx.set_delete(true);
        assert!(exec_ctx.is_delete());
        assert!(!exec_ctx.should_chain_after_transaction());
        exec_ctx.set_chain_after_transaction(true);
        assert!(exec_ctx.should_chain_after_transaction());
        let txn_ctx = exec_ctx.get_transaction_context();
        assert_eq!(txn_ctx.get_transaction_id(), 0);
        let bpm = exec_ctx.get_buffer_pool_manager();
        assert!(Arc::ptr_eq(&bpm, &test_ctx.bpm));
        let catalog = exec_ctx.get_catalog();
        assert!(Arc::ptr_eq(catalog, &test_ctx.catalog));
    }
}


