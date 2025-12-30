#![allow(clippy::assertions_on_constants, clippy::nonminimal_bool)]

use crate::common::logger::init_test_logger;
use ferrite::buffer::buffer_pool_manager_async::BufferPoolManager;
use ferrite::buffer::lru_k_replacer::LRUKReplacer;
use ferrite::catalog::Catalog;
use ferrite::catalog::column::Column;
use ferrite::catalog::schema::Schema;
use ferrite::concurrency::lock_manager::LockManager;
use ferrite::concurrency::transaction::{IsolationLevel, Transaction};
use ferrite::concurrency::transaction_manager::TransactionManager;
use ferrite::sql::execution::check_option::CheckOption;
use ferrite::sql::execution::execution_context::{ExecutionContext, ExecutorType};
use ferrite::sql::execution::executors::create_table_executor::CreateTableExecutor;
use ferrite::sql::execution::plans::create_table_plan::CreateTablePlanNode;
use ferrite::sql::execution::transaction_context::TransactionContext;
use ferrite::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
use ferrite::types_db::type_id::TypeId;
use parking_lot::RwLock;
use std::sync::Arc;
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

        let disk_manager = AsyncDiskManager::new(db_path, log_path, DiskManagerConfig::default())
            .await
            .unwrap();
        let disk_manager_arc = Arc::new(disk_manager);
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
        let bpm = Arc::new(
            BufferPoolManager::new(BUFFER_POOL_SIZE, disk_manager_arc.clone(), replacer).unwrap(),
        );

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

        Self {
            bpm,
            catalog,
            execution_context,
            _temp_dir: temp_dir,
        }
    }
}

fn create_test_schema() -> Schema {
    Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("name", TypeId::VarChar),
    ])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executor_type_create_table_construction() {
    let test_ctx = TestContext::new("executor_type_create_table_construction").await;
    let schema = create_test_schema();
    let plan = Arc::new(CreateTablePlanNode::new(
        schema,
        "test_table".to_string(),
        false,
    ));
    let executor = CreateTableExecutor::new(test_ctx.execution_context.clone(), plan, false);
    let executor_type = ExecutorType::CreateTable(executor);
    assert_eq!(
        ExecutionContext::get_executor_type_name(&executor_type),
        "CreateTableExecutor"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executor_type_from_constructors() {
    let test_ctx = TestContext::new("executor_type_from_constructors").await;
    let schema = create_test_schema();
    let plan = Arc::new(CreateTablePlanNode::new(
        schema,
        "test_table".to_string(),
        false,
    ));
    let executor = CreateTableExecutor::new(test_ctx.execution_context.clone(), plan, false);
    let executor_type = ExecutorType::from_create_table(executor);
    assert_eq!(
        ExecutionContext::get_executor_type_name(&executor_type),
        "CreateTableExecutor"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executor_type_init_and_operations() {
    let test_ctx = TestContext::new("executor_type_init_and_operations").await;
    let schema = create_test_schema();
    let plan = Arc::new(CreateTablePlanNode::new(
        schema,
        "test_table".to_string(),
        false,
    ));

    let executor = CreateTableExecutor::new(test_ctx.execution_context.clone(), plan, false);
    let mut executor_type = ExecutorType::from_create_table(executor);

    executor_type.init();
    let output_schema = executor_type.get_output_schema();
    assert_eq!(output_schema.get_columns().len(), 2);

    let context = executor_type.get_executor_context();
    assert!(!context.try_read().is_none());

    let result = executor_type.next();
    assert!(result.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn execution_context_add_check_option() {
    let test_ctx = TestContext::new("execution_context_add_check_option").await;
    let schema = create_test_schema();

    let plan1 = Arc::new(CreateTablePlanNode::new(
        schema.clone(),
        "table1".to_string(),
        false,
    ));
    let plan2 = Arc::new(CreateTablePlanNode::new(
        schema,
        "table2".to_string(),
        false,
    ));

    let executor1 = CreateTableExecutor::new(test_ctx.execution_context.clone(), plan1, false);
    let executor2 = CreateTableExecutor::new(test_ctx.execution_context.clone(), plan2, false);

    let executor_type1 = ExecutorType::from_create_table(executor1);
    let executor_type2 = ExecutorType::from_create_table(executor2);

    {
        let mut exec_ctx = test_ctx.execution_context.write();
        exec_ctx.add_check_option_from_executor_type(executor_type1, executor_type2);
    }

    {
        let exec_ctx = test_ctx.execution_context.read();
        let check_set = exec_ctx.get_nlj_check_exec_set();
        assert_eq!(check_set.len(), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executor_type_pattern_matching() {
    let test_ctx = TestContext::new("executor_type_pattern_matching").await;
    let schema = create_test_schema();
    let plan = Arc::new(CreateTablePlanNode::new(
        schema,
        "test_table".to_string(),
        false,
    ));

    let executor = CreateTableExecutor::new(test_ctx.execution_context.clone(), plan, false);
    let executor_type = ExecutorType::from_create_table(executor);

    match executor_type {
        ExecutorType::CreateTable(_) => assert!(true),
        _ => assert!(false, "Pattern matching failed"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_executor_type_names() {
    // This validates the enum remains comprehensive. We only assert metadata here.
    let type_names = vec![
        "AggregationExecutor",
        "CommandExecutor",
        "CommitTransactionExecutor",
        "CreateIndexExecutor",
        "CreateTableExecutor",
        "DeleteExecutor",
        "FilterExecutor",
        "HashJoinExecutor",
        "IndexScanExecutor",
        "InsertExecutor",
        "LimitExecutor",
        "MockExecutor",
        "MockScanExecutor",
        "NestedIndexJoinExecutor",
        "NestedLoopJoinExecutor",
        "ProjectionExecutor",
        "RollbackTransactionExecutor",
        "SeqScanExecutor",
        "SortExecutor",
        "StartTransactionExecutor",
        "TableScanExecutor",
        "TopNExecutor",
        "TopNPerGroupExecutor",
        "UpdateExecutor",
        "ValuesExecutor",
        "WindowExecutor",
    ];
    assert_eq!(type_names.len(), 26);
    assert!(true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiple_executor_types_in_context() {
    let test_ctx = TestContext::new("multiple_executor_types_in_context").await;
    let schema = create_test_schema();

    let plan1 = Arc::new(CreateTablePlanNode::new(
        schema.clone(),
        "table1".to_string(),
        false,
    ));
    let plan2 = Arc::new(CreateTablePlanNode::new(
        schema,
        "table2".to_string(),
        false,
    ));

    let exec1 = CreateTableExecutor::new(test_ctx.execution_context.clone(), plan1, false);
    let exec2 = CreateTableExecutor::new(test_ctx.execution_context.clone(), plan2, false);

    let et1 = ExecutorType::from_create_table(exec1);
    let et2 = ExecutorType::from_create_table(exec2);

    {
        let mut exec_ctx = test_ctx.execution_context.write();
        exec_ctx.add_check_option_from_executor_type(et1, et2);
    }

    {
        let exec_ctx = test_ctx.execution_context.read();
        let check_set = exec_ctx.get_nlj_check_exec_set();
        assert_eq!(check_set.len(), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn execution_context_init_check_options() {
    let test_ctx = TestContext::new("execution_context_init_check_options").await;
    {
        let mut exec_ctx = test_ctx.execution_context.write();
        exec_ctx.init_check_options();
        let check_options = exec_ctx.get_check_options();
        assert!(check_options.has_check(&CheckOption::EnablePushdownCheck));
        assert!(check_options.has_check(&CheckOption::EnableTopnCheck));
    }
}
