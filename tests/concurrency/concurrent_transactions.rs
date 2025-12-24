use futures::future;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;
use ferrite::buffer::buffer_pool_manager_async::BufferPoolManager;
use ferrite::buffer::lru_k_replacer::LRUKReplacer;
use ferrite::catalog::Catalog;
use ferrite::common::result_writer::CliResultWriter;
use ferrite::concurrency::transaction::IsolationLevel;
use ferrite::concurrency::transaction_manager::TransactionManager;
use ferrite::concurrency::transaction_manager_factory::TransactionManagerFactory;
use ferrite::recovery::log_manager::LogManager;
use ferrite::recovery::wal_manager::WALManager;
use ferrite::sql::execution::execution_context::ExecutionContext;
use ferrite::sql::execution::execution_engine::ExecutionEngine;
use ferrite::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
use tokio::sync::Mutex as AsyncMutex;

pub struct ConcurrentTestContext {
    catalog: Arc<RwLock<Catalog>>,
    buffer_pool_manager: Arc<BufferPoolManager>,
    transaction_factory: Arc<TransactionManagerFactory>,
    execution_engine: Arc<AsyncMutex<ExecutionEngine>>,
}

impl ConcurrentTestContext {
    pub async fn new(name: &str) -> Self {
        // Initialize components
        let disk_manager = AsyncDiskManager::new(
            format!("tests/data/{}.db", name),
            format!("tests/data/{}.log", name),
            DiskManagerConfig::default(),
        )
        .await
        .unwrap();

        let disk_manager_arc = Arc::new(disk_manager);

        let buffer_pool_manager = Arc::new(
            BufferPoolManager::new(
                32,
                disk_manager_arc.clone(),
                Arc::new(RwLock::new(LRUKReplacer::new(32, 2))),
            )
            .unwrap(),
        );

        let log_manager = Arc::new(RwLock::new(LogManager::new(disk_manager_arc.clone())));
        let transaction_manager = Arc::new(TransactionManager::new());

        let catalog = Arc::new(RwLock::new(Catalog::new(
            buffer_pool_manager.clone(),
            transaction_manager.clone(),
        )));

        let transaction_factory =
            Arc::new(TransactionManagerFactory::new(buffer_pool_manager.clone()));

        let wal_manager = Arc::new(WALManager::new(log_manager));

        let execution_engine = Arc::new(AsyncMutex::new(ExecutionEngine::new(
            catalog.clone(),
            buffer_pool_manager.clone(),
            transaction_factory.clone(),
            wal_manager,
        )));

        Self {
            catalog,
            buffer_pool_manager,
            transaction_factory,
            execution_engine,
        }
    }

    pub async fn setup_test_table(&self) -> Result<(), String> {
        let mut writer = CliResultWriter::new();
        let create_sql = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value INTEGER);";

        let txn_ctx = self
            .transaction_factory
            .begin_transaction(IsolationLevel::RepeatableRead);
        let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
            self.buffer_pool_manager.clone(),
            self.catalog.clone(),
            txn_ctx.clone(),
        )));

        let result = self
            .execution_engine
            .lock()
            .await
            .execute_sql(create_sql, exec_ctx, &mut writer)
            .await
            .map_err(|e| format!("Failed to create table: {}", e))?;

        if result {
            self.transaction_factory.commit_transaction(txn_ctx).await;
        } else {
            self.transaction_factory.abort_transaction(txn_ctx);
        }

        Ok(())
    }
}

#[tokio::test]
async fn test_concurrent_inserts() {
    let ctx = Arc::new(ConcurrentTestContext::new("concurrent_inserts").await);
    ctx.setup_test_table().await.unwrap();

    let thread_count = 10;
    let mut handles = vec![];

    // Spawn multiple async tasks to perform concurrent inserts
    for i in 0..thread_count {
        let ctx_clone = Arc::clone(&ctx);
        let handle = tokio::spawn(async move {
            let mut writer = CliResultWriter::new();
            let insert_sql = format!("INSERT INTO test_table VALUES ({}, {});", i, i * 100);

            let txn_ctx = ctx_clone
                .transaction_factory
                .begin_transaction(IsolationLevel::RepeatableRead);
            let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
                ctx_clone.buffer_pool_manager.clone(),
                ctx_clone.catalog.clone(),
                txn_ctx.clone(),
            )));

            let result = {
                let mut engine = ctx_clone.execution_engine.lock().await;
                engine.execute_sql(&insert_sql, exec_ctx, &mut writer).await
            };

            match result {
                Ok(_) => {
                    ctx_clone
                        .transaction_factory
                        .commit_transaction(txn_ctx)
                        .await
                }
                Err(_) => {
                    ctx_clone.transaction_factory.abort_transaction(txn_ctx);
                    false
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    let results: Vec<bool> = future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert!(results.iter().any(|&r| r), "All transactions failed");

    // Verify the results
    let mut writer = CliResultWriter::new();
    let select_sql = "SELECT COUNT(*) FROM test_table";
    let txn_ctx = ctx
        .transaction_factory
        .begin_transaction(IsolationLevel::RepeatableRead);
    let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
        ctx.buffer_pool_manager.clone(),
        ctx.catalog.clone(),
        txn_ctx.clone(),
    )));
    let result = ctx
        .execution_engine
        .lock()
        .await
        .execute_sql(select_sql, exec_ctx, &mut writer)
        .await;

    assert!(result.is_ok());
    ctx.transaction_factory.commit_transaction(txn_ctx).await;
}

#[tokio::test]
async fn test_concurrent_updates() {
    let ctx = Arc::new(ConcurrentTestContext::new("concurrent_updates").await);
    ctx.setup_test_table().await.unwrap();

    // Insert initial data
    let mut writer = CliResultWriter::new();
    let txn_ctx = ctx
        .transaction_factory
        .begin_transaction(IsolationLevel::RepeatableRead);
    let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
        ctx.buffer_pool_manager.clone(),
        ctx.catalog.clone(),
        txn_ctx.clone(),
    )));
    let insert_sql = "INSERT INTO test_table VALUES (1, 100);";
    let result = ctx
        .execution_engine
        .lock()
        .await
        .execute_sql(insert_sql, exec_ctx, &mut writer)
        .await;
    assert!(result.is_ok());
    assert!(ctx.transaction_factory.commit_transaction(txn_ctx).await);

    let thread_count = 5;
    let mut handles = vec![];

    for i in 0..thread_count {
        let ctx_clone = Arc::clone(&ctx);
        let handle = tokio::spawn(async move {
            let mut writer = CliResultWriter::new();
            let update_sql = format!("UPDATE test_table SET value = {} WHERE id = 1;", i * 100);

            let txn_ctx = ctx_clone
                .transaction_factory
                .begin_transaction(IsolationLevel::RepeatableRead);
            let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
                ctx_clone.buffer_pool_manager.clone(),
                ctx_clone.catalog.clone(),
                txn_ctx.clone(),
            )));
            let result = ctx_clone
                .execution_engine
                .lock()
                .await
                .execute_sql(&update_sql, exec_ctx, &mut writer)
                .await;

            match result {
                Ok(_) => {
                    ctx_clone
                        .transaction_factory
                        .commit_transaction(txn_ctx)
                        .await
                }
                Err(_) => {
                    ctx_clone.transaction_factory.abort_transaction(txn_ctx);
                    false
                }
            }
        });
        handles.push(handle);
    }

    let results: Vec<bool> = future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert!(
        results.iter().any(|&r| !r),
        "Expected some transactions to fail due to conflicts"
    );
}

#[tokio::test]
async fn test_deadlock_detection() {
    let ctx = Arc::new(ConcurrentTestContext::new("deadlock_detection").await);
    ctx.setup_test_table().await.unwrap();

    // Insert initial data
    let mut writer = CliResultWriter::new();
    let txn_ctx = ctx
        .transaction_factory
        .begin_transaction(IsolationLevel::RepeatableRead);
    let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
        ctx.buffer_pool_manager.clone(),
        ctx.catalog.clone(),
        txn_ctx.clone(),
    )));
    let insert_sql = "INSERT INTO test_table VALUES (1, 100), (2, 200);";
    let result = ctx
        .execution_engine
        .lock()
        .await
        .execute_sql(insert_sql, exec_ctx, &mut writer)
        .await;
    assert!(result.is_ok());
    assert!(ctx.transaction_factory.commit_transaction(txn_ctx).await);

    let ctx_clone1 = Arc::clone(&ctx);
    let ctx_clone2 = Arc::clone(&ctx);

    let handle1 = tokio::spawn(async move {
        let mut writer = CliResultWriter::new();
        let txn_ctx = ctx_clone1
            .transaction_factory
            .begin_transaction(IsolationLevel::RepeatableRead);
        let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
            ctx_clone1.buffer_pool_manager.clone(),
            ctx_clone1.catalog.clone(),
            txn_ctx.clone(),
        )));

        // Update first row
        let sql1 = "UPDATE test_table SET value = 150 WHERE id = 1;";
        let result1 = ctx_clone1
            .execution_engine
            .lock()
            .await
            .execute_sql(sql1, exec_ctx.clone(), &mut writer)
            .await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Try to update second row
        let sql2 = "UPDATE test_table SET value = 250 WHERE id = 2;";
        let result2 = ctx_clone1
            .execution_engine
            .lock()
            .await
            .execute_sql(sql2, exec_ctx, &mut writer)
            .await;

        match (result1, result2) {
            (Ok(_), Ok(_)) => {
                ctx_clone1
                    .transaction_factory
                    .commit_transaction(txn_ctx)
                    .await
            }
            _ => {
                ctx_clone1.transaction_factory.abort_transaction(txn_ctx);
                false
            }
        }
    });

    let handle2 = tokio::spawn(async move {
        let mut writer = CliResultWriter::new();
        let txn_ctx = ctx_clone2
            .transaction_factory
            .begin_transaction(IsolationLevel::RepeatableRead);
        let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
            ctx_clone2.buffer_pool_manager.clone(),
            ctx_clone2.catalog.clone(),
            txn_ctx.clone(),
        )));

        // Update second row
        let sql1 = "UPDATE test_table SET value = 250 WHERE id = 2;";
        let result1 = ctx_clone2
            .execution_engine
            .lock()
            .await
            .execute_sql(sql1, exec_ctx.clone(), &mut writer)
            .await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Try to update first row
        let sql2 = "UPDATE test_table SET value = 150 WHERE id = 1;";
        let result2 = ctx_clone2
            .execution_engine
            .lock()
            .await
            .execute_sql(sql2, exec_ctx, &mut writer)
            .await;

        match (result1, result2) {
            (Ok(_), Ok(_)) => {
                ctx_clone2
                    .transaction_factory
                    .commit_transaction(txn_ctx)
                    .await
            }
            _ => {
                ctx_clone2.transaction_factory.abort_transaction(txn_ctx);
                false
            }
        }
    });

    let result1 = handle1.await.unwrap();
    let result2 = handle2.await.unwrap();
    assert!(
        result1 != result2,
        "Expected one transaction to fail due to deadlock"
    );
}
