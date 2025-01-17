use parking_lot::RwLock;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tkdb::buffer::buffer_pool_manager::BufferPoolManager;
use tkdb::buffer::lru_k_replacer::LRUKReplacer;
use tkdb::catalog::catalog::Catalog;
use tkdb::common::result_writer::CliResultWriter;
use tkdb::concurrency::transaction::IsolationLevel;
use tkdb::concurrency::transaction_manager_factory::TransactionManagerFactory;
use tkdb::sql::execution::execution_context::ExecutionContext;
use tkdb::sql::execution::execution_engine::ExecutionEngine;
use tkdb::recovery::log_manager::LogManager;
use tkdb::storage::disk::disk_manager::FileDiskManager;
use tkdb::storage::disk::disk_scheduler::DiskScheduler;

pub struct ConcurrentTestContext {
    catalog: Arc<RwLock<Catalog>>,
    buffer_pool_manager: Arc<BufferPoolManager>,
    transaction_factory: Arc<TransactionManagerFactory>,
    execution_engine: Arc<RwLock<ExecutionEngine>>,
    test_name: String,
}

impl ConcurrentTestContext {
    pub fn new(name: &str) -> Self {
        let test_name = name.to_string();
        
        // Initialize components
        let disk_manager = Arc::new(FileDiskManager::new(
            format!("tests/data/{}_db.db", name),
            format!("tests/data/{}_log.log", name),
            4096,
        ));
        
        let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));
        
        let buffer_pool_manager = Arc::new(BufferPoolManager::new(
            32,
            disk_scheduler.clone(),
            disk_manager.clone(),
            Arc::new(RwLock::new(LRUKReplacer::new(32, 2))),
        ));

        let catalog = Arc::new(RwLock::new(Catalog::new(
            buffer_pool_manager.clone(),
            0,
            0,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
        )));

        let log_manager = Arc::new(RwLock::new(LogManager::new(disk_manager)));
        
        let transaction_factory = Arc::new(TransactionManagerFactory::new(
            catalog.clone(),
            log_manager,
        ));

        let execution_engine = Arc::new(RwLock::new(ExecutionEngine::new(
            catalog.clone(),
            buffer_pool_manager.clone(),
            transaction_factory.clone(),
        )));

        Self {
            catalog,
            buffer_pool_manager,
            transaction_factory,
            execution_engine,
            test_name,
        }
    }

    pub fn setup_test_table(&self) -> Result<(), String> {
        let mut writer = CliResultWriter::new();
        let create_sql = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value INTEGER);";
        
        let txn_ctx = self.transaction_factory.begin_transaction(IsolationLevel::RepeatableRead);
        let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
            self.buffer_pool_manager.clone(),
            self.catalog.clone(),
            txn_ctx.clone(),
        )));

        let result = self.execution_engine
            .write()
            .execute_sql(create_sql, exec_ctx, &mut writer)
            .map_err(|e| format!("Failed to create table: {}", e))?;

        if result {
            self.transaction_factory.commit_transaction(txn_ctx);
        } else {
            self.transaction_factory.abort_transaction(txn_ctx);
        }

        Ok(())
    }
}

#[test]
fn test_concurrent_inserts() {
    let ctx = Arc::new(ConcurrentTestContext::new("concurrent_inserts"));
    ctx.setup_test_table().unwrap();

    let thread_count = 10;
    let mut handles = vec![];

    // Spawn multiple threads to perform concurrent inserts
    for i in 0..thread_count {
        let ctx_clone = Arc::clone(&ctx);
        let handle = thread::spawn(move || {
            let mut writer = CliResultWriter::new();
            let insert_sql = format!("INSERT INTO test_table VALUES ({}, {});", i, i * 100);
            
            let txn_ctx = ctx_clone.transaction_factory.begin_transaction(IsolationLevel::RepeatableRead);
            let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
                ctx_clone.buffer_pool_manager.clone(),
                ctx_clone.catalog.clone(),
                txn_ctx.clone(),
            )));

            let result = ctx_clone
                .execution_engine
                .write()
                .execute_sql(&insert_sql, exec_ctx, &mut writer);

            match result {
                Ok(_) => ctx_clone.transaction_factory.commit_transaction(txn_ctx),
                Err(_) => {
                    ctx_clone.transaction_factory.abort_transaction(txn_ctx);
                    false
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    let results: Vec<bool> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert!(results.iter().any(|&r| r), "All transactions failed");

    // Verify the results
    let mut writer = CliResultWriter::new();
    let select_sql = "SELECT COUNT(*) FROM test_table;";
    let txn_ctx = ctx.transaction_factory.begin_transaction(IsolationLevel::RepeatableRead);
    let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
        ctx.buffer_pool_manager.clone(),
        ctx.catalog.clone(),
        txn_ctx.clone(),
    )));
    let result = ctx
        .execution_engine
        .write()
        .execute_sql(select_sql, exec_ctx, &mut writer);
    
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_concurrent_updates() {
    let ctx = Arc::new(ConcurrentTestContext::new("concurrent_updates"));
    ctx.setup_test_table().unwrap();

    // Insert initial data
    let mut writer = CliResultWriter::new();
    let txn_ctx = ctx.transaction_factory.begin_transaction(IsolationLevel::RepeatableRead);
    let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
        ctx.buffer_pool_manager.clone(),
        ctx.catalog.clone(),
        txn_ctx.clone(),
    )));
    let insert_sql = "INSERT INTO test_table VALUES (1, 100);";
    let result = ctx
        .execution_engine
        .write()
        .execute_sql(insert_sql, exec_ctx, &mut writer);
    assert!(result.is_ok());
    assert!(ctx.transaction_factory.commit_transaction(txn_ctx));

    let thread_count = 5;
    let mut handles = vec![];

    for i in 0..thread_count {
        let ctx_clone = Arc::clone(&ctx);
        let handle = thread::spawn(move || {
            let mut writer = CliResultWriter::new();
            let update_sql = format!("UPDATE test_table SET value = {} WHERE id = 1;", i * 100);

            let txn_ctx = ctx_clone.transaction_factory.begin_transaction(IsolationLevel::RepeatableRead);
            let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
                ctx_clone.buffer_pool_manager.clone(),
                ctx_clone.catalog.clone(),
                txn_ctx.clone(),
            )));
            let result = ctx_clone
                .execution_engine
                .write()
                .execute_sql(&update_sql, exec_ctx, &mut writer);

            match result {
                Ok(_) => ctx_clone.transaction_factory.commit_transaction(txn_ctx),
                Err(_) => {
                    ctx_clone.transaction_factory.abort_transaction(txn_ctx);
                    false
                }
            }
        });
        handles.push(handle);
    }

    let results: Vec<bool> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert!(results.iter().any(|&r| !r), "Expected some transactions to fail due to conflicts");
}

#[test]
#[ignore]
fn test_deadlock_detection() {
    let ctx = Arc::new(ConcurrentTestContext::new("deadlock_detection"));
    ctx.setup_test_table().unwrap();

    // Insert initial data
    let mut writer = CliResultWriter::new();
    let txn_ctx = ctx.transaction_factory.begin_transaction(IsolationLevel::RepeatableRead);
    let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
        ctx.buffer_pool_manager.clone(),
        ctx.catalog.clone(),
        txn_ctx.clone(),
    )));
    let insert_sql = "INSERT INTO test_table VALUES (1, 100), (2, 200);";
    let result = ctx
        .execution_engine
        .write()
        .execute_sql(insert_sql, exec_ctx, &mut writer);
    assert!(result.is_ok());
    assert!(ctx.transaction_factory.commit_transaction(txn_ctx));

    let ctx_clone1 = Arc::clone(&ctx);
    let ctx_clone2 = Arc::clone(&ctx);

    let handle1 = thread::spawn(move || {
        let mut writer = CliResultWriter::new();
        let txn_ctx = ctx_clone1.transaction_factory.begin_transaction(IsolationLevel::RepeatableRead);
        let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
            ctx_clone1.buffer_pool_manager.clone(),
            ctx_clone1.catalog.clone(),
            txn_ctx.clone(),
        )));
        
        // Update first row
        let sql1 = "UPDATE test_table SET value = 150 WHERE id = 1;";
        let result1 = ctx_clone1
            .execution_engine
            .write()
            .execute_sql(sql1, exec_ctx.clone(), &mut writer);
        
        thread::sleep(Duration::from_millis(100));
        
        // Try to update second row
        let sql2 = "UPDATE test_table SET value = 250 WHERE id = 2;";
        let result2 = ctx_clone1
            .execution_engine
            .write()
            .execute_sql(sql2, exec_ctx, &mut writer);

        match (result1, result2) {
            (Ok(_), Ok(_)) => ctx_clone1.transaction_factory.commit_transaction(txn_ctx),
            _ => {
                ctx_clone1.transaction_factory.abort_transaction(txn_ctx);
                false
            }
        }
    });

    let handle2 = thread::spawn(move || {
        let mut writer = CliResultWriter::new();
        let txn_ctx = ctx_clone2.transaction_factory.begin_transaction(IsolationLevel::RepeatableRead);
        let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
            ctx_clone2.buffer_pool_manager.clone(),
            ctx_clone2.catalog.clone(),
            txn_ctx.clone(),
        )));

        // Update second row
        let sql1 = "UPDATE test_table SET value = 250 WHERE id = 2;";
        let result1 = ctx_clone2
            .execution_engine
            .write()
            .execute_sql(sql1, exec_ctx.clone(), &mut writer);
        
        thread::sleep(Duration::from_millis(100));
        
        // Try to update first row
        let sql2 = "UPDATE test_table SET value = 150 WHERE id = 1;";
        let result2 = ctx_clone2
            .execution_engine
            .write()
            .execute_sql(sql2, exec_ctx, &mut writer);

        match (result1, result2) {
            (Ok(_), Ok(_)) => ctx_clone2.transaction_factory.commit_transaction(txn_ctx),
            _ => {
                ctx_clone2.transaction_factory.abort_transaction(txn_ctx);
                false
            }
        }
    });

    let result1 = handle1.join().unwrap();
    let result2 = handle2.join().unwrap();
    assert!(
        result1 != result2,
        "Expected one transaction to fail due to deadlock"
    );
} 