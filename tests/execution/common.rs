use crate::common::logger::init_test_logger;
use parking_lot::RwLock;
use std::sync::Arc;
use tempfile::TempDir;
use tkdb::buffer::buffer_pool_manager_async::BufferPoolManager;
use tkdb::buffer::lru_k_replacer::LRUKReplacer;
use tkdb::catalog::Catalog;
use tkdb::catalog::column::Column;
use tkdb::catalog::schema::Schema;
use tkdb::common::result_writer::ResultWriter;
use tkdb::concurrency::transaction::IsolationLevel;
use tkdb::concurrency::transaction_manager_factory::TransactionManagerFactory;
use tkdb::recovery::log_manager::LogManager;
use tkdb::recovery::wal_manager::WALManager;
use tkdb::sql::execution::execution_context::ExecutionContext;
use tkdb::sql::execution::execution_engine::ExecutionEngine;
use tkdb::storage::disk::async_disk::AsyncDiskManager;
use tkdb::storage::disk::async_disk::DiskManagerConfig;
use tkdb::storage::table::table_heap::TableInfo;
use tkdb::storage::table::transactional_table_heap::TransactionalTableHeap;
use tkdb::types_db::type_id::TypeId;
use tkdb::types_db::value::Value;

pub struct TestContext {
    pub engine: ExecutionEngine,
    catalog: Arc<RwLock<Catalog>>,
    pub exec_ctx: Arc<RwLock<ExecutionContext>>,
    _temp_dir: TempDir,
}

impl TestContext {
    pub async fn new(name: &str) -> Self {
        init_test_logger();
        // Increase buffer pool size for tests that need to handle more data
        let buffer_pool_size = if name.contains("bulk") {
            200 // Use larger pool size for bulk operations
        } else {
            10 // Default size for regular tests
        };
        const K: usize = 2;

        // Create temporary directory
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

        // Create disk components
        let disk_manager = AsyncDiskManager::new(
            db_path.clone(),
            log_path.clone(),
            DiskManagerConfig::default(),
        )
        .await;
        let disk_manager_arc = Arc::new(disk_manager.unwrap());
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(buffer_pool_size, K)));
        let bpm = Arc::new(
            BufferPoolManager::new(buffer_pool_size, disk_manager_arc.clone(), replacer.clone())
                .unwrap(),
        );

        let log_manager = Arc::new(RwLock::new(LogManager::new(disk_manager_arc.clone())));
        log_manager.write().run_flush_thread();

        // Create WAL manager with the log manager
        let wal_manager = Arc::new(WALManager::new(log_manager.clone()));

        // Create transaction factory with WAL manager
        let transaction_factory = Arc::new(TransactionManagerFactory::with_wal_manager(
            bpm.clone(),
            wal_manager.clone(),
        ));

        // Get the transaction manager from the factory to ensure consistency
        let txn_manager = transaction_factory.get_transaction_manager();

        let catalog = Arc::new(RwLock::new(Catalog::new(
            Arc::clone(&bpm),
            txn_manager.clone(),
        )));

        // Create a transaction using the factory's transaction manager
        let transaction_context =
            transaction_factory.begin_transaction(IsolationLevel::ReadUncommitted);

        let exec_ctx = Arc::new(RwLock::new(ExecutionContext::new(
            bpm.clone(),
            catalog.clone(),
            transaction_context.clone(),
        )));

        let engine = ExecutionEngine::new(
            catalog.clone(),
            bpm.clone(),
            transaction_factory,
            wal_manager,
        );

        Self {
            engine,
            catalog,
            exec_ctx,
            _temp_dir: temp_dir,
        }
    }

    pub fn create_test_table(&self, table_name: &str, schema: Schema) -> Result<TableInfo, String> {
        let mut catalog = self.catalog.write();
        Ok(catalog
            .create_table(table_name.to_string(), schema.clone())
            .unwrap())
    }

    pub fn insert_tuples(
        &self,
        table_name: &str,
        tuples: Vec<Vec<Value>>,
        schema: Schema,
    ) -> Result<(), String> {
        let catalog = self.catalog.read();
        let table = catalog.get_table(table_name).unwrap();
        let table_heap = table.get_table_heap();

        // Create a transactional wrapper around the table heap
        let transactional_table_heap =
            TransactionalTableHeap::new(table_heap.clone(), table.get_table_oidt());

        // Use the current transaction context instead of hardcoded transaction ID
        let txn_ctx = self.exec_ctx.read().get_transaction_context();
        for values in tuples {
            transactional_table_heap
                .insert_tuple_from_values(values, &schema, txn_ctx.clone())
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

pub struct TestResultWriter {
    schema: Option<Schema>,
    rows: Vec<Vec<Value>>,
}

impl TestResultWriter {
    pub fn new() -> Self {
        Self {
            schema: None,
            rows: Vec::new(),
        }
    }

    pub fn get_rows(&self) -> &Vec<Vec<Value>> {
        &self.rows
    }
}

impl ResultWriter for TestResultWriter {
    fn write_schema_header(&mut self, columns: Vec<String>) {
        let schema_columns = columns
            .into_iter()
            .map(|name| Column::new(&name, TypeId::VarChar))
            .collect();
        self.schema = Some(Schema::new(schema_columns));
    }

    fn write_row(&mut self, values: Vec<Value>) {
        self.rows.push(values);
    }

    fn write_row_with_schema(&mut self, values: Vec<Value>, schema: &Schema) {
        // Store the schema for potential future use
        self.schema = Some(schema.clone());
        self.rows.push(values);
    }

    fn write_message(&mut self, message: &str) {
        println!("{}", message);
    }
}
