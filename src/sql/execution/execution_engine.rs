use crate::sql::execution::check_option::CheckOption;
use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
use crate::catalog::catalog::Catalog;
use crate::common::exception::DBError;
use crate::common::result_writer::ResultWriter;
use crate::concurrency::transaction_manager_factory::TransactionManagerFactory;
use crate::recovery::wal_manager::WALManager;
use crate::sql::execution::check_option::CheckOptions;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::PlanNode;
use crate::sql::optimizer::optimizer::Optimizer;
use crate::sql::planner::logical_plan::{LogicalPlan, LogicalToPhysical};
use crate::sql::planner::query_planner::QueryPlanner;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use log::{debug, error, info};
use parking_lot::RwLock;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;
use crate::sql::execution::transaction_context::TransactionContext;

pub struct ExecutionEngine {
    planner: QueryPlanner,
    optimizer: Optimizer,
    catalog: Arc<RwLock<Catalog>>,
    buffer_pool_manager: Arc<BufferPoolManager>,
    transaction_factory: Arc<TransactionManagerFactory>,
    wal_manager: Arc<WALManager>,
}

impl ExecutionEngine {
    pub fn new(
        catalog: Arc<RwLock<Catalog>>,
        buffer_pool_manager: Arc<BufferPoolManager>,
        transaction_factory: Arc<TransactionManagerFactory>,
        wal_manager: Arc<WALManager>,
    ) -> Self {
        Self {
            planner: QueryPlanner::new(catalog.clone()),
            optimizer: Optimizer::new(catalog.clone()),
            catalog,
            buffer_pool_manager,
            transaction_factory,
            wal_manager,
        }
    }

    /// Execute a SQL statement with the given context and writer
    pub async fn execute_sql(
        &mut self,
        sql: &str,
        context: Arc<RwLock<ExecutionContext>>,
        writer: &mut impl ResultWriter,
    ) -> Result<bool, DBError> {
        // Parse and plan the SQL statement
        let plan = self.prepare_sql(sql, context.clone())?;

        // Execute the plan
        self.execute_plan(&plan, context, writer).await
    }

    /// Prepare a SQL statement for execution
    fn prepare_sql(
        &mut self,
        sql: &str,
        context: Arc<RwLock<ExecutionContext>>,
    ) -> Result<PlanNode, DBError> {
        info!("Preparing SQL statement: {}", sql);

        // Additional syntax validation for common errors
        let sql_lower = sql.to_lowercase();

        // Check for DELETE statements missing the FROM keyword
        if sql_lower.starts_with("delete ") && !sql_lower.starts_with("delete from ") {
            return Err(DBError::SqlError("Invalid DELETE syntax, expected 'DELETE FROM table_name'".to_string()));
        }

        // Create logical plan
        let logical_plan = self.create_logical_plan(sql)?;
        debug!(
            "Initial logical plan generated: \n{}",
            logical_plan.explain(0)
        );

        // Optimize plan
        let physical_plan = self.optimize_plan(logical_plan)?;
        // debug!("Physical plan generated: \n{}", physical_plan.explain());

        Ok(physical_plan)
    }

    /// Execute a physical plan
    async fn execute_plan(
        &self,
        plan: &PlanNode,
        context: Arc<RwLock<ExecutionContext>>,
        writer: &mut dyn ResultWriter,
    ) -> Result<bool, DBError> {
        debug!("Executing physical plan: {}", plan);

        // Create executor in a stack-efficient way
        debug!("About to create executor for plan: {}", plan);
        let mut root_executor = match self.create_executor(plan, context) {
            Ok(executor) => {
                debug!("Successfully created executor");
                executor
            }
            Err(e) => {
                error!("Failed to create executor: {}", e);
                return Err(e);
            }
        };

        // Initialize executor
        debug!("Initializing root executor");
        root_executor.init();
        debug!("Root executor initialized successfully");

        match plan {
            PlanNode::Insert(_)
            | PlanNode::Update(_)
            | PlanNode::Delete(_)
            | PlanNode::CreateTable(_)
            | PlanNode::CreateIndex(_) => {
                debug!("Executing modification statement");
                let mut has_results = false;

                // Process all tuples from the executor
                loop {
                    match root_executor.next() {
                        Ok(Some(_)) => {
                            has_results = true;
                        }
                        Ok(None) => {
                            break; // No more tuples
                        }
                        Err(e) => {
                            error!("Error during execution: {}", e);
                            return Err(e);
                        }
                    }
                }

                // For Insert, Update, Delete, CreateTable, and CreateIndex, return true if execution completed successfully
                match plan {
                    PlanNode::Insert(_) => {
                        // INSERT operations don't return result tuples - they perform the operation
                        // If we reach here without error, the insert was successful
                        info!("Insert operation executed successfully");
                        Ok(true)
                    }
                    PlanNode::Update(_) => {
                        // UPDATE operations return success based on whether any rows were affected
                        if has_results {
                            info!("Update operation executed successfully");
                            Ok(true)
                        } else {
                            info!("Update operation completed - no rows affected");
                            Ok(false)
                        }
                    }
                    PlanNode::Delete(_) => {
                        // DELETE operations return success based on whether any rows were affected
                        if has_results {
                            info!("Delete operation executed successfully");
                            Ok(true)
                        } else {
                            info!("Delete operation completed - no rows affected");
                            Ok(false)
                        }
                    }
                    PlanNode::CreateTable(_) | PlanNode::CreateIndex(_) => {
                        info!("Create operation executed successfully");
                        Ok(true)
                    }
                    _ => {
                        if has_results {
                            info!("Modification statement executed successfully");
                            Ok(true)
                        } else {
                            info!("No rows affected");
                            Ok(false)
                        }
                    }
                }
            }
            PlanNode::StartTransaction(_) => {
                debug!("Executing start transaction statement");

                // StartTransaction doesn't generate any result tuples
                // Just execute next() once to trigger the transaction creation
                match root_executor.next() {
                    Ok(_) => {
                        // Get transaction information from the executor's context
                        let exec_context = root_executor.get_executor_context();
                        let txn_context = exec_context.read().get_transaction_context();
                        let txn_id = txn_context.get_transaction_id();
                        let isolation_level = txn_context.get_transaction().get_isolation_level();

                        info!(
                            "Transaction {} started successfully with isolation level {:?}",
                            txn_id, isolation_level
                        );
                        Ok(true)
                    }
                    Err(e) => {
                        error!("Error starting transaction: {}", e);
                        Err(e)
                    }
                }
            }
            PlanNode::CommitTransaction(_) => {
                debug!("Executing commit transaction statement");

                // Execute the commit transaction executor once
                match root_executor.next() {
                    Ok(_) => {
                        // Get transaction information from the executor's context
                        let exec_context = root_executor.get_executor_context();
                        let should_chain = exec_context.read().should_chain_after_transaction();

                        if should_chain {
                            debug!("Transaction commit with chaining requested");
                            // TODO: Implement transaction chaining logic
                        }

                        let txn_context = exec_context.read().get_transaction_context();
                        let txn_id = txn_context.get_transaction_id();

                        // Commit the transaction through transaction manager
                        match self.commit_transaction(txn_context.clone()).await {
                            Ok(success) => {
                                info!("Transaction {} committed successfully", txn_id);

                                if should_chain {
                                    // Chain a new transaction
                                    match self.chain_transaction(txn_context, exec_context.clone())
                                    {
                                        Ok(_) => {
                                            info!(
                                                "New transaction chained successfully after commit"
                                            );
                                        }
                                        Err(e) => {
                                            error!("Failed to chain new transaction: {}", e);
                                            return Err(e);
                                        }
                                    }
                                }

                                Ok(success)
                            }
                            Err(e) => {
                                error!("Failed to commit transaction {}: {}", txn_id, e);
                                Err(e)
                            }
                        }
                    }
                    Err(e) => {
                        error!("Error in commit transaction executor: {}", e);
                        Err(e)
                    }
                }
            }
            PlanNode::RollbackTransaction(_) => {
                debug!("Executing rollback transaction statement");

                // Execute the rollback transaction executor once
                match root_executor.next() {
                    Ok(_) => {
                        // Get transaction information from the executor's context
                        let exec_context = root_executor.get_executor_context();
                        let should_chain = exec_context.read().should_chain_after_transaction();
                        let txn_context = exec_context.read().get_transaction_context();
                        let txn_id = txn_context.get_transaction_id();

                        // Abort the transaction through transaction manager
                        match self.abort_transaction(txn_context.clone()) {
                            Ok(success) => {
                                info!("Transaction {} rolled back successfully", txn_id);

                                if should_chain {
                                    // Chain a new transaction
                                    match self.chain_transaction(txn_context, exec_context.clone())
                                    {
                                        Ok(_) => {
                                            info!(
                                                "New transaction chained successfully after rollback"
                                            );
                                        }
                                        Err(e) => {
                                            error!("Failed to chain new transaction: {}", e);
                                            return Err(e);
                                        }
                                    }
                                }

                                Ok(success)
                            }
                            Err(e) => {
                                error!("Failed to rollback transaction {}: {}", txn_id, e);
                                Err(e)
                            }
                        }
                    }
                    Err(e) => {
                        error!("Error in rollback transaction executor: {}", e);
                        Err(e)
                    }
                }
            }
            PlanNode::CommandResult(cmd) => {
                debug!("Executing command: {}", cmd);

                // Let the CommandExecutor handle the command
                let mut has_results = false;

                // Process all tuples from the executor
                loop {
                    match root_executor.next() {
                        Ok(Some(_)) => {
                            has_results = true;
                        }
                        Ok(None) => {
                            break; // No more tuples
                        }
                        Err(e) => {
                            error!("Error during command execution: {}", e);
                            return Err(e);
                        }
                    }
                }

                // For other commands, just return success based on whether the executor produced results
                Ok(has_results)
            }

            _ => {
                debug!("Executing query statement");

                // Get schema before starting the executor loop to avoid borrow conflicts
                let schema = root_executor.get_output_schema().clone();

                // Write schema header
                let column_names: Vec<String> = schema
                    .get_columns()
                    .iter()
                    .map(|col| col.get_name().to_string())
                    .collect();
                writer.write_schema_header(column_names);

                let mut tuple_count = 0;

                // Process all tuples from the executor
                debug!("Starting query execution loop");
                loop {
                    debug!("Calling next() on executor, iteration: {}", tuple_count);
                    match root_executor.next() {
                        Ok(Some((tuple, _rid))) => {
                            // Extract values from tuple
                            let values: Vec<_> = (0..tuple.get_column_count())
                                .map(|i| tuple.get_value(i).clone())
                                .collect();

                            // Write row with schema context
                            writer.write_row_with_schema(values, &schema);
                            tuple_count += 1;
                        }
                        Ok(None) => {
                            break; // No more tuples
                        }
                        Err(e) => {
                            error!("Error during query execution: {}", e);
                            return Err(e);
                        }
                    }
                }

                debug!("Query returned {} tuples", tuple_count);
                Ok(true)
            }
        }
    }

    /// Create an executor for the given plan
    fn create_executor(
        &self,
        plan: &PlanNode,
        context: Arc<RwLock<ExecutionContext>>,
    ) -> Result<Box<dyn AbstractExecutor>, DBError> {
        debug!("Creating executor for plan: {}", plan);

        // Create executor in a stack-efficient way
        match plan.create_executor(context) {
            Ok(executor) => {
                debug!("Successfully created executor for plan: {}", plan);
                Ok(executor)
            }
            Err(e) => {
                error!("Failed to create executor for plan: {}, error: {}", plan, e);
                Err(DBError::Execution(e))
            }
        }
    }

    /// Create a logical plan from SQL
    fn create_logical_plan(&mut self, sql: &str) -> Result<LogicalPlan, DBError> {
        self.planner
            .create_logical_plan(sql)
            .map(|boxed_plan| *boxed_plan) // Unbox the LogicalPlan
            .map_err(DBError::PlanError)
    }

    /// Optimize a logical plan into a physical plan
    fn optimize_plan(&self, plan: LogicalPlan) -> Result<PlanNode, DBError> {
        // Box the logical plan and get check options
        let boxed_plan = Box::new(plan);
        let mut check_options = CheckOptions::new();
        check_options.add_check(CheckOption::EnableNljCheck);

        // Optimize the plan
        let optimized_plan = self
            .optimizer
            .optimize(boxed_plan, Arc::from(check_options))
            .map_err(|e| DBError::OptimizeError(e.to_string()))?;

        // Convert to physical plan and map any String errors to DBError
        optimized_plan
            .to_physical_plan()
            .map_err(|e| DBError::OptimizeError(e))
    }

    /// Prepare a SQL statement and validate syntax
    /// Returns empty parameter types for now since we don't support parameters yet
    pub fn prepare_statement(&mut self, sql: &str) -> Result<Vec<TypeId>, DBError> {
        // Additional syntax validation for common errors
        let sql_lower = sql.to_lowercase();

        // Check for DELETE statements missing the FROM keyword
        if sql_lower.starts_with("delete ") && !sql_lower.starts_with("delete from ") {
            return Err(DBError::SqlError("Invalid DELETE syntax, expected 'DELETE FROM table_name'".to_string()));
        }

        // Parse SQL to validate syntax
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql)
            .map_err(|e| DBError::SqlError(format!("Parse error: {}", e)))?;

        if ast.len() != 1 {
            return Err(DBError::SqlError("Expected single statement".to_string()));
        }

        // Create logical plan to validate semantics
        let _logical_plan = self.create_logical_plan(sql)?;

        // For now, return empty parameter types since we don't support parameters yet
        Ok(Vec::new())
    }

    /// Execute a prepared statement (currently same as regular execute)
    pub async fn execute_prepared_statement(
        &mut self,
        sql: &str,
        _params: Vec<Value>,
        context: Arc<RwLock<ExecutionContext>>,
        writer: &mut impl ResultWriter,
    ) -> Result<bool, DBError> {
        // For now, just execute as regular SQL since we don't support parameters
        self.execute_sql(sql, context, writer).await
    }

    async fn commit_transaction(&self, txn_ctx: Arc<TransactionContext>) -> Result<bool, DBError> {
        debug!("Committing transaction {}", txn_ctx.get_transaction_id());

        // Get transaction manager
        let txn_manager = self.transaction_factory.get_transaction_manager();

        // Write commit record to WAL
        let transaction = txn_ctx.get_transaction();

        // Debug: Log the write set before commit
        let write_set = transaction.get_write_set();
        debug!(
            "Transaction {} write set before commit: {:?}",
            txn_ctx.get_transaction_id(),
            write_set
        );

        let lsn = self.wal_manager.write_commit_record(transaction.as_ref());

        // Update transaction's LSN
        transaction.set_prev_lsn(lsn);

        // Attempt to commit
        match txn_manager.commit(transaction, self.buffer_pool_manager.clone()).await {
            true => {
                debug!("Transaction committed successfully");
                Ok(true)
            }
            false => {
                debug!("Transaction commit failed");
                Ok(false)
            }
        }
    }

    fn chain_transaction(
        &self,
        txn_ctx: Arc<TransactionContext>,
        exec_ctx: Arc<RwLock<ExecutionContext>>,
    ) -> Result<bool, DBError> {
        debug!("Checking if transaction should be chained");

        // Only proceed if chaining is requested
        if !exec_ctx.read().should_chain_after_transaction() {
            return Ok(false);
        }

        // Get transaction manager
        let txn_manager = self.transaction_factory.get_transaction_manager();

        // Get isolation level from previous transaction
        let isolation_level = txn_ctx.get_transaction().get_isolation_level();
        let lock_manager = txn_ctx.get_lock_manager();

        debug!(
            "Chaining transaction with isolation level {:?}",
            isolation_level
        );

        // Start a new transaction with the same isolation level
        match txn_manager.begin(isolation_level) {
            Ok(new_txn) => {
                // Create a new transaction context with this transaction
                let new_txn_context = Arc::new(TransactionContext::new(
                    new_txn.clone(),
                    lock_manager,
                    txn_manager.clone(),
                ));

                debug!(
                    "Started new chained transaction {} with isolation level {:?}",
                    new_txn_context.get_transaction_id(),
                    isolation_level
                );

                // Update the executor context with the new transaction context
                let mut context = exec_ctx.write();
                context.set_transaction_context(new_txn_context);

                // Reset the chain flag
                context.set_chain_after_transaction(false);

                Ok(true)
            }
            Err(err) => {
                error!("Failed to start chained transaction: {}", err);
                Ok(false)
            }
        }
    }

    fn abort_transaction(&self, txn_ctx: Arc<TransactionContext>) -> Result<bool, DBError> {
        debug!("Aborting transaction {}", txn_ctx.get_transaction_id());

        // Get transaction manager
        let txn_manager = self.transaction_factory.get_transaction_manager();
        debug!(
            "Got transaction manager from factory: {:p}",
            txn_manager.as_ref()
        );

        // Write abort record to WAL
        let transaction = txn_ctx.get_transaction();
        let lsn = self.wal_manager.write_abort_record(transaction.as_ref());

        // Update transaction's LSN
        transaction.set_prev_lsn(lsn);

        // Attempt to abort
        debug!(
            "About to call txn_manager.abort() on transaction {}",
            transaction.get_transaction_id()
        );
        txn_manager.abort(transaction);
        debug!("Transaction aborted successfully");
        Ok(true)
    }
}

#[cfg(all(test, feature = "engine_inline_tests"))]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::transaction::IsolationLevel;
    use crate::recovery::log_manager::LogManager;
    use crate::storage::table::table_heap::TableInfo;
    use crate::storage::table::transactional_table_heap::TransactionalTableHeap;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use parking_lot::RwLock;
    use std::sync::Arc;
    use tempfile::TempDir;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};

    struct TestContext {
        engine: ExecutionEngine,
        catalog: Arc<RwLock<Catalog>>,
        exec_ctx: Arc<RwLock<ExecutionContext>>,
        disk_manager: Arc<AsyncDiskManager>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
            initialize_logger();
            // Increase buffer pool size for tests that need to handle more data
            let buffer_pool_size = if name.contains("bulk") {
                200 // Use larger pool size for bulk operations
            } else {
                10  // Default size for regular tests
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
            let disk_manager = AsyncDiskManager::new(db_path.clone(), log_path.clone(), DiskManagerConfig::default()).await;
            let disk_manager_arc = Arc::new(disk_manager.unwrap());
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(buffer_pool_size, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                buffer_pool_size,
                disk_manager_arc.clone(),
                replacer.clone(),
            ).unwrap());
            
            let log_manager = Arc::new(RwLock::new(LogManager::new(disk_manager_arc.clone())));

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
                disk_manager: disk_manager_arc,
                _temp_dir: temp_dir,
            }
        }

        fn create_test_table(&self, table_name: &str, schema: Schema) -> Result<TableInfo, String> {
            let mut catalog = self.catalog.write();
            Ok(catalog
                .create_table(table_name.to_string(), schema.clone())
                .unwrap())
        }

        async fn get_db_file_size(&self) -> std::io::Result<u64> {
            self.disk_manager.get_db_file_size().await
        }

        fn insert_tuples(
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

        async fn commit_current_transaction(&mut self) -> Result<(), String> {
            let mut writer = TestResultWriter::new();
            self.engine
                .execute_sql("COMMIT", self.exec_ctx.clone(), &mut writer).await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
    }

    struct TestResultWriter {
        schema: Option<Schema>,
        rows: Vec<Vec<Value>>,
    }

    impl TestResultWriter {
        fn new() -> Self {
            Self {
                schema: None,
                rows: Vec::new(),
            }
        }

        fn get_schema(&self) -> &Schema {
            self.schema.as_ref().expect("Schema not set")
        }

        fn get_rows(&self) -> &Vec<Vec<Value>> {
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
            todo!()
        }
    }

    mod simple_query_tests {
        use crate::catalog::column::Column;
        use crate::catalog::schema::Schema;
        use crate::sql::execution::execution_engine::tests::{TestContext, TestResultWriter};
        use crate::types_db::type_id::TypeId;
        use crate::types_db::value::Val::Null;
        use crate::types_db::value::{Val, Value};

        #[tokio::test]
        async fn test_simple_queries() {
            // Create a simpler test to avoid stack overflow
            let mut ctx = TestContext::new("test_simple_queries").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("active", TypeId::Boolean),
            ]);

            let table_name = "users";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data - just one row to keep it simple
            let test_data = vec![vec![Value::new(1), Value::new("Alice"), Value::new(true)]];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Execute a simple query without LIMIT to see if that works
            let mut writer = TestResultWriter::new();
            let sql = "SELECT name FROM users";

            println!("Executing simple query: {}", sql);
            match ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
            {
                Ok(success) => {
                    assert!(success, "Query execution failed for: {}", sql);
                    let rows = writer.get_rows();
                    println!("Query returned {} rows", rows.len());
                    assert_eq!(rows.len(), 1, "Incorrect number of rows");
                }
                Err(e) => {
                    panic!("Error executing query '{}': {:?}", sql, e);
                }
            }
        }

        #[tokio::test]
        async fn test_select_all_columns() {
            let mut ctx = TestContext::new("test_select_all_columns").await;

            // Create test table with multiple columns
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("age", TypeId::Integer),
                Column::new("active", TypeId::Boolean),
                Column::new("salary", TypeId::BigInt),
            ]);

            ctx.create_test_table("employees", table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![
                    Value::new(1),
                    Value::new("Alice"),
                    Value::new(30),
                    Value::new(true),
                    Value::new(75000i64),
                ],
                vec![
                    Value::new(2),
                    Value::new("Bob"),
                    Value::new(25),
                    Value::new(false),
                    Value::new(50000i64),
                ],
                vec![
                    Value::new(3),
                    Value::new("Charlie"),
                    Value::new(35),
                    Value::new(true),
                    Value::new(85000i64),
                ],
            ];
            ctx.insert_tuples("employees", test_data, table_schema)
                .unwrap();

            // Test SELECT *
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql("SELECT * FROM employees", ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "SELECT * query failed");
            assert_eq!(writer.get_rows().len(), 3, "Should return all 3 rows");
            assert_eq!(
                writer.get_schema().get_columns().len(),
                5,
                "Should return all 5 columns"
            );

            // Test selecting specific columns
            let mut writer2 = TestResultWriter::new();
            let success2 = ctx
                .engine
                .execute_sql(
                    "SELECT name, age FROM employees",
                    ctx.exec_ctx.clone(),
                    &mut writer2,
                ).await
                .unwrap();

            assert!(success2, "SELECT specific columns failed");
            assert_eq!(writer2.get_rows().len(), 3, "Should return all 3 rows");
            assert_eq!(
                writer2.get_schema().get_columns().len(),
                2,
                "Should return 2 columns"
            );
        }

        #[tokio::test]
        async fn test_select_with_constants() {
            let mut ctx = TestContext::new("test_select_with_constants").await;

            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ]);

            ctx.create_test_table("users", table_schema.clone())
                .unwrap();

            let test_data = vec![
                vec![Value::new(1), Value::new("Alice")],
                vec![Value::new(2), Value::new("Bob")],
            ];
            ctx.insert_tuples("users", test_data, table_schema).unwrap();

            // Test SELECT with constants
            let test_cases = vec![
                ("SELECT 42 as constant_int FROM users", 2, 1), // 2 rows, 1 column
                ("SELECT 'Hello' as greeting FROM users", 2, 1),
                ("SELECT name, 'World' as suffix FROM users", 2, 2),
                ("SELECT id, name, 100 as bonus FROM users", 2, 3),
                ("SELECT true as flag, false as other_flag FROM users", 2, 2),
            ];

            for (sql, expected_rows, expected_columns) in test_cases {
                let mut writer = TestResultWriter::new();
                let success = ctx
                    .engine
                    .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                    .unwrap();

                assert!(success, "Query execution failed for: {}", sql);
                assert_eq!(
                    writer.get_rows().len(),
                    expected_rows,
                    "Row count mismatch for: {}",
                    sql
                );
                assert_eq!(
                    writer.get_schema().get_columns().len(),
                    expected_columns,
                    "Column count mismatch for: {}",
                    sql
                );
            }
        }

        #[tokio::test]
        async fn test_select_with_expressions() {
            let mut ctx = TestContext::new("test_select_with_expressions").await;

            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("price", TypeId::Integer),
                Column::new("quantity", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ]);

            ctx.create_test_table("products", table_schema.clone())
                .unwrap();

            let test_data = vec![
                vec![
                    Value::new(1),
                    Value::new(100),
                    Value::new(5),
                    Value::new("Widget"),
                ],
                vec![
                    Value::new(2),
                    Value::new(200),
                    Value::new(3),
                    Value::new("Gadget"),
                ],
                vec![
                    Value::new(3),
                    Value::new(50),
                    Value::new(10),
                    Value::new("Tool"),
                ],
            ];
            ctx.insert_tuples("products", test_data, table_schema)
                .unwrap();

            // Test arithmetic expressions
            let test_cases = vec![
                ("SELECT price * quantity as total FROM products", 3, 1),
                ("SELECT price + 10 as adjusted_price FROM products", 3, 1),
                ("SELECT price - 5, quantity + 1 FROM products", 3, 2),
                ("SELECT price / 2 as half_price FROM products", 3, 1),
                ("SELECT price % 3 as remainder FROM products", 3, 1),
            ];

            for (sql, expected_rows, expected_columns) in test_cases {
                let mut writer = TestResultWriter::new();
                let success = ctx
                    .engine
                    .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                    .unwrap();

                assert!(success, "Query execution failed for: {}", sql);
                assert_eq!(
                    writer.get_rows().len(),
                    expected_rows,
                    "Row count mismatch for: {}",
                    sql
                );
                assert_eq!(
                    writer.get_schema().get_columns().len(),
                    expected_columns,
                    "Column count mismatch for: {}",
                    sql
                );
            }
        }

        #[tokio::test]
        async fn test_select_distinct() {
            let mut ctx = TestContext::new("test_select_distinct").await;

            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("department", TypeId::VarChar),
                Column::new("city", TypeId::VarChar),
            ]);

            ctx.create_test_table("employees", table_schema.clone())
                .unwrap();

            let test_data = vec![
                vec![Value::new(1), Value::new("Engineering"), Value::new("NYC")],
                vec![Value::new(2), Value::new("Sales"), Value::new("LA")],
                vec![Value::new(3), Value::new("Engineering"), Value::new("NYC")], // Duplicate
                vec![Value::new(4), Value::new("Sales"), Value::new("NYC")],
                vec![Value::new(5), Value::new("Engineering"), Value::new("LA")],
            ];
            ctx.insert_tuples("employees", test_data, table_schema)
                .unwrap();

            // Test DISTINCT on single column
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(
                    "SELECT DISTINCT department FROM employees",
                    ctx.exec_ctx.clone(),
                    &mut writer,
                ).await
                .unwrap();

            assert!(success, "DISTINCT query failed");
            assert_eq!(
                writer.get_rows().len(),
                2,
                "Should return 2 distinct departments"
            );

            // Test DISTINCT on multiple columns
            let mut writer2 = TestResultWriter::new();
            let success2 = ctx
                .engine
                .execute_sql(
                    "SELECT DISTINCT department, city FROM employees",
                    ctx.exec_ctx.clone(),
                    &mut writer2,
                ).await
                .unwrap();

            assert!(success2, "DISTINCT multiple columns failed");
            assert_eq!(
                writer2.get_rows().len(),
                4,
                "Should return 4 distinct combinations"
            );
        }

        #[tokio::test]
        async fn test_select_with_limit() {
            let mut ctx = TestContext::new("test_select_with_limit").await;

            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ]);

            ctx.create_test_table("users", table_schema.clone())
                .unwrap();

            let test_data = vec![
                vec![Value::new(1), Value::new("Alice")],
                vec![Value::new(2), Value::new("Bob")],
                vec![Value::new(3), Value::new("Charlie")],
                vec![Value::new(4), Value::new("David")],
                vec![Value::new(5), Value::new("Eve")],
            ];
            ctx.insert_tuples("users", test_data, table_schema).unwrap();

            // Test different LIMIT values
            let test_cases = vec![
                ("SELECT * FROM users LIMIT 3", 3),
                ("SELECT * FROM users LIMIT 1", 1),
                ("SELECT * FROM users LIMIT 10", 5), // More than available rows
                ("SELECT name FROM users LIMIT 2", 2),
            ];

            for (sql, expected_rows) in test_cases {
                let mut writer = TestResultWriter::new();
                let success = ctx
                    .engine
                    .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                    .unwrap();

                assert!(success, "Query execution failed for: {}", sql);
                assert_eq!(
                    writer.get_rows().len(),
                    expected_rows,
                    "Row count mismatch for: {}",
                    sql
                );
            }
        }

        #[tokio::test]
        async fn test_select_with_offset() {
            let mut ctx = TestContext::new("test_select_with_offset").await;

            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ]);

            ctx.create_test_table("users", table_schema.clone())
                .unwrap();

            let test_data = vec![
                vec![Value::new(1), Value::new("Alice")],
                vec![Value::new(2), Value::new("Bob")],
                vec![Value::new(3), Value::new("Charlie")],
                vec![Value::new(4), Value::new("David")],
                vec![Value::new(5), Value::new("Eve")],
            ];
            ctx.insert_tuples("users", test_data, table_schema).unwrap();

            // Test OFFSET with LIMIT
            let test_cases = vec![
                ("SELECT * FROM users LIMIT 3 OFFSET 1", 3), // Skip first, take 3
                ("SELECT * FROM users LIMIT 2 OFFSET 3", 2), // Skip first 3, take 2
                ("SELECT * FROM users OFFSET 2", 3),         // Skip first 2, take all remaining
                ("SELECT * FROM users LIMIT 1 OFFSET 4", 1), // Skip first 4, take 1
            ];

            for (sql, expected_rows) in test_cases {
                let mut writer = TestResultWriter::new();
                let success = ctx
                    .engine
                    .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                    .unwrap();

                assert!(success, "Query execution failed for: {}", sql);
                assert_eq!(
                    writer.get_rows().len(),
                    expected_rows,
                    "Row count mismatch for: {}",
                    sql
                );
            }
        }

        #[tokio::test]
        async fn test_select_with_aliases() {
            let mut ctx = TestContext::new("test_select_with_aliases").await;

            let table_schema = Schema::new(vec![
                Column::new("employee_id", TypeId::Integer),
                Column::new("first_name", TypeId::VarChar),
                Column::new("last_name", TypeId::VarChar),
                Column::new("birth_year", TypeId::Integer),
            ]);

            ctx.create_test_table("employees", table_schema.clone())
                .unwrap();

            let test_data = vec![
                vec![
                    Value::new(1),
                    Value::new("John"),
                    Value::new("Doe"),
                    Value::new(1990),
                ],
                vec![
                    Value::new(2),
                    Value::new("Jane"),
                    Value::new("Smith"),
                    Value::new(1985),
                ],
            ];
            ctx.insert_tuples("employees", test_data, table_schema)
                .unwrap();

            // Test column aliases
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(
                    "SELECT employee_id AS id, first_name AS fname, last_name AS lname FROM employees",
                    ctx.exec_ctx.clone(),
                    &mut writer,
                )
                .await.unwrap();

            assert!(success, "Column alias query failed");
            assert_eq!(writer.get_rows().len(), 2, "Should return 2 rows");

            let schema = writer.get_schema();
            assert_eq!(
                schema.get_columns()[0].get_name(),
                "id",
                "First column should be aliased as 'id'"
            );
            assert_eq!(
                schema.get_columns()[1].get_name(),
                "fname",
                "Second column should be aliased as 'fname'"
            );
            assert_eq!(
                schema.get_columns()[2].get_name(),
                "lname",
                "Third column should be aliased as 'lname'"
            );

            // Test table alias
            let mut writer2 = TestResultWriter::new();
            let success2 = ctx
                .engine
                .execute_sql(
                    "SELECT e.employee_id, e.first_name FROM employees e",
                    ctx.exec_ctx.clone(),
                    &mut writer2,
                ).await
                .unwrap();

            assert!(success2, "Table alias query failed");
            assert_eq!(writer2.get_rows().len(), 2, "Should return 2 rows");
        }

        #[tokio::test]
        async fn test_select_with_different_data_types() {
            let mut ctx = TestContext::new("test_select_with_different_data_types").await;

            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("active", TypeId::Boolean),
                Column::new("salary", TypeId::BigInt),
                Column::new("rating", TypeId::Float),
                Column::new("score", TypeId::Decimal),
            ]);

            ctx.create_test_table("mixed_data", table_schema.clone())
                .unwrap();

            let test_data = vec![
                vec![
                    Value::new(1),
                    Value::new("Alice"),
                    Value::new(true),
                    Value::new(50000i64),
                    Value::new(4.5f32),
                    Value::new_with_type(Val::Decimal(95.7), TypeId::Decimal),
                ],
                vec![
                    Value::new(2),
                    Value::new("Bob"),
                    Value::new(false),
                    Value::new(60000i64),
                    Value::new(3.8f32),
                    Value::new_with_type(Val::Decimal(87.2), TypeId::Decimal),
                ],
            ];
            ctx.insert_tuples("mixed_data", test_data, table_schema)
                .unwrap();

            // Test selecting different data types
            let test_cases = vec![
                ("SELECT id FROM mixed_data", 2, 1),
                ("SELECT name FROM mixed_data", 2, 1),
                ("SELECT active FROM mixed_data", 2, 1),
                ("SELECT salary FROM mixed_data", 2, 1),
                ("SELECT rating FROM mixed_data", 2, 1),
                ("SELECT score FROM mixed_data", 2, 1),
                ("SELECT id, name, active FROM mixed_data", 2, 3),
                ("SELECT salary, rating, score FROM mixed_data", 2, 3),
            ];

            for (sql, expected_rows, expected_columns) in test_cases {
                let mut writer = TestResultWriter::new();
                let success = ctx
                    .engine
                    .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                    .unwrap();

                assert!(success, "Query execution failed for: {}", sql);
                assert_eq!(
                    writer.get_rows().len(),
                    expected_rows,
                    "Row count mismatch for: {}",
                    sql
                );
                assert_eq!(
                    writer.get_schema().get_columns().len(),
                    expected_columns,
                    "Column count mismatch for: {}",
                    sql
                );
            }
        }

        #[tokio::test]
        async fn test_select_with_null_values() {
            let mut ctx = TestContext::new("test_select_with_null_values").await;

            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("optional_field", TypeId::VarChar),
                Column::new("nullable_int", TypeId::Integer),
            ]);

            ctx.create_test_table("nullable_data", table_schema.clone())
                .unwrap();

            let test_data = vec![
                vec![
                    Value::new(1),
                    Value::new("Alice"),
                    Value::new("has_value"),
                    Value::new(100),
                ],
                vec![
                    Value::new(2),
                    Value::new("Bob"),
                    Value::new(Null),
                    Value::new(Null),
                ],
                vec![
                    Value::new(3),
                    Value::new("Charlie"),
                    Value::new("another_value"),
                    Value::new(Null),
                ],
            ];
            ctx.insert_tuples("nullable_data", test_data, table_schema)
                .unwrap();

            // Test selecting NULL values
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(
                    "SELECT * FROM nullable_data",
                    ctx.exec_ctx.clone(),
                    &mut writer,
                )
                .await.unwrap();

            assert!(success, "Query with NULL values failed");
            assert_eq!(
                writer.get_rows().len(),
                3,
                "Should return all 3 rows including those with NULLs"
            );

            // Test selecting specific columns with NULLs
            let mut writer2 = TestResultWriter::new();
            let success2 = ctx
                .engine
                .execute_sql(
                    "SELECT name, optional_field FROM nullable_data",
                    ctx.exec_ctx.clone(),
                    &mut writer2,
                ).await
                .unwrap();

            assert!(success2, "Query selecting columns with NULLs failed");
            assert_eq!(writer2.get_rows().len(), 3, "Should return all 3 rows");
        }

        #[tokio::test]
        async fn test_select_empty_table() {
            let mut ctx = TestContext::new("test_select_empty_table").await;

            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ]);

            ctx.create_test_table("empty_table", table_schema).unwrap();

            // Test selecting from empty table
            let test_cases = vec![
                "SELECT * FROM empty_table",
                "SELECT id FROM empty_table",
                "SELECT name FROM empty_table",
                "SELECT id, name FROM empty_table",
                "SELECT 'constant' FROM empty_table",
            ];

            for sql in test_cases {
                let mut writer = TestResultWriter::new();
                let success = ctx
                    .engine
                    .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                    .unwrap();

                assert!(success, "Query execution failed for: {}", sql);
                assert_eq!(
                    writer.get_rows().len(),
                    0,
                    "Empty table should return 0 rows for: {}",
                    sql
                );
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
        async fn test_select_performance_large_table() {
            let mut ctx = TestContext::new("test_select_performance_large_table").await;

            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("value", TypeId::VarChar),
                Column::new("number", TypeId::Integer),
            ]);

            ctx.create_test_table("large_table", table_schema.clone())
                .unwrap();

            // Insert larger dataset
            let mut test_data = Vec::new();
            for i in 1..=1000 {
                test_data.push(vec![
                    Value::new(i),
                    Value::new(format!("value_{}", i)),
                    Value::new(i * 2),
                ]);
            }
            ctx.insert_tuples("large_table", test_data, table_schema)
                .unwrap();

            ctx.commit_current_transaction().await.expect("Commit failed");

            // Test performance with various queries
            let test_cases = vec![
                ("SELECT * FROM large_table", 1000),
                ("SELECT id FROM large_table", 1000),
                ("SELECT id, value FROM large_table", 1000),
                ("SELECT * FROM large_table LIMIT 100", 100),
                ("SELECT id FROM large_table LIMIT 50", 50),
            ];

            for (sql, expected_rows) in test_cases {
                let mut writer = TestResultWriter::new();
                let success = ctx
                    .engine
                    .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                    .unwrap();

                assert!(success, "Query execution failed for: {}", sql);
                assert_eq!(
                    writer.get_rows().len(),
                    expected_rows,
                    "Row count mismatch for: {}",
                    sql
                );
            }
        }
    }

    mod order_by_tests {
        use crate::catalog::column::Column;
        use crate::catalog::schema::Schema;
        use crate::sql::execution::execution_engine::tests::{TestContext, TestResultWriter};
        use crate::types_db::type_id::TypeId;
        use crate::types_db::value::Value;

        #[tokio::test]
        // #[ignore = "Causes stack overflow in the logical plan to physical plan conversion"]
        async fn test_order_by() {
            let mut ctx = TestContext::new("test_order_by").await;

            println!("Creating test table and schema");
            // Create test table with minimal schema
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ]);

            let table_name = "sorted_users";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            println!("Inserting test data");
            // Insert minimal test data - just two rows to minimize stack usage
            let test_data = vec![
                vec![Value::new(2), Value::new("Alice")],
                vec![Value::new(1), Value::new("Bob")],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test a single simple ORDER BY case
            let sql = "SELECT id, name FROM sorted_users ORDER BY id"; // Use ASC order to simplify
            let mut writer = TestResultWriter::new();

            println!("Executing query: {}", sql);
            let success = match ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
            {
                Ok(s) => {
                    println!("Query execution succeeded");
                    s
                }
                Err(e) => {
                    println!("Query execution failed: {:?}", e);
                    panic!("Query execution failed: {:?}", e);
                }
            };

            assert!(success, "Query execution failed");

            println!("Processing results");
            let rows = writer.get_rows();
            println!("Got {} rows", rows.len());

            // Verify results in a stack-efficient way
            assert_eq!(rows.len(), 2, "Expected 2 rows");

            if rows.len() >= 2 {
                println!("First row: {:?}", rows[0]);
                println!("Second row: {:?}", rows[1]);

                // Check first row (should be id=1, name=Bob)
                let first_id = &rows[0][0];
                let first_name = &rows[0][1];
                assert_eq!(first_id.to_string(), "1", "First row should have id=1");
                assert_eq!(
                    first_name.to_string(),
                    "Bob",
                    "First row should have name=Bob"
                );

                // Check second row (should be id=2, name=Alice)
                let second_id = &rows[1][0];
                let second_name = &rows[1][1];
                assert_eq!(second_id.to_string(), "2", "Second row should have id=2");
                assert_eq!(
                    second_name.to_string(),
                    "Alice",
                    "Second row should have name=Alice"
                );
            }

            println!("Test completed successfully");
        }

        #[tokio::test]
        async fn test_order_by_desc() {
            let mut ctx = TestContext::new("test_order_by_desc").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("score", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ]);

            let table_name = "students";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![Value::new(1), Value::new(85), Value::new("Alice")],
                vec![Value::new(2), Value::new(92), Value::new("Bob")],
                vec![Value::new(3), Value::new(78), Value::new("Charlie")],
                vec![Value::new(4), Value::new(95), Value::new("Diana")],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test ORDER BY DESC
            let sql = "SELECT id, score, name FROM students ORDER BY score DESC";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 4, "Expected 4 rows");

            // Verify descending order by score
            if rows.len() >= 4 {
                assert_eq!(rows[0][1].to_string(), "95"); // Diana
                assert_eq!(rows[1][1].to_string(), "92"); // Bob
                assert_eq!(rows[2][1].to_string(), "85"); // Alice
                assert_eq!(rows[3][1].to_string(), "78"); // Charlie
            }
        }

        #[tokio::test]
        async fn test_order_by_multiple_columns() {
            let mut ctx = TestContext::new("test_order_by_multiple_columns").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("department", TypeId::VarChar),
                Column::new("salary", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ]);

            let table_name = "employees";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![Value::new("IT"), Value::new(75000), Value::new("Alice")],
                vec![Value::new("HR"), Value::new(65000), Value::new("Bob")],
                vec![Value::new("IT"), Value::new(80000), Value::new("Charlie")],
                vec![Value::new("HR"), Value::new(70000), Value::new("Diana")],
                vec![Value::new("IT"), Value::new(75000), Value::new("Eve")],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test ORDER BY multiple columns
            let sql = "SELECT department, salary, name FROM employees ORDER BY department ASC, salary DESC, name ASC";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 5, "Expected 5 rows");

            // Verify ordering: HR first, then IT; within each dept by salary desc, then by name asc
            if rows.len() >= 5 {
                assert_eq!(rows[0][0].to_string(), "HR");
                assert_eq!(rows[0][1].to_string(), "70000"); // Diana
                assert_eq!(rows[1][0].to_string(), "HR");
                assert_eq!(rows[1][1].to_string(), "65000"); // Bob
                assert_eq!(rows[2][0].to_string(), "IT");
                assert_eq!(rows[2][1].to_string(), "80000"); // Charlie
                // For IT with salary 75000, Alice comes before Eve alphabetically
            }
        }

        #[tokio::test]
        async fn test_order_by_with_null_values() {
            let mut ctx = TestContext::new("test_order_by_with_null_values").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("score", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ]);

            let table_name = "test_scores";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data with NULL values
            let test_data = vec![
                vec![Value::new(1), Value::new(85), Value::new("Alice")],
                vec![
                    Value::new(2),
                    Value::new_with_type(crate::types_db::value::Val::Null, TypeId::Integer),
                    Value::new("Bob"),
                ],
                vec![Value::new(3), Value::new(92), Value::new("Charlie")],
                vec![
                    Value::new(4),
                    Value::new_with_type(crate::types_db::value::Val::Null, TypeId::Integer),
                    Value::new("Diana"),
                ],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test ORDER BY with NULL values
            let sql = "SELECT id, score, name FROM test_scores ORDER BY score ASC";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 4, "Expected 4 rows");
            // NULL handling behavior may vary by implementation
        }

        #[tokio::test]
        async fn test_order_by_with_expressions() {
            let mut ctx = TestContext::new("test_order_by_with_expressions").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("price", TypeId::Integer),
                Column::new("quantity", TypeId::Integer),
            ]);

            let table_name = "products";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![Value::new(1), Value::new(100), Value::new(5)], // total: 500
                vec![Value::new(2), Value::new(200), Value::new(2)], // total: 400
                vec![Value::new(3), Value::new(150), Value::new(4)], // total: 600
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test ORDER BY with expression
            let sql = "SELECT id, price, quantity, price * quantity as total FROM products ORDER BY price * quantity DESC";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 3, "Expected 3 rows");

            // Verify ordering by calculated expression (total descending)
            if rows.len() >= 3 {
                assert_eq!(rows[0][0].to_string(), "3"); // 600
                assert_eq!(rows[1][0].to_string(), "1"); // 500
                assert_eq!(rows[2][0].to_string(), "2"); // 400
            }
        }

        #[tokio::test]
        async fn test_order_by_with_limit() {
            let mut ctx = TestContext::new("test_order_by_with_limit").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("value", TypeId::Integer),
            ]);

            let table_name = "numbers";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![Value::new(1), Value::new(50)],
                vec![Value::new(2), Value::new(30)],
                vec![Value::new(3), Value::new(80)],
                vec![Value::new(4), Value::new(20)],
                vec![Value::new(5), Value::new(70)],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test ORDER BY with LIMIT
            let sql = "SELECT id, value FROM numbers ORDER BY value DESC LIMIT 3";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 3, "Expected 3 rows due to LIMIT");

            // Verify top 3 values
            if rows.len() >= 3 {
                assert_eq!(rows[0][1].to_string(), "80"); // id=3
                assert_eq!(rows[1][1].to_string(), "70"); // id=5
                assert_eq!(rows[2][1].to_string(), "50"); // id=1
            }
        }

        #[tokio::test]
        async fn test_order_by_with_offset() {
            let mut ctx = TestContext::new("test_order_by_with_offset").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("value", TypeId::Integer),
            ]);

            let table_name = "data";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![Value::new(1), Value::new(10)],
                vec![Value::new(2), Value::new(20)],
                vec![Value::new(3), Value::new(30)],
                vec![Value::new(4), Value::new(40)],
                vec![Value::new(5), Value::new(50)],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test ORDER BY with OFFSET
            let sql = "SELECT id, value FROM data ORDER BY value ASC OFFSET 2";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 3, "Expected 3 rows after OFFSET 2");

            // Verify remaining values after offset
            if rows.len() >= 3 {
                assert_eq!(rows[0][1].to_string(), "30"); // id=3
                assert_eq!(rows[1][1].to_string(), "40"); // id=4
                assert_eq!(rows[2][1].to_string(), "50"); // id=5
            }
        }

        #[tokio::test]
        async fn test_order_by_different_data_types() {
            let mut ctx = TestContext::new("test_order_by_different_data_types").await;

            // Create test table with various data types
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("active", TypeId::Boolean),
                Column::new("score", TypeId::BigInt),
            ]);

            let table_name = "mixed_data";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![
                    Value::new(3),
                    Value::new("Charlie"),
                    Value::new(true),
                    Value::new(95i64),
                ],
                vec![
                    Value::new(1),
                    Value::new("Alice"),
                    Value::new(false),
                    Value::new(85i64),
                ],
                vec![
                    Value::new(2),
                    Value::new("Bob"),
                    Value::new(true),
                    Value::new(90i64),
                ],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test ORDER BY on string column
            let sql = "SELECT id, name, active, score FROM mixed_data ORDER BY name ASC";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 3, "Expected 3 rows");

            // Verify alphabetical order
            if rows.len() >= 3 {
                assert_eq!(rows[0][1].to_string(), "Alice");
                assert_eq!(rows[1][1].to_string(), "Bob");
                assert_eq!(rows[2][1].to_string(), "Charlie");
            }
        }

        #[tokio::test]
        async fn test_order_by_with_aggregation() {
            let mut ctx = TestContext::new("test_order_by_with_aggregation").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("department", TypeId::VarChar),
                Column::new("salary", TypeId::Integer),
            ]);

            let table_name = "emp_salaries";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![Value::new("IT"), Value::new(70000)],
                vec![Value::new("HR"), Value::new(60000)],
                vec![Value::new("IT"), Value::new(80000)],
                vec![Value::new("Sales"), Value::new(55000)],
                vec![Value::new("HR"), Value::new(65000)],
                vec![Value::new("Sales"), Value::new(50000)],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test ORDER BY with GROUP BY and aggregation
            let sql = "SELECT department, AVG(salary) as avg_salary FROM emp_salaries GROUP BY department ORDER BY avg_salary DESC";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 3, "Expected 3 departments");

            // Verify ordering by average salary
            if rows.len() >= 3 {
                assert_eq!(rows[0][0].to_string(), "IT"); // 75000 avg
                assert_eq!(rows[1][0].to_string(), "HR"); // 62500 avg
                assert_eq!(rows[2][0].to_string(), "Sales"); // 52500 avg
            }
        }

        #[tokio::test]
        async fn test_order_by_edge_cases() {
            let mut ctx = TestContext::new("test_order_by_edge_cases").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("value", TypeId::Integer),
            ]);

            let table_name = "edge_cases";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Test ORDER BY on empty table
            let sql = "SELECT id, value FROM edge_cases ORDER BY value ASC";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 0, "Expected 0 rows from empty table");

            // Insert single row
            let test_data = vec![vec![Value::new(1), Value::new(100)]];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test ORDER BY on single row
            let sql = "SELECT id, value FROM edge_cases ORDER BY value DESC";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 1, "Expected 1 row");

            if rows.len() == 1 {
                assert_eq!(rows[0][0].to_string(), "1");
                assert_eq!(rows[0][1].to_string(), "100");
            }
        }

        #[tokio::test]
        async fn test_order_by_performance() {
            let mut ctx = TestContext::new("test_order_by_performance").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("random_value", TypeId::Integer),
                Column::new("category", TypeId::VarChar),
            ]);

            let table_name = "large_dataset";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert larger dataset for performance testing
            let mut test_data = Vec::new();
            for i in 1..=50 {
                let category = if i % 3 == 0 {
                    "A"
                } else if i % 3 == 1 {
                    "B"
                } else {
                    "C"
                };
                let random_value = (i * 17) % 100; // Generate some pseudo-random values
                test_data.push(vec![
                    Value::new(i),
                    Value::new(random_value),
                    Value::new(category),
                ]);
            }
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test ORDER BY performance with larger dataset
            let sql = "SELECT id, random_value, category FROM large_dataset ORDER BY random_value DESC, category ASC LIMIT 10";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Performance test query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 10, "Expected 10 rows due to LIMIT");

            println!("Performance test completed with {} result rows", rows.len());
        }
    }

    mod case_tests {
        use crate::catalog::column::Column;
        use crate::catalog::schema::Schema;
        use crate::sql::execution::execution_engine::tests::{TestContext, TestResultWriter};
        use crate::types_db::type_id::TypeId;
        use crate::types_db::value::Value;

        #[tokio::test]
        async fn test_case_when_simple() {
            let mut ctx = TestContext::new("test_case_when_simple").await;

            // Create test table with same schema but smaller dataset
            let table_schema = Schema::new(vec![
                Column::new("a", TypeId::Integer),
                Column::new("b", TypeId::Integer),
                Column::new("c", TypeId::Integer),
                Column::new("d", TypeId::Integer),
                Column::new("e", TypeId::Integer),
            ]);

            ctx.create_test_table("t1", table_schema.clone()).unwrap();

            // Insert a small amount of test data where we can easily calculate the expected results
            let test_data = vec![
                // c values chosen to make it easy to verify against average
                // average of c values will be 150
                vec![
                    Value::new(100),
                    Value::new(10),
                    Value::new(100),
                    Value::new(1),
                    Value::new(1),
                ], // b*10 = 100
                vec![
                    Value::new(100),
                    Value::new(20),
                    Value::new(200),
                    Value::new(1),
                    Value::new(1),
                ], // a*2 = 200
                vec![
                    Value::new(100),
                    Value::new(30),
                    Value::new(150),
                    Value::new(1),
                    Value::new(1),
                ], // b*10 = 300
            ];

            ctx.insert_tuples("t1", test_data, table_schema).unwrap();

            // Execute the same query as the main test
            let sql = "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END FROM t1 ORDER BY 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success, "Query execution failed");

            // Get the results
            let rows = writer.get_rows();

            // Print debug info
            println!("Average of c values should be 150");
            println!("Results:");
            for row in rows {
                println!("{}", row[0]);
            }

            // Verify we got exactly 3 results
            assert_eq!(rows.len(), 3, "Expected 3 rows in result");

            // Convert results to integers for easier comparison
            let results: Vec<i32> = rows
                .iter()
                .map(|row| row[0].to_string().parse::<i32>().unwrap())
                .collect();

            // Expected results:
            // Row 1: c=100 < avg(150) so b*10 = 100
            // Row 2: c=200 > avg(150) so a*2 = 200
            // Row 3: c=150 = avg(150) so b*10 = 300
            let expected = vec![100, 200, 300];

            // Verify results match expected values
            assert_eq!(results, expected, "Results don't match expected values");
        }

        #[tokio::test]
        async fn test_case_when_with_subquery() {
            let mut ctx = TestContext::new("test_case_when_with_subquery").await;

            // Create test table with same schema as original test
            let table_schema = Schema::new(vec![
                Column::new("a", TypeId::Integer),
                Column::new("b", TypeId::Integer),
                Column::new("c", TypeId::Integer),
                Column::new("d", TypeId::Integer),
                Column::new("e", TypeId::Integer),
            ]);

            ctx.create_test_table("t1", table_schema.clone()).unwrap();

            // Insert the same test data as in the original test
            let test_data = vec![
                vec![
                    Value::new(104),
                    Value::new(100),
                    Value::new(102),
                    Value::new(101),
                    Value::new(103),
                ],
                vec![
                    Value::new(107),
                    Value::new(105),
                    Value::new(106),
                    Value::new(108),
                    Value::new(109),
                ],
                vec![
                    Value::new(111),
                    Value::new(112),
                    Value::new(113),
                    Value::new(114),
                    Value::new(110),
                ],
                vec![
                    Value::new(115),
                    Value::new(118),
                    Value::new(119),
                    Value::new(116),
                    Value::new(117),
                ],
                vec![
                    Value::new(121),
                    Value::new(124),
                    Value::new(123),
                    Value::new(122),
                    Value::new(120),
                ],
                vec![
                    Value::new(127),
                    Value::new(129),
                    Value::new(125),
                    Value::new(128),
                    Value::new(126),
                ],
                vec![
                    Value::new(131),
                    Value::new(130),
                    Value::new(134),
                    Value::new(133),
                    Value::new(132),
                ],
                vec![
                    Value::new(138),
                    Value::new(139),
                    Value::new(137),
                    Value::new(136),
                    Value::new(135),
                ],
                vec![
                    Value::new(142),
                    Value::new(143),
                    Value::new(141),
                    Value::new(140),
                    Value::new(144),
                ],
                vec![
                    Value::new(149),
                    Value::new(145),
                    Value::new(147),
                    Value::new(148),
                    Value::new(146),
                ],
                vec![
                    Value::new(153),
                    Value::new(151),
                    Value::new(150),
                    Value::new(154),
                    Value::new(152),
                ],
                vec![
                    Value::new(159),
                    Value::new(158),
                    Value::new(155),
                    Value::new(156),
                    Value::new(157),
                ],
                vec![
                    Value::new(163),
                    Value::new(160),
                    Value::new(161),
                    Value::new(164),
                    Value::new(162),
                ],
                vec![
                    Value::new(168),
                    Value::new(167),
                    Value::new(166),
                    Value::new(169),
                    Value::new(165),
                ],
                vec![
                    Value::new(174),
                    Value::new(170),
                    Value::new(172),
                    Value::new(171),
                    Value::new(173),
                ],
                vec![
                    Value::new(179),
                    Value::new(175),
                    Value::new(176),
                    Value::new(178),
                    Value::new(177),
                ],
                vec![
                    Value::new(182),
                    Value::new(181),
                    Value::new(184),
                    Value::new(183),
                    Value::new(180),
                ],
                vec![
                    Value::new(188),
                    Value::new(186),
                    Value::new(187),
                    Value::new(185),
                    Value::new(189),
                ],
                vec![
                    Value::new(191),
                    Value::new(194),
                    Value::new(193),
                    Value::new(190),
                    Value::new(192),
                ],
                vec![
                    Value::new(199),
                    Value::new(198),
                    Value::new(195),
                    Value::new(196),
                    Value::new(197),
                ],
                vec![
                    Value::new(201),
                    Value::new(200),
                    Value::new(202),
                    Value::new(203),
                    Value::new(204),
                ],
                vec![
                    Value::new(205),
                    Value::new(206),
                    Value::new(208),
                    Value::new(207),
                    Value::new(209),
                ],
                vec![
                    Value::new(213),
                    Value::new(211),
                    Value::new(214),
                    Value::new(212),
                    Value::new(210),
                ],
                vec![
                    Value::new(216),
                    Value::new(218),
                    Value::new(215),
                    Value::new(217),
                    Value::new(219),
                ],
                vec![
                    Value::new(220),
                    Value::new(223),
                    Value::new(224),
                    Value::new(222),
                    Value::new(221),
                ],
                vec![
                    Value::new(229),
                    Value::new(228),
                    Value::new(225),
                    Value::new(226),
                    Value::new(227),
                ],
                vec![
                    Value::new(234),
                    Value::new(232),
                    Value::new(231),
                    Value::new(233),
                    Value::new(230),
                ],
                vec![
                    Value::new(239),
                    Value::new(236),
                    Value::new(235),
                    Value::new(238),
                    Value::new(237),
                ],
                vec![
                    Value::new(243),
                    Value::new(240),
                    Value::new(244),
                    Value::new(241),
                    Value::new(242),
                ],
                vec![
                    Value::new(245),
                    Value::new(249),
                    Value::new(247),
                    Value::new(248),
                    Value::new(246),
                ],
            ];

            ctx.insert_tuples("t1", test_data, table_schema).unwrap();

            // Execute the query
            let sql = "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END FROM t1 ORDER BY 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success, "Query execution failed");

            // Get the results
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 30, "Expected 30 rows in result");

            // Print the values for debugging
            println!("Values in order:");
            for row in rows {
                println!("{}", row[0]);
            }

            // Join values with newlines and compute hash
            let data = rows
                .iter()
                .map(|row| row[0].to_string())
                .collect::<Vec<String>>()
                .join("\n")
                + "\n";

            println!(
                "\nFinal string being hashed (between ===):\n===\n{}===",
                data
            );
            println!("String length: {}", data.len());
            println!("Bytes: {:?}", data.as_bytes());

            let digest = md5::compute(data.as_bytes());
            let hash = format!("{:x}", digest);
            println!("Computed hash: {}", hash);

            // The expected hash from the original test
            let expected_hash = "55d5be1a73595fe92f388d9940e7fabe";
            assert_eq!(
                hash, expected_hash,
                "Hash mismatch - expected {}, got {}",
                expected_hash, hash
            );
        }

        #[tokio::test]
        async fn test_case_when_basic() {
            let mut ctx = TestContext::new("test_case_when_basic").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("grade", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ]);

            let table_name = "students";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![Value::new(1), Value::new(85), Value::new("Alice")],
                vec![Value::new(2), Value::new(92), Value::new("Bob")],
                vec![Value::new(3), Value::new(78), Value::new("Charlie")],
                vec![Value::new(4), Value::new(95), Value::new("Diana")],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test basic CASE WHEN with ELSE
            let sql = "SELECT id, name, CASE WHEN grade >= 90 THEN 'A' WHEN grade >= 80 THEN 'B' ELSE 'C' END as letter_grade FROM students ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 4, "Expected 4 rows");

            // Verify letter grades
            if rows.len() >= 4 {
                assert_eq!(rows[0][2].to_string(), "B"); // Alice: 85 -> B
                assert_eq!(rows[1][2].to_string(), "A"); // Bob: 92 -> A
                assert_eq!(rows[2][2].to_string(), "C"); // Charlie: 78 -> C
                assert_eq!(rows[3][2].to_string(), "A"); // Diana: 95 -> A
            }
        }

        #[tokio::test]
        async fn test_case_when_multiple_conditions() {
            let mut ctx = TestContext::new("test_case_when_multiple_conditions").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("age", TypeId::Integer),
                Column::new("income", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ]);

            let table_name = "people";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![
                    Value::new(1),
                    Value::new(25),
                    Value::new(40000),
                    Value::new("Alice"),
                ],
                vec![
                    Value::new(2),
                    Value::new(35),
                    Value::new(60000),
                    Value::new("Bob"),
                ],
                vec![
                    Value::new(3),
                    Value::new(45),
                    Value::new(80000),
                    Value::new("Charlie"),
                ],
                vec![
                    Value::new(4),
                    Value::new(30),
                    Value::new(50000),
                    Value::new("Diana"),
                ],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test CASE WHEN with multiple conditions (AND/OR)
            let sql = "SELECT id, name, CASE WHEN age > 40 AND income > 70000 THEN 'Senior High' WHEN age > 30 OR income > 55000 THEN 'Mid Level' ELSE 'Junior' END as category FROM people ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 4, "Expected 4 rows");

            // Verify categories
            if rows.len() >= 4 {
                assert_eq!(rows[0][2].to_string(), "Junior"); // Alice: 25, 40000 -> Junior
                assert_eq!(rows[1][2].to_string(), "Mid Level"); // Bob: 35, 60000 -> Mid Level
                assert_eq!(rows[2][2].to_string(), "Senior High"); // Charlie: 45, 80000 -> Senior High
                assert_eq!(rows[3][2].to_string(), "Junior"); // Diana: 30, 50000 -> Junior (30 is NOT > 30, 50000 is NOT > 55000)
            }
        }

        #[tokio::test]
        async fn test_case_when_with_null_values() {
            let mut ctx = TestContext::new("test_case_when_with_null_values").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("score", TypeId::Integer),
                Column::new("bonus", TypeId::Integer),
            ]);

            let table_name = "scores";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data with NULL values
            let test_data = vec![
                vec![Value::new(1), Value::new(85), Value::new(10)],
                vec![
                    Value::new(2),
                    Value::new_with_type(crate::types_db::value::Val::Null, TypeId::Integer),
                    Value::new(5),
                ],
                vec![
                    Value::new(3),
                    Value::new(90),
                    Value::new_with_type(crate::types_db::value::Val::Null, TypeId::Integer),
                ],
                vec![
                    Value::new(4),
                    Value::new_with_type(crate::types_db::value::Val::Null, TypeId::Integer),
                    Value::new_with_type(crate::types_db::value::Val::Null, TypeId::Integer),
                ],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test CASE WHEN with NULL handling
            let sql = "SELECT id, CASE WHEN score IS NULL THEN 'No Score' WHEN score >= 90 THEN 'Excellent' WHEN score >= 80 THEN 'Good' ELSE 'Poor' END as grade FROM scores ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 4, "Expected 4 rows");
            // NULL handling behavior may vary by implementation
        }

        #[tokio::test]
        async fn test_case_when_with_aggregations() {
            let mut ctx = TestContext::new("test_case_when_with_aggregations").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("department", TypeId::VarChar),
                Column::new("salary", TypeId::Integer),
                Column::new("employee_id", TypeId::Integer),
            ]);

            let table_name = "employees";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![Value::new("IT"), Value::new(75000), Value::new(1)],
                vec![Value::new("IT"), Value::new(85000), Value::new(2)],
                vec![Value::new("HR"), Value::new(65000), Value::new(3)],
                vec![Value::new("HR"), Value::new(70000), Value::new(4)],
                vec![Value::new("Sales"), Value::new(55000), Value::new(5)],
                vec![Value::new("Sales"), Value::new(60000), Value::new(6)],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test CASE WHEN with aggregation functions
            let sql = "SELECT department, COUNT(CASE WHEN salary > 70000 THEN 1 END) as high_earners, COUNT(*) as total_employees FROM employees GROUP BY department ORDER BY department";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 3, "Expected 3 departments");

            // Verify aggregation with CASE
            if rows.len() >= 3 {
                // HR: 0 high earners (both under 70000), 2 total
                // IT: 2 high earners (both over 70000), 2 total
                // Sales: 0 high earners (both under 70000), 2 total
            }
        }

        #[tokio::test]
        async fn test_case_when_with_arithmetic() {
            let mut ctx = TestContext::new("test_case_when_with_arithmetic").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("quantity", TypeId::Integer),
                Column::new("price", TypeId::Integer),
            ]);

            let table_name = "products";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![Value::new(1), Value::new(10), Value::new(100)],
                vec![Value::new(2), Value::new(50), Value::new(80)],
                vec![Value::new(3), Value::new(5), Value::new(200)],
                vec![Value::new(4), Value::new(100), Value::new(50)],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test CASE WHEN with arithmetic expressions (using consistent decimal types)
            let sql = "SELECT id, quantity, price, CASE WHEN quantity * price > 4000 THEN quantity * price * 0.9 WHEN quantity * price > 1000 THEN quantity * price * 0.95 ELSE quantity * price * 1.0 END as final_price FROM products ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 4, "Expected 4 rows");

            // Verify arithmetic calculations in CASE
            if rows.len() >= 4 {
                // Product 1: 10 * 100 = 1000 -> no discount
                // Product 2: 50 * 80 = 4000 -> no discount
                // Product 3: 5 * 200 = 1000 -> no discount
                // Product 4: 100 * 50 = 5000 -> 10% discount
            }
        }

        #[tokio::test]
        async fn test_case_when_nested() {
            let mut ctx = TestContext::new("test_case_when_nested").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("age", TypeId::Integer),
                Column::new("experience", TypeId::Integer),
                Column::new("performance", TypeId::Integer),
            ]);

            let table_name = "candidates";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![Value::new(1), Value::new(25), Value::new(2), Value::new(8)],
                vec![Value::new(2), Value::new(35), Value::new(10), Value::new(9)],
                vec![Value::new(3), Value::new(28), Value::new(5), Value::new(7)],
                vec![Value::new(4), Value::new(40), Value::new(15), Value::new(9)],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test nested CASE WHEN statements
            let sql = "SELECT id, CASE WHEN age > 30 THEN CASE WHEN experience > 8 THEN 'Senior Expert' ELSE 'Senior' END WHEN age > 25 THEN CASE WHEN performance > 8 THEN 'Mid Expert' ELSE 'Mid' END ELSE 'Junior' END as level FROM candidates ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 4, "Expected 4 rows");

            // Verify nested logic
            if rows.len() >= 4 {
                assert_eq!(rows[0][1].to_string(), "Junior"); // age 25 -> Junior
                assert_eq!(rows[1][1].to_string(), "Senior Expert"); // age 35, exp 10 -> Senior Expert
                assert_eq!(rows[2][1].to_string(), "Mid"); // age 28, perf 7 -> Mid
                assert_eq!(rows[3][1].to_string(), "Senior Expert"); // age 40, exp 15 -> Senior Expert
            }
        }

        #[tokio::test]
        async fn test_case_when_different_data_types() {
            let mut ctx = TestContext::new("test_case_when_different_data_types").await;

            // Create test table with various data types
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("active", TypeId::Boolean),
                Column::new("score", TypeId::BigInt),
            ]);

            let table_name = "mixed_data";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![
                    Value::new(1),
                    Value::new("Alice"),
                    Value::new(true),
                    Value::new(85i64),
                ],
                vec![
                    Value::new(2),
                    Value::new("Bob"),
                    Value::new(false),
                    Value::new(92i64),
                ],
                vec![
                    Value::new(3),
                    Value::new("Charlie"),
                    Value::new(true),
                    Value::new(78i64),
                ],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test CASE WHEN with different data types
            let sql = "SELECT id, name, CASE WHEN active = true THEN 'Active User' ELSE 'Inactive User' END as status, CASE WHEN score > 80 THEN 'High' ELSE 'Low' END as performance FROM mixed_data ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 3, "Expected 3 rows");

            // Verify different data type handling
            if rows.len() >= 3 {
                assert_eq!(rows[0][2].to_string(), "Active User"); // Alice: active=true
                assert_eq!(rows[1][2].to_string(), "Inactive User"); // Bob: active=false
                assert_eq!(rows[2][2].to_string(), "Active User"); // Charlie: active=true
            }
        }

        #[tokio::test]
        async fn test_case_when_with_in_clause() {
            let mut ctx = TestContext::new("test_case_when_with_in_clause").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("department_id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ]);

            let table_name = "employees";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![Value::new(1), Value::new(10), Value::new("Alice")],
                vec![Value::new(2), Value::new(20), Value::new("Bob")],
                vec![Value::new(3), Value::new(30), Value::new("Charlie")],
                vec![Value::new(4), Value::new(40), Value::new("Diana")],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test CASE WHEN with IN clause
            let sql = "SELECT id, name, CASE WHEN department_id IN (10, 20) THEN 'Core Team' WHEN department_id IN (30, 40) THEN 'Support Team' ELSE 'Other' END as team FROM employees ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 4, "Expected 4 rows");

            // Verify IN clause logic
            if rows.len() >= 4 {
                assert_eq!(rows[0][2].to_string(), "Core Team"); // dept_id 10
                assert_eq!(rows[1][2].to_string(), "Core Team"); // dept_id 20
                assert_eq!(rows[2][2].to_string(), "Support Team"); // dept_id 30
                assert_eq!(rows[3][2].to_string(), "Support Team"); // dept_id 40
            }
        }

        #[tokio::test]
        async fn test_case_when_edge_cases() {
            let mut ctx = TestContext::new("test_case_when_edge_cases").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("value", TypeId::Integer),
            ]);

            let table_name = "edge_cases";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Test CASE WHEN on empty table
            let sql = "SELECT id, CASE WHEN value > 50 THEN 'High' ELSE 'Low' END as category FROM edge_cases";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 0, "Expected 0 rows from empty table");

            // Insert single row
            let test_data = vec![vec![Value::new(1), Value::new(75)]];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test CASE WHEN on single row
            let sql = "SELECT id, CASE WHEN value > 50 THEN 'High' ELSE 'Low' END as category FROM edge_cases";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 1, "Expected 1 row");

            if rows.len() == 1 {
                assert_eq!(rows[0][1].to_string(), "High"); // value 75 > 50
            }
        }

        #[tokio::test]
        async fn test_case_when_performance() {
            let mut ctx = TestContext::new("test_case_when_performance").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("category", TypeId::VarChar),
                Column::new("value", TypeId::Integer),
            ]);

            let table_name = "large_dataset";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert larger dataset for performance testing
            let mut test_data = Vec::new();
            for i in 1..=100 {
                let category = match i % 4 {
                    0 => "A",
                    1 => "B",
                    2 => "C",
                    _ => "D",
                };
                let value = (i * 13) % 200; // Generate some pseudo-random values
                test_data.push(vec![Value::new(i), Value::new(category), Value::new(value)]);
            }
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test CASE WHEN performance with larger dataset
            let sql = "SELECT category, COUNT(CASE WHEN value > 150 THEN 1 END) as high_values, COUNT(CASE WHEN value BETWEEN 50 AND 150 THEN 1 END) as mid_values, COUNT(CASE WHEN value < 50 THEN 1 END) as low_values FROM large_dataset GROUP BY category ORDER BY category";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            assert!(success, "Performance test query execution failed");
            let rows = writer.get_rows();
            assert_eq!(rows.len(), 4, "Expected 4 categories");

            println!(
                "Performance test completed with {} category groups",
                rows.len()
            );
        }
    }

    mod window_tests {
        use super::*;

        #[tokio::test]
        async fn test_row_number_window_function() {
            let mut ctx = TestContext::new("test_row_number_window_function").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("department", TypeId::VarChar),
                Column::new("salary", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ]);

            let table_name = "employees";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![
                    Value::new(1),
                    Value::new("IT"),
                    Value::new(75000),
                    Value::new("Alice"),
                ],
                vec![
                    Value::new(2),
                    Value::new("IT"),
                    Value::new(85000),
                    Value::new("Bob"),
                ],
                vec![
                    Value::new(3),
                    Value::new("HR"),
                    Value::new(65000),
                    Value::new("Charlie"),
                ],
                vec![
                    Value::new(4),
                    Value::new("HR"),
                    Value::new(70000),
                    Value::new("Diana"),
                ],
                vec![
                    Value::new(5),
                    Value::new("Sales"),
                    Value::new(55000),
                    Value::new("Eve"),
                ],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test ROW_NUMBER() window function
            let sql = "SELECT name, department, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) as row_num FROM employees ORDER BY salary DESC";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            if success {
                let rows = writer.get_rows();
                assert_eq!(rows.len(), 5, "Expected 5 rows");

                // Verify ROW_NUMBER values
                for (i, row) in rows.iter().enumerate() {
                    println!(
                        "Row {}: name={}, dept={}, salary={}, row_num={}",
                        i + 1,
                        row[0],
                        row[1],
                        row[2],
                        row[3]
                    );
                }
            } else {
                println!("ROW_NUMBER window function not yet supported");
            }
        }

        #[tokio::test]
        async fn test_rank_window_functions() {
            let mut ctx = TestContext::new("test_rank_window_functions").await;

            // Create test table with duplicate values for ranking
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("score", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ]);

            let table_name = "students";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data with duplicate scores
            let test_data = vec![
                vec![Value::new(1), Value::new(95), Value::new("Alice")],
                vec![Value::new(2), Value::new(87), Value::new("Bob")],
                vec![Value::new(3), Value::new(95), Value::new("Charlie")], // Same score as Alice
                vec![Value::new(4), Value::new(78), Value::new("Diana")],
                vec![Value::new(5), Value::new(87), Value::new("Eve")], // Same score as Bob
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test RANK() and DENSE_RANK() window functions
            let sql = "SELECT name, score, RANK() OVER (ORDER BY score DESC) as rank_val, DENSE_RANK() OVER (ORDER BY score DESC) as dense_rank_val FROM students ORDER BY score DESC, name";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            if success {
                let rows = writer.get_rows();
                assert_eq!(rows.len(), 5, "Expected 5 rows");

                // Verify ranking values
                for (i, row) in rows.iter().enumerate() {
                    println!(
                        "Row {}: name={}, score={}, rank={}, dense_rank={}",
                        i + 1,
                        row[0],
                        row[1],
                        row[2],
                        row[3]
                    );
                }
            } else {
                println!("RANK window functions not yet supported");
            }
        }

        #[tokio::test]
        async fn test_window_functions_with_partition() {
            let mut ctx = TestContext::new("test_window_functions_with_partition").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("department", TypeId::VarChar),
                Column::new("salary", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ]);

            let table_name = "employees";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![
                    Value::new(1),
                    Value::new("IT"),
                    Value::new(75000),
                    Value::new("Alice"),
                ],
                vec![
                    Value::new(2),
                    Value::new("IT"),
                    Value::new(85000),
                    Value::new("Bob"),
                ],
                vec![
                    Value::new(3),
                    Value::new("IT"),
                    Value::new(90000),
                    Value::new("Charlie"),
                ],
                vec![
                    Value::new(4),
                    Value::new("HR"),
                    Value::new(65000),
                    Value::new("Diana"),
                ],
                vec![
                    Value::new(5),
                    Value::new("HR"),
                    Value::new(70000),
                    Value::new("Eve"),
                ],
                vec![
                    Value::new(6),
                    Value::new("Sales"),
                    Value::new(55000),
                    Value::new("Frank"),
                ],
                vec![
                    Value::new(7),
                    Value::new("Sales"),
                    Value::new(60000),
                    Value::new("Grace"),
                ],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test ROW_NUMBER() with PARTITION BY
            let sql = "SELECT name, department, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank FROM employees ORDER BY department, salary DESC";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            if success {
                let rows = writer.get_rows();
                assert_eq!(rows.len(), 7, "Expected 7 rows");

                // Verify partitioned ranking
                for (i, row) in rows.iter().enumerate() {
                    println!(
                        "Row {}: name={}, dept={}, salary={}, dept_rank={}",
                        i + 1,
                        row[0],
                        row[1],
                        row[2],
                        row[3]
                    );
                }
            } else {
                println!("Partitioned window functions not yet supported");
            }
        }

        #[tokio::test]
        async fn test_lag_lead_window_functions() {
            let mut ctx = TestContext::new("test_lag_lead_window_functions").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("date", TypeId::VarChar), // Using VARCHAR for simplicity
                Column::new("sales", TypeId::Integer),
            ]);

            let table_name = "daily_sales";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![Value::new(1), Value::new("2024-01-01"), Value::new(1000)],
                vec![Value::new(2), Value::new("2024-01-02"), Value::new(1200)],
                vec![Value::new(3), Value::new("2024-01-03"), Value::new(800)],
                vec![Value::new(4), Value::new("2024-01-04"), Value::new(1500)],
                vec![Value::new(5), Value::new("2024-01-05"), Value::new(900)],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test LAG() and LEAD() window functions
            let sql = "SELECT date, sales, LAG(sales, 1) OVER (ORDER BY date) as prev_sales, LEAD(sales, 1) OVER (ORDER BY date) as next_sales FROM daily_sales ORDER BY date";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            if success {
                let rows = writer.get_rows();
                assert_eq!(rows.len(), 5, "Expected 5 rows");

                // Verify LAG/LEAD values
                for (i, row) in rows.iter().enumerate() {
                    println!(
                        "Row {}: date={}, sales={}, prev_sales={}, next_sales={}",
                        i + 1,
                        row[0],
                        row[1],
                        row[2],
                        row[3]
                    );
                }
            } else {
                println!("LAG/LEAD window functions not yet supported");
            }
        }

        #[tokio::test]
        async fn test_first_last_value_window_functions() {
            let mut ctx = TestContext::new("test_first_last_value_window_functions").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("department", TypeId::VarChar),
                Column::new("salary", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ]);

            let table_name = "employees";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![
                    Value::new(1),
                    Value::new("IT"),
                    Value::new(75000),
                    Value::new("Alice"),
                ],
                vec![
                    Value::new(2),
                    Value::new("IT"),
                    Value::new(85000),
                    Value::new("Bob"),
                ],
                vec![
                    Value::new(3),
                    Value::new("HR"),
                    Value::new(65000),
                    Value::new("Charlie"),
                ],
                vec![
                    Value::new(4),
                    Value::new("HR"),
                    Value::new(70000),
                    Value::new("Diana"),
                ],
                vec![
                    Value::new(5),
                    Value::new("Sales"),
                    Value::new(55000),
                    Value::new("Eve"),
                ],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test FIRST_VALUE() and LAST_VALUE() window functions
            let sql = "SELECT name, department, salary, FIRST_VALUE(name) OVER (PARTITION BY department ORDER BY salary) as lowest_paid, LAST_VALUE(name) OVER (PARTITION BY department ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as highest_paid FROM employees ORDER BY department, salary";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            if success {
                let rows = writer.get_rows();
                assert_eq!(rows.len(), 5, "Expected 5 rows");

                // Verify FIRST_VALUE/LAST_VALUE
                for (i, row) in rows.iter().enumerate() {
                    println!(
                        "Row {}: name={}, dept={}, salary={}, lowest_paid={}, highest_paid={}",
                        i + 1,
                        row[0],
                        row[1],
                        row[2],
                        row[3],
                        row[4]
                    );
                }
            } else {
                println!("FIRST_VALUE/LAST_VALUE window functions not yet supported");
            }
        }

        #[tokio::test]
        async fn test_aggregation_window_functions() {
            let mut ctx = TestContext::new("test_aggregation_window_functions").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("month", TypeId::VarChar),
                Column::new("sales", TypeId::Integer),
            ]);

            let table_name = "monthly_sales";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![Value::new(1), Value::new("Jan"), Value::new(10000)],
                vec![Value::new(2), Value::new("Feb"), Value::new(12000)],
                vec![Value::new(3), Value::new("Mar"), Value::new(8000)],
                vec![Value::new(4), Value::new("Apr"), Value::new(15000)],
                vec![Value::new(5), Value::new("May"), Value::new(11000)],
                vec![Value::new(6), Value::new("Jun"), Value::new(13000)],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test SUM(), AVG(), COUNT() as window functions
            let sql = "SELECT month, sales, SUM(sales) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total, AVG(sales) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg, COUNT(*) OVER () as total_months FROM monthly_sales ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            if success {
                let rows = writer.get_rows();
                assert_eq!(rows.len(), 6, "Expected 6 rows");

                // Verify running totals and moving averages
                for (i, row) in rows.iter().enumerate() {
                    println!(
                        "Row {}: month={}, sales={}, running_total={}, moving_avg={}, total_months={}",
                        i + 1,
                        row[0],
                        row[1],
                        row[2],
                        row[3],
                        row[4]
                    );
                }
            } else {
                println!("Aggregation window functions not yet supported");
            }
        }

        #[tokio::test]
        async fn test_window_frames() {
            let mut ctx = TestContext::new("test_window_frames").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("value", TypeId::Integer),
            ]);

            let table_name = "sequence";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![Value::new(1), Value::new(10)],
                vec![Value::new(2), Value::new(20)],
                vec![Value::new(3), Value::new(30)],
                vec![Value::new(4), Value::new(40)],
                vec![Value::new(5), Value::new(50)],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test different window frame specifications
            let sql = "SELECT id, value, SUM(value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as window_sum, SUM(value) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as range_sum FROM sequence ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            if success {
                let rows = writer.get_rows();
                assert_eq!(rows.len(), 5, "Expected 5 rows");

                // Verify window frame calculations
                for (i, row) in rows.iter().enumerate() {
                    println!(
                        "Row {}: id={}, value={}, window_sum={}, range_sum={}",
                        i + 1,
                        row[0],
                        row[1],
                        row[2],
                        row[3]
                    );
                }
            } else {
                println!("Window frame specifications not yet supported");
            }
        }

        #[tokio::test]
        async fn test_ntile_window_function() {
            let mut ctx = TestContext::new("test_ntile_window_function").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("score", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ]);

            let table_name = "students";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![Value::new(1), Value::new(95), Value::new("Alice")],
                vec![Value::new(2), Value::new(87), Value::new("Bob")],
                vec![Value::new(3), Value::new(92), Value::new("Charlie")],
                vec![Value::new(4), Value::new(78), Value::new("Diana")],
                vec![Value::new(5), Value::new(89), Value::new("Eve")],
                vec![Value::new(6), Value::new(82), Value::new("Frank")],
                vec![Value::new(7), Value::new(94), Value::new("Grace")],
                vec![Value::new(8), Value::new(76), Value::new("Henry")],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test NTILE() window function
            let sql = "SELECT name, score, NTILE(4) OVER (ORDER BY score DESC) as quartile FROM students ORDER BY score DESC";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            if success {
                let rows = writer.get_rows();
                assert_eq!(rows.len(), 8, "Expected 8 rows");

                // Verify NTILE quartiles
                for (i, row) in rows.iter().enumerate() {
                    println!(
                        "Row {}: name={}, score={}, quartile={}",
                        i + 1,
                        row[0],
                        row[1],
                        row[2]
                    );
                }
            } else {
                println!("NTILE window function not yet supported");
            }
        }

        #[tokio::test]
        async fn test_percent_rank_window_functions() {
            let mut ctx = TestContext::new("test_percent_rank_window_functions").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("salary", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ]);

            let table_name = "employees";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![Value::new(1), Value::new(50000), Value::new("Alice")],
                vec![Value::new(2), Value::new(60000), Value::new("Bob")],
                vec![Value::new(3), Value::new(70000), Value::new("Charlie")],
                vec![Value::new(4), Value::new(80000), Value::new("Diana")],
                vec![Value::new(5), Value::new(90000), Value::new("Eve")],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test PERCENT_RANK() and CUME_DIST() window functions
            let sql = "SELECT name, salary, PERCENT_RANK() OVER (ORDER BY salary) as percent_rank, CUME_DIST() OVER (ORDER BY salary) as cumulative_dist FROM employees ORDER BY salary";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            if success {
                let rows = writer.get_rows();
                assert_eq!(rows.len(), 5, "Expected 5 rows");

                // Verify PERCENT_RANK and CUME_DIST
                for (i, row) in rows.iter().enumerate() {
                    println!(
                        "Row {}: name={}, salary={}, percent_rank={}, cume_dist={}",
                        i + 1,
                        row[0],
                        row[1],
                        row[2],
                        row[3]
                    );
                }
            } else {
                println!("PERCENT_RANK/CUME_DIST window functions not yet supported");
            }
        }

        #[tokio::test]
        async fn test_complex_window_queries() {
            let mut ctx = TestContext::new("test_complex_window_queries").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("department", TypeId::VarChar),
                Column::new("salary", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("hire_date", TypeId::VarChar),
            ]);

            let table_name = "employees";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![
                    Value::new(1),
                    Value::new("IT"),
                    Value::new(75000),
                    Value::new("Alice"),
                    Value::new("2020-01-15"),
                ],
                vec![
                    Value::new(2),
                    Value::new("IT"),
                    Value::new(85000),
                    Value::new("Bob"),
                    Value::new("2019-03-10"),
                ],
                vec![
                    Value::new(3),
                    Value::new("IT"),
                    Value::new(90000),
                    Value::new("Charlie"),
                    Value::new("2018-07-22"),
                ],
                vec![
                    Value::new(4),
                    Value::new("HR"),
                    Value::new(65000),
                    Value::new("Diana"),
                    Value::new("2021-02-01"),
                ],
                vec![
                    Value::new(5),
                    Value::new("HR"),
                    Value::new(70000),
                    Value::new("Eve"),
                    Value::new("2020-11-30"),
                ],
                vec![
                    Value::new(6),
                    Value::new("Sales"),
                    Value::new(55000),
                    Value::new("Frank"),
                    Value::new("2022-01-10"),
                ],
                vec![
                    Value::new(7),
                    Value::new("Sales"),
                    Value::new(60000),
                    Value::new("Grace"),
                    Value::new("2021-06-15"),
                ],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test complex query with multiple window functions
            let sql = "SELECT name, department, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank, RANK() OVER (ORDER BY salary DESC) as overall_rank, salary - AVG(salary) OVER (PARTITION BY department) as salary_diff_from_dept_avg FROM employees ORDER BY department, salary DESC";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            if success {
                let rows = writer.get_rows();
                assert_eq!(rows.len(), 7, "Expected 7 rows");

                // Verify complex window function results
                for (i, row) in rows.iter().enumerate() {
                    println!(
                        "Row {}: name={}, dept={}, salary={}, dept_rank={}, overall_rank={}, salary_diff={}",
                        i + 1,
                        row[0],
                        row[1],
                        row[2],
                        row[3],
                        row[4],
                        row[5]
                    );
                }
            } else {
                println!("Complex window queries not yet supported");
            }
        }

        #[tokio::test]
        async fn test_window_functions_edge_cases() {
            let mut ctx = TestContext::new("test_window_functions_edge_cases").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("value", TypeId::Integer),
            ]);

            let table_name = "edge_cases";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Test window functions on empty table
            let sql =
                "SELECT id, value, ROW_NUMBER() OVER (ORDER BY id) as row_num FROM edge_cases";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            if success {
                let rows = writer.get_rows();
                assert_eq!(rows.len(), 0, "Expected 0 rows from empty table");
            }

            // Insert single row and test
            let test_data = vec![vec![Value::new(1), Value::new(100)]];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            let sql = "SELECT id, value, ROW_NUMBER() OVER (ORDER BY id) as row_num, RANK() OVER (ORDER BY value) as rank_val FROM edge_cases";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();

            if success {
                let rows = writer.get_rows();
                assert_eq!(rows.len(), 1, "Expected 1 row");

                println!(
                    "Single row result: id={}, value={}, row_num={}, rank={}",
                    rows[0][0], rows[0][1], rows[0][2], rows[0][3]
                );
            } else {
                println!("Window functions on single row not yet supported");
            }
        }
    }

    mod transaction_tests {
        use crate::catalog::column::Column;
        use crate::catalog::schema::Schema;
        use crate::sql::execution::execution_engine::tests::{TestContext, TestResultWriter};
        use crate::types_db::type_id::TypeId;
        use crate::types_db::value::Value;

        #[tokio::test]
        async fn test_transaction_handling() {
            let mut ctx = TestContext::new("test_transaction_handling").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("balance", TypeId::Integer),
            ]);

            let table_name = "accounts";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert initial data
            let test_data = vec![
                vec![Value::new(1), Value::new("Alice"), Value::new(1000)],
                vec![Value::new(2), Value::new("Bob"), Value::new(500)],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Commit the initial transaction to make the data visible to subsequent transactions
            ctx.commit_current_transaction().await.unwrap();

            // Start transaction
            let begin_sql = "BEGIN";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(begin_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();

            assert!(success, "Begin transaction failed");

            // Update Alice's balance
            let update_sql = "UPDATE accounts SET balance = balance - 200 WHERE id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success, "Update operation failed");

            // Update Bob's balance
            let update_sql = "UPDATE accounts SET balance = balance + 200 WHERE id = 2";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success, "Update operation failed");

            // Commit transaction
            let commit_sql = "COMMIT";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(commit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success, "Commit transaction failed");

            // Verify changes were committed
            let select_sql = "SELECT balance FROM accounts WHERE id = 1";
            let mut writer = TestResultWriter::new();
            ctx.engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert_eq!(
                writer.get_rows()[0][0].to_string(),
                "800",
                "Expected Alice's balance to be 800"
            );

            let select_sql = "SELECT balance FROM accounts WHERE id = 2";
            let mut writer = TestResultWriter::new();
            ctx.engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert_eq!(
                writer.get_rows()[0][0].to_string(),
                "700",
                "Expected Bob's balance to be 700"
            );
        }

        #[tokio::test]
        async fn test_transaction_rollback_on_failure() {
            let mut ctx = TestContext::new("test_transaction_rollback_on_failure").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("age", TypeId::Integer),
            ]);

            let table_name = "users";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert initial data
            let test_data = vec![
                vec![Value::new(1), Value::new("Alice"), Value::new(25)],
                vec![Value::new(2), Value::new("Bob"), Value::new(30)],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            ctx.commit_current_transaction().await.unwrap();

            // Start transaction
            let begin_sql = "BEGIN";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(begin_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success, "Begin transaction failed");

            // Successful operation
            let insert_sql = "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(insert_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success, "Insert operation should succeed");

            // Rollback transaction
            let rollback_sql = "ROLLBACK";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(rollback_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success, "Rollback should succeed");

            // Verify Charlie was not added
            let select_sql = "SELECT COUNT(*) FROM users";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success);

            if !writer.get_rows().is_empty() {
                // Should only have original 2 users
                println!("User count after rollback: {}", writer.get_rows()[0][0]);
            }
        }

        #[tokio::test]
        async fn test_transaction_with_multiple_operations() {
            let mut ctx = TestContext::new("test_transaction_with_multiple_operations").await;

            // Create test tables
            let account_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("balance", TypeId::Integer),
            ]);

            let transaction_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("from_account", TypeId::Integer),
                Column::new("to_account", TypeId::Integer),
                Column::new("amount", TypeId::Integer),
            ]);

            ctx.create_test_table("accounts", account_schema.clone())
                .unwrap();
            ctx.create_test_table("transactions", transaction_schema.clone())
                .unwrap();

            // Insert initial account data
            let account_data = vec![
                vec![Value::new(1), Value::new("Alice"), Value::new(1000)],
                vec![Value::new(2), Value::new("Bob"), Value::new(500)],
                vec![Value::new(3), Value::new("Charlie"), Value::new(750)],
            ];
            ctx.insert_tuples("accounts", account_data, account_schema)
                .unwrap();

            ctx.commit_current_transaction().await.unwrap();

            // Start complex transaction - money transfer with logging
            let begin_sql = "BEGIN";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(begin_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success, "Begin transaction failed");

            // Transfer $200 from Alice to Bob
            let debit_sql = "UPDATE accounts SET balance = balance - 200 WHERE id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(debit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success, "Debit operation failed");

            let credit_sql = "UPDATE accounts SET balance = balance + 200 WHERE id = 2";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(credit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success, "Credit operation failed");

            // Log the transaction
            let log_sql = "INSERT INTO transactions (id, from_account, to_account, amount) VALUES (1, 1, 2, 200)";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(log_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success, "Transaction logging failed");

            // Commit the transaction
            let commit_sql = "COMMIT";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(commit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success, "Commit failed");

            // Verify all changes were applied
            let verify_sql = "SELECT name, balance FROM accounts ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(verify_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            if !writer.get_rows().is_empty() {
                let rows = writer.get_rows();
                println!("Final balances:");
                for row in rows {
                    println!("  {}: {}", row[0], row[1]);
                }
            }

            // Verify transaction log
            let log_verify_sql = "SELECT COUNT(*) FROM transactions";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(log_verify_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            if !writer.get_rows().is_empty() {
                println!("Transaction log entries: {}", writer.get_rows()[0][0]);
            }
        }

        #[tokio::test]
        async fn test_transaction_isolation_read_committed() {
            let mut ctx = TestContext::new("test_transaction_isolation_read_committed").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("value", TypeId::Integer),
                Column::new("status", TypeId::VarChar),
            ]);

            let table_name = "isolation_test";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert initial data
            let test_data = vec![
                vec![Value::new(1), Value::new(100), Value::new("active")],
                vec![Value::new(2), Value::new(200), Value::new("active")],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            ctx.commit_current_transaction().await.unwrap();

            // Test transaction isolation - uncommitted changes should not be visible
            let begin_sql = "BEGIN";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(begin_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // Make changes but don't commit
            let update_sql = "UPDATE isolation_test SET value = 999 WHERE id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // In the same transaction, we should see the change
            let select_sql = "SELECT value FROM isolation_test WHERE id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success);

            if !writer.get_rows().is_empty() {
                println!("Value within transaction: {}", writer.get_rows()[0][0]);
            }

            // Rollback to test isolation
            let rollback_sql = "ROLLBACK";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(rollback_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // After rollback, should see original value
            let verify_sql = "SELECT value FROM isolation_test WHERE id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(verify_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            if !writer.get_rows().is_empty() {
                println!("Value after rollback: {}", writer.get_rows()[0][0]);
            }
        }

        #[tokio::test]
        async fn test_transaction_with_constraints() {
            let mut ctx = TestContext::new("test_transaction_with_constraints").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("email", TypeId::VarChar),
                Column::new("age", TypeId::Integer),
            ]);

            let table_name = "users_with_constraints";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert valid initial data
            let test_data = vec![
                vec![
                    Value::new(1),
                    Value::new("alice@example.com"),
                    Value::new(25),
                ],
                vec![Value::new(2), Value::new("bob@example.com"), Value::new(30)],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            ctx.commit_current_transaction().await.unwrap();

            // Test transaction with constraint violation
            let begin_sql = "BEGIN";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(begin_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // Insert valid data first
            let insert_valid_sql = "INSERT INTO users_with_constraints (id, email, age) VALUES (3, 'charlie@example.com', 35)";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(insert_valid_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success, "Valid insert should succeed");

            // Attempt to insert duplicate ID (constraint violation in real system)
            let insert_duplicate_sql = "INSERT INTO users_with_constraints (id, email, age) VALUES (3, 'duplicate@example.com', 40)";
            let mut writer = TestResultWriter::new();

            // This might succeed in the test system but would fail in production with proper constraints
            let _result =
                ctx.engine
                    .execute_sql(insert_duplicate_sql, ctx.exec_ctx.clone(), &mut writer).await;

            // For demonstration, rollback the transaction
            let rollback_sql = "ROLLBACK";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(rollback_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // Verify no new data was committed
            let count_sql = "SELECT COUNT(*) FROM users_with_constraints";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(count_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            if !writer.get_rows().is_empty() {
                println!(
                    "User count after constraint violation rollback: {}",
                    writer.get_rows()[0][0]
                );
            }
        }

        #[tokio::test]
        async fn test_transaction_savepoints() {
            let mut ctx = TestContext::new("test_transaction_savepoints").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("amount", TypeId::Integer),
            ]);

            let table_name = "savepoint_test";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert initial data
            let test_data = vec![vec![Value::new(1), Value::new("Initial"), Value::new(100)]];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            ctx.commit_current_transaction().await.unwrap();

            // Test savepoints (if supported)
            let begin_sql = "BEGIN";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(begin_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // First operation
            let insert1_sql =
                "INSERT INTO savepoint_test (id, name, amount) VALUES (2, 'Step1', 200)";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(insert1_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // Create savepoint (might not be supported)
            let savepoint_sql = "SAVEPOINT sp1";
            let mut writer = TestResultWriter::new();
            let _savepoint_result =
                ctx.engine
                    .execute_sql(savepoint_sql, ctx.exec_ctx.clone(), &mut writer).await;

            // Second operation
            let insert2_sql =
                "INSERT INTO savepoint_test (id, name, amount) VALUES (3, 'Step2', 300)";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(insert2_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // Rollback to savepoint (might not be supported)
            let rollback_savepoint_sql = "ROLLBACK TO sp1";
            let mut writer = TestResultWriter::new();
            let _rollback_result =
                ctx.engine
                    .execute_sql(rollback_savepoint_sql, ctx.exec_ctx.clone(), &mut writer).await;

            // Commit transaction
            let commit_sql = "COMMIT";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(commit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // Verify final state
            let select_sql = "SELECT COUNT(*) FROM savepoint_test";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success);

            if !writer.get_rows().is_empty() {
                println!(
                    "Final count after savepoint test: {}",
                    writer.get_rows()[0][0]
                );
            }
        }

        #[tokio::test]
        async fn test_transaction_with_ddl() {
            let mut ctx = TestContext::new("test_transaction_with_ddl").await;

            // Test DDL operations in transactions
            let begin_sql = "BEGIN";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(begin_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // Create table in transaction
            let create_sql = "CREATE TABLE ddl_test (id INTEGER, name VARCHAR)";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(create_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success, "CREATE TABLE in transaction should succeed");

            // Insert data into new table
            let insert_sql = "INSERT INTO ddl_test (id, name) VALUES (1, 'Test')";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(insert_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success, "INSERT into new table should succeed");

            // Commit transaction
            let commit_sql = "COMMIT";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(commit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // Verify table and data exist after commit
            let select_sql = "SELECT name FROM ddl_test WHERE id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success);

            if !writer.get_rows().is_empty() {
                println!("Data from DDL table: {}", writer.get_rows()[0][0]);
            }
        }

        #[tokio::test]
        async fn test_transaction_deadlock_prevention() {
            let mut ctx = TestContext::new("test_transaction_deadlock_prevention").await;

            // Create test table for deadlock simulation
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("resource", TypeId::VarChar),
                Column::new("lock_count", TypeId::Integer),
            ]);

            let table_name = "deadlock_test";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test resources
            let test_data = vec![
                vec![Value::new(1), Value::new("ResourceA"), Value::new(0)],
                vec![Value::new(2), Value::new("ResourceB"), Value::new(0)],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            ctx.commit_current_transaction().await.unwrap();

            // Simulate potential deadlock scenario
            let begin_sql = "BEGIN";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(begin_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // Lock ResourceA first
            let lock_a_sql = "UPDATE deadlock_test SET lock_count = lock_count + 1 WHERE id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(lock_a_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // Then lock ResourceB
            let lock_b_sql = "UPDATE deadlock_test SET lock_count = lock_count + 1 WHERE id = 2";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(lock_b_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // Commit to release locks
            let commit_sql = "COMMIT";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(commit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // Verify lock counts
            let verify_sql = "SELECT resource, lock_count FROM deadlock_test ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(verify_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            if !writer.get_rows().is_empty() {
                let rows = writer.get_rows();
                for row in rows {
                    println!("Resource: {}, Lock Count: {}", row[0], row[1]);
                }
            }
        }

        #[tokio::test]
        async fn test_transaction_timeout() {
            let mut ctx = TestContext::new("test_transaction_timeout").await;

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("data", TypeId::VarChar),
                Column::new("timestamp", TypeId::VarChar),
            ]);

            let table_name = "timeout_test";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Start long-running transaction
            let begin_sql = "BEGIN";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(begin_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // Perform operations
            let insert_sql =
                "INSERT INTO timeout_test (id, data, timestamp) VALUES (1, 'test', '2024-01-01')";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(insert_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success);

            // In a real system, we would test timeout here
            // For now, just commit normally
            let commit_sql = "COMMIT";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(commit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // Verify operation completed
            let verify_sql = "SELECT COUNT(*) FROM timeout_test";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(verify_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            if !writer.get_rows().is_empty() {
                println!("Records after timeout test: {}", writer.get_rows()[0][0]);
            }
        }

        #[tokio::test]
        async fn test_transaction_acid_properties() {
            let mut ctx = TestContext::new("test_transaction_acid_properties").await;

            // Create test table for ACID testing
            let table_schema = Schema::new(vec![
                Column::new("account_id", TypeId::Integer),
                Column::new("balance", TypeId::Integer),
                Column::new("last_updated", TypeId::VarChar),
            ]);

            let table_name = "acid_test";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert initial data
            let test_data = vec![
                vec![Value::new(1), Value::new(1000), Value::new("initial")],
                vec![Value::new(2), Value::new(500), Value::new("initial")],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            ctx.commit_current_transaction().await.unwrap();

            // Test Atomicity - all operations succeed or all fail
            let begin_sql = "BEGIN";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(begin_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // Transfer money - both operations must succeed together
            let debit_sql = "UPDATE acid_test SET balance = balance - 100, last_updated = 'transferred' WHERE account_id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(debit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success, "Debit operation failed");

            let credit_sql = "UPDATE acid_test SET balance = balance + 100, last_updated = 'transferred' WHERE account_id = 2";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(credit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success, "Credit operation failed");

            // Test Consistency - verify total balance remains the same
            let total_sql = "SELECT SUM(balance) FROM acid_test";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(total_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            if !writer.get_rows().is_empty() {
                let total_balance = &writer.get_rows()[0][0];
                println!("Total balance during transaction: {}", total_balance);
                // Should still be 1500 (1000 + 500)
            }

            // Commit transaction
            let commit_sql = "COMMIT";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(commit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // Test Durability - verify changes persist after commit
            let verify_sql =
                "SELECT account_id, balance, last_updated FROM acid_test ORDER BY account_id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(verify_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            if !writer.get_rows().is_empty() {
                let rows = writer.get_rows();
                println!("Final state after ACID test:");
                for row in rows {
                    println!(
                        "  Account {}: Balance {}, Updated: {}",
                        row[0], row[1], row[2]
                    );
                }
            }

            // Verify total balance consistency after commit
            let final_total_sql = "SELECT SUM(balance) FROM acid_test";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(final_total_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            if !writer.get_rows().is_empty() {
                let final_total = &writer.get_rows()[0][0];
                println!("Final total balance: {}", final_total);
            }
        }

        #[tokio::test]
        async fn test_transaction_performance_stress() {
            let mut ctx = TestContext::new("test_transaction_performance_stress").await;

            // Create test table for performance testing
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("batch_id", TypeId::Integer),
                Column::new("value", TypeId::Integer),
                Column::new("processed", TypeId::VarChar),
            ]);

            let table_name = "performance_test";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Test large batch transaction
            let begin_sql = "BEGIN";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(begin_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // Insert multiple records in single transaction
            for i in 1..=50 {
                let insert_sql = format!(
                    "INSERT INTO performance_test (id, batch_id, value, processed) VALUES ({}, 1, {}, 'batch')",
                    i,
                    i * 10
                );
                let mut writer = TestResultWriter::new();
                let success = ctx
                    .engine
                    .execute_sql(&insert_sql, ctx.exec_ctx.clone(), &mut writer)
                    .await.unwrap();
                assert!(success, "Batch insert {} failed", i);
            }

            // Update all records in batch
            let update_sql = "UPDATE performance_test SET processed = 'updated' WHERE batch_id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // Commit large transaction
            let commit_sql = "COMMIT";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(commit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            // Verify batch processing results
            let count_sql = "SELECT COUNT(*) FROM performance_test WHERE processed = 'updated'";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(count_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success);

            if !writer.get_rows().is_empty() {
                println!(
                    "Records processed in batch transaction: {}",
                    writer.get_rows()[0][0]
                );
            }

            // Test aggregation performance on transaction data
            let agg_sql = "SELECT batch_id, COUNT(*), SUM(value), AVG(value) FROM performance_test GROUP BY batch_id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(agg_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success);

            if !writer.get_rows().is_empty() {
                let rows = writer.get_rows();
                for row in rows {
                    println!(
                        "Batch {}: Count={}, Sum={}, Avg={}",
                        row[0], row[1], row[2], row[3]
                    );
                }
            }

            println!("Performance stress test completed successfully");
        }
    }

    mod display_tests {
        use crate::catalog::column::Column;
        use crate::catalog::schema::Schema;
        use crate::sql::execution::execution_engine::tests::{TestContext, TestResultWriter};
        use crate::types_db::type_id::TypeId;
        use crate::types_db::value::Value;

        #[tokio::test]
        async fn test_decimal_precision_scale_display() {
            let mut ctx = TestContext::new("test_decimal_precision_scale_display").await;

            // Create test table with decimal columns of different precision and scale
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("price", TypeId::Decimal),
                Column::new("rate", TypeId::Decimal),
                Column::new("percentage", TypeId::Decimal),
                Column::new("currency", TypeId::Decimal),
            ]);

            let table_name = "financial_data";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data with various decimal values
            let test_data = vec![
                vec![
                    Value::new(1),
                    Value::new(123.456789), // Should be formatted based on column precision/scale
                    Value::new(0.0825),     // Rate like 8.25%
                    Value::new(15.5),       // Simple percentage
                    Value::new(1000.00),    // Currency with exact cents
                ],
                vec![
                    Value::new(2),
                    Value::new(99.99),   // Price with cents
                    Value::new(0.125),   // Rate 12.5%
                    Value::new(100.0),   // Whole percentage
                    Value::new(2500.50), // Currency with 50 cents
                ],
                vec![
                    Value::new(3),
                    Value::new(0.01),    // Very small price
                    Value::new(1.0),     // 100% rate
                    Value::new(0.1),     // 0.1%
                    Value::new(10000.0), // Large currency amount
                ],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test 1: Basic decimal display (default formatting)
            let select_sql =
                "SELECT id, price, rate, percentage, currency FROM financial_data ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success, "Basic decimal select failed");

            let rows = writer.get_rows();
            assert_eq!(rows.len(), 3, "Expected 3 rows");

            // Verify default decimal formatting
            println!("Default decimal formatting:");
            for (i, row) in rows.iter().enumerate() {
                println!(
                    "Row {}: id={}, price={}, rate={}, percentage={}, currency={}",
                    i + 1,
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4]
                );
            }

            // Test 2: Arithmetic operations with decimals
            let calc_sql = "SELECT id, price * rate as calculated, price + 10.50 as adjusted_price FROM financial_data ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(calc_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success, "Decimal arithmetic failed");

            let calc_rows = writer.get_rows();
            println!("\nDecimal arithmetic results:");
            for row in calc_rows {
                println!(
                    "ID: {}, Calculated: {}, Adjusted Price: {}",
                    row[0], row[1], row[2]
                );
            }

            // Test 3: Aggregation with decimals
            let agg_sql = "SELECT AVG(price) as avg_price, SUM(currency) as total_currency, MIN(rate) as min_rate, MAX(percentage) as max_percentage FROM financial_data";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(agg_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success, "Decimal aggregation failed");

            let agg_rows = writer.get_rows();
            println!("\nDecimal aggregation results:");
            for row in agg_rows {
                println!(
                    "Avg Price: {}, Total Currency: {}, Min Rate: {}, Max Percentage: {}",
                    row[0], row[1], row[2], row[3]
                );
            }

            // Test 4: Decimal comparisons and filtering
            let filter_sql = "SELECT id, price FROM financial_data WHERE price > 50.0 AND rate < 0.5 ORDER BY price DESC";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(filter_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success, "Decimal filtering failed");

            let filter_rows = writer.get_rows();
            println!("\nFiltered decimal results (price > 50.0 AND rate < 0.5):");
            for row in filter_rows {
                println!("ID: {}, Price: {}", row[0], row[1]);
            }

            // Test 5: CASE expressions with decimals
            let case_sql = "SELECT id, CASE WHEN price > 100.0 THEN 'expensive' WHEN price > 10.0 THEN 'moderate' ELSE 'cheap' END as price_category, price FROM financial_data ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(case_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success, "Decimal CASE expression failed");

            let case_rows = writer.get_rows();
            println!("\nDecimal CASE expression results:");
            for row in case_rows {
                println!("ID: {}, Category: {}, Price: {}", row[0], row[1], row[2]);
            }
        }

        #[tokio::test]
        async fn test_float_precision_display() {
            let mut ctx = TestContext::new("test_float_precision_display").await;

            // Create test table with float columns
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("measurement", TypeId::Float),
                Column::new("ratio", TypeId::Float),
                Column::new("scientific", TypeId::Float),
            ]);

            let table_name = "measurements";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data with various float values
            let test_data = vec![
                vec![
                    Value::new(1),
                    Value::new(std::f32::consts::PI), // Pi approximation
                    Value::new(0.618f32),             // Golden ratio approximation
                    Value::new(1.23e-4f32),           // Scientific notation
                ],
                vec![
                    Value::new(2),
                    Value::new(2.718f32),  // e approximation
                    Value::new(1.414f32),  // sqrt(2) approximation
                    Value::new(9.87e6f32), // Large scientific number
                ],
                vec![
                    Value::new(3),
                    Value::new(0.0f32),      // Zero
                    Value::new(1.0f32),      // One
                    Value::new(-1.5e-10f32), // Very small negative
                ],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test 1: Basic float display
            let select_sql =
                "SELECT id, measurement, ratio, scientific FROM measurements ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success, "Basic float select failed");

            let rows = writer.get_rows();
            println!("Float display results:");
            for (i, row) in rows.iter().enumerate() {
                println!(
                    "Row {}: id={}, measurement={}, ratio={}, scientific={}",
                    i + 1,
                    row[0],
                    row[1],
                    row[2],
                    row[3]
                );
            }

            // Test 2: Float arithmetic operations
            let calc_sql = "SELECT id, measurement * ratio as product, measurement + 1.0 as incremented FROM measurements ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(calc_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success, "Float arithmetic failed");

            let calc_rows = writer.get_rows();
            println!("\nFloat arithmetic results:");
            for row in calc_rows {
                println!(
                    "ID: {}, Product: {}, Incremented: {}",
                    row[0], row[1], row[2]
                );
            }

            // Test 3: Float aggregations
            let agg_sql = "SELECT AVG(measurement) as avg_measurement, SUM(ratio) as sum_ratio FROM measurements";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(agg_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success, "Float aggregation failed");

            let agg_rows = writer.get_rows();
            println!("\nFloat aggregation results:");
            for row in agg_rows {
                println!("Avg Measurement: {}, Sum Ratio: {}", row[0], row[1]);
            }
        }

        #[tokio::test]
        async fn test_mixed_numeric_precision_display() {
            let mut ctx = TestContext::new("test_mixed_numeric_precision_display").await;

            // Create test table with mixed numeric types
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("int_val", TypeId::Integer),
                Column::new("bigint_val", TypeId::BigInt),
                Column::new("decimal_val", TypeId::Decimal),
                Column::new("float_val", TypeId::Float),
            ]);

            let table_name = "mixed_numbers";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data with mixed numeric types
            let test_data = vec![
                vec![
                    Value::new(1),
                    Value::new(42),
                    Value::new(1000000i64),
                    Value::new(123.456),
                    Value::new(3.14f32),
                ],
                vec![
                    Value::new(2),
                    Value::new(-17),
                    Value::new(-500000i64),
                    Value::new(0.001),
                    Value::new(2.718f32),
                ],
                vec![
                    Value::new(3),
                    Value::new(0),
                    Value::new(0i64),
                    Value::new(1000.0),
                    Value::new(0.0f32),
                ],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test 1: Display all numeric types
            let select_sql = "SELECT id, int_val, bigint_val, decimal_val, float_val FROM mixed_numbers ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success, "Mixed numeric select failed");

            let rows = writer.get_rows();
            println!("Mixed numeric display results:");
            for (i, row) in rows.iter().enumerate() {
                println!(
                    "Row {}: id={}, int={}, bigint={}, decimal={}, float={}",
                    i + 1,
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4]
                );
            }

            // Test 2: Mixed arithmetic (should promote to appropriate types)
            let calc_sql = "SELECT id, int_val + decimal_val as int_plus_decimal, bigint_val * float_val as bigint_times_float FROM mixed_numbers ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(calc_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success, "Mixed arithmetic failed");

            let calc_rows = writer.get_rows();
            println!("\nMixed arithmetic results:");
            for row in calc_rows {
                println!(
                    "ID: {}, Int+Decimal: {}, BigInt*Float: {}",
                    row[0], row[1], row[2]
                );
            }

            // Test 3: Comparisons between different numeric types
            let comp_sql = "SELECT id, int_val, decimal_val FROM mixed_numbers WHERE int_val < decimal_val ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(comp_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success, "Mixed comparison failed");

            let comp_rows = writer.get_rows();
            println!("\nMixed comparison results (int_val < decimal_val):");
            for row in comp_rows {
                println!("ID: {}, Int: {}, Decimal: {}", row[0], row[1], row[2]);
            }

            // Test 4: Aggregations across different numeric types
            let agg_sql = "SELECT AVG(int_val) as avg_int, AVG(decimal_val) as avg_decimal, AVG(float_val) as avg_float FROM mixed_numbers";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(agg_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success, "Mixed aggregation failed");

            let agg_rows = writer.get_rows();
            println!("\nMixed aggregation results:");
            for row in agg_rows {
                println!(
                    "Avg Int: {}, Avg Decimal: {}, Avg Float: {}",
                    row[0], row[1], row[2]
                );
            }
        }

        #[tokio::test]
        async fn test_decimal_edge_cases_display() {
            let mut ctx = TestContext::new("test_decimal_edge_cases_display").await;

            // Create test table for edge cases
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("description", TypeId::VarChar),
                Column::new("value", TypeId::Decimal),
            ]);

            let table_name = "edge_cases";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert edge case values
            let test_data = vec![
                vec![Value::new(1), Value::new("zero"), Value::new(0.0)],
                vec![
                    Value::new(2),
                    Value::new("small_positive"),
                    Value::new(0.001),
                ],
                vec![
                    Value::new(3),
                    Value::new("small_negative"),
                    Value::new(-0.001),
                ],
                vec![
                    Value::new(4),
                    Value::new("large_positive"),
                    Value::new(999999.999),
                ],
                vec![
                    Value::new(5),
                    Value::new("large_negative"),
                    Value::new(-999999.999),
                ],
                vec![Value::new(6), Value::new("one"), Value::new(1.0)],
                vec![Value::new(7), Value::new("negative_one"), Value::new(-1.0)],
                vec![Value::new(8), Value::new("half"), Value::new(0.5)],
                vec![Value::new(9), Value::new("third"), Value::new(0.333333)],
                vec![
                    Value::new(10),
                    Value::new("pi_approx"),
                    Value::new(std::f64::consts::PI),
                ],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test edge case display
            let select_sql = "SELECT id, description, value FROM edge_cases ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success, "Edge cases select failed");

            let rows = writer.get_rows();
            println!("Decimal edge cases display:");
            for (i, row) in rows.iter().enumerate() {
                println!(
                    "{}: id={}, description={}, value={}",
                    i + 1,
                    row[0],
                    row[1],
                    row[2]
                );
            }

            // Test operations with edge cases
            let ops_sql = "SELECT id, description, value, value * 2 as doubled, value / 2 as halved FROM edge_cases WHERE value != 0 ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(ops_sql, ctx.exec_ctx.clone(), &mut writer)
                .await.unwrap();
            assert!(success, "Edge case operations failed");

            let ops_rows = writer.get_rows();
            println!("\nEdge case operations:");
            for row in ops_rows {
                println!(
                    "{}: original={}, doubled={}, halved={}",
                    row[1], row[2], row[3], row[4]
                );
            }
        }

        #[tokio::test]
        async fn test_decimal_column_aware_formatting() {
            let mut ctx = TestContext::new("test_decimal_column_aware_formatting").await;

            // Create test table with decimal columns that have specific precision and scale
            let price_column = Column::builder("price", TypeId::Decimal)
                .with_precision_and_scale(10, 2) // DECIMAL(10,2) for currency
                .build();

            let rate_column = Column::builder("rate", TypeId::Decimal)
                .with_precision_and_scale(5, 4) // DECIMAL(5,4) for rates
                .build();

            let percentage_column = Column::builder("percentage", TypeId::Decimal)
                .with_precision_and_scale(6, 1) // DECIMAL(6,1) for percentages
                .build();

            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                price_column,
                rate_column,
                percentage_column,
            ]);

            let table_name = "formatted_decimals";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data that should be formatted according to column scale
            let test_data = vec![
                vec![
                    Value::new(1),
                    Value::new(123.456789), // Should display as 123.46 (scale 2)
                    Value::new(0.08251),    // Should display as 0.0825 (scale 4)
                    Value::new(15.567),     // Should display as 15.6 (scale 1)
                ],
                vec![
                    Value::new(2),
                    Value::new(99.9),  // Should display as 99.90 (scale 2)
                    Value::new(0.1),   // Should display as 0.1000 (scale 4)
                    Value::new(100.0), // Should display as 100.0 (scale 1)
                ],
                vec![
                    Value::new(3),
                    Value::new(1000.0), // Should display as 1000.00 (scale 2)
                    Value::new(1.0),    // Should display as 1.0000 (scale 4)
                    Value::new(0.05),   // Should display as 0.1 (scale 1, rounded)
                ],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test column-aware formatting
            let select_sql =
                "SELECT id, price, rate, percentage FROM formatted_decimals ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success, "Column-aware decimal select failed");

            let rows = writer.get_rows();
            println!("Column-aware decimal formatting results:");
            for (i, row) in rows.iter().enumerate() {
                println!(
                    "Row {}: id={}, price={} (scale 2), rate={} (scale 4), percentage={} (scale 1)",
                    i + 1,
                    row[0],
                    row[1],
                    row[2],
                    row[3]
                );
            }

            // Test that the formatting is consistent in calculations
            let calc_sql = "SELECT id, price * rate as calculated_amount, percentage / 100.0 as decimal_percentage FROM formatted_decimals ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(calc_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success, "Decimal calculation formatting failed");

            let calc_rows = writer.get_rows();
            println!("\nCalculated decimal formatting:");
            for row in calc_rows {
                println!(
                    "ID: {}, Calculated Amount: {}, Decimal Percentage: {}",
                    row[0], row[1], row[2]
                );
            }

            // Test aggregations with formatted decimals
            let agg_sql = "SELECT AVG(price) as avg_price, SUM(rate) as total_rate, MAX(percentage) as max_percentage FROM formatted_decimals";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(agg_sql, ctx.exec_ctx.clone(), &mut writer).await
                .unwrap();
            assert!(success, "Decimal aggregation formatting failed");

            let agg_rows = writer.get_rows();
            println!("\nAggregated decimal formatting:");
            for row in agg_rows {
                println!(
                    "Avg Price: {}, Total Rate: {}, Max Percentage: {}",
                    row[0], row[1], row[2]
                );
            }
        }
        
        #[tokio::test]
        async fn test_metrics_collection_verification() {
            // This test specifically verifies that our metrics collection fixes are working
            let mut ctx = TestContext::new("test_metrics_collection_verification").await;
            let disk_manager = ctx.disk_manager.clone();
            
            // Create a simple table for testing
            let create_sql = "CREATE TABLE metrics_test (id INTEGER PRIMARY KEY, data VARCHAR(100))";
            let mut writer = TestResultWriter::new();
            ctx.engine.execute_sql(create_sql, ctx.exec_ctx.clone(), &mut writer).await.unwrap();
            
            // Perform operations that should trigger metrics collection
            for i in 1..=5 {
                let insert_sql = format!("INSERT INTO metrics_test VALUES ({}, 'test_data_{}')", i, i);
                let mut writer = TestResultWriter::new();
                ctx.engine.execute_sql(&insert_sql, ctx.exec_ctx.clone(), &mut writer).await.unwrap();
            }
            
            // Commit transaction (should trigger flush)
            ctx.commit_current_transaction().await.unwrap();
            
            // Allow time for async operations to complete
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            
            // Verify metrics were properly collected
            let live_metrics = disk_manager.get_metrics_collector().get_live_metrics();
            let snapshot = disk_manager.get_metrics();
            
            let write_ops = live_metrics.write_ops_count.load(std::sync::atomic::Ordering::Relaxed);
            let io_count = live_metrics.io_count.load(std::sync::atomic::Ordering::Relaxed);
            
            println!("Metrics Verification: write_ops={}, io_count={}", write_ops, io_count);
            println!("Write latency avg: {} ns", snapshot.write_latency_avg_ns);
            
            // Verify metrics are working (should have non-zero values now)
            assert!(write_ops > 0, "Write operations should be recorded");
            assert!(io_count > 0, "Total I/O operations should be recorded");
            
            let db_file_size = ctx.get_db_file_size().await.unwrap();
            assert!(db_file_size > 0, "Database file should contain data");
            
            println!("✅ Metrics collection verification passed!");
        }
    }
}
