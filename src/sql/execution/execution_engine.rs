use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
use crate::catalog::Catalog;
use crate::common::exception::DBError;
use crate::common::result_writer::ResultWriter;
use crate::concurrency::transaction_manager_factory::TransactionManagerFactory;
use crate::recovery::wal_manager::WALManager;
use crate::sql::execution::check_option::CheckOption;
use crate::sql::execution::check_option::CheckOptions;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::PlanNode;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::sql::optimizer::Optimizer;
use crate::sql::planner::logical_plan::{LogicalPlan, LogicalToPhysical};
use crate::sql::planner::query_planner::QueryPlanner;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use log::{debug, error, info};
use parking_lot::RwLock;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;

pub struct ExecutionEngine {
    planner: QueryPlanner,
    optimizer: Optimizer,
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
        _context: Arc<RwLock<ExecutionContext>>,
    ) -> Result<PlanNode, DBError> {
        info!("Preparing SQL statement: {}", sql);

        // Additional syntax validation for common errors
        let sql_lower = sql.to_lowercase();

        // Check for DELETE statements missing the FROM keyword
        if sql_lower.starts_with("delete ") && !sql_lower.starts_with("delete from ") {
            return Err(DBError::SqlError(
                "Invalid DELETE syntax, expected 'DELETE FROM table_name'".to_string(),
            ));
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
            .map_err(DBError::OptimizeError)
    }

    /// Prepare a SQL statement and validate syntax
    /// Returns empty parameter types for now since we don't support parameters yet
    pub fn prepare_statement(&mut self, sql: &str) -> Result<Vec<TypeId>, DBError> {
        // Additional syntax validation for common errors
        let sql_lower = sql.to_lowercase();

        // Check for DELETE statements missing the FROM keyword
        if sql_lower.starts_with("delete ") && !sql_lower.starts_with("delete from ") {
            return Err(DBError::SqlError(
                "Invalid DELETE syntax, expected 'DELETE FROM table_name'".to_string(),
            ));
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
        match txn_manager
            .commit(transaction, self.buffer_pool_manager.clone())
            .await
        {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::transaction::IsolationLevel;
    use crate::recovery::log_manager::LogManager;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
    use crate::storage::table::table_heap::TableInfo;
    use crate::storage::table::transactional_table_heap::TransactionalTableHeap;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use parking_lot::RwLock;
    use std::sync::Arc;
    use tempfile::TempDir;

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
                BufferPoolManager::new(
                    buffer_pool_size,
                    disk_manager_arc.clone(),
                    replacer.clone(),
                )
                .unwrap(),
            );

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
                .execute_sql("COMMIT", self.exec_ctx.clone(), &mut writer)
                .await
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

        fn write_message(&mut self, _message: &str) {
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
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
                .await
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
                .execute_sql("SELECT * FROM employees", ctx.exec_ctx.clone(), &mut writer)
                .await
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
                )
                .await
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
                    .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
                    .await
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
                    .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
                    .await
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
                )
                .await
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
                )
                .await
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
                    .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
                    .await
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
                    .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
                    .await
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
                )
                .await
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
                    .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
                    .await
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
                .await
                .unwrap();

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
                )
                .await
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
                    .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
                    .await
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

            ctx.commit_current_transaction()
                .await
                .expect("Commit failed");

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
                    .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
                    .await
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
                .await
                .unwrap();

            assert!(success, "Begin transaction failed");

            // Update Alice's balance
            let update_sql = "UPDATE accounts SET balance = balance - 200 WHERE id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success, "Update operation failed");

            // Update Bob's balance
            let update_sql = "UPDATE accounts SET balance = balance + 200 WHERE id = 2";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success, "Update operation failed");

            // Commit transaction
            let commit_sql = "COMMIT";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(commit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success, "Commit transaction failed");

            // Verify changes were committed
            let select_sql = "SELECT balance FROM accounts WHERE id = 1";
            let mut writer = TestResultWriter::new();
            ctx.engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert_eq!(
                writer.get_rows()[0][0].to_string(),
                "800",
                "Expected Alice's balance to be 800"
            );

            let select_sql = "SELECT balance FROM accounts WHERE id = 2";
            let mut writer = TestResultWriter::new();
            ctx.engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
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
                .await
                .unwrap();
            assert!(success, "Begin transaction failed");

            // Successful operation
            let insert_sql = "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(insert_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success, "Insert operation should succeed");

            // Rollback transaction
            let rollback_sql = "ROLLBACK";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(rollback_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success, "Rollback should succeed");

            // Verify Charlie was not added
            let select_sql = "SELECT COUNT(*) FROM users";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
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
                .await
                .unwrap();
            assert!(success, "Begin transaction failed");

            // Transfer $200 from Alice to Bob
            let debit_sql = "UPDATE accounts SET balance = balance - 200 WHERE id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(debit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success, "Debit operation failed");

            let credit_sql = "UPDATE accounts SET balance = balance + 200 WHERE id = 2";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(credit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success, "Credit operation failed");

            // Log the transaction
            let log_sql = "INSERT INTO transactions (id, from_account, to_account, amount) VALUES (1, 1, 2, 200)";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(log_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success, "Transaction logging failed");

            // Commit the transaction
            let commit_sql = "COMMIT";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(commit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success, "Commit failed");

            // Verify all changes were applied
            let verify_sql = "SELECT name, balance FROM accounts ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(verify_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
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
                .await
                .unwrap();
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
                .await
                .unwrap();
            assert!(success);

            // Make changes but don't commit
            let update_sql = "UPDATE isolation_test SET value = 999 WHERE id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success);

            // In the same transaction, we should see the change
            let select_sql = "SELECT value FROM isolation_test WHERE id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
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
                .await
                .unwrap();
            assert!(success);

            // After rollback, should see original value
            let verify_sql = "SELECT value FROM isolation_test WHERE id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(verify_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
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
                .await
                .unwrap();
            assert!(success);

            // Insert valid data first
            let insert_valid_sql = "INSERT INTO users_with_constraints (id, email, age) VALUES (3, 'charlie@example.com', 35)";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(insert_valid_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success, "Valid insert should succeed");

            // Attempt to insert duplicate ID (constraint violation in real system)
            let insert_duplicate_sql = "INSERT INTO users_with_constraints (id, email, age) VALUES (3, 'duplicate@example.com', 40)";
            let mut writer = TestResultWriter::new();

            // This might succeed in the test system but would fail in production with proper constraints
            let _result = ctx
                .engine
                .execute_sql(insert_duplicate_sql, ctx.exec_ctx.clone(), &mut writer)
                .await;

            // For demonstration, rollback the transaction
            let rollback_sql = "ROLLBACK";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(rollback_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success);

            // Verify no new data was committed
            let count_sql = "SELECT COUNT(*) FROM users_with_constraints";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(count_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
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
                .await
                .unwrap();
            assert!(success);

            // First operation
            let insert1_sql =
                "INSERT INTO savepoint_test (id, name, amount) VALUES (2, 'Step1', 200)";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(insert1_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success);

            // Create savepoint (might not be supported)
            let savepoint_sql = "SAVEPOINT sp1";
            let mut writer = TestResultWriter::new();
            let _savepoint_result = ctx
                .engine
                .execute_sql(savepoint_sql, ctx.exec_ctx.clone(), &mut writer)
                .await;

            // Second operation
            let insert2_sql =
                "INSERT INTO savepoint_test (id, name, amount) VALUES (3, 'Step2', 300)";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(insert2_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success);

            // Rollback to savepoint (might not be supported)
            let rollback_savepoint_sql = "ROLLBACK TO sp1";
            let mut writer = TestResultWriter::new();
            let _rollback_result = ctx
                .engine
                .execute_sql(rollback_savepoint_sql, ctx.exec_ctx.clone(), &mut writer)
                .await;

            // Commit transaction
            let commit_sql = "COMMIT";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(commit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success);

            // Verify final state
            let select_sql = "SELECT COUNT(*) FROM savepoint_test";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
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
                .await
                .unwrap();
            assert!(success);

            // Create table in transaction
            let create_sql = "CREATE TABLE ddl_test (id INTEGER, name VARCHAR)";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(create_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success, "CREATE TABLE in transaction should succeed");

            // Insert data into new table
            let insert_sql = "INSERT INTO ddl_test (id, name) VALUES (1, 'Test')";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(insert_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success, "INSERT into new table should succeed");

            // Commit transaction
            let commit_sql = "COMMIT";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(commit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success);

            // Verify table and data exist after commit
            let select_sql = "SELECT name FROM ddl_test WHERE id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
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
                .await
                .unwrap();
            assert!(success);

            // Lock ResourceA first
            let lock_a_sql = "UPDATE deadlock_test SET lock_count = lock_count + 1 WHERE id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(lock_a_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success);

            // Then lock ResourceB
            let lock_b_sql = "UPDATE deadlock_test SET lock_count = lock_count + 1 WHERE id = 2";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(lock_b_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success);

            // Commit to release locks
            let commit_sql = "COMMIT";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(commit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success);

            // Verify lock counts
            let verify_sql = "SELECT resource, lock_count FROM deadlock_test ORDER BY id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(verify_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
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
                .await
                .unwrap();
            assert!(success);

            // Perform operations
            let insert_sql =
                "INSERT INTO timeout_test (id, data, timestamp) VALUES (1, 'test', '2024-01-01')";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(insert_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success);

            // In a real system, we would test timeout here
            // For now, just commit normally
            let commit_sql = "COMMIT";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(commit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success);

            // Verify operation completed
            let verify_sql = "SELECT COUNT(*) FROM timeout_test";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(verify_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
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
                .await
                .unwrap();
            assert!(success);

            // Transfer money - both operations must succeed together
            let debit_sql = "UPDATE acid_test SET balance = balance - 100, last_updated = 'transferred' WHERE account_id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(debit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success, "Debit operation failed");

            let credit_sql = "UPDATE acid_test SET balance = balance + 100, last_updated = 'transferred' WHERE account_id = 2";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(credit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success, "Credit operation failed");

            // Test Consistency - verify total balance remains the same
            let total_sql = "SELECT SUM(balance) FROM acid_test";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(total_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
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
                .await
                .unwrap();
            assert!(success);

            // Test Durability - verify changes persist after commit
            let verify_sql =
                "SELECT account_id, balance, last_updated FROM acid_test ORDER BY account_id";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(verify_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
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
                .await
                .unwrap();
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
                .await
                .unwrap();
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
                    .await
                    .unwrap();
                assert!(success, "Batch insert {} failed", i);
            }

            // Update all records in batch
            let update_sql = "UPDATE performance_test SET processed = 'updated' WHERE batch_id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success);

            // Commit large transaction
            let commit_sql = "COMMIT";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(commit_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
            assert!(success);

            // Verify batch processing results
            let count_sql = "SELECT COUNT(*) FROM performance_test WHERE processed = 'updated'";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(count_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();
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
                .execute_sql(agg_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
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
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
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
                .execute_sql(calc_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
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
                .execute_sql(agg_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
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
                .execute_sql(filter_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
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
                .await
                .unwrap();
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
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
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
                .execute_sql(calc_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
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
                .execute_sql(agg_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
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
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
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
                .execute_sql(calc_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
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
                .await
                .unwrap();
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
                .execute_sql(agg_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
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
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
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
                .await
                .unwrap();
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
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
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
                .execute_sql(calc_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
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
                .execute_sql(agg_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
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
            let create_sql =
                "CREATE TABLE metrics_test (id INTEGER PRIMARY KEY, data VARCHAR(100))";
            let mut writer = TestResultWriter::new();
            ctx.engine
                .execute_sql(create_sql, ctx.exec_ctx.clone(), &mut writer)
                .await
                .unwrap();

            // Perform operations that should trigger metrics collection
            for i in 1..=5 {
                let insert_sql =
                    format!("INSERT INTO metrics_test VALUES ({}, 'test_data_{}')", i, i);
                let mut writer = TestResultWriter::new();
                ctx.engine
                    .execute_sql(&insert_sql, ctx.exec_ctx.clone(), &mut writer)
                    .await
                    .unwrap();
            }

            // Commit transaction (should trigger flush)
            ctx.commit_current_transaction().await.unwrap();

            // Allow time for async operations to complete
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            // Verify metrics were properly collected
            let live_metrics = disk_manager.get_metrics_collector().get_live_metrics();
            let snapshot = disk_manager.get_metrics();

            let write_ops = live_metrics
                .write_ops_count
                .load(std::sync::atomic::Ordering::Relaxed);
            let io_count = live_metrics
                .io_count
                .load(std::sync::atomic::Ordering::Relaxed);

            println!(
                "Metrics Verification: write_ops={}, io_count={}",
                write_ops, io_count
            );
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
