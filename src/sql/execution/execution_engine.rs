use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::catalog::catalog::Catalog;
use crate::common::exception::DBError;
use crate::common::result_writer::ResultWriter;
use crate::concurrency::transaction_manager_factory::TransactionManagerFactory;
use crate::recovery::wal_manager::WALManager;
use crate::sql::execution::check_option::CheckOptions;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::sql::execution::plans::abstract_plan::PlanNode;
use crate::sql::execution::plans::insert_plan::InsertNode;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::sql::optimizer::optimizer::Optimizer;
use crate::sql::planner::logical_plan::{LogicalPlan, LogicalToPhysical};
use crate::sql::planner::query_planner::QueryPlanner;
use crate::storage::table::transactional_table_heap::TransactionalTableHeap;
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
    pub fn execute_sql(
        &mut self,
        sql: &str,
        context: Arc<RwLock<ExecutionContext>>,
        writer: &mut impl ResultWriter,
    ) -> Result<bool, DBError> {
        // Parse and plan the SQL statement
        let plan = self.prepare_sql(sql, context.clone())?;

        // Execute the plan
        self.execute_plan(&plan, context, writer)
    }

    /// Prepare a SQL statement for execution
    fn prepare_sql(
        &mut self,
        sql: &str,
        context: Arc<RwLock<ExecutionContext>>,
    ) -> Result<PlanNode, DBError> {
        info!("Preparing SQL statement: {}", sql);

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
    fn execute_plan(
        &self,
        plan: &PlanNode,
        context: Arc<RwLock<ExecutionContext>>,
        writer: &mut dyn ResultWriter,
    ) -> Result<bool, DBError> {
        debug!("Executing physical plan: {}", plan);

        // Create executor in a stack-efficient way
        let mut root_executor = match self.create_executor(plan, context) {
            Ok(executor) => executor,
            Err(e) => {
                error!("Failed to create executor: {}", e);
                return Err(e);
            }
        };

        // Initialize executor
        debug!("Initializing root executor");
        root_executor.init();
        debug!("Root executor initialized");

        match plan {
            PlanNode::Insert(_) | PlanNode::CreateTable(_) | PlanNode::CreateIndex(_) => {
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

                // For Insert, CreateTable, and CreateIndex, return true if execution completed successfully
                match plan {
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
                        match self.commit_transaction(txn_context.clone()) {
                            Ok(success) => {
                                info!("Transaction {} committed successfully", txn_id);
                                
                                if should_chain {
                                    // Chain a new transaction
                                    match self.chain_transaction(txn_context, exec_context.clone()) {
                                        Ok(_) => {
                                            info!("New transaction chained successfully after commit");
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
                                    match self.chain_transaction(txn_context, exec_context.clone()) {
                                        Ok(_) => {
                                            info!("New transaction chained successfully after rollback");
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
            PlanNode::Update(_) => {
                debug!("Executing update statement");
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
                            error!("Error during update execution: {}", e);
                            return Err(e);
                        }
                    }
                }

                if has_results {
                    info!("Update executed successfully");
                    Ok(true)
                } else {
                    info!("No rows affected by update");
                    Ok(false)
                }
            }
            PlanNode::Delete(_) => {
                debug!("Executing delete statement");
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
                            error!("Error during delete execution: {}", e);
                            return Err(e);
                        }
                    }
                }

                if has_results {
                    info!("Delete executed successfully");
                    Ok(true)
                } else {
                    info!("No rows affected by delete");
                    Ok(false)
                }
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
                loop {
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
                Ok(tuple_count > 0)
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
        let check_options = Arc::new(CheckOptions::new());

        // Optimize the plan
        let optimized_plan = self
            .optimizer
            .optimize(boxed_plan, check_options)
            .map_err(|e| DBError::OptimizeError(e.to_string()))?;

        // Convert to physical plan and map any String errors to DBError
        optimized_plan
            .to_physical_plan()
            .map_err(|e| DBError::OptimizeError(e))
    }

    /// Clean up after execution
    fn cleanup_after_execution(&self) {
        debug!("Starting post-execution cleanup");
        // Add cleanup logic here if needed
        debug!("Post-execution cleanup complete");
    }

    /// Prepare a SQL statement and validate syntax
    /// Returns empty parameter types for now since we don't support parameters yet
    pub fn prepare_statement(&mut self, sql: &str) -> Result<Vec<TypeId>, DBError> {
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
    pub fn execute_prepared_statement(
        &mut self,
        sql: &str,
        _params: Vec<Value>,
        context: Arc<RwLock<ExecutionContext>>,
        writer: &mut impl ResultWriter,
    ) -> Result<bool, DBError> {
        // For now, just execute as regular SQL since we don't support parameters
        self.execute_sql(sql, context, writer)
    }

    fn execute_insert(
        &self,
        plan: &mut InsertNode,
        txn_ctx: Arc<TransactionContext>,
    ) -> Result<(), DBError> {
        debug!("Executing insert plan");

        let binding = self.catalog.read();
        let table_info = binding
            .get_table(plan.get_table_name())
            .ok_or_else(|| DBError::TableNotFound(plan.get_table_name().to_string()))?;

        // Get the table schema
        let schema = table_info.get_table_schema();

        // Get the values to insert
        let values_to_insert = plan.get_input_values();
        if values_to_insert.is_empty() {
            return Err(DBError::Execution("No values to insert".to_string()));
        }

        // Get table heap and create a transactional wrapper around it
        let table_heap = table_info.get_table_heap();
        let transactional_table_heap =
            TransactionalTableHeap::new(table_heap.clone(), table_info.get_table_oidt());

        // Insert each set of values
        for value_set in values_to_insert {
            transactional_table_heap
                .insert_tuple_from_values(value_set.clone(), &schema, txn_ctx.clone())
                .map_err(|e| DBError::Execution(format!("Insert failed: {}", e)))?;
        }

        debug!("Insert executed successfully");
        Ok(())
    }

    fn commit_transaction(&self, txn_ctx: Arc<TransactionContext>) -> Result<bool, DBError> {
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
        match txn_manager.commit(transaction, self.buffer_pool_manager.clone()) {
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
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::common::logger::initialize_logger;
    use crate::concurrency::transaction::IsolationLevel;
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::recovery::log_manager::LogManager;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::table::table_heap::TableInfo;
    use crate::storage::table::transactional_table_heap::TransactionalTableHeap;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Val::Null;
    use crate::types_db::value::Value;
    use parking_lot::RwLock;
    use std::sync::Arc;
    use tempfile::TempDir;

    struct TestContext {
        engine: ExecutionEngine,
        catalog: Arc<RwLock<Catalog>>,
        exec_ctx: Arc<RwLock<ExecutionContext>>,
        planner: QueryPlanner,
        bpm: Arc<BufferPoolManager>,
        txn_manager: Arc<TransactionManager>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        fn new(name: &str) -> Self {
            initialize_logger();

            const BUFFER_POOL_SIZE: usize = 100;
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
            let disk_manager = Arc::new(FileDiskManager::new(db_path, log_path, 10));
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(7, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler.clone(),
                disk_manager.clone(),
                replacer,
            ));

            let log_manager = Arc::new(RwLock::new(LogManager::new(disk_manager.clone())));

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

            let planner = QueryPlanner::new(catalog.clone());

            Self {
                engine,
                catalog,
                exec_ctx,
                planner,
                bpm,
                txn_manager,
                _temp_dir: temp_dir,
            }
        }

        fn create_test_table(&self, table_name: &str, schema: Schema) -> Result<TableInfo, String> {
            let mut catalog = self.catalog.write();
            Ok(catalog
                .create_table(table_name.to_string(), schema.clone())
                .unwrap())
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

        fn transaction_context(&self) -> Arc<TransactionContext> {
            self.exec_ctx.read().get_transaction_context()
        }

        fn commit_current_transaction(&mut self) -> Result<(), String> {
            let mut writer = TestResultWriter::new();
            self.engine
                .execute_sql("COMMIT", self.exec_ctx.clone(), &mut writer)
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

    #[test]
    // #[ignore]
    fn test_group_by_column_names() {
        let mut ctx = TestContext::new("test_group_by_column_names");

        // Create test table
        let table_schema = Schema::new(vec![
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
            Column::new("salary", TypeId::BigInt),
        ]);

        let table_name = "employees";
        ctx.create_test_table(table_name, table_schema.clone())
            .unwrap();

        // Insert test data
        let test_data = vec![
            vec![Value::new("Alice"), Value::new(25), Value::new(50000i64)],
            vec![Value::new("Alice"), Value::new(25), Value::new(52000i64)],
            vec![Value::new("Bob"), Value::new(30), Value::new(60000i64)],
            vec![Value::new("Bob"), Value::new(30), Value::new(65000i64)],
            vec![Value::new("Charlie"), Value::new(35), Value::new(70000i64)],
        ];
        ctx.insert_tuples(table_name, test_data, table_schema)
            .unwrap();

        // Test different GROUP BY queries
        let test_cases = vec![
            (
                "SELECT name, SUM(age) as total_age, COUNT(*) as emp_count FROM employees GROUP BY name",
                vec!["name", "total_age", "emp_count"],
                3, // Expected number of groups
            ),
            (
                "SELECT name, AVG(salary) as avg_salary FROM employees GROUP BY name",
                vec!["name", "avg_salary"],
                3,
            ),
            (
                "SELECT name, MIN(age) as min_age, MAX(salary) as max_salary FROM employees GROUP BY name",
                vec!["name", "min_age", "max_salary"],
                3,
            ),
        ];

        for (sql, expected_columns, expected_groups) in test_cases {
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            assert!(success, "Query execution failed");

            // Check column names
            let schema = writer.get_schema();
            for (i, expected_name) in expected_columns.iter().enumerate() {
                assert_eq!(
                    schema.get_columns()[i].get_name(),
                    *expected_name,
                    "Column name mismatch for query: {}",
                    sql
                );
            }

            // Check number of result groups
            assert_eq!(
                writer.get_rows().len(),
                expected_groups,
                "Incorrect number of groups for query: {}",
                sql
            );
        }
    }

    #[test]
    fn test_group_by_aggregates() {
        let mut ctx = TestContext::new("test_group_by_aggregates");

        // Create test table
        let table_schema = Schema::new(vec![
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
            Column::new("salary", TypeId::BigInt),
            Column::new("department", TypeId::VarChar),
        ]);

        let table_name = "employees";
        ctx.create_test_table(table_name, table_schema.clone())
            .unwrap();

        // Insert test data
        let test_data = vec![
            vec![
                Value::new("Alice"),
                Value::new(25),
                Value::new(50000i64),
                Value::new("Engineering"),
            ],
            vec![
                Value::new("Alice"),
                Value::new(25),
                Value::new(52000i64),
                Value::new("Engineering"),
            ],
            vec![
                Value::new("Bob"),
                Value::new(30),
                Value::new(60000i64),
                Value::new("Sales"),
            ],
            vec![
                Value::new("Bob"),
                Value::new(30),
                Value::new(65000i64),
                Value::new("Sales"),
            ],
            vec![
                Value::new("Charlie"),
                Value::new(35),
                Value::new(70000i64),
                Value::new("Engineering"),
            ],
            vec![
                Value::new("David"),
                Value::new(40),
                Value::new(80000i64),
                Value::new("Sales"),
            ],
        ];
        ctx.insert_tuples(table_name, test_data, table_schema)
            .unwrap();

        let test_cases = vec![
            // Basic GROUP BY with multiple aggregates
            (
                "SELECT name, COUNT(*) as count, SUM(salary) as total_salary FROM employees GROUP BY name",
                vec!["name", "count", "total_salary"],
                4, // Alice, Bob, Charlie, David
            ),
            // GROUP BY with AVG
            (
                "SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department",
                vec!["department", "avg_salary"],
                2, // Engineering, Sales
            ),
            // GROUP BY with MIN/MAX
            (
                "SELECT department, MIN(age) as min_age, MAX(salary) as max_salary FROM employees GROUP BY department",
                vec!["department", "min_age", "max_salary"],
                2,
            ),
            // Multiple GROUP BY columns
            (
                "SELECT department, age, COUNT(*) as count FROM employees GROUP BY department, age",
                vec!["department", "age", "count"],
                4, // Unique department-age combinations: Engineering-25, Engineering-35, Sales-30, Sales-40
            ),
            // GROUP BY with HAVING clause
            // (
            //     "SELECT department, COUNT(*) as emp_count FROM employees GROUP BY department HAVING COUNT(*) > 2",
            //     vec!["department", "emp_count"],
            //     2,
            // ),
        ];

        for (sql, expected_columns, expected_groups) in test_cases {
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();

            assert!(success, "Query execution failed for: {}", sql);

            // Verify column names
            let schema = writer.get_schema();
            assert_eq!(
                schema.get_columns().len(),
                expected_columns.len(),
                "Incorrect number of columns for query: {}",
                sql
            );

            for (i, expected_name) in expected_columns.iter().enumerate() {
                assert_eq!(
                    schema.get_columns()[i].get_name(),
                    *expected_name,
                    "Column name mismatch for query: {}",
                    sql
                );
            }

            // Verify number of result rows
            assert_eq!(
                writer.get_rows().len(),
                expected_groups,
                "Incorrect number of groups for query: {}",
                sql
            );
        }
    }

    #[test]
    fn test_simple_queries() {
        // Create a simpler test to avoid stack overflow
        let mut ctx = TestContext::new("test_simple_queries");

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

    #[test]
    // #[ignore = "Causes stack overflow in the logical plan to physical plan conversion"]
    fn test_order_by() {
        let mut ctx = TestContext::new("test_order_by");

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
            .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
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

    #[test]
    fn test_where_clause() {
        let mut ctx = TestContext::new("test_where_clause");

        // Create test table
        let table_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
            Column::new("active", TypeId::Boolean),
        ]);

        let table_name = "users";
        ctx.create_test_table(table_name, table_schema.clone())
            .unwrap();

        // Insert test data
        let test_data = vec![
            vec![
                Value::new(1),
                Value::new("Alice"),
                Value::new(25),
                Value::new(true),
            ],
            vec![
                Value::new(2),
                Value::new("Bob"),
                Value::new(30),
                Value::new(true),
            ],
            vec![
                Value::new(3),
                Value::new("Charlie"),
                Value::new(35),
                Value::new(false),
            ],
            vec![
                Value::new(4),
                Value::new("David"),
                Value::new(40),
                Value::new(true),
            ],
        ];
        ctx.insert_tuples(table_name, test_data, table_schema)
            .unwrap();

        let test_cases = vec![
            // Simple equality condition
            ("SELECT name FROM users WHERE id = 2", vec!["Bob"]),
            // Comparison operator
            (
                "SELECT name FROM users WHERE age > 30",
                vec!["Charlie", "David"],
            ),
            // Boolean condition
            (
                "SELECT name FROM users WHERE active = true",
                vec!["Alice", "Bob", "David"],
            ),
            // Multiple conditions with AND
            (
                "SELECT name FROM users WHERE age > 25 AND active = true",
                vec!["Bob", "David"],
            ),
            // Multiple conditions with OR
            (
                "SELECT name FROM users WHERE id = 1 OR id = 3",
                vec!["Alice", "Charlie"],
            ),
        ];

        for (sql, expected_names) in test_cases {
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();

            assert!(success, "Query execution failed for: {}", sql);

            let actual_names: Vec<String> = writer
                .get_rows()
                .iter()
                .map(|row| row[0].to_string())
                .collect();

            assert_eq!(
                actual_names.len(),
                expected_names.len(),
                "Incorrect number of results for query: {}",
                sql
            );

            for name in expected_names {
                assert!(
                    actual_names.contains(&name.to_string()),
                    "Expected name '{}' not found in results for query: {}",
                    name,
                    sql
                );
            }
        }
    }

    #[test]
    fn test_join_operations() {
        let mut ctx = TestContext::new("test_join_operations");

        // Create users table
        let users_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("dept_id", TypeId::Integer),
        ]);
        ctx.create_test_table("users", users_schema.clone())
            .unwrap();

        // Create departments table
        let depts_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        ctx.create_test_table("departments", depts_schema.clone())
            .unwrap();

        // Insert test data for users
        let users_data = vec![
            vec![Value::new(1), Value::new("Alice"), Value::new(1)],
            vec![Value::new(2), Value::new("Bob"), Value::new(2)],
            vec![Value::new(3), Value::new("Charlie"), Value::new(1)],
            vec![Value::new(4), Value::new("David"), Value::new(3)],
            vec![Value::new(5), Value::new("Eve"), Value::new(Null)],
        ];
        ctx.insert_tuples("users", users_data, users_schema)
            .unwrap();

        // Insert test data for departments
        let depts_data = vec![
            vec![Value::new(1), Value::new("Engineering")],
            vec![Value::new(2), Value::new("Sales")],
            vec![Value::new(3), Value::new("Marketing")],
        ];
        ctx.insert_tuples("departments", depts_data, depts_schema)
            .unwrap();

        let test_cases = vec![
            // Inner join
            (
                "SELECT u.name, d.name FROM users u JOIN departments d ON u.dept_id = d.id",
                4, // Alice, Bob, Charlie, David
            ),
            // Left join
            (
                "SELECT u.name, d.name FROM users u LEFT OUTER JOIN departments d ON u.dept_id = d.id",
                5, // All users including Eve with NULL department
            ),
            (
                "SELECT u.name, d.name FROM users u LEFT JOIN departments d ON u.dept_id = d.id",
                5, // All users including Eve with NULL department
            ),
            // Join with additional conditions
            (
                "SELECT u.name, d.name FROM users u JOIN departments d ON u.dept_id = d.id WHERE d.name = 'Engineering'",
                2, // Alice, Charlie
            ),
        ];

        for (sql, expected_rows) in test_cases {
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();

            assert!(success, "Query execution failed for: {}", sql);

            assert_eq!(
                writer.get_rows().len(),
                expected_rows,
                "Incorrect number of rows for query: {}",
                sql
            );
        }
    }

    #[test]
    fn test_insert_operations() {
        let mut ctx = TestContext::new("test_insert_operations");

        // Create test table
        let table_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ]);

        let table_name = "users";
        ctx.create_test_table(table_name, table_schema.clone())
            .unwrap();

        // Test INSERT with VALUES
        let insert_sql = "INSERT INTO users VALUES (1, 'Alice', 25)";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(insert_sql, ctx.exec_ctx.clone(), &mut writer)
            .unwrap();
        assert!(success, "Insert operation failed");

        // Verify the insert worked
        let select_sql = "SELECT * FROM users";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
            .unwrap();
        assert!(success, "Select operation failed");
        assert_eq!(writer.get_rows().len(), 1, "Expected 1 row after insert");

        // Test INSERT with multiple rows
        let multi_insert_sql = "INSERT INTO users VALUES (2, 'Bob', 30), (3, 'Charlie', 35)";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(multi_insert_sql, ctx.exec_ctx.clone(), &mut writer)
            .unwrap();
        assert!(success, "Multi-row insert operation failed");

        // Verify multiple inserts worked
        let select_sql = "SELECT * FROM users";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
            .unwrap();
        assert!(success, "Select operation failed");
        assert_eq!(
            writer.get_rows().len(),
            3,
            "Expected 3 rows after multiple inserts"
        );

        // Test INSERT with SELECT
        let create_temp_sql = "CREATE TABLE temp_users (id INTEGER, name VARCHAR, age INTEGER)";
        let mut writer = TestResultWriter::new();
        ctx.engine
            .execute_sql(create_temp_sql, ctx.exec_ctx.clone(), &mut writer)
            .unwrap();

        let insert_temp_sql = "INSERT INTO temp_users VALUES (4, 'David', 40), (5, 'Eve', 45)";
        let mut writer = TestResultWriter::new();
        ctx.engine
            .execute_sql(insert_temp_sql, ctx.exec_ctx.clone(), &mut writer)
            .unwrap();

        let insert_select_sql = "INSERT INTO users SELECT * FROM temp_users";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(insert_select_sql, ctx.exec_ctx.clone(), &mut writer)
            .unwrap();
        assert!(success, "Insert with SELECT operation failed");

        // Verify INSERT with SELECT worked
        let select_sql = "SELECT * FROM users";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
            .unwrap();
        assert!(success, "Select operation failed");
        assert_eq!(
            writer.get_rows().len(),
            5,
            "Expected 5 rows after INSERT with SELECT"
        );
    }

    #[test]
    fn test_create_table_operations() {
        let mut ctx = TestContext::new("test_create_table_operations");

        // Test CREATE TABLE
        let create_sql =
            "CREATE TABLE test_table (id INTEGER, name VARCHAR(50), age INTEGER, active BOOLEAN)";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(create_sql, ctx.exec_ctx.clone(), &mut writer)
            .unwrap();
        assert!(success, "Create table operation failed");

        // Verify table was created by inserting and selecting
        let insert_sql = "INSERT INTO test_table VALUES (1, 'Alice', 25, true)";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(insert_sql, ctx.exec_ctx.clone(), &mut writer)
            .unwrap();
        assert!(success, "Insert operation failed");

        let select_sql = "SELECT * FROM test_table";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
            .unwrap();
        assert!(success, "Select operation failed");
        assert_eq!(
            writer.get_rows().len(),
            1,
            "Expected 1 row in newly created table"
        );
    }

    #[test]
    fn test_transaction_handling() {
        let mut ctx = TestContext::new("test_transaction_handling");

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
        ctx.commit_current_transaction().unwrap();

        // Start transaction
        let begin_sql = "BEGIN";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(begin_sql, ctx.exec_ctx.clone(), &mut writer)
            .unwrap();

        assert!(success, "Begin transaction failed");

        // Update Alice's balance
        let update_sql = "UPDATE accounts SET balance = balance - 200 WHERE id = 1";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer)
            .unwrap();
        assert!(success, "Update operation failed");

        // Update Bob's balance
        let update_sql = "UPDATE accounts SET balance = balance + 200 WHERE id = 2";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer)
            .unwrap();
        assert!(success, "Update operation failed");

        // Commit transaction
        let commit_sql = "COMMIT";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(commit_sql, ctx.exec_ctx.clone(), &mut writer)
            .unwrap();
        assert!(success, "Commit transaction failed");

        // Verify changes were committed
        let select_sql = "SELECT balance FROM accounts WHERE id = 1";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
            .unwrap();
        assert_eq!(
            writer.get_rows()[0][0].to_string(),
            "800",
            "Expected Alice's balance to be 800"
        );

        let select_sql = "SELECT balance FROM accounts WHERE id = 2";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
            .unwrap();
        assert_eq!(
            writer.get_rows()[0][0].to_string(),
            "700",
            "Expected Bob's balance to be 700"
        );

        // Test rollback
        let begin_sql = "BEGIN";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(begin_sql, ctx.exec_ctx.clone(), &mut writer)
            .unwrap();
        let rollback_sql = "ROLLBACK";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(rollback_sql, ctx.exec_ctx.clone(), &mut writer)
            .unwrap();

        // Verify changes were rolled back
        let select_sql = "SELECT balance FROM accounts WHERE id = 1";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
            .unwrap();
        assert_eq!(
            writer.get_rows()[0][0].to_string(),
            "800",
            "Expected Alice's balance to still be 800 after rollback"
        );
    }

    #[test]
    fn test_case_when_simple() {
        let mut ctx = TestContext::new("test_case_when_simple");

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
        let sql =
            "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END FROM t1 ORDER BY 1";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
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

    #[test]
    fn test_case_when_with_subquery() {
        let mut ctx = TestContext::new("test_case_when_with_subquery");

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
        let sql =
            "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END FROM t1 ORDER BY 1";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
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

    #[test]
    fn test_decimal_precision_scale_display() {
        let mut ctx = TestContext::new("test_decimal_precision_scale_display");

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
            .unwrap();
        assert!(success, "Decimal CASE expression failed");

        let case_rows = writer.get_rows();
        println!("\nDecimal CASE expression results:");
        for row in case_rows {
            println!("ID: {}, Category: {}, Price: {}", row[0], row[1], row[2]);
        }
    }

    #[test]
    fn test_float_precision_display() {
        let mut ctx = TestContext::new("test_float_precision_display");

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
                Value::new(3.14159f32), // Pi approximation
                Value::new(0.618f32),   // Golden ratio approximation
                Value::new(1.23e-4f32), // Scientific notation
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
        let select_sql = "SELECT id, measurement, ratio, scientific FROM measurements ORDER BY id";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
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
        let agg_sql =
            "SELECT AVG(measurement) as avg_measurement, SUM(ratio) as sum_ratio FROM measurements";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(agg_sql, ctx.exec_ctx.clone(), &mut writer)
            .unwrap();
        assert!(success, "Float aggregation failed");

        let agg_rows = writer.get_rows();
        println!("\nFloat aggregation results:");
        for row in agg_rows {
            println!("Avg Measurement: {}, Sum Ratio: {}", row[0], row[1]);
        }
    }

    #[test]
    fn test_mixed_numeric_precision_display() {
        let mut ctx = TestContext::new("test_mixed_numeric_precision_display");

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
        let select_sql =
            "SELECT id, int_val, bigint_val, decimal_val, float_val FROM mixed_numbers ORDER BY id";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
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

    #[test]
    fn test_decimal_edge_cases_display() {
        let mut ctx = TestContext::new("test_decimal_edge_cases_display");

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
                Value::new(3.141592653589793),
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

    #[test]
    fn test_decimal_column_aware_formatting() {
        let mut ctx = TestContext::new("test_decimal_column_aware_formatting");

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
        let select_sql = "SELECT id, price, rate, percentage FROM formatted_decimals ORDER BY id";
        let mut writer = TestResultWriter::new();
        let success = ctx
            .engine
            .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
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

    mod update_tests {
        use super::*;

        #[test]
        fn test_update_basic_operations() {
            let mut ctx = TestContext::new("test_update_basic_operations");

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("age", TypeId::Integer),
                Column::new("salary", TypeId::BigInt),
                Column::new("active", TypeId::Boolean),
            ]);

            let table_name = "employees";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![
                    Value::new(1),
                    Value::new("Alice"),
                    Value::new(25),
                    Value::new(50000i64),
                    Value::new(true),
                ],
                vec![
                    Value::new(2),
                    Value::new("Bob"),
                    Value::new(30),
                    Value::new(60000i64),
                    Value::new(true),
                ],
                vec![
                    Value::new(3),
                    Value::new("Charlie"),
                    Value::new(35),
                    Value::new(70000i64),
                    Value::new(false),
                ],
                vec![
                    Value::new(4),
                    Value::new("David"),
                    Value::new(40),
                    Value::new(80000i64),
                    Value::new(true),
                ],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test 1: Basic UPDATE with single column
            let update_sql = "UPDATE employees SET age = 26 WHERE id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            assert!(success, "Basic update operation failed");

            // Verify the update
            let select_sql = "SELECT age FROM employees WHERE id = 1";
            let mut writer = TestResultWriter::new();
            ctx.engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            assert_eq!(
                writer.get_rows()[0][0].to_string(),
                "26",
                "Age should be updated to 26"
            );

            // Test 2: UPDATE multiple columns
            let update_sql =
                "UPDATE employees SET name = 'Alice Smith', salary = 55000 WHERE id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            assert!(success, "Multi-column update operation failed");

            // Verify the multi-column update
            let select_sql = "SELECT name, salary FROM employees WHERE id = 1";
            let mut writer = TestResultWriter::new();
            ctx.engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            let row = &writer.get_rows()[0];
            assert_eq!(row[0].to_string(), "Alice Smith", "Name should be updated");
            assert_eq!(row[1].to_string(), "55000", "Salary should be updated");

            // Test 3: UPDATE with boolean value
            let update_sql = "UPDATE employees SET active = false WHERE id = 2";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            assert!(success, "Boolean update operation failed");

            // Verify boolean update
            let select_sql = "SELECT active FROM employees WHERE id = 2";
            let mut writer = TestResultWriter::new();
            ctx.engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            assert_eq!(
                writer.get_rows()[0][0].to_string(),
                "false",
                "Active status should be updated to false"
            );
        }

        #[test]
        fn test_update_with_conditions() {
            let mut ctx = TestContext::new("test_update_with_conditions");

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("age", TypeId::Integer),
                Column::new("salary", TypeId::BigInt),
                Column::new("department", TypeId::VarChar),
            ]);

            let table_name = "employees";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![
                    Value::new(1),
                    Value::new("Alice"),
                    Value::new(25),
                    Value::new(50000i64),
                    Value::new("Engineering"),
                ],
                vec![
                    Value::new(2),
                    Value::new("Bob"),
                    Value::new(30),
                    Value::new(60000i64),
                    Value::new("Sales"),
                ],
                vec![
                    Value::new(3),
                    Value::new("Charlie"),
                    Value::new(35),
                    Value::new(70000i64),
                    Value::new("Engineering"),
                ],
                vec![
                    Value::new(4),
                    Value::new("David"),
                    Value::new(40),
                    Value::new(80000i64),
                    Value::new("Marketing"),
                ],
                vec![
                    Value::new(5),
                    Value::new("Eve"),
                    Value::new(28),
                    Value::new(55000i64),
                    Value::new("Sales"),
                ],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test 1: UPDATE with range condition
            let update_sql = "UPDATE employees SET salary = salary + 5000 WHERE age > 30";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            assert!(success, "Range condition update failed");

            // Verify employees over 30 got salary increase
            let select_sql = "SELECT name, salary FROM employees WHERE age > 30 ORDER BY name";
            let mut writer = TestResultWriter::new();
            ctx.engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            let rows = writer.get_rows();

            // Charlie (age 35) and David (age 40) should have increased salaries
            assert_eq!(rows.len(), 2, "Should have 2 employees over 30");

            // Find Charlie and David in results
            for row in rows {
                let name = row[0].to_string();
                let salary_str = row[1].to_string();

                // Handle both integer and decimal formats
                let salary = if salary_str.contains('.') {
                    salary_str.parse::<f64>().unwrap() as i64
                } else {
                    salary_str.parse::<i64>().unwrap()
                };

                if name == "Charlie" {
                    assert_eq!(salary, 75000, "Charlie's salary should be 75000");
                } else if name == "David" {
                    assert_eq!(salary, 85000, "David's salary should be 85000");
                }
            }

            // Test 2: UPDATE with string condition
            let update_sql =
                "UPDATE employees SET salary = salary * 1.1 WHERE department = 'Engineering'";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            assert!(success, "String condition update failed");

            // Verify Engineering employees got 10% raise
            let select_sql =
                "SELECT name, salary FROM employees WHERE department = 'Engineering' ORDER BY name";
            let mut writer = TestResultWriter::new();
            ctx.engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            let rows = writer.get_rows();

            for row in rows {
                let name = row[0].to_string();
                let salary_str = row[1].to_string();

                // Handle both integer and decimal formats
                let salary = if salary_str.contains('.') {
                    salary_str.parse::<f64>().unwrap() as i64
                } else {
                    salary_str.parse::<i64>().unwrap()
                };

                if name == "Alice" {
                    assert_eq!(
                        salary, 55000,
                        "Alice's salary should be 55000 (50000 * 1.1)"
                    );
                } else if name == "Charlie" {
                    assert_eq!(
                        salary, 82500,
                        "Charlie's salary should be 82500 (75000 * 1.1)"
                    );
                }
            }

            // Test 3: UPDATE with multiple conditions (AND)
            let update_sql =
                "UPDATE employees SET age = age + 1 WHERE department = 'Sales' AND age < 35";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            assert!(success, "Multiple condition update failed");

            // Verify only Bob and Eve (Sales, age < 35) got age increment
            let select_sql =
                "SELECT name, age FROM employees WHERE department = 'Sales' ORDER BY name";
            let mut writer = TestResultWriter::new();
            ctx.engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            let rows = writer.get_rows();

            for row in rows {
                let name = row[0].to_string();
                let age = row[1].to_string().parse::<i32>().unwrap();

                if name == "Bob" {
                    assert_eq!(age, 31, "Bob's age should be 31");
                } else if name == "Eve" {
                    assert_eq!(age, 29, "Eve's age should be 29");
                }
            }
        }

        #[test]
        fn test_update_with_expressions() {
            let mut ctx = TestContext::new("test_update_with_expressions");

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("base_salary", TypeId::BigInt),
                Column::new("bonus", TypeId::BigInt),
                Column::new("years_experience", TypeId::Integer),
            ]);

            let table_name = "employees";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![
                    Value::new(1),
                    Value::new("Alice"),
                    Value::new(50000i64),
                    Value::new(5000i64),
                    Value::new(3),
                ],
                vec![
                    Value::new(2),
                    Value::new("Bob"),
                    Value::new(60000i64),
                    Value::new(8000i64),
                    Value::new(5),
                ],
                vec![
                    Value::new(3),
                    Value::new("Charlie"),
                    Value::new(70000i64),
                    Value::new(10000i64),
                    Value::new(7),
                ],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test 1: UPDATE with arithmetic expression
            let update_sql =
                "UPDATE employees SET base_salary = base_salary + (years_experience * 1000)";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            assert!(success, "Arithmetic expression update failed");

            // Verify the arithmetic updates
            let select_sql = "SELECT id, name, base_salary FROM employees ORDER BY id";
            let mut writer = TestResultWriter::new();
            ctx.engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            let rows = writer.get_rows();

            // Alice: 50000 + (3 * 1000) = 53000
            assert_eq!(
                rows[0][2].to_string(),
                "53000",
                "Alice's salary should be 53000"
            );
            // Bob: 60000 + (5 * 1000) = 65000
            assert_eq!(
                rows[1][2].to_string(),
                "65000",
                "Bob's salary should be 65000"
            );
            // Charlie: 70000 + (7 * 1000) = 77000
            assert_eq!(
                rows[2][2].to_string(),
                "77000",
                "Charlie's salary should be 77000"
            );

            // Test 2: UPDATE with column reference in expression
            let update_sql =
                "UPDATE employees SET bonus = base_salary * 0.1 WHERE years_experience >= 5";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            assert!(success, "Column reference expression update failed");

            // Verify bonus updates for employees with 5+ years experience
            let select_sql =
                "SELECT name, bonus FROM employees WHERE years_experience >= 5 ORDER BY name";
            let mut writer = TestResultWriter::new();
            ctx.engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            let rows = writer.get_rows();

            for row in rows {
                let name = row[0].to_string();
                let bonus_str = row[1].to_string();

                // Handle both integer and decimal formats
                let bonus = if bonus_str.contains('.') {
                    bonus_str.parse::<f64>().unwrap() as i64
                } else {
                    bonus_str.parse::<i64>().unwrap()
                };

                if name == "Bob" {
                    assert_eq!(bonus, 6500, "Bob's bonus should be 6500 (10% of 65000)");
                } else if name == "Charlie" {
                    assert_eq!(bonus, 7700, "Charlie's bonus should be 7700 (10% of 77000)");
                }
            }
        }

        #[test]
        fn test_update_no_rows_affected() {
            let mut ctx = TestContext::new("test_update_no_rows_affected");

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("age", TypeId::Integer),
            ]);

            let table_name = "employees";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![Value::new(1), Value::new("Alice"), Value::new(25)],
                vec![Value::new(2), Value::new("Bob"), Value::new(30)],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test UPDATE with condition that matches no rows
            let update_sql = "UPDATE employees SET age = 35 WHERE id = 999";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();

            // Should return false since no rows were affected
            assert!(!success, "UPDATE with no matching rows should return false");

            // Verify no data was changed
            let select_sql = "SELECT id, age FROM employees ORDER BY id";
            let mut writer = TestResultWriter::new();
            ctx.engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            let rows = writer.get_rows();

            assert_eq!(rows[0][1].to_string(), "25", "Alice's age should remain 25");
            assert_eq!(rows[1][1].to_string(), "30", "Bob's age should remain 30");
        }

        #[test]
        fn test_update_all_rows() {
            let mut ctx = TestContext::new("test_update_all_rows");

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("status", TypeId::VarChar),
            ]);

            let table_name = "employees";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![Value::new(1), Value::new("Alice"), Value::new("active")],
                vec![Value::new(2), Value::new("Bob"), Value::new("inactive")],
                vec![Value::new(3), Value::new("Charlie"), Value::new("pending")],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Test UPDATE without WHERE clause (affects all rows)
            let update_sql = "UPDATE employees SET status = 'updated'";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            assert!(success, "UPDATE all rows should succeed");

            // Commit the transaction to make changes visible
            ctx.commit_current_transaction().unwrap();

            // Verify all rows were updated
            let select_sql = "SELECT status FROM employees";
            let mut writer = TestResultWriter::new();
            ctx.engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            let rows = writer.get_rows();

            assert_eq!(rows.len(), 3, "Should have 3 rows");
            for row in rows {
                assert_eq!(
                    row[0].to_string(),
                    "updated",
                    "All statuses should be 'updated'"
                );
            }
        }

        #[test]
        fn test_update_in_transaction() {
            let mut ctx = TestContext::new("test_update_in_transaction");

            // Create test table
            let table_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("balance", TypeId::BigInt),
            ]);

            let table_name = "accounts";
            ctx.create_test_table(table_name, table_schema.clone())
                .unwrap();

            // Insert test data
            let test_data = vec![
                vec![Value::new(1), Value::new("Alice"), Value::new(1000i64)],
                vec![Value::new(2), Value::new("Bob"), Value::new(500i64)],
            ];
            ctx.insert_tuples(table_name, test_data, table_schema)
                .unwrap();

            // Commit initial data
            ctx.commit_current_transaction().unwrap();

            // Test 1: UPDATE in transaction with commit
            let begin_sql = "BEGIN";
            let mut writer = TestResultWriter::new();
            ctx.engine
                .execute_sql(begin_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();

            // Transfer money from Alice to Bob
            let update_sql1 = "UPDATE accounts SET balance = balance - 200 WHERE id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(update_sql1, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            assert!(success, "First update in transaction failed");

            let update_sql2 = "UPDATE accounts SET balance = balance + 200 WHERE id = 2";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(update_sql2, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            assert!(success, "Second update in transaction failed");

            // Commit transaction
            let commit_sql = "COMMIT";
            let mut writer = TestResultWriter::new();
            ctx.engine
                .execute_sql(commit_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();

            // Verify changes were committed
            let select_sql = "SELECT balance FROM accounts WHERE id = 1";
            let mut writer = TestResultWriter::new();
            ctx.engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
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
                .unwrap();
            assert_eq!(
                writer.get_rows()[0][0].to_string(),
                "700",
                "Expected Bob's balance to be 700"
            );

            // Test 2: UPDATE in transaction with rollback
            let begin_sql = "BEGIN";
            let mut writer = TestResultWriter::new();
            ctx.engine
                .execute_sql(begin_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();

            // Try to transfer more money
            let update_sql = "UPDATE accounts SET balance = balance - 500 WHERE id = 1";
            let mut writer = TestResultWriter::new();
            let success = ctx
                .engine
                .execute_sql(update_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            assert!(success, "Update in transaction failed");

            // Rollback transaction
            let rollback_sql = "ROLLBACK";
            let mut writer = TestResultWriter::new();
            ctx.engine
                .execute_sql(rollback_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();

            // Verify changes were rolled back
            let select_sql = "SELECT balance FROM accounts WHERE id = 1";
            let mut writer = TestResultWriter::new();
            ctx.engine
                .execute_sql(select_sql, ctx.exec_ctx.clone(), &mut writer)
                .unwrap();
            assert_eq!(
                writer.get_rows()[0][0].to_string(),
                "800",
                "Expected Alice's balance to still be 800 after rollback"
            );
        }
    }
}
