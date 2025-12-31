//! # Command Executor Module
//!
//! This module implements the executor for database administrative commands
//! (DDL and DCL statements) that affect database-level state rather than
//! table data.
//!
//! ## Supported Commands
//!
//! | Command                              | Description                          |
//! |--------------------------------------|--------------------------------------|
//! | `CREATE DATABASE <name>`             | Creates a new database               |
//! | `CREATE DATABASE IF NOT EXISTS <n>`  | Creates database if it doesn't exist |
//! | `USE <database>`                     | Switches to specified database       |
//!
//! ## Execution Model
//!
//! Unlike data-manipulation executors that iterate over tuples, `CommandExecutor`
//! executes a single command and returns a result message. It produces exactly
//! one tuple containing a status message, then signals completion.
//!
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │             CommandExecutor                 │
//! │  ┌───────────────────────────────────────┐  │
//! │  │ Command: "CREATE DATABASE mydb"       │  │
//! │  └───────────────────────────────────────┘  │
//! │                    │                        │
//! │                    ▼                        │
//! │  ┌───────────────────────────────────────┐  │
//! │  │ Result: "Database 'mydb' created"     │  │
//! │  └───────────────────────────────────────┘  │
//! └─────────────────────────────────────────────┘
//! ```
//!
//! ## Output Schema
//!
//! All commands produce a single-column result with schema:
//! - Column: `result` (VARCHAR) - Human-readable status message

use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::common::rid::RID;
use crate::sql::execution::execution_context::ExecutionContext;
use crate::sql::execution::executors::abstract_executor::AbstractExecutor;
use crate::storage::table::tuple::Tuple;
use crate::storage::table::tuple::TupleMeta;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use log::warn;
use parking_lot::RwLock;
use std::sync::Arc;

/// Executor for database administrative commands.
///
/// `CommandExecutor` handles DDL/DCL statements that modify database-level
/// state, such as creating databases or switching the active database context.
/// These commands interact directly with the catalog rather than table data.
///
/// # Execution Behavior
///
/// - Produces exactly one result tuple with a status message
/// - Subsequent calls to `next()` return `None`
/// - Commands are executed lazily on first `next()` call
///
/// # Example
///
/// ```ignore
/// let executor = CommandExecutor::new(
///     context,
///     "CREATE DATABASE analytics".to_string(),
/// );
/// executor.init();
///
/// // Returns: ("Database 'analytics' created successfully",)
/// let result = executor.next()?;
/// ```
///
/// # Thread Safety
///
/// The executor acquires write locks on the catalog during command execution.
/// Commands are atomic with respect to catalog state.
pub struct CommandExecutor {
    /// Shared execution context providing access to the catalog.
    context: Arc<RwLock<ExecutionContext>>,
    /// The raw command string to execute.
    command: String,
    /// Output schema (single VARCHAR column for result message).
    schema: Schema,
    /// Flag indicating whether the command has been executed.
    executed: bool,
}

impl CommandExecutor {
    /// Creates a new `CommandExecutor` for the given command string.
    ///
    /// # Arguments
    ///
    /// * `context` - Shared execution context providing catalog access.
    /// * `command` - The raw SQL command string to execute (e.g., "CREATE DATABASE mydb").
    ///
    /// # Returns
    ///
    /// A new `CommandExecutor` ready for initialization and execution.
    ///
    /// # Output Schema
    ///
    /// The executor produces tuples with a single VARCHAR column named "result"
    /// containing human-readable status messages.
    pub fn new(context: Arc<RwLock<ExecutionContext>>, command: String) -> Self {
        // Create a simple schema with a single VARCHAR column for result messages
        let schema = Schema::new(vec![Column::new("result", TypeId::VarChar)]);

        Self {
            context,
            command,
            schema,
            executed: false,
        }
    }

    /// Dispatches and executes the command based on its type.
    ///
    /// Parses the command string to determine the operation type and delegates
    /// to the appropriate handler method. Unknown commands are echoed back
    /// as-is in the result message.
    ///
    /// # Returns
    ///
    /// * `Some((meta, tuple))` - Command executed successfully with result message
    /// * `None` - Command parsing failed (malformed syntax)
    fn execute_command(&mut self) -> Option<(TupleMeta, Tuple)> {
        let command = self.command.trim();

        // Parse the command to determine the operation
        if command.starts_with("CREATE DATABASE") {
            self.execute_create_database()
        } else if command.starts_with("USE") {
            self.execute_use_database()
        } else {
            // For other commands, just return the command as a message
            let values = vec![Value::new(command.to_string())];
            let meta = TupleMeta::new(0); // Using 0 as transaction ID for system commands
            let rid = Default::default();
            let tuple = Tuple::new(&values, &self.schema, rid);
            Some((meta, tuple))
        }
    }

    /// Executes a `CREATE DATABASE` command.
    ///
    /// Parses and executes database creation with optional `IF NOT EXISTS` clause.
    ///
    /// # Syntax
    ///
    /// ```sql
    /// CREATE DATABASE <database_name>
    /// CREATE DATABASE IF NOT EXISTS <database_name>
    /// ```
    ///
    /// # Returns
    ///
    /// * `Some((meta, tuple))` - Result tuple with success/failure message
    /// * `None` - Command syntax was invalid
    ///
    /// # Result Messages
    ///
    /// | Condition                          | Message                                    |
    /// |------------------------------------|--------------------------------------------|
    /// | Created successfully               | "Database '<name>' created successfully"   |
    /// | Already exists (IF NOT EXISTS)     | "Database '<name>' already exists"         |
    /// | Already exists (no IF NOT EXISTS)  | "Failed to create database '<name>': ..."  |
    fn execute_create_database(&self) -> Option<(TupleMeta, Tuple)> {
        // Parse command: "CREATE DATABASE [IF NOT EXISTS] <db_name>"
        let parts: Vec<&str> = self.command.split_whitespace().collect();
        if parts.len() < 3 {
            warn!("Invalid CREATE DATABASE command format");
            return None;
        }

        let (db_name, if_not_exists) =
            if parts.len() >= 5 && parts[2] == "IF" && parts[3] == "NOT" && parts[4] == "EXISTS" {
                if parts.len() < 6 {
                    warn!("Missing database name in CREATE DATABASE IF NOT EXISTS command");
                    return None;
                }
                (parts[5], true)
            } else {
                (parts[2], false)
            };

        // Get catalog from context and create the database
        let context = self.context.read();
        let mut catalog = context.get_catalog().write();

        let success = catalog.create_database(db_name.to_string());

        // Return result message
        let message = if success {
            format!("Database '{}' created successfully", db_name)
        } else if if_not_exists {
            format!("Database '{}' already exists", db_name)
        } else {
            format!("Failed to create database '{}': already exists", db_name)
        };

        let values = vec![Value::new(message)];
        let meta = TupleMeta::new(0); // Using 0 as transaction ID for system commands
        let rid = Default::default();
        let tuple = Tuple::new(&values, &self.schema, rid);

        Some((meta, tuple))
    }

    /// Executes a `USE` command to switch the active database.
    ///
    /// Changes the current database context in the catalog, affecting
    /// subsequent queries that reference unqualified table names.
    ///
    /// # Syntax
    ///
    /// ```sql
    /// USE <database_name>
    /// ```
    ///
    /// # Returns
    ///
    /// * `Some((meta, tuple))` - Result tuple with success/failure message
    /// * `None` - Command syntax was invalid
    ///
    /// # Result Messages
    ///
    /// | Condition            | Message                              |
    /// |----------------------|--------------------------------------|
    /// | Switch successful    | "Switched to database '<name>'"      |
    /// | Database not found   | "Database '<name>' does not exist"   |
    fn execute_use_database(&self) -> Option<(TupleMeta, Tuple)> {
        // Parse command: "USE <db_name>"
        let parts: Vec<&str> = self.command.split_whitespace().collect();
        if parts.len() < 2 {
            warn!("Invalid USE command format");
            return None;
        }

        let db_name = parts[1];

        // Get catalog from context and switch database
        let context = self.context.read();
        let mut catalog = context.get_catalog().write();

        let success = catalog.use_database(db_name);

        // Return result message
        let message = if success {
            format!("Switched to database '{}'", db_name)
        } else {
            format!("Database '{}' does not exist", db_name)
        };

        let values = vec![Value::new(message)];
        let meta = TupleMeta::new(0); // Using 0 as transaction ID for system commands
        let rid = Default::default();
        let tuple = Tuple::new(&values, &self.schema, rid);

        Some((meta, tuple))
    }
}

impl AbstractExecutor for CommandExecutor {
    /// Initializes the command executor.
    ///
    /// No initialization is required for command execution. This method
    /// exists to satisfy the `AbstractExecutor` trait contract.
    fn init(&mut self) {
        // No initialization needed
    }

    /// Executes the command and returns the result tuple.
    ///
    /// On first call, executes the command and returns a tuple containing
    /// the result message. Subsequent calls return `None` since commands
    /// produce exactly one result.
    ///
    /// # Returns
    ///
    /// * `Ok(Some((tuple, rid)))` - First call returns the result message tuple
    /// * `Ok(None)` - Subsequent calls indicate no more results
    /// * `Err(DBError)` - Never returned (errors handled via result messages)
    ///
    /// # Execution Timing
    ///
    /// The command is executed lazily on the first `next()` call, not during
    /// construction or `init()`. This allows the executor to be created and
    /// passed around before the command takes effect.
    fn next(&mut self) -> Result<Option<(Arc<Tuple>, RID)>, DBError> {
        if self.executed {
            return Ok(None);
        }

        self.executed = true;

        // Execute the command and get the result tuple
        match self.execute_command() {
            Some((_meta, tuple)) => {
                let rid = tuple.get_rid();
                Ok(Some((Arc::new(tuple), rid)))
            },
            None => Ok(None),
        }
    }

    /// Returns the output schema for command results.
    ///
    /// All commands produce a single-column schema:
    /// - `result` (VARCHAR): Human-readable status message
    fn get_output_schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns the shared execution context.
    ///
    /// Provides access to the catalog for database operations.
    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.context.clone()
    }
}
