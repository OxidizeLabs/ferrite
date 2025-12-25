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

/// Executor for handling database command operations like CREATE DATABASE and USE
pub struct CommandExecutor {
    context: Arc<RwLock<ExecutionContext>>,
    command: String,
    schema: Schema,
    executed: bool,
}

impl CommandExecutor {
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
    fn init(&mut self) {
        // No initialization needed
    }

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

    fn get_output_schema(&self) -> &Schema {
        &self.schema
    }

    fn get_executor_context(&self) -> Arc<RwLock<ExecutionContext>> {
        self.context.clone()
    }
}
