use crate::common::db_instance::{DBConfig, DBInstance};
use crate::common::exception::DBError;
use crate::common::result_writer::CliResultWriter;
use crate::concurrency::transaction::IsolationLevel;
use crate::sql::execution::transaction_context::TransactionContext;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use std::sync::Arc;

pub struct CLI {
    db: DBInstance,
    rl: DefaultEditor,
    current_transaction: Option<Arc<TransactionContext>>,
}

impl CLI {
    pub async fn new() -> Result<Self, DBError> {
        let config = DBConfig::default();
        let db = DBInstance::new(config).await?;
        let rl = DefaultEditor::new().map_err(|e| DBError::Client(e.to_string()))?;

        Ok(Self {
            db,
            rl,
            current_transaction: None,
        })
    }

    pub async fn run(&mut self) -> Result<(), DBError> {
        println!("Welcome to TKDB. Commands end with ;");
        println!("Type 'help;' for help.");

        loop {
            let mut buffer = String::new();

            loop {
                match self.rl.readline(if buffer.is_empty() {
                    "tkdb> "
                } else {
                    "    -> "
                }) {
                    Ok(line) => {
                        let _ = self.rl.add_history_entry(line.as_str());
                        buffer.push_str(&line);

                        if line.trim().ends_with(';') {
                            break;
                        }
                    }
                    Err(ReadlineError::Interrupted) => {
                        println!("^C");
                        buffer.clear();
                        break;
                    }
                    Err(ReadlineError::Eof) => {
                        println!("Bye!");
                        return Ok(());
                    }
                    Err(err) => {
                        println!("Error: {:?}", err);
                        return Ok(());
                    }
                }
            }

            let command = buffer.trim();
            if command.is_empty() {
                continue;
            }

            // Remove trailing semicolon
            let command = command.trim_end_matches(';');

            match command.to_lowercase().as_str() {
                "exit" | "quit" => {
                    self.cleanup_transaction();
                    println!("Bye!");
                    break;
                }
                "help" => {
                    self.print_help();
                }
                "begin" => {
                    self.begin_transaction(IsolationLevel::RepeatableRead)?;
                }
                "commit" => {
                    self.commit_transaction().await?;
                }
                "rollback" => {
                    self.rollback_transaction()?;
                }
                "show tables" => {
                    let mut writer = CliResultWriter::new();
                    self.db.display_tables(&mut writer)?;
                }
                _ => {
                    self.execute_command(command).await?;
                }
            }
        }

        Ok(())
    }

    fn begin_transaction(&mut self, isolation_level: IsolationLevel) -> Result<(), DBError> {
        if self.current_transaction.is_some() {
            println!("Transaction already in progress. Commit or rollback first.");
            return Ok(());
        }

        let txn_ctx = self.db.begin_transaction(isolation_level);
        println!(
            "Transaction started with ID: {}",
            txn_ctx.get_transaction_id()
        );
        self.current_transaction = Some(txn_ctx);
        Ok(())
    }

    async fn commit_transaction(&mut self) -> Result<(), DBError> {
        if let Some(txn_ctx) = self.current_transaction.take() {
            match self
                .db
                .commit_transaction(txn_ctx.get_transaction_id())
                .await
            {
                Ok(_) => {
                    println!("Transaction committed successfully.");
                }
                Err(e) => {
                    println!("Error during commit: {}. Rolling back changes.", e);
                    // Attempt rollback on error
                    self.db.get_transaction_factory().abort_transaction(txn_ctx);
                }
            }
        } else {
            println!("No active transaction to commit.");
        }
        Ok(())
    }

    fn rollback_transaction(&mut self) -> Result<(), DBError> {
        if let Some(txn_ctx) = self.current_transaction.take() {
            self.db
                .get_transaction_factory()
                .abort_transaction(txn_ctx.clone());
            println!(
                "Transaction {} rolled back successfully.",
                txn_ctx.get_transaction_id()
            );
        } else {
            println!("No active transaction to roll back.");
        }
        Ok(())
    }

    fn cleanup_transaction(&mut self) {
        if let Some(txn_ctx) = self.current_transaction.take() {
            self.db
                .get_transaction_factory()
                .abort_transaction(txn_ctx.clone());
            println!(
                "Active transaction {} rolled back during cleanup.",
                txn_ctx.get_transaction_id()
            );
        }
    }

    async fn execute_command(&mut self, sql: &str) -> Result<(), DBError> {
        let mut writer = CliResultWriter::new();

        match &self.current_transaction {
            Some(txn_ctx) => {
                // Execute within existing transaction
                match self
                    .db
                    .execute_transaction(sql, txn_ctx.clone(), &mut writer)
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        println!("Error executing query: {:?}", e);
                        Ok(())
                    }
                }
            }
            None => {
                // Auto-commit mode
                match self
                    .db
                    .execute_sql(sql, IsolationLevel::ReadCommitted, &mut writer)
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        println!("Error executing query: {:?}", e);
                        Ok(())
                    }
                }
            }
        }
    }

    fn print_help(&self) {
        println!("\nCommands:");
        println!("  BEGIN;                    -- Start a new transaction");
        println!("  COMMIT;                   -- Commit current transaction");
        println!("  ROLLBACK;                 -- Rollback current transaction");
        println!("  SHOW TABLES;              -- List all tables");
        println!("  CREATE TABLE ...;         -- Create a new table");
        println!("  INSERT INTO ...;          -- Insert data");
        println!("  SELECT ...;               -- Query data");
        println!("  UPDATE ...;               -- Update data");
        println!("  DELETE FROM ...;          -- Delete data");
        println!("  EXIT;                     -- Exit the program");
        println!("  HELP;                     -- Show this help\n");
    }
}
