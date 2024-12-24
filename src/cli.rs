use crate::common::db_instance::{DBConfig, DBInstance, ResultWriter};
use crate::common::logger::initialize_logger;
use crate::execution::result_writer::CliResultWriter;
use clap::Parser;
use colored::*;
use log::{debug, info, warn};
use parking_lot::Mutex;
use rustyline::DefaultEditor;
use std::error::Error;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    db_name: Option<String>,
    #[arg(short, long)]
    buffer_size: Option<usize>,
    #[arg(short, long)]
    frames: Option<usize>,
    #[arg(short, long)]
    k_value: Option<usize>,
}

struct DBCommandExecutor {
    instance: Arc<Mutex<DBInstance>>,
}

impl DBCommandExecutor {
    fn new(instance: Arc<Mutex<DBInstance>>) -> Self {
        Self { instance }
    }

    fn execute_command(&mut self, command: &str) -> Result<(), Box<dyn Error>> {
        info!("Executing command: {}", command);
        let mut writer = CliResultWriter::new();

        let result = match command.trim().to_lowercase().as_str() {
            "status" => {
                debug!("Handling status command");
                self.handle_status(&mut writer)
            }
            "info" => {
                debug!("Handling info command");
                self.handle_info(&mut writer)
            }
            "help" => {
                debug!("Displaying help");
                self.handle_help();
                Ok(())
            }
            "catalog" => {
                debug!("Handling tables command");
                self.handle_catalog(&mut writer)
            }
            "flush" => {
                debug!("Handling flush command");
                self.handle_flush();
                Ok(())
            }
            _ => {
                info!("Processing SQL command");
                self.execute_sql(command, &mut writer)
            }
        };

        if let Err(e) = &result {
            warn!("Command execution failed: {}", e);
        }

        debug!("Ensuring result writer cleanup");
        writer.end_table();

        debug!("Command execution completed");
        result
    }

    fn handle_flush(&self) {
        let instance_guard = self.instance.lock();
        let bpm = instance_guard.get_buffer_pool_manager().unwrap();
        bpm.flush_all_pages();
        debug!("Flush all pages completed");
    }

    fn handle_status(&self, writer: &mut impl ResultWriter) -> Result<(), Box<dyn Error>> {
        let instance = self.instance.lock();
        let config = instance.get_config();

        debug!("Writing status information");
        writer.begin_table(true);
        writer.begin_header();
        writer.write_header_cell("Setting");
        writer.write_header_cell("Value");
        writer.end_header();

        // Write configuration details
        self.write_status_row(writer, "Database File", &config.db_filename);
        self.write_status_row(writer, "Log File", &config.db_log_filename);
        self.write_status_row(
            writer,
            "Buffer Pool Size",
            &config.buffer_pool_size.to_string(),
        );
        self.write_status_row(writer, "LRU-K Value", &config.lru_k.to_string());
        self.write_status_row(
            writer,
            "LRU Sample Size",
            &config.lru_sample_size.to_string(),
        );
        self.write_status_row(
            writer,
            "Logging Enabled",
            &config.enable_logging.to_string(),
        );

        writer.end_table();
        debug!("Status display completed");
        Ok(())
    }

    fn write_status_row(&self, writer: &mut impl ResultWriter, label: &str, value: &str) {
        writer.begin_row();
        writer.write_cell(label);
        writer.write_cell(value);
        writer.end_row();
    }

    fn handle_info(&self, writer: &mut impl ResultWriter) -> Result<(), Box<dyn Error>> {
        let instance = self.instance.lock();
        debug!("Writing detailed system information");

        // Component Status Table
        writer.begin_table(true);
        writer.begin_header();
        writer.write_header_cell("Component");
        writer.write_header_cell("Status");
        writer.end_header();

        self.write_info_row(writer, "Buffer Pool", instance.get_buffer_pool_manager().is_some());
        self.write_info_row(writer, "Transaction Manager", instance.get_transaction_manager().is_some());
        self.write_info_row(writer, "Log Manager", instance.get_log_manager().is_some());
        self.write_info_row(writer, "Lock Manager", instance.get_lock_manager().is_some());
        self.write_info_row(writer, "Checkpoint Manager", instance.get_checkpoint_manager().is_some());
        writer.end_table();

        // Buffer Pool Details
        if let Some(bpm) = instance.get_buffer_pool_manager() {
            writer.begin_table(true);
            writer.begin_header();
            writer.write_header_cell("Buffer Pool Statistics");
            writer.write_header_cell("Value");
            writer.end_header();

            writer.begin_row();
            writer.write_cell("Pool Size");
            writer.write_cell(&bpm.get_pool_size().to_string());
            writer.end_row();

            writer.begin_row();
            writer.write_cell("Free Frames");
            writer.write_cell(&bpm.get_free_list_size().to_string());
            writer.end_row();

            writer.end_table();
        }

        // Transaction Manager Details
        if let Some(txn_mgr) = instance.get_transaction_manager() {
            let txn_mgr_guard = txn_mgr.lock();
            writer.begin_table(true);
            writer.begin_header();
            writer.write_header_cell("Transaction Statistics");
            writer.write_header_cell("Value");
            writer.end_header();

            writer.begin_row();
            writer.write_cell("Active Transactions");
            writer.write_cell(&txn_mgr_guard.get_active_transaction_count().to_string());
            writer.end_row();

            writer.begin_row();
            writer.write_cell("Next Transaction ID");
            writer.write_cell(&txn_mgr_guard.get_next_transaction_id().to_string());
            writer.end_row();

            writer.end_table();
        }

        // Log Manager Details
        if let Some(log_mgr) = instance.get_log_manager() {
            writer.begin_table(true);
            writer.begin_header();
            writer.write_header_cell("Log Statistics");
            writer.write_header_cell("Value");
            writer.end_header();

            writer.begin_row();
            writer.write_cell("Persistent LSN");
            writer.write_cell(&log_mgr.get_persistent_lsn().to_string());
            writer.end_row();

            writer.begin_row();
            writer.write_cell("Log Buffer Size");
            writer.write_cell(&log_mgr.get_log_buffer_size().to_string());
            writer.end_row();

            writer.end_table();
        }

        // Lock Manager Details
        if let Some(lock_mgr) = instance.get_lock_manager() {
            writer.begin_table(true);
            writer.begin_header();
            writer.write_header_cell("Lock Statistics");
            writer.write_header_cell("Value");
            writer.end_header();

            writer.begin_row();
            writer.write_cell("Active Locks");
            writer.write_cell(&lock_mgr.get_active_lock_count().to_string());
            writer.end_row();

            writer.begin_row();
            writer.write_cell("Waiting Transactions");
            writer.write_cell(&lock_mgr.get_waiting_lock_count().to_string());
            writer.end_row();

            writer.end_table();
        }

        // Replacer Details
        if let Some(bpm) = instance.get_buffer_pool_manager() {
            if let Some(replacer) = bpm.get_replacer() {
                writer.begin_table(true);
                writer.begin_header();
                writer.write_header_cell("Replacer Statistics");
                writer.write_header_cell("Value");
                writer.end_header();

                writer.begin_row();
                writer.write_cell("K Value");
                writer.write_cell(&replacer.get_k().to_string());
                writer.end_row();

                writer.begin_row();
                writer.write_cell("Sample Size");
                writer.write_cell(&replacer.get_replacer_size().to_string());
                writer.end_row();

                writer.begin_row();
                writer.write_cell("Victim Count");
                writer.write_cell(&replacer.total_evictable_frames().to_string());
                writer.end_row();

                writer.end_table();
            }
        }

        debug!("System information display completed");
        Ok(())
    }

    fn write_info_row(&self, writer: &mut impl ResultWriter, component: &str, is_available: bool) {
        writer.begin_row();
        writer.write_cell(component);
        writer.write_cell(if is_available {
            "Available"
        } else {
            "Disabled"
        });
        writer.end_row();
    }

    fn handle_catalog(&self, writer: &mut impl ResultWriter) -> Result<(), Box<dyn Error>> {
        let instance = self.instance.lock();
        debug!("Retrieving catalog information");

        let catalog = instance.get_catalog();
        let catalog_read = catalog.read();
        let table_names = catalog_read.get_table_names();

        debug!("Writing catalog information");
        writer.begin_table(false);
        writer.begin_header();
        writer.write_header_cell("Table OID");
        writer.write_header_cell("Table Name");
        writer.write_header_cell("Schema");
        writer.write_header_cell("Indexes");
        writer.end_header();

        for name in table_names {
            if let Some(table_info) = catalog_read.get_table(&name) {
                // Prepare schema string
                let schema = table_info.get_table_schema();
                let schema_str = (0..schema.get_column_count())
                    .map(|i| {
                        let col = schema.get_column(i as usize).unwrap();
                        format!("{}: {:?}", col.get_name(), col.get_type())
                    })
                    .collect::<Vec<_>>()
                    .join(", ");

                // Prepare indexes string
                let indexes = catalog_read.get_table_indexes(&name)
                    .iter()
                    .map(|idx| idx.get_index_name())
                    .collect::<Vec<_>>()
                    .join(", ");

                writer.begin_row();
                writer.write_cell(&table_info.get_table_oidt().to_string());
                writer.write_cell(&table_info.get_table_name());
                writer.write_cell(&schema_str);
                writer.write_cell(&indexes);
                writer.end_row();
            }
        }

        writer.end_table();
        debug!("Catalog display completed");
        Ok(())
    }

    fn handle_help(&self) {
        println!("\n{}", "Available Commands:".bold());
        println!("\nMeta Commands:");
        println!("  status  - Display database configuration and status");
        println!("  info    - Show detailed system component information");
        println!("  catalog - Show catalog of the database");
        println!("  flush   - Flush all pages in buffer pool to disk");
        println!("  help    - Show this help message");
        println!("  exit    - Exit the database");

        println!("\nSQL Commands:");
        println!("  CREATE TABLE tablename (col1 type1, col2 type2, ...)");
        println!("  INSERT INTO tablename VALUES (val1, val2, ...)");
        println!("  SELECT col1, col2 FROM tablename [WHERE conditions]");
        println!("  DROP TABLE tablename");

        debug!("Help display completed");
    }

    fn execute_sql(&self, sql: &str, writer: &mut impl ResultWriter) -> Result<(), Box<dyn Error>> {
        debug!("Starting SQL execution");
        let mut instance = self.instance.lock();

        match instance.execute_sql(sql, writer, None) {
            Ok(_) => {
                debug!("SQL execution completed successfully");
                Ok(())
            }
            Err(e) => {
                warn!("SQL execution failed: {}", e);
                Err(Box::new(e))
            }
        }
    }
}

pub fn run_cli() -> Result<(), Box<dyn Error>> {
    initialize_logger();
    let args = Args::parse();

    info!("Starting CLI with configuration");
    debug!("Initializing database instance");

    let config = DBConfig {
        db_filename: args
            .db_name
            .clone()
            .unwrap_or_else(|| "default_db.db".to_string()),
        db_log_filename: format!(
            "{}.log",
            args.db_name.unwrap_or_else(|| "default_db".to_string())
        ),
        buffer_pool_size: args.buffer_size.unwrap_or(1024),
        enable_logging: false,
        enable_managed_transactions: false,
        lru_k: args.k_value.unwrap_or(2),
        lru_sample_size: args.frames.unwrap_or(7),
    };

    println!("{}", "\nTK Database System".blue().bold());
    println!("Type 'help' for commands\n");

    let instance = Arc::new(Mutex::new(DBInstance::new(config)?));
    let mut executor = DBCommandExecutor::new(Arc::clone(&instance));

    debug!("Setting up command line interface");
    let mut rl = DefaultEditor::new()?;
    if rl.load_history("history.txt").is_err() {
        println!("{}", "No previous history.".yellow());
    }

    info!("Entering command loop");
    loop {
        debug!("Waiting for command input");
        match rl.readline("db> ") {
            Ok(line) => {
                let command = line.trim();
                if command.is_empty() {
                    debug!("Empty command, continuing");
                    continue;
                }

                rl.add_history_entry(command)?;

                if command == "exit" {
                    info!("Received exit command");
                    println!("Shutting down...");
                    break;
                }

                debug!("Executing command and waiting for completion");
                match executor.execute_command(command) {
                    Ok(_) => {
                        debug!("Command completed successfully, preparing for next input");
                    }
                    Err(e) => {
                        warn!("Command failed: {}", e);
                        println!("{}", format!("Error: {}", e).red());
                    }
                }
                debug!("Command cycle complete");
            }
            Err(err) => {
                warn!("CLI input error: {}", err);
                println!("Error: {}", err);
                break;
            }
        }
    }

    info!("Saving command history and shutting down");
    rl.save_history("history.txt")?;
    Ok(())
}
