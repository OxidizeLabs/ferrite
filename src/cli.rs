use crate::common::db_instance::{DBConfig, DBInstance, ResultWriter};
use crate::common::logger::initialize_logger;
use clap::Parser;
use colored::*;
use parking_lot::Mutex;
use rustyline::DefaultEditor;
use sqlparser::dialect::GenericDialect;
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

struct CliResultWriter {
    table_started: bool,
}

impl CliResultWriter {
    fn new() -> Self {
        Self {
            table_started: false,
        }
    }
}

impl ResultWriter for CliResultWriter {
    fn begin_table(&mut self, _bordered: bool) {
        self.table_started = true;
    }

    fn end_table(&mut self) {
        if self.table_started {
            println!();
            self.table_started = false;
        }
    }

    fn begin_header(&mut self) {}

    fn end_header(&mut self) {
        println!();
    }

    fn begin_row(&mut self) {}

    fn end_row(&mut self) {
        println!();
    }

    fn write_cell(&mut self, content: &str) {
        print!("{:<15}", content);
    }

    fn write_header_cell(&mut self, content: &str) {
        print!("{:<15}", content.bold());
    }

    fn one_cell(&mut self, content: &str) {
        println!("{}", content);
    }
}

struct DBCommandExecutor {
    instance: Arc<Mutex<DBInstance>>,
    dialect: GenericDialect,
}

impl DBCommandExecutor {
    fn new(instance: Arc<Mutex<DBInstance>>) -> Self {
        Self {
            instance,
            dialect: GenericDialect {},
        }
    }

    fn execute_command(&mut self, command: &str) -> Result<(), Box<dyn Error>> {
        let mut writer = CliResultWriter::new();

        match command.trim().to_lowercase().as_str() {
            "status" => self.handle_status(&mut writer)?,
            "info" => self.handle_info(&mut writer)?,
            "help" => self.display_help(),
            "tables" => self.handle_tables(&mut writer)?,
            _ => {
                // Parse and execute SQL with a new transaction
                self.execute_sql(command, &mut writer)?;
            }
        }

        Ok(())
    }

    fn handle_status(&self, writer: &mut impl ResultWriter) -> Result<(), Box<dyn Error>> {
        let instance = self.instance.lock();

        let config = instance.get_config();
        writer.begin_table(true);
        writer.begin_header();
        writer.write_header_cell("Setting");
        writer.write_header_cell("Value");
        writer.end_header();

        writer.begin_row();
        writer.write_cell("Database File");
        writer.write_cell(&config.db_filename);
        writer.end_row();

        writer.begin_row();
        writer.write_cell("Log File");
        writer.write_cell(&config.db_log_filename);
        writer.end_row();

        writer.begin_row();
        writer.write_cell("Buffer Pool Size");
        writer.write_cell(&config.buffer_pool_size.to_string());
        writer.end_row();

        writer.begin_row();
        writer.write_cell("LRU-K Value");
        writer.write_cell(&config.lru_k.to_string());
        writer.end_row();

        writer.begin_row();
        writer.write_cell("LRU Sample Size");
        writer.write_cell(&config.lru_sample_size.to_string());
        writer.end_row();

        writer.begin_row();
        writer.write_cell("Logging Enabled");
        writer.write_cell(&config.enable_logging.to_string());
        writer.end_row();

        writer.end_table();
        Ok(())
    }

    fn handle_info(&self, writer: &mut impl ResultWriter) -> Result<(), Box<dyn Error>> {
        let instance = self.instance.lock();

        writer.begin_table(true);
        writer.begin_header();
        writer.write_header_cell("Component");
        writer.write_header_cell("Status");
        writer.end_header();

        // Buffer Pool Status
        writer.begin_row();
        writer.write_cell("Buffer Pool");
        writer.write_cell(if instance.get_buffer_pool_manager().is_some() {
            "Available"
        } else {
            "Disabled"
        });
        writer.end_row();

        // Log Manager Status
        writer.begin_row();
        writer.write_cell("Log Manager");
        writer.write_cell(if instance.get_log_manager().is_some() {
            "Available"
        } else {
            "Disabled"
        });
        writer.end_row();

        // Checkpoint Manager Status
        writer.begin_row();
        writer.write_cell("Checkpoint Manager");
        writer.write_cell(if instance.get_checkpoint_manager().is_some() {
            "Available"
        } else {
            "Disabled"
        });
        writer.end_row();

        writer.end_table();
        Ok(())
    }

    fn handle_tables(&self, writer: &mut impl ResultWriter) -> Result<(), Box<dyn Error>> {
        let instance = self.instance.lock();
        instance.handle_cmd_display_tables(writer)?;
        Ok(())
    }

    fn execute_sql(&self, sql: &str, writer: &mut impl ResultWriter) -> Result<(), Box<dyn Error>> {
        let mut instance = self.instance.lock();

        match instance.execute_sql(sql, writer, None) {
            Ok(_) => Ok(()),
            Err(e) => Err(Box::new(e)),
        }
    }

    fn display_help(&self) {
        println!("\n{}", "Available Commands:".bold());
        println!("\nMeta Commands:");
        println!("  status  - Display database configuration and status");
        println!("  info    - Show component status and information");
        println!("  tables  - List all tables in the database");
        println!("  help    - Show this help message");
        println!("  exit    - Exit the database");

        println!("\nSQL Commands:");
        println!("  CREATE TABLE tablename (col1 type1, col2 type2, ...)");
        println!("  INSERT INTO tablename VALUES (val1, val2, ...)");
        println!("  SELECT col1, col2 FROM tablename [WHERE conditions]");
        println!("  DROP TABLE tablename");
    }
}

pub fn run_cli() -> Result<(), Box<dyn Error>> {
    initialize_logger();
    let args = Args::parse();

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

    let mut rl = DefaultEditor::new()?;
    if rl.load_history("history.txt").is_err() {
        println!("{}", "No previous history.".yellow());
    }

    loop {
        match rl.readline("db> ") {
            Ok(line) => {
                let command = line.trim();
                if command.is_empty() {
                    continue;
                }

                rl.add_history_entry(command)?;

                if command == "exit" {
                    println!("Shutting down...");
                    break;
                }

                match executor.execute_command(command) {
                    Ok(_) => {}
                    Err(e) => println!("{}", format!("Error: {}", e).red()),
                }
            }
            Err(err) => {
                println!("Error: {}", err);
                break;
            }
        }
    }

    rl.save_history("history.txt")?;
    Ok(())
}
