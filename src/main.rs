#![allow(clippy::field_reassign_with_default)]

use clap::{Parser, Subcommand};
use colored::Colorize;
use env_logger::Builder;
use log::info;
use rustyline::DefaultEditor;
use std::error;
use std::sync::Arc;
use tkdb::cli::CLI;
use tkdb::client::DatabaseClient;
use tkdb::common::db_instance::{DBConfig, DBInstance};
use tkdb::common::exception::DBError;
use tkdb::common::logger::initialize_logger;
use tkdb::server::{ServerConfig, ServerHandle};
use tokio::signal;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the database server
    Server {
        #[arg(short = 'P', long, default_value = "5432")]
        port: u16,
        #[arg(short = 'c', long)]
        config: Option<String>,
    },
    /// Start an interactive client
    Client {
        #[arg(short = 'H', long, default_value = "127.0.0.1")]
        host: String,
        #[arg(short = 'P', long, default_value = "5432")]
        port: u16,
    },
    Cli,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Server { port, config } => run_server(port, config).await,
        Commands::Client { host, port } => run_client(&format!("{}:{}", host, port)).await,
        Commands::Cli => run_cli().await,
    }
}

async fn run_server(port: u16, config_path: Option<String>) -> Result<(), Box<dyn error::Error>> {
    // Initialize logger with custom format for server
    // Builder::new()
    //     .filter_level(log::LevelFilter::Debug)
    //     .format(|buf, record| {
    //         use std::io::Write;
    //         writeln!(
    //             buf,
    //             "{} [{}] [Server] {}",
    //             chrono::Local::now().format("%H:%M:%S"),
    //             record.level(),
    //             record.args()
    //         )
    //     })
    //     .init();

    // Load server configuration
    let server_config = if let Some(config_path) = config_path {
        ServerConfig::load(std::path::Path::new(&config_path))?
    } else {
        let mut config = ServerConfig::default();
        config.port = port; // Override with command line port
        config
    };

    // Create config with server enabled
    let mut db_config = DBConfig::default();
    db_config.server_enabled = true;
    db_config.server_port = server_config.port;

    info!("Starting TKDB server on port {}", server_config.port);

    // Create database instance
    let db = Arc::new(DBInstance::new(db_config).await?);

    // Create and start server
    let mut server = ServerHandle::new(server_config.port);
    server.start(db.clone())?;

    info!("Server is running. Press Ctrl+C to stop.");

    // Wait for shutdown signal
    signal::ctrl_c().await?;

    info!("Shutting down server...");

    // Graceful shutdown
    server.shutdown()?;

    Ok(())
}

async fn run_client(addr: &str) -> Result<(), Box<dyn error::Error>> {
    // Initialize logger with custom format and debug level
    Builder::new()
        .filter_level(log::LevelFilter::Debug)
        // Filter out rustyline debug logs
        .filter_module("rustyline", log::LevelFilter::Info)
        .format(|buf, record| {
            use std::io::Write;
            writeln!(
                buf,
                "{} [{}] {}",
                chrono::Local::now().format("%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .init();

    let mut client = DatabaseClient::connect(addr).await?;
    let mut rl = DefaultEditor::new()?;

    println!("\nConnected to TKDB at {}.", addr);
    println!("Type your queries (end with ;)");
    println!("Type 'exit;' to quit\n");

    loop {
        let mut buffer = String::new();

        loop {
            let prompt = if buffer.is_empty() {
                "tkdb> "
            } else {
                "   -> "
            };
            match rl.readline(prompt) {
                Ok(line) => {
                    rl.add_history_entry(line.as_str())?;
                    buffer.push_str(&line);

                    if line.trim().ends_with(';') {
                        break;
                    }
                }
                Err(_) => return Ok(()),
            }
        }

        let query = buffer.trim();
        if query == "exit;" {
            println!("\nBye!");
            break;
        }

        match client.execute_query(query).await {
            Ok(results) => {
                // Print any messages first
                for message in &results.messages {
                    println!("\n{}", message);
                }

                // Create and format table if there are columns/rows
                if !results.column_names.is_empty() {
                    use prettytable::{Cell, Row, Table, format};
                    let mut table = Table::new();

                    // Set table format with clean borders and proper alignment
                    table.set_format(*format::consts::FORMAT_BOX_CHARS);

                    // Add header row with bold and centered headers
                    table.set_titles(Row::new(
                        results
                            .column_names
                            .iter()
                            .map(|name| {
                                let mut cell = Cell::new(&name.bold().to_string());
                                cell.align(format::Alignment::CENTER);
                                cell
                            })
                            .collect(),
                    ));

                    // Add data rows with right alignment for numeric values
                    for row in results.rows {
                        table.add_row(Row::new(
                            row.iter()
                                .map(|v| {
                                    let mut cell = Cell::new(&v.to_string());
                                    if v.is_numeric() {
                                        cell.align(format::Alignment::RIGHT);
                                    }
                                    cell
                                })
                                .collect(),
                        ));
                    }

                    // Print the table
                    println!();
                    table.printstd();
                    println!();
                }

                // Print generic success message if no specific messages
                if results.messages.is_empty() && results.column_names.is_empty() {
                    println!("\nQuery executed successfully");
                }
            }
            Err(e) => {
                // Format error message for better readability
                let error_msg = match e {
                    DBError::Io(msg) => format!("IO Error: {}", msg),
                    DBError::LockError(msg) => format!("Lock Error: {}", msg),
                    DBError::Transaction(msg) => format!("Transaction Error: {}", msg),
                    DBError::NotImplemented(msg) => format!("Operation not implemented: {}", msg),
                    DBError::Catalog(msg) => format!("Catalog Error: {}", msg),
                    DBError::Execution(msg) => format!("Execution Error: {}", msg),
                    DBError::Validation(msg) => format!("Validation Error: {}", msg),
                    DBError::TableNotFound(msg) => format!("Table not found: {}", msg),
                    DBError::PlanError(msg) => format!("Planning Error: {}", msg),
                    DBError::Internal(msg) => format!("Internal Error: {}", msg),
                    DBError::OptimizeError(msg) => format!("Optimization Error: {}", msg),
                    DBError::SqlError(msg) => format!("SQL Error: {}", msg),
                    DBError::Client(msg) => format!("Database Error: {}", msg),
                    DBError::Recovery(msg) => format!("Recovery Error: {}", msg),
                };
                eprintln!("\n{}\n", error_msg);
            }
        }
    }

    Ok(())
}

async fn run_cli() -> Result<(), Box<dyn error::Error>> {
    initialize_logger();

    let cli = CLI::new().await;

    let _: () = cli.unwrap().run().await.unwrap();
    Ok(())
}
