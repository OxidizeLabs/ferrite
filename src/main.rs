use clap::{Parser, Subcommand};
use tokio::signal;
use tkdb::client::DatabaseClient;
use tkdb::common::db_instance::{DBConfig, DBInstance};
use tkdb::server::ServerHandle;
use std::sync::Arc;
use rustyline::DefaultEditor;
use env_logger::{Builder, Env};
use log::{error, info};
use tkdb::common::exception::DBError;

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
    },
    /// Start an interactive client
    Client {
        #[arg(short = 'H', long, default_value = "127.0.0.1")]
        host: String,
        #[arg(short = 'P', long, default_value = "5432")]
        port: u16,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Server { port } => run_server(port).await,
        Commands::Client { host, port } => run_client(&format!("{}:{}", host, port)).await,
    }
}

async fn run_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger with custom format for server
    Builder::new()
        .filter_level(log::LevelFilter::Debug)
        .format(|buf, record| {
            use std::io::Write;
            writeln!(
                buf,
                "{} [{}] [Server] {}",
                chrono::Local::now().format("%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .init();

    // Create config with server enabled
    let mut config = DBConfig::default();
    config.server_enabled = true;
    config.server_port = port;
    
    info!("Starting TKDB server on port {}", port);
    
    // Create database instance
    let db = Arc::new(DBInstance::new(config)?);
    
    // Create and start server
    let mut server = ServerHandle::new(port);
    server.start(db.clone())?;
    
    info!("Server is running. Press Ctrl+C to stop.");
    
    // Wait for shutdown signal
    signal::ctrl_c().await?;
    
    info!("Shutting down server...");
    
    // Graceful shutdown
    server.shutdown()?;
    
    Ok(())
}

async fn run_client(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
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
            let prompt = if buffer.is_empty() { "tkdb> " } else { "   -> " };
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

                // Print column headers if there are any
                if !results.column_names.is_empty() {
                    println!("\n{}", results.column_names.join(" | "));
                    println!("{}", "-".repeat(results.column_names.join(" | ").len()));
                }

                // Print rows if there are any
                for row in results.rows {
                    println!("{}", row.iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(" | "));
                }

                // Print generic success message if no specific messages
                if results.messages.is_empty() && results.column_names.is_empty() {
                    println!("\nQuery executed successfully");
                }
                println!(); // Extra newline for readability
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
                    DBError::Client(msg) => format!("Database Error: {}", msg)
                };
                eprintln!("\n{}\n", error_msg);
            }
        }
    }

    Ok(())
}
