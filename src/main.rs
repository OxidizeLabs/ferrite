use clap::{Parser, Subcommand};
use tokio::signal;
use tkdb::client::DatabaseClient;
use tkdb::common::db_instance::{DBConfig, DBInstance};
use tkdb::server::ServerHandle;
use std::sync::Arc;
use rustyline::DefaultEditor;

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
    // Create config with server enabled
    let mut config = DBConfig::default();
    config.server_enabled = true;
    config.server_port = port;
    
    println!("Starting TKDB server on port {}", port);
    
    // Create database instance
    let db = Arc::new(DBInstance::new(config)?);
    
    // Create and start server
    let mut server = ServerHandle::new(port);
    server.start(db.clone())?;
    
    println!("Server is running. Press Ctrl+C to stop.");
    
    // Wait for shutdown signal
    signal::ctrl_c().await?;
    
    println!("Shutting down server...");
    
    // Graceful shutdown
    server.shutdown()?;
    
    Ok(())
}

async fn run_client(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = DatabaseClient::connect(addr).await?;
    let mut rl = DefaultEditor::new()?;

    println!("Connected to TKDB at {}. Type your queries (end with ;)", addr);
    println!("Type 'exit;' to quit");

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
            break;
        }

        match client.execute_query(query).await {
            Ok(results) => {
                println!("\nColumns: {:?}", results.column_names);
                for row in results.rows {
                    println!("Row: {:?}", row);
                }
                println!();
            }
            Err(e) => eprintln!("Error: {}\n", e),
        }
    }

    Ok(())
}
