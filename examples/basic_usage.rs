//! Basic usage example for Ferrite database
//!
//! This example demonstrates how to:
//! - Create a database instance
//! - Execute SQL queries
//! - Handle query results

use std::error::Error;

use ferrite::common::db_instance::{DBConfig, DBInstance};
use ferrite::common::result_writer::CliResultWriter;
use ferrite::concurrency::transaction::IsolationLevel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging (optional)
    env_logger::init();

    // Configure the database
    let config = DBConfig {
        db_filename: "example.db".to_string(),
        db_log_filename: "example.log".to_string(),
        buffer_pool_size: 1024,
        enable_logging: true,
        ..Default::default()
    };

    // Create the database instance
    println!("Creating database instance...");
    let db = DBInstance::new(config).await?;

    // Create a result writer for output
    let mut writer = CliResultWriter::new();

    // Create a table
    println!("\n--- Creating table ---");
    db.execute_sql(
        "CREATE TABLE users (id INTEGER, name VARCHAR(100), email VARCHAR(200));",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await?;

    // Insert some data
    println!("\n--- Inserting data ---");
    db.execute_sql(
        "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await?;

    db.execute_sql(
        "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await?;

    db.execute_sql(
        "INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com');",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await?;

    // Query the data
    println!("\n--- Querying all users ---");
    db.execute_sql(
        "SELECT * FROM users;",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await?;

    // Query with a filter
    println!("\n--- Querying users with id > 1 ---");
    db.execute_sql(
        "SELECT name, email FROM users WHERE id > 1;",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await?;

    // Update data
    println!("\n--- Updating Alice's email ---");
    db.execute_sql(
        "UPDATE users SET email = 'alice.new@example.com' WHERE id = 1;",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await?;

    // Verify the update
    println!("\n--- Verifying update ---");
    db.execute_sql(
        "SELECT * FROM users WHERE id = 1;",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await?;

    // Delete data
    println!("\n--- Deleting Bob ---");
    db.execute_sql(
        "DELETE FROM users WHERE id = 2;",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await?;

    // Final state
    println!("\n--- Final table state ---");
    db.execute_sql(
        "SELECT * FROM users ORDER BY id;",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await?;

    println!("\nExample completed successfully!");

    // Clean up (optional - remove database files)
    std::fs::remove_file("example.db").ok();
    std::fs::remove_file("example.log").ok();

    Ok(())
}
