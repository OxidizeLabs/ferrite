//! Transaction example for Ferrite database
//!
//! This example demonstrates how to:
//! - Use explicit transactions
//! - Work with different isolation levels
//! - Handle transaction commit and rollback

use std::error::Error;

use ferrite::common::db_instance::{DBConfig, DBInstance};
use ferrite::common::result_writer::CliResultWriter;
use ferrite::concurrency::transaction::IsolationLevel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    // Configure the database
    let config = DBConfig {
        db_filename: "transactions_example.db".to_string(),
        db_log_filename: "transactions_example.log".to_string(),
        buffer_pool_size: 1024,
        enable_logging: true,
        ..Default::default()
    };

    let db = DBInstance::new(config).await?;
    let mut writer = CliResultWriter::new();

    // Setup: Create a table
    println!("Setting up table...");
    db.execute_sql(
        "CREATE TABLE accounts (id INTEGER, name VARCHAR(100), balance INTEGER);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await?;

    // Insert initial data
    db.execute_sql(
        "INSERT INTO accounts VALUES (1, 'Alice', 1000);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await?;

    db.execute_sql(
        "INSERT INTO accounts VALUES (2, 'Bob', 500);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await?;

    println!("\n--- Initial balances ---");
    db.execute_sql(
        "SELECT * FROM accounts;",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await?;

    // Example 1: Successful transaction (simulated transfer)
    println!("\n--- Performing transfer: Alice -> Bob ($200) ---");

    // Begin an explicit transaction
    let txn_ctx = db.begin_transaction(IsolationLevel::Serializable);

    // Debit Alice
    db.execute_transaction(
        "UPDATE accounts SET balance = balance - 200 WHERE id = 1;",
        txn_ctx.clone(),
        &mut writer,
    )
    .await?;

    // Credit Bob
    db.execute_transaction(
        "UPDATE accounts SET balance = balance + 200 WHERE id = 2;",
        txn_ctx.clone(),
        &mut writer,
    )
    .await?;

    // Note: Transaction will be committed when txn_ctx is dropped or explicitly committed
    // For this example, we let the auto-commit handle it

    println!("\n--- Balances after transfer ---");
    db.execute_sql(
        "SELECT * FROM accounts;",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await?;

    // Example 2: Using different isolation levels
    println!("\n--- Demonstrating isolation levels ---");

    // Read Committed - sees only committed data
    println!("\nRead Committed query:");
    db.execute_sql(
        "SELECT * FROM accounts WHERE id = 1;",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await?;

    // Serializable - highest isolation, prevents all anomalies
    println!("\nSerializable query:");
    db.execute_sql(
        "SELECT * FROM accounts WHERE id = 1;",
        IsolationLevel::Serializable,
        &mut writer,
    )
    .await?;

    // Example 3: Rollback scenario
    println!("\n--- Demonstrating rollback ---");
    println!("Initial state:");
    db.execute_sql(
        "SELECT * FROM accounts;",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await?;

    // Start a transaction that we'll rollback
    println!("\nAttempting invalid transfer (will rollback)...");
    db.execute_sql(
        "UPDATE accounts SET balance = balance - 5000 WHERE id = 1;",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await?;

    // Rollback the transaction
    db.execute_sql("ROLLBACK;", IsolationLevel::ReadCommitted, &mut writer)
        .await?;

    println!("\nState after rollback (should be unchanged):");
    db.execute_sql(
        "SELECT * FROM accounts;",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await?;

    println!("\nTransaction example completed!");

    // Clean up
    std::fs::remove_file("transactions_example.db").ok();
    std::fs::remove_file("transactions_example.log").ok();

    Ok(())
}
