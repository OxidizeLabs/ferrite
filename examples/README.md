# Ferrite Examples

This directory contains example programs demonstrating how to use Ferrite.

## Running Examples

You can run any example using cargo:

```bash
# Basic usage example
cargo run --example basic_usage

# Transaction example
cargo run --example transactions
```

## Examples Overview

### `basic_usage.rs`

Demonstrates fundamental database operations:
- Creating a database instance with configuration
- Creating tables with SQL DDL
- Inserting, updating, and deleting data
- Querying data with SELECT statements
- Using WHERE clauses for filtering

### `transactions.rs`

Shows transaction management:
- Using explicit transactions
- Different isolation levels (ReadCommitted, Serializable)
- Transaction commit and rollback
- Simulating a bank transfer scenario

## Common Patterns

### Creating a Database Instance

```rust
use ferrite::common::db_instance::{DBConfig, DBInstance};

let config = DBConfig {
    db_filename: "my_database.db".to_string(),
    db_log_filename: "my_database.log".to_string(),
    buffer_pool_size: 1024,
    enable_logging: true,
    ..Default::default()
};

let db = DBInstance::new(config).await?;
```

### Executing SQL

```rust
use ferrite::common::result_writer::CliResultWriter;
use ferrite::concurrency::transaction::IsolationLevel;

let mut writer = CliResultWriter::new();

db.execute_sql(
    "SELECT * FROM users;",
    IsolationLevel::ReadCommitted,
    &mut writer,
).await?;
```

### Using Transactions

```rust
// Begin a transaction
let txn_ctx = db.begin_transaction(IsolationLevel::Serializable);

// Execute within the transaction
db.execute_transaction(
    "UPDATE accounts SET balance = balance - 100 WHERE id = 1;",
    txn_ctx.clone(),
    &mut writer,
).await?;

// The transaction commits when txn_ctx is dropped
```

## More Information

- See the [documentation](../docs/) for architecture details
- Check [CONTRIBUTING.md](../CONTRIBUTING.md) for development guidelines
- Read the [README](../README.md) for project overview
