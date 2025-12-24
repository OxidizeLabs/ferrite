<p align="center">
  <img src="https://img.shields.io/badge/Rust-2024_Edition-orange?style=flat-square&logo=rust" alt="Rust 2024">
  <img src="https://img.shields.io/badge/License-MIT%2FApache--2.0-blue?style=flat-square" alt="License">
  <img src="https://img.shields.io/badge/Platform-macOS%20%7C%20Linux-lightgrey?style=flat-square" alt="Platform">
</p>

# Ferrite

**A Rust-native database management system built on proven DBMS techniques.**

Ferrite combines decades of database research with Rust's memory safety and performance guarantees to deliver a reliable, high-performance storage engine. Built with the rigor of academic database systems and the ergonomics of modern Rust.

---

## ✨ Key Features

### Core Database Capabilities

| Feature | Description |
|---------|-------------|
| **Buffer Pool Management** | LRU-K replacement policy for scan-resistant page caching |
| **B+ Tree Indexes** | Self-balancing indexes with latch crabbing for concurrent access |
| **Write-Ahead Logging** | ARIES-style recovery with checkpoints for crash durability |
| **ACID Transactions** | Full transaction support with multiple isolation levels |
| **Concurrency Control** | Two-phase locking with deadlock detection |
| **SQL Support** | DDL, DML, queries, joins, aggregations, window functions |

### Technical Highlights

- 🦀 **Memory-Safe by Design** — Pure Rust with no unsafe in critical paths
- ⚡ **Async I/O** — Tokio-powered async execution with backpressure
- 🔒 **Robust Concurrency** — `parking_lot` locks, MVCC-ready watermarks
- 🗄️ **Flexible Deployment** — Embedded library or client-server modes

### Supported SQL Features

```sql
-- DDL
CREATE TABLE users (id INTEGER, name VARCHAR(100), email VARCHAR(200));
CREATE INDEX idx_users_email ON users(email);

-- DML
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
UPDATE users SET email = 'alice@new.com' WHERE id = 1;
DELETE FROM users WHERE id = 1;

-- Queries
SELECT * FROM users WHERE id > 10 ORDER BY name LIMIT 100;
SELECT department, COUNT(*), AVG(salary) FROM employees GROUP BY department;
SELECT *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) FROM employees;

-- Joins
SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id;
```

---

## 🚀 Getting Started

### Prerequisites

- **Rust toolchain** (stable, 2024 edition) — Install via [rustup](https://rustup.rs/)
- **Git** for version control

### Installation

```bash
# Clone the repository
git clone https://github.com/ferritedb/ferrite.git
cd ferrite

# Build the project
cargo build --release

# Run tests to verify installation
cargo test
```

### Quick Start

#### Option 1: Run SQL Scripts

```bash
# Execute a SQL file
cargo run --bin ferrite --features cli -- script.sql
```

#### Option 2: Use as an Embedded Database

Add Ferrite to your `Cargo.toml`:

```toml
[dependencies]
ferrite = { git = "https://github.com/ferritedb/ferrite.git" }
tokio = { version = "1", features = ["full"] }
```

Then use it in your application:

```rust
use ferrite::common::db_instance::{DBConfig, DBInstance};
use ferrite::common::result_writer::CliResultWriter;
use ferrite::concurrency::transaction::IsolationLevel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure and create the database
    let config = DBConfig {
        db_filename: "my_app.db".to_string(),
        db_log_filename: "my_app.log".to_string(),
        buffer_pool_size: 1024,
        enable_logging: true,
        ..Default::default()
    };

    let db = DBInstance::new(config).await?;
    let mut writer = CliResultWriter::new();

    // Create a table
    db.execute_sql(
        "CREATE TABLE users (id INTEGER, name VARCHAR(100));",
        IsolationLevel::ReadCommitted,
        &mut writer,
    ).await?;

    // Insert data
    db.execute_sql(
        "INSERT INTO users VALUES (1, 'Alice');",
        IsolationLevel::ReadCommitted,
        &mut writer,
    ).await?;

    // Query data
    db.execute_sql(
        "SELECT * FROM users;",
        IsolationLevel::ReadCommitted,
        &mut writer,
    ).await?;

    Ok(())
}
```

#### Option 3: Client-Server Mode

```bash
# Start the server
cargo run --bin ferrite --features server -- server --port 5432

# Connect with the client (in another terminal)
cargo run --bin ferrite --features client -- client --host localhost --port 5432
```

---

## 📖 Examples

Run the included examples to see Ferrite in action:

```bash
# Basic database operations (create, insert, query, update, delete)
cargo run --example basic_usage

# Transaction management with isolation levels
cargo run --example transactions
```

### Basic Usage Example

```rust
use ferrite::common::db_instance::{DBConfig, DBInstance};
use ferrite::common::result_writer::CliResultWriter;
use ferrite::concurrency::transaction::IsolationLevel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = DBConfig::default();
    let db = DBInstance::new(config).await?;
    let mut writer = CliResultWriter::new();

    // Create table
    db.execute_sql(
        "CREATE TABLE products (id INTEGER, name VARCHAR(100), price INTEGER);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    ).await?;

    // Insert products
    db.execute_sql(
        "INSERT INTO products VALUES (1, 'Laptop', 999);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    ).await?;

    // Query with filtering
    db.execute_sql(
        "SELECT name, price FROM products WHERE price > 500;",
        IsolationLevel::ReadCommitted,
        &mut writer,
    ).await?;

    Ok(())
}
```

### Transaction Example

```rust
use ferrite::common::db_instance::{DBConfig, DBInstance};
use ferrite::common::result_writer::CliResultWriter;
use ferrite::concurrency::transaction::IsolationLevel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = DBConfig::default();
    let db = DBInstance::new(config).await?;
    let mut writer = CliResultWriter::new();

    // Setup accounts table
    db.execute_sql(
        "CREATE TABLE accounts (id INTEGER, balance INTEGER);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    ).await?;

    db.execute_sql("INSERT INTO accounts VALUES (1, 1000);", IsolationLevel::ReadCommitted, &mut writer).await?;
    db.execute_sql("INSERT INTO accounts VALUES (2, 500);", IsolationLevel::ReadCommitted, &mut writer).await?;

    // Perform a transfer within a transaction
    let txn = db.begin_transaction(IsolationLevel::Serializable);

    db.execute_transaction(
        "UPDATE accounts SET balance = balance - 200 WHERE id = 1;",
        txn.clone(),
        &mut writer,
    ).await?;

    db.execute_transaction(
        "UPDATE accounts SET balance = balance + 200 WHERE id = 2;",
        txn.clone(),
        &mut writer,
    ).await?;

    // Transaction commits when txn goes out of scope
    Ok(())
}
```

---

## 📚 Documentation

### Architecture Overview

Ferrite follows a classic layered database architecture:

```
┌─────────────────────────────────────────────────────────┐
│                    Access Layer                         │
│         (CLI / Client / Server / Network)               │
├─────────────────────────────────────────────────────────┤
│                   SQL Layer                             │
│    (Parser → Binder → Planner → Optimizer → Executor)   │
├─────────────────────────────────────────────────────────┤
│               Concurrency Control                       │
│      (Transaction Manager / Lock Manager / MVCC)        │
├─────────────────────────────────────────────────────────┤
│                  Storage Layer                          │
│  (Buffer Pool / Table Heap / Indexes / Disk Manager)    │
├─────────────────────────────────────────────────────────┤
│                  Recovery Layer                         │
│        (WAL / Log Manager / Checkpoints)                │
└─────────────────────────────────────────────────────────┘
```

### Detailed Documentation

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | Comprehensive system architecture with diagrams |
| [BENCHMARKING.md](docs/BENCHMARKING.md) | Performance benchmarking guide |
| [PERFORMANCE_TESTS.md](docs/PERFORMANCE_TESTS.md) | Performance testing methodology |
| [ZERO_COPY_ARCHITECTURE.md](docs/ZERO_COPY_ARCHITECTURE.md) | Zero-copy I/O design details |

### Module Structure

| Module | Path | Description |
|--------|------|-------------|
| `buffer` | `src/buffer/` | Buffer pool management and LRU-K page replacement |
| `catalog` | `src/catalog/` | Database metadata (tables, schemas, constraints) |
| `concurrency` | `src/concurrency/` | Transaction management and lock handling |
| `recovery` | `src/recovery/` | WAL, checkpoints, crash recovery |
| `sql` | `src/sql/` | Query processing pipeline |
| `storage` | `src/storage/` | Disk I/O, pages, indexes, table heaps |
| `types_db` | `src/types_db/` | Database type system |

### Supported Types

| Category | Types |
|----------|-------|
| **Numeric** | `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`, `DECIMAL` |
| **String** | `VARCHAR(n)`, `BINARY(n)` |
| **Date/Time** | `DATE`, `TIME`, `TIMESTAMP`, `INTERVAL` |
| **Complex** | `BOOLEAN`, `UUID`, `JSON`, `ARRAY`, `STRUCT`, `ENUM`, `POINT`, `VECTOR` |

---

## 🤝 How to Contribute

We welcome contributions from the community! Here's how to get started:

### Development Setup

```bash
# Fork and clone the repository
git clone https://github.com/YOUR_USERNAME/ferrite.git
cd ferrite

# Add upstream remote
git remote add upstream https://github.com/ferritedb/ferrite.git

# Create a feature branch
git checkout -b feature/your-feature-name

# Build and test
cargo build
cargo test
```

### Before Submitting a PR

```bash
# Format your code
cargo fmt

# Run clippy and fix all warnings
cargo clippy -- -D warnings

# Run the full test suite
cargo test

# Run benchmarks (optional)
cargo bench
```

### Contribution Guidelines

1. **Find or create an issue** for the work you want to do
2. **Follow coding standards**: Use `snake_case` for functions, `PascalCase` for types
3. **Write tests** for new functionality
4. **Update documentation** when changing public APIs
5. **Use conventional commits**: `feat:`, `fix:`, `docs:`, `refactor:`, `perf:`, `test:`

### Good First Issues

Look for issues labeled `good first issue` — these are curated for newcomers with clear scope and guidance.

### Pull Request Process

1. Reference the related issue in your PR description
2. Describe what changes were made and why
3. Note any breaking changes
4. Request review from maintainers

For detailed guidelines, see [CONTRIBUTING.md](CONTRIBUTING.md).

---

## 🗺️ Roadmap

### Phase 1: Alpha Release
*Goal: Make basic types and SQL statements production-ready*

- [ ] **Core SQL Completeness**
  - [ ] IN operator support (`WHERE id IN (1, 2, 3)`)
  - [ ] BETWEEN operator (`WHERE age BETWEEN 18 AND 65`)
  - [ ] LIKE/ILIKE pattern matching (`WHERE name LIKE 'A%'`)
  - [ ] DEFAULT values in INSERT statements
  - [ ] Subquery support in WHERE clauses
- [ ] **Index Improvements**
  - [ ] VarChar index support
  - [ ] Composite/multi-column index support
- [ ] **Window Functions**
  - [ ] ROW_NUMBER, RANK, DENSE_RANK
  - [ ] LAG, LEAD, FIRST_VALUE, LAST_VALUE

### Phase 2: Beta Release

- [ ] **Advanced SQL**
  - [ ] UNION, INTERSECT, EXCEPT (set operations)
  - [ ] Common Table Expressions (WITH clause / CTEs)
  - [ ] Correlated subqueries
- [ ] **Prepared Statement Parameters**
  - [ ] Bind parameter support (`$1`, `?` placeholders)
- [ ] **Optimizer Improvements**
  - [ ] Cost-based join selection
  - [ ] Join reordering
  - [ ] Predicate pushdown optimizations
- [ ] **Savepoint Support**
  - [ ] Partial transaction rollback (`ROLLBACK TO SAVEPOINT`)

### Phase 3: Production Hardening

- [ ] **MVCC Completion**
  - [ ] Garbage collection for old versions
  - [ ] Snapshot isolation
- [ ] **Security**
  - [ ] User authentication
  - [ ] Role-based access control (RBAC)
  - [ ] Connection encryption (TLS)
- [ ] **Observability**
  - [ ] Tracing integration
  - [ ] Prometheus metrics export
  - [ ] EXPLAIN ANALYZE

### Phase 4: Advanced Features

- [ ] NUMA affinity for operators and buffer frames
- [ ] Parallel query execution
- [ ] Materialized views
- [ ] Stored procedures / user-defined functions

---

## 📄 License

Ferrite is dual-licensed under your choice of:

- **MIT License** ([LICENSE-MIT](LICENSE-MIT))
- **Apache License, Version 2.0** ([LICENSE-APACHE](LICENSE-APACHE))

---

## 🙏 Acknowledgments

Ferrite is inspired by the CMU 15-445/645 Database Systems course and builds on decades of database research. Special thanks to the Rust community for the excellent ecosystem of libraries that make this project possible.

---

<p align="center">
  <sub>Built with 🦀 by the Ferrite team</sub>
</p>
