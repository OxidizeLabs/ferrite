# Ferrite

A Rust-native database management system built on proven DBMS techniques. Ferrite combines decades of database research with Rust's memory safety and performance guarantees to deliver a reliable, high-performance storage engine.

## Philosophy

Databases are a solved problem at the architecture level. What wasn't solved was implementing them safely and efficiently. Rust changes that equation. Ferrite is built on proven database techniques—buffer management, B+ trees, write-ahead logging, MVCC—implemented with Rust's safety guarantees.

## Highlights

- Memory-safe by design (Rust, no unsafe in critical paths)
- Proven DBMS techniques with modern implementation
- Async execution with backpressure
- Robust concurrency control and crash recovery
- Deep observability: structured logs, tracing-ready, metrics hooks

## Getting Started

Prerequisites: Rust toolchain (stable), `cargo`.

Build and test:
```bash
cargo build
cargo test
```

Run simple SQL script:
```bash
cargo run --bin ferrite --features cli -- test.sql
```

## Architecture (Overview)

- Storage: page-based layout, buffer pool with LRU-K replacement
- Execution: vectorized and iterator operators, async task orchestration
- Concurrency: transaction manager, lock manager, deadlock detection
- Recovery: WAL, checkpoints, crash recovery
- SQL: parser, binder, optimizer, execution engine

## Roadmap (snapshot)

- NUMA affinity for operators and buffer frames
- Tracing integration and Prometheus metrics
- Enhanced cost-based optimizer
- Secondary index improvements and MVCC exploration

## Contributing

Open to issues and PRs. Please run `cargo fmt`, `cargo clippy`, and `cargo test` before submitting.

## License

Dual-licensed under MIT or Apache-2.0 at your option.
