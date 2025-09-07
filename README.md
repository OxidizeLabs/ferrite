# TokamakDB (tkdb)

A Rust-native OLTP database focused on memory safety, high-concurrency, and first-class observability. TokamakDB combines a NUMA-aware, async execution engine with rigorous transaction control to deliver predictable low-latency throughput under contention.

## Highlights

- Memory-safe by design (Rust, no unsafe in critical paths)
- NUMA-aware scheduling and data locality
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
cargo run --bin tkdb --features cli -- test.sql
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
