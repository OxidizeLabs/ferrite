# Vision

Ferrite exists to be **the smallest, clearest, well-documented OLTP engine** that demonstrates **modern Rust concurrency and recovery design**.

## What “smallest and clearest” means here

- **Small surface area**: fewer features, fewer modes, fewer “magic” abstractions.
- **Readable internals**: simple data structures, explicit invariants, heavy use of diagrams and docs.
- **Teachable correctness**: concurrency + recovery are implemented in a way you can trace and test.

## Target audience

- **Rust engineers** who want to learn “how real databases work” by reading and modifying code.
- **DBMS learners** (15-445 style) who want an end-to-end system with modern Rust patterns.
- **Systems hackers** who want a baseline for experimenting with buffer management, locking, WAL, and recovery.

## Design principles

- **Evidence over claims**: every major guarantee should have a test and/or a runnable demo.
- **Correctness first**: keep behavior well-specified, even before it’s fast.
- **Explainable concurrency**: prefer explicit lock ordering and simple shared-state patterns (`Arc`, `parking_lot`) over cleverness.
- **Recovery is a first-class feature**: WAL and crash recovery aren’t optional “later” components.

## How to contribute to the vision

- Improve docs and invariants.
- Add small, scoped SQL features that are easy to test and explain.
- Add “evidence” (tests, demos, benchmarks) that make guarantees reproducible.
