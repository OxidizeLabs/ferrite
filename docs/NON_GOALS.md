# Non-goals (Scope Guardrails)

Ferrite optimizes for **clarity, smallness, and reproducible correctness evidence**. That requires saying “no” to a lot of reasonable ideas.

## Explicit non-goals (for now)

- **Distributed database features**
  - No replication, sharding, Raft/Paxos, partition tolerance, multi-region, etc.
- **PostgreSQL compatibility**
  - No Postgres wire protocol, no “drop-in replacement” promise.
- **Full SQL coverage**
  - We implement what’s needed to demonstrate OLTP fundamentals, not every corner of the SQL standard.
- **Production-hardening guarantees**
  - No promise of backwards-compatible storage formats, operational tooling, or hardened security posture yet.
- **Benchmark-driven development**
  - Performance work is welcome, but only when it keeps the system explainable and testable.

## What we *do* prioritize instead

- A small set of features that are **implemented clearly** and **proved via tests/demos**:
  - Concurrency control
  - WAL + crash recovery
  - Simple indexing and storage layout
  - A minimal SQL pipeline to drive end-to-end behavior
