# Evidence

Ferrite’s philosophy is **evidence over claims**: if we say “it works,” you should be able to reproduce that with a test or demo.

## What Ferrite claims (and how to verify)

### Crash recovery (WAL / ARIES-style)

- **Claim**: committed work survives a crash; uncommitted work is rolled back on restart.
- **Evidence**: the crash-recovery evidence demo:

```bash
make evidence-crash-recovery
```

This runs a small scenario that:
- creates a table (in a non-crashing setup run),
- commits one row,
- performs another write in an *uncommitted* transaction,
- forcefully crashes the process,
- restarts Ferrite and verifies only the committed row is visible.

### End-to-end SQL behavior

- **Evidence**: integration tests and SQL logic tests:

```bash
make test
make test-sqllogic
```

### Concurrency behavior

- **Evidence**: concurrency integration tests:

```bash
cargo test -p ferrite concurrency
```

## How to add new evidence

- If you add a feature, add at least one of:
  - a unit test for invariants,
  - an integration test for end-to-end behavior,
  - a demo script that proves the claim in a reproducible way.
