# Ferrite Architecture

This document provides a comprehensive overview of Ferrite's architecture, a Rust-native database management system built on proven DBMS techniques.

## System Overview

```
                              ┌─────────────────────────────────────────────────────────────────────────┐
                              │                           Ferrite DBMS                                  │
                              │                                                                         │
┌─────────────┐               │   ┌─────────────────────────────────────────────────────────────────┐   │
│   Client    │◄──────────────┼──►│                        Network Layer                            │   │
│  (ferrite   │   TCP/bincode │   │   ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │   │
│   client)   │               │   │   │ ServerHandle │  │  Connection  │  │  Protocol (Request/  │  │   │
└─────────────┘               │   │   │    (TCP)     │  │   Handler    │  │    Response/Query)   │  │   │
                              │   │   └──────────────┘  └──────────────┘  └──────────────────────┘  │   │
┌─────────────┐               │   └─────────────────────────────────────────────────────────────────┘   │
│    CLI      │◄──────────────┼──►                                │                                     │
│  (ferrite   │    Direct     │                                   ▼                                     │
│    cli)     │               │   ┌─────────────────────────────────────────────────────────────────┐   │
└─────────────┘               │   │                         DBInstance                              │   │
                              │   │                                                                 │   │
                              │   │   ┌─────────────────────────────────────────────────────────┐   │   │
                              │   │   │                    SQL Layer                            │   │   │
                              │   │   │                                                         │   │   │
                              │   │   │   SQL Query                                             │   │   │
                              │   │   │       │                                                 │   │   │
                              │   │   │       ▼                                                 │   │   │
                              │   │   │   ┌──────────┐    ┌──────────┐    ┌──────────────────┐  │   │   │
                              │   │   │   │  Parser  │───►│  Binder  │───►│    Optimizer     │  │   │   │
                              │   │   │   │(sqlparser)│    │          │    │                  │  │   │   │
                              │   │   │   └──────────┘    └──────────┘    └──────────────────┘  │   │   │
                              │   │   │                                            │            │   │   │
                              │   │   │                                            ▼            │   │   │
                              │   │   │                                   ┌──────────────────┐  │   │   │
                              │   │   │                                   │     Planner      │  │   │   │
                              │   │   │                                   │  (Logical Plan)  │  │   │   │
                              │   │   │                                   └──────────────────┘  │   │   │
                              │   │   │                                            │            │   │   │
                              │   │   │                                            ▼            │   │   │
                              │   │   │   ┌─────────────────────────────────────────────────┐   │   │   │
                              │   │   │   │              Execution Engine                   │   │   │   │
                              │   │   │   │                                                 │   │   │   │
                              │   │   │   │   ┌────────────────────────────────────────┐    │   │   │   │
                              │   │   │   │   │              Executors                 │    │   │   │   │
                              │   │   │   │   │  SeqScan, Filter, Projection, Join,   │    │   │   │   │
                              │   │   │   │   │  Aggregation, Sort, Limit, TopN, ...  │    │   │   │   │
                              │   │   │   │   └────────────────────────────────────────┘    │   │   │   │
                              │   │   │   │                                                 │   │   │   │
                              │   │   │   │   ┌────────────────────────────────────────┐    │   │   │   │
                              │   │   │   │   │            Expressions                 │    │   │   │   │
                              │   │   │   │   │  Arithmetic, Comparison, Boolean,     │    │   │   │   │
                              │   │   │   │   │  Aggregate, Window, Case, Cast, ...   │    │   │   │   │
                              │   │   │   │   └────────────────────────────────────────┘    │   │   │   │
                              │   │   │   └─────────────────────────────────────────────────┘   │   │   │
                              │   │   └─────────────────────────────────────────────────────────┘   │   │
                              │   │                         │                                       │   │
                              │   │                         ▼                                       │   │
                              │   │   ┌───────────────────────────────────────────────────────┐     │   │
                              │   │   │                  Concurrency Control                  │     │   │
                              │   │   │                                                       │     │   │
                              │   │   │   ┌─────────────────┐    ┌───────────────────────┐    │     │   │
                              │   │   │   │   Transaction   │    │     Lock Manager      │    │     │   │
                              │   │   │   │     Manager     │    │ (S/X/IS/IX/SIX locks) │    │     │   │
                              │   │   │   └─────────────────┘    └───────────────────────┘    │     │   │
                              │   │   │           │                         │                 │     │   │
                              │   │   │           └─────────────────────────┘                 │     │   │
                              │   │   │                         │                             │     │   │
                              │   │   │                         ▼                             │     │   │
                              │   │   │              ┌─────────────────────┐                  │     │   │
                              │   │   │              │      Watermark      │                  │     │   │
                              │   │   │              │  (MVCC timestamps)  │                  │     │   │
                              │   │   │              └─────────────────────┘                  │     │   │
                              │   │   └───────────────────────────────────────────────────────┘     │   │
                              │   │                         │                                       │   │
                              │   │                         ▼                                       │   │
                              │   │   ┌───────────────────────────────────────────────────────┐     │   │
                              │   │   │                     Catalog                           │     │   │
                              │   │   │  ┌──────────┐  ┌──────────┐  ┌────────┐  ┌─────────┐  │     │   │
                              │   │   │  │ Database │  │  Schema  │  │ Column │  │Constraint│ │     │   │
                              │   │   │  └──────────┘  └──────────┘  └────────┘  └─────────┘  │     │   │
                              │   │   └───────────────────────────────────────────────────────┘     │   │
                              │   └─────────────────────────────────────────────────────────────────┘   │
                              │                                 │                                       │
                              │                                 ▼                                       │
                              │   ┌─────────────────────────────────────────────────────────────────┐   │
                              │   │                       Storage Layer                             │   │
                              │   │                                                                 │   │
                              │   │   ┌─────────────────────────────────────────────────────────┐   │   │
                              │   │   │                   Buffer Pool                           │   │   │
                              │   │   │                                                         │   │   │
                              │   │   │   ┌───────────────────┐    ┌─────────────────────────┐  │   │   │
                              │   │   │   │ BufferPoolManager │    │    LRU-K Replacer       │  │   │   │
                              │   │   │   │     (Async)       │    │ (Scan-Resistant Cache)  │  │   │   │
                              │   │   │   └───────────────────┘    └─────────────────────────┘  │   │   │
                              │   │   │                                                         │   │   │
                              │   │   │   ┌────────────────────────────────────────────────┐    │   │   │
                              │   │   │   │                    Frames                      │    │   │   │
                              │   │   │   │  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐  │    │   │   │
                              │   │   │   │  │Page 0│ │Page 1│ │Page 2│ │ ...  │ │Page N│  │    │   │   │
                              │   │   │   │  └──────┘ └──────┘ └──────┘ └──────┘ └──────┘  │    │   │   │
                              │   │   │   └────────────────────────────────────────────────┘    │   │   │
                              │   │   └─────────────────────────────────────────────────────────┘   │   │
                              │   │                                 │                               │   │
                              │   │   ┌──────────────────┬──────────┴───────────┬─────────────────┐ │   │
                              │   │   │                  │                      │                 │ │   │
                              │   │   ▼                  ▼                      ▼                 ▼ │   │
                              │   │ ┌──────────────┐ ┌───────────┐ ┌────────────────┐ ┌──────────┐  │   │
                              │   │ │  Table Heap  │ │   Index   │ │   Page Types   │ │ Recovery │  │   │
                              │   │ │              │ │           │ │                │ │          │  │   │
                              │   │ │ ┌──────────┐ │ │ ┌───────┐ │ │ ┌────────────┐ │ │ ┌──────┐ │  │   │
                              │   │ │ │  Tuples  │ │ │ │B+ Tree│ │ │ │Table Page  │ │ │ │ WAL  │ │  │   │
                              │   │ │ └──────────┘ │ │ └───────┘ │ │ │Index Page  │ │ │ │      │ │  │   │
                              │   │ │ ┌──────────┐ │ │ ┌───────┐ │ │ │Header Page │ │ │ └──────┘ │  │   │
                              │   │ │ │ Records  │ │ │ │ Hash  │ │ │ │Directory   │ │ │ ┌──────┐ │  │   │
                              │   │ │ └──────────┘ │ │ │ Table │ │ │ │  Page      │ │ │ │ Log  │ │  │   │
                              │   │ └──────────────┘ │ └───────┘ │ │ │BTree Page  │ │ │ │      │ │  │   │
                              │   │                  └───────────┘ │ └────────────┘ │ │ └──────┘ │  │   │
                              │   │                                └────────────────┘ │ ┌──────┐ │  │   │
                              │   │                                                   │ │Chkpt │ │  │   │
                              │   │                                                   │ └──────┘ │  │   │
                              │   │                                                   └──────────┘  │   │
                              │   │                                 │                               │   │
                              │   │                                 ▼                               │   │
                              │   │   ┌─────────────────────────────────────────────────────────┐   │   │
                              │   │   │                    Disk Layer                           │   │   │
                              │   │   │                                                         │   │   │
                              │   │   │   ┌───────────────────────────────────────────────────┐ │   │   │
                              │   │   │   │              Async Disk Manager                   │ │   │   │
                              │   │   │   │                                                   │ │   │   │
                              │   │   │   │   ┌────────────┐   ┌────────────┐   ┌──────────┐  │ │   │   │
                              │   │   │   │   │ IO Engine  │   │ IO Queues  │   │Completion│  │ │   │   │
                              │   │   │   │   │ (Read/     │   │ (Priority  │   │ Tracker  │  │ │   │   │
                              │   │   │   │   │  Write)    │   │  Ordered)  │   │          │  │ │   │   │
                              │   │   │   │   └────────────┘   └────────────┘   └──────────┘  │ │   │   │
                              │   │   │   │                                                   │ │   │   │
                              │   │   │   │   ┌────────────────────────────────────────────┐  │ │   │   │
                              │   │   │   │   │                  Cache Layer               │  │ │   │   │
                              │   │   │   │   │   LRU │ LRU-K │ LFU │ Heap-LFU │ FIFO      │  │ │   │   │
                              │   │   │   │   └────────────────────────────────────────────┘  │ │   │   │
                              │   │   │   └───────────────────────────────────────────────────┘ │   │   │
                              │   │   │                                                         │   │   │
                              │   │   │   ┌───────────────────────────────────────────────────┐ │   │   │
                              │   │   │   │                   Direct I/O                      │ │   │   │
                              │   │   │   │   O_DIRECT │ Aligned Buffers │ Bypass OS Cache    │ │   │   │
                              │   │   │   └───────────────────────────────────────────────────┘ │   │   │
                              │   │   └─────────────────────────────────────────────────────────┘   │   │
                              │   └─────────────────────────────────────────────────────────────────┘   │
                              │                                 │                                       │
                              └─────────────────────────────────┼───────────────────────────────────────┘
                                                                │
                                                                ▼
                                                     ┌──────────────────────┐
                                                     │      File System     │
                                                     │  *.db  *.log  *.wal  │
                                                     └──────────────────────┘
```

## Module Overview

| Module        | Path               | Description                                           |
|---------------|---------------------|-------------------------------------------------------|
| `buffer`      | `src/buffer/`      | Buffer pool management and page caching               |
| `catalog`     | `src/catalog/`     | Database metadata (tables, schemas, columns)          |
| `cli`         | `src/cli.rs`       | Interactive command-line interface                    |
| `client`      | `src/client/`      | Network client for remote database access             |
| `common`      | `src/common/`      | Shared utilities, config, errors, RID, logging        |
| `concurrency` | `src/concurrency/` | Transaction management and locking                    |
| `container`   | `src/container/`   | Hash table implementations (disk-based)               |
| `network`     | `src/network/`     | Wire protocol codec and packet handling               |
| `recovery`    | `src/recovery/`    | WAL, checkpoints, crash recovery                      |
| `server`      | `src/server/`      | TCP server and connection handling                    |
| `sql`         | `src/sql/`         | Query processing (planner, optimizer, execution)      |
| `storage`     | `src/storage/`     | Disk I/O, pages, indexes, table heaps                 |
| `types_db`    | `src/types_db/`    | Database type system (INTEGER, VARCHAR, etc.)         |

---

## Layer Details

### 1. Access Layer

The access layer provides interfaces for database interaction:

```
┌───────────────────────────────────────────────────────────────────────────┐
│                              Access Layer                                 │
│                                                                           │
│   ┌──────────────────────┐  ┌───────────────────┐  ┌──────────────────┐   │
│   │       Server         │  │      Client       │  │       CLI        │   │
│   │                      │  │                   │  │                  │   │
│   │  • TCP listener      │  │  • Connect to     │  │  • Interactive   │   │
│   │  • Connection pool   │  │    remote server  │  │    shell         │   │
│   │  • Request routing   │  │  • Prepared       │  │  • Embedded mode │   │
│   │  • bincode protocol  │  │    statements     │  │  • File scripts  │   │
│   │                      │  │  • Result display │  │                  │   │
│   └──────────────────────┘  └───────────────────┘  └──────────────────┘   │
│                                                                           │
│   Entry Points:                                                           │
│   • ferrite server -P 5432      (start server)                            │
│   • ferrite client -H localhost (connect to server)                       │
│   • ferrite cli                 (embedded interactive mode)               │
└───────────────────────────────────────────────────────────────────────────┘
```

#### Server (`src/server/`)

| Component       | File                | Purpose                                     |
|-----------------|---------------------|---------------------------------------------|
| `ServerHandle`  | `server_impl.rs`    | TCP server lifecycle management             |
| `handle_connection` | `connection.rs` | Per-connection request processing           |
| `DatabaseRequest/Response` | `protocol.rs` | Wire protocol message types          |
| `ServerConfig`  | `config.rs`         | Server configuration (port, limits)         |

#### Client (`src/client/`)

| Component        | File              | Purpose                                     |
|------------------|-------------------|---------------------------------------------|
| `DatabaseClient` | `client_impl.rs`  | Connect, execute queries, manage sessions   |

#### Wire Protocol

- **Serialization**: `bincode` (compact binary format)
- **Framing**: Length-prefixed messages
- **Message Types**: `Execute`, `Prepare`, `ExecutePrepared`, `Ping`, `Disconnect`

---

### 2. SQL Processing Layer

The SQL layer transforms text queries into executable plans:

```
┌───────────────────────────────────────────────────────────────────────────┐
│                           SQL Processing Pipeline                         │
│                                                                           │
│   "SELECT * FROM users WHERE id = 1"                                      │
│                      │                                                    │
│                      ▼                                                    │
│   ┌──────────────────────────────────────────────────────────────────┐    │
│   │  Parser (sqlparser crate)                                        │    │
│   │  • Tokenize SQL                                                  │    │
│   │  • Build AST                                                     │    │
│   │  • Validate syntax                                               │    │
│   └──────────────────────────────────────────────────────────────────┘    │
│                      │                                                    │
│                      ▼  AST (Abstract Syntax Tree)                        │
│   ┌──────────────────────────────────────────────────────────────────┐    │
│   │  Binder                                                          │    │
│   │  • Resolve table/column names                                    │    │
│   │  • Check catalog for existence                                   │    │
│   │  • Bind expressions to schema                                    │    │
│   │  • Type checking                                                 │    │
│   └──────────────────────────────────────────────────────────────────┘    │
│                      │                                                    │
│                      ▼  Bound Statement                                   │
│   ┌──────────────────────────────────────────────────────────────────┐    │
│   │  Planner                                                         │    │
│   │  • Generate logical plan                                         │    │
│   │  • Plan nodes: SeqScan, Filter, Project, Join, Sort, ...         │    │
│   └──────────────────────────────────────────────────────────────────┘    │
│                      │                                                    │
│                      ▼  Logical Plan                                      │
│   ┌──────────────────────────────────────────────────────────────────┐    │
│   │  Optimizer                                                       │    │
│   │  • Predicate pushdown                                            │    │
│   │  • Join reordering                                               │    │
│   │  • Index selection                                               │    │
│   │  • Cost-based optimization                                       │    │
│   └──────────────────────────────────────────────────────────────────┘    │
│                      │                                                    │
│                      ▼  Optimized Plan                                    │
│   ┌──────────────────────────────────────────────────────────────────┐    │
│   │  Execution Engine                                                │    │
│   │  • Build executor tree                                           │    │
│   │  • Volcano/Iterator model                                        │    │
│   │  • Execute and produce tuples                                    │    │
│   └──────────────────────────────────────────────────────────────────┘    │
│                      │                                                    │
│                      ▼                                                    │
│   Result Tuples: [(1, "Alice", ...)]                                      │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

#### Planner (`src/sql/planner/`)

| Component      | File                | Purpose                                     |
|----------------|---------------------|---------------------------------------------|
| `QueryPlanner` | `query_planner.rs`  | Convert bound statements to logical plans   |
| `PlanBuilder`  | `plan_builder.rs`   | Builder pattern for plan construction       |
| `LogicalPlan`  | `logical_plan.rs`   | Plan node definitions                       |

#### Optimizer (`src/sql/optimizer/`)

| Component   | File               | Purpose                                        |
|-------------|---------------------|-----------------------------------------------|
| `Optimizer` | `optimizer_impl.rs` | Apply optimization rules to logical plans     |

#### Execution Engine (`src/sql/execution/`)

**Executors** (`src/sql/execution/executors/`):

| Executor                 | Description                                    |
|--------------------------|------------------------------------------------|
| `SeqScanExecutor`        | Full table scan                                |
| `IndexScanExecutor`      | Index-based lookup                             |
| `FilterExecutor`         | WHERE clause evaluation                        |
| `ProjectionExecutor`     | SELECT column projection                       |
| `HashJoinExecutor`       | Hash-based equi-join                           |
| `NestedLoopJoinExecutor` | Nested loop join (cross/theta joins)           |
| `AggregationExecutor`    | GROUP BY with aggregate functions              |
| `SortExecutor`           | ORDER BY implementation                        |
| `LimitExecutor`          | LIMIT clause                                   |
| `TopNExecutor`           | Optimized ORDER BY + LIMIT                     |
| `WindowExecutor`         | Window functions (ROW_NUMBER, RANK, etc.)      |
| `InsertExecutor`         | INSERT INTO execution                          |
| `UpdateExecutor`         | UPDATE execution                               |
| `DeleteExecutor`         | DELETE execution                               |
| `CreateTableExecutor`    | CREATE TABLE DDL                               |
| `CreateIndexExecutor`    | CREATE INDEX DDL                               |

**Expressions** (`src/sql/execution/expressions/`):

| Category    | Examples                                              |
|-------------|-------------------------------------------------------|
| Arithmetic  | Add, Subtract, Multiply, Divide, Modulo               |
| Comparison  | Equal, NotEqual, LessThan, GreaterThan, Between       |
| Boolean     | And, Or, Not, IsNull, IsNotNull                       |
| Aggregate   | Count, Sum, Avg, Min, Max                             |
| Window      | RowNumber, Rank, DenseRank, Lead, Lag                 |
| String      | Concat, Substring, Upper, Lower, Trim, Length         |
| Type        | Cast, Coalesce, NullIf                                |

---

### 3. Concurrency Control Layer

Ferrite implements pessimistic concurrency control with two-phase locking:

```
┌───────────────────────────────────────────────────────────────────────────┐
│                          Concurrency Control                              │
│                                                                           │
│   ┌─────────────────────────────────────────────────────────────────┐     │
│   │                    Transaction Manager                          │     │
│   │                                                                 │     │
│   │   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐       │     │
│   │   │ Transaction │     │ Transaction │     │ Transaction │       │     │
│   │   │   (Txn 1)   │     │   (Txn 2)   │     │   (Txn 3)   │       │     │
│   │   │             │     │             │     │             │       │     │
│   │   │ state: GROW │     │ state: SHRINK│    │ state: COMMIT│      │     │
│   │   │ isolation:  │     │ isolation:  │     │ isolation:  │       │     │
│   │   │  SERIALIZABLE│    │  REPEATABLE │     │  READ_COMMIT│       │     │
│   │   └─────────────┘     └─────────────┘     └─────────────┘       │     │
│   │                                                                 │     │
│   │   Transaction States:                                           │     │
│   │   GROWING → SHRINKING → COMMITTED/ABORTED                       │     │
│   │                                                                 │     │
│   └─────────────────────────────────────────────────────────────────┘     │
│                                    │                                      │
│                                    ▼                                      │
│   ┌─────────────────────────────────────────────────────────────────┐     │
│   │                       Lock Manager                              │     │
│   │                                                                 │     │
│   │   Lock Modes:                                                   │     │
│   │   ┌────────────────────────────────────────────────────────┐    │     │
│   │   │  S  (Shared)        - Read lock                        │    │     │
│   │   │  X  (Exclusive)     - Write lock                       │    │     │
│   │   │  IS (Intent Shared) - Intent to read children          │    │     │
│   │   │  IX (Intent Exclusive) - Intent to write children      │    │     │
│   │   │  SIX (Shared + Intent Exclusive)                       │    │     │
│   │   └────────────────────────────────────────────────────────┘    │     │
│   │                                                                 │     │
│   │   Lock Compatibility Matrix:                                    │     │
│   │   ┌─────┬─────┬─────┬─────┬─────┬─────┐                         │     │
│   │   │     │  S  │  X  │ IS  │ IX  │ SIX │                         │     │
│   │   ├─────┼─────┼─────┼─────┼─────┼─────┤                         │     │
│   │   │  S  │  ✓  │  ✗  │  ✓  │  ✗  │  ✗  │                         │     │
│   │   │  X  │  ✗  │  ✗  │  ✗  │  ✗  │  ✗  │                         │     │
│   │   │ IS  │  ✓  │  ✗  │  ✓  │  ✓  │  ✓  │                         │     │
│   │   │ IX  │  ✗  │  ✗  │  ✓  │  ✓  │  ✗  │                         │     │
│   │   │ SIX │  ✗  │  ✗  │  ✓  │  ✗  │  ✗  │                         │     │
│   │   └─────┴─────┴─────┴─────┴─────┴─────┘                         │     │
│   │                                                                 │     │
│   │   Deadlock Detection: Wait-for graph cycle detection           │     │
│   │                                                                 │     │
│   └─────────────────────────────────────────────────────────────────┘     │
│                                    │                                      │
│                                    ▼                                      │
│   ┌─────────────────────────────────────────────────────────────────┐     │
│   │                         Watermark                               │     │
│   │                                                                 │     │
│   │   Tracks active transaction timestamps for MVCC visibility      │     │
│   │   • Low watermark: oldest active transaction                    │     │
│   │   • Used for garbage collection of old versions                 │     │
│   │                                                                 │     │
│   └─────────────────────────────────────────────────────────────────┘     │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

#### Components (`src/concurrency/`)

| Component            | File                            | Purpose                           |
|----------------------|---------------------------------|-----------------------------------|
| `Transaction`        | `transaction.rs`                | Transaction state and metadata    |
| `TransactionManager` | `transaction_manager.rs`        | Begin, commit, abort transactions |
| `LockManager`        | `lock_manager.rs`               | Acquire/release locks             |
| `Watermark`          | `watermark.rs`                  | Track active transaction IDs      |

---

### 4. Storage Layer

The storage layer manages all disk I/O and in-memory page caching:

```
┌───────────────────────────────────────────────────────────────────────────┐
│                              Storage Layer                                │
│                                                                           │
│   ┌─────────────────────────────────────────────────────────────────┐     │
│   │                      Buffer Pool Manager                        │     │
│   │                                                                 │     │
│   │   ┌─────────────────────────────────────────────────────────┐   │     │
│   │   │                    Frame Pool                           │   │     │
│   │   │                                                         │   │     │
│   │   │   ┌─────────┐ ┌─────────┐ ┌─────────┐     ┌─────────┐   │   │     │
│   │   │   │ Frame 0 │ │ Frame 1 │ │ Frame 2 │ ... │ Frame N │   │   │     │
│   │   │   │         │ │         │ │         │     │         │   │   │     │
│   │   │   │ PageID:3│ │ PageID:7│ │ PageID:1│     │ PageID:9│   │   │     │
│   │   │   │ Dirty:✓ │ │ Dirty:✗ │ │ Dirty:✓ │     │ Dirty:✗ │   │   │     │
│   │   │   │ Pin: 2  │ │ Pin: 0  │ │ Pin: 1  │     │ Pin: 0  │   │   │     │
│   │   │   └─────────┘ └─────────┘ └─────────┘     └─────────┘   │   │     │
│   │   │                                                         │   │     │
│   │   └─────────────────────────────────────────────────────────┘   │     │
│   │                              │                                  │     │
│   │   API:                       ▼                                  │     │
│   │   • fetch_page(page_id) → PageGuard                             │     │
│   │   • new_page() → PageGuard                                      │     │
│   │   • unpin_page(page_id, is_dirty)                               │     │
│   │   • flush_page(page_id)                                         │     │
│   │   • flush_all_pages()                                           │     │
│   │                                                                 │     │
│   └─────────────────────────────────────────────────────────────────┘     │
│                              │                                            │
│                              ▼                                            │
│   ┌─────────────────────────────────────────────────────────────────┐     │
│   │                      LRU-K Replacer                             │     │
│   │                                                                 │     │
│   │   • K-distance: time since Kth most recent access               │     │
│   │   • Evicts frame with largest backward K-distance               │     │
│   │   • Scan-resistant: prevents sequential scans from              │     │
│   │     evicting frequently-accessed pages                          │     │
│   │   • Typical K=2 (uses 2nd-to-last access time)                  │     │
│   │                                                                 │     │
│   │   Operations:                                                   │     │
│   │   • record_access(frame_id) - Record page access                │     │
│   │   • evict() → Option<FrameId> - Get victim frame                │     │
│   │   • set_evictable(frame_id, evictable) - Pin/unpin              │     │
│   │   • remove(frame_id) - Remove from tracking                     │     │
│   │                                                                 │     │
│   └─────────────────────────────────────────────────────────────────┘     │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

#### Buffer Pool (`src/buffer/`)

| Component                 | File                           | Purpose                          |
|---------------------------|--------------------------------|----------------------------------|
| `BufferPoolManagerAsync`  | `buffer_pool_manager_async.rs` | Async page fetch/write           |
| `LRUKReplacer`            | `lru_k_replacer.rs`            | Scan-resistant page replacement  |

#### Page Types (`src/storage/page/`)

```
┌───────────────────────────────────────────────────────────────────────────┐
│                              Page Layout                                  │
│                                                                           │
│   Page Size: 4096 bytes (configurable)                                    │
│                                                                           │
│   ┌─────────────────────────────────────────────────────────────────┐     │
│   │                      Page Header (fixed)                        │     │
│   │                                                                 │     │
│   │   ┌──────────────┬───────────────┬──────────────┬────────────┐  │     │
│   │   │   Page ID    │   Page Type   │    LSN       │  Checksum  │  │     │
│   │   │   (4 bytes)  │   (1 byte)    │  (8 bytes)   │  (4 bytes) │  │     │
│   │   └──────────────┴───────────────┴──────────────┴────────────┘  │     │
│   │                                                                 │     │
│   └─────────────────────────────────────────────────────────────────┘     │
│                                                                           │
│   Page Types:                                                             │
│   ┌────────────────────────────────────────────────────────────────┐      │
│   │ TablePage    │ Slotted page for tuple storage                  │      │
│   │ BTreePage    │ B+ tree internal/leaf nodes                     │      │
│   │ HashPage     │ Hash table buckets                              │      │
│   │ HeaderPage   │ Database/table metadata                         │      │
│   │ DirectoryPage│ Page directory for hash tables                  │      │
│   │ OverflowPage │ Variable-length data overflow                   │      │
│   └────────────────────────────────────────────────────────────────┘      │
│                                                                           │
│   Table Page (Slotted):                                                   │
│   ┌─────────────────────────────────────────────────────────────────┐     │
│   │  Header  │  Slot Array  │     Free Space     │     Tuples      │     │
│   │          │  ────────►   │                    │   ◄────────     │     │
│   │          │  [s1][s2][s3]│ ←── grows ──►      │  [T3][T2][T1]   │     │
│   └─────────────────────────────────────────────────────────────────┘     │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

| Component    | File                          | Purpose                             |
|--------------|-------------------------------|-------------------------------------|
| `Page`       | `page_impl.rs`                | Base page with header operations    |
| `PageGuard`  | `page_guard.rs`               | RAII guard for pinned pages         |
| `TablePage`  | `page_types/table_page.rs`    | Tuple storage with slots            |
| `BTreePage`  | `page_types/btree_*.rs`       | B+ tree nodes                       |

#### Table Heap (`src/storage/table/`)

| Component              | File                         | Purpose                            |
|------------------------|------------------------------|------------------------------------|
| `TableHeap`            | `table_heap.rs`              | Collection of table pages          |
| `TransactionalTableHeap` | `transactional_table_heap.rs` | MVCC-aware table operations     |
| `Tuple`                | `tuple.rs`                   | Row data structure                 |
| `Record`               | `record.rs`                  | Tuple with transaction metadata    |

#### Index Structures (`src/storage/index/`)

```
┌───────────────────────────────────────────────────────────────────────────┐
│                           B+ Tree Index                                   │
│                                                                           │
│                        ┌─────────────┐                                    │
│                        │  Root Node  │                                    │
│                        │ [10 | 20]   │                                    │
│                        └──────┬──────┘                                    │
│                    ┌──────────┼──────────┐                                │
│                    ▼          ▼          ▼                                │
│              ┌─────────┐ ┌─────────┐ ┌─────────┐                          │
│              │Internal │ │Internal │ │Internal │                          │
│              │ [5|8]   │ │[12|15]  │ │[22|25]  │                          │
│              └────┬────┘ └────┬────┘ └────┬────┘                          │
│              ┌────┴────┐ ┌────┴────┐ ┌────┴────┐                          │
│              ▼    ▼    ▼ ▼    ▼    ▼ ▼    ▼    ▼                          │
│           ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐                       │
│           │Leaf│→│Leaf│→│Leaf│→│Leaf│→│Leaf│→│Leaf│                       │
│           │1,3,│ │5,6,│ │10, │ │15, │ │22, │ │25, │                       │
│           │4   │ │8   │ │12  │ │18  │ │23  │ │30  │                       │
│           └────┘ └────┘ └────┘ └────┘ └────┘ └────┘                       │
│                                                                           │
│   Properties:                                                             │
│   • Self-balancing                                                        │
│   • O(log n) search, insert, delete                                       │
│   • Leaf nodes linked for range scans                                     │
│   • Latch crabbing for concurrent access                                  │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

| Component             | File                        | Purpose                           |
|-----------------------|-----------------------------|-----------------------------------|
| `BPlusTree`           | `b_plus_tree.rs`            | B+ tree implementation            |
| `BPlusTreeIndex`      | `b_plus_tree_index.rs`      | Index interface wrapper           |
| `ExtendableHashTableIndex` | `extendable_hash_table_index.rs` | Hash index              |
| `LatchCrabbing`       | `latch_crabbing.rs`         | Concurrent B+ tree traversal      |

#### Disk Manager (`src/storage/disk/`)

```
┌───────────────────────────────────────────────────────────────────────────┐
│                          Async Disk Manager                               │
│                                                                           │
│   ┌─────────────────────────────────────────────────────────────────┐     │
│   │                      AsyncIOEngine                              │     │
│   │                                                                 │     │
│   │   submit_read(page_id) ──────────►  ┌───────────────┐           │     │
│   │   submit_write(page_id, data) ───►  │   IO Queue    │           │     │
│   │                                     │  (Priority)   │           │     │
│   │                                     │               │           │     │
│   │   ┌──────────────────────────────►  │  HIGH: [...]  │           │     │
│   │   │                                 │  NORMAL:[...] │           │     │
│   │   │                                 │  LOW:  [...]  │           │     │
│   │   │                                 └───────┬───────┘           │     │
│   │   │                                         │                   │     │
│   │   │                                         ▼                   │     │
│   │   │                               ┌───────────────────┐         │     │
│   │   │                               │  IO Executor      │         │     │
│   │   │                               │                   │         │     │
│   │   │                               │  ┌─────────────┐  │         │     │
│   │   │                               │  │ Direct I/O  │  │         │     │
│   │   │                               │  │ (O_DIRECT)  │  │         │     │
│   │   │                               │  └─────────────┘  │         │     │
│   │   │                               │  ┌─────────────┐  │         │     │
│   │   │                               │  │ Buffered IO │  │         │     │
│   │   │                               │  │ (fallback)  │  │         │     │
│   │   │                               │  └─────────────┘  │         │     │
│   │   │                               └─────────┬─────────┘         │     │
│   │   │                                         │                   │     │
│   │   │                                         ▼                   │     │
│   │   │                               ┌───────────────────┐         │     │
│   │   │                               │CompletionTracker  │         │     │
│   │   │                               │                   │         │     │
│   │   └───────── await result ◄────── │  • Track status   │         │     │
│   │                                   │  • Notify callers │         │     │
│   │                                   │  • Collect metrics│         │     │
│   │                                   └───────────────────┘         │     │
│   │                                                                 │     │
│   └─────────────────────────────────────────────────────────────────┘     │
│                                                                           │
│   Cache Layer (multiple policies):                                        │
│   ┌──────────────────────────────────────────────────────────────────┐    │
│   │  LRU  │  LRU-K  │  LFU  │  Heap-LFU  │  FIFO                     │    │
│   │       │ (scan-  │       │ (O(log n)  │  (simple,                 │    │
│   │       │  resist)│       │  eviction) │   predictable)            │    │
│   └──────────────────────────────────────────────────────────────────┘    │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

| Component           | File                              | Purpose                          |
|---------------------|-----------------------------------|----------------------------------|
| `AsyncIOEngine`     | `async_disk/io/io_impl.rs`        | Main async I/O coordinator       |
| `IOQueueManager`    | `async_disk/io/queue.rs`          | Priority-based operation queuing |
| `IOOperationExecutor` | `async_disk/io/executor.rs`     | Execute read/write operations    |
| `CompletionTracker` | `async_disk/io/completion.rs`     | Track async operation status     |
| `DirectIO`          | `direct_io.rs`                    | O_DIRECT file operations         |

---

### 5. Recovery Layer

The recovery layer ensures durability and crash recovery:

```
┌───────────────────────────────────────────────────────────────────────────┐
│                            Recovery Layer                                 │
│                                                                           │
│   ┌─────────────────────────────────────────────────────────────────┐     │
│   │                    Write-Ahead Logging (WAL)                    │     │
│   │                                                                 │     │
│   │   Principle: Log BEFORE modifying data                          │     │
│   │                                                                 │     │
│   │   ┌───────────────────────────────────────────────────────┐     │     │
│   │   │                    Log Record Types                   │     │     │
│   │   │                                                       │     │     │
│   │   │  BEGIN(txn_id)     - Transaction start                │     │     │
│   │   │  COMMIT(txn_id)    - Transaction commit               │     │     │
│   │   │  ABORT(txn_id)     - Transaction abort                │     │     │
│   │   │  UPDATE(txn_id, page_id, offset, old, new)            │     │     │
│   │   │  INSERT(txn_id, page_id, slot, data)                  │     │     │
│   │   │  DELETE(txn_id, page_id, slot, data)                  │     │     │
│   │   │  CHECKPOINT        - Consistency point                │     │     │
│   │   │                                                       │     │     │
│   │   └───────────────────────────────────────────────────────┘     │     │
│   │                                                                 │     │
│   │   Log Structure:                                                │     │
│   │   ┌──────┬──────┬──────┬──────┬──────┬──────┬──────┬──────┐     │     │
│   │   │ LSN  │ LSN  │ LSN  │ LSN  │ LSN  │ LSN  │ LSN  │ ...  │     │     │
│   │   │  1   │  2   │  3   │  4   │  5   │  6   │  7   │      │     │     │
│   │   │BEGIN │INSERT│UPDATE│BEGIN │COMMIT│UPDATE│CHKPT │      │     │     │
│   │   │ T1   │ T1   │ T1   │ T2   │ T1   │ T2   │      │      │     │     │
│   │   └──────┴──────┴──────┴──────┴──────┴──────┴──────┴──────┘     │     │
│   │                                                                 │     │
│   └─────────────────────────────────────────────────────────────────┘     │
│                                                                           │
│   ┌─────────────────────────────────────────────────────────────────┐     │
│   │                      ARIES Recovery                             │     │
│   │                                                                 │     │
│   │   Phase 1: ANALYSIS                                             │     │
│   │   • Scan log from last checkpoint                               │     │
│   │   • Build active transaction table (ATT)                        │     │
│   │   • Build dirty page table (DPT)                                │     │
│   │                                                                 │     │
│   │   Phase 2: REDO                                                 │     │
│   │   • Re-apply all logged changes                                 │     │
│   │   • Restore database to crash state                             │     │
│   │                                                                 │     │
│   │   Phase 3: UNDO                                                 │     │
│   │   • Roll back uncommitted transactions                          │     │
│   │   • Process in reverse LSN order                                │     │
│   │                                                                 │     │
│   └─────────────────────────────────────────────────────────────────┘     │
│                                                                           │
│   ┌─────────────────────────────────────────────────────────────────┐     │
│   │                     Checkpoint Manager                          │     │
│   │                                                                 │     │
│   │   • Periodic checkpoints                                        │     │
│   │   • Fuzzy checkpoints (no full quiesce)                         │     │
│   │   • Reduces recovery time                                       │     │
│   │                                                                 │     │
│   └─────────────────────────────────────────────────────────────────┘     │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

#### Components (`src/recovery/`)

| Component            | File                    | Purpose                               |
|----------------------|-------------------------|---------------------------------------|
| `WALManager`         | `wal_manager.rs`        | Write-ahead log file management       |
| `LogManager`         | `log_manager.rs`        | Log record serialization/buffering    |
| `LogRecord`          | `log_record.rs`         | Log record types and structure        |
| `LogIterator`        | `log_iterator.rs`       | Iterate over log records              |
| `LogRecoveryManager` | `log_recovery.rs`       | ARIES recovery implementation         |
| `CheckpointManager`  | `checkpoint_manager.rs` | Checkpoint creation and management    |

---

### 6. Catalog

The catalog stores database metadata:

```
┌───────────────────────────────────────────────────────────────────────────┐
│                              Catalog                                      │
│                                                                           │
│   ┌─────────────────────────────────────────────────────────────────┐     │
│   │                        Database                                 │     │
│   │   name: "ferrite_db"                                            │     │
│   │   oid: 1                                                        │     │
│   │                                                                 │     │
│   │   ┌─────────────────────────────────────────────────────────┐   │     │
│   │   │                      Tables                             │   │     │
│   │   │                                                         │   │     │
│   │   │   ┌─────────────────────────────────────────────────┐   │   │     │
│   │   │   │  Table: "users"                                 │   │   │     │
│   │   │   │  oid: 100                                       │   │   │     │
│   │   │   │                                                 │   │   │     │
│   │   │   │  Schema:                                        │   │   │     │
│   │   │   │  ┌─────────────────────────────────────────┐    │   │   │     │
│   │   │   │  │  Column     │  Type     │ Nullable      │    │   │   │     │
│   │   │   │  ├─────────────┼───────────┼───────────────┤    │   │   │     │
│   │   │   │  │  id         │ INTEGER   │ NOT NULL      │    │   │   │     │
│   │   │   │  │  name       │ VARCHAR   │ NULL          │    │   │   │     │
│   │   │   │  │  email      │ VARCHAR   │ NOT NULL      │    │   │   │     │
│   │   │   │  │  created_at │ TIMESTAMP │ NOT NULL      │    │   │   │     │
│   │   │   │  └─────────────┴───────────┴───────────────┘    │   │   │     │
│   │   │   │                                                 │   │   │     │
│   │   │   │  Constraints:                                   │   │   │     │
│   │   │   │  • PRIMARY KEY (id)                             │   │   │     │
│   │   │   │  • UNIQUE (email)                               │   │   │     │
│   │   │   │                                                 │   │   │     │
│   │   │   │  Indexes:                                       │   │   │     │
│   │   │   │  • users_pkey (B+ Tree, id)                     │   │   │     │
│   │   │   │  • users_email_idx (B+ Tree, email)             │   │   │     │
│   │   │   │                                                 │   │   │     │
│   │   │   └─────────────────────────────────────────────────┘   │   │     │
│   │   │                                                         │   │     │
│   │   │   ┌─────────────────────────────────────────────────┐   │   │     │
│   │   │   │  Table: "orders"                                │   │   │     │
│   │   │   │  ...                                            │   │   │     │
│   │   │   └─────────────────────────────────────────────────┘   │   │     │
│   │   │                                                         │   │     │
│   │   └─────────────────────────────────────────────────────────┘   │     │
│   │                                                                 │     │
│   └─────────────────────────────────────────────────────────────────┘     │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

#### Components (`src/catalog/`)

| Component       | File                 | Purpose                               |
|-----------------|----------------------|---------------------------------------|
| `Catalog`       | `catalog_impl.rs`    | Main catalog interface                |
| `Database`      | `database.rs`        | Database metadata                     |
| `Schema`        | `schema.rs`          | Table schema (columns, types)         |
| `Column`        | `column.rs`          | Column definition                     |
| `Constraints`   | `constraints.rs`     | PRIMARY KEY, UNIQUE, FOREIGN KEY, etc.|
| `SystemCatalog` | `system_catalog.rs`  | System tables metadata                |

---

### 7. Type System

Ferrite implements a comprehensive type system:

```
┌───────────────────────────────────────────────────────────────────────────┐
│                              Type System                                  │
│                                                                           │
│   ┌─────────────────────────────────────────────────────────────────┐     │
│   │                      Numeric Types                              │     │
│   │                                                                 │     │
│   │   TINYINT   (1 byte)   -128 to 127                              │     │
│   │   SMALLINT  (2 bytes)  -32,768 to 32,767                        │     │
│   │   INTEGER   (4 bytes)  -2^31 to 2^31-1                          │     │
│   │   BIGINT    (8 bytes)  -2^63 to 2^63-1                          │     │
│   │   FLOAT     (4 bytes)  IEEE 754 single precision                │     │
│   │   DECIMAL   (variable) Arbitrary precision                      │     │
│   │                                                                 │     │
│   └─────────────────────────────────────────────────────────────────┘     │
│                                                                           │
│   ┌─────────────────────────────────────────────────────────────────┐     │
│   │                      String Types                               │     │
│   │                                                                 │     │
│   │   VARCHAR(n)  - Variable-length string up to n chars            │     │
│   │   BINARY(n)   - Fixed-length binary data                        │     │
│   │                                                                 │     │
│   └─────────────────────────────────────────────────────────────────┘     │
│                                                                           │
│   ┌─────────────────────────────────────────────────────────────────┐     │
│   │                    Date/Time Types                              │     │
│   │                                                                 │     │
│   │   DATE       - Date without time                                │     │
│   │   TIME       - Time without date                                │     │
│   │   TIMESTAMP  - Date and time                                    │     │
│   │   INTERVAL   - Time duration                                    │     │
│   │                                                                 │     │
│   └─────────────────────────────────────────────────────────────────┘     │
│                                                                           │
│   ┌─────────────────────────────────────────────────────────────────┐     │
│   │                    Complex Types                                │     │
│   │                                                                 │     │
│   │   BOOLEAN    - true/false                                       │     │
│   │   UUID       - 128-bit universally unique identifier            │     │
│   │   JSON       - JSON document                                    │     │
│   │   ARRAY      - Array of values                                  │     │
│   │   STRUCT     - Composite type                                   │     │
│   │   ENUM       - Enumerated type                                  │     │
│   │   POINT      - 2D point (x, y)                                  │     │
│   │   VECTOR     - Fixed-size numeric vector                        │     │
│   │                                                                 │     │
│   └─────────────────────────────────────────────────────────────────┘     │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

#### Components (`src/types_db/`)

| Component    | File               | Purpose                            |
|--------------|--------------------|------------------------------------|
| `TypeId`     | `type_id.rs`       | Type identifier enumeration        |
| `Value`      | `value.rs`         | Runtime value representation       |
| `*Type`      | `*_type.rs`        | Type-specific operations           |

---

## Data Flow Examples

### Query Execution Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│   SELECT name FROM users WHERE age > 21;                                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│   1. PARSE                                                                  │
│   sqlparser → AST: Select { columns: [name], from: users, where: age > 21 } │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│   2. BIND                                                                   │
│   • Resolve "users" → table_oid: 100                                        │
│   • Resolve "name" → column_idx: 1, type: VARCHAR                           │
│   • Resolve "age"  → column_idx: 2, type: INTEGER                           │
│   • Check types: INTEGER > INTEGER ✓                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│   3. PLAN                                                                   │
│                                                                             │
│   Projection(name)                                                          │
│       │                                                                     │
│       └── Filter(age > 21)                                                  │
│               │                                                             │
│               └── SeqScan(users)                                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│   4. OPTIMIZE                                                               │
│                                                                             │
│   • Check for index on "age" → Found: users_age_idx                         │
│   • Rewrite plan:                                                           │
│                                                                             │
│   Projection(name)                                                          │
│       │                                                                     │
│       └── IndexScan(users_age_idx, age > 21)                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│   5. EXECUTE                                                                │
│                                                                             │
│   ProjectionExecutor.next()                                                 │
│       │                                                                     │
│       └── IndexScanExecutor.next()                                          │
│               │                                                             │
│               ├── B+ Tree: find first key > 21                              │
│               │       │                                                     │
│               │       └── fetch_page(leaf_page_id)                          │
│               │               │                                             │
│               │               └── BufferPool: return page from cache        │
│               │                                                             │
│               ├── For each matching RID:                                    │
│               │       └── TableHeap: get_tuple(rid) → (22, "Alice", ...)    │
│               │                                                             │
│               └── Yield tuples matching predicate                           │
│                                                                             │
│   Result: [("Alice"), ("Bob"), ("Carol")]                                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Insert with WAL

```
┌─────────────────────────────────────────────────────────────────────────────┐
│   INSERT INTO users VALUES (1, 'Alice', 25);                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│   1. BEGIN TRANSACTION (txn_id: 42)                                         │
│      └── WAL: append(BEGIN, txn_id=42, lsn=100)                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│   2. ACQUIRE LOCKS                                                          │
│      └── LockManager: acquire IX lock on table "users"                      │
│      └── LockManager: acquire X lock on new row                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│   3. LOG THE CHANGE (BEFORE modifying data)                                 │
│      └── WAL: append(INSERT, txn_id=42, table=users, data=(1,'Alice',25),   │
│                      lsn=101)                                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│   4. MODIFY DATA                                                            │
│      └── BufferPool: fetch_page(table_page_id)                              │
│      └── TablePage: insert_tuple(data) → slot_id: 5                         │
│      └── Page: set_lsn(101), mark_dirty()                                   │
│      └── Update index (if any)                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│   5. COMMIT                                                                 │
│      └── WAL: append(COMMIT, txn_id=42, lsn=102)                            │
│      └── WAL: flush() ← ensures durability                                  │
│      └── LockManager: release all locks                                     │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│   6. LAZY PAGE FLUSH                                                        │
│      └── BufferPool: eventually writes dirty pages to disk                  │
│      └── (Safe because WAL already flushed)                                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Concurrency & Thread Safety

### Locking Strategy

| Resource Type    | Lock Type Used      | Purpose                              |
|------------------|---------------------|--------------------------------------|
| Buffer frames    | `parking_lot::RwLock` | Fast read-heavy access             |
| Page contents    | `Arc<RwLock<Page>>` | Concurrent page access               |
| Transaction state | `Mutex`            | Serialize state changes              |
| Lock manager     | `parking_lot::RwLock` | Lock table access                  |
| Catalog          | `RwLock`            | Schema reads, DDL writes             |

### Key Patterns

```rust
// Pattern 1: Arc<RwLock<T>> for shared mutable state
let catalog: Arc<RwLock<Catalog>> = ...;
let guard = catalog.read();  // Many readers
let guard = catalog.write(); // Exclusive writer

// Pattern 2: PageGuard for RAII page management
let page = buffer_pool.fetch_page(page_id)?;
// Page automatically unpinned when guard drops

// Pattern 3: Transaction scope
let txn = transaction_manager.begin()?;
// ... operations ...
transaction_manager.commit(&txn)?; // or abort
```

---

## Error Handling

All operations return `Result<T, DBError>`:

```rust
#[derive(Error, Debug)]
pub enum DBError {
    #[error("I/O error: {0}")]
    Io(String),

    #[error("Lock error: {0}")]
    LockError(String),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Catalog error: {0}")]
    Catalog(String),

    #[error("Execution error: {0}")]
    Execution(String),

    // ... more variants
}
```

---

## Configuration

### DBConfig

| Field                | Type      | Default      | Description                    |
|----------------------|-----------|--------------|--------------------------------|
| `db_path`            | `String`  | `"ferrite.db"` | Database file path           |
| `log_path`           | `String`  | `"ferrite.log"` | WAL file path               |
| `buffer_pool_size`   | `usize`   | `1024`       | Number of buffer frames        |
| `page_size`          | `usize`   | `4096`       | Page size in bytes             |
| `server_enabled`     | `bool`    | `false`      | Enable TCP server              |
| `server_port`        | `u16`     | `5432`       | Server listen port             |

---

## Performance Characteristics

| Operation                | Time Complexity | Notes                           |
|--------------------------|-----------------|----------------------------------|
| Buffer pool fetch (hit)  | O(1)            | HashMap lookup + LRU-K update   |
| Buffer pool fetch (miss) | O(1) + I/O      | Evict + disk read               |
| B+ tree search           | O(log n)        | n = number of keys              |
| B+ tree insert           | O(log n)        | May split nodes                 |
| Hash table lookup        | O(1) avg        | May resize on high load         |
| Sequential scan          | O(n)            | n = table size                  |
| Index scan               | O(log n + k)    | k = matching rows               |
| Sort                     | O(n log n)      | External sort for large data    |
| Hash join                | O(n + m)        | n, m = input sizes              |

---

## File Structure

```
ferrite.db           # Main database file (pages)
ferrite.log          # Write-ahead log
ferrite.wal          # WAL segments
ferrite.checkpoint   # Checkpoint metadata
```

---

## See Also

- [ZERO_COPY_ARCHITECTURE.md](./ZERO_COPY_ARCHITECTURE.md) - Zero-copy I/O design
- [BENCHMARKING.md](./BENCHMARKING.md) - Performance benchmarks
- [PERFORMANCE_TESTS.md](./PERFORMANCE_TESTS.md) - Performance testing guide

