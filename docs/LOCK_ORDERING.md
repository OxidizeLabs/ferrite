# Lock Ordering Documentation

This document defines the global lock acquisition order in Ferrite to prevent deadlocks. All developers must follow these invariants when writing concurrent code.

## Table of Contents

- [Overview](#overview)
- [Global Lock Hierarchy](#global-lock-hierarchy)
- [Component-Specific Lock Ordering](#component-specific-lock-ordering)
  - [Lock Manager (Transaction Locks)](#lock-manager-transaction-locks)
  - [Buffer Pool Manager](#buffer-pool-manager)
  - [B+ Tree Index (Latch Crabbing)](#b-plus-tree-index-latch-crabbing)
  - [Transaction Manager](#transaction-manager)
  - [Catalog](#catalog)
  - [Async Disk Manager](#async-disk-manager)
  - [Recovery/WAL](#recoverywal)
- [Lock Types Used](#lock-types-used)
- [Deadlock Prevention Strategies](#deadlock-prevention-strategies)
- [Common Patterns](#common-patterns)
- [Anti-Patterns to Avoid](#anti-patterns-to-avoid)

---

## Overview

Ferrite uses multiple lock types across different components:

- **`parking_lot::RwLock`**: Reader-writer locks for shared data (preferred for performance)
- **`parking_lot::Mutex`**: Exclusive locks for simpler synchronization
- **`std::sync::atomic`**: Lock-free counters and flags
- **`parking_lot::Condvar`**: Condition variables for blocking waits

The fundamental rule for preventing deadlocks:

> **Always acquire locks in the order specified by the global hierarchy. Never acquire a higher-level lock while holding a lower-level lock.**

---

## Global Lock Hierarchy

The following hierarchy defines the global lock acquisition order from **highest to lowest priority**:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           GLOBAL LOCK HIERARCHY                              │
│                     (Acquire from top to bottom only)                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Level 1: System-Wide Locks                                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  1.1  Catalog Lock (Arc<RwLock<Catalog>>)                           │    │
│  │  1.2  Transaction Manager State                                      │    │
│  │  1.3  Recovery/WAL Manager Locks                                     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                              │                                               │
│                              ▼                                               │
│  Level 2: Lock Manager Locks                                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  2.1  Table Lock Map (Mutex<HashMap<TableOid, LockRequestQueue>>)   │    │
│  │  2.2  Row Lock Map (Mutex<HashMap<RID, LockRequestQueue>>)          │    │
│  │  2.3  Transaction Lock Sets (Mutex<HashMap<TxnId, TxnLockState>>)   │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                              │                                               │
│                              ▼                                               │
│  Level 3: Buffer Pool Manager Locks                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  3.1  Page Table (Arc<RwLock<HashMap<PageId, FrameId>>>)            │    │
│  │  3.2  Pages Collection (Arc<RwLock<Vec<Option<...>>>>)              │    │
│  │  3.3  Replacer (Arc<RwLock<LRUKReplacer>>)                          │    │
│  │  3.4  Free List (Arc<RwLock<Vec<FrameId>>>)                         │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                              │                                               │
│                              ▼                                               │
│  Level 4: Index Structure Locks (B+ Tree)                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  4.1  B+ Tree Root Lock (Arc<RwLock<BPlusTree>>)                    │    │
│  │  4.2  Internal Page Latches (top-down acquisition)                  │    │
│  │  4.3  Leaf Page Latches                                             │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                              │                                               │
│                              ▼                                               │
│  Level 5: Page-Level Locks                                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  5.1  Individual Page RwLocks (Arc<RwLock<T: PageTrait>>)           │    │
│  │  5.2  Table Page Locks (within TableHeap operations)                │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                              │                                               │
│                              ▼                                               │
│  Level 6: Disk I/O Locks                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  6.1  Disk Cache Locks (cache_manager, LRU cache)                   │    │
│  │  6.2  Write Buffer Locks (write_staging_buffer)                     │    │
│  │  6.3  I/O Queue Locks                                               │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Component-Specific Lock Ordering

### Lock Manager (Transaction Locks)

The Lock Manager implements strict two-phase locking (2PL) with multi-granularity locking.

#### Internal Lock Order

```rust
// CORRECT: Acquire locks in this order within LockStateManager
let mut table_locks = self.table_lock_map.lock();        // 1st
let queue = table_locks.entry(oid).or_insert_with(...);
let mut queue_guard = queue.lock();                      // 2nd
let mut txn_locks = self.txn_lock_sets.lock();           // 3rd

// ALWAYS drop in reverse order
drop(queue_guard);
drop(table_locks);
```

#### Multi-Granularity Lock Ordering

```
┌─────────────────────────────────────────────────────────────────┐
│                  MULTI-GRANULARITY LOCKING                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   Acquire table-level intention lock BEFORE row-level locks:    │
│                                                                  │
│   1. Table Lock (IS/IX/S/SIX/X)                                 │
│        │                                                         │
│        ▼                                                         │
│   2. Row Lock (S/X)                                             │
│                                                                  │
│   Release in reverse order:                                      │
│                                                                  │
│   1. Row Locks (all rows on table)                              │
│        │                                                         │
│        ▼                                                         │
│   2. Table Lock                                                  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Key Rules:**
- Acquire `IS` or `IX` on table before any row lock on that table
- Release all row locks before releasing table lock
- The `force_release_txn()` method releases row locks first, then table locks

#### Lock Request Queue Order

Within a single resource, lock requests are processed in FIFO order with upgrade priority:
- Upgrade requests (same transaction wanting stronger lock) get priority
- New requests queue behind existing requests

### Buffer Pool Manager

The BPM uses multiple internal locks that must be acquired in a specific order.

#### Lock Order

```rust
// CORRECT order for fetching a page
let page_table = self.page_table.read();                 // 1st: page table
let frame_id = page_table.get(&page_id);
drop(page_table);

let pages = self.pages.read();                            // 2nd: pages collection
let page = pages[frame_id as usize].clone();
drop(pages);

let mut replacer = self.replacer.write();                 // 3rd: replacer
replacer.record_access(frame_id, access_type);
replacer.set_evictable(frame_id, false);
drop(replacer);
```

#### Page Pinning Invariant

```
┌─────────────────────────────────────────────────────────────────┐
│                     PAGE PINNING RULES                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Increment pin_count BEFORE marking non-evictable            │
│  2. Mark evictable AFTER decrementing pin_count to 0            │
│  3. Hold page lock while modifying pin_count                    │
│                                                                  │
│  Pin Sequence:                                                   │
│    page.lock() → pin_count++ → replacer.set_evictable(false)    │
│                                                                  │
│  Unpin Sequence:                                                 │
│    page.lock() → pin_count-- → if 0: replacer.set_evictable(true)│
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### B+ Tree Index (Latch Crabbing)

B+ tree operations use **latch crabbing** (lock coupling) for safe concurrent access.

#### Latch Acquisition Protocol

```
┌─────────────────────────────────────────────────────────────────┐
│                    LATCH CRABBING PROTOCOL                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  READ Operations (Search, Range Scan):                           │
│  ────────────────────────────────────                            │
│    1. Acquire read latch on root                                 │
│    2. Acquire read latch on child                                │
│    3. Release parent read latch (hand-over-hand)                 │
│    4. Repeat until leaf                                          │
│                                                                  │
│  WRITE Operations - Optimistic:                                  │
│  ─────────────────────────────                                   │
│    1. Traverse with read latches (like reads)                    │
│    2. Acquire write latch only on leaf                           │
│    3. If leaf is safe → perform operation                        │
│    4. If split/merge needed → RESTART with pessimistic          │
│                                                                  │
│  WRITE Operations - Pessimistic:                                 │
│  ──────────────────────────────                                  │
│    1. Acquire write latch on root                                │
│    2. Acquire write latch on child                               │
│    3. If child is "safe" → release ALL ancestor latches         │
│    4. Repeat until leaf                                          │
│                                                                  │
│  Node is "SAFE" when:                                            │
│    - Insert: size < max_size - 1 (room for key without split)   │
│    - Delete: size > min_size OR is root (no underflow risk)     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

#### HeldWriteLock Semantics

The `HeldWriteLock` struct in `latch_crabbing.rs` properly holds write latches:

```rust
pub struct HeldWriteLock<K, C> {
    _page_guard: PageGuard<BPlusTreeInternalPage<K, C>>,  // Keeps page pinned
    write_guard: ArcRwLockWriteGuard<...>,                 // HOLDS the write lock
    page_id: PageId,
    acquired_at: Instant,                                  // For metrics
}
```

**Key Invariant:** The `write_guard` owns a cloned `Arc<RwLock<T>>`, so the lock is genuinely held for the lifetime of `HeldWriteLock`.

### Transaction Manager

#### State Locks

```rust
pub struct Transaction {
    // Immutable fields (no lock needed)
    txn_id: TxnId,
    isolation_level: IsolationLevel,
    thread_id: thread::ThreadId,

    // Mutable fields with interior mutability
    state: RwLock<TransactionState>,           // Transaction lifecycle
    read_ts: RwLock<Timestamp>,                // Snapshot point
    commit_ts: RwLock<Timestamp>,              // Commit timestamp
    undo_logs: Mutex<Vec<Arc<UndoLog>>>,       // Rollback info
    write_set: Mutex<HashMap<TableOidT, HashSet<RID>>>,
    scan_predicates: Mutex<HashMap<u32, Vec<Arc<Expression>>>>,
    prev_lsn: RwLock<Lsn>,                     // WAL chaining
}
```

**Lock Order within Transaction:**
1. `state` (read/write lifecycle state)
2. `read_ts` / `commit_ts` (timestamps)
3. `write_set` / `undo_logs` (operation logs)
4. `scan_predicates`
5. `prev_lsn`

#### Watermark Access

The `Watermark` component must be wrapped in external synchronization:

```rust
// CORRECT: Wrap in Mutex for thread safety
let watermark = Arc::new(Mutex::new(Watermark::new()));

// Thread-safe timestamp allocation
let ts = watermark.lock().get_next_ts_and_register();
// ... do work ...
watermark.lock().unregister_txn(ts);
```

### Catalog

The Catalog is **not internally synchronized**. Callers must provide external synchronization:

```rust
// CORRECT: Wrap catalog in RwLock
let catalog = Arc::new(RwLock::new(Catalog::new(bpm, txn_manager)));

// For reads
let catalog_read = catalog.read();
let table = catalog_read.get_table("users");

// For writes (table creation, etc.)
let mut catalog_write = catalog.write();
catalog_write.create_table("orders", schema);
```

**Lock Order with Catalog:**
1. Catalog lock (external `Arc<RwLock<Catalog>>`)
2. Individual table/index locks (internal to Database)
3. B+ tree locks (for index access)
4. Page locks (for table heap access)

### Async Disk Manager

#### Internal Locks

```rust
// Cache Manager locks
cache_manager: RwLock<CacheManager>

// Write Buffer locks
write_staging_buffer: Mutex<WriteStagingBuffer>
flush_coordinator: Mutex<FlushCoordinator>

// I/O Queue locks
io_queue: Mutex<IoQueue>
completion_queue: Mutex<CompletionQueue>
```

**Lock Order:**
1. `cache_manager` (for cache lookups/updates)
2. `write_staging_buffer` (for buffering writes)
3. `flush_coordinator` (for coordinating flushes)
4. `io_queue` (for submitting I/O)

### Recovery/WAL

#### Log Manager Locks

```rust
// CORRECT order for log operations
let log_manager = log_manager.lock();  // 1st: log manager
let buffer = log_manager.get_buffer(); // Access buffer
// Write log record
log_manager.flush();                   // Flush to disk
```

---

## Lock Types Used

| Lock Type | Use Case | Performance Characteristics |
|-----------|----------|----------------------------|
| `parking_lot::RwLock` | Read-heavy shared data | Better than std for read contention |
| `parking_lot::Mutex` | Exclusive access | Lower overhead than std |
| `AtomicU64` | Counters, IDs | Lock-free, highest performance |
| `Condvar` | Blocking waits | Used with Mutex for lock queues |

**Why `parking_lot` over `std`:**
- No poisoning (simpler error handling)
- Better performance under contention
- Fair scheduling options
- Smaller lock size

---

## Deadlock Prevention Strategies

### 1. Strict Lock Ordering

Always acquire locks according to the global hierarchy. Never acquire a higher-level lock while holding a lower-level lock.

### 2. Timeout-Based Detection

For B+ tree operations, use timeout-based detection to catch potential deadlocks:

```rust
// In btree_observability configuration
pub struct LockConfig {
    pub warn_after: Duration,      // Warn after this wait time
    pub warn_every: Duration,      // Warning interval
    pub timeout_after: Option<Duration>,  // Panic after timeout (tests only)
}
```

### 3. Waits-For Graph

The Lock Manager maintains a waits-for graph for transaction deadlock detection:

```
┌─────────────────────────────────────────────────────────────────┐
│                    DEADLOCK DETECTION                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Waits-For Graph:                                                │
│    T1 ──▶ T2 ──▶ T3                                             │
│           │      │                                               │
│           └──────┘  ◄── CYCLE DETECTED                          │
│                                                                  │
│  Detection Algorithm:                                            │
│    1. Background thread runs DFS every 50ms                     │
│    2. When cycle found, abort transaction with highest TxnId    │
│    3. Aborted transaction releases all locks                    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 4. Try-Lock Patterns

Use try-lock when possible to avoid blocking indefinitely:

```rust
// Prefer try_write_arc for contended locks
if let Some(guard) = RwLock::try_write_arc(&arc) {
    // Got the lock
} else {
    // Implement backoff or alternative strategy
}
```

---

## Common Patterns

### Pattern 1: Lock-Then-Clone

For read-only access to shared data, clone under lock then release:

```rust
let data = {
    let guard = shared_data.read();
    guard.clone()  // Clone the data
};  // Lock released here
// Work with cloned data without holding lock
process(&data);
```

### Pattern 2: Two-Phase Access

Separate lookup from modification:

```rust
// Phase 1: Lookup (read lock)
let frame_id = {
    let page_table = self.page_table.read();
    page_table.get(&page_id).copied()
};

// Phase 2: Modify (write lock, if needed)
if let Some(frame_id) = frame_id {
    let mut pages = self.pages.write();
    // Modify pages[frame_id]
}
```

### Pattern 3: Hierarchical Release

Release locks in reverse acquisition order:

```rust
// Acquire
let lock_a = resource_a.lock();
let lock_b = resource_b.lock();
let lock_c = resource_c.lock();

// ... do work ...

// Release in reverse order
drop(lock_c);
drop(lock_b);
drop(lock_a);
```

---

## Anti-Patterns to Avoid

### ❌ Lock Inversion

```rust
// WRONG: Acquiring page lock before page_table lock
let page = pages.read()[0].clone();
let page_table = self.page_table.write();  // DANGER: Lock inversion!
```

### ❌ Holding Locks Across Await Points

```rust
// WRONG: Lock held across .await
let guard = data.lock();
some_async_operation().await;  // DANGER: Guard held across await!
drop(guard);
```

### ❌ Nested Lock Acquisition in Callbacks

```rust
// WRONG: Callback acquires lock on same resource
fn process_with_lock(&self) {
    let guard = self.data.lock();
    self.process_item(|item| {
        let guard2 = self.data.lock();  // DEADLOCK!
    });
}
```

### ❌ Recursive Lock Acquisition

```rust
// WRONG: Recursive function that acquires same lock
fn recursive_op(&self) {
    let guard = self.data.lock();
    if condition {
        self.recursive_op();  // DEADLOCK: Already holding lock!
    }
}
```

---

## Checklist for New Code

When adding new locks or modifying concurrent code:

- [ ] Identify where the new lock fits in the global hierarchy
- [ ] Verify all acquisition paths follow the lock order
- [ ] Use `parking_lot` locks unless there's a specific reason for `std`
- [ ] Add timeout/warning logging for locks that might block long
- [ ] Document any lock ordering assumptions in code comments
- [ ] Test with stress tests to verify no deadlocks
- [ ] Consider using try-lock patterns where appropriate

---

## References

- **Lock Manager**: `src/concurrency/lock_manager.rs`
- **Buffer Pool Manager**: `src/buffer/buffer_pool_manager_async.rs`
- **Latch Crabbing**: `src/storage/index/latch_crabbing.rs`
- **Transaction Manager**: `src/concurrency/transaction_manager.rs`
- **Watermark**: `src/concurrency/watermark.rs`
- **Catalog**: `src/catalog/catalog_impl.rs`
