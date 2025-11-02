# Zero-Copy Database Architecture Design

## Overview

This document outlines the redesign of TKDB's tuple, metadata, and page management system to achieve true zero-copy operations while maintaining memory safety and thread safety without garbage collection.

## Current Problems

1. **Excessive Cloning**: `TupleMeta` is cloned frequently across operations
2. **Repeated Serialization**: Tuple data is serialized/deserialized multiple times
3. **Data Copying**: Values are copied between different representations
4. **Indirect Access**: No direct memory access to page data
5. **Memory Fragmentation**: Scattered allocations instead of contiguous layout

## Design Principles

### 1. Memory Layout First
- Design data structures to be directly readable from memory
- Use fixed-size layouts where possible
- Align data structures for CPU cache efficiency
- Minimize pointer indirection

### 2. Borrow-Based Access
- Return references instead of owned values
- Use lifetimes to ensure memory safety
- Leverage Rust's borrow checker for correctness

### 3. Atomic Operations for Concurrency
- Use atomic types for frequently updated fields
- Avoid locks for read-heavy operations
- Design lock-free data structures where possible

### 4. Minimize Serialization
- Work with raw bytes directly when possible
- Use memory-mapped representations
- Defer serialization until absolutely necessary

## Architecture Components

### 1. Memory-Mapped Table Page

```rust
/// Zero-copy table page with direct memory access
pub struct TablePage {
    /// Raw page data - all other data is accessed via offsets into this buffer
    data: AlignedBuffer<DB_PAGE_SIZE>,

    /// Cached metadata for fast access (points into data buffer)
    metadata_cache: Vec<TupleMetaRef>,

    /// Tuple directory for O(1) lookup (points into data buffer)
    tuple_directory: Vec<TupleSlot>,

    /// Page-level metadata
    header: AtomicPageHeader,

    /// Concurrency control
    pin_count: AtomicU32,
    is_dirty: AtomicBool,
}

/// Reference to tuple metadata within the page buffer
#[derive(Copy, Clone)]
pub struct TupleMetaRef {
    offset: u16,          // Offset in page data
    _phantom: PhantomData<TupleMeta>,
}

/// Tuple slot directory entry
#[derive(Copy, Clone)]
pub struct TupleSlot {
    offset: u16,      // Offset to tuple data in page
    length: u16,      // Length of tuple data
    meta_offset: u16, // Offset to metadata in page
}
```

### 2. Zero-Copy Tuple Design

```rust
/// Zero-copy tuple that references data in place
pub struct Tuple<'a> {
    /// Direct reference to serialized data in page
    data: &'a [u8],

    /// Schema for interpreting the data
    schema: &'a Schema,

    /// RID for identification
    rid: RID,

    /// Cached field offsets for O(1) access
    field_offsets: Option<&'a [u16]>,
}

/// Owned tuple for cases where lifetime can't be preserved
pub struct OwnedTuple {
    data: Vec<u8>,
    schema: Arc<Schema>,
    rid: RID,
    field_offsets: Vec<u16>,
}
```

### 3. Atomic Metadata Design

```rust
/// Thread-safe tuple metadata using atomic operations
#[repr(C, packed)]
pub struct TupleMeta {
    creator_txn_id: AtomicU64,
    commit_timestamp: AtomicU64,
    flags: AtomicU32,  // deleted, visible, etc.
    undo_log_idx: AtomicUsize,
}

/// Non-atomic version for single-threaded access
#[repr(C, packed)]
pub struct TupleMetaLocal {
    creator_txn_id: u64,
    commit_timestamp: u64,
    flags: u32,
    undo_log_idx: usize,
}
```

## Implementation Steps

### Phase 1: Foundation (Week 1)

#### Step 1.1: Create Aligned Buffer System
```rust
/// Cache-aligned buffer for page data
#[repr(align(64))]  // CPU cache line alignment
pub struct AlignedBuffer<const N: usize> {
    data: [u8; N],
}
```

**Justification**: Proper alignment reduces CPU cache misses and enables SIMD operations.

**Trade-offs**:
- ✅ Better performance
- ❌ Slightly more memory usage due to alignment padding

**Potential Issues**:
- Must ensure all offset calculations account for alignment
- Cross-platform alignment differences

#### Step 1.2: Design Memory Layout Protocol
```rust
/// Page layout: [Header][Metadata_Array][Tuple_Directory][Free_Space][Tuple_Data]
pub struct PageLayout {
    header_size: u16,
    metadata_start: u16,
    directory_start: u16,
    free_space_start: u16,
    tuple_data_end: u16,  // Grows backward
}
```

**Justification**: Fixed layout enables direct memory access without parsing.

**Trade-offs**:
- ✅ O(1) access to any component
- ❌ Less flexible than variable layouts
- ❌ May waste space with sparse data

### Phase 2: Zero-Copy Access (Week 2)

#### Step 2.1: Implement Reference-Based Tuple Access
```rust
impl TablePage {
    /// Get tuple reference without copying data
    pub fn get_tuple_ref(&self, rid: RID) -> Result<TupleRef<'_>, PageError> {
        let slot = self.get_tuple_slot(rid.slot())?;
        let data = &self.data[slot.offset as usize..(slot.offset + slot.length) as usize];
        Ok(TupleRef::new(data, self.get_schema(), rid))
    }

    /// Get mutable tuple reference for in-place updates
    pub fn get_tuple_mut(&mut self, rid: RID) -> Result<TupleMut<'_>, PageError> {
        let slot = self.get_tuple_slot(rid.slot())?;
        let data = &mut self.data[slot.offset as usize..(slot.offset + slot.length) as usize];
        Ok(TupleMut::new(data, self.get_schema(), rid))
    }
}
```

**Justification**: References eliminate copying while preserving memory safety through lifetimes.

**Trade-offs**:
- ✅ Zero-copy reads and writes
- ✅ Compile-time memory safety
- ❌ Lifetime complexity in calling code
- ❌ Cannot move tuples across thread boundaries easily

**Potential Issues**:
- Lifetime annotations can become complex
- May need unsafe code for some advanced operations
- Reference invalidation on page restructuring

#### Step 2.2: Atomic Metadata Operations
```rust
impl TupleMeta {
    /// Compare-and-swap update for lock-free modifications
    pub fn update_commit_timestamp(&self, old: u64, new: u64) -> Result<u64, u64> {
        self.commit_timestamp.compare_exchange(old, new, Ordering::Release, Ordering::Relaxed)
    }

    /// Atomic flag operations
    pub fn mark_deleted(&self) -> bool {
        let old_flags = self.flags.fetch_or(DELETED_FLAG, Ordering::Release);
        (old_flags & DELETED_FLAG) == 0
    }
}
```

**Justification**: Atomic operations enable lock-free concurrent access to metadata.

**Trade-offs**:
- ✅ Lock-free performance
- ✅ No deadlock potential
- ❌ ABA problems possible
- ❌ Memory ordering complexity

**Potential Issues**:
- Atomic operations have overhead on some architectures
- Complex memory ordering requirements
- Potential for subtle race conditions

### Phase 3: Advanced Zero-Copy Operations (Week 3)

#### Step 3.1: Direct Value Access
```rust
impl<'a> TupleRef<'a> {
    /// Get value reference without deserialization
    pub fn get_value_ref(&self, column: usize) -> Result<ValueRef<'a>, TupleError> {
        let offset = self.field_offsets[column];
        let value_data = &self.data[offset as usize..];
        ValueRef::from_bytes(value_data, self.schema.get_column(column)?.get_type())
    }

    /// Update value in-place where possible
    pub fn update_value_inplace(&mut self, column: usize, value: &Value) -> Result<(), TupleError> {
        let offset = self.field_offsets[column];
        let field_size = self.schema.get_column(column)?.get_fixed_size();

        if let Some(size) = field_size {
            // Fixed-size field - can update in place
            value.write_to_bytes(&mut self.data[offset as usize..(offset + size) as usize])?;
            Ok(())
        } else {
            // Variable-size field - need reallocation
            Err(TupleError::RequiresReallocation)
        }
    }
}
```

**Justification**: Direct memory access eliminates serialization overhead.

**Trade-offs**:
- ✅ Fastest possible access
- ✅ No allocation for reads
- ❌ Type safety complexity
- ❌ Endianness considerations

#### Step 3.2: Memory-Mapped Value Types
```rust
/// Zero-copy value reference
pub enum ValueRef<'a> {
    Int32(&'a i32),
    Int64(&'a i64),
    Bool(&'a bool),
    VarChar(&'a str),
    // ... other types
}

impl<'a> ValueRef<'a> {
    /// Create from raw bytes without copying
    pub fn from_bytes(data: &'a [u8], type_id: TypeId) -> Result<Self, ValueError> {
        match type_id {
            TypeId::Integer => {
                let int_bytes = data[..4].try_into()?;
                Ok(ValueRef::Int32(unsafe { std::mem::transmute(int_bytes) }))
            },
            TypeId::VarChar => {
                let len = u16::from_le_bytes(data[..2].try_into()?) as usize;
                let str_data = &data[2..2 + len];
                Ok(ValueRef::VarChar(std::str::from_utf8(str_data)?))
            },
            // ... other types
        }
    }
}
```

**Justification**: Direct memory interpretation eliminates deserialization.

**Trade-offs**:
- ✅ Zero deserialization cost
- ✅ Direct CPU register access
- ❌ Unsafe code required
- ❌ Platform-dependent behavior

**Potential Issues**:
- Alignment requirements for different types
- Endianness portability
- Memory safety with unsafe transmutes

### Phase 4: Concurrency and Safety (Week 4)

#### Step 4.1: Lock-Free Page Access
```rust
/// Epoch-based memory management for lock-free access
pub struct EpochGuard<'a> {
    epoch: &'a AtomicU64,
    local_epoch: u64,
}

impl TablePage {
    /// Get snapshot view for consistent reads
    pub fn snapshot<'a>(&'a self, guard: &EpochGuard<'a>) -> PageSnapshot<'a> {
        PageSnapshot {
            page: self,
            epoch: guard.local_epoch,
        }
    }
}

pub struct PageSnapshot<'a> {
    page: &'a TablePage,
    epoch: u64,
}

impl<'a> PageSnapshot<'a> {
    /// All reads are consistent to the snapshot epoch
    pub fn get_tuple(&self, rid: RID) -> Result<TupleRef<'a>, PageError> {
        // Implementation ensures consistent view
        self.page.get_tuple_ref(rid)
    }
}
```

**Justification**: Epoch-based reclamation enables lock-free reads with memory safety.

**Trade-offs**:
- ✅ Excellent read performance
- ✅ No reader/writer blocking
- ❌ Complex memory management
- ❌ Potential memory overhead from deferred cleanup

#### Step 4.2: MVCC Integration
```rust
/// Multi-version concurrency control with zero-copy
impl TablePage {
    /// Get tuple version visible to transaction
    pub fn get_tuple_version(&self, rid: RID, txn_id: TxnId, read_ts: Timestamp)
        -> Result<Option<TupleRef<'_>>, PageError> {

        let tuple_ref = self.get_tuple_ref(rid)?;
        let meta = self.get_tuple_meta_ref(rid)?;

        // Check visibility without copying metadata
        if meta.is_visible_to(txn_id, read_ts) {
            Ok(Some(tuple_ref))
        } else {
            Ok(None)
        }
    }
}
```

**Justification**: MVCC visibility checks can be performed without copying data.

## Migration Strategy

### Phase 1: Preparation (3 days)
1. Add feature flag for new zero-copy implementation
2. Create compatibility layer for existing code
3. Implement core aligned buffer and layout types

### Phase 2: Gradual Replacement (1 week)
1. Replace tuple access methods one by one
2. Update tests to use new APIs
3. Benchmark performance improvements

### Phase 3: Full Migration (3 days)
1. Remove old implementation code
2. Clean up compatibility layers
3. Final performance validation

## Performance Expectations

### Memory Improvements
- **50-80% reduction** in heap allocations
- **30-50% reduction** in memory usage
- **Elimination** of clone-related allocations

### CPU Improvements
- **2-3x faster** tuple reads
- **40-60% faster** metadata operations
- **Elimination** of serialization overhead for reads

### Concurrency Improvements
- **Lock-free reads** scale linearly with cores
- **Reduced contention** on metadata updates
- **Better cache locality** from aligned access

## Risk Mitigation

### Memory Safety
- **Comprehensive testing** with AddressSanitizer and Miri
- **Formal verification** of unsafe code blocks
- **Gradual rollout** with feature flags

### Performance Regression
- **Continuous benchmarking** during development
- **Fallback mechanisms** to old implementation
- **Performance tests** in CI/CD pipeline

### Complexity Management
- **Clear documentation** of unsafe code reasoning
- **Modular design** allowing independent testing
- **Code review** requirements for unsafe blocks

## Success Metrics

1. **Zero heap allocations** for tuple reads in steady state
2. **Linear scalability** of concurrent reads
3. **Sub-microsecond** tuple access latency
4. **Memory usage reduction** of at least 30%
5. **No memory safety issues** in extensive testing

This architecture provides a foundation for high-performance, memory-safe, zero-copy database operations while leveraging Rust's type system for correctness guarantees.
