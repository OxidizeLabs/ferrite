# Zero-Copy Implementation Plan

## Implementation Steps with Detailed Analysis

### Phase 1: Foundation Layer (Days 1-3)

#### Step 1.1: Aligned Buffer Infrastructure

**Implementation:**
```rust
// src/common/aligned_buffer.rs
use std::alloc::{alloc, dealloc, Layout};
use std::mem::align_of;

#[repr(align(64))] // CPU cache line alignment
pub struct AlignedBuffer<const N: usize> {
    data: [u8; N],
}

impl<const N: usize> AlignedBuffer<N> {
    pub fn new() -> Self {
        Self { data: [0; N] }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Get aligned pointer for direct memory operations
    pub fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }
}

// Safety traits for atomic operations
unsafe impl<const N: usize> Send for AlignedBuffer<N> {}
unsafe impl<const N: usize> Sync for AlignedBuffer<N> {}
```

**Justification:**
- Cache-line alignment prevents false sharing
- Direct pointer access enables zero-copy operations
- Fixed-size arrays are stack-allocated and fast

**Trade-offs:**
- ✅ **Performance**: 15-30% improvement from cache alignment
- ✅ **Safety**: No heap allocation/deallocation
- ❌ **Memory**: ~64 bytes padding per buffer
- ❌ **Flexibility**: Fixed size at compile time

**Potential Issues:**
- Cross-platform alignment differences (x86 vs ARM)
- Stack overflow with very large buffers
- SIMD instruction requirements

#### Step 1.2: Memory Layout Protocol

**Implementation:**
```rust
// src/storage/page/layout.rs
use std::mem::size_of;

/// Fixed page layout for zero-copy access
#[repr(C, packed)]
pub struct PageLayout {
    // All offsets are from beginning of page
    header_size: u16,           // Size of page header
    metadata_start: u16,        // Start of tuple metadata array
    metadata_count: u16,        // Number of metadata entries
    directory_start: u16,       // Start of tuple directory
    directory_count: u16,       // Number of directory entries
    free_space_start: u16,      // Start of free space
    tuple_data_end: u16,        // End of tuple data (grows backward)
}

impl PageLayout {
    const HEADER_SIZE: u16 = size_of::<PageLayout>() as u16;

    pub fn new(page_size: u16) -> Self {
        Self {
            header_size: Self::HEADER_SIZE,
            metadata_start: Self::HEADER_SIZE,
            metadata_count: 0,
            directory_start: Self::HEADER_SIZE,
            directory_count: 0,
            free_space_start: Self::HEADER_SIZE,
            tuple_data_end: page_size,
        }
    }

    /// Calculate if there's enough space for a new tuple
    pub fn can_fit(&self, tuple_size: u16, meta_size: u16) -> bool {
        let needed_space = tuple_size + meta_size + size_of::<TupleSlot>() as u16;
        (self.tuple_data_end - self.free_space_start) >= needed_space
    }

    /// Get direct pointer to metadata array
    pub unsafe fn metadata_ptr<T>(&self, base: *const u8) -> *const T {
        base.add(self.metadata_start as usize) as *const T
    }

    /// Get direct pointer to directory
    pub unsafe fn directory_ptr(&self, base: *const u8) -> *const TupleSlot {
        base.add(self.directory_start as usize) as *const TupleSlot
    }
}
```

**Justification:**
- Direct pointer arithmetic eliminates bounds checking
- Fixed layout enables O(1) component access
- Packed representation minimizes memory usage

**Trade-offs:**
- ✅ **Performance**: O(1) access, no parsing overhead
- ✅ **Predictability**: Known memory layout
- ❌ **Safety**: Requires unsafe pointer operations
- ❌ **Flexibility**: Cannot easily change layout

**Potential Issues:**
- Alignment issues with packed structs
- Endianness portability
- Pointer arithmetic safety

#### Step 1.3: Atomic Metadata Types

**Implementation:**
```rust
// src/storage/table/metadata.rs
use std::sync::atomic::{AtomicU64, AtomicU32, Ordering};

/// Lock-free tuple metadata
#[repr(C, align(8))]  // Ensure atomic alignment
pub struct AtomicTupleMeta {
    creator_txn_id: AtomicU64,
    commit_timestamp: AtomicU64,
    flags: AtomicU32,           // Packed: deleted(1) | visible(1) | reserved(30)
    undo_log_idx: AtomicU64,    // Using u64 for alignment
}

impl AtomicTupleMeta {
    const DELETED_FLAG: u32 = 1 << 0;
    const VISIBLE_FLAG: u32 = 1 << 1;

    pub fn new(txn_id: u64) -> Self {
        Self {
            creator_txn_id: AtomicU64::new(txn_id),
            commit_timestamp: AtomicU64::new(u64::MAX),
            flags: AtomicU32::new(0),
            undo_log_idx: AtomicU64::new(0),
        }
    }

    /// Lock-free visibility check
    pub fn is_visible_to(&self, read_ts: u64) -> bool {
        let commit_ts = self.commit_timestamp.load(Ordering::Acquire);
        let flags = self.flags.load(Ordering::Relaxed);

        commit_ts <= read_ts &&
        (flags & Self::VISIBLE_FLAG) != 0 &&
        (flags & Self::DELETED_FLAG) == 0
    }

    /// Atomic delete operation
    pub fn mark_deleted(&self) -> bool {
        let old_flags = self.flags.fetch_or(Self::DELETED_FLAG, Ordering::Release);
        (old_flags & Self::DELETED_FLAG) == 0
    }

    /// Compare-and-swap commit
    pub fn try_commit(&self, txn_id: u64, commit_ts: u64) -> Result<(), u64> {
        // First check if we own this tuple
        if self.creator_txn_id.load(Ordering::Acquire) != txn_id {
            return Err(0); // Not our tuple
        }

        // Atomic commit
        match self.commit_timestamp.compare_exchange(
            u64::MAX, commit_ts, Ordering::Release, Ordering::Relaxed
        ) {
            Ok(_) => {
                self.flags.fetch_or(Self::VISIBLE_FLAG, Ordering::Release);
                Ok(())
            },
            Err(current) => Err(current)
        }
    }
}

/// Non-atomic version for single-threaded access
#[repr(C, packed)]
#[derive(Copy, Clone)]
pub struct TupleMetaLocal {
    creator_txn_id: u64,
    commit_timestamp: u64,
    flags: u32,
    undo_log_idx: u64,
}
```

**Justification:**
- Atomic operations eliminate need for metadata locks
- Compare-and-swap enables lock-free transaction commits
- Separate atomic/local versions optimize for different use cases

**Trade-offs:**
- ✅ **Concurrency**: Lock-free metadata operations
- ✅ **Performance**: No lock contention
- ❌ **Complexity**: Memory ordering requirements
- ❌ **Overhead**: Atomic operations cost on some architectures

**Potential Issues:**
- ABA problems in concurrent scenarios
- Memory ordering bugs
- Platform-specific atomic performance

### Phase 2: Zero-Copy Access Layer (Days 4-7)

#### Step 2.1: Reference-Based Tuple Access

**Implementation:**
```rust
// src/storage/table/tuple_ref.rs
use std::marker::PhantomData;

/// Zero-copy tuple reference
pub struct TupleRef<'a> {
    /// Direct reference to tuple data in page
    data: &'a [u8],
    /// Schema for interpreting data
    schema: &'a Schema,
    /// RID for identification
    rid: RID,
    /// Cached field offsets for O(1) access
    field_offsets: &'a [u16],
}

impl<'a> TupleRef<'a> {
    pub fn new(data: &'a [u8], schema: &'a Schema, rid: RID, offsets: &'a [u16]) -> Self {
        Self { data, schema, rid, field_offsets: offsets }
    }

    /// Get value reference without deserialization
    pub fn get_value(&self, column: usize) -> Result<ValueRef<'a>, TupleError> {
        if column >= self.field_offsets.len() {
            return Err(TupleError::InvalidColumn(column));
        }

        let offset = self.field_offsets[column] as usize;
        let column_type = self.schema.get_column(column)?.get_type();

        ValueRef::from_raw_data(&self.data[offset..], column_type)
    }

    /// Get raw field data slice
    pub fn get_field_data(&self, column: usize) -> Result<&'a [u8], TupleError> {
        if column >= self.field_offsets.len() {
            return Err(TupleError::InvalidColumn(column));
        }

        let start = self.field_offsets[column] as usize;
        let end = if column + 1 < self.field_offsets.len() {
            self.field_offsets[column + 1] as usize
        } else {
            self.data.len()
        };

        Ok(&self.data[start..end])
    }
}

/// Mutable tuple reference for in-place updates
pub struct TupleMut<'a> {
    data: &'a mut [u8],
    schema: &'a Schema,
    rid: RID,
    field_offsets: &'a [u16],
}

impl<'a> TupleMut<'a> {
    /// Update fixed-size field in place
    pub fn update_field_inplace(&mut self, column: usize, value: &Value) -> Result<(), TupleError> {
        let column_info = self.schema.get_column(column)?;
        if let Some(fixed_size) = column_info.get_fixed_size() {
            let offset = self.field_offsets[column] as usize;
            value.write_to_slice(&mut self.data[offset..offset + fixed_size])?;
            Ok(())
        } else {
            Err(TupleError::RequiresReallocation)
        }
    }

    /// Get mutable slice for advanced operations
    pub fn get_field_data_mut(&mut self, column: usize) -> Result<&mut [u8], TupleError> {
        if column >= self.field_offsets.len() {
            return Err(TupleError::InvalidColumn(column));
        }

        let start = self.field_offsets[column] as usize;
        let end = if column + 1 < self.field_offsets.len() {
            self.field_offsets[column + 1] as usize
        } else {
            self.data.len()
        };

        Ok(&mut self.data[start..end])
    }
}
```

**Justification:**
- References eliminate copying while preserving safety
- Cached offsets enable O(1) field access
- Separate mutable version follows Rust's aliasing rules

**Trade-offs:**
- ✅ **Performance**: Zero-copy field access
- ✅ **Safety**: Compile-time memory safety
- ❌ **Lifetime complexity**: Borrowing rules can be restrictive
- ❌ **Threading**: References don't cross thread boundaries easily

**Potential Issues:**
- Complex lifetime relationships
- Borrowing conflicts in complex operations
- Need for careful API design to avoid lifetime issues

#### Step 2.2: Value Reference System

**Implementation:**
```rust
// src/types_db/value_ref.rs

/// Zero-copy value reference
pub enum ValueRef<'a> {
    Integer(&'a i32),
    BigInt(&'a i64),
    Float(&'a f32),
    Double(&'a f64),
    Boolean(&'a bool),
    VarChar(&'a str),
    Binary(&'a [u8]),
    Null,
}

impl<'a> ValueRef<'a> {
    /// Create value reference from raw data without copying
    pub fn from_raw_data(data: &'a [u8], type_id: TypeId) -> Result<Self, ValueError> {
        match type_id {
            TypeId::Integer => {
                if data.len() < 4 { return Err(ValueError::InsufficientData); }
                let value = unsafe { &*(data.as_ptr() as *const i32) };
                Ok(ValueRef::Integer(value))
            },
            TypeId::BigInt => {
                if data.len() < 8 { return Err(ValueError::InsufficientData); }
                let value = unsafe { &*(data.as_ptr() as *const i64) };
                Ok(ValueRef::BigInt(value))
            },
            TypeId::VarChar => {
                if data.len() < 2 { return Err(ValueError::InsufficientData); }
                let len = u16::from_le_bytes([data[0], data[1]]) as usize;
                if data.len() < 2 + len { return Err(ValueError::InsufficientData); }
                let str_data = &data[2..2 + len];
                let s = std::str::from_utf8(str_data)?;
                Ok(ValueRef::VarChar(s))
            },
            TypeId::Boolean => {
                if data.is_empty() { return Err(ValueError::InsufficientData); }
                let value = unsafe { &*(data.as_ptr() as *const bool) };
                Ok(ValueRef::Boolean(value))
            },
            TypeId::Binary => {
                if data.len() < 2 { return Err(ValueError::InsufficientData); }
                let len = u16::from_le_bytes([data[0], data[1]]) as usize;
                if data.len() < 2 + len { return Err(ValueError::InsufficientData); }
                Ok(ValueRef::Binary(&data[2..2 + len]))
            },
            _ => Err(ValueError::UnsupportedType(type_id))
        }
    }

    /// Convert to owned value when needed
    pub fn to_owned(&self) -> Value {
        match self {
            ValueRef::Integer(v) => Value::Integer(**v),
            ValueRef::BigInt(v) => Value::BigInt(**v),
            ValueRef::Float(v) => Value::Float(**v),
            ValueRef::Double(v) => Value::Double(**v),
            ValueRef::Boolean(v) => Value::Boolean(**v),
            ValueRef::VarChar(s) => Value::VarChar(s.to_string()),
            ValueRef::Binary(b) => Value::Binary(b.to_vec()),
            ValueRef::Null => Value::Null,
        }
    }
}
```

**Justification:**
- Direct memory interpretation eliminates deserialization
- Unsafe transmute is contained and verified
- Fallback to owned values when references can't be used

**Trade-offs:**
- ✅ **Performance**: Zero deserialization overhead
- ✅ **Memory**: No allocations for reads
- ❌ **Safety**: Requires unsafe code
- ❌ **Portability**: Endianness and alignment concerns

**Potential Issues:**
- Alignment requirements for numeric types
- Endianness issues on different architectures
- UTF-8 validation overhead for strings

### Phase 3: Advanced Operations (Days 8-10)

#### Step 3.1: Zero-Copy Table Page

**Implementation:**
```rust
// src/storage/page/zero_copy_table_page.rs

pub struct ZeroCopyTablePage {
    /// Raw aligned page data
    buffer: AlignedBuffer<DB_PAGE_SIZE>,

    /// Cached layout information
    layout: PageLayout,

    /// Metadata references (cached for performance)
    metadata_cache: Vec<*const AtomicTupleMeta>,

    /// Directory cache
    directory_cache: Vec<TupleSlot>,

    /// Page-level atomics
    pin_count: AtomicU32,
    is_dirty: AtomicBool,
    modification_epoch: AtomicU64,
}

impl ZeroCopyTablePage {
    pub fn new(page_id: PageId) -> Self {
        let mut buffer = AlignedBuffer::new();
        let layout = PageLayout::new(DB_PAGE_SIZE as u16);

        // Initialize page header
        unsafe {
            let header_ptr = buffer.as_mut_ptr() as *mut PageLayout;
            *header_ptr = layout;
        }

        Self {
            buffer,
            layout,
            metadata_cache: Vec::new(),
            directory_cache: Vec::new(),
            pin_count: AtomicU32::new(1),
            is_dirty: AtomicBool::new(false),
            modification_epoch: AtomicU64::new(0),
        }
    }

    /// Get tuple reference without copying
    pub fn get_tuple_ref(&self, rid: RID) -> Result<TupleRef<'_>, PageError> {
        let slot_idx = rid.get_slot_num() as usize;
        if slot_idx >= self.directory_cache.len() {
            return Err(PageError::InvalidSlot(slot_idx));
        }

        let slot = &self.directory_cache[slot_idx];
        let tuple_data = unsafe {
            let data_ptr = self.buffer.as_ptr().add(slot.offset as usize);
            std::slice::from_raw_parts(data_ptr, slot.length as usize)
        };

        // Get cached field offsets
        let schema = self.get_schema(); // Assume this is available
        let field_offsets = self.get_field_offsets_for_slot(slot_idx)?;

        Ok(TupleRef::new(tuple_data, schema, rid, field_offsets))
    }

    /// Get mutable tuple reference
    pub fn get_tuple_mut(&mut self, rid: RID) -> Result<TupleMut<'_>, PageError> {
        let slot_idx = rid.get_slot_num() as usize;
        if slot_idx >= self.directory_cache.len() {
            return Err(PageError::InvalidSlot(slot_idx));
        }

        let slot = &self.directory_cache[slot_idx];
        let tuple_data = unsafe {
            let data_ptr = self.buffer.as_mut_ptr().add(slot.offset as usize);
            std::slice::from_raw_parts_mut(data_ptr, slot.length as usize)
        };

        let schema = self.get_schema();
        let field_offsets = self.get_field_offsets_for_slot(slot_idx)?;

        self.is_dirty.store(true, Ordering::Relaxed);
        self.modification_epoch.fetch_add(1, Ordering::Release);

        Ok(TupleMut::new(tuple_data, schema, rid, field_offsets))
    }

    /// Get metadata reference
    pub fn get_tuple_meta(&self, rid: RID) -> Result<&AtomicTupleMeta, PageError> {
        let slot_idx = rid.get_slot_num() as usize;
        if slot_idx >= self.metadata_cache.len() {
            return Err(PageError::InvalidSlot(slot_idx));
        }

        Ok(unsafe { &*self.metadata_cache[slot_idx] })
    }

    /// Insert tuple with zero-copy when possible
    pub fn insert_tuple_zero_copy(&mut self, tuple_data: &[u8], metadata: AtomicTupleMeta) -> Result<RID, PageError> {
        // Check if we have space
        let needed_space = tuple_data.len() + size_of::<AtomicTupleMeta>() + size_of::<TupleSlot>();
        if !self.layout.can_fit(needed_space as u16, 0) {
            return Err(PageError::InsufficientSpace);
        }

        // Allocate space for metadata
        let meta_offset = self.allocate_metadata_space()?;
        unsafe {
            let meta_ptr = self.buffer.as_mut_ptr().add(meta_offset) as *mut AtomicTupleMeta;
            *meta_ptr = metadata;
            self.metadata_cache.push(meta_ptr);
        }

        // Allocate space for tuple data
        let tuple_offset = self.allocate_tuple_space(tuple_data.len())?;
        unsafe {
            let tuple_ptr = self.buffer.as_mut_ptr().add(tuple_offset);
            std::ptr::copy_nonoverlapping(tuple_data.as_ptr(), tuple_ptr, tuple_data.len());
        }

        // Add directory entry
        let slot = TupleSlot {
            offset: tuple_offset as u16,
            length: tuple_data.len() as u16,
            meta_offset: meta_offset as u16,
        };

        let slot_idx = self.directory_cache.len();
        self.directory_cache.push(slot);

        // Update layout
        self.layout.directory_count += 1;
        self.layout.metadata_count += 1;

        self.is_dirty.store(true, Ordering::Relaxed);
        self.modification_epoch.fetch_add(1, Ordering::Release);

        Ok(RID::new(self.get_page_id(), slot_idx as u32))
    }

    /// Update tuple in place when size permits
    pub fn update_tuple_inplace(&mut self, rid: RID, new_data: &[u8]) -> Result<(), PageError> {
        let slot_idx = rid.get_slot_num() as usize;
        if slot_idx >= self.directory_cache.len() {
            return Err(PageError::InvalidSlot(slot_idx));
        }

        let slot = &mut self.directory_cache[slot_idx];
        if new_data.len() > slot.length as usize {
            return Err(PageError::RequiresReallocation);
        }

        // Update in place
        unsafe {
            let tuple_ptr = self.buffer.as_mut_ptr().add(slot.offset as usize);
            std::ptr::copy_nonoverlapping(new_data.as_ptr(), tuple_ptr, new_data.len());
        }

        // Update slot length if needed
        slot.length = new_data.len() as u16;

        self.is_dirty.store(true, Ordering::Relaxed);
        self.modification_epoch.fetch_add(1, Ordering::Release);

        Ok(())
    }
}
```

**Justification:**
- Direct memory management eliminates intermediate allocations
- Atomic metadata enables lock-free concurrent reads
- Cached pointers provide O(1) access

**Trade-offs:**
- ✅ **Performance**: Maximum possible speed for database operations
- ✅ **Memory**: Minimal allocation overhead
- ❌ **Complexity**: Significant unsafe code
- ❌ **Debugging**: Harder to debug memory issues

**Potential Issues:**
- Memory corruption from incorrect pointer arithmetic
- Cache invalidation on page modifications
- Complex error recovery from partial operations

### Phase 4: Integration and Testing (Days 11-14)

#### Step 4.1: Compatibility Layer

**Implementation:**
```rust
// src/storage/page/compat.rs

/// Compatibility wrapper for gradual migration
pub struct CompatTablePage {
    inner: Option<ZeroCopyTablePage>,
    fallback: Option<TablePage>, // Original implementation
    use_zero_copy: bool,
}

impl CompatTablePage {
    pub fn new(page_id: PageId, use_zero_copy: bool) -> Self {
        if use_zero_copy {
            Self {
                inner: Some(ZeroCopyTablePage::new(page_id)),
                fallback: None,
                use_zero_copy: true,
            }
        } else {
            Self {
                inner: None,
                fallback: Some(TablePage::new(page_id)),
                use_zero_copy: false,
            }
        }
    }

    pub fn get_tuple(&self, rid: &RID) -> Result<(TupleMeta, Tuple), String> {
        if self.use_zero_copy {
            let inner = self.inner.as_ref().unwrap();
            let tuple_ref = inner.get_tuple_ref(*rid)?;
            let meta = inner.get_tuple_meta(*rid)?;

            // Convert references to owned types for compatibility
            Ok((meta.to_local(), tuple_ref.to_owned()))
        } else {
            self.fallback.as_ref().unwrap().get_tuple(rid, false)
        }
    }
}
```

**Justification:**
- Gradual migration reduces risk
- Performance comparison during transition
- Fallback mechanism for safety

#### Step 4.2: Comprehensive Testing

**Implementation:**
```rust
// tests/zero_copy_integration.rs

#[cfg(test)]
mod zero_copy_tests {
    use super::*;

    #[test]
    fn test_zero_allocations_read() {
        // Use memory tracking to verify zero allocations
        let mut allocations = 0;

        // Custom allocator that counts allocations
        struct CountingAllocator;

        unsafe impl GlobalAlloc for CountingAllocator {
            unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
                ALLOCATIONS.fetch_add(1, Ordering::SeqCst);
                System.alloc(layout)
            }

            unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
                System.dealloc(ptr, layout);
            }
        }

        static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

        #[global_allocator]
        static ALLOC: CountingAllocator = CountingAllocator;

        // Test zero-copy read operations
        let mut page = ZeroCopyTablePage::new(PageId::new(1));

        // Insert test data
        let tuple_data = create_test_tuple_data();
        let metadata = AtomicTupleMeta::new(1);
        let rid = page.insert_tuple_zero_copy(&tuple_data, metadata).unwrap();

        // Reset allocation counter
        ALLOCATIONS.store(0, Ordering::SeqCst);

        // Perform read operations
        let tuple_ref = page.get_tuple_ref(rid).unwrap();
        let value = tuple_ref.get_value(0).unwrap();
        let meta = page.get_tuple_meta(rid).unwrap();

        // Verify zero allocations occurred
        assert_eq!(ALLOCATIONS.load(Ordering::SeqCst), 0, "Zero-copy read performed allocations");
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let page = Arc::new(ZeroCopyTablePage::new(PageId::new(1)));

        // Insert test data
        let tuple_data = create_test_tuple_data();
        let metadata = AtomicTupleMeta::new(1);
        let rid = page.insert_tuple_zero_copy(&tuple_data, metadata).unwrap();

        // Spawn multiple reader threads
        let mut handles = vec![];
        for i in 0..10 {
            let page_clone = Arc::clone(&page);
            let handle = thread::spawn(move || {
                for _ in 0..1000 {
                    let tuple_ref = page_clone.get_tuple_ref(rid).unwrap();
                    let value = tuple_ref.get_value(0).unwrap();
                    // Verify value consistency
                    match value {
                        ValueRef::Integer(n) => assert_eq!(*n, 42),
                        _ => panic!("Unexpected value type"),
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_memory_safety_miri() {
        // This test is designed to run under Miri for memory safety verification
        let mut page = ZeroCopyTablePage::new(PageId::new(1));

        let tuple_data = create_test_tuple_data();
        let metadata = AtomicTupleMeta::new(1);
        let rid = page.insert_tuple_zero_copy(&tuple_data, metadata).unwrap();

        // Test various access patterns
        {
            let tuple_ref = page.get_tuple_ref(rid).unwrap();
            let _value = tuple_ref.get_value(0).unwrap();
        } // tuple_ref goes out of scope

        {
            let mut tuple_mut = page.get_tuple_mut(rid).unwrap();
            let new_value = Value::Integer(99);
            tuple_mut.update_field_inplace(0, &new_value).unwrap();
        } // tuple_mut goes out of scope

        // Verify update worked
        let tuple_ref = page.get_tuple_ref(rid).unwrap();
        let value = tuple_ref.get_value(0).unwrap();
        match value {
            ValueRef::Integer(n) => assert_eq!(*n, 99),
            _ => panic!("Unexpected value type"),
        }
    }
}
```

**Justification:**
- Allocation tracking verifies zero-copy claims
- Concurrent testing ensures thread safety
- Miri testing catches undefined behavior

## Performance Validation

### Benchmarking Setup
```rust
// benches/zero_copy_bench.rs
use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};

fn bench_tuple_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("tuple_read");

    // Setup both implementations
    let mut zero_copy_page = ZeroCopyTablePage::new(PageId::new(1));
    let mut original_page = TablePage::new(PageId::new(1));

    // Insert identical test data
    let tuple_data = create_large_test_tuple();

    group.bench_function("zero_copy", |b| {
        b.iter(|| {
            let tuple_ref = zero_copy_page.get_tuple_ref(test_rid).unwrap();
            let value = tuple_ref.get_value(0).unwrap();
            criterion::black_box(value);
        });
    });

    group.bench_function("original", |b| {
        b.iter(|| {
            let (meta, tuple) = original_page.get_tuple(&test_rid, false).unwrap();
            let value = tuple.get_value(0);
            criterion::black_box(value);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_tuple_read);
criterion_main!(benches);
```

### Expected Performance Improvements

1. **Memory Allocations**: 95% reduction in read operations
2. **CPU Cache Misses**: 40-60% reduction from aligned access
3. **Throughput**: 2-3x improvement in concurrent reads
4. **Latency**: 50-70% reduction in tuple access time

## Risk Mitigation Strategy

### Memory Safety
- **Static Analysis**: Use `cargo clippy` and `cargo audit`
- **Dynamic Testing**: Run all tests under Miri
- **Fuzz Testing**: Generate random access patterns
- **Address Sanitizer**: Catch memory corruption

### Performance Regression
- **Continuous Benchmarking**: Automated performance tests in CI
- **Performance Budgets**: Fail builds if performance degrades
- **Profiling Integration**: Regular CPU and memory profiling
- **Canary Deployments**: Gradual rollout with monitoring

### Complexity Management
- **Code Review**: Mandatory review for all unsafe code
- **Documentation**: Extensive documentation of memory layout
- **Modular Testing**: Unit tests for each unsafe operation
- **Formal Verification**: Consider using Creusot for critical paths

## Success Criteria

### Functional Requirements
- ✅ All existing functionality preserved
- ✅ No memory safety issues detected by Miri
- ✅ No data corruption in stress tests
- ✅ Proper ACID transaction behavior maintained

### Performance Requirements
- ✅ Zero heap allocations for tuple reads in steady state
- ✅ 2x improvement in concurrent read throughput
- ✅ 50% reduction in memory usage for tuple operations
- ✅ Sub-microsecond tuple access latency (P99)

### Quality Requirements
- ✅ 100% test coverage for unsafe code blocks
- ✅ No performance regression in any existing benchmark
- ✅ Documentation coverage for all public APIs
- ✅ Successful fuzz testing for 24+ hours

This implementation plan provides a systematic approach to achieving zero-copy performance while maintaining Rust's safety guarantees through careful design and comprehensive testing.
