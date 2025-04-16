# tkdb
DBMS written in Rust inspired by CMU 15-445/645 

# B+ Tree Internal Node Implementation

This project implements a B+ tree data structure with a focus on internal node management. The implementation provides efficient key-value storage and retrieval operations.

## Overview

Internal nodes in a B+ tree serve as routing elements, organizing and directing operations through the tree structure. They don't store actual data (which is handled by leaf nodes) but instead guide search, insert, and delete operations to the correct subtrees.

## Structure and Organization

### Keys as Routing Elements
- Keys act as dividers or routing elements
- They don't store actual data (reserved for leaf nodes)
- Guide search/insert/delete operations to correct subtrees

### Pointer-Key Relationship
For an internal node with n keys, there are n+1 pointers to child nodes:

```
Keys:    [k₁, k₂, ..., kₙ]
Pointers: [p₀, p₁, p₂, ..., pₙ]
```

The relationship works as follows:
- p₀ points to nodes with keys < k₁
- p₁ points to nodes with keys ≥ k₁ and < k₂
- p₂ points to nodes with keys ≥ k₂ and < k₃
- ...
- pₙ points to nodes with keys ≥ kₙ

### Sorted Order
- Keys must be maintained in sorted order
- Enables efficient binary search to find correct child pointer

## Page-Based B+ Tree Structure

### Page Types
A B+ tree implementation typically requires several page types:
1. **Header Page**: Stores metadata about the B+ tree (root page ID, tree height, etc.)
2. **Internal Node Pages**: Store keys and child pointers (as described above)
3. **Leaf Node Pages**: Store keys and actual values or record pointers
4. **Overflow Pages**: Optional, for handling large values that don't fit in leaf pages

### Page Layout
Each page should have a fixed size (typically 4KB or 8KB to match OS page size) and include:

1. **Page Header**:
   - Page type identifier (internal, leaf, header, etc.)
   - Page ID
   - Parent page ID (optional for faster traversal)
   - Number of keys/entries
   - Next/previous page pointers (for leaf pages, to support range queries)

2. **Page Body**:
   - For internal pages: keys and child page IDs
   - For leaf pages: keys and values (or record IDs)

### Memory Management

B+ trees should be integrated with a buffer pool manager to handle:

1. **Page Fetching**:
   - Load pages from disk to memory
   - Track page references with pin count

2. **Page Eviction**:
   - Implement replacement policies (LRU, CLOCK, etc.)
   - Write dirty pages back to disk

3. **Page Allocation**:
   - Allocate new pages for splits
   - Reclaim deleted pages

### Implementation Approach

1. **Tree Structure**:
   ```
   ┌─────────────┐
   │ Header Page │
   └──────┬──────┘
          │
          ▼
   ┌─────────────┐
   │  Root Page  │ (Internal or Leaf)
   └──────┬──────┘
          │
     ┌────┴────┐
     ▼         ▼
   ┌─────┐   ┌─────┐
   │Page1│   │Page2│  ...
   └─────┘   └─────┘
   ```

2. **Page Serialization**:
   - Develop fixed-size serialization format for each page type
   - Include checksums for data integrity (optional)
   - Ensure format allows efficient binary search

3. **Concurrency Control**:
   - Implement latching protocols for page access
   - Consider lock crabbing/coupling for efficient concurrent access

### Page IDs as Pointers

In a disk-based system:
- Physical pointers are not used
- Instead, use page IDs to reference child nodes
- Page IDs map to file offsets or logical storage locations
- The buffer pool manager translates page IDs to in-memory pages

### Optimizations

1. **Bulk Loading**:
   - Build the tree bottom-up for initial data loading
   - More efficient than individual insertions

2. **Variable-Size Pages**:
   - Optimize for different workloads
   - Root and top-level nodes might be kept in smaller pages

3. **Page Compression**:
   - Compress leaf pages to save storage
   - Prefix compression for keys with common prefixes

## Operations

### Insertion
1. Key-value pair insertion:
   - Key determines which child to navigate to
   - New key added in sorted order during split operations
   - Child pointer associated with key typically goes to its right

### Searching
1. Compare target key with keys in node
2. Follow appropriate pointer based on comparison
3. Repeat until reaching leaf node

### Deletion
1. When removing key k₁:
   - Remove pointer p₁
   - Reorganize remaining pointers
   - Maintain tree structure

## Implementation Details

### Key Storage and Pointer Management
- Store keys in sorted order
- For each key at index i:
  - Pointer at i+1 represents its "right" child
  - Pointer at index 0 is the leftmost child
- Adjust pointers when inserting/removing keys

### Method Requirements
Methods like `find_key_index`, `insert_key_value` must maintain these invariants:
1. Keys remain sorted
2. Each key has correct pointer relationships
3. Leftmost pointer exists
4. n+1 pointers for n keys

### Example Structure
```
Internal Node:
Keys:    [2, 5, 8]
Pointers: [p₀, p₁, p₂, p₃]

Where:
- p₀ → nodes with keys < 2
- p₁ → nodes with 2 ≤ keys < 5
- p₂ → nodes with 5 ≤ keys < 8
- p₃ → nodes with keys ≥ 8
```

## Testing

The implementation includes comprehensive test cases to verify:
- Key-pointer relationships
- Insertion and deletion operations
- Edge cases
- Page management
- Serialization/deserialization

## Usage

```rust
// Create a new internal page with integer keys
let page = BPlusTreeInternalPage::new_with_options(page_id, max_size, int_comparator);

// Insert key-value pairs
page.insert_key_value(2, 3);
page.insert_key_value(1, 2);
page.insert_key_value(4, 5);
page.insert_key_value(3, 4);

// Retrieve values
let value = page.get_value_at(1);
```

## License

[Add your license information here]
