//! Table storage engine components.
//!
//! This module contains the implementation of heap-based tables, including
//! tuple storage, MVCC versioning, page management, and scanning iterators.

mod record;
pub mod table_heap;
pub mod table_iterator;
pub mod transactional_table_heap;
pub mod tuple;
