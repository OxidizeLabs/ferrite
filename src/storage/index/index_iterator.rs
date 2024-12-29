use std::fmt::Debug;
/// For range scan of B+ tree
///
use std::marker::PhantomData;
use std::ptr::NonNull;

use crate::storage::index::b_plus_tree_index::{KeyComparator, KeyType, ValueType};
use crate::storage::page::page_types::b_plus_tree_leaf_page::BPlusTreeLeafPage;

/// Represents a mapping from key to value
pub type MappingType<K, V> = (K, V);

/// Index iterator for range scan of B+ tree.
pub struct IndexIterator<K, V, C>
where
    K: KeyType,
    V: ValueType,
    C: KeyComparator<K>,
{
    // You may define your own member variables here
    leaf_page: Option<NonNull<BPlusTreeLeafPage<K, V, C>>>,
    index: usize,
    _marker: PhantomData<(K, V, C)>,
}

impl<K, V, C> IndexIterator<K, V, C>
where
    K: KeyType,
    V: ValueType,
    C: KeyComparator<K>,
{
    /// Creates a new `IndexIterator`.
    pub fn new() -> Self {
        IndexIterator {
            leaf_page: None,
            index: 0,
            _marker: PhantomData,
        }
    }

    /// Checks if the iterator has reached the end.
    pub fn is_end(&self) -> bool {
        unimplemented!()
    }

    /// Returns a reference to the current mapping.
    pub fn value(&self) -> &MappingType<K, V> {
        unimplemented!()
    }

    /// Moves to the next element in the iterator.
    pub fn next(&mut self) -> &mut Self {
        unimplemented!()
    }
}

impl<K, V, C> PartialEq for IndexIterator<K, V, C>
where
    K: KeyType,
    V: ValueType,
    C: KeyComparator<K>,
{
    fn eq(&self, other: &Self) -> bool {
        unimplemented!()
    }
}

impl<K, V, C> Eq for IndexIterator<K, V, C>
where
    K: KeyType,
    V: ValueType,
    C: KeyComparator<K>,
{}

impl<K, V, C> Debug for IndexIterator<K, V, C>
where
    K: KeyType,
    V: ValueType,
    C: KeyComparator<K>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IndexIterator {{ ... }}")
    }
}
