use std::cmp::Ordering;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use crate::storage::index::b_plus_tree::BPlusTree;

// Traits for KeyType and ValueType
pub trait KeyType: Clone + Ord {}
pub trait ValueType: Clone {}

// Implementing the traits for some example types
impl KeyType for i32 {}
impl ValueType for String {}

// Define KeyComparator as a trait
pub trait KeyComparator<K: KeyType> {
    fn compare(&self, lhs: &K, rhs: &K) -> Ordering;
}

// Example implementation for i32 key type comparator
pub struct I32Comparator;

impl KeyComparator<i32> for I32Comparator {
    fn compare(&self, lhs: &i32, rhs: &i32) -> Ordering {
        lhs.cmp(rhs)
    }
}

pub struct BPlusTreeIndex<K, V, C>
where
    K: KeyType,
    V: ValueType,
    C: KeyComparator<K>,
{
    comparator: C,
    container: Arc<Mutex<BPlusTree<K, V, C>>>,
    _marker: PhantomData<(K, V)>,
}

// impl<K, V, C> BPlusTreeIndex<K, V, C>
// where
//     K: KeyType,
//     V: ValueType,
//     C: KeyComparator<K> + for<'a, 'b> Fn(&'a K, &'b K),
// {
//     /// Creates a new `BPlusTreeIndex`.
//     ///
//     /// # Parameters
//     /// - `metadata`: Metadata for the index.
//     /// - `buffer_pool_manager`: The buffer pool manager.
//     ///
//     /// # Returns
//     /// A new `BPlusTreeIndex` instance.
//     pub fn new(metadata: Box<IndexMetadata>, buffer_pool_manager: Arc<BufferPoolManager>, comparator: C) -> Self {
//         unimplemented!()
//     }
//
//     /// Inserts an entry into the B+ tree index.
//     ///
//     /// # Parameters
//     /// - `key`: The key to insert.
//     /// - `rid`: The record ID.
//     /// - `transaction`: The transaction.
//     ///
//     /// # Returns
//     /// `true` if the insertion was successful, `false` otherwise.
//     pub fn insert_entry(&self, key: &Tuple, rid: RID, transaction: &Transaction) -> bool {
//         unimplemented!()
//     }
//
//     /// Deletes an entry from the B+ tree index.
//     ///
//     /// # Parameters
//     /// - `key`: The key to delete.
//     /// - `rid`: The record ID.
//     /// - `transaction`: The transaction.
//     pub fn delete_entry(&self, key: &Tuple, rid: RID, transaction: &Transaction) {
//         unimplemented!()
//     }
//
//     /// Scans the B+ tree index for a given key.
//     ///
//     /// # Parameters
//     /// - `key`: The key to scan.
//     /// - `result`: The vector to store the result.
//     /// - `transaction`: The transaction.
//     pub fn scan_key(&self, key: &Tuple, result: &mut Vec<RID>, transaction: &Transaction) {
//         unimplemented!()
//     }
//
//     /// Returns an iterator to the beginning of the B+ tree index.
//     ///
//     /// # Returns
//     /// An iterator to the beginning of the B+ tree index.
//     pub fn get_begin_iterator(&self) -> IndexIterator<K, V, C> {
//         unimplemented!()
//     }
//
//     /// Returns an iterator to the beginning of the B+ tree index for a given key.
//     ///
//     /// # Parameters
//     /// - `key`: The key to start the iterator at.
//     ///
//     /// # Returns
//     /// An iterator to the beginning of the B+ tree index for the given key.
//     pub fn get_begin_iterator_with_key(&self, key: &K) -> IndexIterator<K, V, C> {
//         unimplemented!()
//     }
//
//     /// Returns an iterator to the end of the B+ tree index.
//     ///
//     /// # Returns
//     /// An iterator to the end of the B+ tree index.
//     pub fn get_end_iterator(&self) -> IndexIterator<K, V, C> {
//         unimplemented!()
//     }
// }
//
// // We only support index table with one integer key for now in BusTub. Hardcode everything here.
// pub const TWO_INTEGER_SIZE_B_TREE: usize = 8;
// pub type IntegerKeyTypeBTree = GenericKey<TWO_INTEGER_SIZE_B_TREE>;
// pub type IntegerValueTypeBTree = RID;
// pub type IntegerComparatorTypeBTree<'a> = GenericComparator<'a, TWO_INTEGER_SIZE_B_TREE>;
// pub type BPlusTreeIndexForTwoIntegerColumn<'a> = BPlusTreeIndex<IntegerKeyTypeBTree, IntegerValueTypeBTree, IntegerComparatorTypeBTree<'a>>;
// pub type BPlusTreeIndexIteratorForTwoIntegerColumn<'a> = IndexIterator<IntegerKeyTypeBTree, IntegerValueTypeBTree, IntegerComparatorTypeBTree<'a>>;
// pub type IntegerHashFunctionType = HashFunction<IntegerKeyTypeBTree>;
