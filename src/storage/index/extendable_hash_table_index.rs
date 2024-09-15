// use std::hash::Hash;
// use std::sync::Arc;
// use parking_lot::Mutex;
// use crate::buffer::buffer_pool_manager::BufferPoolManager;
// use crate::common::rid::RID;
// use crate::concurrency::transaction::Transaction;
// use crate::container::disk_extendable_hash_table::DiskExtendableHashTable;
// use crate::container::hash_function::HashFunction;
// use crate::storage::index::b_plus_tree_index::{KeyComparator, KeyType, ValueType};
// use crate::storage::index::index::{Index, IndexMetadata};
// use crate::storage::table::tuple::Tuple;
//
// /// An index implementation using an extendable hash table.
// pub struct ExtendableHashTableIndex<K, V, C>
// where
//     K: KeyType + for<'a> From<&'a Tuple> + Hash,
//     V: ValueType,
//     C: KeyComparator<K> + Clone,
// {
//     metadata: Arc<IndexMetadata>,
//     comparator: C,
//     container: Arc<Mutex<DiskExtendableHashTable<K, RID, C>>>,
// }
//
// impl<K, V, C> ExtendableHashTableIndex<K, V, C>
// where
//     K: KeyType + for<'a> From<&'a Tuple> + Hash,
//     V: ValueType,
//     C: KeyComparator<K> + Clone,
// {
//     /// Creates a new `ExtendableHashTableIndex`.
//     ///
//     /// # Arguments
//     ///
//     /// * `metadata` - The index metadata.
//     /// * `buffer_pool_manager` - The buffer pool manager to be used.
//     /// * `comparator` - Comparator for keys.
//     /// * `hash_fn` - The hash function.
//     pub fn new(
//         metadata: Arc<IndexMetadata>,
//         buffer_pool_manager: Arc<BufferPoolManager>,
//         comparator: C,
//         hash_fn: HashFunction<K>,
//     ) -> Self {
//         let container = Arc::new(Mutex::new(DiskExtendableHashTable::new(
//             metadata.get_name().to_string(),
//             buffer_pool_manager,
//             comparator.clone(),
//             hash_fn,
//             4, // header_max_depth
//             4, // directory_max_depth
//             4, // bucket_max_size
//         )));
//
//         Self {
//             metadata,
//             comparator,
//             container,
//         }
//     }
//
//     /// Inserts a key-value pair into the index.
//     ///
//     /// # Arguments
//     ///
//     /// * `key` - The key to insert.
//     /// * `value` - The value (RID) to be associated with the key.
//     /// * `transaction` - The current transaction.
//     ///
//     /// # Returns
//     ///
//     /// Returns `true` if the insert succeeded, `false` otherwise.
//     pub fn insert_entry(&self, key: &Tuple, value: RID, transaction: &Transaction) -> bool {
//         let index_key: K = key.into();
//         self.container.lock().insert(index_key, value, Some(transaction))
//     }
//
//     /// Deletes a key-value pair from the index.
//     ///
//     /// # Arguments
//     ///
//     /// * `key` - The key to delete.
//     /// * `value` - The value (RID) associated with the key.
//     /// * `transaction` - The current transaction.
//     pub fn delete_entry(&self, key: &Tuple, value: RID, transaction: &Transaction) {
//         let index_key: K = key.into();
//         self.container.lock().remove(&index_key, Some(transaction));
//     }
//
//     /// Scans the index for entries matching the given key.
//     ///
//     /// # Arguments
//     ///
//     /// * `key` - The key to scan for.
//     /// * `result` - A vector to store the matching RIDs.
//     /// * `transaction` - The current transaction.
//     pub fn scan_key(&self, key: &Tuple, result: &mut Vec<RID>, transaction: &Transaction) {
//         let index_key: K = key.into();
//         if let Some(rid) = self.container.lock().get_value(&index_key, Some(transaction)) {
//             result.push(rid);
//         }
//     }
// }
//
// impl<K, V, C> Index for ExtendableHashTableIndex<K, V, C>
// where
//     K: KeyType + for<'a> From<&'a Tuple> + Hash,
//     V: ValueType,
//     C: KeyComparator<K> + Clone,
// {
//     fn new(metadata: Box<IndexMetadata>) -> Self
//     where
//         Self: Sized,
//     {
//         unimplemented!("Use ExtendableHashTableIndex::new() instead")
//     }
//
//     fn get_metadata(&self) -> &IndexMetadata {
//         &self.metadata
//     }
//
//     fn insert_entry(&self, key: &Tuple, rid: RID, transaction: &Transaction) -> bool {
//         self.insert_entry(key, rid, transaction)
//     }
//
//     fn delete_entry(&self, key: &Tuple, rid: RID, transaction: &Transaction) {
//         self.delete_entry(key, rid, transaction)
//     }
//
//     fn scan_key(&self, key: &Tuple, result: &mut Vec<RID>, transaction: &Transaction) {
//         self.scan_key(key, result, transaction)
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::common::config::PageId;
//     use crate::storage::disk::disk_manager::FileDiskManager;
//     use crate::storage::index::index::IndexMetadata;
//     use std::cmp::Ordering;
//
//     #[derive(Clone)]
//     struct TestComparator;
//
//     impl KeyComparator<i32> for TestComparator {
//         fn compare(&self, a: &i32, b: &i32) -> Ordering {
//             a.cmp(b)
//         }
//     }
//
//     fn create_test_index() -> ExtendableHashTableIndex<i32, RID, TestComparator> {
//         let disk_manager = Arc::new(FileDiskManager::new("test.db".to_string()));
//         let buffer_pool_manager = Arc::new(BufferPoolManager::new(10, disk_manager));
//         let metadata = Arc::new(IndexMetadata::new(
//             "test_index".to_string(),
//             "test_table".to_string(),
//             vec!["test_column".to_string()],
//             0,
//         ));
//         let comparator = TestComparator;
//         let hash_fn = HashFunction::new();
//
//         ExtendableHashTableIndex::new(metadata, buffer_pool_manager, comparator, hash_fn)
//     }
//
//     #[test]
//     fn test_insert_and_scan() {
//         let index = create_test_index();
//         let transaction = Transaction::new(0);
//
//         // Create test tuples
//         let tuple1 = Tuple::new(vec![1i32.into()]);
//         let tuple2 = Tuple::new(vec![2i32.into()]);
//
//         // Insert entries
//         assert!(index.insert_entry(&tuple1, RID::new(PageId::new(1), 1), &transaction));
//         assert!(index.insert_entry(&tuple2, RID::new(PageId::new(1), 2), &transaction));
//
//         // Scan for entries
//         let mut result = Vec::new();
//         index.scan_key(&tuple1, &mut result, &transaction);
//         assert_eq!(result.len(), 1);
//         assert_eq!(result[0], RID::new(PageId::new(1), 1));
//
//         result.clear();
//         index.scan_key(&tuple2, &mut result, &transaction);
//         assert_eq!(result.len(), 1);
//         assert_eq!(result[0], RID::new(PageId::new(1), 2));
//     }
//
//     #[test]
//     fn test_delete() {
//         let index = create_test_index();
//         let transaction = Transaction::new(0);
//
//         // Create test tuple
//         let tuple = Tuple::new(vec![1i32.into()]);
//
//         // Insert and then delete an entry
//         assert!(index.insert_entry(&tuple, RID::new(PageId::new(1), 1), &transaction));
//         index.delete_entry(&tuple, RID::new(PageId::new(1), 1), &transaction);
//
//         // Scan for the deleted entry
//         let mut result = Vec::new();
//         index.scan_key(&tuple, &mut result, &transaction);
//         assert_eq!(result.len(), 0);
//     }
//
//     // Add more tests as needed
// }