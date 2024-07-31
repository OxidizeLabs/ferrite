use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::storage::index::index::{Index, IndexMetadata};
use crate::storage::table::tuple::Tuple;
use crate::common::rid::RID;
use crate::concurrency::transaction::Transaction;
use crate::container::disk_extendable_hash_table::DiskExtendableHashTable;
use crate::container::hash_function::HashFunction;
use crate::storage::index::b_plus_tree_index::{KeyComparator, KeyType, ValueType};


pub struct ExtendableHashTableIndex;


// impl<K, V, C> ExtendableHashTableIndex<K, V, C>
// where
//     K: KeyType + for<'a> From<&'a Tuple> + for<'a> From<&'a Tuple> + for<'a> From<&'a Tuple>,
//     V: ValueType,
//     C: KeyComparator<K> + Clone,
// {
//     pub fn new(
//         metadata: Arc<IndexMetadata>,
//         buffer_pool_manager: Arc<BufferPoolManager>,
//         comparator: C,
//         hash_fn: HashFunction<K>,
//     ) -> Self {
//         let container = Arc::new(Mutex::new(DiskExtendableHashTable::new(metadata.get_name().parse().unwrap(), buffer_pool_manager, comparator.clone(), hash_fn, 0, 0, 0)));
//         Self {
//             metadata,
//             comparator,
//             container,
//         }
//     }
//
//     pub fn insert_entry(&self, key: &Tuple, rid: RID, transaction: &Transaction) -> bool {
//         let index_key: K = key.into(); // Assuming you have a way to convert Tuple to K
//         self.container.lock().unwrap().insert(index_key, rid, transaction)
//     }
//
//     pub fn delete_entry(&self, key: &Tuple, rid: RID, transaction: &Transaction) {
//         let index_key: K = key.into(); // Assuming you have a way to convert Tuple to K
//         self.container.lock().unwrap().remove(&index_key, transaction);
//     }
//
//     pub fn scan_key(&self, key: &Tuple, result: &mut Vec<RID>, transaction: &Transaction) {
//         let index_key: K = key.into(); // Assuming you have a way to convert Tuple to K
//         self.container.lock().unwrap().get_value(&index_key, result, transaction);
//     }
// }
//
// impl<K, V, C> Index for ExtendableHashTableIndex<K, V, C>
// where
//     K: KeyType,
//     V: ValueType,
//     C: KeyComparator<K>,
// {
//     fn new(metadata: Box<IndexMetadata>) -> Self
//     where
//         Self: Sized
//     {
//         todo!()
//     }
//
//     fn get_metadata(&self) -> &IndexMetadata {
//         &*Arc::clone(&self.metadata)
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
