// use parking_lot::RwLock;
// use crate::storage::index::b_plus_tree::BPlusTree;
// use crate::types_db::value::Value;
// use std::cmp::Ordering;
// use std::marker::PhantomData;
// use std::sync::Arc;
// use crate::storage::page::page::{PageTrait, PAGE_TYPE_OFFSET};
//
// use crate::buffer::buffer_pool_manager::BufferPoolManager;
// use crate::catalog::schema::Schema;
// use crate::common::{config::{PageId, INVALID_PAGE_ID}, rid::RID};
// use crate::concurrency::transaction::Transaction;
// use crate::container::hash_function::HashFunction;
// use crate::storage::index::generic_key::{GenericKey, GenericKeyComparator};
// use crate::storage::index::index::Index;
// use crate::storage::index::index_iterator_mem::IndexIterator;
// use crate::storage::page::page::{Page, PageType};
// use crate::storage::page::page_types::{
//     b_plus_tree_internal_page::BPlusTreeInternalPage,
//     b_plus_tree_leaf_page::BPlusTreeLeafPage,
//     b_plus_tree_header_page::BPlusTreeHeaderPage,
// };
// use crate::storage::table::tuple::Tuple;
// use crate::types_db::integer_type::IntegerType;
// use crate::types_db::type_id::TypeId::Integer;
// use crate::buffer::lru_k_replacer::AccessType;
// use crate::types_db::types::Type;
//
// /// Trait for types that can be used as keys in a B+ tree index
// pub trait KeyType: Clone + Ord {}
//
// /// Trait for types that can be used as values in a B+ tree index
// pub trait ValueType: Clone {}
//
// // Implementation of KeyType for basic types
// impl KeyType for i32 {}
// impl ValueType for String {}
// impl ValueType for RID {}
//
// /// Trait for comparing keys in a B+ tree
// pub trait KeyComparator<K: KeyType> {
//     fn compare(&self, lhs: &K, rhs: &K) -> Ordering;
// }
//
// /// Example comparator for i32 keys
// pub struct I32Comparator;
//
// impl KeyComparator<i32> for I32Comparator {
//     fn compare(&self, lhs: &i32, rhs: &i32) -> Ordering {
//         lhs.cmp(rhs)
//     }
// }
//
// /// Structure representing metadata about an index
// #[derive(Debug, Clone)]
// pub struct IndexInfo {
//     pub name: String,
//     pub key_schema: Schema,
//     pub unique_index: bool,
// }
//
// /// Main B+ tree index structure
// pub struct BPlusTreeIndex<K, V, C>
// where
//     K: KeyType + Send + Sync,
//     V: ValueType + Send + Sync,
//     C: KeyComparator<K> + Send + Sync,
// {
//     /// The key comparator
//     comparator: C,
//     /// Index metadata
//     metadata: Box<IndexInfo>,
//     /// Buffer pool manager for page operations
//     buffer_pool_manager: Arc<BufferPoolManager>,
//     /// ID of the header page for this B+ tree
//     header_page_id: PageId,
//     /// Marker for generic types
//     _marker: PhantomData<(K, V)>,
// }
//
// impl<K, V, C> BPlusTreeIndex<K, V, C>
// where
//     K: KeyType + Send + Sync + 'static,
//     V: ValueType + Send + Sync + 'static,
//     C: KeyComparator<K> + Fn(&K, &K) -> Ordering + Send + Sync + 'static + Clone,
// {
//     /// Create a new B+ tree index
//     pub fn new(
//         metadata: Box<IndexInfo>,
//         buffer_pool_manager: Arc<BufferPoolManager>,
//         comparator: C,
//     ) -> Self {
//         // Allocate a header page for the B+ tree
//         let header_page = buffer_pool_manager
//             .new_page::<BPlusTreeHeaderPage>()
//             .expect("Failed to allocate header page");
//         let header_page_id = header_page.get_page_id();
//
//         // Initialize the header page
//         let mut header = BPlusTreeHeaderPage::new(header_page_id);
//         header.set_root_page_id(INVALID_PAGE_ID);
//
//         // Write the header page to disk
//         let header_bytes = header.serialize();
//         let mut header_page_mut = header_page.write();
//         header_page_mut.set_data(0, &header_bytes).expect("Failed to set header data");
//         header_page_mut.set_dirty(true);
//
//         if !buffer_pool_manager.unpin_page(header_page_id, true, AccessType::Index) {
//             panic!("Failed to unpin header page");
//         }
//
//         Self {
//             comparator,
//             metadata,
//             buffer_pool_manager,
//             header_page_id,
//             _marker: PhantomData,
//         }
//     }
//
//     /// Get the root page ID from the header page
//     pub fn get_root_page_id(&self) -> PageId {
//         let header_page = self.buffer_pool_manager
//             .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
//             .expect("Failed to fetch header page");
//
//         let header_data = header_page.read().get_data().to_vec();
//         let header = BPlusTreeHeaderPage::deserialize(&header_data, self.header_page_id);
//
//         if !self.buffer_pool_manager.unpin_page(self.header_page_id, false, AccessType::Index) {
//             panic!("Failed to unpin header page");
//         }
//
//         header.get_root_page_id()
//     }
//
//     /// Update the root page ID in the header page
//     fn update_root_page_id(&self, new_root_id: PageId) {
//         let header_page = self.buffer_pool_manager
//             .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
//             .expect("Failed to fetch header page");
//
//         let header_data = header_page.read().get_data().to_vec();
//         let mut header = BPlusTreeHeaderPage::deserialize(&header_data, self.header_page_id);
//
//         header.set_root_page_id(new_root_id);
//
//         let header_bytes = header.serialize();
//         let mut header_page_mut = header_page.write();
//         header_page_mut.set_data(0, &header_bytes).expect("Failed to set header data");
//         header_page_mut.set_dirty(true);
//
//         if !self.buffer_pool_manager.unpin_page(self.header_page_id, true, AccessType::Index) {
//             panic!("Failed to unpin header page");
//         }
//     }
//
//     /// Check if the B+ tree is empty
//     pub fn is_empty(&self) -> bool {
//         self.get_root_page_id() == INVALID_PAGE_ID
//     }
//
//     /// Convert a tuple to a key used by the B+ tree
//     fn key_from_tuple(&self, tuple: &Tuple) -> K {
//         // For now, we'll assume the key is a single integer field
//         // This should be generalized based on the actual key type
//         if let Some(value) = tuple.get_values().first() {
//             match value.as_integer() {
//                 Ok(int_value) => {
//                     // This is a simplification - in a real implementation we would properly
//                     // convert the value based on K's type and handle errors
//                     let k_value: K = unsafe { std::mem::transmute_copy(&int_value) };
//                     return k_value;
//                 },
//                 Err(_) => panic!("Failed to convert tuple key to integer")
//             }
//         }
//
//         panic!("Failed to convert tuple to key: invalid key type or missing key fields");
//     }
//
//     /// Insert a key-value pair into the B+ tree
//     pub fn insert_entry(&self, key: &Tuple, rid: RID, _transaction: &Transaction) -> bool {
//         let key = self.key_from_tuple(key);
//         let value = rid; // No need to cast, RID is already a ValueType
//
//         // If tree is empty, create a new root leaf page
//         if self.is_empty() {
//             let new_leaf_page = self.buffer_pool_manager
//                 .new_page::<BPlusTreeLeafPage<K, V, C>>()
//                 .expect("Failed to allocate leaf page");
//
//             let new_leaf_id = new_leaf_page.get_page_id();
//             let mut leaf_page = BPlusTreeLeafPage::new_with_options(
//                 new_leaf_id,
//                 4, // Maximum size for demonstration
//                 self.comparator.clone()
//             );
//
//             // Insert the key-value pair
//             leaf_page.insert_key_value(key, value);
//
//             // Serialize and store the page
//             let mut page_data = vec![0u8; 4096]; // Use actual page size
//             leaf_page.serialize(&mut page_data);
//
//             let mut page_mut = new_leaf_page.write();
//             page_mut.set_data(0, &page_data).expect("Failed to set leaf page data");
//             page_mut.set_dirty(true);
//
//             if !self.buffer_pool_manager.unpin_page(new_leaf_id, true, AccessType::Index) {
//                 panic!("Failed to unpin leaf page");
//             }
//
//             // Update the root page ID
//             self.update_root_page_id(new_leaf_id);
//
//             return true;
//         }
//
//         // For non-empty tree, find the leaf page where the key should be inserted
//         let root_page_id = self.get_root_page_id();
//         let mut current_page_id = root_page_id;
//
//         loop {
//             let page = self.buffer_pool_manager
//                 .fetch_page(current_page_id)
//                 .expect("Failed to fetch page");
//
//             let page_type = {
//                 let page_guard = page.read();
//                 let page_data = page_guard.get_data();
//                 PageType::from_u8(page_data[PAGE_TYPE_OFFSET]).unwrap_or(PageType::Invalid)
//             };
//
//             match page_type {
//                 PageType::BTreeLeaf => {
//                     // We've found a leaf page, try to insert
//                     let mut leaf_page = BPlusTreeLeafPage::new_with_options(
//                         current_page_id,
//                         4, // Maximum size for demonstration
//                         self.comparator.clone()
//                     );
//                     let page_data = page.read::<BPlusTreeLeafPage<K, V, C>>().get_data().to_vec();
//                     leaf_page.deserialize(&page_data);
//
//                     if leaf_page.insert_key_value(key.clone(), value.clone()) {
//                         // Insert successful, update the page
//                         let mut page_data = vec![0u8; 4096];
//                         leaf_page.serialize(&mut page_data);
//
//                         let mut page_mut = page.write::<BPlusTreeLeafPage<K, V, C>>();
//                         page_mut.set_data(0, &page_data).expect("Failed to set page data");
//                         page_mut.set_dirty(true);
//                         drop(page_mut);
//
//                         if !self.buffer_pool_manager.unpin_page(current_page_id, true, AccessType::Index) {
//                             panic!("Failed to unpin page");
//                         }
//                         return true;
//                     } else {
//                         // Page is full, need to split
//                         let new_leaf_page = self.buffer_pool_manager
//                             .new_page::<BPlusTreeLeafPage<K, V, C>>()
//                             .expect("Failed to allocate new leaf page");
//
//                         let new_leaf_id = new_leaf_page.get_page_id();
//                         let mut new_leaf = BPlusTreeLeafPage::new_with_options(
//                             new_leaf_id,
//                             4, // Maximum size for demonstration
//                             self.comparator.clone()
//                         );
//
//                         // Split the keys between old and new leaf
//                         let mid = leaf_page.get_size() / 2;
//                         for i in mid..leaf_page.get_size() {
//                             if let (Some(k), Some(v)) = (leaf_page.get_key_at(i), leaf_page.get_value_at(i)) {
//                                 new_leaf.insert_key_value(k.clone(), v.clone());
//                             }
//                         }
//
//                         // Insert the new key-value pair into the appropriate leaf
//                         if (self.comparator)(&key, leaf_page.get_key_at(mid).unwrap()) == Ordering::Less {
//                             leaf_page.insert_key_value(key, value);
//                         } else {
//                             new_leaf.insert_key_value(key, value);
//                         }
//
//                         // Update the old leaf's size
//                         leaf_page.set_size(mid);
//
//                         // Link the leaves
//                         new_leaf.set_next_page_id(leaf_page.get_next_page_id());
//                         leaf_page.set_next_page_id(Some(new_leaf_id));
//
//                         // Serialize and store both pages
//                         let mut old_page_data = vec![0u8; 4096];
//                         leaf_page.serialize(&mut old_page_data);
//
//                         let mut new_page_data = vec![0u8; 4096];
//                         new_leaf.serialize(&mut new_page_data);
//
//                         let mut old_page_mut = page.write::<BPlusTreeLeafPage<K, V, C>>();
//                         old_page_mut.set_data(0, &old_page_data).expect("Failed to set old leaf data");
//                         old_page_mut.set_dirty(true);
//                         drop(old_page_mut);
//
//                         let mut new_page_mut = new_leaf_page.write::<BPlusTreeLeafPage<K, V, C>>();
//                         new_page_mut.set_data(0, &new_page_data).expect("Failed to set new leaf data");
//                         new_page_mut.set_dirty(true);
//                         drop(new_page_mut);
//
//                         if !self.buffer_pool_manager.unpin_page(current_page_id, true, AccessType::Index) {
//                             panic!("Failed to unpin old leaf page");
//                         }
//                         if !self.buffer_pool_manager.unpin_page(new_leaf_id, true, AccessType::Index) {
//                             panic!("Failed to unpin new leaf page");
//                         }
//
//                         // Update parent node with new leaf
//                         // This would involve creating a new internal node if needed
//                         // For now, we'll just make the new leaf the root
//                         self.update_root_page_id(new_leaf_id);
//                         return true;
//                     }
//                 }
//                 PageType::BTreeInternal => {
//                     let mut internal_page = BPlusTreeInternalPage::<K, V>::new_with_options(
//                         current_page_id,
//                         4, // Maximum size for demonstration
//                         self.comparator.clone()
//                     );
//                     let page_data = page.read().get_data().to_vec();
//                     internal_page.deserialize(&page_data);
//
//                     let child_index = internal_page.find_key_index(&key);
//                     current_page_id = internal_page.get_value_at(child_index)
//                         .expect("Failed to get child page ID");
//
//                     if !self.buffer_pool_manager.unpin_page(current_page_id, false, AccessType::Index) {
//                         panic!("Failed to unpin internal page");
//                     }
//                 }
//                 _ => panic!("Invalid page type in B+ tree"),
//             }
//         }
//     }
//
//     /// Delete an entry from the B+ tree
//     pub fn delete_entry(&self, key: &Tuple, rid: RID, _transaction: &Transaction) {
//         let key = self.key_from_tuple(key);
//
//         // Implementation would go here, similar to insert but handling merges
//         // when pages become underfilled
//     }
//
//     /// Scan for keys matching the given tuple
//     pub fn scan_key(&self, key: &Tuple, result: &mut Vec<RID>, _transaction: &Transaction) {
//         let key = self.key_from_tuple(key);
//         let root_page_id = self.get_root_page_id();
//
//         if root_page_id == INVALID_PAGE_ID {
//             return;
//         }
//
//         // Implementation would fetch the appropriate pages from disk
//         // and search for matching keys, adding their RIDs to result
//     }
//
//     /// Get an iterator pointing to the beginning of the B+ tree
//     pub fn get_begin_iterator(&self) -> IndexIterator {
//         unimplemented!("Iterator not implemented")
//     }
//
//     /// Get an iterator pointing to the first occurrence of the given key
//     pub fn get_begin_iterator_with_key(&self, key: &K) -> IndexIterator {
//         unimplemented!("Iterator not implemented")
//     }
//
//     /// Get an iterator pointing to the end of the B+ tree
//     pub fn get_end_iterator(&self) -> IndexIterator {
//         unimplemented!("Iterator not implemented")
//     }
//
//     /// Update metadata about the B+ tree (height, number of keys)
//     fn update_tree_metadata(&self, height: Option<u32>, num_keys_delta: Option<isize>) {
//         let header_page = self.buffer_pool_manager
//             .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
//             .expect("Failed to fetch header page");
//
//         let header_data = header_page.read().get_data().to_vec();
//         let mut header = BPlusTreeHeaderPage::deserialize(&header_data, self.header_page_id);
//
//         if let Some(h) = height {
//             header.set_tree_height(h);
//         }
//
//         if let Some(delta) = num_keys_delta {
//             if delta > 0 {
//                 // Increment
//                 for _ in 0..delta {
//                     header.increment_num_keys();
//                 }
//             } else {
//                 // Decrement
//                 for _ in 0..delta.abs() {
//                     header.decrement_num_keys();
//                 }
//             }
//         }
//
//         let header_bytes = header.serialize();
//         let mut header_page_mut = header_page.write();
//         header_page_mut.set_data(0, &header_bytes).expect("Failed to set header data");
//         header_page_mut.set_dirty(true);
//
//         if !self.buffer_pool_manager.unpin_page(self.header_page_id, true, AccessType::Index) {
//             panic!("Failed to unpin header page");
//         }
//     }
// }
//
// impl<K, V, C> Index for BPlusTreeIndex<K, V, C>
// where
//     K: KeyType + Send + Sync + 'static,
//     V: ValueType + Send + Sync + 'static,
//     C: KeyComparator<K> + Fn(&K, &K) -> Ordering + Send + Sync + 'static + Clone,
// {
//     fn new(metadata: Box<IndexInfo>) -> Self
//     wherea
//         Self: Sized,
//     {
//         panic!("BPlusTreeIndex::new() requires buffer_pool_manager and comparator parameters, use specific constructor instead");
//     }
//
//     fn get_index_column_count(&self) -> u32 {
//         self.metadata.key_schema.get_column_count()
//     }
//
//     fn get_index_name(&self) -> String {
//         self.metadata.name.clone()
//     }
//
//     fn get_key_schema(&self) -> Schema {
//         self.metadata.key_schema.clone()
//     }
//
//     fn get_key_attrs(&self) -> Vec<usize> {
//         vec![] // TODO: Implement key attributes
//     }
//
//     fn to_string(&self) -> String {
//         format!("BPlusTreeIndex: {}", self.metadata.name)
//     }
//
//     fn insert_entry(&mut self, key: &Tuple, rid: RID, transaction: &Transaction) -> bool {
//         self.insert_entry(key, rid, transaction)
//     }
//
//     fn delete_entry(&self, key: &Tuple, rid: RID, transaction: &Transaction) -> bool {
//         self.delete_entry(key, rid, transaction);
//         true // Assuming delete_entry always succeeds
//     }
//
//     fn scan_key(
//         &self,
//         key: &Tuple,
//         transaction: &Transaction,
//     ) -> Result<Vec<(Value, RID)>, String> {
//         let mut rids = Vec::new();
//         self.scan_key(key, &mut rids, transaction);
//
//         // Convert RIDs to (Value, RID) pairs
//         let key_value = Value::new(Integer);
//         Ok(rids.into_iter().map(|rid| (key_value.clone(), rid)).collect())
//     }
//
//     fn create_iterator(&self, start_key: Option<Tuple>, end_key: Option<Tuple>) -> IndexIterator {
//         // Convert to B+ tree specific iterator
//         let metadata = Arc::new(crate::storage::index::index::IndexInfo::new(
//             self.metadata.key_schema.clone(),
//             self.metadata.name.clone(),
//             0, // TODO: Get proper index OID
//             "".to_string(), // TODO: Get proper table name
//             0, // TODO: Get proper key size
//             false, // TODO: Get proper primary key status
//             crate::storage::index::index::IndexType::BPlusTreeIndex,
//             vec![], // TODO: Get proper key attributes
//         ));
//         let tree = Arc::new(RwLock::new(BPlusTree::new(4, metadata)));
//         IndexIterator::new(tree, start_key, end_key)
//     }
//
//     fn create_point_iterator(&self, key: &Tuple) -> IndexIterator {
//         // For a point query, start and end key are the same
//         let key_clone = key.clone();
//         self.create_iterator(Some(key_clone.clone()), Some(key_clone))
//     }
//
//     fn get_metadata(&self) -> Arc<crate::storage::index::index::IndexInfo> {
//         Arc::new(crate::storage::index::index::IndexInfo::new(
//             self.metadata.key_schema.clone(),
//             self.metadata.name.clone(),
//             0, // TODO: Get proper index OID
//             "".to_string(), // TODO: Get proper table name
//             0, // TODO: Get proper key size
//             false, // TODO: Get proper primary key status
//             crate::storage::index::index::IndexType::BPlusTreeIndex,
//             vec![], // TODO: Get proper key attributes
//         ))
//     }
// }
//
// // Type definitions for commonly used B+ tree index configurations
// pub const TWO_INTEGER_SIZE_B_TREE: usize = 8;
// pub type IntegerKeyTypeBTree = GenericKey<IntegerType, TWO_INTEGER_SIZE_B_TREE>;
// pub type IntegerValueTypeBTree = RID;
// pub type IntegerComparatorTypeBTree<'a> =
// GenericKeyComparator<IntegerType, TWO_INTEGER_SIZE_B_TREE>;
// pub type BPlusTreeIndexForTwoIntegerColumn<'a> =
// BPlusTreeIndex<IntegerKeyTypeBTree, IntegerValueTypeBTree, IntegerComparatorTypeBTree<'a>>;
// pub type IntegerHashFunctionType = HashFunction<IntegerKeyTypeBTree>;