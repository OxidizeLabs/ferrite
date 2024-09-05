// use crate::buffer::buffer_pool_manager::BufferPoolManager;
// use crate::common::config::PageId;
// use crate::concurrency::transaction::Transaction;
// use crate::container::hash_function::HashFunction;
// use crate::storage::index::generic_key::GenericKeyComparator;
// use crate::storage::page::page_guard::SpecificPageReadGuard;
// use crate::storage::page::page_types::hash_table_header_page::HashTableHeaderPage;
// use crate::storage::page::page_types::{
//     extendable_hash_table_bucket_page::ExtendableHTableBucketPage,
//     extendable_hash_table_directory_page::ExtendableHTableDirectoryPage,
//     extendable_hash_table_header_page::ExtendableHTableHeaderPage,
// };
// use log::{debug, error, info, warn};
// use std::fmt::Debug;
// use std::hash::Hash;
// use std::marker::PhantomData;
// use std::string::String;
// use std::sync::Arc;
// use std::vec::Vec;
//
// /// `DiskExtendableHashTable` represents an extendable hash table that operates on disk.
// ///
// /// # Type Parameters
// /// - `K`: The key type.
// /// - `V`: The value type.
// /// - `C`: The comparator type used for key comparison.
// pub struct DiskExtendableHashTable<K, V, C>
// where
//     C: GenericKeyComparator<K, N>,
// {
//     index_name: String,
//     bpm: Arc<BufferPoolManager>,
//     cmp: C,
//     hash_fn: HashFunction<K>,
//     header_max_depth: u32,
//     directory_max_depth: u32,
//     bucket_max_size: usize,
//     header_page_id: PageId,
//     _marker: PhantomData<V>,
// }
//
// impl<K, V, C> DiskExtendableHashTable<K, V, C>
// where
//     K: Eq + Hash + Clone + Debug + 'static,
//     V: Clone + Debug,
//     C: GenericKeyComparator<K, N> + Clone,
// {
//     /// Creates a new `DiskExtendableHashTable`.
//     ///
//     /// # Arguments
//     /// - `name`: The name of the hash table.
//     /// - `bpm`: The buffer pool manager to be used.
//     /// - `cmp`: The comparator for keys.
//     /// - `hash_fn`: The hash function.
//     /// - `header_max_depth`: The max depth allowed for the header page.
//     /// - `directory_max_depth`: The max depth allowed for the directory page.
//     /// - `bucket_max_size`: The max size allowed for the bucket page array.
//     ///
//     /// # Returns
//     /// A new instance of `DiskExtendableHashTable`.
//     pub fn new(
//         name: String,
//         bpm: Arc<BufferPoolManager>,
//         cmp: C,
//         hash_fn: HashFunction<K>,
//         header_max_depth: u32,
//         directory_max_depth: u32,
//         bucket_max_size: usize,
//     ) -> Self {
//         info!("Creating a new DiskExtendableHashTable: {}", name);
//
//         let mut header_guard = match bpm.new_page_guarded() {
//             Some(guard) => guard,
//             None => {
//                 error!("Failed to create a new page for header");
//                 panic!("Cannot proceed without a header page");
//             }
//         };
//         let header_page = header_guard.as_type_mut::<ExtendableHTableHeaderPage>();
//         header_page.init(header_max_depth);
//         let header_page_id = header_guard.get_page_id();
//
//         info!("DiskExtendableHashTable created with header page ID: {}", header_page_id);
//
//         Self {
//             index_name: name,
//             bpm,
//             cmp,
//             hash_fn,
//             header_max_depth,
//             directory_max_depth,
//             bucket_max_size,
//             header_page_id,
//             _marker: Default::default(),
//         }
//     }
//
//     /// Inserts a key-value pair into the hash table.
//     ///
//     /// # Arguments
//     /// - `key`: The key to insert.
//     /// - `value`: The value to be associated with the key.
//     /// - `transaction`: The current transaction (optional).
//     ///
//     /// # Returns
//     /// `true` if the insert succeeded, `false` otherwise.
//     pub fn insert(&self, key: &K, value: &V, _transaction: Option<&Transaction>) -> bool {
//         let hash = self.hash(key);
//         debug!("Inserting key: {:?}, hash: {}", key, hash);
//
//         let mut header_page_guard = match self.bpm.fetch_page_basic(self.header_page_id) {
//             Some(guard) => guard,
//             None => {
//                 error!("Failed to fetch header page");
//                 return false;
//             }
//         };
//
//         let mut header_page_guard_clone = Arc::make_mut(&mut header_page_guard);
//         let mut header_page = header_page_guard_clone.as_type_mut::<ExtendableHTableHeaderPage>();
//
//         let directory_idx = header_page.hash_to_directory_index(hash);
//         debug!("Calculated directory index: {}", directory_idx);
//
//         let Some(directory_page_id) = header_page.get_directory_page_id(directory_idx as usize) else { todo!() };
//         let mut directory_page_guard = match self.bpm.fetch_page_basic(directory_page_id) {
//             Some(guard) => guard,
//             None => {
//                 error!("Failed to fetch directory page ID: {:?}", directory_page_id);
//                 return false;
//             }
//         };
//
//
//         let mut directory_page_lock = Arc::make_mut(&mut directory_page_guard);
//         let directory_page = directory_page_lock.as_type_mut::<ExtendableHTableDirectoryPage>();
//
//         let bucket_idx = hash & ((1 << self.directory_max_depth) - 1);
//         debug!("Calculated bucket index: {}", bucket_idx);
//
//         let Some(bucket_page_id) = directory_page.get_bucket_page_id(bucket_idx as usize) else { todo!() };
//         let mut bucket_page_guard = match self.bpm.fetch_page_basic(bucket_page_id) {
//             Some(guard) => guard,
//             None => {
//                 error!("Failed to fetch bucket page ID: {}", bucket_page_id);
//                 return false;
//             }
//         };
//
//         let mut bucket_page_lock = Arc::make_mut(&mut bucket_page_guard);
//         let bucket_page = bucket_page_lock.as_type_mut::<ExtendableHTableBucketPage<K, V, C>>();
//
//         let success = bucket_page.insert(key.clone(), value.clone(), &self.cmp);
//
//         if success {
//             info!("Successfully inserted key-value pair: {:?} -> {:?}", key, value);
//         } else {
//             warn!("Failed to insert key-value pair: {:?} -> {:?}", key, value);
//         }
//
//         success
//     }
//
//     /// Removes a key-value pair from the hash table.
//     ///
//     /// # Arguments
//     /// - `key`: The key to delete.
//     /// - `transaction`: The current transaction (optional).
//     ///
//     /// # Returns
//     /// `true` if the remove succeeded, `false` otherwise.
//     pub fn remove(&self, key: &K, _transaction: Option<&Transaction>) -> bool {
//         let hash = self.hash(key);
//         debug!("Removing key: {:?}, hash: {}", key, hash);
//
//         let mut header_page_guard = match self.bpm.fetch_page_basic(self.header_page_id) {
//             Some(guard) => guard,
//             None => {
//                 error!("Failed to fetch header page");
//                 return false;
//             }
//         };
//         let mut header_page_lock = Arc::make_mut(&mut header_page_guard);
//         let header_page = header_page_lock.as_type_mut::<ExtendableHTableHeaderPage>();
//
//         let directory_idx = header_page.hash_to_directory_index(hash);
//         debug!("Calculated directory index: {}", directory_idx);
//
//         let Some(directory_page_id) = header_page.get_directory_page_id(directory_idx as usize) else { todo!() };
//         let mut directory_page_guard = match self.bpm.fetch_page_basic(directory_page_id) {
//             Some(guard) => guard,
//             None => {
//                 error!("Failed to fetch directory page ID: {}", directory_page_id);
//                 return false;
//             }
//         };
//
//         let mut directory_page_lock = Arc::make_mut(&mut directory_page_guard);
//         let directory_page = directory_page_lock.as_type_mut::<ExtendableHTableDirectoryPage>();
//
//         let bucket_idx = hash & ((1 << self.directory_max_depth) - 1);
//         debug!("Calculated bucket index: {}", bucket_idx);
//
//         let Some(bucket_page_id) = directory_page.get_bucket_page_id(bucket_idx as usize) else { todo!() };
//         let mut bucket_page_guard = match self.bpm.fetch_page_basic(bucket_page_id) {
//             Some(guard) => guard,
//             None => {
//                 error!("Failed to fetch bucket page ID: {}", bucket_page_id);
//                 return false;
//             }
//         };
//
//         let mut bucket_page_lock = Arc::make_mut(&mut bucket_page_guard);
//         let bucket_page = bucket_page_lock.as_type_mut::<ExtendableHTableBucketPage<K, V, C>>();
//
//         let success = bucket_page.remove(&key.clone(), &self.cmp);
//
//         if success {
//             info!("Successfully removed key: {:?}", key);
//         } else {
//             warn!("Failed to remove key: {:?}", key);
//         }
//
//         success
//     }
//
//     /// Retrieves the value(s) associated with a given key in the hash table.
//     ///
//     /// # Arguments
//     /// - `key`: The key to look up.
//     /// - `result`: A mutable vector to store the value(s) associated with the given key.
//     /// - `transaction`: The current transaction (optional).
//     ///
//     /// # Returns
//     /// `true` if the lookup succeeded, `false` otherwise.
//     pub fn get_value(&self, key: &K, result: &mut Vec<V>, _transaction: Option<&Transaction>) -> bool {
//         let hash = self.hash(key);
//         debug!("Getting value for key: {:?}, hash: {}", key, hash);
//
//         let mut header_page_guard = match self.bpm.fetch_page_basic(self.header_page_id) {
//             Some(guard) => guard,
//             None => {
//                 error!("Failed to fetch header page");
//                 return false;
//             }
//         };
//
//         let mut header_page_lock = Arc::make_mut(&mut header_page_guard);
//         let header_page = header_page_lock.as_type_mut::<ExtendableHTableHeaderPage>();
//
//         let directory_idx = header_page.hash_to_directory_index(hash);
//         debug!("Calculated directory index: {}", directory_idx);
//
//         let Some(directory_page_id) = header_page.get_directory_page_id(directory_idx as usize) else { todo!() };
//         let mut directory_page_guard = match self.bpm.fetch_page_basic(directory_page_id) {
//             Some(guard) => guard,
//             None => {
//                 error!("Failed to fetch directory page ID: {}", directory_page_id);
//                 return false;
//             }
//         };
//         let mut directory_page_lock = Arc::make_mut(&mut directory_page_guard);
//         let directory_page = directory_page_lock.as_type_mut::<ExtendableHTableDirectoryPage>();
//
//         let bucket_idx = hash & ((1 << self.directory_max_depth) - 1);
//         debug!("Calculated bucket index: {}", bucket_idx);
//
//         let Some(bucket_page_id) = directory_page.get_bucket_page_id(bucket_idx as usize) else { todo!() };
//         let mut bucket_page_guard = match self.bpm.fetch_page_basic(bucket_page_id) {
//             Some(guard) => guard,
//             None => {
//                 error!("Failed to fetch bucket page ID: {}", bucket_page_id);
//                 return false;
//             }
//         };
//
//         let mut bucket_page_lock = Arc::make_mut(&mut bucket_page_guard);
//         let bucket_page = bucket_page_lock.as_type_mut::<ExtendableHTableBucketPage<K, V, C>>();
//
//         if let Some(value) = bucket_page.lookup(key, &self.cmp) {
//             result.push(value.clone());
//             info!("Found value for key {:?}: {:?}", key, value);
//             true
//         } else {
//             warn!("Value not found for key {:?}", key);
//             false
//         }
//     }
//
//     /// Verifies the integrity of the extendable hash table's directory and bucket pages.
//     pub fn verify_integrity(&self) -> bool {
//         info!("Starting integrity verification for DiskExtendableHashTable: {}", self.index_name);
//
//         let mut header_page_guard = match self.bpm.fetch_page_basic(self.header_page_id) {
//             Some(guard) => guard,
//             None => {
//                 error!("Failed to fetch header page");
//                 return false;
//             }
//         };
//
//         let mut header_page_lock = Arc::make_mut(&mut header_page_guard);
//         let header_page = header_page_lock.as_type_mut::<ExtendableHTableHeaderPage>();
//
//         for directory_idx in 0..(1 << self.header_max_depth) {
//             let Some(directory_page_id) = header_page.get_directory_page_id(directory_idx) else { todo!() };
//             let mut directory_page_guard = match self.bpm.fetch_page_basic(directory_page_id) {
//                 Some(guard) => guard,
//                 None => {
//                     error!("Failed to fetch directory page ID: {}", directory_page_id);
//                     return false;
//                 }
//             };
//
//             let mut directory_page_lock = Arc::make_mut(&mut directory_page_guard);
//             let directory_page = directory_page_lock.as_type_mut::<ExtendableHTableDirectoryPage>();
//             directory_page.verify_integrity();
//         }
//
//         info!("Integrity verification completed for DiskExtendableHashTable: {}", self.index_name);
//         return true;
//     }
//
//     /// Prints the structure of the hash table, including header, directory, and bucket pages.
//     pub fn print_ht(&self) -> bool {
//         info!("Printing DiskExtendableHashTable: {}", self.index_name);
//
//         let mut header_page_guard = match self.bpm.fetch_page_basic(self.header_page_id) {
//             Some(guard) => guard,
//             None => {
//                 error!("Failed to fetch header page");
//                 return false;
//             }
//         };
//
//         let mut header_page_lock = Arc::make_mut(&mut header_page_guard);
//         let header_page = header_page_lock.as_type_mut::<ExtendableHTableHeaderPage>();
//         header_page.print_header();
//
//         for directory_idx in 0..(1 << self.header_max_depth) {
//             let Some(directory_page_id) = header_page.get_directory_page_id(directory_idx) else { todo!() };
//             let mut directory_page_guard = match self.bpm.fetch_page_basic(directory_page_id) {
//                 Some(guard) => guard,
//                 None => {
//                     error!("Failed to fetch directory page ID: {}", directory_page_id);
//                     return false;
//                 }
//             };
//
//             let mut directory_page_lock = Arc::make_mut(&mut directory_page_guard);
//             let directory_page = directory_page_lock.as_type_mut::<ExtendableHTableDirectoryPage>();
//             directory_page.print_directory();
//
//             for bucket_idx in 0..(1 << self.directory_max_depth) {
//                 let Some(bucket_page_id) = directory_page.get_bucket_page_id(bucket_idx) else { todo!() };
//                 let mut bucket_page_guard = match self.bpm.fetch_page_basic(bucket_page_id) {
//                     Some(guard) => guard,
//                     None => {
//                         error!("Failed to fetch bucket page ID: {}", bucket_page_id);
//                         return false;
//                     }
//                 };
//
//                 let mut bucket_page_lock = Arc::make_mut(&mut bucket_page_guard);
//                 let bucket_page = bucket_page_lock.as_type_mut::<ExtendableHTableBucketPage<K, V, C>>();
//                 bucket_page.print_bucket();
//             }
//         }
//
//         info!("Finished printing DiskExtendableHashTable: {}", self.index_name);
//         return true;
//     }
//
//     /// Hashes a key and returns the resulting 32-bit hash value.
//     ///
//     /// # Arguments
//     /// - `key`: The key to hash.
//     ///
//     /// # Returns
//     /// The 32-bit hash value of the key.
//     fn hash(&self, key: &K) -> u32 {
//         debug!("Hashing key: {:?}", key);
//         let hash = self.hash_fn.get_hash(key) as u32;
//         debug!("Hash for key {:?} is {}", key, hash);
//         hash
//     }
// }
//
// struct TestContext {
//     bpm: Arc<BufferPoolManager>,
//     db_file: String,
//     db_log_file: String,
// }
//
// impl TestContext {
//     async fn new(test_name: &str) -> Self {
//         initialize_logger();
//         let buffer_pool_size: usize = 5;
//         const K: usize = 2;
//         let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
//         let db_file = format!("{}_{}.db", test_name, timestamp);
//         let db_log_file = format!("{}_{}.log", test_name, timestamp);
//         let disk_manager =
//             Arc::new(FileDiskManager::new(db_file.clone(), db_log_file.clone()).await);
//         let disk_scheduler = Arc::new(Mutex::new(DiskScheduler::new(Arc::clone(
//             &disk_manager,
//         ))));
//         let replacer = Arc::new(spin::Mutex::new(LRUKReplacer::new(buffer_pool_size, K)));
//         let bpm = Arc::new(BufferPoolManager::new(
//             buffer_pool_size,
//             disk_scheduler,
//             disk_manager.clone(),
//             replacer.clone(),
//         ));
//
//         Self {
//             bpm,
//             db_file,
//             db_log_file,
//         }
//     }
//
//     async fn cleanup(&self) {
//         let _ = remove_file(&self.db_file).await;
//         let _ = remove_file(&self.db_log_file).await;
//     }
// }
//
// impl Drop for TestContext {
//     fn drop(&mut self) {
//         let db_file = self.db_file.clone();
//         let db_log_file = self.db_log_file.clone();
//         let _ = std::thread::spawn(move || {
//             let rt = tokio::runtime::Runtime::new().unwrap();
//             rt.block_on(async {
//                 let _ = remove_file(db_file).await;
//                 let _ = remove_file(db_log_file).await;
//             });
//         }).join();
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use crate::container::disk_extendable_hash_table::{DiskExtendableHashTable, TestContext};
//     use crate::container::hash_function::HashFunction;
//     use crate::storage::index::generic_key::GenericKeyComparator;
//
//     #[test]
//     fn test_insert1() {
//         let ctx = TestContext::new("test_insert1").await;
//         let bpm = &ctx.bpm;
//
//         let ht = DiskExtendableHashTable::new(
//             "blah".to_string(),
//             bpm.clone(),
//             GenericKeyComparator::new(),
//             HashFunction::new(),
//             2,
//             2,
//             0,
//         );
//
//         let num_keys = 8;
//
//         // insert some values
//         for i in 0..num_keys {
//             let inserted = ht.insert(&i, &i, None);
//             assert!(inserted);
//             let mut res = vec![];
//             let found = ht.get_value(&i, &mut res, None);
//             assert!(found);
//             assert_eq!(res.len(), 1);
//             assert_eq!(res[0], i);
//         }
//
//         ht.verify_integrity();
//
//         // attempt another insert, this should fail because table is full
//         assert!(!ht.insert(&num_keys, &num_keys, None);
//     }
//
//     #[test]
//     fn test_insert2() {
//         let ctx = TestContext::new("test_insert2");
//         let bpm = &ctx.bpm;
//
//         let ht = DiskExtendableHashTable::new(
//             "blah".to_string(),
//             bpm.clone(),
//             GenericKeyComparator::new(),
//             HashFunction::new(),
//             2,
//             2,
//             0,
//         );
//
//         let num_keys = 5;
//
//         // insert some values
//         for i in 0..num_keys {
//             let inserted = ht.insert(&i, &i, None);
//             assert!(inserted);
//             let mut res = vec![];
//             let found = ht.get_value(&i, &mut res, None);
//             assert!(found);
//             assert_eq!(res.len(), 1);
//             assert_eq!(res[0], i);
//         }
//
//         ht.verify_integrity();
//
//         // check that they were actually inserted
//         for i in 0..num_keys {
//             let mut res = vec![];
//             let found = ht.get_value(&i, &mut res, None);
//             assert!(found);
//             assert_eq!(res.len(), 1);
//             assert_eq!(res[0], i);
//         }
//
//         ht.verify_integrity();
//
//         // try to get some keys that don't exist/were not inserted
//         for i in num_keys..2 * num_keys {
//             let mut res = vec![];
//             let found = ht.get_value(&i, &mut res, None);
//             assert!(!found);
//             assert_eq!(res.len(), 0);
//         }
//
//         ht.verify_integrity();
//     }
//
//     #[test]
//     fn test_remove1() {
//         let ctx = TestContext::new("test_remove1").await;
//         let bpm = &ctx.bpm;
//
//         let ht = DiskExtendableHashTable::new(
//             "blah".to_string(),
//             bpm.clone(),
//             GenericKeyComparator::new(),
//             HashFunction::new(),
//             2,
//             2,
//             0,
//         );
//
//         let num_keys = 5;
//
//         // insert some values
//         for i in 0..num_keys {
//             let inserted = ht.insert(&i, &i, None);
//             assert!(inserted);
//             let mut res = vec![];
//             let found = ht.get_value(&i, &mut res, None);
//             assert!(found);
//             assert_eq!(res.len(), 1);
//             assert_eq!(res[0], i);
//         }
//
//         ht.verify_integrity();
//
//         // check that they were actually inserted
//         for i in 0..num_keys {
//             let mut res = vec![];
//             let found = ht.get_value(&i, &mut res, None);
//             assert!(found);
//             assert_eq!(res.len(), 1);
//             assert_eq!(res[0], i);
//         }
//
//         ht.verify_integrity();
//
//         // try to get some keys that don't exist/were not inserted
//         for i in num_keys..2 * num_keys {
//             let mut res = vec![];
//             let found = ht.get_value(&i, &mut res, None);
//             assert!(!found);
//             assert_eq!(res.len(), 0);
//         }
//
//         ht.verify_integrity();
//
//         // remove the keys we inserted
//         for i in 0..num_keys {
//             let removed = ht.remove(&i, None);
//             assert!(removed);
//             let mut res = vec![];
//             let found = ht.get_value(&i, &mut res, None);
//             assert!(!found);
//             assert_eq!(res.len(), 0);
//         }
//
//         ht.verify_integrity();
//
//         // try to remove some keys that don't exist/were not inserted
//         for i in num_keys..2 * num_keys {
//             let removed = ht.remove(&i, None);
//             assert!(!removed);
//             let mut res = vec![];
//             let found = ht.get_value(&i, &mut res, None);
//             assert!(!found);
//             assert_eq!(res.len(), 0);
//         }
//
//         ht.verify_integrity()
//     }
// }
