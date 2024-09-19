// use std::hash::Hash;
// use std::sync::Arc;
// use crate::buffer::buffer_pool_manager::BufferPoolManager;
// use crate::common::config::PageId;
// use crate::concurrency::transaction::Transaction;
// use crate::container::hash_function::HashFunction;
// use crate::storage::page::page_types::extendable_hash_table_header_page::ExtendableHTableHeaderPage;
// use crate::storage::page::page_types::extendable_hash_table_directory_page::ExtendableHTableDirectoryPage;
// use crate::storage::page::page_types::extendable_hash_table_bucket_page::ExtendableHTableBucketPage;
//
// /// Implementation of an extendable hash table backed by a buffer pool manager.
// /// Non-unique keys are supported. Supports insert and delete. The table grows/shrinks
// /// dynamically as buckets become full/empty.
// pub struct DiskExtendableHashTable<K, V, KC> {
//     index_name: String,
//     bpm: Arc<BufferPoolManager>,
//     cmp: KC,
//     hash_fn: HashFunction<K>,
//     header_max_depth: u32,
//     directory_max_depth: u32,
//     bucket_max_size: u32,
//     header_page_id: PageId,
// }
//
// impl<K, V, KC> DiskExtendableHashTable<K, V, KC>
// where
//     K: Eq + Hash + Clone,
//     V: Clone,
//     KC: Fn(&K, &K) -> std::cmp::Ordering,
// {
//     /// Creates a new `DiskExtendableHashTable`.
//     ///
//     /// # Arguments
//     ///
//     /// * `name` - The name of the hash table.
//     /// * `bpm` - The buffer pool manager to be used.
//     /// * `cmp` - Comparator for keys.
//     /// * `hash_fn` - The hash function.
//     /// * `header_max_depth` - The max depth allowed for the header page.
//     /// * `directory_max_depth` - The max depth allowed for the directory page.
//     /// * `bucket_max_size` - The max size allowed for the bucket page array.
//     pub fn new(
//         name: String,
//         bpm: Arc<BufferPoolManager>,
//         cmp: KC,
//         hash_fn: HashFunction<K>,
//         header_max_depth: u32,
//         directory_max_depth: u32,
//         bucket_max_size: u32,
//     ) -> Self {
//         let header_page_id = bpm.new_page().expect("Failed to create header page").get_page_id();
//         let mut header_page = bpm.fetch_page(header_page_id).expect("Failed to fetch header page");
//         let mut header = header_page.into_specific_type::<ExtendableHTableHeaderPage, 8>().unwrap();
//         header.write().access_mut(|page| page.init(header_max_depth));
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
//         }
//     }
//
//     /// Inserts a key-value pair into the hash table.
//     ///
//     /// # Arguments
//     ///
//     /// * `key` - The key to insert.
//     /// * `value` - The value to be associated with the key.
//     /// * `transaction` - The current transaction (optional).
//     ///
//     /// # Returns
//     ///
//     /// Returns `true` if the insert succeeded, `false` otherwise.
//     pub fn insert(&mut self, key: K, value: V, transaction: Option<&Transaction>) -> bool {
//         let hash = self.hash_fn.hash(&key);
//         let mut header_page = self.bpm.fetch_page(self.header_page_id).expect("Failed to fetch header page");
//         let header = header_page.into_specific_type::<ExtendableHTableHeaderPage, 8>().unwrap();
//
//         let directory_index = header.read().access(|page| page.hash_to_directory_index(hash));
//         let directory_page_id = header.read().access(|page| page.get_directory_page_id(directory_index as usize).unwrap());
//
//         let mut directory_page = self.bpm.fetch_page(directory_page_id).expect("Failed to fetch directory page");
//         let directory = directory_page.into_specific_type::<ExtendableHTableDirectoryPage, 8>().unwrap();
//
//         let bucket_index = directory.read().access(|page| page.hash_to_bucket_index(hash));
//         let bucket_page_id = directory.read().access(|page| page.get_bucket_page_id(bucket_index as usize).unwrap());
//
//         let mut bucket_page = self.bpm.fetch_page(bucket_page_id).expect("Failed to fetch bucket page");
//         let bucket = bucket_page.into_specific_type::<ExtendableHTableBucketPage<K, V>, 8>().unwrap();
//
//         if bucket.read().access(|page| page.is_full()) {
//             self.split_bucket(directory_index, bucket_index, &key, &value);
//             // After splitting, we need to re-insert
//             return self.insert(key, value, transaction);
//         }
//
//         bucket.write().access_mut(|page| page.insert(key, value, &self.cmp))
//     }
//
//     /// Removes a key-value pair from the hash table.
//     ///
//     /// # Arguments
//     ///
//     /// * `key` - The key to delete.
//     /// * `transaction` - The current transaction (optional).
//     ///
//     /// # Returns
//     ///
//     /// Returns `true` if the remove succeeded, `false` otherwise.
//     pub fn remove(&mut self, key: &K, transaction: Option<&Transaction>) -> bool {
//         let hash = self.hash_fn.hash(key);
//         let mut header_page = self.bpm.fetch_page(self.header_page_id).expect("Failed to fetch header page");
//         let header = header_page.into_specific_type::<ExtendableHTableHeaderPage, 8>().unwrap();
//
//         let directory_index = header.read().access(|page| page.hash_to_directory_index(hash));
//         let directory_page_id = header.read().access(|page| page.get_directory_page_id(directory_index as usize).unwrap());
//
//         let mut directory_page = self.bpm.fetch_page(directory_page_id).expect("Failed to fetch directory page");
//         let directory = directory_page.into_specific_type::<ExtendableHTableDirectoryPage, 8>().unwrap();
//
//         let bucket_index = directory.read().access(|page| page.hash_to_bucket_index(hash));
//         let bucket_page_id = directory.read().access(|page| page.get_bucket_page_id(bucket_index as usize).unwrap());
//
//         let mut bucket_page = self.bpm.fetch_page(bucket_page_id).expect("Failed to fetch bucket page");
//         let bucket = bucket_page.into_specific_type::<ExtendableHTableBucketPage<K, V>, 8>().unwrap();
//
//         let removed = bucket.write().access_mut(|page| page.remove(key, &self.cmp));
//
//         if removed && bucket.read().access(|page| page.is_empty()) {
//             self.merge_bucket(directory_index, bucket_index);
//         }
//
//         removed
//     }
//
//     /// Gets the value associated with a given key in the hash table.
//     ///
//     /// # Arguments
//     ///
//     /// * `key` - The key to look up.
//     /// * `transaction` - The current transaction (optional).
//     ///
//     /// # Returns
//     ///
//     /// Returns `Some(value)` if the key is found, `None` otherwise.
//     pub fn get_value(&self, key: &mut K, transaction: Option<&Transaction>) -> Option<V> {
//         let hash = self.hash_fn.hash(key);
//         let mut header_page = self.bpm.fetch_page(self.header_page_id).expect("Failed to fetch header page");
//         let header = header_page.into_specific_type::<ExtendableHTableHeaderPage, 8>().unwrap();
//
//         let directory_index = header.read().access(|page| page.hash_to_directory_index(hash));
//         let directory_page_id = header.read().access(|page| page.get_directory_page_id(directory_index as usize).unwrap());
//
//         let mut directory_page = self.bpm.fetch_page(directory_page_id).expect("Failed to fetch directory page");
//         let directory = directory_page.into_specific_type::<ExtendableHTableDirectoryPage, 8>().unwrap();
//
//         let bucket_index = directory.read().access(|page| page.hash_to_bucket_index(hash));
//         let bucket_page_id = directory.read().access(|page| page.get_bucket_page_id(bucket_index as usize).unwrap());
//
//         let mut bucket_page = self.bpm.fetch_page(bucket_page_id).expect("Failed to fetch bucket page");
//         let bucket = bucket_page.into_specific_type::<ExtendableHTableBucketPage<K, V>, 8>().unwrap();
//
//         bucket.read().access(|page| page.lookup(key, &self.cmp))
//     }
//
//     /// Helper function to split a bucket when it becomes full.
//     fn split_bucket(&mut self, directory_index: u32, bucket_index: u32, key: &K, value: &V) {
//         // Implementation details for splitting a bucket
//         // This would involve creating a new bucket, redistributing entries, and updating the directory
//     }
//
//     /// Helper function to merge buckets when one becomes empty.
//     fn merge_bucket(&mut self, directory_index: u32, bucket_index: u32) {
//         // Implementation details for merging buckets
//         // This would involve removing an empty bucket and updating the directory
//     }
//
//     /// Helper function to verify the integrity of the extendible hash table's directory.
//     pub fn verify_integrity(&self) {
//         // Implementation to verify the integrity of the hash table
//         // This could involve checking the consistency of the header, directory, and buckets
//     }
//
//     /// Helper function to expose the header page ID.
//     pub fn header_page_id(&self) -> PageId {
//         self.header_page_id
//     }
//
//     /// Helper function to print out the hash table.
//     pub fn print_ht(&self) {
//         // Implementation to print the hash table structure
//         // This could involve printing the header, directory, and a summary of buckets
//     }
// }
//
// #[cfg(tests)]
// mod tests {
//     use super::*;
//     use crate::buffer::buffer_pool_manager::BufferPoolManager;
//     use crate::storage::disk::disk_manager::FileDiskManager;
//     use std::sync::Arc;
//     use parking_lot::RwLock;
//
//     // Helper function to create a buffer pool manager for testing
//     fn create_bpm() -> Arc<BufferPoolManager> {
//         let disk_manager = Arc::new(RwLock::new(FileDiskManager::new("tests.db".to_string(), "tests.log".to_string(), 100)));
//         Arc::new(BufferPoolManager::new(10, disk_manager))
//     }
//
//     #[tests]
//     fn test_insert_and_get() {
//         let bpm = create_bpm();
//         let cmp = |a: &u32, b: &u32| a.cmp(b);
//         let hash_fn = HashFunction::new();
//
//         let mut ht = DiskExtendableHashTable::new(
//             "test_table".to_string(),
//             bpm,
//             cmp,
//             hash_fn,
//             4,
//             4,
//             4,
//         );
//
//         assert!(ht.insert(1, "value1".to_string(), None));
//         assert!(ht.insert(2, "value2".to_string(), None));
//
//         assert_eq!(ht.get_value(&1, None), Some("value1".to_string()));
//         assert_eq!(ht.get_value(&2, None), Some("value2".to_string()));
//         assert_eq!(ht.get_value(&3, None), None);
//     }
//
//     #[tests]
//     fn test_remove() {
//         let bpm = create_bpm();
//         let cmp = |a: &u32, b: &u32| a.cmp(b);
//         let hash_fn = HashFunction::new();
//
//         let mut ht = DiskExtendableHashTable::new(
//             "test_table".to_string(),
//             bpm,
//             cmp,
//             hash_fn,
//             4,
//             4,
//             4,
//         );
//
//         ht.insert(1, "value1".to_string(), None);
//         assert!(ht.remove(&1, None));
//         assert_eq!(ht.get_value(&1, None), None);
//     }
//
//     // Add more tests as needed
// }