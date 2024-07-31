use crate::storage::page::extendable_hash_table_header_page::ExtendableHTableHeaderPage;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::common::config::{INVALID_PAGE_ID, PageId};
use crate::concurrency::transaction::Transaction;
use crate::container::hash_function::HashFunction;
use crate::storage::index::b_plus_tree_index::{KeyComparator, KeyType, ValueType};
use crate::storage::page::extendable_hash_table_bucket_page::ExtendableHTableBucketPage;
use crate::storage::page::extendable_hash_table_directory_page::ExtendableHTableDirectoryPage;

const HTABLE_HEADER_MAX_DEPTH: u32 = 32;
const HTABLE_DIRECTORY_MAX_DEPTH: u32 = 32;
// const HTableBucketArraySize: usize = 128;

pub struct DiskExtendableHashTable;


// impl<K, V, C> DiskExtendableHashTable<K, V, C>
// where
//     K: KeyType,
//     V: ValueType,
//     C: KeyComparator<K>,
// {
//     pub fn new(
//         name: String,
//         bpm: Arc<BufferPoolManager>,
//         cmp: C,
//         hash_fn: HashFunction<K>,
//         header_max_depth: u32,
//         directory_max_depth: u32,
//         bucket_max_size: usize,
//     ) -> Self {
//         Self {
//             index_name: name,
//             bpm,
//             cmp,
//             hash_fn,
//             header_max_depth,
//             directory_max_depth,
//             bucket_max_size,
//             header_page_id: INVALID_PAGE_ID,
//         }
//     }
//
//     pub fn insert(&self, key: K, value: V, transaction: &Transaction) -> bool {
//         unimplemented!();
//     }
//
//     pub fn remove(&self, key: &K, transaction: &Transaction) -> bool {
//         unimplemented!();
//     }
//
//     pub fn get_value(&self, key: &K, result: &mut Vec<V>, transaction: &Transaction) -> bool {
//         unimplemented!();
//     }
//
//     pub fn verify_integrity(&self) {
//         unimplemented!();
//     }
//
//     pub fn get_header_page_id(&self) -> PageId {
//         self.header_page_id
//     }
//
//     pub fn print_ht(&self) {
//         unimplemented!();
//     }
//
//     fn hash(&self, key: &K) -> u32 {
//         let mut hasher = self.hash_fn.clone();
//         hasher.write(&key);
//         hasher.finish() as u32
//     }
//
//     fn insert_to_new_directory(
//         &self,
//         header: &ExtendableHTableHeaderPage,
//         directory_idx: u32,
//         hash: u32,
//         key: &K,
//         value: &V,
//     ) -> bool {
//         unimplemented!();
//     }
//
//     fn insert_to_new_bucket(
//         &self,
//         directory: &ExtendableHTableDirectoryPage,
//         bucket_idx: u32,
//         key: &K,
//         value: &V,
//     ) -> bool {
//         unimplemented!();
//     }
//
//     fn update_directory_mapping(
//         &self,
//         directory: &ExtendableHTableDirectoryPage,
//         new_bucket_idx: u32,
//         new_bucket_page_id: PageId,
//         new_local_depth: u32,
//         local_depth_mask: u32,
//     ) {
//         unimplemented!();
//     }
//
//     fn migrate_entries(
//         &self,
//         old_bucket: &mut ExtendableHTableBucketPage<K, V, C>,
//         new_bucket: &mut ExtendableHTableBucketPage<K, V, C>,
//         new_bucket_idx: u32,
//         local_depth_mask: u32,
//     ) {
//         unimplemented!();
//     }
// }
//
