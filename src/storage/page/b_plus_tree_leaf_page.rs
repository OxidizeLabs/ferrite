use std::marker::PhantomData;
use crate::common::config::{INVALID_PAGE_ID, PageId};
use crate::storage::index::b_plus_tree_index::{KeyComparator, KeyType, ValueType};
use crate::storage::page::b_plus_tree_page::{BPlusTreePage, IndexPageType};

pub type MappingType<K, V> = (K, V);

pub const LEAF_PAGE_HEADER_SIZE: usize = 16;
pub const BUSTUB_PAGE_SIZE: usize = 4096;
pub const LEAF_PAGE_SIZE: usize = (BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / std::mem::size_of::<MappingType<i32, i32>>(); // Using i32 for simplicity

/// B+ tree leaf page that stores indexed keys and record ids.
///
/// Leaf page format (keys are stored in order):
/// -----------------------------------------------------------------------
/// | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)  |
/// -----------------------------------------------------------------------
///
/// Header format (size in bytes, 16 bytes in total):
/// -----------------------------------------------------------------------
/// | PageType (4) | CurrentSize (4) | MaxSize (4) | NextPageId (4) | ... |
/// -----------------------------------------------------------------------
pub struct BPlusTreeLeafPage<K: KeyType, V: ValueType, C: KeyComparator<K>> {
    page: BPlusTreePage<K, V, C>,
    next_page_id: PageId,
    array: Vec<MappingType<K, V>>,
    _marker: PhantomData<(K, V)>,
}

impl<K: KeyType, V: ValueType, C: KeyComparator<K>> BPlusTreeLeafPage<K, V, C> {
    /// Initializes a new leaf page with the specified maximum size.
    ///
    /// # Parameters
    /// - `max_size`: The maximum size of the leaf node.
    pub fn init(&mut self, max_size: usize) {
        self.page.set_page_type(IndexPageType::LeafPage);
        self.page.set_size(0);
        self.page.set_max_size(max_size);
        self.next_page_id = INVALID_PAGE_ID;
    }

    /// Returns the ID of the next page.
    pub fn get_next_page_id(&self) -> PageId {
        self.next_page_id
    }

    /// Sets the ID of the next page.
    ///
    /// # Parameters
    /// - `next_page_id`: The ID of the next page.
    pub fn set_next_page_id(&mut self, next_page_id: PageId) {
        self.next_page_id = next_page_id;
    }

    /// Returns the key at the specified index.
    ///
    /// # Parameters
    /// - `index`: The index to retrieve the key from.
    ///
    /// # Returns
    /// The key at the specified index.
    pub fn key_at(&self, index: usize) -> &K {
        &self.array[index].0
    }

    /// Returns a string representation of all keys in this leaf page,
    /// formatted as "(key1,key2,key3,...)"
    ///
    /// # Returns
    /// The string representation of all keys in the current leaf page.
    pub fn to_string(&self) -> String {
        let keys: Vec<String> = self.array.iter().map(|(key, _)| key.to_string()).collect();
        format!("({})", keys.join(","))
    }
}
