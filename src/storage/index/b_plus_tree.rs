use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, RwLock};

use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::concurrency::transaction::Transaction;
use crate::storage::index::b_plus_tree_index::{KeyComparator, KeyType, ValueType};
use crate::storage::index::index_iterator::IndexIterator;
use crate::storage::page::b_plus_tree_page::BPlusTreePage;
use crate::storage::page::page_guard::{ReadPageGuard, WritePageGuard};

/// Context class to help keep track of pages being modified or accessed.
pub struct Context {
    /// The write guard of the header page during insert/remove operations.
    pub header_page: Option<WritePageGuard>,
    /// The root page ID for easier identification of the root page.
    pub root_page_id: u32,
    /// The write guards of the pages being modified.
    pub write_set: VecDeque<WritePageGuard>,
    /// The read guards of the pages being read.
    pub read_set: VecDeque<ReadPageGuard>,
}

impl Context {
    /// Checks if the given page ID is the root page.
    ///
    /// # Parameters
    /// - `page_id`: The page ID to check.
    ///
    /// # Returns
    /// `true` if the page ID is the root page, `false` otherwise.
    pub fn is_root_page(&self, page_id: u32) -> bool {
        page_id == self.root_page_id
    }
}

/// A simple B+ tree data structure.
pub struct BPlusTree<K, V, C> {
    index_name: String,
    bpm: Arc<RwLock<BufferPoolManager>>,
    comparator: C,
    leaf_max_size: usize,
    internal_max_size: usize,
    header_page_id: u32,
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V, C> BPlusTree<K, V, C>
where
    K: KeyType,
    V: ValueType,
    C: KeyComparator<K> + for<'a, 'b> Fn(&'a K, &'b K),
{
    /// Creates a new `BPlusTree`.
    ///
    /// # Parameters
    /// - `name`: The name of the index.
    /// - `header_page_id`: The page ID of the header page.
    /// - `buffer_pool_manager`: The buffer pool manager.
    /// - `comparator`: The key comparator.
    /// - `leaf_max_size`: The maximum size of leaf pages.
    /// - `internal_max_size`: The maximum size of internal pages.
    ///
    /// # Returns
    /// A new `BPlusTree` instance.
    pub fn new(
        name: String,
        header_page_id: u32,
        buffer_pool_manager: Arc<RwLock<BufferPoolManager>>,
        comparator: C,
        leaf_max_size: usize,
        internal_max_size: usize,
    ) -> Self {
        unimplemented!()
    }

    /// Checks if the B+ tree is empty.
    ///
    /// # Returns
    /// `true` if the tree is empty, `false` otherwise.
    pub fn is_empty(&self) -> bool {
        unimplemented!()
    }

    /// Inserts a key-value pair into the B+ tree.
    ///
    /// # Parameters
    /// - `key`: The key to insert.
    /// - `value`: The value to insert.
    /// - `txn`: The transaction (optional).
    ///
    /// # Returns
    /// `true` if the insertion was successful, `false` otherwise.
    pub fn insert(&self, key: K, value: V, txn: Option<&Transaction>) -> bool {
        unimplemented!()
    }

    /// Removes a key and its value from the B+ tree.
    ///
    /// # Parameters
    /// - `key`: The key to remove.
    /// - `txn`: The transaction (optional).
    pub fn remove(&self, key: &K, txn: Option<&Transaction>) {
        unimplemented!()
    }

    /// Returns the value associated with a given key.
    ///
    /// # Parameters
    /// - `key`: The key to search for.
    /// - `result`: The vector to store the result.
    /// - `txn`: The transaction (optional).
    ///
    /// # Returns
    /// `true` if the key was found, `false` otherwise.
    pub fn get_value(&self, key: &K, result: &mut Vec<V>, txn: Option<&Transaction>) -> bool {
        unimplemented!()
    }

    /// Returns the page ID of the root node.
    ///
    /// # Returns
    /// The page ID of the root node.
    pub fn get_root_page_id(&self) -> u32 {
        unimplemented!()
    }

    /// Returns an iterator to the beginning of the B+ tree.
    ///
    /// # Returns
    /// An iterator to the beginning of the B+ tree.
    pub fn begin(&self) -> IndexIterator<K, V, C> {
        unimplemented!()
    }

    /// Returns an iterator to the end of the B+ tree.
    ///
    /// # Returns
    /// An iterator to the end of the B+ tree.
    // pub fn end(&self) -> IndexIterator<K, V> {
    //     unimplemented!()
    // }

    /// Returns an iterator starting at the given key.
    ///
    /// # Parameters
    /// - `key`: The key to start the iterator at.
    ///
    /// # Returns
    /// An iterator starting at the given key.
    pub fn begin_with_key(&self, key: &K) -> IndexIterator<K, V, C> {
        unimplemented!()
    }

    /// Prints the B+ tree.
    ///
    /// # Parameters
    /// - `bpm`: The buffer pool manager.
    pub fn print(&self, bpm: &BufferPoolManager) {
        unimplemented!()
    }

    /// Draws the B+ tree.
    ///
    /// # Parameters
    /// - `bpm`: The buffer pool manager.
    /// - `outf`: The output file.
    pub fn draw(&self, bpm: &BufferPoolManager, outf: &str) {
        unimplemented!()
    }

    /// Draws the B+ tree and returns it as a string.
    ///
    /// # Returns
    /// The B+ tree as a string.
    pub fn draw_b_plus_tree(&self) -> String {
        unimplemented!()
    }

    /// Reads data from a file and inserts it into the B+ tree.
    ///
    /// # Parameters
    /// - `file_name`: The name of the file to read from.
    /// - `txn`: The transaction (optional).
    pub fn insert_from_file(&self, file_name: &str, txn: Option<&Transaction>) {
        unimplemented!()
    }

    /// Reads data from a file and removes it from the B+ tree.
    ///
    /// # Parameters
    /// - `file_name`: The name of the file to read from.
    /// - `txn`: The transaction (optional).
    pub fn remove_from_file(&self, file_name: &str, txn: Option<&Transaction>) {
        unimplemented!()
    }

    /// Reads batch operations from a file and performs them on the B+ tree.
    ///
    /// # Parameters
    /// - `file_name`: The name of the file to read from.
    /// - `txn`: The transaction (optional).
    pub fn batch_ops_from_file(&self, file_name: &str, txn: Option<&Transaction>) {
        unimplemented!()
    }

    /// Converts the B+ tree into a graph and writes it to the output.
    ///
    /// # Parameters
    /// - `page_id`: The page ID of the root page.
    /// - `page`: The B+ tree page.
    /// - `out`: The output formatter.
    fn to_graph(&self, page_id: u32, page: &BPlusTreePage<K, V, C>, out: &mut fmt::Formatter<'_>) {
        unimplemented!()
    }

    /// Prints the B+ tree starting from the given page.
    ///
    /// # Parameters
    /// - `page_id`: The page ID of the root page.
    /// - `page`: The B+ tree page.
    fn print_tree(&self, page_id: u32, page: &BPlusTreePage<K, V, C>) {
        unimplemented!()
    }

    /// Converts the B+ tree into a printable B+ tree.
    ///
    /// # Parameters
    /// - `root_id`: The page ID of the root page.
    ///
    /// # Returns
    /// A printable B+ tree.
    fn to_printable_b_plus_tree(&self, root_id: u32) -> PrintableBPlusTree {
        unimplemented!()
    }
}

/// A printable B+ tree for debugging purposes.
pub struct PrintableBPlusTree {
    size: usize,
    keys: String,
    children: Vec<PrintableBPlusTree>,
}

impl PrintableBPlusTree {
    /// Prints the B+ tree to the output buffer using BFS traversal.
    ///
    /// # Parameters
    /// - `out_buf`: The output formatter.
    pub fn print(&self, out_buf: &mut fmt::Formatter<'_>) {
        let mut queue = vec![self];
        while !queue.is_empty() {
            let mut new_queue = vec![];
            for node in &queue {
                let padding = (node.size - node.keys.len()) / 2;
                write!(
                    out_buf,
                    "{}{}{}",
                    " ".repeat(padding),
                    node.keys,
                    " ".repeat(padding)
                )
                .unwrap();
                for child in &node.children {
                    new_queue.push(child);
                }
            }
            writeln!(out_buf).unwrap();
            queue = new_queue;
        }
    }
}
