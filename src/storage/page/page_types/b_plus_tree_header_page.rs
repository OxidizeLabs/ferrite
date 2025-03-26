use std::mem;
use crate::common::config::{PageId, INVALID_PAGE_ID};

/// The header page for a B+ tree.
/// This page keeps track of the root page ID and other metadata about the B+ tree.
#[derive(Debug, Clone)]
pub struct BPlusTreeHeaderPage {
    /// The ID of the root page
    root_page_id: PageId,
    /// The height of the B+ tree
    tree_height: u32,
    /// The number of keys in the B+ tree
    num_keys: usize,
}

impl BPlusTreeHeaderPage {
    /// Create a new header page
    pub fn new() -> Self {
        Self {
            root_page_id: INVALID_PAGE_ID, // Use your appropriate invalid page ID constant
            tree_height: 0,
            num_keys: 0,
        }
    }

    /// Get the root page ID
    pub fn get_root_page_id(&self) -> PageId {
        self.root_page_id
    }

    /// Set the root page ID
    pub fn set_root_page_id(&mut self, page_id: PageId) {
        self.root_page_id = page_id;
    }

    /// Get the current height of the B+ tree
    pub fn get_tree_height(&self) -> u32 {
        self.tree_height
    }

    /// Set the height of the B+ tree
    pub fn set_tree_height(&mut self, height: u32) {
        self.tree_height = height;
    }

    /// Increment the height of the B+ tree by 1
    pub fn increment_tree_height(&mut self) {
        self.tree_height += 1;
    }

    /// Decrement the height of the B+ tree by 1
    pub fn decrement_tree_height(&mut self) {
        if self.tree_height > 0 {
            self.tree_height -= 1;
        }
    }

    /// Get the number of keys in the B+ tree
    pub fn get_num_keys(&self) -> usize {
        self.num_keys
    }

    /// Set the number of keys in the B+ tree
    pub fn set_num_keys(&mut self, num: usize) {
        self.num_keys = num;
    }

    /// Increment the number of keys in the B+ tree
    pub fn increment_num_keys(&mut self) {
        self.num_keys += 1;
    }

    /// Decrement the number of keys in the B+ tree
    pub fn decrement_num_keys(&mut self) {
        if self.num_keys > 0 {
            self.num_keys -= 1;
        }
    }

    /// Check if the B+ tree is empty
    pub fn is_empty(&self) -> bool {
        self.root_page_id == INVALID_PAGE_ID
    }

    /// Serialize the header page to bytes for storage
    pub fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(mem::size_of::<Self>());

        // Add the root page ID
        buffer.extend_from_slice(&self.root_page_id.to_le_bytes());

        // Add the tree height
        buffer.extend_from_slice(&self.tree_height.to_le_bytes());

        // Add the number of keys
        buffer.extend_from_slice(&(self.num_keys as u64).to_le_bytes());

        buffer
    }

    /// Deserialize bytes into a header page
    pub fn deserialize(data: &[u8]) -> Self {
        // Ensure we have enough data
        assert!(data.len() >= mem::size_of::<Self>());

        let mut offset = 0;

        // Read root page ID
        let root_page_id = PageId::from_le_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
            data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7],
        ]);
        offset += 8;

        // Read tree height
        let tree_height = u32::from_le_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
        ]);
        offset += 4;

        // Read number of keys
        let num_keys = u64::from_le_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
            data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7],
        ]) as usize;

        Self {
            root_page_id,
            tree_height,
            num_keys,
        }
    }
}