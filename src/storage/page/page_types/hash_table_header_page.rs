//! Header page implementation for linear probing hash table indexes.
//!
//! This module provides [`HashTableHeaderPage`], which serves as the metadata
//! and entry point for a linear probing hash table. It tracks all block pages
//! that store the actual key-value entries.
//!
//! # Page Layout
//!
//! The header has a compact 16-byte fixed format:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │ LSN (4) │ Size (4) │ PageId (4) │ NextBlockIndex (4)        │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! Followed by a dynamic array of block page IDs.
//!
//! # Linear Probing Structure
//!
//! The hash table consists of:
//!
//! ```text
//! ┌──────────────┐
//! │ Header Page  │ ← Metadata + block page list
//! └──────┬───────┘
//!        │
//!   ┌────┴────┬────────┬────────┐
//!   ▼         ▼        ▼        ▼
//! [Block₀] [Block₁] [Block₂] ... [Blockₙ]
//! ```
//!
//! Each block page stores key-value pairs using linear probing for collision
//! resolution.
//!
//! # Key Operations
//!
//! - [`add_block_page_id`](HashTableHeaderPage::add_block_page_id): Append a new block
//! - [`get_block_page_id`](HashTableHeaderPage::get_block_page_id): Access block by index
//! - [`num_blocks`](HashTableHeaderPage::num_blocks): Get current block count
//! - [`set_size`](HashTableHeaderPage::set_size): Update hash table size
//!
//! # Recovery Support
//!
//! The [`lsn`](HashTableHeaderPage::get_lsn) (Log Sequence Number) field supports
//! write-ahead logging for crash recovery.
//!
//! # Thread Safety
//!
//! Block page IDs are protected by a `Mutex` for concurrent access.

use crate::common::config::PageId;
use std::sync::Mutex;

pub type LsnT = u32;
pub type PageIdT = u32;

/**
 * Header Page for linear probing hash table.
 *
 * Header format (size in byte, 16 bytes in total):
 * -------------------------------------------------------------
 * | LSN (4) | Size (4) | PageId(4) | NextBlockIndex(4)
 * -------------------------------------------------------------
 */
pub struct HashTableHeaderPage {
    lsn: LsnT,
    size: usize,
    page_id: PageId,
    next_ind: usize,
    block_page_ids: Mutex<Vec<PageId>>,
}

impl Default for HashTableHeaderPage {
    fn default() -> Self {
        Self::new()
    }
}

impl HashTableHeaderPage {
    /// Creates a new `HashTableHeaderPage`.
    pub fn new() -> Self {
        Self {
            lsn: 0,
            size: 0,
            page_id: 0,
            next_ind: 0,
            block_page_ids: Mutex::new(Vec::new()),
        }
    }

    /// Sets the size field of the hash table to the given size.
    ///
    /// # Arguments
    ///
    /// * `size` - The size to set.
    pub fn set_size(&mut self, size: usize) {
        self.size = size;
    }

    /// Returns the page ID of this page.
    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    /// Sets the page ID of this page.
    ///
    /// # Arguments
    ///
    /// * `page_id` - The page ID to set.
    pub fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = page_id;
    }

    /// Returns the log sequence number (LSN) of this page.
    pub fn get_lsn(&self) -> LsnT {
        self.lsn
    }

    /// Sets the log sequence number (LSN) of this page.
    ///
    /// # Arguments
    ///
    /// * `lsn` - The log sequence number to set.
    pub fn set_lsn(&mut self, lsn: LsnT) {
        self.lsn = lsn;
    }

    /// Adds a block page ID to the end of the header page.
    ///
    /// # Arguments
    ///
    /// * `page_id` - The block page ID to add.
    pub fn add_block_page_id(&mut self, page_id: PageId) {
        let mut block_page_ids = self.block_page_ids.lock().unwrap();
        block_page_ids.push(page_id);
        self.next_ind += 1;
    }

    /// Returns the page ID of the block at the given index.
    ///
    /// # Arguments
    ///
    /// * `index` - The index of the block.
    ///
    /// # Returns
    ///
    /// The page ID of the block.
    pub fn get_block_page_id(&self, index: usize) -> PageId {
        let block_page_ids = self.block_page_ids.lock().unwrap();
        block_page_ids[index]
    }

    /// Returns the number of blocks currently stored in the header page.
    pub fn num_blocks(&self) -> usize {
        self.block_page_ids.lock().unwrap().len()
    }
}
