use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::common::config::{PageId, DB_PAGE_SIZE};
use crate::common::exception::PageError;
use crate::common::rid::RID;
use crate::storage::index::index::IndexInfo;
use crate::storage::page::page::{
    Page, PageTrait, PageType, PageTypeId, PAGE_ID_OFFSET, PAGE_TYPE_OFFSET,
};
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::Val::Null;
use crate::types_db::value::{Val, Value};
use std::any::Any;
use std::sync::Arc;

// Constants
const BTREE_MAX_KEY_SIZE: usize = 32;
const BTREE_MAX_RID_SIZE: usize = 12;

// B+ Tree Header Page
#[derive(Debug)]
pub struct BPlusTreeHeaderPage {
    data: Box<[u8; DB_PAGE_SIZE as usize]>,
    page_id: PageId,
    pin_count: i32,
    is_dirty: bool,
}

// B+ Tree Node Page (for both internal and leaf nodes)
#[derive(Debug)]
pub struct BPlusTreeNodePage {
    data: Box<[u8; DB_PAGE_SIZE as usize]>,
    page_id: PageId,
    pin_count: i32,
    is_dirty: bool,
}

// Persistent B+ Tree implementation
pub struct PersistentBPlusTree {
    pub buffer_pool_manager: Arc<BufferPoolManager>,
    pub header_page_id: PageId,
    metadata: Arc<IndexInfo>,
}

impl PageTypeId for BPlusTreeHeaderPage {
    const TYPE_ID: PageType = PageType::BTreeHeader;
}

impl Page for BPlusTreeHeaderPage {
    fn new(page_id: PageId) -> Self {
        let mut data = Box::new([0; DB_PAGE_SIZE as usize]);

        // Set page type in the header
        data[PAGE_TYPE_OFFSET] = PageType::BTreeHeader.to_u8();

        // Set page ID in the header
        let page_id_bytes = page_id.to_le_bytes();
        data[PAGE_ID_OFFSET..PAGE_ID_OFFSET + 8].copy_from_slice(&page_id_bytes);

        Self {
            data,
            page_id,
            pin_count: 0,
            is_dirty: false,
        }
    }
}

// Implement PageTrait for BPlusTreeHeaderPage
impl PageTrait for BPlusTreeHeaderPage {
    // Standard implementation similar to BasicPage
    fn get_page_id(&self) -> PageId {
        self.page_id
    }

    fn get_page_type(&self) -> PageType {
        PageType::BTreeHeader
    }

    fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    fn set_dirty(&mut self, is_dirty: bool) {
        self.is_dirty = is_dirty;
    }

    fn get_pin_count(&self) -> i32 {
        self.pin_count
    }

    fn increment_pin_count(&mut self) {
        self.pin_count += 1;
    }

    fn decrement_pin_count(&mut self) {
        if self.pin_count > 0 {
            self.pin_count -= 1;
        }
    }

    fn get_data(&self) -> &[u8; DB_PAGE_SIZE as usize] {
        &self.data
    }

    fn get_data_mut(&mut self) -> &mut [u8; DB_PAGE_SIZE as usize] {
        &mut self.data
    }

    fn set_data(&mut self, offset: usize, new_data: &[u8]) -> Result<(), PageError> {
        if offset + new_data.len() > self.data.len() {
            return Err(PageError::OffsetOutOfBounds);
        }
        self.data[offset..offset + new_data.len()].copy_from_slice(new_data);
        self.is_dirty = true;
        Ok(())
    }

    fn set_pin_count(&mut self, pin_count: i32) {
        self.pin_count = pin_count;
    }

    fn reset_memory(&mut self) {
        self.data.fill(0);
        self.data[PAGE_TYPE_OFFSET] = PageType::BTreeHeader.to_u8();
        self.is_dirty = false;
        self.pin_count = 0;
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// Additional helper methods for BPlusTreeHeaderPage
impl BPlusTreeHeaderPage {
    // Header layout:
    // [0]: Page type
    // [1-8]: Page ID
    // [9-16]: Root page ID
    // [17-20]: Order
    // [21-24]: Size (number of records)

    pub fn get_root_page_id(&self) -> Option<PageId> {
        let root_id_bytes = &self.data[9..17];
        let root_id = u64::from_le_bytes(root_id_bytes.try_into().unwrap());
        if root_id == 0 {
            None
        } else {
            Some(root_id)
        }
    }

    pub fn set_root_page_id(&mut self, root_id: Option<PageId>) {
        let id = match root_id {
            Some(id) => id,
            None => 0,
        };
        let bytes = id.to_le_bytes();
        self.data[9..17].copy_from_slice(&bytes);
        self.is_dirty = true;
    }

    pub fn get_order(&self) -> usize {
        let bytes = &self.data[17..21];
        u32::from_le_bytes(bytes.try_into().unwrap()) as usize
    }

    pub fn set_order(&mut self, order: usize) {
        let bytes = (order as u32).to_le_bytes();
        self.data[17..21].copy_from_slice(&bytes);
        self.is_dirty = true;
    }

    pub fn get_size(&self) -> usize {
        let bytes = &self.data[21..25];
        u32::from_le_bytes(bytes.try_into().unwrap()) as usize
    }

    pub fn set_size(&mut self, size: usize) {
        let bytes = (size as u32).to_le_bytes();
        self.data[21..25].copy_from_slice(&bytes);
        self.is_dirty = true;
    }
}

// Common implementation for BPlusTreeNodePage (standard PageTrait methods)
impl PageTypeId for BPlusTreeNodePage {
    const TYPE_ID: PageType = PageType::BTreeNode;
}

impl Page for BPlusTreeNodePage {
    fn new(page_id: PageId) -> Self {
        Self {
            data: Box::new([0; DB_PAGE_SIZE as usize]),
            page_id,
            pin_count: 0,
            is_dirty: false,
        }
    }
}

impl PageTrait for BPlusTreeNodePage {
    fn get_page_id(&self) -> PageId {
        self.page_id
    }

    fn get_page_type(&self) -> PageType {
        PageType::BTreeNode
    }

    fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    fn set_dirty(&mut self, is_dirty: bool) {
        self.is_dirty = is_dirty;
    }

    fn get_pin_count(&self) -> i32 {
        self.pin_count
    }

    fn increment_pin_count(&mut self) {
        self.pin_count += 1;
    }

    fn decrement_pin_count(&mut self) {
        self.pin_count -= 1;
    }

    fn get_data(&self) -> &[u8; DB_PAGE_SIZE as usize] {
        &self.data
    }

    fn get_data_mut(&mut self) -> &mut [u8; DB_PAGE_SIZE as usize] {
        &mut self.data
    }

    fn set_data(&mut self, offset: usize, new_data: &[u8]) -> Result<(), PageError> {
        if offset + new_data.len() > DB_PAGE_SIZE as usize {
            return Err(PageError::OffsetOutOfBounds);
        }
        self.data[offset..offset + new_data.len()].copy_from_slice(new_data);
        Ok(())
    }

    fn set_pin_count(&mut self, pin_count: i32) {
        self.pin_count = pin_count;
    }

    fn reset_memory(&mut self) {
        self.data.fill(0);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// Implement BPlusTreeNodePage for internal nodes
impl BPlusTreeNodePage {
    // Page layout:
    // [0]: Page type (internal or leaf)
    // [1-8]: Page ID
    // [9-12]: Number of keys
    // [13-16]: Parent page ID
    // For internal nodes:
    //   [17-20]: Number of children
    //   [21+]: Variable length region for keys and child page IDs
    // For leaf nodes:
    //   [17-24]: Next leaf page ID
    //   [25+]: Variable length region for keys and RIDs

    pub fn new_internal(page_id: PageId) -> Self {
        let mut data = Box::new([0; DB_PAGE_SIZE as usize]);

        // Set page type
        data[PAGE_TYPE_OFFSET] = PageType::BTreeInternal.to_u8();

        // Set page ID
        let page_id_bytes = page_id.to_le_bytes();
        data[PAGE_ID_OFFSET..PAGE_ID_OFFSET + 8].copy_from_slice(&page_id_bytes);

        // Initialize number of keys and children
        data[9..13].copy_from_slice(&(0_u32).to_le_bytes());
        // Initialize parent page ID (8 bytes)
        data[13..21].copy_from_slice(&(0_u64).to_le_bytes());
        // Initialize number of children
        data[21..25].copy_from_slice(&(0_u32).to_le_bytes());

        Self {
            data,
            page_id,
            pin_count: 0,
            is_dirty: false,
        }
    }

    pub fn new_leaf(page_id: PageId) -> Self {
        let mut data = Box::new([0; DB_PAGE_SIZE as usize]);

        // Set page type
        data[PAGE_TYPE_OFFSET] = PageType::BTreeLeaf.to_u8();

        // Set page ID
        let page_id_bytes = page_id.to_le_bytes();
        data[PAGE_ID_OFFSET..PAGE_ID_OFFSET + 8].copy_from_slice(&page_id_bytes);

        // Initialize number of keys
        data[9..13].copy_from_slice(&(0_u32).to_le_bytes());
        // Initialize parent page ID (8 bytes)
        data[13..21].copy_from_slice(&(0_u64).to_le_bytes());
        // Initialize next leaf page ID to 0 (none)
        data[21..29].copy_from_slice(&(0_u64).to_le_bytes());

        Self {
            data,
            page_id,
            pin_count: 0,
            is_dirty: false,
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.data[PAGE_TYPE_OFFSET] == PageType::BTreeLeaf.to_u8()
    }

    pub fn get_num_keys(&self) -> usize {
        let bytes = &self.data[9..13];
        u32::from_le_bytes(bytes.try_into().unwrap()) as usize
    }

    pub fn set_num_keys(&mut self, num: usize) {
        let bytes = (num as u32).to_le_bytes();
        self.data[9..13].copy_from_slice(&bytes);
        self.is_dirty = true;
    }

    pub fn get_parent_page_id(&self) -> Option<PageId> {
        let bytes = &self.data[13..21];
        let parent_id = u64::from_le_bytes(bytes.try_into().unwrap());
        if parent_id == 0 {
            None
        } else {
            Some(parent_id)
        }
    }

    pub fn set_parent_page_id(&mut self, parent_id: Option<PageId>) {
        let id = match parent_id {
            Some(id) => id,
            None => 0,
        };
        let bytes = id.to_le_bytes();
        self.data[13..21].copy_from_slice(&bytes);
        self.is_dirty = true;
    }

    // Internal node specific methods
    pub fn get_num_children(&self) -> usize {
        if self.is_leaf() {
            return 0;
        }

        let bytes = &self.data[21..25];
        u32::from_le_bytes(bytes.try_into().unwrap()) as usize
    }

    pub fn set_num_children(&mut self, num: usize) {
        if !self.is_leaf() {
            let bytes = (num as u32).to_le_bytes();
            self.data[21..25].copy_from_slice(&bytes);
            self.is_dirty = true;
        }
    }

    // Leaf node specific methods
    pub fn get_next_leaf_page_id(&self) -> Option<PageId> {
        if !self.is_leaf() {
            return None;
        }

        let bytes = &self.data[21..29];
        let next_id = u64::from_le_bytes(bytes.try_into().unwrap());
        if next_id == 0 {
            None
        } else {
            Some(next_id)
        }
    }

    pub fn set_next_leaf_page_id(&mut self, next_id: Option<PageId>) {
        if self.is_leaf() {
            let id = match next_id {
                Some(id) => id,
                None => 0,
            };
            let bytes = id.to_le_bytes();
            self.data[21..29].copy_from_slice(&bytes);
            self.is_dirty = true;
        }
    }

    // Key and value storage methods

    // For internal nodes: offset for key i (0-indexed)
    fn key_offset_internal(&self, i: usize) -> usize {
        25 + i * BTREE_MAX_KEY_SIZE
    }

    // For internal nodes: offset for child page ID i (0-indexed)
    fn child_offset_internal(&self, i: usize) -> usize {
        25 + self.get_num_keys() * BTREE_MAX_KEY_SIZE + i * 8
    }

    // For leaf nodes: offset for key i (0-indexed)
    fn key_offset_leaf(&self, i: usize) -> usize {
        25 + i * (BTREE_MAX_KEY_SIZE + BTREE_MAX_RID_SIZE)
    }

    // For leaf nodes: offset for RID i (0-indexed)
    fn rid_offset_leaf(&self, i: usize) -> usize {
        25 + i * (BTREE_MAX_KEY_SIZE + BTREE_MAX_RID_SIZE) + BTREE_MAX_KEY_SIZE
    }

    // Store and retrieve keys and children/RIDs

    pub fn get_key(&self, index: usize) -> Value {
        if index >= self.get_num_keys() {
            panic!("Index out of bounds");
        }

        let offset = if self.is_leaf() {
            self.key_offset_leaf(index)
        } else {
            self.key_offset_internal(index)
        };

        // Deserialize Value from bytes - simplified example
        // In practice, you need proper serialization/deserialization
        let key_type = self.data[offset];
        match key_type {
            1 => {
                // Integer type
                let value_bytes = &self.data[offset + 1..offset + 5];
                let value = i32::from_le_bytes(value_bytes.try_into().unwrap());
                Value::from(value)
            }
            2 => {
                // Float type
                let value_bytes = &self.data[offset + 1..offset + 9];
                let value = f64::from_le_bytes(value_bytes.try_into().unwrap());
                Value::from(value)
            }
            // Add other value types as needed
            _ => Value::new(Null),
        }
    }

    pub fn set_key(&mut self, index: usize, key: &Value) {
        if index >= self.get_num_keys() && index > 0 {
            panic!("Index out of bounds");
        }

        let offset = if self.is_leaf() {
            self.key_offset_leaf(index)
        } else {
            self.key_offset_internal(index)
        };

        // Remove this line as it's not needed and conflicts with the key parameter
        // let value = Value::from(i);

        match &key.value_ {
            Val::Integer(i) => {
                self.data[offset] = 1; // Type marker
                self.data[offset + 1..offset + 5].copy_from_slice(&i.to_le_bytes());
            }
            Val::Decimal(f) => {
                self.data[offset] = 2; // Type marker
                self.data[offset + 1..offset + 9].copy_from_slice(&f.to_le_bytes());
            }
            // Add other value types as needed
            _ => {
                self.data[offset] = 0; // Null
            }
        }

        self.is_dirty = true;
    }

    pub fn get_child_page_id(&self, index: usize) -> PageId {
        if self.is_leaf() || index > self.get_num_children() {
            panic!("Invalid child index or not an internal node");
        }

        let offset = self.child_offset_internal(index);
        let bytes = &self.data[offset..offset + 8];
        u64::from_le_bytes(bytes.try_into().unwrap())
    }

    pub fn set_child_page_id(&mut self, index: usize, child_id: PageId) {
        if self.is_leaf() || index > self.get_num_children() {
            panic!("Invalid child index or not an internal node");
        }

        let offset = self.child_offset_internal(index);
        self.data[offset..offset + 8].copy_from_slice(&child_id.to_le_bytes());
        self.is_dirty = true;
    }

    pub fn get_rid(&self, index: usize) -> RID {
        if !self.is_leaf() || index >= self.get_num_keys() {
            panic!("Invalid RID index or not a leaf node");
        }

        let offset = self.rid_offset_leaf(index);

        // Read 8 bytes for PageId
        let page_id_bytes = &self.data[offset..offset + 8];
        // Read 4 bytes for slot_num
        let slot_id_bytes = &self.data[offset + 8..offset + 12];

        let page_id = u64::from_le_bytes(page_id_bytes.try_into().unwrap());
        let slot_num = u32::from_le_bytes(slot_id_bytes.try_into().unwrap());

        RID::new(page_id, slot_num)
    }

    pub fn set_rid(&mut self, index: usize, rid: RID) {
        if !self.is_leaf() || index >= self.get_num_keys() && index > 0 {
            panic!("Invalid RID index or not a leaf node");
        }

        let offset = self.rid_offset_leaf(index);
        let page_id_bytes = rid.get_page_id().to_le_bytes();
        let slot_id_bytes = rid.get_slot_num().to_le_bytes();

        // Use 8 bytes for PageId (u64)
        self.data[offset..offset + 8].copy_from_slice(&page_id_bytes);
        // Use 4 bytes for slot_num (u32)
        self.data[offset + 8..offset + 12].copy_from_slice(&slot_id_bytes);

        self.is_dirty = true;
    }

    // Insert and delete operations

    pub fn insert_key_and_rid(&mut self, index: usize, key: &Value, rid: RID) {
        if !self.is_leaf() {
            panic!("Can only insert keys and RIDs in leaf nodes");
        }

        // Shift all keys and RIDs after index
        let num_keys = self.get_num_keys();
        for i in (index..num_keys).rev() {
            let key_to_move = self.get_key(i);
            let rid_to_move = self.get_rid(i);

            self.set_key(i + 1, &key_to_move);
            self.set_rid(i + 1, rid_to_move);
        }

        // Insert new key and RID
        self.set_key(index, key);
        self.set_rid(index, rid);

        // Update number of keys
        self.set_num_keys(num_keys + 1);
    }

    pub fn insert_key_and_child(&mut self, index: usize, key: &Value, child_id: PageId) {
        if self.is_leaf() {
            panic!("Can only insert keys and children in internal nodes");
        }

        // Shift all keys after index
        let num_keys = self.get_num_keys();
        for i in (index..num_keys).rev() {
            let key_to_move = self.get_key(i);
            self.set_key(i + 1, &key_to_move);
        }

        // Shift all children after index + 1
        let num_children = self.get_num_children();
        for i in (index + 1..num_children).rev() {
            let child_to_move = self.get_child_page_id(i);
            self.set_child_page_id(i + 1, child_to_move);
        }

        // Insert new key and child
        self.set_key(index, key);
        self.set_child_page_id(index + 1, child_id);

        // Update number of keys and children
        self.set_num_keys(num_keys + 1);
        self.set_num_children(num_children + 1);
    }

    pub fn delete_key_and_rid(&mut self, index: usize) {
        if !self.is_leaf() {
            panic!("Can only delete keys and RIDs from leaf nodes");
        }

        let num_keys = self.get_num_keys();
        if index >= num_keys {
            panic!("Index out of bounds");
        }

        // Shift all keys and RIDs after index
        for i in index + 1..num_keys {
            let key_to_move = self.get_key(i);
            let rid_to_move = self.get_rid(i);

            self.set_key(i - 1, &key_to_move);
            self.set_rid(i - 1, rid_to_move);
        }

        // Update number of keys
        self.set_num_keys(num_keys - 1);
    }

    pub fn delete_key_and_child(&mut self, index: usize) {
        if self.is_leaf() {
            panic!("Can only delete keys and children from internal nodes");
        }

        let num_keys = self.get_num_keys();
        let num_children = self.get_num_children();

        if index >= num_keys {
            panic!("Index out of bounds");
        }

        // Shift all keys after index
        for i in index + 1..num_keys {
            let key_to_move = self.get_key(i);
            self.set_key(i - 1, &key_to_move);
        }

        // Shift all children after index + 1
        for i in index + 2..num_children {
            let child_to_move = self.get_child_page_id(i);
            self.set_child_page_id(i - 1, child_to_move);
        }

        // Update number of keys and children
        self.set_num_keys(num_keys - 1);
        self.set_num_children(num_children - 1);
    }
}

impl PersistentBPlusTree {
    pub fn new(
        buffer_pool_manager: Arc<BufferPoolManager>,
        order: usize,
        metadata: Arc<IndexInfo>,
    ) -> Self {
        // Create a new header page
        let header_page_guard = buffer_pool_manager
            .new_page::<BPlusTreeHeaderPage>()
            .expect("Failed to create header page");

        let header_page_id = header_page_guard.get_page_id();

        // Initialize header page
        {
            let mut header = header_page_guard.write();
            header.set_order(order);
            header.set_size(0);
            // Root page is initially null
            header.set_root_page_id(None);
        }

        Self {
            buffer_pool_manager,
            header_page_id,
            metadata,
        }
    }

    pub fn from_existing(
        buffer_pool_manager: Arc<BufferPoolManager>,
        header_page_id: PageId,
        metadata: Arc<IndexInfo>,
    ) -> Self {
        Self {
            buffer_pool_manager,
            header_page_id,
            metadata,
        }
    }

    pub fn get_metadata(&self) -> Arc<IndexInfo> {
        self.metadata.clone()
    }

    pub fn insert(&self, key: Value, rid: RID) -> bool {
        // Check if tree is empty
        let root_page_id_opt = self.get_root_page_id();
        let order = self.get_order();

        if root_page_id_opt.is_none() {
            // Create first leaf node as root
            let new_leaf_guard = self
                .buffer_pool_manager
                .new_page::<BPlusTreeNodePage>()
                .expect("Failed to create leaf node");

            let new_leaf_id = new_leaf_guard.get_page_id();

            {
                let mut new_leaf = new_leaf_guard.write();
                // Initialize as leaf
                *new_leaf = BPlusTreeNodePage::new_leaf(new_leaf_id);

                // Insert the key-rid pair
                new_leaf.set_key(0, &key);
                new_leaf.set_rid(0, rid);
                new_leaf.set_num_keys(1);
            }

            // Update header to point to new root
            let header_guard = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
                .expect("Failed to fetch header page");

            {
                let mut header = header_guard.write();
                let current_size = header.get_size();
                header.set_size(current_size + 1);
                // Set the root page ID to the new leaf
                header.set_root_page_id(Some(new_leaf_id));
            }

            return true;
        }

        // Tree already has a root
        let root_page_id = root_page_id_opt.unwrap();

        // Check if we need to split the root
        let root_guard = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeNodePage>(root_page_id)
            .expect("Failed to fetch root page");

        let is_root_full = {
            let root = root_guard.read();
            root.get_num_keys() >= 2 * order - 1 // Full condition
        };

        if is_root_full {
            // Create a new root (internal node)
            let new_root_guard = self
                .buffer_pool_manager
                .new_page::<BPlusTreeNodePage>()
                .expect("Failed to create new root node");

            let new_root_id = new_root_guard.get_page_id();

            {
                let mut new_root = new_root_guard.write();
                *new_root = BPlusTreeNodePage::new_internal(new_root_id);

                // Old root becomes the first child of new root
                new_root.set_child_page_id(0, root_page_id);
                new_root.set_num_children(1);
            }

            // Split the old root
            self.split_child(new_root_id, 0);

            // Update header to point to new root
            let header_guard = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
                .expect("Failed to fetch header page");

            {
                let mut header = header_guard.write();
                header.set_root_page_id(Some(new_root_id));
            }

            // Now insert into the new root structure
            self.insert_non_full(new_root_id, key, rid)
        } else {
            // Insert into the existing root
            self.insert_non_full(root_page_id, key, rid)
        }
    }

    pub fn find(&self, key: &Value) -> Option<RID> {
        // Get the root page ID from the header
        let root_page_id_opt = self.get_root_page_id();

        // If tree is empty, return None
        if root_page_id_opt.is_none() {
            return None;
        }

        let root_page_id = root_page_id_opt.unwrap();

        // Start search from the root
        self.find_in_node(root_page_id, key)
    }

    fn get_root_page_id(&self) -> Option<PageId> {
        // Fetch header page
        let header_page_guard = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .expect("Failed to fetch header page");

        let header = header_page_guard.read();
        header.get_root_page_id()
    }

    fn get_order(&self) -> usize {
        // Fetch header page
        let header_page_guard = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
            .expect("Failed to fetch header page");

        let header = header_page_guard.read();
        header.get_order()
    }

    fn insert_non_full(&self, node_page_id: PageId, key: Value, rid: RID) -> bool {
        let node_guard = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeNodePage>(node_page_id)
            .expect("Failed to fetch node page");

        let is_leaf = {
            let node = node_guard.read();
            node.is_leaf()
        };

        if is_leaf {
            // Insert into leaf node
            let mut node = node_guard.write();
            let num_keys = node.get_num_keys();

            // Find the insertion position
            let mut idx = num_keys;
            for i in 0..num_keys {
                if self.compare_keys(&key, &node.get_key(i)) != CmpBool::CmpTrue {
                    // If key already exists, check if the exact same record exists
                    if self.compare_keys(&key, &node.get_key(i)) == CmpBool::CmpTrue
                        && node.get_rid(i) == rid
                    {
                        return false; // Already exists
                    }
                    idx = i;
                    break;
                }
            }

            // Shift elements to make room for the new key/value
            for i in (idx..num_keys).rev() {
                let node_key = &node.get_key(i);
                let node_rid = node.get_rid(i);
                node.set_key(i + 1, node_key);
                node.set_rid(i + 1, node_rid);
            }

            // Insert the new key/value
            node.set_key(idx, &key);
            node.set_rid(idx, rid);
            node.set_num_keys(num_keys + 1);

            // Update tree size in header
            let header_guard = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeHeaderPage>(self.header_page_id)
                .expect("Failed to fetch header page");

            {
                let mut header = header_guard.write();
                let current_size = header.get_size();
                header.set_size(current_size + 1);
            }

            true
        } else {
            // Internal node case
            let child_idx = {
                let node = node_guard.read();
                let num_keys = node.get_num_keys();

                // Find the child to descend to
                let mut idx = 0;
                while idx < num_keys
                    && self.compare_keys(&key, &node.get_key(idx)) == CmpBool::CmpTrue
                {
                    idx += 1;
                }
                idx
            };

            let child_page_id = {
                let node = node_guard.read();
                node.get_child_page_id(child_idx)
            };

            // Check if child node is full
            let child_guard = self
                .buffer_pool_manager
                .fetch_page::<BPlusTreeNodePage>(child_page_id)
                .expect("Failed to fetch child page");

            let is_child_full = {
                let child = child_guard.read();
                let order = self.get_order();
                child.get_num_keys() >= 2 * order - 1 // Full condition
            };

            if is_child_full {
                // Split the child
                self.split_child(node_page_id, child_idx);

                // After splitting, determine which child to descend to
                let new_child_idx = {
                    let node = node_guard.read();
                    if self.compare_keys(&key, &node.get_key(child_idx)) == CmpBool::CmpTrue {
                        child_idx + 1 // Go to the new right child
                    } else {
                        child_idx // Stay with the left child
                    }
                };

                let new_child_page_id = {
                    let node = node_guard.read();
                    node.get_child_page_id(new_child_idx)
                };

                // Recursively insert into the appropriate child
                self.insert_non_full(new_child_page_id, key, rid)
            } else {
                // Recursively insert into the child
                self.insert_non_full(child_page_id, key, rid)
            }
        }
    }

    fn split_child(&self, parent_page_id: PageId, child_idx: usize) {
        let order = self.get_order();

        // Get the parent node
        let parent_guard = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeNodePage>(parent_page_id)
            .expect("Failed to fetch parent page");

        // Get the child to be split
        let child_page_id = {
            let parent = parent_guard.read();
            parent.get_child_page_id(child_idx)
        };

        let child_guard = self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeNodePage>(child_page_id)
            .expect("Failed to fetch child page");

        // Create a new sibling node
        let new_sibling_guard = self
            .buffer_pool_manager
            .new_page::<BPlusTreeNodePage>()
            .expect("Failed to create new sibling node");

        let new_sibling_id = new_sibling_guard.get_page_id();
        let is_leaf = {
            let child = child_guard.read();
            child.is_leaf()
        };

        // Initialize the new sibling with the same type as the child
        {
            let mut new_sibling = new_sibling_guard.write();
            if is_leaf {
                *new_sibling = BPlusTreeNodePage::new_leaf(new_sibling_id);
                // Set parent in the new sibling
                new_sibling.set_parent_page_id(Some(parent_page_id));
            } else {
                *new_sibling = BPlusTreeNodePage::new_internal(new_sibling_id);
                // Set parent in the new sibling
                new_sibling.set_parent_page_id(Some(parent_page_id));
                // For internal nodes, initialize num_children to 0
                new_sibling.set_num_children(0);
            }
        }

        // Find median key (will be promoted to parent)
        let median_idx = order - 1;
        let median_key = {
            let child = child_guard.read();
            child.get_key(median_idx).clone()
        };

        // Move half of the keys and pointers from child to new sibling
        {
            let child = child_guard.read();
            let mut new_sibling = new_sibling_guard.write();

            // Move keys and values/children
            for i in 0..(order - 1) {
                let src_idx = i + order;
                new_sibling.set_key(i, &child.get_key(src_idx));

                if is_leaf {
                    new_sibling.set_rid(i, child.get_rid(src_idx));
                } else {
                    new_sibling.set_child_page_id(i, child.get_child_page_id(src_idx));
                }
            }

            // If it's an internal node, move the last child pointer
            if !is_leaf {
                new_sibling.set_child_page_id(order - 1, child.get_child_page_id(2 * order - 1));
                new_sibling.set_num_children(order);
            }

            new_sibling.set_num_keys(order - 1);

            // Set up sibling pointers for leaf nodes
            if is_leaf {
                // Set new_sibling's next pointer to child's next pointer
                new_sibling.set_next_leaf_page_id(child.get_next_leaf_page_id());
            }
        }

        // Update child's key count
        {
            let mut child = child_guard.write();
            child.set_num_keys(order - 1);

            // Update leaf node's next pointer
            if is_leaf {
                child.set_next_leaf_page_id(Some(new_sibling_id));
            }
        }

        // Make room in the parent for the new key and child pointer
        {
            let mut parent = parent_guard.write();
            let parent_num_keys = parent.get_num_keys();

            // Shift keys and children to make room
            for i in (child_idx..parent_num_keys).rev() {
                let parent_key = &parent.get_key(i);
                parent.set_key(i + 1, parent_key);
            }

            for i in (child_idx + 1..=parent_num_keys).rev() {
                let child_page_id = parent.get_child_page_id(i);
                parent.set_child_page_id(i + 1, child_page_id);
            }

            // Insert the median key and pointer to the new sibling
            parent.set_key(child_idx, &median_key);
            parent.set_child_page_id(child_idx + 1, new_sibling_id);

            // Update parent's key count
            parent.set_num_keys(parent_num_keys + 1);
            let num_of_children = parent.get_num_children();
            parent.set_num_children(num_of_children + 1);
        }
    }

    fn compare_keys(&self, key1: &Value, key2: &Value) -> CmpBool {
        // First check less than
        if key1.compare_less_than(key2) == CmpBool::CmpTrue {
            return CmpBool::CmpTrue;
        }

        // Then check greater than
        if key1.compare_greater_than(key2) == CmpBool::CmpTrue {
            return CmpBool::CmpTrue;
        }

        // If neither less than nor greater than, must be equal
        CmpBool::CmpFalse
    }

    fn find_in_node(&self, node_page_id: PageId, key: &Value) -> Option<RID> {
        // Correctly match on Result, not Option
        let node_guard = match self
            .buffer_pool_manager
            .fetch_page::<BPlusTreeNodePage>(node_page_id)
        {
            Some(guard) => guard,
            None => return None, // Return None if we can't fetch the node
        };

        // Rest of function remains unchanged
        let is_leaf = {
            let node = node_guard.read();
            node.is_leaf()
        };

        if is_leaf {
            // Search in leaf node
            let node = node_guard.read();
            let num_keys = node.get_num_keys();

            for i in 0..num_keys {
                let node_key = node.get_key(i);

                // If keys match exactly, return the associated RID
                if node_key.compare_equals(key) == CmpBool::CmpTrue {
                    return Some(node.get_rid(i));
                }
            }

            // Key not found
            None
        } else {
            // Navigate through internal node
            let child_idx = {
                let node = node_guard.read();
                let num_keys = node.get_num_keys();

                // Find the child to descend to
                let mut idx = 0;
                while idx < num_keys
                    && self.compare_keys(key, &node.get_key(idx)) == CmpBool::CmpTrue
                {
                    idx += 1;
                }
                idx
            };

            let child_page_id = {
                let node = node_guard.read();
                node.get_child_page_id(child_idx)
            };

            // Recursively search in the child node
            self.find_in_node(child_page_id, key)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::schema::Schema;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::index::index::IndexType;
    use parking_lot::RwLock;
    use std::fs;
    use std::path::Path;
    use tempfile::tempdir;

    // Helper function to create a buffer pool manager for testing
    fn create_test_buffer_pool_manager() -> Arc<BufferPoolManager> {
        let temp_dir = tempdir().expect("Failed to create temp directory");
        let db_file_path = temp_dir.path().join("test_bptree.db");
        let log_file_path = temp_dir.path().join("test_bptree.log");

        // Ensure the file doesn't exist
        if Path::new(&db_file_path).exists() {
            fs::remove_file(&db_file_path).expect("Failed to remove existing test file");
        }

        // Create components needed for the buffer pool manager
        let pool_size = 50; // Enough pages for testing
        let disk_manager = Arc::new(FileDiskManager::new(
            db_file_path.to_string_lossy().to_string(),
            log_file_path.to_string_lossy().to_string(),
            10, // log_timeout
        ));
        let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(pool_size, 2))); // K=2 for LRU-2

        Arc::new(BufferPoolManager::new(
            pool_size,
            disk_scheduler,
            disk_manager,
            replacer,
        ))
    }

    // Helper function to create a test index info
    fn create_test_index_info() -> Arc<IndexInfo> {
        // Create minimal Schema for testing - empty schema is fine for our tests
        let key_schema = Schema::new(vec![]);

        Arc::new(IndexInfo::new(
            key_schema,                // key_schema
            "test_index".to_string(),  // index_name
            1,                         // index_oid
            "test_table".to_string(),  // table_name
            4,                         // key_size
            true,                      // is_primary_key
            IndexType::BPlusTreeIndex, // index_type
            vec![0],                   // key_attrs
        ))
    }

    #[test]
    fn test_create_bplus_tree() {
        // Create BPM and index info
        let bpm = create_test_buffer_pool_manager();
        let index_info = create_test_index_info();

        // Create a new B+ tree with order 3
        let bptree = PersistentBPlusTree::new(bpm, 3, index_info.clone());

        // Verify the header page was initialized correctly through the constructor
        let header_page_guard = bptree
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(bptree.header_page_id)
            .expect("Failed to fetch header page");

        {
            let header = header_page_guard.read();
            assert_eq!(header.get_order(), 3);
            assert_eq!(header.get_size(), 0);
            assert_eq!(header.get_root_page_id(), None); // No root initially
        }
    }

    #[test]
    fn test_insert_first_key() {
        // Create BPM and index info
        let bpm = create_test_buffer_pool_manager();
        let index_info = create_test_index_info();

        // Create a new B+ tree with order 3
        let bptree = PersistentBPlusTree::new(bpm, 3, index_info.clone());

        // Insert a key-value pair
        let key = Value::from(42_i32);
        let rid = RID::new(1, 1);
        let result = bptree.insert(key.clone(), rid);

        assert!(result); // Insert should succeed

        // Verify the size was updated
        let header_page_guard = bptree
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(bptree.header_page_id)
            .expect("Failed to fetch header page");

        {
            let header = header_page_guard.read();
            assert_eq!(header.get_size(), 1); // Size should be updated
        }
    }

    #[test]
    fn test_insert_multiple_keys() {
        // Create BPM and index info
        let bpm = create_test_buffer_pool_manager();
        let index_info = create_test_index_info();

        // Create a new B+ tree with order 3
        let bptree = PersistentBPlusTree::new(bpm.clone(), 3, index_info.clone());

        // Insert multiple key-value pairs
        for i in 0..10 {
            let key = Value::from(i as i32);
            let rid = RID::new(i as u64, i as u32);
            let result = bptree.insert(key.clone(), rid);
            assert!(result); // Each insert should succeed
        }

        // Verify the size was updated
        let header_page_guard = bpm
            .fetch_page::<BPlusTreeHeaderPage>(bptree.header_page_id)
            .expect("Failed to fetch header page");

        {
            let header = header_page_guard.read();
            assert_eq!(header.get_size(), 10); // Size should be updated
            assert!(header.get_root_page_id().is_some()); // Root page should exist
        }
    }

    #[test]
    fn test_insert_duplicate_keys() {
        // Create BPM and index info
        let bpm = create_test_buffer_pool_manager();
        let index_info = create_test_index_info();

        // Create a new B+ tree with order 3
        let bptree = PersistentBPlusTree::new(bpm, 3, index_info.clone());

        // Insert a key-value pair
        let key = Value::from(42_i32);
        let rid = RID::new(1, 1);
        let result1 = bptree.insert(key.clone(), rid);

        // Try to insert the same key-value pair again
        let result2 = bptree.insert(key.clone(), rid);

        assert!(result1); // First insert should succeed
        assert!(!result2); // Second insert should fail (duplicate)

        // Verify the size was updated only once
        let header_page_guard = bptree
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(bptree.header_page_id)
            .expect("Failed to fetch header page");

        {
            let header = header_page_guard.read();
            assert_eq!(header.get_size(), 1); // Size should be 1
        }
    }

    #[test]
    fn test_tree_splitting() {
        // Create BPM and index info
        let bpm = create_test_buffer_pool_manager();
        let index_info = create_test_index_info();

        // Create a new B+ tree with order 3 (small order to force splits)
        let bptree = PersistentBPlusTree::new(bpm.clone(), 3, index_info.clone());

        // Insert keys in ascending order to force splits
        for i in 0..20 {
            let key = Value::from(i as i32);
            let rid = RID::new(i as u64, i as u32);
            bptree.insert(key.clone(), rid);
        }

        // Verify tree structure - root should not be a leaf after splitting
        let header_page_guard = bpm
            .fetch_page::<BPlusTreeHeaderPage>(bptree.header_page_id)
            .expect("Failed to fetch header page");

        let root_page_id = {
            let header = header_page_guard.read();
            assert_eq!(header.get_size(), 20); // Size should be updated
            header.get_root_page_id().expect("Root should exist")
        };

        // Check if the root is an internal node (not a leaf)
        let root_page_guard = bpm
            .fetch_page::<BPlusTreeNodePage>(root_page_id)
            .expect("Failed to fetch root page");

        {
            let root = root_page_guard.read();
            assert!(!root.is_leaf()); // Root should be an internal node after splitting
            assert!(root.get_num_keys() > 0); // Root should have keys
        }
    }

    #[test]
    fn test_from_existing() {
        // Create BPM and index info
        let bpm = create_test_buffer_pool_manager();
        let index_info = create_test_index_info();

        // Create a new B+ tree with order 3
        let bptree1 = PersistentBPlusTree::new(bpm.clone(), 3, index_info.clone());

        // Insert some keys
        for i in 0..5 {
            let key = Value::from(i as i32);
            let rid = RID::new(i as u64, i as u32);
            bptree1.insert(key, rid);
        }

        // Create another B+ tree instance using the same header page
        let bptree2 = PersistentBPlusTree::from_existing(
            bpm.clone(),
            bptree1.header_page_id,
            index_info.clone(),
        );

        // Verify both trees have the same header
        assert_eq!(bptree1.header_page_id, bptree2.header_page_id);

        // Insert another key through the second tree
        let key = Value::from(100_i32);
        let rid = RID::new(100, 100);
        bptree2.insert(key, rid);

        // Verify the size was updated
        let header_page_guard = bpm
            .fetch_page::<BPlusTreeHeaderPage>(bptree1.header_page_id)
            .expect("Failed to fetch header page");

        {
            let header = header_page_guard.read();
            assert_eq!(header.get_size(), 6); // 5 original + 1 new key
        }
    }

    #[test]
    fn test_leaf_node_operations() {
        // Create a new leaf node directly and test its operations
        let bpm = create_test_buffer_pool_manager();

        let leaf_page_guard = bpm
            .new_page::<BPlusTreeNodePage>()
            .expect("Failed to create leaf page");

        let leaf_page_id = leaf_page_guard.get_page_id();

        {
            let mut leaf = leaf_page_guard.write();
            *leaf = BPlusTreeNodePage::new_leaf(leaf_page_id);

            // Test key and RID operations
            assert_eq!(leaf.get_num_keys(), 0);

            // Insert some key-RID pairs
            for i in 0..3 {
                let key = Value::from(i as i32 * 10);
                let rid = RID::new(i as u64, i as u32);
                leaf.set_key(i, &key);
                leaf.set_rid(i, rid);
            }

            leaf.set_num_keys(3);

            // Verify the insertions
            assert_eq!(leaf.get_num_keys(), 3);

            for i in 0..3 {
                let key = leaf.get_key(i);
                let rid = leaf.get_rid(i);

                match key.get_val() {
                    crate::types_db::value::Val::Integer(val) => {
                        assert_eq!(*val, i as i32 * 10);
                    }
                    _ => panic!("Expected integer value"),
                }

                assert_eq!(rid.get_page_id(), i as u64);
                assert_eq!(rid.get_slot_num(), i as u32);
            }

            // Test deletion
            leaf.delete_key_and_rid(1); // Delete the middle entry

            assert_eq!(leaf.get_num_keys(), 2);

            // Verify the remaining entries are correct
            let key0 = leaf.get_key(0);

            match key0.get_val() {
                crate::types_db::value::Val::Integer(val) => {
                    assert_eq!(*val, 0); // First key should still be 0
                }
                _ => panic!("Expected integer value"),
            }

            let key1 = leaf.get_key(1);

            match key1.get_val() {
                crate::types_db::value::Val::Integer(val) => {
                    assert_eq!(*val, 20); // Third key should now be at position 1
                }
                _ => panic!("Expected integer value"),
            }
        }
    }

    #[test]
    fn test_internal_node_operations() {
        // Create a new internal node directly and test its operations
        let bpm = create_test_buffer_pool_manager();

        let internal_page_guard = bpm
            .new_page::<BPlusTreeNodePage>()
            .expect("Failed to create internal page");

        let internal_page_id = internal_page_guard.get_page_id();

        {
            let mut internal = internal_page_guard.write();
            *internal = BPlusTreeNodePage::new_internal(internal_page_id);

            // Test key and child page ID operations
            assert_eq!(internal.get_num_keys(), 0);
            assert_eq!(internal.get_num_children(), 0);

            // Set up child pointers and keys
            internal.set_child_page_id(0, 100); // First child

            for i in 0..3 {
                let key = Value::from(i as i32 * 10);
                internal.set_key(i, &key);
                internal.set_child_page_id(i + 1, 100 + (i + 1) as u64);
            }

            internal.set_num_keys(3);
            internal.set_num_children(4);

            // Verify the setup
            assert_eq!(internal.get_num_keys(), 3);
            assert_eq!(internal.get_num_children(), 4);

            for i in 0..3 {
                let key = internal.get_key(i);

                match key.get_val() {
                    crate::types_db::value::Val::Integer(val) => {
                        assert_eq!(*val, i as i32 * 10);
                    }
                    _ => panic!("Expected integer value"),
                }
            }

            for i in 0..4 {
                let child_id = internal.get_child_page_id(i);
                assert_eq!(child_id, 100 + i as u64);
            }

            // Test deletion
            internal.delete_key_and_child(1); // Delete the middle entry and child

            assert_eq!(internal.get_num_keys(), 2);
            assert_eq!(internal.get_num_children(), 3);

            // Verify the remaining entries are correct
            let key0 = internal.get_key(0);

            match key0.get_val() {
                crate::types_db::value::Val::Integer(val) => {
                    assert_eq!(*val, 0); // First key should still be 0
                }
                _ => panic!("Expected integer value"),
            }

            let key1 = internal.get_key(1);

            match key1.get_val() {
                crate::types_db::value::Val::Integer(val) => {
                    assert_eq!(*val, 20); // Third key should now be at position 1
                }
                _ => panic!("Expected integer value"),
            }

            // Check children
            assert_eq!(internal.get_child_page_id(0), 100);
            assert_eq!(internal.get_child_page_id(1), 102);
            assert_eq!(internal.get_child_page_id(2), 103);
        }
    }

    #[test]
    fn test_random_insert_order() {
        // Create BPM and index info
        let bpm = create_test_buffer_pool_manager();
        let index_info = create_test_index_info();

        // Create a new B+ tree with order 3
        let bptree = PersistentBPlusTree::new(bpm, 3, index_info.clone());

        // Insert keys in random order
        let keys = vec![42, 15, 7, 23, 56, 38, 91, 4, 30, 19];

        for (i, &key_val) in keys.iter().enumerate() {
            let key = Value::from(key_val as i32);
            let rid = RID::new(i as u64, i as u32);
            let result = bptree.insert(key, rid);
            assert!(result);
        }

        // Verify tree size
        let header_page_guard = bptree
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(bptree.header_page_id)
            .expect("Failed to fetch header page");

        {
            let header = header_page_guard.read();
            assert_eq!(header.get_size(), keys.len()); // Size should match number of inserts
        }
    }

    #[test]
    fn test_header_page_operations() {
        // Create a header page directly and test its operations
        let bpm = create_test_buffer_pool_manager();

        let header_page_guard = bpm
            .new_page::<BPlusTreeHeaderPage>()
            .expect("Failed to create header page");

        {
            let mut header = header_page_guard.write();

            // Initial state
            assert_eq!(header.get_size(), 0);
            assert_eq!(header.get_order(), 0);
            assert_eq!(header.get_root_page_id(), None);

            // Set properties
            header.set_size(42);
            header.set_order(5);
            header.set_root_page_id(Some(123));

            // Verify properties
            assert_eq!(header.get_size(), 42);
            assert_eq!(header.get_order(), 5);
            assert_eq!(header.get_root_page_id(), Some(123));

            // Reset root page
            header.set_root_page_id(None);
            assert_eq!(header.get_root_page_id(), None);
        }
    }

    #[test]
    fn test_insert_performance() {
        // Create BPM and index info
        let bpm = create_test_buffer_pool_manager();
        let index_info = create_test_index_info();

        // Create a new B+ tree with order 4
        let bptree = PersistentBPlusTree::new(bpm, 4, index_info.clone());

        // Insert a larger number of keys to test performance
        let num_keys = 100; // Reduced from 1000 for faster testing
        let start_time = std::time::Instant::now();

        for i in 0..num_keys {
            let key = Value::from(i as i32);
            let rid = RID::new(i as u64, i as u32);
            bptree.insert(key, rid);
        }

        let elapsed = start_time.elapsed();
        println!("Time to insert {} keys: {:?}", num_keys, elapsed);

        // Verify the size was updated correctly
        let header_page_guard = bptree
            .buffer_pool_manager
            .fetch_page::<BPlusTreeHeaderPage>(bptree.header_page_id)
            .expect("Failed to fetch header page");

        {
            let header = header_page_guard.read();
            assert_eq!(header.get_size(), num_keys);
        }
    }

    #[test]
    fn test_find() {
        // Create BPM and index info
        let bpm = create_test_buffer_pool_manager();
        let index_info = create_test_index_info();

        // Create a new B+ tree with order 3
        let bptree = PersistentBPlusTree::new(bpm, 3, index_info.clone());

        // Insert some key-value pairs
        let test_data = vec![
            (5, RID::new(5, 5)),
            (10, RID::new(10, 10)),
            (15, RID::new(15, 15)),
            (20, RID::new(20, 20)),
            (25, RID::new(25, 25)),
        ];

        for &(key_val, rid) in &test_data {
            let key = Value::from(key_val as i32);
            bptree.insert(key, rid);
        }

        // Test finding existing keys
        for &(key_val, expected_rid) in &test_data {
            let key = Value::from(key_val as i32);
            let result = bptree.find(&key);

            assert!(result.is_some());
            assert_eq!(result.unwrap(), expected_rid);
        }

        // Test finding non-existent keys
        let non_existent_keys = vec![1, 7, 12, 22, 30];

        for &key_val in &non_existent_keys {
            let key = Value::from(key_val as i32);
            let result = bptree.find(&key);

            assert!(result.is_none());
        }
    }

    #[test]
    fn test_find_in_empty_tree() {
        // Create BPM and index info
        let bpm = create_test_buffer_pool_manager();
        let index_info = create_test_index_info();

        // Create a new B+ tree with order 3
        let bptree = PersistentBPlusTree::new(bpm, 3, index_info.clone());

        // Try to find a key in an empty tree
        let key = Value::from(42 as i32);
        let result = bptree.find(&key);

        assert!(result.is_none());
    }

    #[test]
    fn test_find_after_splits() {
        // Create BPM and index info
        let bpm = create_test_buffer_pool_manager();
        let index_info = create_test_index_info();

        // Create a new B+ tree with order 3 (small order to force splits)
        let bptree = PersistentBPlusTree::new(bpm, 3, index_info.clone());

        // Insert enough keys to force multiple splits
        let num_keys = 50;
        for i in 0..num_keys {
            let key = Value::from(i as i32);
            let rid = RID::new(i as u64, i as u32);
            bptree.insert(key, rid);
        }

        // Test finding keys throughout the tree
        for i in 0..num_keys {
            let key = Value::from(i as i32);
            let expected_rid = RID::new(i as u64, i as u32);
            let result = bptree.find(&key);

            assert!(result.is_some());
            assert_eq!(result.unwrap(), expected_rid);
        }
    }
}
