use crate::common::config::{PageId, DB_PAGE_SIZE, INVALID_PAGE_ID};
use crate::common::exception::PageError;
use crate::common::rid::RID;
use crate::common::time::TimeStamp;
use crate::storage::page::page::PageTrait;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use log;
use log::{debug, error};
use std::mem::size_of;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Represents a table page using a slotted page format.
///
/// # Slotted page format:
///  -------------------------------------------------------------
///  | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
///  -------------------------------------------------------------
///
///  Header format (size in bytes):
///  -------------------------------------------------------------
///  | NextPageId (4)| NumTuples(2) | NumDeletedTuples(2) |
///  -------------------------------------------------------------
///  -------------------------------------------------------------
///  | Tuple_1 offset+size (4) | Tuple_2 offset+size (4) | ... |
///  -------------------------------------------------------------
///
/// Tuple format:
/// | meta | data |
#[derive(Debug, Clone)]
pub struct TablePage {
    data: Box<[u8; DB_PAGE_SIZE as usize]>,
    header: TablePageHeader,
    tuple_info: Vec<(u16, u16, TupleMeta)>,
    is_dirty: bool,
    pin_count: i32,
}

#[derive(Debug, Clone)]
struct TablePageHeader {
    page_id: PageId,
    next_page_id: PageId,
    prev_page_id: PageId,
    num_tuples: u16,
    num_deleted_tuples: u16,
}

impl TablePageHeader {
    fn new(page_id: PageId) -> Self {
        Self {
            page_id,
            next_page_id: INVALID_PAGE_ID,
            prev_page_id: INVALID_PAGE_ID,
            num_tuples: 0,
            num_deleted_tuples: 0,
        }
    }

    fn size() -> usize {
        28 // 8 (page_id) + 8 (next_page_id) + 8 (prev_page_id) + 2 (num_tuples) + 2 (num_deleted_tuples)
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(Self::size());
        buffer.extend_from_slice(&self.page_id.to_le_bytes());
        buffer.extend_from_slice(&self.next_page_id.to_le_bytes());
        buffer.extend_from_slice(&self.prev_page_id.to_le_bytes());
        buffer.extend_from_slice(&self.num_tuples.to_le_bytes());
        buffer.extend_from_slice(&self.num_deleted_tuples.to_le_bytes());

        // Ensure we have the correct size
        debug_assert_eq!(buffer.len(), Self::size());
        buffer
    }

    fn deserialize(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < Self::size() {
            return Err(format!(
                "Buffer too small for header: {} < {}",
                bytes.len(),
                Self::size()
            ));
        }

        let mut offset = 0;

        // Read page_id (8 bytes)
        let page_id = PageId::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read next_page_id (8 bytes)
        let next_page_id = PageId::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read prev_page_id (8 bytes)
        let prev_page_id = PageId::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Read num_tuples (2 bytes)
        let num_tuples = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap());
        offset += 2;

        // Read num_deleted_tuples (2 bytes)
        let num_deleted_tuples = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap());

        Ok(Self {
            page_id,
            next_page_id,
            prev_page_id,
            num_tuples,
            num_deleted_tuples,
        })
    }
}

impl TablePage {
    /// Creates a new `TablePage` with the given page ID.
    pub fn new(page_id: PageId) -> Self {
        Self {
            data: Box::new([0; DB_PAGE_SIZE as usize]),
            header: TablePageHeader::new(page_id),
            tuple_info: Vec::new(),
            is_dirty: false,
            pin_count: 0,
        }
    }

    /// Initializes the table page.
    pub fn init(&mut self) {
        self.header = TablePageHeader::new(self.header.page_id);
        self.tuple_info.clear();
        self.is_dirty = true;
    }

    /// Gets the next offset to insert a tuple.
    pub fn get_next_tuple_offset(&self, tuple: &Tuple) -> Option<u16> {
        // First serialize the tuple to get its actual size
        let tuple_data = match bincode::serialize(tuple) {
            Ok(data) => data,
            Err(_) => return None,
        };
        let tuple_size = tuple_data.len() as u16;

        let slot_end_offset = if self.header.num_tuples > 0 {
            self.tuple_info[self.header.num_tuples as usize - 1].0
        } else {
            DB_PAGE_SIZE as u16
        };

        let tuple_offset = slot_end_offset.saturating_sub(tuple_size);
        let offset_size = TablePage::header_size() + self.tuple_info_size();

        if tuple_offset < offset_size {
            None
        } else {
            Some(tuple_offset)
        }
    }

    /// Low-level tuple insertion into the page
    pub fn insert_tuple(&mut self, meta: &TupleMeta, tuple: &mut Tuple) -> Option<RID> {
        // Get the next available offset for the tuple
        if let Some(tuple_offset) = self.get_next_tuple_offset(tuple) {
            // Create RID for the new tuple
            let rid = RID::new(self.header.page_id, self.header.num_tuples as u32);
            tuple.set_rid(rid);

            // Serialize tuple data
            let tuple_data = match bincode::serialize(tuple) {
                Ok(data) => data,
                Err(_) => return None,
            };

            // Store tuple metadata and data
            self.tuple_info
                .push((tuple_offset, tuple_data.len() as u16, meta.clone()));

            // Write tuple data to page
            let start = tuple_offset as usize;
            let end = start + tuple_data.len();
            if end <= self.data.len() {
                self.data[start..end].copy_from_slice(&tuple_data);
                self.header.num_tuples += 1;
                self.is_dirty = true;
                Some(rid)
            } else {
                self.tuple_info.pop();
                None
            }
        } else {
            None
        }
    }

    pub fn update_tuple(
        &mut self,
        meta: &TupleMeta,
        tuple: &mut Tuple,
        rid: RID,
    ) -> Result<(), PageError> {
        let tuple_id = rid.get_slot_num() as usize;
        if tuple_id >= self.header.num_tuples as usize {
            return Err(PageError::TupleInvalid);
        }

        // Serialize the new tuple data
        let tuple_data = match bincode::serialize(tuple) {
            Ok(data) => data,
            Err(_) => return Err(PageError::TupleInvalid),
        };
        let new_size = tuple_data.len() as u16;

        // Get current tuple info
        let (current_offset, current_size, _) = self.tuple_info[tuple_id];

        // If new tuple has same size, we can update in place
        if current_size == new_size {
            // Update the tuple metadata
            self.tuple_info[tuple_id].2 = meta.clone();

            // Update the tuple data
            let start = current_offset as usize;
            let end = start + new_size as usize;
            if end <= self.data.len() {
                self.data[start..end].copy_from_slice(&tuple_data);
                self.is_dirty = true;
                Ok(())
            } else {
                Err(PageError::TupleInvalid)
            }
        } else {
            // If sizes differ, we need to check if we have space for the new tuple
            if let Some(new_offset) = self.get_next_tuple_offset(tuple) {
                // Update tuple info
                self.tuple_info[tuple_id] = (new_offset, new_size, meta.clone());

                // Write new tuple data
                let start = new_offset as usize;
                let end = start + new_size as usize;
                if end <= self.data.len() {
                    self.data[start..end].copy_from_slice(&tuple_data);
                    self.is_dirty = true;
                    Ok(())
                } else {
                    Err(PageError::TupleInvalid)
                }
            } else {
                Err(PageError::TupleInvalid)
            }
        }
    }

    /// Updates the metadata of a tuple.
    pub fn update_tuple_meta(&mut self, meta: &TupleMeta, rid: &RID) -> Result<(), PageError> {
        let tuple_id = rid.get_slot_num() as usize;
        if tuple_id >= self.header.num_tuples as usize {
            return Err(PageError::TupleInvalid);
        }

        let old_meta = &mut self.tuple_info[tuple_id].2;
        if !old_meta.is_deleted() && meta.is_deleted() {
            self.header.num_deleted_tuples += 1;
        } else if old_meta.is_deleted() && !meta.is_deleted() {
            self.header.num_deleted_tuples = self.header.num_deleted_tuples.saturating_sub(1);
        }
        *old_meta = meta.clone();
        self.is_dirty = true;

        Ok(())
    }

    /// Gets a tuple from the table.
    pub fn get_tuple(&self, rid: &RID) -> Result<(TupleMeta, Tuple), PageError> {
        let tuple_id = rid.get_slot_num() as usize;
        debug!("Getting tuple with id: {}", tuple_id);
        debug!("Current num_tuples: {}", self.header.num_tuples);

        if tuple_id >= self.header.num_tuples as usize {
            error!(
                "Tuple ID {} >= num_tuples {}",
                tuple_id, self.header.num_tuples
            );
            return Err(PageError::TupleInvalid);
        }

        let (offset, size, meta) = &self.tuple_info[tuple_id];
        debug!("Tuple info - offset: {}, size: {}", offset, size);

        let start = *offset as usize;
        let end = start + *size as usize;

        if end > self.data.len() {
            error!("Tuple end {} > data length {}", end, self.data.len());
            return Err(PageError::TupleInvalid);
        }

        let tuple_data = &self.data[start..end];
        debug!(
            "Attempting to deserialize tuple data of length {}",
            tuple_data.len()
        );

        let tuple = match bincode::deserialize(tuple_data) {
            Ok(t) => t,
            Err(e) => {
                error!("Failed to deserialize tuple: {}", e);
                return Err(PageError::TupleInvalid);
            }
        };

        Ok((meta.clone(), tuple))
    }

    /// Gets the metadata of a tuple.
    pub fn get_tuple_meta(&self, rid: &RID) -> Result<TupleMeta, PageError> {
        let tuple_id = rid.get_slot_num() as usize;
        if tuple_id >= self.header.num_tuples as usize {
            return Err(PageError::TupleInvalid);
        }

        Ok(self.tuple_info[tuple_id].2.clone())
    }

    pub fn get_num_tuples(&self) -> u16 {
        self.header.num_tuples
    }

    pub fn get_num_deleted_tuples(&self) -> u16 {
        self.header.num_deleted_tuples
    }

    pub fn get_next_page_id(&self) -> PageId {
        self.header.next_page_id
    }

    pub fn set_next_page_id(&mut self, page_id: PageId) {
        self.header.next_page_id = page_id;
        self.is_dirty = true;
    }

    pub fn set_prev_page_id(&mut self, page_id: PageId) {
        self.header.prev_page_id = page_id;
        self.is_dirty = true;
    }

    /// Serializes the page into a fixed-size byte array
    pub fn serialize(&self) -> [u8; DB_PAGE_SIZE as usize] {
        let mut buffer = [0u8; DB_PAGE_SIZE as usize];
        let mut offset = 0;

        debug!("Serializing header");
        // Serialize header
        let header_bytes = self.header.serialize();
        debug!("Header size: {}", header_bytes.len());
        buffer[offset..offset + header_bytes.len()].copy_from_slice(&header_bytes);
        offset += header_bytes.len();

        // Write a magic number for validation
        let magic = 0xDEADBEEFu32;
        debug!("Writing magic number at offset {}", offset);
        buffer[offset..offset + 4].copy_from_slice(&magic.to_le_bytes());
        offset += 4;

        // Write tuple info count
        let tuple_count = self.tuple_info.len() as u32;
        debug!("Serializing tuple count: {}", tuple_count);
        buffer[offset..offset + 4].copy_from_slice(&tuple_count.to_le_bytes());
        offset += 4;

        // Write tuple info entries
        for (i, (tuple_offset, size, meta)) in self.tuple_info.iter().enumerate() {
            debug!(
                "Serializing tuple info {}: offset={}, size={}, timestamp={:?}",
                i,
                tuple_offset,
                size,
                meta.get_commit_timestamp()
            );

            // Write tuple offset and size
            buffer[offset..offset + 2].copy_from_slice(&tuple_offset.to_le_bytes());
            offset += 2;
            buffer[offset..offset + 2].copy_from_slice(&size.to_le_bytes());
            offset += 2;

            // Write tuple meta
            let meta_bytes = match bincode::serialize(&meta) {
                Ok(bytes) => bytes,
                Err(e) => {
                    error!("Failed to serialize tuple meta: {}", e);
                    continue;
                }
            };
            let meta_size = meta_bytes.len() as u32;
            debug!("Meta serialized size: {}", meta_size);

            buffer[offset..offset + 4].copy_from_slice(&meta_size.to_le_bytes());
            offset += 4;
            buffer[offset..offset + meta_bytes.len()].copy_from_slice(&meta_bytes);
            offset += meta_bytes.len();
        }

        // Write another magic number to mark the end of metadata
        let end_magic = 0xCAFEBABEu32;
        if offset + 4 <= buffer.len() {
            buffer[offset..offset + 4].copy_from_slice(&end_magic.to_le_bytes());
        }

        // Copy tuple data in their original positions
        for (tuple_offset, size, _) in &self.tuple_info {
            let start = *tuple_offset as usize;
            let end = start + *size as usize;
            if end <= self.data.len() && end <= buffer.len() {
                buffer[start..end].copy_from_slice(&self.data[start..end]);
            }
        }

        buffer
    }

    /// Deserializes a page from a fixed-size byte array
    pub fn deserialize(bytes: &[u8; DB_PAGE_SIZE as usize]) -> Result<Self, String> {
        let mut offset = 0;

        debug!("Deserializing header");
        // Deserialize header
        let header_size = TablePageHeader::size();
        debug!("Expected header size: {}", header_size);
        let header = TablePageHeader::deserialize(&bytes[..header_size])?;
        offset += header_size;

        debug!("Reading magic number at offset {}", offset);
        // Verify magic number
        let magic = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        if magic != 0xDEADBEEF {
            return Err(format!(
                "Invalid magic number in page data: {:x} != {:x}",
                magic, 0xDEADBEEFu32 as i32
            ));
        }
        offset += 4;

        // Read tuple count
        let tuple_count = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        debug!("Tuple count: {}", tuple_count);

        if tuple_count > (DB_PAGE_SIZE / 8) as u32 {
            return Err(format!("Invalid tuple count: {}", tuple_count));
        }

        let mut page = TablePage {
            data: Box::new(*bytes),
            header,
            tuple_info: Vec::with_capacity(tuple_count as usize),
            is_dirty: false,
            pin_count: 0,
        };

        // Read tuple info entries
        for i in 0..tuple_count {
            debug!("Reading tuple info {}", i);

            if offset + 8 > bytes.len() {
                return Err("Unexpected end of data while reading tuple info".to_string());
            }

            // Read tuple offset and size
            let tuple_offset = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap());
            offset += 2;
            let size = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap());
            offset += 2;

            // Read meta size
            let meta_size =
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;

            if meta_size > 1024 {
                // Reasonable max size for meta
                return Err(format!("Invalid meta size: {}", meta_size));
            }

            debug!("Reading meta at offset {} with size {}", offset, meta_size);

            if offset + meta_size > bytes.len() {
                return Err("Unexpected end of data while reading tuple meta".to_string());
            }

            // Deserialize meta
            let meta: TupleMeta = bincode::deserialize(&bytes[offset..offset + meta_size])
                .map_err(|e| {
                    format!(
                        "Failed to deserialize tuple meta at offset {}: {}",
                        offset, e
                    )
                })?;
            offset += meta_size;

            page.tuple_info.push((tuple_offset, size, meta));
        }

        // Verify end magic number
        let end_magic = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        if end_magic != 0xCAFEBABE {
            return Err("Invalid end magic number in page data".to_string());
        }

        Ok(page)
    }

    /// Updates a tuple in place.
    ///
    /// # Safety
    ///
    /// This method is unsafe because it doesn't perform any bounds checking.
    pub unsafe fn update_tuple_in_place_unsafe(
        &mut self,
        meta: &TupleMeta,
        tuple: &Tuple,
        rid: RID,
    ) -> Result<(), PageError> {
        let tuple_id = rid.get_slot_num() as usize;
        if tuple_id >= self.header.num_tuples as usize {
            return Err(PageError::TupleInvalid);
        }

        let (offset, size, old_meta) = &mut self.tuple_info[tuple_id];

        // Serialize the tuple to get its actual size
        let tuple_data = match bincode::serialize(tuple) {
            Ok(data) => data,
            Err(_) => return Err(PageError::TupleInvalid),
        };

        if *size as usize != tuple_data.len() {
            return Err(PageError::TupleInvalid);
        }

        if !old_meta.is_deleted() && meta.is_deleted() {
            self.header.num_deleted_tuples += 1;
        }

        *old_meta = meta.clone();
        self.data[*offset as usize..(*offset + *size) as usize].copy_from_slice(&tuple_data);
        self.is_dirty = true;

        Ok(())
    }

    fn header_size() -> u16 {
        (size_of::<PageId>() + size_of::<u16>() + size_of::<u16>()) as u16
    }

    fn tuple_info_size(&self) -> u16 {
        (self.header.num_tuples as usize * size_of::<(u16, u16, TupleMeta)>()) as u16
    }

    /// Gets the total size of all tuple data in the page
    fn get_tuple_data_size(&self) -> usize {
        // Simply sum up the sizes stored in tuple_info
        self.tuple_info
            .iter()
            .map(|(_, size, _)| *size as usize)
            .sum()
    }

    /// Gets the amount of free space available in the page
    pub fn get_free_space(&self) -> usize {
        let header_size = TablePageHeader::size() as usize;
        let tuple_info_size = self.tuple_info_size() as usize;
        let tuple_data_size = self.get_tuple_data_size();

        DB_PAGE_SIZE as usize - header_size - tuple_info_size - tuple_data_size
    }

    /// Checks if the page has enough space for a tuple
    pub fn has_space_for<'a>(&self, tuple_or_size: impl Into<TupleSizeInput<'a>>) -> bool {
        let required_size = match tuple_or_size.into() {
            TupleSizeInput::Tuple(tuple) => {
                // Get size from serialized tuple
                match bincode::serialize(tuple) {
                    Ok(data) => data.len(),
                    Err(_) => return false,
                }
            }
            TupleSizeInput::Size(size) => size,
        };

        // Calculate required space including metadata
        let tuple_info_entry_size = std::mem::size_of::<(u16, u16, TupleMeta)>();
        let total_required_space = required_size + tuple_info_entry_size;

        // Check if we have enough space
        self.get_free_space() >= total_required_space
    }
}

/// Input type for has_space_for method
pub enum TupleSizeInput<'a> {
    Tuple(&'a Tuple),
    Size(usize),
}

impl<'a> From<&'a Tuple> for TupleSizeInput<'a> {
    fn from(tuple: &'a Tuple) -> Self {
        TupleSizeInput::Tuple(tuple)
    }
}

impl From<usize> for TupleSizeInput<'_> {
    fn from(size: usize) -> Self {
        TupleSizeInput::Size(size)
    }
}

impl PageTrait for TablePage {
    fn get_page_id(&self) -> PageId {
        self.header.page_id
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
        self.data[offset..offset + new_data.len()].copy_from_slice(new_data);
        Ok(())
    }

    fn set_pin_count(&mut self, pin_count: i32) {
        self.pin_count = pin_count;
    }

    fn reset_memory(&mut self) {
        self.data = Box::new([0; DB_PAGE_SIZE as usize]);
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    fn create_test_tuple(id: i32) -> (TupleMeta, Tuple) {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let values = vec![Value::from(id), Value::from("Test".to_string())];
        let rid = RID::new(1, 0);
        let tuple = Tuple::new(&values, schema, rid);
        let meta = TupleMeta::new(123);
        (meta, tuple)
    }

    #[test]
    fn test_insert_and_get_tuple() {
        let mut page = TablePage::new(1);
        let (meta, mut tuple) = create_test_tuple(1);

        let tuple_id = page.insert_tuple(&meta, &mut tuple).unwrap();
        assert_eq!(page.header.num_tuples, 1);

        let (retrieved_meta, retrieved_tuple) = page.get_tuple(&tuple_id).unwrap();
        assert_eq!(
            retrieved_meta.get_commit_timestamp(),
            meta.get_commit_timestamp()
        );
        assert_eq!(retrieved_meta.is_deleted(), meta.is_deleted());
        assert_eq!(retrieved_tuple.get_value(0), tuple.get_value(0));
    }

    #[test]
    fn test_update_tuple_meta() {
        let mut page = TablePage::new(1);
        let (meta, mut tuple) = create_test_tuple(1);

        let tuple_rid_id = page.insert_tuple(&meta, &mut tuple).unwrap();

        // Create new metadata with deleted flag set to true
        let mut new_meta = TupleMeta::new(456);
        new_meta.set_deleted(true);

        page.update_tuple_meta(&new_meta, &tuple_rid_id).unwrap();
        let retrieved_meta = page.get_tuple_meta(&tuple_rid_id).unwrap();

        assert_eq!(
            retrieved_meta.get_commit_timestamp(),
            new_meta.get_commit_timestamp()
        );
        assert_eq!(retrieved_meta.is_deleted(), new_meta.is_deleted());
        assert_eq!(page.header.num_deleted_tuples, 1);
    }

    #[test]
    fn test_page_full() {
        let mut page = TablePage::new(1);
        let (meta, mut tuple) = create_test_tuple(1);

        let mut inserted_count = 0;
        while page.insert_tuple(&meta, &mut tuple).is_some() {
            inserted_count += 1;
        }

        assert!(inserted_count > 0);
        assert!(page.get_next_tuple_offset(&tuple).is_none());
    }

    #[test]
    fn test_update_tuple_in_place_unsafe() {
        let mut page = TablePage::new(1);
        let (meta, mut tuple) = create_test_tuple(1);

        let tuple_id = page.insert_tuple(&meta, &mut tuple).unwrap();

        let new_meta = TupleMeta::new(789);
        let new_tuple = create_test_tuple(2).1;

        unsafe {
            page.update_tuple_in_place_unsafe(&new_meta, &new_tuple, tuple_id)
                .unwrap();
        }

        let (retrieved_meta, retrieved_tuple) = page.get_tuple(&tuple_id).unwrap();
        assert_eq!(
            retrieved_meta.get_commit_timestamp(),
            new_meta.get_commit_timestamp()
        );
        assert_eq!(retrieved_tuple.get_value(0), new_tuple.get_value(0));
    }
    #[test]
    fn test_table_page_creation() {
        let page = TablePage::new(1);
        assert_eq!(page.get_page_id(), 1);
        assert_eq!(page.header.num_tuples, 0);
        assert_eq!(page.header.num_deleted_tuples, 0);
        assert_eq!(page.header.next_page_id, INVALID_PAGE_ID);
        assert!(!page.is_dirty());
        assert_eq!(page.pin_count, 0);
    }

    #[test]
    fn test_page_initialization() {
        let mut page = TablePage::new(1);
        page.set_next_page_id(2);
        page.set_prev_page_id(3);
        page.set_dirty(true);
        page.increment_pin_count();

        page.init();

        assert_eq!(page.header.next_page_id, INVALID_PAGE_ID);
        assert_eq!(page.header.prev_page_id, INVALID_PAGE_ID);
        assert_eq!(page.header.num_tuples, 0);
        assert_eq!(page.header.num_deleted_tuples, 0);
        assert!(page.tuple_info.is_empty());
    }
}

#[cfg(test)]
mod tuple_operation_tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    fn create_test_tuple(id: i32) -> (TupleMeta, Tuple) {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let values = vec![Value::from(id), Value::from("Test".to_string())];
        let rid = RID::new(1, 0);
        let tuple = Tuple::new(&values, schema, rid);
        let meta = TupleMeta::new(123);
        (meta, tuple)
    }

    #[test]
    fn test_basic_tuple_insertion() {
        let mut page = TablePage::new(1);
        let (meta, mut tuple) = create_test_tuple(1);

        let rid = page.insert_tuple(&meta, &mut tuple).unwrap();
        assert_eq!(page.header.num_tuples, 1);
        assert_eq!(rid.get_page_id(), 1);
        assert_eq!(rid.get_slot_num(), 0);
    }

    #[test]
    fn test_tuple_retrieval() {
        let mut page = TablePage::new(1);
        let (meta, mut tuple) = create_test_tuple(1);

        let rid = page.insert_tuple(&meta, &mut tuple).unwrap();
        let (retrieved_meta, retrieved_tuple) = page.get_tuple(&rid).unwrap();

        assert_eq!(
            retrieved_meta.get_commit_timestamp(),
            meta.get_commit_timestamp()
        );
        assert_eq!(retrieved_meta.is_deleted(), meta.is_deleted());
        assert_eq!(retrieved_tuple.get_value(0), tuple.get_value(0));
        assert_eq!(retrieved_tuple.get_value(1), tuple.get_value(1));
    }

    #[test]
    fn test_tuple_metadata_update() {
        let mut page = TablePage::new(1);
        let (meta, mut tuple) = create_test_tuple(1);

        let rid = page.insert_tuple(&meta, &mut tuple).unwrap();

        // Create new metadata with deleted flag set to true
        let mut new_meta = TupleMeta::new(456);
        new_meta.set_deleted(true);

        page.update_tuple_meta(&new_meta, &rid).unwrap();
        let retrieved_meta = page.get_tuple_meta(&rid).unwrap();

        assert_eq!(
            retrieved_meta.get_commit_timestamp(),
            new_meta.get_commit_timestamp()
        );
        assert_eq!(retrieved_meta.is_deleted(), new_meta.is_deleted());
        assert_eq!(page.header.num_deleted_tuples, 1);
    }

    #[test]
    fn test_tuple_in_place_update() {
        let mut page = TablePage::new(1);
        let (meta, mut tuple) = create_test_tuple(1);

        let rid = page.insert_tuple(&meta, &mut tuple).unwrap();
        let new_meta = TupleMeta::new(789);
        let new_tuple = create_test_tuple(2).1;

        unsafe {
            page.update_tuple_in_place_unsafe(&new_meta, &new_tuple, rid)
                .unwrap();
        }

        let (retrieved_meta, retrieved_tuple) = page.get_tuple(&rid).unwrap();
        assert_eq!(
            retrieved_meta.get_commit_timestamp(),
            new_meta.get_commit_timestamp()
        );
        assert_eq!(retrieved_tuple.get_value(0), new_tuple.get_value(0));
    }
}

#[cfg(test)]
mod error_handling_tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    fn create_test_tuple(id: i32) -> (TupleMeta, Tuple) {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let values = vec![Value::from(id), Value::from("Test".to_string())];
        let rid = RID::new(1, 0);
        let tuple = Tuple::new(&values, schema, rid);
        let meta = TupleMeta::new(123);
        (meta, tuple)
    }

    #[test]
    fn test_invalid_tuple_retrieval() {
        let page = TablePage::new(1);
        let invalid_rid = RID::new(1, 100);
        assert!(matches!(
            page.get_tuple(&invalid_rid),
            Err(PageError::TupleInvalid)
        ));
    }

    #[test]
    fn test_invalid_meta_update() {
        let mut page = TablePage::new(1);
        let invalid_rid = RID::new(1, 100);
        let meta = TupleMeta::new(123);
        assert!(matches!(
            page.update_tuple_meta(&meta, &invalid_rid),
            Err(PageError::TupleInvalid)
        ));
    }

    #[test]
    fn test_invalid_in_place_update() {
        let mut page = TablePage::new(1);
        let (meta, tuple) = create_test_tuple(1);
        let invalid_rid = RID::new(1, 100);

        unsafe {
            assert!(matches!(
                page.update_tuple_in_place_unsafe(&meta, &tuple, invalid_rid),
                Err(PageError::TupleInvalid)
            ));
        }
    }
}

#[cfg(test)]
mod capacity_tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    fn create_test_tuple(id: i32) -> (TupleMeta, Tuple) {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let values = vec![Value::from(id), Value::from("Test".to_string())];
        let rid = RID::new(1, 0);
        let tuple = Tuple::new(&values, schema, rid);
        let meta = TupleMeta::new(123);
        (meta, tuple)
    }

    #[test]
    fn test_page_capacity() {
        let mut page = TablePage::new(1);
        let (meta, mut tuple) = create_test_tuple(1);

        let mut inserted_count = 0;
        while page.insert_tuple(&meta, &mut tuple).is_some() {
            inserted_count += 1;
        }

        assert!(inserted_count > 0);
        assert!(page.get_next_tuple_offset(&tuple).is_none());
    }

    #[test]
    fn test_space_management() {
        let mut page = TablePage::new(1);
        let (meta, mut tuple) = create_test_tuple(1);

        // Insert first tuple
        page.insert_tuple(&meta, &mut tuple).unwrap();
        let first_offset = page.tuple_info[0].0;

        // Insert second tuple
        page.insert_tuple(&meta, &mut tuple).unwrap();
        let second_offset = page.tuple_info[1].0;

        // Verify tuples are packed from the end of the page
        assert!(second_offset < first_offset);
        assert_eq!(page.header.num_tuples, 2);
    }

    #[test]
    fn test_header_size_constraints() {
        let page = TablePage::new(1);
        assert!(TablePage::header_size() < DB_PAGE_SIZE as u16);
        assert_eq!(page.tuple_info_size(), 0);
    }
}

#[cfg(test)]
mod serialization_tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::common::logger::initialize_logger;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use log::{debug, error};

    fn create_test_tuple() -> (TupleMeta, Tuple) {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let values = vec![Value::new(42), Value::new("Test")];
        let rid = RID::new(1, 0);
        let tuple = Tuple::new(&values, schema, rid);
        let mut meta = TupleMeta::new(123);
        meta.set_commit_timestamp(TimeStamp::new(123));
        (meta, tuple)
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        initialize_logger();

        // Create and populate original page
        let mut original_page = TablePage::new(1);
        let (meta, mut tuple) = create_test_tuple();

        debug!("Inserting tuple into original page");
        let rid = original_page.insert_tuple(&meta, &mut tuple).unwrap();
        debug!("Original page state after insert:");
        debug!("  num_tuples: {}", original_page.header.num_tuples);
        debug!("  tuple_info: {:?}", original_page.tuple_info);

        // Verify original insertion
        let (orig_meta, orig_tuple) = original_page.get_tuple(&rid).unwrap();
        assert_eq!(orig_meta.get_commit_timestamp(), TimeStamp::new(123));
        assert_eq!(orig_tuple.get_value(0), &Value::new(42));

        // Serialize
        debug!("Serializing page");
        let serialized = original_page.serialize();

        // Deserialize
        debug!("Deserializing page");
        let deserialized_page = TablePage::deserialize(&serialized).unwrap();
        debug!("Deserialized page state:");
        debug!("  num_tuples: {}", deserialized_page.header.num_tuples);
        debug!("  tuple_info: {:?}", deserialized_page.tuple_info);

        // Verify metadata
        assert_eq!(deserialized_page.get_page_id(), original_page.get_page_id());
        assert_eq!(
            deserialized_page.header.num_tuples,
            original_page.header.num_tuples
        );

        // Verify tuple
        debug!("Retrieving tuple from deserialized page");
        let (deserialized_meta, deserialized_tuple) = deserialized_page.get_tuple(&rid).unwrap();

        debug!("Comparing metadata");
        assert_eq!(
            deserialized_meta.get_commit_timestamp(),
            meta.get_commit_timestamp()
        );
        assert_eq!(deserialized_meta.is_deleted(), meta.is_deleted());

        debug!("Comparing tuple values");
        assert_eq!(deserialized_tuple.get_value(0), tuple.get_value(0));
        assert_eq!(deserialized_tuple.get_value(1), tuple.get_value(1));
    }

    #[test]
    fn test_concurrent_reads() {
        initialize_logger();

        // Create a mutex-protected page
        let page = Arc::new(Mutex::new(TablePage::new(1)));

        // Insert test tuple before spawning threads
        {
            let mut page_guard = page.lock().unwrap();
            let (meta, mut tuple) = create_test_tuple();
            debug!("Original meta timestamp: {:?}", meta.get_commit_timestamp());
            page_guard.insert_tuple(&meta, &mut tuple).unwrap();

            // Verify initial insertion
            let rid = RID::new(1, 0);
            let (initial_meta, initial_tuple) = page_guard.get_tuple(&rid).unwrap();
            assert_eq!(initial_meta.get_commit_timestamp(), TimeStamp::new(123));
            assert_eq!(initial_tuple.get_value(0), &Value::new(42));
        }

        let mut handles = vec![];
        let rid = RID::new(1, 0);

        // Spawn reader threads with timeout
        for i in 0..3 {
            let page_clone = Arc::clone(&page);
            let handle = thread::spawn(move || {
                let result = std::panic::catch_unwind(|| {
                    // Add timeout for lock acquisition
                    let page_guard = match page_clone.try_lock() {
                        Ok(guard) => guard,
                        Err(_) => {
                            debug!("Thread {} failed to acquire lock, retrying...", i);
                            thread::sleep(Duration::from_millis(10));
                            page_clone.lock().unwrap()
                        }
                    };

                    match page_guard.get_tuple(&rid) {
                        Ok((retrieved_meta, retrieved_tuple)) => {
                            debug!(
                                "Thread {} - Retrieved meta timestamp: {:?}",
                                i,
                                retrieved_meta.get_commit_timestamp()
                            );

                            assert_eq!(retrieved_meta.get_commit_timestamp(), TimeStamp::new(123));
                            assert_eq!(retrieved_tuple.get_value(0), &Value::new(42));
                            Ok(())
                        }
                        Err(e) => {
                            error!("Thread {} failed to retrieve tuple: {:?}", i, e);
                            Err(e)
                        }
                    }
                });

                match result {
                    Ok(inner_result) => inner_result,
                    Err(e) => {
                        error!("Thread {} panicked: {:?}", i, e);
                        Err(PageError::TupleInvalid)
                    }
                }
            });
            handles.push(handle);

            // Add small delay between thread spawns
            thread::sleep(Duration::from_millis(1));
        }

        // Wait for all threads with timeout
        for (i, handle) in handles.into_iter().enumerate() {
            match handle.join() {
                Ok(result) => {
                    result.expect(&format!("Thread {} failed to read tuple", i));
                }
                Err(e) => {
                    error!("Thread {} panicked: {:?}", i, e);
                    panic!("Thread {} failed: {:?}", i, e);
                }
            }
        }
    }
}
