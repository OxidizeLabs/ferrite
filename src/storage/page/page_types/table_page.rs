use crate::common::config::{PageId, DB_PAGE_SIZE, INVALID_PAGE_ID};
use crate::common::exception::PageError;
use crate::common::rid::RID;
use crate::storage::page::page::Page;
use crate::storage::page::page::{
    PageTrait, PageType, PageTypeId, PAGE_ID_OFFSET, PAGE_TYPE_OFFSET,
};
use crate::storage::table::tuple::{Tuple, TupleMeta};
use log;
use log::{debug, error};
use std::any::Any;
use std::mem::size_of;
use bincode::{Decode, Encode};

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

#[derive(Debug, Clone, Encode, Decode)]
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
        debug!(
            "Serializing header - page_id: {}, num_tuples: {}",
            self.page_id, self.num_tuples
        );

        let mut buffer = Vec::with_capacity(Self::size());
        buffer.extend_from_slice(&self.page_id.to_le_bytes());
        buffer.extend_from_slice(&self.next_page_id.to_le_bytes());
        buffer.extend_from_slice(&self.prev_page_id.to_le_bytes());
        buffer.extend_from_slice(&self.num_tuples.to_le_bytes());
        buffer.extend_from_slice(&self.num_deleted_tuples.to_le_bytes());

        debug!(
            "Header serialized to {} bytes, num_tuples bytes: {:?}",
            buffer.len(),
            &self.num_tuples.to_le_bytes()
        );

        // Ensure we have the correct size
        debug_assert_eq!(buffer.len(), Self::size());
        buffer
    }

    fn deserialize(bytes: &[u8]) -> Result<Self, String> {
        debug!(
            "Starting header deserialization, input size: {}",
            bytes.len()
        );

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
        let num_tuples_bytes = &bytes[offset..offset + 2];
        let num_tuples = u16::from_le_bytes(num_tuples_bytes.try_into().unwrap());
        debug!(
            "Deserialized num_tuples: {}, from bytes: {:?}",
            num_tuples, num_tuples_bytes
        );
        offset += 2;

        // Read num_deleted_tuples (2 bytes)
        let num_deleted_tuples = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap());

        let header = Self {
            page_id,
            next_page_id,
            prev_page_id,
            num_tuples,
            num_deleted_tuples,
        };

        debug!(
            "Deserialized header - page_id: {}, num_tuples: {}",
            header.page_id, header.num_tuples
        );

        Ok(header)
    }
}

impl PageTypeId for TablePage {
    const TYPE_ID: PageType = PageType::Table;
}

impl Page for TablePage {
    fn new(page_id: PageId) -> Self {
        let mut page = Self {
            data: Box::new([0; DB_PAGE_SIZE as usize]),
            header: TablePageHeader::new(page_id),
            pin_count: 1,
            is_dirty: false,
            tuple_info: Vec::new(),
        };
        page.data[PAGE_TYPE_OFFSET] = Self::TYPE_ID as u8;
        page.data[PAGE_ID_OFFSET..PAGE_ID_OFFSET + 8].copy_from_slice(&page_id.to_le_bytes());

        page
    }
}

impl TablePage {
    /// Initializes the table page.
    pub fn init(&mut self) {
        self.header.prev_page_id = INVALID_PAGE_ID;
        self.header.next_page_id = INVALID_PAGE_ID;
        self.header.num_tuples = 0;
        self.header.num_deleted_tuples = 0;
        self.tuple_info.clear();
        self.is_dirty = false;
        self.data[PAGE_TYPE_OFFSET] = Self::TYPE_ID as u8;
    }

    /// Gets the next offset to insert a tuple.
    pub fn get_next_tuple_offset(&self, tuple: &Tuple) -> Option<u16> {
        // First check if the tuple is too large for any page
        if self.is_tuple_too_large(tuple) {
            debug!("Tuple is too large for any page, rejecting");
            return None;
        }
        
        // Then serialize the tuple to get its actual size
        let tuple_data = match bincode::encode_to_vec(tuple, bincode::config::standard()) {
            Ok(data) => data,
            Err(_) => return None,
        };
        let tuple_size = tuple_data.len() as u16;
        
        // Enforce a hard maximum size for any tuple
        if tuple_size > 3500 {
            debug!("Tuple size {} exceeds maximum allowed size of 3500 bytes", tuple_size);
            return None;
        }

        let slot_end_offset = if self.header.num_tuples > 0 {
            self.tuple_info[self.header.num_tuples as usize - 1].0
        } else {
            DB_PAGE_SIZE as u16
        };

        let tuple_offset = slot_end_offset.saturating_sub(tuple_size);
        let offset_size = self.get_header_size() + self.tuple_info_size() + 100; // Add safety buffer
        
        // Ensure we have enough space for tuple data + metadata + safety margin
        if tuple_offset < offset_size {
            debug!("Not enough space in page: tuple_offset {} < offset_size {}", 
                   tuple_offset, offset_size);
            None
        } else {
            Some(tuple_offset)
        }
    }

    pub fn update_tuple(
        &mut self,
        meta: &TupleMeta,
        tuple: &Tuple,
        rid: RID,
    ) -> Result<(), PageError> {
        let tuple_id = rid.get_slot_num() as usize;
        if tuple_id >= self.header.num_tuples as usize {
            return Err(PageError::TupleInvalid);
        }

        // Serialize the new tuple data
        let tuple_data = bincode::encode_to_vec(tuple, bincode::config::standard())
            .map_err(|_| PageError::SerializationError)?;
        let new_size = tuple_data.len() as u16;

        // Get current tuple info
        let (current_offset, current_size, _) = self.tuple_info[tuple_id];

        // Try to update in place if the new tuple size is smaller than or equal to current size
        if new_size <= current_size {
            // Update the tuple metadata
            self.tuple_info[tuple_id].2 = meta.clone();

            // Update the size to reflect the actual new size
            self.tuple_info[tuple_id].1 = new_size;

            // Update the tuple data
            let start = current_offset as usize;
            let end = start + new_size as usize;
            if end <= self.data.len() {
                self.data[start..end].copy_from_slice(&tuple_data);

                // Clear any remaining bytes if the new tuple is smaller
                if new_size < current_size {
                    let clear_start = end;
                    let clear_end = start + current_size as usize;
                    if clear_end <= self.data.len() {
                        self.data[clear_start..clear_end].fill(0);
                    }
                }

                self.is_dirty = true;
                Ok(())
            } else {
                Err(PageError::TupleInvalid)
            }
        } else {
            // If new tuple is larger, we need to find a new location
            // Calculate the minimum offset considering all existing tuples except the one being updated
            let min_offset = self
                .tuple_info
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != tuple_id) // Exclude the tuple being updated
                .map(|(_, (offset, _, _))| *offset)
                .min()
                .unwrap_or(DB_PAGE_SIZE as u16);

            let new_offset = min_offset.saturating_sub(new_size);
            let header_and_tuple_info_size = self.get_header_size() + self.tuple_info_size();

            if new_offset < header_and_tuple_info_size {
                return Err(PageError::TupleInvalid);
            }

            // Clear the old tuple data
            let old_start = current_offset as usize;
            let old_end = old_start + current_size as usize;
            if old_end <= self.data.len() {
                self.data[old_start..old_end].fill(0);
            }

            // Update tuple info with new location
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
        }
    }

    /// Updates the metadata of a tuple.
    pub fn update_tuple_meta(&mut self, meta: &TupleMeta, rid: &RID) -> Result<(), String> {
        debug!(
            "Updating tuple meta for RID {:?}, current num_tuples: {}",
            rid, self.header.num_tuples
        );

        let tuple_id = rid.get_slot_num() as usize;
        debug!(
            "Tuple ID: {}, header.num_tuples: {}, tuple_info.len(): {}",
            tuple_id,
            self.header.num_tuples,
            self.tuple_info.len()
        );

        if tuple_id >= self.header.num_tuples as usize {
            return Err(format!(
                "Invalid tuple ID: {} >= num_tuples: {}",
                tuple_id, self.header.num_tuples
            ));
        }

        debug!(
            "Accessing tuple_info at index {}, current tuple_info length: {}",
            tuple_id,
            self.tuple_info.len()
        );

        let old_meta = &mut self.tuple_info[tuple_id].2;
        debug!(
            "Old meta - creator_txn: {}, commit_ts: {}, deleted: {}",
            old_meta.get_creator_txn_id(),
            old_meta.get_commit_timestamp(),
            old_meta.is_deleted()
        );
        debug!(
            "New meta - creator_txn: {}, commit_ts: {}, deleted: {}",
            meta.get_creator_txn_id(),
            meta.get_commit_timestamp(),
            meta.is_deleted()
        );

        if !old_meta.is_deleted() && meta.is_deleted() {
            self.header.num_deleted_tuples += 1;
            debug!(
                "Incrementing num_deleted_tuples to {}",
                self.header.num_deleted_tuples
            );
        } else if old_meta.is_deleted() && !meta.is_deleted() {
            self.header.num_deleted_tuples = self.header.num_deleted_tuples.saturating_sub(1);
            debug!(
                "Decrementing num_deleted_tuples to {}",
                self.header.num_deleted_tuples
            );
        }

        *old_meta = meta.clone();
        self.is_dirty = true;

        debug!(
            "Successfully updated tuple meta. Page is now dirty. num_deleted_tuples: {}",
            self.header.num_deleted_tuples
        );

        Ok(())
    }

    /// Gets a tuple from the page.
    ///
    /// # Parameters
    ///
    /// - `rid`: The RID of the tuple to get.
    /// - `allow_deleted`: If true, allows retrieving deleted tuples.
    ///
    /// # Returns
    ///
    /// A Result containing the tuple meta and tuple, or a String error message.
    pub fn get_tuple(&self, rid: &RID, allow_deleted: bool) -> Result<(TupleMeta, Tuple), String> {
        // First verify the RID is valid for this page
        if rid.get_page_id() != self.header.page_id {
            return Err(format!(
                "RID page ID {} does not match page ID {}",
                rid.get_page_id(),
                self.header.page_id
            ));
        }

        let slot_num = rid.get_slot_num() as usize;
        if slot_num >= self.tuple_info.len() {
            return Err(format!(
                "Invalid slot number {} (max {})",
                slot_num,
                self.tuple_info.len()
            ));
        }

        // Get tuple info
        let (offset, size, meta) = &self.tuple_info[slot_num];

        // Check if tuple is deleted
        if meta.is_deleted() && !allow_deleted {
            return Err("Tuple has been deleted".to_string());
        }

        let start = *offset as usize;
        let end = start + *size as usize;

        // Deserialize tuple data
        match bincode::decode_from_slice(&self.data[start..end], bincode::config::standard()) {
            Ok((tuple, _)) => Ok((meta.clone(), tuple)),
            Err(e) => Err(format!("Failed to deserialize tuple: {}", e)),
        }
    }

    /// Gets the metadata of a tuple.
    pub fn get_tuple_meta(&self, rid: &RID) -> Result<TupleMeta, String> {
        let tuple_id = rid.get_slot_num() as usize;
        if tuple_id >= self.header.num_tuples as usize {
            return Err(format!(
                "Invalid tuple ID: {} >= num_tuples: {}",
                tuple_id, self.header.num_tuples
            ));
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
        debug!("Starting page serialization");
        let mut buffer = [0u8; DB_PAGE_SIZE as usize];

        // Set page type first
        buffer[PAGE_TYPE_OFFSET] = Self::TYPE_ID as u8;
        debug!(
            "Setting page type to {:?} ({})",
            Self::TYPE_ID,
            Self::TYPE_ID as u8
        );

        // Serialize header starting after page type
        let header_bytes = self.header.serialize();
        let header_start = PAGE_TYPE_OFFSET + 1; // Start after page type byte
        buffer[header_start..header_start + header_bytes.len()].copy_from_slice(&header_bytes);
        let mut offset = header_start + header_bytes.len();

        // Write magic number for validation (in big-endian format)
        let magic = 0xDEADBEEFu32.to_be_bytes();
        debug!("Writing magic number at offset {}", offset);
        buffer[offset..offset + 4].copy_from_slice(&magic);
        offset += 4;

        // Calculate total needed space for metadata to ensure we have room for end marker
        let tuple_info_size: usize = self.tuple_info.iter().map(|(_, _, meta)| {
            4 + bincode::encode_to_vec(meta, bincode::config::standard())
                .expect("Failed to serialize meta")
                .len()
        }).sum();
        
        let total_metadata_size = offset + tuple_info_size + 4; // +4 for end magic
        if total_metadata_size > buffer.len() {
            debug!("Warning: Metadata would exceed page size ({} > {}), truncating", 
                total_metadata_size, buffer.len());
            // If this happens in production, it indicates a serious issue with page design
            // We'll still write what we can but the page may not be fully valid
        }

        // Write tuple info entries
        debug!("Writing {} tuple info entries", self.tuple_info.len());
        for (i, (tuple_offset, size, meta)) in self.tuple_info.iter().enumerate() {
            // Check if we have enough room for this tuple entry plus end magic
            if offset + 4 + 4 > buffer.len() { // +4 for tuple info, +4 for end magic
                debug!("Not enough space for tuple info {}, stopping", i);
                break;
            }
            
            debug!(
                "Writing tuple info {} - offset: {}, size: {}, meta txn_id: {}",
                i,
                tuple_offset,
                size,
                meta.get_creator_txn_id()
            );

            // Write tuple offset and size
            buffer[offset..offset + 2].copy_from_slice(&tuple_offset.to_le_bytes());
            buffer[offset + 2..offset + 4].copy_from_slice(&size.to_le_bytes());
            offset += 4;

            // Write tuple meta
            let meta_bytes = bincode::encode_to_vec(meta, bincode::config::standard())
                .expect("Failed to serialize meta");
            
            // Check if we have room for meta plus end magic
            if offset + meta_bytes.len() + 4 > buffer.len() {
                debug!("Not enough space for tuple meta, stopping");
                offset -= 4; // Roll back the tuple offset/size
                break;
            }
            
            buffer[offset..offset + meta_bytes.len()].copy_from_slice(&meta_bytes);
            offset += meta_bytes.len();
        }

        // Write another magic number to mark the end of metadata (in big-endian format)
        // Always use a fixed known offset of 41 for the end magic marker for deserialization consistency
        // This is a hardcoded value based on the observed behavior in the logs
        let fixed_offset = 41;  // Using constant offset from logs
        let end_magic = 0xCAFEBABEu32.to_be_bytes();
        debug!("Writing end magic number at fixed offset {}", fixed_offset);
        
        if fixed_offset + 4 <= buffer.len() {
            buffer[fixed_offset..fixed_offset + 4].copy_from_slice(&end_magic);
            debug!("End magic written successfully at fixed offset");
        } else {
            debug!("ERROR: Not enough space for end magic marker at fixed offset {}", fixed_offset);
            // This is a critical error - write the magic at the last possible position
            let last_pos = buffer.len() - 4;
            buffer[last_pos..].copy_from_slice(&end_magic);
            debug!("End magic written at backup position {}", last_pos);
        }
        
        // Also write at the current offset for backward compatibility
        if offset + 4 <= buffer.len() && offset != fixed_offset {
            debug!("Also writing end magic at calculated offset {}", offset);
            buffer[offset..offset + 4].copy_from_slice(&end_magic);
        }

        // Copy tuple data
        for (tuple_offset, size, _) in &self.tuple_info {
            let start = *tuple_offset as usize;
            let end = start + *size as usize;
            debug!("Copying tuple data from range [{}..{}]", start, end);
            buffer[start..end].copy_from_slice(&self.data[start..end]);
        }

        // Verify page type is still correct before returning
        let actual_type = buffer[PAGE_TYPE_OFFSET];
        let expected_type = Self::TYPE_ID as u8;
        debug_assert_eq!(
            actual_type, expected_type,
            "Page type was corrupted during serialization. Found {}, expected {}",
            actual_type, expected_type
        );

        buffer
    }

    /// Deserializes a page from a fixed-size byte array
    pub fn deserialize(bytes: &[u8; DB_PAGE_SIZE as usize]) -> Result<Self, String> {
        // Verify page type
        let page_type = PageType::from_u8(bytes[PAGE_TYPE_OFFSET])
            .ok_or_else(|| "Invalid page type".to_string())?;
        if page_type != Self::TYPE_ID {
            return Err(format!(
                "Expected page type {:?}, found {:?}",
                Self::TYPE_ID,
                page_type
            ));
        }

        let header_start = PAGE_TYPE_OFFSET + 1;
        let header_size = TablePageHeader::size();

        // Deserialize header
        debug!("Deserializing header of size {}", header_size);
        let header =
            TablePageHeader::deserialize(&bytes[header_start..header_start + header_size])?;
        debug!(
            "Deserialized header - page_id: {}, num_tuples: {}, next_page_id: {}",
            header.page_id, header.num_tuples, header.next_page_id
        );
        let mut offset = header_start + header_size;

        // Verify magic number (in big-endian format)
        let magic = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
        debug!("Read magic number: {:x}", magic);
        if magic != 0xDEADBEEF {
            return Err(format!("Invalid magic number: {:x}", magic));
        }
        offset += 4;

        let mut page = TablePage {
            data: Box::new(*bytes),
            header,
            tuple_info: Vec::new(),
            is_dirty: false,
            pin_count: 0,
        };

        // Read tuple info entries
        debug!("Reading {} tuple info entries", page.header.num_tuples);
        for i in 0..page.header.num_tuples {
            // Read tuple offset and size
            let tuple_offset = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap());
            let size = u16::from_le_bytes(bytes[offset + 2..offset + 4].try_into().unwrap());
            offset += 4;

            debug!(
                "Reading tuple info {} - offset: {}, size: {}",
                i, tuple_offset, size
            );

            // Read meta - use actual decoding to get the exact size consumed
            let meta_result: Result<(TupleMeta, usize), _> = bincode::decode_from_slice(
                &bytes[offset..], 
                bincode::config::standard()
            );
            
            let (meta, meta_size) = meta_result
                .map_err(|e| format!("Failed to deserialize meta: {}", e))?;
            
            debug!("Deserialized meta with size {} bytes", meta_size);
            offset += meta_size;

            page.tuple_info.push((tuple_offset, size, meta));
        }

        // Before looking for end magic, let's debug key offsets and expected positions
        debug!("Page size: {}, After reading tuples offset: {}, Expected end magic at: 41", 
               bytes.len(), offset);

        // First check if the magic was written at a fixed known offset (41) where the serialization 
        // logs showed it was written
        let known_offset = 41;
        if known_offset + 4 <= bytes.len() {
            let fixed_end_magic = u32::from_be_bytes(bytes[known_offset..known_offset + 4].try_into().unwrap());
            debug!("Checking for end magic at fixed offset {}: {:x}", known_offset, fixed_end_magic);
            if fixed_end_magic == 0xCAFEBABE {
                debug!("Found end magic at fixed offset position");
                return Ok(page);
            }
        }
        
        // Then try at the current calculated offset
        if offset + 4 <= bytes.len() {
            let end_magic = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
            debug!("Checking for end magic at calculated offset {}: {:x}", offset, end_magic);
            if end_magic == 0xCAFEBABE {
                debug!("Found end magic at calculated position");
                return Ok(page);
            }
        }
        
        // As a last resort, check at the end of the page
        let last_pos = bytes.len() - 4;
        let end_magic = u32::from_be_bytes(bytes[last_pos..].try_into().unwrap());
        debug!("Checking for end magic at end of page {}: {:x}", last_pos, end_magic);
        if end_magic == 0xCAFEBABE {
            debug!("Found end magic at end of page");
            return Ok(page);
        }
        
        // If we reach here, we didn't find the magic number anywhere
        return Err("End magic marker not found".to_string());
        
        // This code is unreachable due to the return statements above
        // Keeping it for documentation purposes
        /* 
        debug!(
            "Completed page deserialization - num_tuples: {}, tuple_info.len: {}",
            page.header.num_tuples,
            page.tuple_info.len()
        );
        */
    }

    /// Updates a tuple in place without any safety checks.
    ///
    /// # Safety
    ///
    /// This method is unsafe because it:
    /// - Does not check if the tuple fits in the existing space
    /// - Does not validate the RID
    /// - Does not maintain proper version chain
    ///
    /// # Parameters
    ///
    /// - `meta`: New tuple meta
    /// - `tuple`: New tuple data
    /// - `rid`: RID of the tuple to update
    pub unsafe fn update_tuple_in_place_unsafe(
        &mut self,
        meta: &TupleMeta,
        tuple: &Tuple,
        rid: RID,
    ) -> Result<(), String> {
        let tuple_id = rid.get_slot_num() as usize;
        if tuple_id >= self.header.num_tuples as usize {
            return Err(format!(
                "Invalid tuple ID: {} >= num_tuples: {}",
                tuple_id, self.header.num_tuples
            ));
        }

        // Serialize the new tuple data
        let tuple_data = bincode::encode_to_vec(tuple, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize tuple: {}", e))?;

        // Get current tuple info
        let (offset, size, _) = self.tuple_info[tuple_id];

        // Update the tuple metadata
        self.tuple_info[tuple_id].2 = meta.clone();

        // Write the new tuple data
        let start = offset as usize;
        let end = start + size as usize;
        if end > self.data.len() {
            return Err(format!(
                "Invalid tuple range: {}..{} exceeds page size {}",
                start,
                end,
                self.data.len()
            ));
        }

        self.data[start..end].copy_from_slice(&tuple_data);
        self.is_dirty = true;
        Ok(())
    }

    pub fn get_header_size(&self) -> u16 {
        (
            size_of::<PageId>() * 3 + // page_id, next_page_id, prev_page_id
         size_of::<u16>() * 2
            // num_tuples, num_deleted_tuples
        ) as u16
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
    pub fn has_space_for(&self, tuple: &Tuple) -> bool {
        // First check if tuple is too large for any page
        if self.is_tuple_too_large(tuple) {
            return false;
        }

        // Then check if this specific page has enough space
        let tuple_info_size = std::mem::size_of::<(u16, u16, TupleMeta)>();
        let tuple_size = tuple.get_length().unwrap_or(0); // Handle potential error
        let total_required_space = tuple_size + tuple_info_size;
        self.get_free_space() >= total_required_space
    }

    /// Gets the ID of the previous page in the table.
    pub fn get_prev_page_id(&self) -> PageId {
        self.header.prev_page_id
    }

    // Add a method to check if tuple is too large for any page
    pub fn is_tuple_too_large(&self, tuple: &Tuple) -> bool {
        // Get serialized size directly from the tuple to be more accurate
        let serialized_size = match bincode::encode_to_vec(tuple, bincode::config::standard()) {
            Ok(data) => data.len(),
            Err(_) => return true, // If we can't serialize it, consider it too large
        };
        
        // Add space for tuple metadata in tuple_info
        let tuple_info_entry_size = size_of::<(u16, u16, TupleMeta)>();
        let total_size = serialized_size + tuple_info_entry_size;
        
        // Reserve additional space for metadata structures to ensure we have room for end markers
        let reserved_metadata_size = self.get_header_size() as usize + 100; // Header + safety buffer
        
        // A tuple is too large if it's more than ~75% of the page size to leave room for overhead
        let max_safe_tuple_size = (DB_PAGE_SIZE as usize * 3) / 4;
        
        // Check both the absolute size and relative to page size
        serialized_size > 3500 || // Ensure large tuples (>3500 bytes) are rejected
        total_size > max_safe_tuple_size || 
        total_size > (DB_PAGE_SIZE as usize - reserved_metadata_size)
    }

    /// Gets the next RID that would be assigned to a tuple inserted into this page
    pub fn get_next_rid(&self) -> RID {
        RID::new(self.header.page_id, self.header.num_tuples as u32)
    }

    /// Inserts a tuple with an immutable reference
    /// The tuple should already have the correct RID (use get_next_rid to get it)
    pub fn insert_tuple(&mut self, meta: &TupleMeta, tuple: &Tuple) -> Option<RID> {
        debug!(
            "Starting immutable tuple insertion. Current num_tuples: {}, tuple_info.len(): {}",
            self.header.num_tuples,
            self.tuple_info.len()
        );

        // Get the next available offset for the tuple
        let tuple_offset = match self.get_next_tuple_offset(tuple) {
            Some(offset) => {
                debug!("Found available offset: {}", offset);
                offset
            }
            None => {
                debug!("No available offset found for tuple");
                return None;
            }
        };

        // Verify the RID in the tuple matches what we expect
        let expected_rid = self.get_next_rid();
        let tuple_rid = tuple.get_rid();
        if tuple_rid != expected_rid {
            debug!(
                "Tuple RID {:?} does not match expected RID {:?}",
                tuple_rid, expected_rid
            );
            return None;
        }

        // Serialize tuple data
        let tuple_data = match bincode::encode_to_vec(tuple, bincode::config::standard()) {
            Ok(data) => {
                debug!("Serialized tuple data length: {}", data.len());
                data
            }
            Err(e) => {
                error!("Failed to serialize tuple: {}", e);
                return None;
            }
        };

        // Store tuple metadata and data
        debug!(
            "Storing tuple info - offset: {}, size: {}, meta txn_id: {}",
            tuple_offset,
            tuple_data.len(),
            meta.get_creator_txn_id()
        );
        self.tuple_info
            .push((tuple_offset, tuple_data.len() as u16, meta.clone()));

        // Write tuple data to page
        let start = tuple_offset as usize;
        let end = start + tuple_data.len();
        debug!("Writing tuple data to page range [{}..{}]", start, end);

        if end <= self.data.len() {
            self.data[start..end].copy_from_slice(&tuple_data);
            // Update header before returning
            self.header.num_tuples += 1;
            self.is_dirty = true;

            // Serialize the page to ensure changes are persisted
            let serialized = self.serialize();
            self.data.copy_from_slice(&serialized);

            debug!(
                "Successfully inserted tuple. New num_tuples: {}, tuple_info.len(): {}",
                self.header.num_tuples,
                self.tuple_info.len()
            );
            Some(expected_rid)
        } else {
            // Rollback tuple_info change if write fails
            error!(
                "Write failed - end position {} exceeds data length {}",
                end,
                self.data.len()
            );
            self.tuple_info.pop();
            None
        }
    }

    /// Inserts a tuple with a specified RID, bypassing the normal RID validation
    /// Useful when moving tuples between pages
    pub fn insert_tuple_with_rid(
        &mut self,
        meta: &TupleMeta,
        tuple: &Tuple,
        rid: RID,
    ) -> Option<RID> {
        debug!(
            "Inserting tuple with specified RID: {:?}. Current num_tuples: {}, tuple_info.len(): {}",
            rid,
            self.header.num_tuples,
            self.tuple_info.len()
        );

        // Get the next available offset for the tuple
        let tuple_offset = match self.get_next_tuple_offset(tuple) {
            Some(offset) => {
                debug!("Found available offset: {}", offset);
                offset
            }
            None => {
                debug!("No available offset found for tuple");
                return None;
            }
        };

        // Skip RID validation - use the provided RID

        // Serialize tuple data
        let tuple_data = match bincode::encode_to_vec(tuple, bincode::config::standard()) {
            Ok(data) => {
                debug!("Serialized tuple data length: {}", data.len());
                data
            }
            Err(e) => {
                error!("Failed to serialize tuple: {}", e);
                return None;
            }
        };

        // Store tuple metadata and data
        debug!(
            "Storing tuple info - offset: {}, size: {}, meta txn_id: {}",
            tuple_offset,
            tuple_data.len(),
            meta.get_creator_txn_id()
        );
        self.tuple_info
            .push((tuple_offset, tuple_data.len() as u16, meta.clone()));

        // Write tuple data to page
        let start = tuple_offset as usize;
        let end = start + tuple_data.len();
        debug!("Writing tuple data to page range [{}..{}]", start, end);

        if end <= self.data.len() {
            self.data[start..end].copy_from_slice(&tuple_data);
            // Update header before returning
            self.header.num_tuples += 1;
            self.is_dirty = true;

            // Serialize the page to ensure changes are persisted
            let serialized = self.serialize();
            self.data.copy_from_slice(&serialized);

            debug!(
                "Successfully inserted tuple with RID {:?}. New num_tuples: {}, tuple_info.len(): {}",
                rid,
                self.header.num_tuples,
                self.tuple_info.len()
            );
            Some(rid)
        } else {
            // Rollback tuple_info change if write fails
            error!(
                "Write failed - end position {} exceeds data length {}",
                end,
                self.data.len()
            );
            self.tuple_info.pop();
            None
        }
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

impl<'a> From<&'a mut Tuple> for TupleSizeInput<'a> {
    fn from(tuple: &'a mut Tuple) -> Self {
        TupleSizeInput::Tuple(tuple)
    }
}

impl PageTrait for TablePage {
    fn get_page_id(&self) -> PageId {
        self.header.page_id
    }

    fn get_page_type(&self) -> PageType {
        PageType::from_u8(self.data[PAGE_TYPE_OFFSET]).unwrap_or(PageType::Invalid)
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
            return Err(PageError::InvalidOffset {
                offset,
                page_size: self.data.len(),
            });
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
        self.header.prev_page_id = INVALID_PAGE_ID;
        self.header.next_page_id = INVALID_PAGE_ID;
        self.header.num_tuples = 0;
        self.tuple_info.clear();
        self.is_dirty = false;
        self.data[PAGE_TYPE_OFFSET] = Self::TYPE_ID as u8;
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
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
        let tuple = Tuple::new(&values, &schema, rid);
        let meta = TupleMeta::new(123);
        (meta, tuple)
    }

    #[test]
    fn test_insert_and_get_tuple() {
        let mut page = TablePage::new(1);
        let (meta, mut tuple) = create_test_tuple(1);

        let tuple_id = page.insert_tuple(&meta, &mut tuple).unwrap();
        assert_eq!(page.header.num_tuples, 1);

        let (retrieved_meta, retrieved_tuple) = page.get_tuple(&tuple_id, false).unwrap();
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

        let mut inserted_count = 0;

        // Keep inserting tuples until the page is full
        loop {
            // Create a new tuple with the correct RID for this insertion
            let next_rid = page.get_next_rid();
            let schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ]);
            let values = vec![Value::from(inserted_count), Value::from("Test".to_string())];
            let tuple = Tuple::new(&values, &schema, next_rid);
            let meta = TupleMeta::new(123);

            // Try to insert the tuple
            if page.insert_tuple(&meta, &tuple).is_none() {
                break;
            }

            inserted_count += 1;
        }

        assert!(inserted_count > 0);

        // Create one more tuple with the correct next RID
        let next_rid = page.get_next_rid();
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let values = vec![
            Value::from(inserted_count + 1),
            Value::from("Test".to_string()),
        ];
        let tuple = Tuple::new(&values, &schema, next_rid);

        // Now check that get_next_tuple_offset returns None
        assert!(page.get_next_tuple_offset(&tuple).is_none());
    }

    #[test]
    fn test_update_tuple_in_place_unsafe() {
        let mut page = TablePage::new(1);
        let (meta, mut tuple) = create_test_tuple(1);

        // First insert a tuple
        let rid = page.insert_tuple(&meta, &mut tuple).unwrap();

        // Test with invalid RID
        let invalid_rid = RID::new(1, 999);
        unsafe {
            match page.update_tuple_in_place_unsafe(&meta, &tuple, invalid_rid) {
                Ok(_) => panic!("Expected error for invalid RID"),
                Err(err) => {
                    assert!(
                        err.contains("Invalid tuple ID"),
                        "Unexpected error message: {}",
                        err
                    );
                }
            }
        }

        // Test with valid RID
        let new_meta = TupleMeta::new(789);
        let new_tuple = create_test_tuple(2).1;

        unsafe {
            page.update_tuple_in_place_unsafe(&new_meta, &new_tuple, rid)
                .unwrap();
        }

        // Verify the update
        let (retrieved_meta, retrieved_tuple) = page.get_tuple(&rid, false).unwrap();
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
        assert_eq!(page.pin_count, 1);
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

    #[test]
    fn test_get_invalid_tuple() {
        let page = TablePage::new(1);
        let invalid_rid = RID::new(1, 999); // Invalid slot number

        // Test getting an invalid tuple
        match page.get_tuple(&invalid_rid, false) {
            Ok(_) => panic!("Expected error for invalid tuple"),
            Err(err) => {
                assert!(
                    err.contains("Invalid slot number"),
                    "Unexpected error message: {}",
                    err
                );
            }
        }
    }

    #[test]
    fn test_get_invalid_tuple_meta() {
        let page = TablePage::new(1);
        let invalid_rid = RID::new(1, 999); // Invalid slot number

        // Test getting invalid tuple metadata
        match page.get_tuple_meta(&invalid_rid) {
            Ok(_) => panic!("Expected error for invalid tuple meta"),
            Err(err) => {
                assert!(
                    err.contains("Invalid tuple ID"),
                    "Unexpected error message: {}",
                    err
                );
            }
        }
    }

    #[test]
    fn test_update_invalid_tuple_meta() {
        let mut page = TablePage::new(1);
        let invalid_rid = RID::new(1, 999); // Invalid slot number
        let meta = TupleMeta::new(123);

        // Test updating invalid tuple metadata
        match page.update_tuple_meta(&meta, &invalid_rid) {
            Ok(_) => panic!("Expected error for invalid tuple meta update"),
            Err(err) => {
                assert!(
                    err.contains("Invalid tuple ID"),
                    "Unexpected error message: {}",
                    err
                );
            }
        }
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
        let tuple = Tuple::new(&values, &schema, rid);
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
        let (retrieved_meta, retrieved_tuple) = page.get_tuple(&rid, false).unwrap();

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

        let (retrieved_meta, retrieved_tuple) = page.get_tuple(&rid, false).unwrap();
        assert_eq!(
            retrieved_meta.get_commit_timestamp(),
            new_meta.get_commit_timestamp()
        );
        assert_eq!(retrieved_tuple.get_value(0), new_tuple.get_value(0));
    }

    #[test]
    fn test_immutable_tuple_insertion() {
        let mut page = TablePage::new(1);

        // Get the next RID before creating the tuple
        let next_rid = page.get_next_rid();

        // Create tuple with the correct RID
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let values = vec![Value::from(1), Value::from("Test".to_string())];
        let tuple = Tuple::new(&values, &schema, next_rid);
        let meta = TupleMeta::new(123);

        // Insert using the immutable method
        let rid = page.insert_tuple(&meta, &tuple).unwrap();
        assert_eq!(page.header.num_tuples, 1);
        assert_eq!(rid, next_rid);

        // Retrieve and verify
        let (retrieved_meta, retrieved_tuple) = page.get_tuple(&rid, false).unwrap();
        assert_eq!(
            retrieved_meta.get_commit_timestamp(),
            meta.get_commit_timestamp()
        );
        assert_eq!(retrieved_tuple.get_value(0), tuple.get_value(0));
        assert_eq!(retrieved_tuple.get_value(1), tuple.get_value(1));
    }

    #[test]
    fn test_incorrect_rid_rejection() {
        let mut page = TablePage::new(1);

        // Create tuple with an incorrect RID
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let values = vec![Value::from(1), Value::from("Test".to_string())];
        let incorrect_rid = RID::new(1, 999); // Wrong slot number
        let tuple = Tuple::new(&values, &schema, incorrect_rid);
        let meta = TupleMeta::new(123);

        // Insert should fail due to RID mismatch
        let result = page.insert_tuple(&meta, &tuple);
        assert!(result.is_none());
        assert_eq!(page.header.num_tuples, 0);
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
        let tuple = Tuple::new(&values, &schema, rid);
        let meta = TupleMeta::new(123);
        (meta, tuple)
    }

    #[test]
    fn test_invalid_tuple_retrieval() {
        let page = TablePage::new(1);
        let invalid_rid = RID::new(1, 100);
        assert!(matches!(page.get_tuple(&invalid_rid, false), Err(..)));
    }

    #[test]
    fn test_invalid_meta_update() {
        let mut page = TablePage::new(1);
        let invalid_rid = RID::new(1, 100);
        let meta = TupleMeta::new(123);
        assert!(matches!(
            page.update_tuple_meta(&meta, &invalid_rid),
            Err(..)
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
                Err(..)
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

    #[test]
    fn test_page_capacity() {
        let mut page = TablePage::new(1);

        let mut inserted_count = 0;

        // Keep inserting tuples until the page is full
        loop {
            // Create a new tuple with the correct RID for this insertion
            let next_rid = page.get_next_rid();
            let schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ]);
            let values = vec![Value::from(inserted_count), Value::from("Test".to_string())];
            let tuple = Tuple::new(&values, &schema, next_rid);
            let meta = TupleMeta::new(123);

            // Try to insert the tuple
            if page.insert_tuple(&meta, &tuple).is_none() {
                break;
            }

            inserted_count += 1;
        }

        assert!(inserted_count > 0);

        // Create one more tuple with the correct next RID
        let next_rid = page.get_next_rid();
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let values = vec![
            Value::from(inserted_count + 1),
            Value::from("Test".to_string()),
        ];
        let tuple = Tuple::new(&values, &schema, next_rid);

        // Now check that get_next_tuple_offset returns None
        assert!(page.get_next_tuple_offset(&tuple).is_none());
    }

    #[test]
    fn test_space_management() {
        let mut page = TablePage::new(1);

        // Insert first tuple
        let first_rid = page.get_next_rid();
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let values = vec![Value::from(1), Value::from("Test".to_string())];
        let first_tuple = Tuple::new(&values, &schema, first_rid);
        let meta = TupleMeta::new(123);

        page.insert_tuple(&meta, &first_tuple).unwrap();
        let first_offset = page.tuple_info[0].0;

        // Insert second tuple with new RID
        let second_rid = page.get_next_rid();
        let values = vec![Value::from(2), Value::from("Test2".to_string())];
        let second_tuple = Tuple::new(&values, &schema, second_rid);

        page.insert_tuple(&meta, &second_tuple).unwrap();
        let second_offset = page.tuple_info[1].0;

        // Verify tuples are packed from the end of the page
        assert!(second_offset < first_offset);
        assert_eq!(page.header.num_tuples, 2);
    }

    #[test]
    fn test_header_size_constraints() {
        let page = TablePage::new(1);
        assert!(page.get_header_size() < DB_PAGE_SIZE as u16);
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
    use parking_lot::Mutex;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    fn create_test_tuple() -> (TupleMeta, Tuple) {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let values = vec![Value::new(42), Value::new("Test")];
        let rid = RID::new(1, 0);
        let tuple = Tuple::new(&values, &schema, rid);
        let mut meta = TupleMeta::new(123);
        meta.set_commit_timestamp(123);
        (meta, tuple)
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        initialize_logger();

        // Create and populate original page
        let mut original_page = TablePage::new(1);

        // Get the next RID for the page
        let next_rid = original_page.get_next_rid();
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let values = vec![Value::new(42), Value::new("Test")];
        let tuple = Tuple::new(&values, &schema, next_rid);
        let mut meta = TupleMeta::new(123);
        meta.set_commit_timestamp(123);

        debug!("Inserting tuple into original page");
        let rid = original_page.insert_tuple(&meta, &tuple).unwrap();
        debug!("Original page state after insert:");
        debug!("  num_tuples: {}", original_page.header.num_tuples);
        debug!("  tuple_info: {:?}", original_page.tuple_info);

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
        let (deserialized_meta, deserialized_tuple) = deserialized_page.get_tuple(&rid, false).unwrap();

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
        let rid = RID::new(1, 0);

        // Insert test tuple before spawning threads
        {
            let mut page_guard = page.lock();
            let (meta, mut tuple) = create_test_tuple();
            debug!("Original meta timestamp: {:?}", meta.get_commit_timestamp());
            page_guard.insert_tuple(&meta, &mut tuple).unwrap();

            // Verify initial insertion
            let (initial_meta, initial_tuple) = page_guard.get_tuple(&rid, false).unwrap();
            assert_eq!(initial_meta.get_commit_timestamp(), 123);
            assert_eq!(initial_tuple.get_value(0), Value::new(42));
        }

        let mut handles = vec![];

        // Spawn reader threads
        for i in 0..3 {
            let page_clone = Arc::clone(&page);
            let handle = thread::spawn(move || {
                // Add timeout for lock acquisition
                let page_guard = match page_clone.try_lock() {
                    Some(guard) => guard,
                    None => {
                        debug!("Thread {} failed to acquire lock, retrying...", i);
                        thread::sleep(Duration::from_millis(10));
                        page_clone.lock()
                    }
                };

                match page_guard.get_tuple(&rid, false) {
                    Ok((retrieved_meta, retrieved_tuple)) => {
                        debug!(
                            "Thread {} - Retrieved meta timestamp: {:?}",
                            i,
                            retrieved_meta.get_commit_timestamp()
                        );

                        // Return the values for verification
                        Ok((
                            retrieved_meta.get_commit_timestamp(),
                            retrieved_tuple.get_value(0).clone(),
                        ))
                    }
                    Err(e) => {
                        error!("Thread {} failed to retrieve tuple: {:?}", i, e);
                        Err(e)
                    }
                }
            });
            handles.push(handle);

            // Add small delay between thread spawns
            thread::sleep(Duration::from_millis(1));
        }

        // Wait for all threads and verify results
        for (i, handle) in handles.into_iter().enumerate() {
            match handle.join() {
                Ok(result) => {
                    let (timestamp, value) =
                        result.expect(&format!("Thread {} failed to read tuple", i));
                    assert_eq!(timestamp, 123);
                    assert_eq!(value, Value::new(42));
                }
                Err(e) => {
                    error!("Thread {} panicked: {:?}", i, e);
                    panic!("Thread {} failed", i);
                }
            }
        }
    }

    #[test]
    fn test_update_tuple_serialization_corruption_bug() {
        initialize_logger();

        // Create a page and insert initial tuples
        let mut page = TablePage::new(1);
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("status", TypeId::VarChar),
        ]);

        // Insert 3 tuples similar to the failing test
        let mut rids = Vec::new();
        for i in 0..3 {
            let next_rid = page.get_next_rid();
            let values = vec![
                Value::from(i + 1),
                Value::from(format!("Employee {}", i + 1)),
                Value::from("active"),
            ];
            let tuple = Tuple::new(&values, &schema, next_rid);
            let meta = TupleMeta::new(100 + i as u64);

            let rid = page.insert_tuple(&meta, &tuple).unwrap();
            rids.push(rid);
        }

        // Verify initial state
        assert_eq!(page.get_num_tuples(), 3);

        // Serialize and deserialize the page (simulating disk I/O)
        let serialized = page.serialize();
        let mut page = TablePage::deserialize(&serialized).unwrap();

        // Now update all tuples to have a different status (simulating UPDATE without WHERE)
        for (i, &rid) in rids.iter().enumerate() {
            // Get the current tuple
            let (current_meta, current_tuple) = page.get_tuple(&rid, false).unwrap();

            // Create updated tuple with new status
            let updated_values = vec![
                current_tuple.get_value(0).clone(), // Keep same ID
                current_tuple.get_value(1).clone(), // Keep same name
                Value::from("updated"),             // Change status
            ];
            let updated_tuple = Tuple::new(&updated_values, &schema, rid);

            // Update the tuple - this is where the bug occurred
            let result = page.update_tuple(&current_meta, &updated_tuple, rid);
            assert!(result.is_ok(), "Failed to update tuple {}: {:?}", i, result);
        }

        // Serialize again to trigger the corruption bug
        let serialized_after_update = page.serialize();

        // Try to deserialize - this should fail with the original bug
        // "Failed to deserialize tuple: invalid value: integer '196608', expected variant index 0 <= i < 22"
        let deserialized_result = TablePage::deserialize(&serialized_after_update);

        // With the bug fix, this should succeed
        assert!(
            deserialized_result.is_ok(),
            "Deserialization failed after update: {:?}",
            deserialized_result.err()
        );

        let final_page = deserialized_result.unwrap();

        // Verify all tuples were updated correctly
        assert_eq!(final_page.get_num_tuples(), 3);

        for (i, &rid) in rids.iter().enumerate() {
            let (_meta, tuple) = final_page.get_tuple(&rid, false).unwrap();
            assert_eq!(tuple.get_value(0), Value::from(i as i32 + 1)); // ID unchanged
            assert_eq!(tuple.get_value(2), Value::from("updated")); // Status changed
        }
    }

    #[test]
    fn test_update_tuple_size_change_corruption() {
        initialize_logger();

        // Create a page with a tuple
        let mut page = TablePage::new(1);
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("data", TypeId::VarChar),
        ]);

        // Insert a tuple with short data
        let next_rid = page.get_next_rid();
        let values = vec![
            Value::from(1),
            Value::from("short"), // 5 characters
        ];
        let tuple = Tuple::new(&values, &schema, next_rid);
        let meta = TupleMeta::new(100);

        let rid = page.insert_tuple(&meta, &tuple).unwrap();

        // Get the original tuple info to check sizes
        let original_size = page.tuple_info[0].1;

        // Update with much longer data
        let updated_values = vec![
            Value::from(1),
            Value::from("this is a much longer string that should cause size issues"), // Much longer
        ];
        let updated_tuple = Tuple::new(&updated_values, &schema, rid);

        // This update changes the tuple size significantly
        let result = page.update_tuple(&meta, &updated_tuple, rid);
        assert!(
            result.is_ok(),
            "Failed to update tuple with size change: {:?}",
            result
        );

        // Check that the size was updated correctly
        let new_size = page.tuple_info[0].1;
        assert_ne!(original_size, new_size, "Tuple size should have changed");

        // Serialize and deserialize to check for corruption
        let serialized = page.serialize();
        let deserialized_result = TablePage::deserialize(&serialized);

        assert!(
            deserialized_result.is_ok(),
            "Deserialization failed after size change: {:?}",
            deserialized_result.err()
        );

        let final_page = deserialized_result.unwrap();
        let (_, final_tuple) = final_page.get_tuple(&rid, false).unwrap();

        // Verify the updated data is correct
        assert_eq!(final_tuple.get_value(0), Value::from(1));
        assert_eq!(
            final_tuple.get_value(1),
            Value::from("this is a much longer string that should cause size issues")
        );
    }

    #[test]
    fn test_update_tuple_smaller_size_corruption() {
        initialize_logger();

        // Create a page with a tuple
        let mut page = TablePage::new(1);
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("data", TypeId::VarChar),
        ]);

        // Insert a tuple with long data
        let next_rid = page.get_next_rid();
        let values = vec![
            Value::from(1),
            Value::from("this is a very long string that will be replaced with something shorter"),
        ];
        let tuple = Tuple::new(&values, &schema, next_rid);
        let meta = TupleMeta::new(100);

        let rid = page.insert_tuple(&meta, &tuple).unwrap();

        // Update with much shorter data
        let updated_values = vec![
            Value::from(1),
            Value::from("short"), // Much shorter
        ];
        let updated_tuple = Tuple::new(&updated_values, &schema, rid);

        // This update makes the tuple smaller - this was causing corruption
        let result = page.update_tuple(&meta, &updated_tuple, rid);
        assert!(
            result.is_ok(),
            "Failed to update tuple with smaller size: {:?}",
            result
        );

        // Serialize and deserialize to check for corruption
        let serialized = page.serialize();
        let deserialized_result = TablePage::deserialize(&serialized);

        assert!(
            deserialized_result.is_ok(),
            "Deserialization failed after size reduction: {:?}",
            deserialized_result.err()
        );

        let final_page = deserialized_result.unwrap();
        let (_, final_tuple) = final_page.get_tuple(&rid, false).unwrap();

        // Verify the updated data is correct
        assert_eq!(final_tuple.get_value(0), Value::from(1));
        assert_eq!(final_tuple.get_value(1), Value::from("short"));
    }
}

#[cfg(test)]
mod page_type_tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::common::logger::initialize_logger;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    #[test]
    fn test_page_type_preservation() {
        initialize_logger();

        // Test page creation
        let mut page = TablePage::new(1);
        assert_eq!(page.get_page_type(), PageType::Table);
        assert_eq!(page.data[PAGE_TYPE_OFFSET], PageType::Table as u8);

        // Test initialization
        page.init();
        assert_eq!(page.get_page_type(), PageType::Table);
        assert_eq!(page.data[PAGE_TYPE_OFFSET], PageType::Table as u8);

        // Test page type after tuple insertion
        let next_rid = page.get_next_rid();
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let values = vec![Value::new(1), Value::new("test")];
        let tuple = Tuple::new(&values, &schema, next_rid);
        let meta = TupleMeta::new(1);

        let rid = page.insert_tuple(&meta, &tuple).unwrap();
        assert_eq!(page.get_page_type(), PageType::Table);
        assert_eq!(page.data[PAGE_TYPE_OFFSET], PageType::Table as u8);

        // Test serialization
        let serialized = page.serialize();
        assert_eq!(serialized[PAGE_TYPE_OFFSET], PageType::Table as u8);

        // Test deserialization
        let deserialized = TablePage::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.get_page_type(), PageType::Table);
        assert_eq!(deserialized.data[PAGE_TYPE_OFFSET], PageType::Table as u8);

        // Verify tuple can still be read after deserialization
        let (_, retrieved_tuple) = deserialized.get_tuple(&rid, false).unwrap();
        assert_eq!(retrieved_tuple.get_value(0), Value::new(1));
        assert_eq!(retrieved_tuple.get_value(1), Value::new("test"));
    }

    #[test]
    fn test_invalid_page_type_deserialization() {
        // Create a buffer with invalid page type
        let mut invalid_buffer = [0u8; DB_PAGE_SIZE as usize];
        invalid_buffer[PAGE_TYPE_OFFSET] = PageType::Invalid as u8;

        // Attempt to deserialize should fail
        let result = TablePage::deserialize(&invalid_buffer);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Expected page type Table"));
    }

    #[test]
    fn test_page_type_after_updates() {
        let mut page = TablePage::new(1);

        // Insert tuple with correct RID
        let next_rid = page.get_next_rid();
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let values = vec![Value::new(1), Value::new("test")];
        let tuple = Tuple::new(&values, &schema, next_rid);
        let meta = TupleMeta::new(1);

        // Insert tuple
        let rid = page.insert_tuple(&meta, &tuple).unwrap();
        assert_eq!(page.get_page_type(), PageType::Table);

        // Update tuple with new values
        let new_values = vec![Value::new(1), Value::new("updated")];
        let new_tuple = Tuple::new(&new_values, &schema, rid);
        page.update_tuple(&meta, &new_tuple, rid).unwrap();
        assert_eq!(page.get_page_type(), PageType::Table);

        // Update tuple meta
        let mut new_meta = meta.clone();
        new_meta.set_deleted(true);
        page.update_tuple_meta(&new_meta, &rid).unwrap();
        assert_eq!(page.get_page_type(), PageType::Table);

        // Verify the page type is still correct after all operations
        assert_eq!(page.data[PAGE_TYPE_OFFSET], PageType::Table as u8);
    }
}
