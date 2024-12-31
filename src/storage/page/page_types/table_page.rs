use crate::common::config::{PageId, DB_PAGE_SIZE, INVALID_PAGE_ID};
use crate::common::exception::PageError;
use crate::common::rid::RID;
use crate::storage::page::page::PageTrait;
use crate::storage::table::tuple::{Tuple, TupleMeta};

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
    page_id: PageId,
    pin_count: i32,
    is_dirty: bool,
    next_page_id: PageId,
    prev_page_id: PageId,
    num_tuples: u16,
    num_deleted_tuples: u16,
    tuple_info: Vec<(u16, u16, TupleMeta)>,
}

impl TablePage {
    /// Creates a new `TablePage` with the given page ID.
    pub fn new(page_id: PageId) -> Self {
        Self {
            data: Box::new([0; DB_PAGE_SIZE as usize]),
            page_id,
            pin_count: 0,
            is_dirty: false,
            next_page_id: INVALID_PAGE_ID,
            prev_page_id: INVALID_PAGE_ID,
            num_tuples: 0,
            num_deleted_tuples: 0,
            tuple_info: Vec::new(),
        }
    }

    /// Initializes the table page.
    pub fn init(&mut self) {
        self.next_page_id = INVALID_PAGE_ID;
        self.prev_page_id = INVALID_PAGE_ID;
        self.num_tuples = 0;
        self.num_deleted_tuples = 0;
        self.tuple_info.clear();
    }

    /// Gets the next offset to insert a tuple.
    pub fn get_next_tuple_offset(&self, tuple: &Tuple) -> Option<u16> {
        let slot_end_offset = if self.num_tuples > 0 {
            self.tuple_info[self.num_tuples as usize - 1].0
        } else {
            DB_PAGE_SIZE as u16
        };

        let tuple_offset = slot_end_offset.saturating_sub(tuple.get_length().unwrap() as u16);
        let offset_size = TablePage::header_size() + self.tuple_info_size();

        if tuple_offset < offset_size {
            None
        } else {
            Some(tuple_offset)
        }
    }

    /// Inserts a tuple into the table.
    pub fn insert_tuple(&mut self, meta: &TupleMeta, tuple: &mut Tuple) -> Option<RID> {
        if let Some(tuple_offset) = self.get_next_tuple_offset(tuple) {
            // Create new RID with the current page's ID
            let rid = RID::new(self.page_id, self.get_num_tuples() as u32);

            // Update the tuple's RID
            tuple.set_rid(rid);

            // Store tuple metadata and data
            self.tuple_info.push((
                tuple_offset,
                tuple.get_length().unwrap() as u16,
                meta.clone(),
            ));

            let start = tuple_offset as usize;
            let end = start + tuple.get_length().unwrap();
            let tuple_data = bincode::serialize(tuple).unwrap();
            self.data[start..end].copy_from_slice(&tuple_data);

            self.num_tuples += 1;
            self.is_dirty = true;

            Some(rid)
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
        if tuple_id >= self.num_tuples as usize {
            return Err(PageError::TupleInvalid);
        }

        // Get current tuple info
        let (current_offset, current_size, _) = self.tuple_info[tuple_id];
        let new_size = tuple.get_length().unwrap() as u16;

        // If new tuple has same size, we can update in place
        if current_size == new_size {
            // Update the tuple metadata
            self.tuple_info[tuple_id].2 = meta.clone();

            // Update the tuple data
            let tuple_data = bincode::serialize(tuple).unwrap();
            let start = current_offset as usize;
            let end = start + new_size as usize;
            self.data[start..end].copy_from_slice(&tuple_data);

            self.is_dirty = true;
            Ok(())
        } else {
            // If sizes differ, we need to check if we have space for the new tuple
            if let Some(new_offset) = self.get_next_tuple_offset(tuple) {
                // Update tuple info
                self.tuple_info[tuple_id] = (new_offset, new_size, meta.clone());

                // Write new tuple data
                let tuple_data = bincode::serialize(tuple).unwrap();
                let start = new_offset as usize;
                let end = start + new_size as usize;
                self.data[start..end].copy_from_slice(&tuple_data);

                self.is_dirty = true;
                Ok(())
            } else {
                Err(PageError::TupleInvalid)
            }
        }
    }

    /// Updates the metadata of a tuple.
    pub fn update_tuple_meta(&mut self, meta: &TupleMeta, rid: &RID) -> Result<(), PageError> {
        let tuple_id = rid.get_slot_num() as usize;
        if tuple_id >= self.num_tuples as usize {
            return Err(PageError::TupleInvalid);
        }

        let old_meta = &mut self.tuple_info[tuple_id].2;
        if !old_meta.is_deleted() && meta.is_deleted() {
            self.num_deleted_tuples += 1;
        }
        *old_meta = meta.clone();

        Ok(())
    }

    /// Gets a tuple from the table.
    pub fn get_tuple(&self, rid: &RID) -> Result<(TupleMeta, Tuple), PageError> {
        let tuple_id = rid.get_slot_num() as usize;
        if tuple_id > self.num_tuples as usize {
            return Err(PageError::TupleInvalid);
        }

        let (offset, size, meta) = &self.tuple_info[tuple_id];
        let mut tuple_data = vec![0; *size as usize];
        tuple_data.copy_from_slice(&self.data[*offset as usize..(*offset + *size) as usize]);
        let tuple = bincode::deserialize(&tuple_data).unwrap();

        Ok((meta.clone(), tuple))
    }

    /// Gets the metadata of a tuple.
    pub fn get_tuple_meta(&self, rid: &RID) -> Result<TupleMeta, PageError> {
        let tuple_id = rid.get_slot_num() as usize;
        if tuple_id >= self.num_tuples as usize {
            return Err(PageError::TupleInvalid);
        }

        Ok(self.tuple_info[tuple_id].2.clone())
    }

    pub fn get_num_tuples(&self) -> u16 {
        self.num_tuples
    }

    pub fn get_num_deleted_tuples(&self) -> u16 {
        self.num_deleted_tuples
    }

    pub fn get_next_page_id(&self) -> PageId {
        self.next_page_id
    }

    pub fn set_next_page_id(&mut self, page_id: PageId) {
        self.next_page_id = page_id;
    }

    pub fn set_prev_page_id(&mut self, page_id: PageId) {
        self.prev_page_id = page_id;
    }

    /// Serializes the full table page state into a byte vector.
    pub fn serialize(&self) -> [u8; DB_PAGE_SIZE as usize] {
        // Initialize buffer with DB_PAGE_SIZE bytes
        let mut buffer = [0; DB_PAGE_SIZE as usize];

        // Header (20 bytes total):
        // page_id (u64 - 8 bytes
        // next_page_id (u64) - 8 bytes
        // prev_page_id (u64) - 8 bytes
        // num_tuples (u16) - 2 bytes
        // num_deleted_tuples (u16) - 2 bytes
        buffer[0..8].copy_from_slice(&self.page_id.to_le_bytes());
        buffer[8..16].copy_from_slice(&self.next_page_id.to_le_bytes());
        buffer[16..24].copy_from_slice(&self.prev_page_id.to_le_bytes());

        buffer[24..26].copy_from_slice(&self.num_tuples.to_le_bytes());
        buffer[26..28].copy_from_slice(&self.num_deleted_tuples.to_le_bytes());
        let mut cursor = 28;

        // Write tuple info entries (13 bytes each)
        for &(offset, size, ref meta) in &self.tuple_info {
            if cursor + 13 > DB_PAGE_SIZE as usize {
                break;
            }
            buffer[cursor..cursor + 2].copy_from_slice(&offset.to_le_bytes());
            buffer[cursor + 2..cursor + 4].copy_from_slice(&size.to_le_bytes());
            buffer[cursor + 4..cursor + 12].copy_from_slice(&meta.get_timestamp().to_le_bytes());
            buffer[cursor + 12] = meta.is_deleted() as u8;
            cursor += 13;
        }

        // Copy page data
        let remaining_space = DB_PAGE_SIZE as usize - cursor;
        if remaining_space > 0 {
            buffer[cursor..].copy_from_slice(&self.data[..remaining_space]);
        }

        buffer
    }

    /// Deserializes a byte vector into a TablePage.
    pub fn deserialize(buffer: &[u8; DB_PAGE_SIZE as usize]) -> Result<Self, Box<dyn std::error::Error>> {
        if buffer.len() != DB_PAGE_SIZE as usize {
            return Err(format!(
                "Invalid buffer size. Expected {} bytes but got {}",
                DB_PAGE_SIZE,
                buffer.len()
            )
            .into());
        }

        // Read header information (20 bytes)
        let page_id = u64::from_le_bytes(buffer[0..8].try_into()?);
        let next_page_id = u64::from_le_bytes(buffer[8..16].try_into()?);
        let prev_page_id = u64::from_le_bytes(buffer[16..24].try_into()?);
        let num_tuples = u16::from_le_bytes(buffer[24..26].try_into()?);
        let num_deleted_tuples = u16::from_le_bytes(buffer[26..28].try_into()?);
        let page_type = u16::from_le_bytes(buffer[28..30].try_into()?);

        let mut cursor = 30;

        // Read tuple info
        let mut tuple_info = Vec::with_capacity(num_tuples as usize);
        for _ in 0..num_tuples {
            if cursor + 13 > buffer.len() {
                break;
            }

            let offset = u16::from_le_bytes(buffer[cursor..cursor + 2].try_into()?);
            let size = u16::from_le_bytes(buffer[cursor + 2..cursor + 4].try_into()?);
            let timestamp = u64::from_le_bytes(buffer[cursor + 4..cursor + 12].try_into()?);
            let is_deleted = buffer[cursor + 12] != 0;
            cursor += 13;

            tuple_info.push((offset, size, TupleMeta::new(timestamp, is_deleted)));
        }

        // Read page data
        let mut data = Box::new([0; DB_PAGE_SIZE as usize]);
        data.copy_from_slice(buffer);

        Ok(Self {
            data,
            page_id,
            pin_count: 0,
            is_dirty: false,
            next_page_id,
            prev_page_id,
            num_tuples,
            num_deleted_tuples,
            tuple_info,
        })
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
        if tuple_id >= self.num_tuples as usize {
            return Err(PageError::TupleInvalid);
        }

        let (offset, size, old_meta) = &mut self.tuple_info[tuple_id];
        if *size as usize != tuple.get_length().unwrap() {
            return Err(PageError::TupleInvalid);
        }

        if !old_meta.is_deleted() && meta.is_deleted() {
            self.num_deleted_tuples += 1;
        }

        *old_meta = meta.clone();
        let tuple = bincode::serialize(&tuple).unwrap();
        self.data[*offset as usize..(*offset + *size) as usize].copy_from_slice(&*tuple);

        Ok(())
    }

    fn header_size() -> u16 {
        (size_of::<PageId>() + size_of::<u16>() + size_of::<u16>()) as u16
    }

    fn tuple_info_size(&self) -> u16 {
        (self.num_tuples as usize * size_of::<(u16, u16, TupleMeta)>()) as u16
    }
}

impl PageTrait for TablePage {
    fn get_page_id(&self) -> PageId {
        self.page_id
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
    use crate::catalogue::column::Column;
    use crate::catalogue::schema::Schema;
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
        let meta = TupleMeta::new(123, false);
        (meta, tuple)
    }

    #[test]
    fn test_insert_and_get_tuple() {
        let mut page = TablePage::new(1);
        let (meta, mut tuple) = create_test_tuple(1);

        let tuple_id = page.insert_tuple(&meta, &mut tuple).unwrap();
        assert_eq!(page.num_tuples, 1);

        let (retrieved_meta, retrieved_tuple) = page.get_tuple(&tuple_id).unwrap();
        assert_eq!(retrieved_meta.get_timestamp(), meta.get_timestamp());
        assert_eq!(retrieved_meta.is_deleted(), meta.is_deleted());
        assert_eq!(retrieved_tuple.get_value(0), tuple.get_value(0));
    }

    #[test]
    fn test_update_tuple_meta() {
        let mut page = TablePage::new(1);
        let (meta, mut tuple) = create_test_tuple(1);

        let tuple_rid_id = page.insert_tuple(&meta, &mut tuple).unwrap();
        let new_meta = TupleMeta::new(456, true);

        page.update_tuple_meta(&new_meta, &tuple_rid_id).unwrap();
        let retrieved_meta = page.get_tuple_meta(&tuple_rid_id).unwrap();

        assert_eq!(retrieved_meta.get_timestamp(), new_meta.get_timestamp());
        assert_eq!(retrieved_meta.is_deleted(), new_meta.is_deleted());
        assert_eq!(page.num_deleted_tuples, 1);
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

        let new_meta = TupleMeta::new(789, false);
        let new_tuple = create_test_tuple(2).1;

        unsafe {
            page.update_tuple_in_place_unsafe(&new_meta, &new_tuple, tuple_id)
                .unwrap();
        }

        let (retrieved_meta, retrieved_tuple) = page.get_tuple(&tuple_id).unwrap();
        assert_eq!(retrieved_meta.get_timestamp(), new_meta.get_timestamp());
        assert_eq!(retrieved_tuple.get_value(0), new_tuple.get_value(0));
    }
    #[test]
    fn test_table_page_creation() {
        let page = TablePage::new(1);
        assert_eq!(page.get_page_id(), 1);
        assert_eq!(page.get_num_tuples(), 0);
        assert_eq!(page.get_num_deleted_tuples(), 0);
        assert_eq!(page.get_next_page_id(), INVALID_PAGE_ID);
        assert!(!page.is_dirty());
        assert_eq!(page.get_pin_count(), 0);
    }

    #[test]
    fn test_page_initialization() {
        let mut page = TablePage::new(1);
        page.set_next_page_id(2);
        page.set_prev_page_id(3);
        page.set_dirty(true);
        page.increment_pin_count();

        page.init();

        assert_eq!(page.get_next_page_id(), INVALID_PAGE_ID);
        assert_eq!(page.prev_page_id, INVALID_PAGE_ID);
        assert_eq!(page.get_num_tuples(), 0);
        assert_eq!(page.get_num_deleted_tuples(), 0);
        assert!(page.tuple_info.is_empty());
    }
}

#[cfg(test)]
mod tuple_operation_tests {
    use super::*;
    use crate::catalogue::column::Column;
    use crate::catalogue::schema::Schema;
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
        let meta = TupleMeta::new(123, false);
        (meta, tuple)
    }

    #[test]
    fn test_basic_tuple_insertion() {
        let mut page = TablePage::new(1);
        let (meta, mut tuple) = create_test_tuple(1);

        let rid = page.insert_tuple(&meta, &mut tuple).unwrap();
        assert_eq!(page.get_num_tuples(), 1);
        assert_eq!(rid.get_page_id(), 1);
        assert_eq!(rid.get_slot_num(), 0);
    }

    #[test]
    fn test_tuple_retrieval() {
        let mut page = TablePage::new(1);
        let (meta, mut tuple) = create_test_tuple(1);

        let rid = page.insert_tuple(&meta, &mut tuple).unwrap();
        let (retrieved_meta, retrieved_tuple) = page.get_tuple(&rid).unwrap();

        assert_eq!(retrieved_meta.get_timestamp(), meta.get_timestamp());
        assert_eq!(retrieved_meta.is_deleted(), meta.is_deleted());
        assert_eq!(retrieved_tuple.get_value(0), tuple.get_value(0));
        assert_eq!(retrieved_tuple.get_value(1), tuple.get_value(1));
    }

    #[test]
    fn test_tuple_metadata_update() {
        let mut page = TablePage::new(1);
        let (meta, mut tuple) = create_test_tuple(1);

        let rid = page.insert_tuple(&meta, &mut tuple).unwrap();
        let new_meta = TupleMeta::new(456, true);

        page.update_tuple_meta(&new_meta, &rid).unwrap();
        let retrieved_meta = page.get_tuple_meta(&rid).unwrap();

        assert_eq!(retrieved_meta.get_timestamp(), new_meta.get_timestamp());
        assert_eq!(retrieved_meta.is_deleted(), new_meta.is_deleted());
        assert_eq!(page.get_num_deleted_tuples(), 1);
    }

    #[test]
    fn test_tuple_in_place_update() {
        let mut page = TablePage::new(1);
        let (meta, mut tuple) = create_test_tuple(1);

        let rid = page.insert_tuple(&meta, &mut tuple).unwrap();
        let new_meta = TupleMeta::new(789, false);
        let new_tuple = create_test_tuple(2).1;

        unsafe {
            page.update_tuple_in_place_unsafe(&new_meta, &new_tuple, rid)
                .unwrap();
        }

        let (retrieved_meta, retrieved_tuple) = page.get_tuple(&rid).unwrap();
        assert_eq!(retrieved_meta.get_timestamp(), new_meta.get_timestamp());
        assert_eq!(retrieved_tuple.get_value(0), new_tuple.get_value(0));
    }
}

#[cfg(test)]
mod error_handling_tests {
    use super::*;
    use crate::catalogue::column::Column;
    use crate::catalogue::schema::Schema;
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
        let meta = TupleMeta::new(123, false);
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
        let meta = TupleMeta::new(123, false);
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
    use crate::catalogue::column::Column;
    use crate::catalogue::schema::Schema;
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
        let meta = TupleMeta::new(123, false);
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
        assert_eq!(page.get_num_tuples(), 2);
    }

    #[test]
    fn test_header_size_constraints() {
        let page = TablePage::new(1);
        assert!(TablePage::header_size() < DB_PAGE_SIZE as u16);
        assert_eq!(page.tuple_info_size(), 0);
    }
}

#[cfg(test)]
mod concurrency_safety_tests {
    use super::*;
    use crate::catalogue::column::Column;
    use crate::catalogue::schema::Schema;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use std::sync::Arc;
    use std::thread;

    fn create_test_tuple(id: i32) -> (TupleMeta, Tuple) {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let values = vec![Value::from(id), Value::from("Test".to_string())];
        let rid = RID::new(1, 0);
        let tuple = Tuple::new(&values, schema, rid);
        let meta = TupleMeta::new(123, false);
        (meta, tuple)
    }

    #[test]
    fn test_pin_count_operations() {
        let mut page = TablePage::new(1);

        page.increment_pin_count();
        assert_eq!(page.get_pin_count(), 1);

        page.increment_pin_count();
        assert_eq!(page.get_pin_count(), 2);

        page.decrement_pin_count();
        assert_eq!(page.get_pin_count(), 1);

        page.decrement_pin_count();
        assert_eq!(page.get_pin_count(), 0);
    }

    #[test]
    fn test_concurrent_reads() {
        let mut page = Arc::new(TablePage::new(1));
        let (meta, mut tuple) = create_test_tuple(1);

        Arc::get_mut(&mut page)
            .unwrap()
            .insert_tuple(&meta, &mut tuple)
            .unwrap();

        let mut handles = vec![];

        for _ in 0..3 {
            let page_clone = Arc::clone(&page);
            let rid = RID::new(1, 0);

            let handle = thread::spawn(move || {
                let (retrieved_meta, retrieved_tuple) = page_clone.get_tuple(&rid).unwrap();
                assert_eq!(retrieved_meta.get_timestamp(), 123);
                assert_eq!(retrieved_tuple.get_value(0), &Value::from(1));
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}

#[cfg(test)]
mod serialization_tests {
    use super::*;
    use crate::catalogue::column::Column;
    use crate::catalogue::schema::Schema;
    use crate::types_db::type_id::TypeId::Integer;
    use crate::types_db::value::Value;

    fn create_test_tuple() -> (TupleMeta, Tuple) {
        let tuple = Tuple::new(
            &[Value::from(1)],
            Schema::new(vec![Column::new("col_a", Integer)]),
            Default::default(),
        );
        let tuple_meta = TupleMeta::new(123, false);
        (tuple_meta, tuple)
    }

    #[test]
    fn test_serialize_deserialize_empty_page() -> Result<(), Box<dyn std::error::Error>> {
        let original_page = TablePage::new(1);
        let serialized = original_page.serialize();

        assert_eq!(serialized.len(), DB_PAGE_SIZE as usize);

        let deserialized = TablePage::deserialize(&serialized)?;

        assert_eq!(deserialized.get_page_id(), original_page.get_page_id());
        assert_eq!(
            deserialized.get_num_tuples(),
            original_page.get_num_tuples()
        );
        assert_eq!(
            deserialized.get_next_page_id(),
            original_page.get_next_page_id()
        );
        assert_eq!(
            deserialized.get_num_deleted_tuples(),
            original_page.get_num_deleted_tuples()
        );

        Ok(())
    }

    #[test]
    fn test_serialize_deserialize_with_single_tuple() -> Result<(), Box<dyn std::error::Error>> {
        let mut original_page = TablePage::new(1);

        // Create and insert a test tuple
        let (meta, mut tuple) = create_test_tuple();
        let rid = original_page.insert_tuple(&meta, &mut tuple).unwrap();

        let serialized = original_page.serialize();
        assert_eq!(serialized.len(), DB_PAGE_SIZE as usize);

        let deserialized = TablePage::deserialize(&serialized)?;

        assert_eq!(deserialized.get_num_tuples(), 1);

        // Verify tuple data
        let original_tuple = original_page.get_tuple(&rid).unwrap();
        let deserialized_tuple = deserialized.get_tuple(&rid).unwrap();

        assert_eq!(
            original_tuple.0.get_timestamp(),
            deserialized_tuple.0.get_timestamp()
        );

        Ok(())
    }
}
