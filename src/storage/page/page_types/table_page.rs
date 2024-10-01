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
    pub fn insert_tuple(&mut self, meta: &TupleMeta, tuple: &Tuple) -> Option<RID> {
        if let Some(tuple_offset) = self.get_next_tuple_offset(tuple) {
            self.tuple_info.push((tuple_offset, tuple.get_length().unwrap() as u16, meta.clone()));
            self.num_tuples += 1;

            let start = tuple_offset as usize;
            let end = start + tuple.get_length().unwrap();
            let tuple_data = bincode::serialize(&tuple).unwrap();
            self.data[start..end].copy_from_slice(tuple_data.as_slice());

            Some(tuple.get_rid())
        } else {
            None
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

    /// Updates a tuple in place.
    ///
    /// # Safety
    ///
    /// This method is unsafe because it doesn't perform any bounds checking.
    pub unsafe fn update_tuple_in_place_unsafe(&mut self, meta: &TupleMeta, tuple: &Tuple, rid: RID) -> Result<(), PageError> {
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

    // Helper methods

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
mod tests {
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
        let values = vec![
            Value::from(id),
            Value::from("Test".to_string()),
        ];
        let rid = RID::new(1, 0);
        let tuple = Tuple::new(values, schema, rid);
        let meta = TupleMeta::new(123, false);
        (meta, tuple)
    }

    #[test]
    fn test_insert_and_get_tuple() {
        let mut page = TablePage::new(1);
        let (meta, tuple) = create_test_tuple(1);

        let tuple_id = page.insert_tuple(&meta, &tuple).unwrap();
        assert_eq!(page.num_tuples, 1);

        let (retrieved_meta, retrieved_tuple) = page.get_tuple(&tuple_id).unwrap();
        assert_eq!(retrieved_meta.get_timestamp(), meta.get_timestamp());
        assert_eq!(retrieved_meta.is_deleted(), meta.is_deleted());
        assert_eq!(retrieved_tuple.get_value(0), tuple.get_value(0));
    }

    #[test]
    fn test_update_tuple_meta() {
        let mut page = TablePage::new(1);
        let (meta, tuple) = create_test_tuple(1);

        let tuple_rid_id = page.insert_tuple(&meta, &tuple).unwrap();
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
        while page.insert_tuple(&meta, &tuple).is_some() {
            inserted_count += 1;
        }

        assert!(inserted_count > 0);
        assert!(page.get_next_tuple_offset(&tuple).is_none());
    }

    #[test]
    fn test_update_tuple_in_place_unsafe() {
        let mut page = TablePage::new(1);
        let (meta, tuple) = create_test_tuple(1);

        let tuple_id = page.insert_tuple(&meta, &tuple).unwrap();

        let new_meta = TupleMeta::new(789, false);
        let mut new_tuple = create_test_tuple(2).1;

        unsafe {
            page.update_tuple_in_place_unsafe(&new_meta, &new_tuple, tuple_id).unwrap();
        }

        let (retrieved_meta, retrieved_tuple) = page.get_tuple(&tuple_id).unwrap();
        assert_eq!(retrieved_meta.get_timestamp(), new_meta.get_timestamp());
        assert_eq!(retrieved_tuple.get_value(0), new_tuple.get_value(0));
    }
}