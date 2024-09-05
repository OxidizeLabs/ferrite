use crate::common::config::{PageId, DB_PAGE_SIZE};
use crate::common::exception::PageError;
use crate::common::rid::RID;
use crate::container::hash_function::Xxh3Hasher;
use crate::storage::page::page::PageTrait;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use crate::catalogue::schema::Schema;
use bincode::{deserialize, serialize};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

// Create a type alias for our custom hasher
type XxHashBuilder = BuildHasherDefault<Xxh3Hasher>;

/// Represents a table page using a slotted page format.
#[derive(Debug, Clone)]
pub struct TablePage {
    page_start: Vec<u8>,
    next_page_id: PageId,
    num_tuples: u32,
    num_deleted_tuples: u32,
    tuple_info: HashMap<RID, (TupleMeta, Tuple), XxHashBuilder>,
}

impl TablePage {
    /// Creates a new `TablePage` with the given page ID.
    pub fn new(page_id: PageId) -> Self {
        let map = HashMap::with_hasher(BuildHasherDefault::<Xxh3Hasher>::default());
        Self {
            page_start: vec![],
            next_page_id: page_id,
            num_tuples: 0,
            num_deleted_tuples: 0,
            tuple_info: map,
        }
    }

    /// Returns the number of tuples in this page.
    pub fn get_num_tuples(&self) -> u32 {
        self.num_tuples as u32
    }

    /// Returns the page ID of the next table page.
    pub fn get_next_page_id(&self) -> PageId {
        self.next_page_id
    }

    /// Sets the page ID of the next page in the table.
    pub fn set_next_page_id(&mut self, next_page_id: PageId) {
        self.next_page_id = next_page_id;
    }

    /// Gets the next offset to insert a tuple.
    pub fn get_next_tuple_offset(&self, meta: &TupleMeta, tuple: &Tuple) -> Option<u32> {
        let tuple_size = self.serialize_tuple(meta, tuple).len() as u32;
        if self.page_start.len() as u32 + tuple_size <= DB_PAGE_SIZE as u32 {
            Some(self.page_start.len() as u32)
        } else {
            None
        }
    }

    /// Inserts a tuple into the table.
    pub fn insert_tuple(&mut self, meta: &TupleMeta, tuple: &Tuple) -> Option<RID> {
        if let Some(offset) = self.get_next_tuple_offset(meta, tuple) {
            let rid = RID::new(self.next_page_id, offset);

            self.tuple_info.insert(rid, (meta.clone(), tuple.clone()));
            self.num_tuples += 1;

            // Update page_start with the new tuple data
            self.page_start.extend_from_slice(&self.serialize_tuple(meta, tuple));

            Some(rid)
        } else {
            None
        }
    }

    /// Updates the metadata of a tuple.
    pub fn update_tuple_meta(&mut self, meta: &TupleMeta, rid: &RID) {
        if let Some((old_meta, tuple)) = self.tuple_info.get_mut(rid) {
            *old_meta = meta.clone();
        }
    }

    /// Reads a tuple from the table.
    pub fn get_tuple(&self, rid: &RID) -> Option<(TupleMeta, Tuple)> {
        if let Some((meta, tuple)) = self.tuple_info.get(rid) {
            Some((meta.clone(), tuple.clone()))
        } else {
            None
        }
    }

    /// Updates a tuple in place.
    ///
    /// # Safety
    ///
    /// This method is unsafe because it doesn't perform any bounds checking.
    pub unsafe fn update_tuple_in_place_unsafe(&mut self, meta: &TupleMeta, tuple: &Tuple, rid: &RID) {
        if let Some((old_meta, old_tuple)) = self.tuple_info.get_mut(rid) {
            *old_meta = meta.clone();
            *old_tuple = tuple.clone();

            // Update the page_start with the new tuple data
            let offset = rid.get_slot_num() as usize * self.calculate_tuple_size(meta, tuple);
            let new_data = self.serialize_tuple(meta, tuple);
            self.page_start[offset..offset + new_data.len()].copy_from_slice(&new_data);
        }
    }

    // Helper methods

    fn calculate_tuple_size(&self, meta: &TupleMeta, tuple: &Tuple) -> usize {
        // Calculate the size of the tuple including its metadata
        let serialized = self.serialize_tuple(meta, tuple);
        serialized.len()
    }

    fn calculate_free_space(&self) -> usize {
        // Calculate the remaining free space in the page
        self.page_start.capacity() - self.page_start.len()
    }

    fn serialize_tuple(&self, meta: &TupleMeta, tuple: &Tuple) -> Vec<u8> {
        serialize(&(meta, tuple)).unwrap_or_else(|_| Vec::new())
    }

    fn deserialize_tuple(&self, data: &[u8]) -> Option<(TupleMeta, Tuple)> {
        // Deserialize the tuple and its metadata from bytes using bincode
        deserialize(data).ok()
    }
}

impl PageTrait for TablePage {
    fn get_page_id(&self) -> PageId {
        todo!()
    }

    fn is_dirty(&self) -> bool {
        todo!()
    }

    fn set_dirty(&mut self, is_dirty: bool) {
        todo!()
    }

    fn get_pin_count(&self) -> i32 {
        todo!()
    }

    fn increment_pin_count(&mut self) {
        todo!()
    }

    fn decrement_pin_count(&mut self) {
        todo!()
    }

    fn get_data(&self) -> &[u8; DB_PAGE_SIZE] {
        todo!()
    }

    fn get_data_mut(&mut self) -> &mut [u8; DB_PAGE_SIZE] {
        todo!()
    }

    fn set_data(&mut self, offset: usize, new_data: &[u8]) -> Result<(), PageError> {
        todo!()
    }

    fn set_pin_count(&mut self, pin_count: i32) {
        todo!()
    }

    fn reset_memory(&mut self) {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use log::info;
    use crate::catalogue::column::Column;
    use crate::types_db::type_id::TypeId::Integer;
    use crate::types_db::value::Value;
    use super::*;

    #[test]
    fn new_table_page() {
        let page = TablePage::new(1);
        assert_eq!(page.get_num_tuples(), 0);
        assert_eq!(page.get_next_page_id(), 1);
    }

    #[test]
    fn insert_and_get_tuple() {
        let mut page = TablePage::new(1);
        let meta = TupleMeta::new(123, false);
        let schema = Schema::new(vec![Column::new("col_1", Integer), Column::new("col_2", Integer), Column::new("col_3", Integer)]);
        let rid = RID::new(0,0);
        let tuple = Tuple::new(vec![Value::from(1), Value::from(2), Value::from(3)], schema.clone(), rid);

        info!("Initial page state: {:?}", page);

        match page.insert_tuple(&meta, &tuple) {
            Some(inserted_rid) => {
                info!("Tuple inserted successfully. RID: {:?}", inserted_rid);
                assert_eq!(page.get_num_tuples(), 1);

                info!("Page state after insertion: {:?}", page);

                match page.get_tuple(&inserted_rid) {
                    Some((retrieved_meta, retrieved_tuple)) => {
                        info!("Retrieved meta: {:?}", retrieved_meta);
                        info!("Retrieved tuple: {:?}", retrieved_tuple);
                        assert_eq!(retrieved_meta.get_timestamp(), meta.get_timestamp());
                        assert_eq!(retrieved_meta.is_deleted(), meta.is_deleted());
                        assert_eq!(retrieved_tuple.get_value(0), tuple.get_value(0));
                    },
                    None => panic!("Failed to retrieve tuple with RID: {:?}", inserted_rid),
                }
            },
            None => panic!("Failed to insert tuple"),
        }
    }

    #[test]
    fn update_tuple_meta() {
        let mut page = TablePage::new(1);
        let meta = TupleMeta::new(123, false);
        let schema = Schema::new(vec![
            Column::new("col_1", Integer),
            Column::new("col_2", Integer),
            Column::new("col_3", Integer)
        ]);
        let rid = RID::new(0,0);
        let tuple = Tuple::new(vec![Value::from(1), Value::from(2), Value::from(3)], schema, rid);

        let new_meta = TupleMeta::new(453, false);
        page.insert_tuple(&meta, &tuple);
        page.update_tuple_meta(&new_meta, &rid);

        let retrieved_meta = page.get_tuple(&rid).unwrap();
        assert_eq!(retrieved_meta.0.get_timestamp(), new_meta.get_timestamp());
        assert_eq!(retrieved_meta.0.is_deleted(), new_meta.is_deleted());
    }

    #[test]
    fn page_full() {
        let mut page = TablePage::new(1);
        let meta = TupleMeta::new(123, false);
        let schema = Schema::new(vec![
            Column::new("col_1", Integer);1000
        ]);
        let rid = RID::new(0,0);
        let tuple = Tuple::new(vec![Value::from(1); 1000], schema, rid);

        // Insert tuples until the page is full
        while page.insert_tuple(&meta, &tuple).is_some() {}

        assert!(page.get_next_tuple_offset(&meta, &tuple).is_none());
        assert!(page.insert_tuple(&meta, &tuple).is_none());
    }

    #[test]
    fn serialize_deserialize_tuple() {
        let mut page = TablePage::new(1);
        let meta = TupleMeta::new(123, false);
        let schema = Schema::new(vec![Column::new("col_1", Integer), Column::new("col_2", Integer), Column::new("col_3", Integer)]);
        let rid = RID::new(0,0);
        let tuple = Tuple::new(vec![Value::from(1), Value::from(1), Value::from(1)], schema, rid);

        let serialized = page.serialize_tuple(&meta, &tuple);
        let (deserialized_meta, deserialized_tuple) = page.deserialize_tuple(&serialized).unwrap();

        assert_eq!(deserialized_meta.get_timestamp(), meta.get_timestamp());
        assert_eq!(deserialized_meta.is_deleted(), meta.is_deleted());
        assert_eq!(deserialized_tuple.get_value(0), tuple.get_value(0));
    }
}
