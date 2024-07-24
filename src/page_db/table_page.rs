use std::collections::HashMap;
use std::option::Option;

use crate::table::tuple::Tuple;

type PageId = i32;

#[derive(Clone, Debug)]
struct TupleMeta {}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct RID {}

pub struct TablePage {
    page_start: Vec<u8>,
    next_page_id: PageId,
    num_tuples: u16,
    num_deleted_tuples: u16,
    tuple_info: HashMap<RID, (u16, u16, TupleMeta)>,
}

impl TablePage {
    pub fn new(page_id: PageId) -> Self {
        Self {
            page_start: vec![],
            next_page_id: page_id,
            num_tuples: 0,
            num_deleted_tuples: 0,
            tuple_info: HashMap::new(),
        }
    }

    pub fn init(&mut self) {
        unimplemented!()
    }

    pub fn get_num_tuples(&self) -> u32 {
        self.num_tuples as u32
    }

    pub fn get_next_page_id(&self) -> PageId {
        self.next_page_id
    }

    pub fn set_next_page_id(&mut self, next_page_id: PageId) {
        self.next_page_id = next_page_id;
    }

    pub fn get_next_tuple_offset(&self, meta: &TupleMeta, tuple: &Tuple) -> Option<u16> {
        unimplemented!()
    }

    pub fn insert_tuple(&mut self, meta: &TupleMeta, tuple: &Tuple) -> Option<u16> {
        unimplemented!()
    }

    pub fn update_tuple_meta(&mut self, meta: &TupleMeta, rid: &RID) {
        unimplemented!()
    }

    pub fn get_tuple(&self, rid: &RID) -> Option<(TupleMeta, Tuple)> {
        unimplemented!()
    }

    pub fn get_tuple_meta(&self, rid: &RID) -> Option<TupleMeta> {
        unimplemented!()
    }

    pub fn update_tuple_in_place_unsafe(&mut self, meta: &TupleMeta, tuple: &Tuple, rid: &RID) {
        unimplemented!()
    }
}

// fn main() {
//     // Example usage of TablePage
//     let mut table_page = TablePage::new();
//     table_page.init();
//
//     // Further code...
// }
